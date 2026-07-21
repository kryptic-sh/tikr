//! Pure functions that translate Hyperliquid wire structs ([`crate::messages`])
//! into [`tikr_core`] domain types.
//!
//! # Defensive parsing
//!
//! Decimal-valued strings are parsed via [`Decimal::from_str_exact`]. On parse
//! failure we fall back to [`Decimal::ZERO`] and emit a [`tracing::warn!`]
//! rather than panic; bad wire data must not take down the strategy loop.
//!
//! # Side convention
//!
//! Hyperliquid's `side` field carries the *taker* side for trades and the
//! *user* side for fills:
//!
//! - `"A"` (ask-taker) → [`Side::Ask`] — someone sold into a bid.
//! - `"B"` (bid-taker) → [`Side::Bid`] — someone bought from an ask.
//!
//! [`tikr_core::MarketEvent::Trade::side`] carries the aggressor side; for
//! [`Fill`] the user's own side is preserved, also using B → Bid / A → Ask.

use crate::exchange::quote_id_from_oid;
use crate::messages::*;
use std::str::FromStr;
use tikr_core::{
    Asset, Decimal, Fill, Level, MarketEvent, Notional, Position, Price, QuoteId, SignedSize, Size,
    Snapshot, Symbol, Timestamp, Uuid,
};
use tikr_venue::OpenOrder;
use tracing::warn;

/// Parse a decimal string defensively. Returns `Decimal::ZERO` on failure.
fn parse_decimal(s: &str, field: &str) -> Decimal {
    match Decimal::from_str_exact(s) {
        Ok(d) => d,
        Err(_) => {
            // Some venues emit scientific notation or extra precision that
            // `from_str_exact` rejects; try the relaxed parser before giving up.
            match Decimal::from_str(s) {
                Ok(d) => d,
                Err(e) => {
                    warn!(field, value = s, error = %e, "failed to parse decimal from hyperliquid wire data");
                    Decimal::ZERO
                }
            }
        }
    }
}

fn ms_to_ns(ms: u64) -> u64 {
    ms.saturating_mul(1_000_000)
}

// ---------------------------------------------------------------------------
// L2 book → Snapshot
// ---------------------------------------------------------------------------

/// Convert an `l2Book` push to a [`Snapshot`]. The `symbol` is supplied by the
/// caller because Hyperliquid only sends the coin name; the venue/quote
/// context lives in the subscription state.
pub fn l2_to_snapshot(symbol: &Symbol, push: &L2BookPush) -> Snapshot {
    let bids = push.levels[0]
        .iter()
        .map(|l| Level {
            price: Price(parse_decimal(&l.px, "l2Book.bid.px")),
            size: Size(parse_decimal(&l.sz, "l2Book.bid.sz")),
        })
        .collect();
    let asks = push.levels[1]
        .iter()
        .map(|l| Level {
            price: Price(parse_decimal(&l.px, "l2Book.ask.px")),
            size: Size(parse_decimal(&l.sz, "l2Book.ask.sz")),
        })
        .collect();
    Snapshot {
        symbol: symbol.clone(),
        bids,
        asks,
        ts: Timestamp(ms_to_ns(push.time)),
    }
}

// ---------------------------------------------------------------------------
// Trade push → MarketEvent::Trade
// ---------------------------------------------------------------------------

/// Convert one trade entry to a [`MarketEvent::Trade`].
pub fn trade_to_event(symbol: &Symbol, t: &TradePush) -> MarketEvent {
    let side = side_from_str(&t.side);
    MarketEvent::Trade {
        symbol: symbol.clone(),
        price: Price(parse_decimal(&t.px, "trade.px")),
        size: Size(parse_decimal(&t.sz, "trade.sz")),
        side,
        ts: Timestamp(ms_to_ns(t.time)),
    }
}

/// Convert an `openOrders` entry to an [`OpenOrder`].
///
/// The `id` is derived from the venue `oid` via [`quote_id_from_oid`] so it
/// matches the `QuoteId` returned by [`Venue::quote`][tikr_venue::Venue::quote]
/// — this is what lets the runner's open-order reconciliation match resting
/// orders against locally-tracked quotes. Per-coin filtering happens upstream
/// in [`HyperliquidClient::open_orders`](crate::client::HyperliquidClient::open_orders).
pub fn open_order_from_entry(symbol: &Symbol, e: &OpenOrderEntry) -> OpenOrder {
    OpenOrder {
        id: quote_id_from_oid(e.oid),
        symbol: symbol.clone(),
        side: side_from_str(&e.side),
        price: Price(parse_decimal(&e.limit_px, "openOrder.limitPx")),
        size: Size(parse_decimal(&e.sz, "openOrder.sz")),
    }
}

fn side_from_str(s: &str) -> tikr_core::Side {
    if s == "B" {
        tikr_core::Side::Bid
    } else {
        // `"A"` (and any unknown value) → Ask. The Hyperliquid spec only
        // emits A or B, so the fallback is purely defensive.
        tikr_core::Side::Ask
    }
}

// ---------------------------------------------------------------------------
// clearinghouseState → Position
// ---------------------------------------------------------------------------

/// Resolve the position for `symbol` from a `clearinghouseState` response.
///
/// If no `assetPositions` entry matches the symbol's base coin, returns a
/// flat position (`size = 0`, `avg_entry = 0`).
///
/// `realized_pnl` is set to zero in this Phase 3 implementation; computing
/// it from `cumFunding` + `unrealizedPnl` is deferred to a later phase.
pub fn position_from_clearinghouse(symbol: &Symbol, resp: &ClearinghouseStateResp) -> Position {
    let coin = symbol.base.0.as_ref();
    for entry in &resp.asset_positions {
        if entry.position.coin.eq_ignore_ascii_case(coin) {
            let size = parse_decimal(&entry.position.szi, "clearinghouseState.szi");
            let entry_price = entry
                .position
                .entry_px
                .as_deref()
                .map(|s| parse_decimal(s, "clearinghouseState.entryPx"))
                .unwrap_or(Decimal::ZERO);
            return Position {
                symbol: symbol.clone(),
                size: SignedSize(size),
                avg_entry: Price(entry_price),
                realized_pnl: Notional(Decimal::ZERO),
            };
        }
    }
    Position {
        symbol: symbol.clone(),
        size: SignedSize(Decimal::ZERO),
        avg_entry: Price(Decimal::ZERO),
        realized_pnl: Notional(Decimal::ZERO),
    }
}

// ---------------------------------------------------------------------------
// userFills → Fill
// ---------------------------------------------------------------------------

/// Convert a `userFills` entry to a [`Fill`].
///
/// Per-symbol filtering happens upstream in
/// [`HyperliquidClient::user_fills_since`](crate::client::HyperliquidClient::user_fills_since)
/// via [`UserFillEntry::coin`] before this mapping runs.
///
/// `oid` (u64) is widened to [`Uuid::from_u128`] to populate
/// [`QuoteId::from_uuid`]. This gives each Hyperliquid order a stable,
/// venue-correlated id without colliding with native UUID-shaped ids.
pub fn fill_from_user_fill(f: &UserFillEntry) -> Fill {
    let side = side_from_str(&f.side);
    let price = parse_decimal(&f.px, "userFill.px");
    let size = parse_decimal(&f.sz, "userFill.sz");
    let fee = parse_decimal(&f.fee, "userFill.fee");
    Fill {
        quote_id: QuoteId::from_uuid(Uuid::from_u128(f.oid as u128)),
        price: Price(price),
        size: Size(size),
        fee_asset: Asset::new(&f.fee_token),
        fee_amount: fee,
        fee_quote: Notional(fee),
        side,
        ts: Timestamp(ms_to_ns(f.time)),
        // Hyperliquid userFill events don't expose a per-fill remaining-size
        // status. Conservatively mark as partial so reconciliation removes
        // filled orders only when open-orders API confirms absence.
        is_full: false,
        // The venue trade id is what `fills_since` reconciliation dedupes
        // against WS-delivered fills — `None` would double-count them.
        trade_id: Some(f.tid),
    }
}
