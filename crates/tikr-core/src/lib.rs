//! Canonical type vocabulary for the tikr market-making engine.
//!
//! All other crates consume these types.
//!
//! # Perspective
//!
//! [`Side`] is expressed from the market-maker's perspective:
//! [`Side::Bid`] is our buy quote, [`Side::Ask`] is our sell quote.
//!
//! # Arithmetic
//!
//! `Price * Size = Notional` via [`Mul`] impls on [`Price`] and [`Size`].
//! No other cross-type multiplication is defined.
//!
//! # Asset normalization
//!
//! [`Asset::new`] uppercases and trims whitespace. [`VenueId::new`] trims
//! only (venue ids are lowercase by convention; case is preserved).

#![deny(missing_docs)]

use std::{
    fmt,
    ops::{Add, Mul, Neg, Sub},
    sync::Arc,
};

use serde::{Deserialize, Serialize};

pub use uuid::Uuid;

pub use rust_decimal::Decimal;

// ---------------------------------------------------------------------------
// Timestamp
// ---------------------------------------------------------------------------

/// Nanoseconds since the UNIX epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(pub u64);

// ---------------------------------------------------------------------------
// Price
// ---------------------------------------------------------------------------

/// A quoted price, represented as a [`Decimal`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Price(pub Decimal);

impl Add<Price> for Price {
    type Output = Price;
    fn add(self, rhs: Price) -> Price {
        Price(self.0 + rhs.0)
    }
}

impl Sub<Price> for Price {
    type Output = Price;
    fn sub(self, rhs: Price) -> Price {
        Price(self.0 - rhs.0)
    }
}

// ---------------------------------------------------------------------------
// Size
// ---------------------------------------------------------------------------

/// An unsigned order/fill size.
///
/// Semantically non-negative; the inner [`Decimal`] does not enforce this.
/// Subtraction may yield a negative inner value — the caller is responsible.
/// A `try_sub` may be added in a future version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Size(pub Decimal);

impl Add<Size> for Size {
    type Output = Size;
    fn add(self, rhs: Size) -> Size {
        Size(self.0 + rhs.0)
    }
}

impl Sub<Size> for Size {
    type Output = Size;
    fn sub(self, rhs: Size) -> Size {
        Size(self.0 - rhs.0)
    }
}

// ---------------------------------------------------------------------------
// SignedSize
// ---------------------------------------------------------------------------

/// A signed size, used for net position accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SignedSize(pub Decimal);

impl Add<SignedSize> for SignedSize {
    type Output = SignedSize;
    fn add(self, rhs: SignedSize) -> SignedSize {
        SignedSize(self.0 + rhs.0)
    }
}

impl Sub<SignedSize> for SignedSize {
    type Output = SignedSize;
    fn sub(self, rhs: SignedSize) -> SignedSize {
        SignedSize(self.0 - rhs.0)
    }
}

impl Neg for SignedSize {
    type Output = SignedSize;
    fn neg(self) -> SignedSize {
        SignedSize(-self.0)
    }
}

// ---------------------------------------------------------------------------
// Notional
// ---------------------------------------------------------------------------

/// A currency-denominated value (price × size).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Notional(pub Decimal);

impl Add<Notional> for Notional {
    type Output = Notional;
    fn add(self, rhs: Notional) -> Notional {
        Notional(self.0 + rhs.0)
    }
}

impl Sub<Notional> for Notional {
    type Output = Notional;
    fn sub(self, rhs: Notional) -> Notional {
        Notional(self.0 - rhs.0)
    }
}

// ---------------------------------------------------------------------------
// Cross-type multiplication: Price × Size = Notional
// ---------------------------------------------------------------------------

impl Mul<Size> for Price {
    type Output = Notional;
    /// `Price × Size = Notional`.
    fn mul(self, rhs: Size) -> Notional {
        Notional(self.0 * rhs.0)
    }
}

impl Mul<Price> for Size {
    type Output = Notional;
    /// `Size × Price = Notional` (symmetric for ergonomics).
    fn mul(self, rhs: Price) -> Notional {
        Notional(self.0 * rhs.0)
    }
}

// ---------------------------------------------------------------------------
// Side
// ---------------------------------------------------------------------------

/// Market-maker quoting side.
///
/// [`Bid`][Side::Bid] is our buy quote; [`Ask`][Side::Ask] is our sell quote.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    /// Buy-side quote (market-maker perspective).
    Bid,
    /// Sell-side quote (market-maker perspective).
    Ask,
}

// ---------------------------------------------------------------------------
// Asset
// ---------------------------------------------------------------------------

/// A normalized asset ticker (e.g. `"BTC"`, `"ETH"`).
///
/// Constructed via [`Asset::new`] which uppercases and trims whitespace.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Asset(pub Arc<str>);

impl Asset {
    /// Normalize `raw`: trim ASCII whitespace and uppercase, then intern as `Arc<str>`.
    pub fn new(raw: &str) -> Self {
        Asset(raw.trim().to_uppercase().into())
    }
}

// ---------------------------------------------------------------------------
// VenueId
// ---------------------------------------------------------------------------

/// An opaque venue identifier (e.g. `"hyperliquid"`).
///
/// Constructed via [`VenueId::new`] which trims whitespace but preserves case
/// (venue ids are lowercase by convention; normalization is the caller's responsibility).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VenueId(pub Arc<str>);

impl VenueId {
    /// Trim whitespace from `raw` and intern as `Arc<str>`. Case is preserved.
    pub fn new(raw: &str) -> Self {
        VenueId(raw.trim().into())
    }
}

// ---------------------------------------------------------------------------
// MarketKind
// ---------------------------------------------------------------------------

/// The market type of a [`Symbol`].
///
/// Distinguishes between spot and perpetual-futures markets so that venues,
/// strategies, and risk layers can make context-aware decisions without
/// inspecting venue-specific naming conventions.
///
/// # Variants
///
/// - [`Spot`][MarketKind::Spot] — Direct asset exchange (e.g. BTC/USDT spot).
/// - [`Perp`][MarketKind::Perp] — Perpetual futures contract (e.g. BTC-PERP).
///   No expiry date; funding-rate mechanics keep the price anchored.
///
/// # Future extensibility
///
/// COIN-M dated futures and options are explicitly out of scope for v0.
/// New variants will be added as separate issues once a venue requires them.
///
/// # Serialization
///
/// Serializes as lowercase JSON strings (`"spot"`, `"perp"`) to match
/// Binance and other venue conventions, and to produce readable
/// `PaperReport` output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MarketKind {
    /// Direct asset exchange; no expiry, no funding rate.
    Spot,
    /// Perpetual futures contract; no expiry, funding-rate anchored.
    Perp,
}

impl fmt::Display for MarketKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MarketKind::Spot => f.write_str("spot"),
            MarketKind::Perp => f.write_str("perp"),
        }
    }
}

// ---------------------------------------------------------------------------
// Symbol
// ---------------------------------------------------------------------------

/// A trading pair on a specific venue.
///
/// Every construction site must declare an explicit [`MarketKind`] — there is
/// no `Default` impl. Each venue knows whether it is dealing with spot or
/// perpetual markets and must annotate accordingly:
/// `tikr-hyperliquid` → [`MarketKind::Perp`],
/// `tikr-dodo` → [`MarketKind::Spot`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Symbol {
    /// Base asset (e.g. `BTC`).
    pub base: Asset,
    /// Quote asset (e.g. `USDT`).
    pub quote: Asset,
    /// Venue where this symbol is traded.
    pub venue: VenueId,
    /// Market type: spot exchange or perpetual futures contract.
    pub kind: MarketKind,
}

// ---------------------------------------------------------------------------
// TimeInForce
// ---------------------------------------------------------------------------

/// Order time-in-force policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeInForce {
    /// Post-only: reject if the order would immediately match.
    PostOnly,
    /// Good-till-cancelled.
    GTC,
    /// Immediate-or-cancel.
    IOC,
    /// Fill-or-kill.
    FOK,
}

// ---------------------------------------------------------------------------
// QuoteKind
// ---------------------------------------------------------------------------

/// Shape of a market-making quote.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteKind {
    /// Single-price point quote.
    Point,
    /// Range quote with a specified width in basis points.
    Range {
        /// Width of the quote range in basis points.
        width_bps: u32,
    },
}

// ---------------------------------------------------------------------------
// QuoteId
// ---------------------------------------------------------------------------

/// Adapter-assigned quote identifier, backed by a UUID v4.
///
/// Use [`QuoteId::new`] to mint a fresh id, or [`QuoteId::from_uuid`] when
/// converting a venue-native UUID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QuoteId(pub Uuid);

impl QuoteId {
    /// Mint a new, globally unique quote identifier.
    pub fn new() -> Self {
        QuoteId(Uuid::new_v4())
    }

    /// Wrap an existing [`Uuid`] as a [`QuoteId`].
    pub fn from_uuid(u: Uuid) -> Self {
        QuoteId(u)
    }
}

impl Default for QuoteId {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Level
// ---------------------------------------------------------------------------

/// A single price level in an order book.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Level {
    /// Price at this level.
    pub price: Price,
    /// Aggregated size available at this level.
    pub size: Size,
}

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

/// A full order-book snapshot for a symbol at a point in time.
///
/// Invariant (adapter-guaranteed, not enforced at construction):
/// `bids` sorted descending by price, `asks` sorted ascending by price.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Symbol this snapshot belongs to.
    pub symbol: Symbol,
    /// Bid levels, sorted descending by price.
    pub bids: Vec<Level>,
    /// Ask levels, sorted ascending by price.
    pub asks: Vec<Level>,
    /// Timestamp of the snapshot.
    pub ts: Timestamp,
}

// ---------------------------------------------------------------------------
// Fill
// ---------------------------------------------------------------------------

/// A completed fill on one of our quotes.
#[derive(Debug, Clone)]
pub struct Fill {
    /// Adapter-assigned quote id that was filled.
    pub quote_id: QuoteId,
    /// Fill price.
    pub price: Price,
    /// Fill size.
    pub size: Size,
    /// Asset in which the fee is denominated.
    pub fee_asset: Asset,
    /// Raw fee amount in `fee_asset` units.
    pub fee_amount: Decimal,
    /// Fee expressed as quote-currency notional.
    pub fee_quote: Notional,
    /// Side of the fill from the market-maker's perspective.
    pub side: Side,
    /// Timestamp of the fill.
    pub ts: Timestamp,
    /// `true` when this fill consumed the entire remaining size of the
    /// resting order (Binance `X=FILLED`, FillSim `size_remaining==0`).
    /// `false` for partial fills — the order is still resting on the book
    /// and strategies should not re-quote.
    pub is_full: bool,
    /// Venue trade id when known (Binance `ORDER_TRADE_UPDATE.o.t` over WS,
    /// or the `id` field from `GET /fapi/v1/userTrades` over REST). Used to
    /// deduplicate fills that arrive via BOTH the WS stream and the REST
    /// gap-fill reconciliation path, so missed fills replay exactly once.
    /// `None` for simulated fills (FillSim, strategy tests) which have no
    /// venue trade identity.
    pub trade_id: Option<u64>,
}

// ---------------------------------------------------------------------------
// LiqEvent
// ---------------------------------------------------------------------------

/// A single forced-liquidation event from the venue's liquidation stream.
///
/// Binance USD-M Futures broadcasts these on `!forceOrder@arr`; the recorder
/// (`record_liquidations`) writes them to parquet with one row per event.
/// Strategies that care about liquidation cascades (e.g. `LiqFade`) consume
/// these via `StrategyContext::recent_liqs` — a rolling window maintained
/// by the runner.
///
/// `side` is from the **liquidated trader's** perspective: a liquidated
/// long is forced to sell (Side::Ask), a liquidated short to buy
/// (Side::Bid). `notional = qty × price`, pre-computed by the recorder
/// so consumers don't repeat the multiply.
#[derive(Debug, Clone, Copy)]
pub struct LiqEvent {
    /// Event timestamp from the venue, nanoseconds since UNIX epoch.
    pub ts: Timestamp,
    /// Side of the forced order on the book (the side the liquidated
    /// trader's hedge crosses to).
    pub side: Side,
    /// Liquidated quantity in base units.
    pub qty: Size,
    /// Fill price of the forced order.
    pub price: Price,
    /// Pre-computed notional in quote currency (`qty × price`).
    pub notional: Notional,
}

// ---------------------------------------------------------------------------
// Position
// ---------------------------------------------------------------------------

/// Current position in a symbol.
#[derive(Debug, Clone, PartialEq)]
pub struct Position {
    /// Symbol held.
    pub symbol: Symbol,
    /// Net signed size (positive = long, negative = short).
    pub size: SignedSize,
    /// Volume-weighted average entry price.
    pub avg_entry: Price,
    /// Cumulative realized PnL.
    pub realized_pnl: Notional,
}

// ---------------------------------------------------------------------------
// LiquidationEvent
// ---------------------------------------------------------------------------

/// A forced liquidation event broadcast by Binance USD-M Futures.
///
/// The `!forceOrder@arr` stream emits one frame per liquidated position.
/// When many liquidations pile up on the same side in a short window it
/// signals a "cascade" — the price typically overshoots and then reverts.
///
/// ## Side semantics
///
/// The `side` field carries the **liquidation order** side (the direction of
/// the force-close market order):
/// - [`Side::Ask`] (`"SELL"`) — a long position was force-closed.
///   The exchange dumped base → price moved down → mean-revert entry is LONG.
/// - [`Side::Bid`] (`"BUY"`) — a short position was force-closed.
///   The exchange bought base → price moved up → mean-revert entry is SHORT.
///
/// ## Symbol
///
/// `symbol` is the raw Binance ticker (e.g. `"BTCUSDT"`). It is kept as a
/// plain `String` because the stream covers all symbols simultaneously;
/// resolving to a typed [`Symbol`] is the caller's responsibility.
#[derive(Debug, Clone)]
pub struct LiquidationEvent {
    /// Raw Binance symbol string, e.g. `"BTCUSDT"`.
    pub symbol: String,
    /// Direction of the force-close order.
    /// [`Side::Ask`] = long liquidated; [`Side::Bid`] = short liquidated.
    pub side: Side,
    /// Liquidated quantity (base asset).
    pub qty: Decimal,
    /// Average fill price (`ap` field) — the actual execution price.
    pub price: Price,
    /// Pre-computed notional value: `qty × price` (USDT).
    pub notional: Decimal,
    /// Event timestamp (nanoseconds since UNIX epoch), derived from `T` (ms).
    pub ts: Timestamp,
}

// ---------------------------------------------------------------------------
// MarketEvent
// ---------------------------------------------------------------------------

/// An event emitted by a market-data or execution adapter.
#[derive(Debug, Clone)]
pub enum MarketEvent {
    /// A full order-book snapshot update.
    BookUpdate {
        /// The new snapshot.
        snapshot: Snapshot,
    },
    /// An individual trade observed on the venue.
    Trade {
        /// Symbol that traded.
        symbol: Symbol,
        /// Execution price.
        price: Price,
        /// Execution size.
        size: Size,
        /// Aggressor side.
        side: Side,
        /// Timestamp of the trade.
        ts: Timestamp,
    },
    /// A fill on one of our resting quotes.
    Fill(Fill),
    /// Liveness heartbeat from the adapter.
    Heartbeat {
        /// Timestamp of the heartbeat.
        ts: Timestamp,
    },
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    fn dec(mantissa: i64, scale: u32) -> Decimal {
        Decimal::new(mantissa, scale)
    }

    #[test]
    fn asset_normalizes_case_and_whitespace() {
        assert_eq!(Asset::new("  btc  "), Asset::new("BTC"));
    }

    #[test]
    fn venue_id_trims_but_keeps_case() {
        assert_eq!(VenueId::new("  Hyperliquid  "), VenueId::new("Hyperliquid"));
        assert_ne!(VenueId::new("Hyperliquid"), VenueId::new("hyperliquid"));
    }

    #[test]
    fn price_size_mul_gives_notional() {
        // 100 * 0.5 = 50.0
        let p = Price(Decimal::new(100, 0));
        let s = Size(Decimal::new(5, 1));
        assert_eq!(p * s, Notional(Decimal::new(500, 1)));
    }

    #[test]
    fn notional_mul_commutative() {
        let p = Price(Decimal::new(100, 0));
        let s = Size(Decimal::new(5, 1));
        assert_eq!(p * s, s * p);
    }

    #[test]
    fn decimal_chain_no_rounding() {
        // 0.1^10 should be representable exactly in Decimal
        let mut val = Decimal::new(1, 1); // 0.1
        let factor = Decimal::new(1, 1);
        for _ in 0..9 {
            val *= factor;
        }
        // 0.1^10 = 1e-10
        let expected = Decimal::new(1, 10);
        assert_eq!(val, expected);
    }

    #[test]
    fn signed_size_negate() {
        let pos = SignedSize(Decimal::new(5, 0));
        let neg = -pos;
        assert_eq!(neg, SignedSize(Decimal::new(-5, 0)));
    }

    #[test]
    fn position_round_trip() {
        let sym = Symbol {
            base: Asset::new("BTC"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("hyperliquid"),
            kind: MarketKind::Perp,
        };
        let pos = Position {
            symbol: sym.clone(),
            size: SignedSize(dec(1, 0)),
            avg_entry: Price(dec(60_000, 0)),
            realized_pnl: Notional(dec(0, 0)),
        };
        let cloned = pos.clone();
        assert_eq!(pos, cloned);
    }

    #[test]
    fn market_event_variants() {
        let sym = Symbol {
            base: Asset::new("ETH"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("hyperliquid"),
            kind: MarketKind::Perp,
        };
        let ts = Timestamp(0);

        let book = MarketEvent::BookUpdate {
            snapshot: Snapshot {
                symbol: sym.clone(),
                bids: vec![],
                asks: vec![],
                ts,
            },
        };
        let trade = MarketEvent::Trade {
            symbol: sym.clone(),
            price: Price(dec(3000, 0)),
            size: Size(dec(1, 0)),
            side: Side::Ask,
            ts,
        };
        let fill = MarketEvent::Fill(Fill {
            quote_id: QuoteId::new(),
            price: Price(dec(3000, 0)),
            size: Size(dec(1, 0)),
            fee_asset: Asset::new("USDT"),
            fee_amount: dec(3, 0),
            fee_quote: Notional(dec(3, 0)),
            side: Side::Bid,
            ts,
            is_full: true,
            trade_id: None,
        });
        let hb = MarketEvent::Heartbeat { ts };

        // Exhaustive pattern match — compiler enforces all variants covered.
        let mut variant_count = 0u32;
        for event in [book, trade, fill, hb] {
            match event {
                MarketEvent::BookUpdate { .. } => variant_count += 1,
                MarketEvent::Trade { .. } => variant_count += 1,
                MarketEvent::Fill(_) => variant_count += 1,
                MarketEvent::Heartbeat { .. } => variant_count += 1,
            }
        }
        assert_eq!(variant_count, 4);
    }

    #[test]
    fn side_inequality() {
        assert_ne!(Side::Bid, Side::Ask);
    }

    #[test]
    fn market_kind_display() {
        assert_eq!(MarketKind::Spot.to_string(), "spot");
        assert_eq!(MarketKind::Perp.to_string(), "perp");
    }

    #[test]
    fn market_kind_serde_roundtrip() {
        let spot = MarketKind::Spot;
        let perp = MarketKind::Perp;
        let spot_json = serde_json::to_string(&spot).unwrap();
        let perp_json = serde_json::to_string(&perp).unwrap();
        assert_eq!(spot_json, "\"spot\"");
        assert_eq!(perp_json, "\"perp\"");
        let spot_back: MarketKind = serde_json::from_str(&spot_json).unwrap();
        let perp_back: MarketKind = serde_json::from_str(&perp_json).unwrap();
        assert_eq!(spot_back, MarketKind::Spot);
        assert_eq!(perp_back, MarketKind::Perp);
    }
}
