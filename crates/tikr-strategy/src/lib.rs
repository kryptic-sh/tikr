//! Strategy trait and reference implementations for the tikr market-making engine.
//!
//! # Determinism invariant
//!
//! Strategy impls MUST be deterministic — given the same config and event
//! sequence, an instance produces the same Action sequence. This is what makes
//! backtest results reproducible and live↔paper↔backtest comparable.
//!
//! # No-I/O invariant
//!
//! Strategy impls MUST NOT perform I/O. All inputs arrive via [`StrategyContext`]
//! and [`MarketEvent`]. I/O lives in the executor + venue layers.
//!
//! # Thread-safety
//!
//! Each Strategy instance is owned by one executor thread. The [`Send`] bound
//! enables moving between threads; there is no shared-mutable assumption.
//!
//! # Single-symbol
//!
//! Each Strategy handles exactly one Symbol. Multi-symbol setups run N strategies
//! in parallel. Cross-symbol strategies (correlation, hedging) are out of scope
//! for v0.
//!
//! # Event vs tick
//!
//! [`Strategy::on_event`] is the primary entry; [`Strategy::on_tick`] is an
//! optional periodic pulse gated by [`Strategy::tick_interval`].

#![deny(missing_docs)]

pub mod avellaneda_stoikov;
pub mod flat_mm;
pub mod glft;
pub mod hawk;
pub mod hydra;
pub mod joker;
pub mod keel;
pub mod ladder_reentry;
pub mod layered_grid;
pub mod liq_fade;
pub mod mantis;
pub mod micro_mean_reversion;
pub mod micro_price;
pub mod ratchet;
pub mod risk;
pub mod rsi_mr;
pub mod simple_gap;
pub mod spread_scalp;
pub mod spread_scalp_old;
pub mod static_grid;
pub mod strangler;
pub mod tidal;
pub mod tide;
pub mod top_of_book;
pub mod volatility;
pub mod volley;
pub mod wave;

pub use avellaneda_stoikov::{AvellanedaStoikov, AvellanedaStoikovConfig};
pub use flat_mm::{FlatMm, FlatMmConfig};
pub use glft::{Glft, GlftConfig};
pub use hawk::{Hawk, HawkConfig};
pub use hydra::{Hydra, HydraConfig};
pub use joker::{Joker, JokerConfig};
pub use keel::{Keel, KeelConfig, KeelMode};
pub use ladder_reentry::{LadderReentry, LadderReentryConfig};
pub use layered_grid::{LayeredGrid, LayeredGridConfig};
pub use liq_fade::{LiqFade, LiqFadeConfig};
pub use mantis::{Mantis, MantisConfig};
pub use micro_mean_reversion::{MicroMeanReversion, MicroMeanReversionConfig};
pub use micro_price::{MicroPrice, MicroPriceConfig};
pub use ratchet::{Ratchet, RatchetConfig};
pub use rsi_mr::{RsiMr, RsiMrConfig};
pub use simple_gap::{SimpleGap, SimpleGapConfig};
pub use spread_scalp::{SpreadScalp, SpreadScalpConfig};
pub use spread_scalp_old::{SpreadScalpOld, SpreadScalpOldConfig};
pub use static_grid::{StaticGrid, StaticGridConfig};
pub use strangler::{Strangler, StranglerConfig};
pub use tidal::{Tidal, TidalConfig};
pub use tide::{Tide, TideConfig};
pub use top_of_book::{TopOfBook, TopOfBookConfig};
pub use volatility::{EwmaConfig, EwmaVolatility};
pub use volley::{Volley, VolleyConfig};
pub use wave::{Wave, WaveConfig};

use tikr_core::{
    Decimal, Fill, LiqEvent, MarketEvent, Position, Price, QuoteKind, Side, Size, Snapshot, Symbol,
    TimeInForce, Timestamp,
};
use tikr_venue::{QuoteId, QuoteIntent};

// ---------------------------------------------------------------------------
// StrategyContext
// ---------------------------------------------------------------------------

/// Read-only view passed to the strategy on each event or tick.
pub struct StrategyContext<'a> {
    /// Symbol this strategy is quoting.
    pub symbol: &'a Symbol,
    /// Current wall-clock time (nanoseconds since UNIX epoch).
    pub now: Timestamp,
    /// Current position in the symbol.
    pub position: &'a Position,
    /// Fills received since the last event or tick.
    pub recent_fills: &'a [Fill],
    /// Most recent full order-book snapshot.
    pub latest_book: &'a Snapshot,
    /// All open quotes: (id, original intent) pairs.
    pub open_quotes: &'a [(QuoteId, QuoteIntent)],
    /// Rolling window of recent forced-liquidation events. Runner-
    /// maintained — strategies that don't care about liqs leave it
    /// empty (default `&[]`). `LiqFade` consumes these to arm + time
    /// the cascade-fade trade. Sorted oldest-first; window length is
    /// a runner-side knob.
    pub recent_liqs: &'a [LiqEvent],
}

// ---------------------------------------------------------------------------
// Action
// ---------------------------------------------------------------------------

/// An action the strategy requests the executor to perform on the venue.
#[derive(Debug, Clone)]
pub enum Action {
    /// Submit a new quote to the venue.
    Quote(QuoteIntent),
    /// Replace an existing quote with a new intent.
    Requote {
        /// Id of the quote to replace.
        id: QuoteId,
        /// Replacement intent.
        intent: QuoteIntent,
    },
    /// Cancel a single open quote by id.
    Cancel(QuoteId),
    /// Cancel all outstanding quotes for this symbol.
    CancelAll,
    /// Explicit no-op: strategy is alive but requests nothing this cycle.
    ///
    /// Returning `NoOp` (rather than an empty `Vec`) is preferred for
    /// telemetry-explicit visibility — "I'm here, doing nothing."
    NoOp,
}

// ---------------------------------------------------------------------------
// Strategy trait
// ---------------------------------------------------------------------------

/// Core trait implemented by every market-making strategy.
///
/// Strategies are synchronous, single-symbol, and deterministic. Each instance
/// is driven by one executor thread and receives events via [`on_event`][Strategy::on_event]
/// and optional periodic ticks via [`on_tick`][Strategy::on_tick].
pub trait Strategy: Send {
    /// Strategy-specific configuration type.
    type Config: Send + Clone;

    /// Construct a new strategy instance from `config`.
    fn new(config: Self::Config) -> Self
    where
        Self: Sized;

    /// Human-readable name for this strategy, used in logs and metrics.
    fn name(&self) -> &str;

    /// Called for every [`MarketEvent`] delivered to this strategy.
    ///
    /// Returns the list of actions the executor should apply in order.
    /// An empty vec is valid and means "do nothing this event."
    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action>;

    /// Optional periodic pulse, called every [`tick_interval`][Strategy::tick_interval].
    ///
    /// Default: returns an empty vec (no-op pulse).
    fn on_tick(&mut self, _ctx: &StrategyContext<'_>) -> Vec<Action> {
        Vec::new()
    }

    /// How often [`on_tick`][Strategy::on_tick] should be called.
    ///
    /// `None` (default) disables periodic ticks entirely.
    fn tick_interval(&self) -> Option<std::time::Duration> {
        None
    }

    /// Called once when the executor is shutting down.
    ///
    /// Default: NOTHING. Killing/turning off a bot must leave its resting orders
    /// AND open position intact on the venue — they persist across restarts and
    /// are resumed (or re-adopted by the manager) on the next start. Closing is
    /// exclusively the manager's job during rotation (`reset_symbol_state` /
    /// `flatten_symbols`), never a side effect of a bot stopping.
    fn on_shutdown(&mut self, _ctx: &StrategyContext<'_>) -> Vec<Action> {
        Vec::new()
    }

    /// Called by the runner when a `Quote` action it just dispatched was
    /// rejected by the venue (post-only would cross, min-notional fail,
    /// risk limit, etc). Lets the strategy recover — typically by
    /// re-anchoring on current book mid and emitting fresh actions.
    ///
    /// Default: no-op. Strategies that want fallback behavior should
    /// override.
    fn on_quote_rejected(
        &mut self,
        _ctx: &StrategyContext<'_>,
        _intent: &tikr_venue::QuoteIntent,
        _reason: &str,
    ) -> Vec<Action> {
        Vec::new()
    }

    /// Called when the runner receives a new account-derived order notional.
    /// Strategies with fiat-sized orders should update future quote sizes and
    /// may return actions to refresh currently resting orders.
    fn on_notional_updated(
        &mut self,
        _ctx: &StrategyContext<'_>,
        _notional_per_order: Decimal,
    ) -> Vec<Action> {
        Vec::new()
    }

    /// Called when the runner receives a new account-derived per-bot position
    /// cap. Strategies that gate adds against `max_position_usdt` should
    /// update their config. Default impl is a no-op for strategies without
    /// a cap concept. Fires alongside [`Self::on_notional_updated`] so order
    /// size and cap track each other in lockstep as the wallet grows.
    fn on_max_position_updated(
        &mut self,
        _ctx: &StrategyContext<'_>,
        _max_position_usdt: Decimal,
    ) -> Vec<Action> {
        Vec::new()
    }

    /// Live introspection for the TUI/dashboard: short `(label, value)` pairs
    /// describing the strategy's current internal state (e.g. Wave's effective
    /// step/inner, showing the static config value or the live auto-sized one).
    /// Default empty — strategies opt in. Read by the runner on each
    /// `LiveSnapshot` publish; kept tiny (rendered in the bot-detail panel).
    fn status_metrics(&self) -> Vec<(&'static str, String)> {
        Vec::new()
    }
}

// ---------------------------------------------------------------------------
// StrategyResume
// ---------------------------------------------------------------------------

/// Optional opt-in for strategies that want to persist internal state across
/// restarts (resume path; #32).
///
/// Default impls are no-ops; strategies that don't override skip serialization
/// (cold-start on resume — acceptable for warmup-recoverable strategies like
/// [`AvellanedaStoikov`] / [`Glft`]). No reference strategy implements this
/// trait in v0; the declaration is wired in for future opt-in.
pub trait StrategyResume {
    /// Serialize the strategy's internal state to bytes. `None` (default) =
    /// don't persist.
    fn serialize_state(&self) -> Option<Vec<u8>> {
        None
    }
    /// Restore internal state from previously-serialized bytes. Default: no-op.
    fn restore_state(&mut self, _bytes: &[u8]) {}
}

/// Compute the mid-price strictly from both sides of the book (no trade fallback).
/// Used by inventory-aware strategies ([`AvellanedaStoikov`], [`Glft`]) that
/// require a real spread to be present before quoting.
/// Returns `None` if either side of the book is empty.
pub(crate) fn compute_mid_strict(snapshot: &Snapshot) -> Option<Price> {
    let best_bid = snapshot.bids.first()?.price;
    let best_ask = snapshot.asks.first()?.price;
    Some(Price((best_bid.0 + best_ask.0) / Decimal::from(2)))
}

/// Decide whether to re-emit quotes based on elapsed time and mid-price drift.
///
/// Returns `true` immediately when no prior requote has occurred. Otherwise,
/// returns `true` when either:
/// - `now - last_ts >= min_interval_ms` (time gate), or
/// - `|new_mid - prev_mid| / prev_mid > (level_step_bps / 2) / 10_000` (drift gate).
pub(crate) fn should_requote_drift(
    last_ts: Option<Timestamp>,
    last_mid: Option<Price>,
    new_mid: Price,
    now: Timestamp,
    min_interval_ms: u64,
    level_step_bps: u32,
) -> bool {
    let (Some(prev_ts), Some(prev_mid)) = (last_ts, last_mid) else {
        return true;
    };
    let elapsed_ns = now.0.saturating_sub(prev_ts.0);
    let interval_ns = min_interval_ms.saturating_mul(1_000_000);
    if elapsed_ns >= interval_ns {
        return true;
    }
    let drift = (new_mid.0 - prev_mid.0).abs();
    let threshold =
        prev_mid.0 * (Decimal::from(level_step_bps) / Decimal::from(2)) / Decimal::from(10_000);
    drift > threshold
}

/// Build a post-only point-quote intent for the given symbol, side, price, and size.
/// Used by inventory-aware strategies ([`AvellanedaStoikov`], [`Glft`]).
pub(crate) fn make_post_only_intent(
    symbol: &Symbol,
    side: Side,
    price: Price,
    size: Size,
) -> QuoteIntent {
    QuoteIntent {
        symbol: symbol.clone(),
        side,
        price,
        size,
        tif: TimeInForce::PostOnly,
        kind: QuoteKind::Point,
    }
}

/// Emit `[CancelAll, Quote(Bid), Quote(Ask)]` as post-only point quotes.
/// Used by [`TopOfBook`] and [`MicroPrice`] — the only difference between
/// their `build_quotes` bodies was the `bid`/`ask` prices, which are
/// parameters here.
pub(crate) fn post_only_pair(symbol: &Symbol, bid: Price, ask: Price, size: Size) -> Vec<Action> {
    let mut actions = Vec::with_capacity(3);
    actions.push(Action::CancelAll);
    for (side, price) in [(Side::Bid, bid), (Side::Ask, ask)] {
        actions.push(Action::Quote(QuoteIntent {
            symbol: symbol.clone(),
            side,
            price,
            size,
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        }));
    }
    actions
}

/// Decide whether to re-emit quotes based on elapsed time or price drift ≥
/// `tick_size` on either side.
///
/// Returns `true` on cold start (any `None` input). Otherwise returns `true`
/// when either:
/// - `now - last_ts >= interval_ms` (time gate), or
/// - `|new_bid - last_bid| >= tick_size` or `|new_ask - last_ask| >= tick_size`
///   (drift gate).
///
/// Used by [`TopOfBook`] and [`MicroPrice`].
#[allow(clippy::too_many_arguments)]
pub(crate) fn should_requote_on_tick_drift(
    last_bid: Option<Price>,
    last_ask: Option<Price>,
    last_ts: Option<Timestamp>,
    new_bid: Price,
    new_ask: Price,
    now: Timestamp,
    interval_ms: u64,
    tick_size: Decimal,
) -> bool {
    let (Some(last_bid), Some(last_ask)) = (last_bid, last_ask) else {
        return true;
    };
    // Time-forced requote.
    if let Some(ts) = last_ts {
        let elapsed_ns = now.0.saturating_sub(ts.0);
        let interval_ns = interval_ms.saturating_mul(1_000_000);
        if elapsed_ns >= interval_ns {
            return true;
        }
    }
    // Drift gate: ≥ 1 tick movement on either side.
    let bid_drift = (new_bid.0 - last_bid.0).abs();
    let ask_drift = (new_ask.0 - last_ask.0).abs();
    bid_drift >= tick_size || ask_drift >= tick_size
}

/// Inventory-skew price shift (signed, in price units).
///
/// Returns `−sign(pos) × floor(|pos| / skew_unit × max_skew_ticks) × tick_size`.
///
/// Long position → negative shift (quotes drift down); short → positive
/// (quotes drift up). Returns zero when position is flat, `max_skew_ticks`
/// is 0, or `skew_unit` is non-positive.
///
/// Used by [`TopOfBook`] and [`MicroPrice`].
pub(crate) fn inventory_skew_price(
    pos: Decimal,
    max_skew_ticks: u32,
    skew_unit: Decimal,
    tick_size: Decimal,
) -> Decimal {
    if max_skew_ticks == 0 || pos == Decimal::ZERO {
        return Decimal::ZERO;
    }
    if skew_unit <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let max = Decimal::from(max_skew_ticks);
    let ratio = (pos.abs() / skew_unit).min(Decimal::from(1));
    let ticks_shifted = (ratio * max).floor();
    let magnitude = ticks_shifted * tick_size;
    if pos > Decimal::ZERO {
        -magnitude
    } else {
        magnitude
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::{Asset, Decimal, MarketKind, VenueId};
    use tikr_core::{Price, QuoteKind, Side, TimeInForce};
    use tikr_venue::QuoteIntent;

    fn make_symbol() -> Symbol {
        Symbol {
            base: Asset::new("BTC"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("test"),
            kind: MarketKind::Spot,
        }
    }

    fn make_intent(sym: &Symbol) -> QuoteIntent {
        QuoteIntent {
            symbol: sym.clone(),
            side: Side::Bid,
            price: Price(Decimal::new(60_000, 0)),
            size: tikr_core::Size(Decimal::new(1, 1)),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        }
    }

    #[test]
    fn action_variants_clone_debug() {
        let sym = make_symbol();
        let intent = make_intent(&sym);
        let id = QuoteId::new();

        let variants: Vec<Action> = vec![
            Action::Quote(intent.clone()),
            Action::Requote {
                id,
                intent: intent.clone(),
            },
            Action::Cancel(id),
            Action::CancelAll,
            Action::NoOp,
        ];

        for a in &variants {
            let cloned = a.clone();
            let dbg = format!("{cloned:?}");
            assert!(!dbg.is_empty());
        }
    }
}
