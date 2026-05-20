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
pub mod glft;
pub mod layered_grid;
pub mod micro_price;
pub mod top_of_book;
pub mod volatility;

pub use avellaneda_stoikov::{AvellanedaStoikov, AvellanedaStoikovConfig};
pub use glft::{Glft, GlftConfig};
pub use layered_grid::{LayeredGrid, LayeredGridConfig};
pub use micro_price::{MicroPrice, MicroPriceConfig};
pub use top_of_book::{TopOfBook, TopOfBookConfig};
pub use volatility::{EwmaConfig, EwmaVolatility};

use tikr_core::{
    Decimal, Fill, MarketEvent, Position, Price, QuoteKind, Side, Size, Snapshot, Symbol,
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
    /// Default: cancels all open quotes to avoid orphaned resting orders.
    fn on_shutdown(&mut self, _ctx: &StrategyContext<'_>) -> Vec<Action> {
        vec![Action::CancelAll]
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
}

// ---------------------------------------------------------------------------
// StrategyResume
// ---------------------------------------------------------------------------

/// Optional opt-in for strategies that want to persist internal state across
/// restarts (resume path; #32).
///
/// Default impls are no-ops; strategies that don't override skip serialization
/// (cold-start on resume — acceptable for stateless strategies like
/// [`NaiveGrid`] or warmup-recoverable strategies like [`AvellanedaStoikov`] /
/// [`Glft`]). No reference strategy implements this trait in v0; the
/// declaration is wired in for future opt-in.
pub trait StrategyResume {
    /// Serialize the strategy's internal state to bytes. `None` (default) =
    /// don't persist.
    fn serialize_state(&self) -> Option<Vec<u8>> {
        None
    }
    /// Restore internal state from previously-serialized bytes. Default: no-op.
    fn restore_state(&mut self, _bytes: &[u8]) {}
}

// ---------------------------------------------------------------------------
// NaiveGrid
// ---------------------------------------------------------------------------

/// Configuration for the [`NaiveGrid`] reference strategy.
#[derive(Debug, Clone)]
pub struct NaiveGridConfig {
    /// Number of price levels to quote on each side of the book.
    pub levels_per_side: u8,
    /// Half-spread at the innermost level, in basis points.
    pub base_spread_bps: u32,
    /// Additional spread increment per level outward, in basis points.
    pub level_step_bps: u32,
    /// Size placed at each individual quote level.
    pub size_per_quote: Size,
    /// Minimum time between full requotes, in milliseconds.
    pub min_requote_interval_ms: u64,
}

/// Symmetric grid market-making strategy.
///
/// Places `levels_per_side` bid and ask levels symmetrically around a
/// mid-price derived from the latest book snapshot.
pub struct NaiveGrid {
    /// Strategy configuration.
    config: NaiveGridConfig,
    /// Timestamp of the most recent full requote cycle, if any.
    last_requote_ts: Option<Timestamp>,
    /// Mid-price at the time of the most recent requote.
    last_quoted_mid: Option<Price>,
    /// Most recent trade price, used as a mid-price fallback when one side
    /// of the book is empty.
    last_trade_price: Option<Price>,
}

impl NaiveGrid {
    /// Timestamp of the most recent full requote, or `None` if no requote has
    /// occurred yet. Used to enforce `min_requote_interval_ms`.
    pub fn last_requote_ts(&self) -> Option<Timestamp> {
        self.last_requote_ts
    }

    /// Build the 2N quote actions for the given mid, then cancel any
    /// previously-known open quotes.
    ///
    /// Order matters: place NEW orders first so we're never naked on the
    /// book between cancel and re-place. Use specific `Cancel(id)` for each
    /// known prior quote — `CancelAll` would also kill the just-placed new
    /// ones since the venue sees them all under the same symbol.
    fn build_quotes(
        &self,
        symbol: &Symbol,
        mid: Price,
        open_quotes: &[(QuoteId, QuoteIntent)],
    ) -> Vec<Action> {
        let n_quotes = 2 * self.config.levels_per_side as usize;
        let mut actions = Vec::with_capacity(n_quotes + open_quotes.len());
        let bps_unit = Decimal::from(10_000);
        for k in 0..self.config.levels_per_side {
            let offset_bps = Decimal::from(self.config.base_spread_bps)
                + Decimal::from(k as u32) * Decimal::from(self.config.level_step_bps);
            let offset = offset_bps / bps_unit;
            let bid_price = Price(mid.0 * (Decimal::from(1) - offset));
            let ask_price = Price(mid.0 * (Decimal::from(1) + offset));
            for (side, price) in [(Side::Bid, bid_price), (Side::Ask, ask_price)] {
                actions.push(Action::Quote(QuoteIntent {
                    symbol: symbol.clone(),
                    side,
                    price,
                    size: self.config.size_per_quote,
                    tif: TimeInForce::PostOnly,
                    kind: QuoteKind::Point,
                }));
            }
        }
        for (id, _) in open_quotes {
            actions.push(Action::Cancel(*id));
        }
        actions
    }
}

/// Compute the mid-price for a snapshot, falling back to `last_trade_price`
/// when one side of the book is empty. Returns `None` if both sides are
/// empty and no trade fallback is available.
fn compute_mid(snapshot: &Snapshot, last_trade_price: Option<Price>) -> Option<Price> {
    let best_bid = snapshot.bids.first().map(|l| l.price);
    let best_ask = snapshot.asks.first().map(|l| l.price);
    match (best_bid, best_ask) {
        (Some(b), Some(a)) => Some(Price((b.0 + a.0) / Decimal::from(2))),
        _ => last_trade_price,
    }
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

impl Strategy for NaiveGrid {
    type Config = NaiveGridConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            last_requote_ts: None,
            last_quoted_mid: None,
            last_trade_price: None,
        }
    }

    fn name(&self) -> &str {
        "naive-grid"
    }

    fn on_quote_rejected(
        &mut self,
        ctx: &StrategyContext<'_>,
        _intent: &tikr_venue::QuoteIntent,
        _reason: &str,
    ) -> Vec<Action> {
        // A post-only / cross / min-notional rejection means our anchor is
        // stale (market moved past our intended price). Re-anchor on the
        // current book mid and emit a fresh pair — this also cancels any
        // remaining lopsided open order on the other side.
        let Some(mid) = compute_mid(ctx.latest_book, self.last_trade_price) else {
            return Vec::new();
        };
        self.last_quoted_mid = Some(mid);
        self.last_requote_ts = Some(ctx.now);
        self.build_quotes(ctx.symbol, mid, ctx.open_quotes)
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        // Static-grid semantics: place a pair on cold start anchored at the
        // first observed mid, then sit. Only re-quote when a Fill occurs —
        // book drift and heartbeats no longer recenter.
        match event {
            MarketEvent::Trade { price, .. } => {
                self.last_trade_price = Some(*price);
                Vec::new()
            }
            MarketEvent::Fill(fill) => {
                // Belt-and-suspenders: the runner already gates `is_full`,
                // but ignore partials here too so a partial-fill leak
                // wouldn't cancel the still-resting remainder.
                if !fill.is_full {
                    return Vec::new();
                }
                // Anchor the next pair on the fill price directly. The fill
                // price is by definition within (or at) the current touch,
                // so new orders at `fill ± spread_bps` land maker-safe even
                // during fast-moving trends. (The previous midpoint-of-prev-
                // and-fill formula lagged the market in trending periods,
                // causing post-only rejections on the catch-up side.)
                let new_mid = fill.price;
                self.last_quoted_mid = Some(new_mid);
                self.last_requote_ts = Some(fill.ts);
                self.build_quotes(ctx.symbol, new_mid, ctx.open_quotes)
            }
            MarketEvent::Heartbeat { ts } => {
                if self.last_quoted_mid.is_some() {
                    return vec![Action::NoOp];
                }
                let Some(mid) = compute_mid(ctx.latest_book, self.last_trade_price) else {
                    return Vec::new();
                };
                self.last_requote_ts = Some(*ts);
                self.last_quoted_mid = Some(mid);
                self.build_quotes(ctx.symbol, mid, ctx.open_quotes)
            }
            MarketEvent::BookUpdate { snapshot } => {
                if self.last_quoted_mid.is_some() {
                    return vec![Action::NoOp];
                }
                let Some(mid) = compute_mid(snapshot, self.last_trade_price) else {
                    return Vec::new();
                };
                self.last_requote_ts = Some(snapshot.ts);
                self.last_quoted_mid = Some(mid);
                self.build_quotes(ctx.symbol, mid, ctx.open_quotes)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::{Asset, Decimal, Level, MarketKind, SignedSize, VenueId};
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

    fn make_ctx<'a>(sym: &'a Symbol, pos: &'a Position, book: &'a Snapshot) -> StrategyContext<'a> {
        StrategyContext {
            symbol: sym,
            now: Timestamp(0),
            position: pos,
            recent_fills: &[],
            latest_book: book,
            open_quotes: &[],
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

    fn make_cfg() -> NaiveGridConfig {
        NaiveGridConfig {
            levels_per_side: 3,
            base_spread_bps: 10,
            level_step_bps: 5,
            size_per_quote: Size(Decimal::new(1, 1)),
            min_requote_interval_ms: 500,
        }
    }

    #[test]
    fn naive_grid_constructs() {
        let cfg = make_cfg();
        let grid = NaiveGrid::new(cfg);
        assert_eq!(grid.name(), "naive-grid");
        assert!(grid.last_requote_ts().is_none());
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

    #[test]
    fn strategy_is_object_safe() {
        let cfg = make_cfg();
        let _s: Box<dyn Strategy<Config = NaiveGridConfig>> = Box::new(NaiveGrid::new(cfg));
    }

    #[test]
    fn on_shutdown_default_cancels_all() {
        let sym = make_symbol();
        let pos = Position {
            symbol: sym.clone(),
            size: SignedSize(Decimal::ZERO),
            avg_entry: Price(Decimal::ZERO),
            realized_pnl: tikr_core::Notional(Decimal::ZERO),
        };
        let book = Snapshot {
            symbol: sym.clone(),
            bids: vec![],
            asks: vec![],
            ts: Timestamp(0),
        };
        let ctx = make_ctx(&sym, &pos, &book);
        let mut grid = NaiveGrid::new(make_cfg());
        let actions = grid.on_shutdown(&ctx);
        assert!(matches!(actions[..], [Action::CancelAll]));
    }

    #[test]
    fn tick_interval_default_is_none() {
        let grid = NaiveGrid::new(make_cfg());
        assert!(grid.tick_interval().is_none());
    }

    #[test]
    fn on_tick_default_empty() {
        let sym = make_symbol();
        let pos = Position {
            symbol: sym.clone(),
            size: SignedSize(Decimal::ZERO),
            avg_entry: Price(Decimal::ZERO),
            realized_pnl: tikr_core::Notional(Decimal::ZERO),
        };
        let book = Snapshot {
            symbol: sym.clone(),
            bids: vec![],
            asks: vec![],
            ts: Timestamp(0),
        };
        let ctx = make_ctx(&sym, &pos, &book);
        let mut grid = NaiveGrid::new(make_cfg());
        let actions = grid.on_tick(&ctx);
        assert!(actions.is_empty());
    }

    // -------------------------------------------------------------------
    // Phase 1 grid-logic tests
    // -------------------------------------------------------------------

    fn make_pos(sym: &Symbol) -> Position {
        Position {
            symbol: sym.clone(),
            size: SignedSize(Decimal::ZERO),
            avg_entry: Price(Decimal::ZERO),
            realized_pnl: tikr_core::Notional(Decimal::ZERO),
        }
    }

    fn make_book(sym: &Symbol, bids: &[(i64, i64)], asks: &[(i64, i64)], ts: u64) -> Snapshot {
        let to_levels = |xs: &[(i64, i64)]| {
            xs.iter()
                .map(|(p, s)| Level {
                    price: Price(Decimal::new(*p, 0)),
                    size: Size(Decimal::new(*s, 0)),
                })
                .collect()
        };
        Snapshot {
            symbol: sym.clone(),
            bids: to_levels(bids),
            asks: to_levels(asks),
            ts: Timestamp(ts),
        }
    }

    fn make_phase1_cfg() -> NaiveGridConfig {
        NaiveGridConfig {
            levels_per_side: 2,
            base_spread_bps: 10,
            level_step_bps: 5,
            size_per_quote: Size(Decimal::new(1, 0)),
            min_requote_interval_ms: 1000,
        }
    }

    #[test]
    fn first_book_update_quotes_immediately() {
        let sym = make_symbol();
        let pos = make_pos(&sym);
        let book = make_book(&sym, &[(100, 1)], &[(102, 1)], 0);
        let ctx = make_ctx(&sym, &pos, &book);
        let mut grid = NaiveGrid::new(make_phase1_cfg());

        let event = MarketEvent::BookUpdate {
            snapshot: make_book(&sym, &[(100, 1)], &[(102, 1)], 0),
        };
        let actions = grid.on_event(&ctx, &event);

        // Cold start: no prior open quotes → 4 Quote actions (2 levels × 2 sides),
        // no Cancels.
        assert_eq!(actions.len(), 4);
        for a in &actions {
            assert!(matches!(a, Action::Quote(_)));
        }
    }

    #[test]
    fn no_requote_after_interval_when_static() {
        let sym = make_symbol();
        let pos = make_pos(&sym);
        let book = make_book(&sym, &[(100, 1)], &[(102, 1)], 1000);
        let ctx = make_ctx(&sym, &pos, &book);
        let mut grid = NaiveGrid::new(make_phase1_cfg());

        // First update consumes the cold-start requote.
        let event1 = MarketEvent::BookUpdate {
            snapshot: make_book(&sym, &[(100, 1)], &[(102, 1)], 1000),
        };
        let actions1 = grid.on_event(&ctx, &event1);
        assert_eq!(actions1.len(), 4);

        // Static-grid: second update one full interval later does NOT requote.
        let event2 = MarketEvent::BookUpdate {
            snapshot: make_book(&sym, &[(100, 1)], &[(102, 1)], 1000 + 1_000_000_000 + 1),
        };
        let actions2 = grid.on_event(&ctx, &event2);
        assert_eq!(actions2.len(), 1);
        assert!(matches!(actions2[0], Action::NoOp));
    }

    #[test]
    fn no_requote_on_mid_drift_when_static() {
        let sym = make_symbol();
        let pos = make_pos(&sym);
        let book = make_book(&sym, &[(100, 1)], &[(100, 1)], 0);
        let ctx = make_ctx(&sym, &pos, &book);
        let mut grid = NaiveGrid::new(make_phase1_cfg());

        // First update at ts=0: mid = 100.
        let event1 = MarketEvent::BookUpdate {
            snapshot: make_book(&sym, &[(100, 1)], &[(100, 1)], 0),
        };
        let actions1 = grid.on_event(&ctx, &event1);
        assert_eq!(actions1.len(), 4);

        // Static-grid: 1% mid jump does NOT trigger requote — orders stay put.
        let event2 = MarketEvent::BookUpdate {
            snapshot: make_book(&sym, &[(101, 1)], &[(101, 1)], 1),
        };
        let actions2 = grid.on_event(&ctx, &event2);
        assert_eq!(actions2.len(), 1);
        assert!(matches!(actions2[0], Action::NoOp));
    }

    #[test]
    fn heartbeat_emits_noop_when_no_requote() {
        let sym = make_symbol();
        let pos = make_pos(&sym);
        let book = make_book(&sym, &[(100, 1)], &[(102, 1)], 1000);
        let ctx = make_ctx(&sym, &pos, &book);
        let mut grid = NaiveGrid::new(make_phase1_cfg());

        // Prime state with a book update at ts=1000.
        let event1 = MarketEvent::BookUpdate {
            snapshot: make_book(&sym, &[(100, 1)], &[(102, 1)], 1000),
        };
        let _ = grid.on_event(&ctx, &event1);

        // Heartbeat 1ns later (within interval, no drift since ctx.latest_book is
        // unchanged).
        let event2 = MarketEvent::Heartbeat {
            ts: Timestamp(1001),
        };
        let actions = grid.on_event(&ctx, &event2);
        assert!(matches!(actions[..], [Action::NoOp]));
    }

    #[test]
    fn empty_book_no_fallback_returns_empty() {
        let sym = make_symbol();
        let pos = make_pos(&sym);
        let book = make_book(&sym, &[], &[], 0);
        let ctx = make_ctx(&sym, &pos, &book);
        let mut grid = NaiveGrid::new(make_phase1_cfg());

        let event = MarketEvent::BookUpdate {
            snapshot: make_book(&sym, &[], &[], 0),
        };
        let actions = grid.on_event(&ctx, &event);
        assert!(actions.is_empty());
    }
}
