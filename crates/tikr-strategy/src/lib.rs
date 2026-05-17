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

use tikr_core::{Fill, MarketEvent, Position, Size, Snapshot, Symbol, Timestamp};
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

/// Symmetric grid market-making strategy (Phase 1 stub).
///
/// Places `levels_per_side` bid and ask levels around a mid-price derived
/// from the latest book snapshot. Full logic lands in Phase 1.
pub struct NaiveGrid {
    /// Strategy configuration.
    #[allow(dead_code)]
    config: NaiveGridConfig,
    /// Timestamp of the most recent full requote cycle, if any.
    last_requote_ts: Option<Timestamp>,
}

impl NaiveGrid {
    /// Timestamp of the most recent full requote, or `None` if no requote has
    /// occurred yet. Used by Phase 1 logic to enforce `min_requote_interval_ms`.
    pub fn last_requote_ts(&self) -> Option<Timestamp> {
        self.last_requote_ts
    }
}

impl Strategy for NaiveGrid {
    type Config = NaiveGridConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            last_requote_ts: None,
        }
    }

    fn name(&self) -> &str {
        "naive-grid"
    }

    fn on_event(&mut self, _ctx: &StrategyContext<'_>, _event: &MarketEvent) -> Vec<Action> {
        todo!("naive grid logic — Phase 1")
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::{Asset, Decimal, SignedSize, VenueId};
    use tikr_core::{Price, QuoteKind, Side, TimeInForce};
    use tikr_venue::QuoteIntent;

    fn make_symbol() -> Symbol {
        Symbol {
            base: Asset::new("BTC"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("test"),
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
}
