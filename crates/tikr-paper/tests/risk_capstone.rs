//! Phase 4 capstone: end-to-end integration test exercising risk + resume +
//! alerting all wired together.
//!
//! Single 4-phase test sharing state via local variables:
//!   Phase 1 — trip drawdown halt
//!   Phase 2 — verify halt alert + risk_state serialized correctly
//!   Phase 3 — resume keeps halt (0 new fills)
//!   Phase 4 — clear_halt enables recovery (fills resume, no new Halt alert)
//!
//! Helpers (`MockVenue`, `RecordingSink`, event builders) live in this file per
//! the Phase 1 expediency precedent (duplicate when sharing across tests not yet
//! justified).
//!
//! ## Deviation from locked spec
//!
//! The spec asserts `report_phase1.net.0 <= Decimal::from(-50)`.  The
//! `PositionTracker::report` computes unrealized P&L as `(last_mid - avg_entry)
//! × position_size`.  After buying 1 BTC at 99.5 and seeing mid fall to 49.5,
//! unrealized = (49.5 - 99.5) × 1 = -50 → net = -50.  The `<=` condition is
//! satisfied exactly.  We use `Decimal::from(-50)` consistently — no threshold
//! adjustment needed.
//!
//! The halt fires when the NEXT QuoteAction is checked post-fill.  The
//! sequence is: BookUpdate@50 → strategy emits Quote → risk gate sees net=-50
//! → `Halt`.

use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use tikr_backtest::fill_sim::{FillSim, FillSimConfig, VenueFees};
use tikr_core::{
    Asset, Decimal, Level, MarketEvent, MarketKind, Notional, Price, QuoteKind, Side, Size,
    Snapshot, Symbol, TimeInForce, Timestamp, VenueId,
};
use tikr_paper::alerts::{Alert, AlertError, AlertSink};
use tikr_paper::{RunnerConfig, run_with_resume};
use tikr_risk::{BasicRiskGate, RiskGate, RiskLimits, RiskState};
use tikr_strategy::{Action, Strategy, StrategyContext};
use tikr_venue::{QuoteId, QuoteIntent, Venue, VenueError};
use tokio::sync::watch;

// ---------------------------------------------------------------------------
// RecordingSink — captures all alerts for post-hoc assertion
// ---------------------------------------------------------------------------

struct RecordingSink {
    alerts: Arc<Mutex<Vec<Alert>>>,
}

impl RecordingSink {
    fn new(alerts: Arc<Mutex<Vec<Alert>>>) -> Self {
        Self { alerts }
    }
}

#[async_trait]
impl AlertSink for RecordingSink {
    async fn send(&self, alert: Alert) -> Result<(), AlertError> {
        self.alerts.lock().unwrap().push(alert);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// MockVenue — finite event stream; shape copied from runner.rs unit tests
// (Phase 1 expediency: duplicate 30 lines vs cross-crate refactor).
// ---------------------------------------------------------------------------

struct MockVenue {
    events: Mutex<Option<Vec<MarketEvent>>>,
}

impl MockVenue {
    fn finite(events: Vec<MarketEvent>) -> Self {
        Self {
            events: Mutex::new(Some(events)),
        }
    }
}

#[async_trait]
impl Venue for MockVenue {
    fn id(&self) -> &str {
        "mock"
    }
    async fn snapshot(&self, _symbol: &Symbol) -> Result<Snapshot, VenueError> {
        unimplemented!()
    }
    async fn subscribe(&self, _symbol: &Symbol) -> Result<BoxStream<'_, MarketEvent>, VenueError> {
        let events = self.events.lock().unwrap().take().unwrap_or_default();
        Ok(Box::pin(stream::iter(events)))
    }
    async fn quote(&self, _intent: QuoteIntent) -> Result<QuoteId, VenueError> {
        unimplemented!()
    }
    async fn requote(&self, _id: QuoteId, _intent: QuoteIntent) -> Result<(), VenueError> {
        unimplemented!()
    }
    async fn cancel(&self, _id: QuoteId) -> Result<(), VenueError> {
        unimplemented!()
    }
    async fn cancel_all(&self, _symbol: &Symbol) -> Result<(), VenueError> {
        unimplemented!()
    }
    async fn position(&self, _symbol: &Symbol) -> Result<tikr_core::Position, VenueError> {
        unimplemented!()
    }
}

// ---------------------------------------------------------------------------
// Symbol + strategy helpers
// ---------------------------------------------------------------------------

fn make_symbol() -> Symbol {
    Symbol {
        base: Asset::new("BTC"),
        quote: Asset::new("USDC"),
        venue: VenueId::new("mock"),
        kind: MarketKind::Perp,
    }
}

/// Minimal test strategy that re-quotes on EVERY BookUpdate so the risk gate
/// is checked on every price move. Mirrors the old NaiveGrid aggressive config
/// used before NaiveGrid removal. Only used in risk_capstone tests.
struct AggressiveTestStrategy {
    spread_bps: u32,
    size: Size,
}

impl AggressiveTestStrategy {
    fn new(spread_bps: u32, size: Size) -> Self {
        Self { spread_bps, size }
    }
}

impl Strategy for AggressiveTestStrategy {
    type Config = ();
    fn new(_config: Self::Config) -> Self {
        unreachable!()
    }
    fn name(&self) -> &str {
        "aggressive-test"
    }
    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        match event {
            MarketEvent::BookUpdate { snapshot } => {
                let best_bid = snapshot.bids.first().map(|l| l.price.0);
                let best_ask = snapshot.asks.first().map(|l| l.price.0);
                let (Some(b), Some(a)) = (best_bid, best_ask) else {
                    return Vec::new();
                };
                let mid = (b + a) / Decimal::from(2);
                let offset = Decimal::from(self.spread_bps) / Decimal::from(10_000);
                let bid = Price(mid * (Decimal::from(1) - offset));
                let ask = Price(mid * (Decimal::from(1) + offset));
                let mut actions = Vec::new();
                for (id, _) in ctx.open_quotes {
                    actions.push(Action::Cancel(*id));
                }
                for (side, price) in [(Side::Bid, bid), (Side::Ask, ask)] {
                    actions.push(Action::Quote(QuoteIntent {
                        symbol: ctx.symbol.clone(),
                        side,
                        price,
                        size: self.size,
                        tif: TimeInForce::PostOnly,
                        kind: QuoteKind::Point,
                    }));
                }
                actions
            }
            MarketEvent::Fill(_) | MarketEvent::Trade { .. } | MarketEvent::Heartbeat { .. } => {
                Vec::new()
            }
        }
    }
}

fn layered_grid_aggressive() -> AggressiveTestStrategy {
    AggressiveTestStrategy::new(50, Size(Decimal::from(1)))
}

/// Zero-latency fill sim so quotes land instantly and trades fill immediately.
fn fill_sim_with_zero_latency() -> FillSim {
    FillSim::new(FillSimConfig {
        submit_latency_ms: 0,
        cancel_latency_ms: 0,
        fees: VenueFees {
            maker_bps: 0,
            taker_bps: 0,
        },
        max_position_notional_usdt: None,
        leverage: rust_decimal::Decimal::ZERO,
        max_position_frac: rust_decimal::Decimal::ZERO,
        silent_cancel_rate_per_min: 0.0,
        rng_seed: 0,
        latency_jitter_ms: 0,
        max_open_orders: None,
        queue_cancel_decay_per_sec: 0.0,
        spot: false,
    })
}

// ---------------------------------------------------------------------------
// Synthetic event stream — Phase 1: force a losing long position
//
// Strategy (AggressiveTestStrategy — re-quotes on every BookUpdate):
//   1. Feed a full book at 100/101 → strategy quotes bid@99.5 and ask@100.5
//      (0 latency → lands immediately on next event)
//   2. Trade event (ask-side taker at 99) → fills our bid → long 1 BTC @ 99.5
//   3. BookUpdate at 49/50 → last_mid drops to 49.5 → unrealized = -50, net = -50
//   4. Strategy re-quotes at the new mid → risk gate checks pnl.net = -50 ≤ -50 → HALT
//
// The sequence yields ~200 events as required by the spec.
// ---------------------------------------------------------------------------

fn make_book_event(symbol: &Symbol, bid: i64, ask: i64, ts_ns: u64) -> MarketEvent {
    MarketEvent::BookUpdate {
        snapshot: Snapshot {
            symbol: symbol.clone(),
            bids: vec![Level {
                price: Price(Decimal::from(bid)),
                size: Size(Decimal::from(1)),
            }],
            asks: vec![Level {
                price: Price(Decimal::from(ask)),
                size: Size(Decimal::from(1)),
            }],
            ts: Timestamp(ts_ns),
        },
    }
}

fn make_trade_event(
    symbol: &Symbol,
    price: i64,
    taker_side: tikr_core::Side,
    ts_ns: u64,
) -> MarketEvent {
    MarketEvent::Trade {
        symbol: symbol.clone(),
        price: Price(Decimal::from(price)),
        size: Size(Decimal::from(1)),
        side: taker_side,
        ts: Timestamp(ts_ns),
    }
}

/// Builds ~200 events that drive `AggressiveTestStrategy` to accumulate a long
/// BTC position then mark it to a loss ≥ $50 so the drawdown gate fires.
///
/// Sequence:
///   Events 0..1  — book at 100/101 (strategy quotes bid@99.5, ask@100.5)
///   Event 2      — ask-side trade at 99 → fills our bid → long 1 BTC @ 99.5
///   Events 3..N  — book at 49/50 (mid=49.5) → net=-50 → risk gate halts on
///                  the first re-quote attempt
fn events_that_force_losing_position(symbol: &Symbol, count: usize) -> Vec<MarketEvent> {
    assert!(
        count >= 4,
        "need at least 4 events for the losing-position sequence"
    );
    let mut events = Vec::with_capacity(count);

    // Seed the book at a high price so the strategy issues an initial quote.
    // Two BookUpdates: the first builds the mid; the quote intent is submitted
    // at ts=0 with 0 submit_latency so it lands before the next event.
    events.push(make_book_event(symbol, 100, 101, 0));
    events.push(make_book_event(symbol, 100, 101, 1_000_000)); // 1ms later

    // Ask-side taker trade at 99 → fills our resting bid → long 1 BTC.
    // taker_side = Ask means someone SOLD into the book (the taker is on the Ask).
    // FillSim: our Bid is matched when taker_side == Ask and trade_price <= quote_price.
    // AggressiveTestStrategy at mid=100.5, spread_bps=50 → bid = 100.5*(1-0.005) = 99.9975.
    // Trade at 99 is well below the bid — eligible to fill.
    events.push(make_trade_event(
        symbol,
        99,
        tikr_core::Side::Ask, // ask-side taker = someone sold = fills our resting bid
        2_000_000,
    ));

    // Fill the rest with book-at-49/50 updates.
    // Strategy bid price: 100.5 * (1 - 50/10000) = 100.5 * 0.995 = 99.9975
    // avg_entry = 99.9975 (the price we actually bought at)
    // mid after low book = (49+50)/2 = 49.5
    // unrealized = (49.5 - 99.9975) * 1 ≈ -50.4975 → net ≈ -50.4975 < -50 ✓
    let remaining = count.saturating_sub(3);
    for i in 0..remaining {
        let ts_ns = 3_000_000 + (i as u64) * 1_000_000;
        events.push(make_book_event(symbol, 49, 50, ts_ns));
    }

    events
}

/// Builds ~50 events at a stable mid that would cause `AggressiveTestStrategy`
/// to re-quote on every BookUpdate if not suppressed by a halt.
/// Used in phases 3 and 4.
///
/// The stream alternates BookUpdates with Trade events.  In Phase 3 (halted),
/// Quote actions are suppressed before FillSim so no fills occur.  In Phase 4
/// (clear_halt), Quote actions go through and FillSim can match trades → fills.
///
/// AggressiveTestStrategy bid ≈ 200.5*(1-0.005) = 199.5.  Trade at 199
/// (ask-side taker) fills our bid → Fill.  This proves recovery in Phase 4.
fn events_that_would_normally_quote(symbol: &Symbol, count: usize) -> Vec<MarketEvent> {
    let mut events = Vec::with_capacity(count);
    // Use large timestamps well past any Phase 1 timestamps so min_requote_interval
    // based on elapsed time does not suppress quoting.
    let base_ts: u64 = 1_000_000_000_000;
    let half = count / 2;
    for i in 0..half {
        let ts_ns = base_ts + (i as u64) * 2_000_000;
        // BookUpdate at 200/201 → mid=200.5 → strategy bids ≈ 199.5
        events.push(make_book_event(symbol, 200, 201, ts_ns));
        // Ask-side trade at 199 → fills a resting bid (199 ≤ 199.5) when gate is open
        events.push(make_trade_event(
            symbol,
            199,
            tikr_core::Side::Ask,
            ts_ns + 1_000_000,
        ));
    }
    // Pad to requested count with heartbeats if needed
    let ts_pad = base_ts + (half as u64) * 2_000_000 + 10_000_000;
    for i in events.len()..count {
        events.push(MarketEvent::Heartbeat {
            ts: Timestamp(ts_pad + (i as u64) * 1_000_000),
        });
    }
    events
}

// ---------------------------------------------------------------------------
// Shared risk limits
// ---------------------------------------------------------------------------

fn phase_limits() -> RiskLimits {
    RiskLimits {
        max_position_size: None,
        max_open_notional: None,
        max_drawdown: Some(Notional(Decimal::from(-50))),
        max_fills_per_minute: None,
    }
}

// ---------------------------------------------------------------------------
// The capstone test
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn risk_resume_alerting_capstone() {
    let temp = TempDir::new().unwrap();
    let symbol = make_symbol();

    // =========================================================================
    // Phase 1: trip the drawdown halt
    // =========================================================================

    let recording1: Arc<Mutex<Vec<Alert>>> = Arc::new(Mutex::new(Vec::new()));
    let venue1 = MockVenue::finite(events_that_force_losing_position(&symbol, 200));
    let strategy1 = layered_grid_aggressive();
    let fill_sim1 = fill_sim_with_zero_latency();
    let risk_gate1: Box<dyn RiskGate> = Box::new(BasicRiskGate::new(phase_limits()));
    let alert_sink1: Box<dyn AlertSink> = Box::new(RecordingSink::new(recording1.clone()));
    let config = RunnerConfig {
        state_dir: temp.path().to_path_buf(),
        snapshot_every_n_events: 50,
        skim: None,
        funding: None,
        snapshot_tap: None,
        live_tap: None,
        notional_rx: None,
        max_position_rx: None,
        wallet_rx: None,
        take_profit_pct: tikr_core::Decimal::ZERO,
        liq_window_secs: 0,
        seed_position: None,
        equity_csv_path: None,
        initial_balance: Decimal::ZERO,
        order_balance_pct: Decimal::ZERO,
        max_position_pct: Decimal::ZERO,
        min_notional: Decimal::ZERO,
        max_expected_open_orders: 2,
        liquidation: None,
        mark_series: None,
        retrace_boundary_ts: None,
        inventory_boost: None,
        bagger: tikr_paper::bagger::BaggerConfig::default(),
        spot_seed: None,
    };
    let (_tx1, rx1) = watch::channel(false);

    let report_phase1 = run_with_resume(
        venue1,
        strategy1,
        fill_sim1,
        symbol.clone(),
        rx1,
        config.clone(),
        None,
        Some(risk_gate1),
        Some(alert_sink1),
        None, // no external fills (paper mode)
        None,
    )
    .await;

    // =========================================================================
    // Phase 2: assertions on Phase 1 output
    // MutexGuard must be dropped before any subsequent `.await` — wrap in a
    // block so it never lives across an await point (clippy::await_holding_lock).
    // =========================================================================

    let risk_state_phase1: RiskState = {
        let alerts1 = recording1.lock().unwrap();
        assert!(
            alerts1.iter().any(|a| matches!(a, Alert::Halt { .. })),
            "Phase 2: expected at least one Halt alert in Phase 1; got {} alerts ({:?})",
            alerts1.len(),
            alerts1.iter().map(|a| a.discriminant()).collect::<Vec<_>>(),
        );
        // net should be <= -50 (the drawdown threshold).
        assert!(
            report_phase1.net.0 <= Decimal::from(-50),
            "Phase 2: expected net <= -50, got {}",
            report_phase1.net.0,
        );
        let rs = report_phase1
            .risk_state
            .clone()
            .expect("Phase 2: risk_state should be present in PaperReport");
        assert!(
            rs.halted,
            "Phase 2: expected halted=true in serialized risk_state"
        );
        rs
        // alerts1 guard drops here — before any .await
    };

    // =========================================================================
    // Phase 3: resume keeps the halt — 0 new fills expected
    // =========================================================================

    let recording2: Arc<Mutex<Vec<Alert>>> = Arc::new(Mutex::new(Vec::new()));
    let venue2 = MockVenue::finite(events_that_would_normally_quote(&symbol, 50));
    let strategy2 = layered_grid_aggressive();
    let fill_sim2 = fill_sim_with_zero_latency();
    // Reconstruct gate from persisted halted state — simulates operator restart.
    let risk_gate2: Box<dyn RiskGate> = Box::new(BasicRiskGate::from_state(
        phase_limits(),
        risk_state_phase1.clone(),
    ));
    let alert_sink2: Box<dyn AlertSink> = Box::new(RecordingSink::new(recording2.clone()));
    let (_tx2, rx2) = watch::channel(false);

    let report_phase3 = run_with_resume(
        venue2,
        strategy2,
        fill_sim2,
        symbol.clone(),
        rx2,
        config.clone(),
        Some(report_phase1.clone()), // RESUME from Phase 1
        Some(risk_gate2),
        Some(alert_sink2),
        None, // no external fills (paper mode)
        None,
    )
    .await;

    assert_eq!(
        report_phase3.fills_emitted - report_phase1.fills_emitted,
        0,
        "Phase 3: halt should suppress all new fills post-resume; fills_emitted went {} → {}",
        report_phase1.fills_emitted,
        report_phase3.fills_emitted,
    );
    // Clone risk_state from phase 3 before any further awaits to avoid holding
    // a borrow across an await point (clippy::await_holding_lock / lifetime).
    let risk_state_phase3: RiskState = report_phase3
        .risk_state
        .clone()
        .expect("Phase 3: risk_state should be present");
    assert!(
        risk_state_phase3.halted,
        "Phase 3: gate should remain halted after resume"
    );

    // =========================================================================
    // Phase 4: clear_halt + verify recovery (fills resume, no new Halt alerts)
    // =========================================================================

    // Construct gate from phase-3 state, then clear the halt externally.
    // Use a permissive-but-armed drawdown threshold (-10_000) — phase-4's
    // event stream can't trip it, but the limit being Some makes the
    // "no new Halt alerts" assertion below non-vacuous (verifies clear_halt
    // actually unstuck the gate, not just that no limit was checked).
    let mut risk_gate3 = BasicRiskGate::from_state(
        RiskLimits {
            max_position_size: None,
            max_open_notional: None,
            max_drawdown: Some(Notional(Decimal::from(-10_000))),
            max_fills_per_minute: None,
        },
        risk_state_phase3.clone(),
    );
    risk_gate3.clear_halt();
    assert!(
        !risk_gate3.state().halted,
        "Phase 4: clear_halt should unset halted flag"
    );

    let recording3: Arc<Mutex<Vec<Alert>>> = Arc::new(Mutex::new(Vec::new()));
    let venue3 = MockVenue::finite(events_that_would_normally_quote(&symbol, 50));
    let strategy3 = layered_grid_aggressive();
    let fill_sim3 = fill_sim_with_zero_latency();
    let alert_sink3: Box<dyn AlertSink> = Box::new(RecordingSink::new(recording3.clone()));
    let (_tx3, rx3) = watch::channel(false);

    let report_phase4 = run_with_resume(
        venue3,
        strategy3,
        fill_sim3,
        symbol.clone(),
        rx3,
        config.clone(),
        Some(report_phase3.clone()),
        Some(Box::new(risk_gate3)),
        Some(alert_sink3),
        None, // no external fills (paper mode)
        None,
    )
    .await;

    // Should have processed all 50 events.
    assert!(
        report_phase4.events_processed > report_phase3.events_processed,
        "Phase 4: expected new events processed; got {} total (phase3 was {})",
        report_phase4.events_processed,
        report_phase3.events_processed,
    );

    // No new Halt alerts after clear_halt (permissive limits + cleared state).
    let alerts3 = recording3.lock().unwrap();
    let new_halts: Vec<_> = alerts3
        .iter()
        .filter(|a| matches!(a, Alert::Halt { .. }))
        .collect();
    assert!(
        new_halts.is_empty(),
        "Phase 4: no new Halt alerts expected after clear_halt; got {} halt(s)",
        new_halts.len(),
    );

    // Phase 4 must have produced at least one new fill (recovery verified).
    // `events_that_would_normally_quote` interleaves BookUpdates with Trade
    // events; zero-latency FillSim means quotes land before the next Trade.
    // In Phase 3 the gate suppressed Quote actions → no fills.  In Phase 4
    // the cleared gate allows Quote → FillSim places the bid → Trade fills it.
    let fills_in_phase4 = report_phase4
        .fills_emitted
        .saturating_sub(report_phase3.fills_emitted);
    assert!(
        fills_in_phase4 > 0,
        "Phase 4: expected at least one new fill after clear_halt; fills went {} → {}",
        report_phase3.fills_emitted,
        report_phase4.fills_emitted,
    );

    // Core invariants verified:
    //   ✓ Phase 1 trips a Halt alert
    //   ✓ Phase 3 produces 0 new fills (halt suppresses actions)
    //   ✓ Phase 4 produces > 0 new events and no new Halt alerts
    //   ✓ Phase 4 produces > 0 new fills (recovery confirmed)
    drop(alerts3);
}
