//! Paper-trading runner — live `Venue` → `Strategy` → `FillSim` → `PaperReport`.
//!
//! # Two modes
//!
//! **Paper mode** (`external_fills = None`): fills are synthesized by
//! [`FillSim`] based on the market-event stream. No real orders are placed.
//!
//! **Live mode** (`external_fills = Some(rx)`): fills arrive over the
//! `external_fills` receiver (e.g. from a Hyperliquid `userEvents` WS task).
//! The [`FillSim`] is still wired for actions (so the strategy's `on_action`
//! results are tracked) but the fills it would synthesize are discarded — real
//! exchange fills drive the tracker instead.

use crate::alerts::{Alert, AlertSink};
use crate::live::LiveSnapshot;
use crate::report::{PaperReport, SCHEMA_VERSION};
use crate::state;
use futures::StreamExt;
use rust_decimal::MathematicalOps;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tikr_backtest::fill_sim::FillSim;
use tikr_backtest::liquidation::{LiquidationConfig, LiquidationModel};
use tikr_backtest::pnl::PositionTracker;
use tikr_core::{
    Decimal, Fill, LiqEvent, MarketEvent, Notional, Position, Price, Side, SignedSize, Snapshot,
    Symbol, Timestamp,
};
use tikr_risk::{RiskContext, RiskDecision, RiskGate};
use tikr_strategy::{Strategy, StrategyContext};
use tikr_venue::{QuoteIntent, Venue, VenueError};
use tokio::sync::{mpsc, watch};
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Max consecutive venue rejections per side before switching to single-sided quoting.
/// When one side hits this threshold, that side is skipped in subsequent quote rounds
/// until either (a) a fill on this symbol arrives, (b) a `CancelAll` action
/// is dispatched, or (c) `SIDE_FAILS_RESET_AFTER` elapses since the most
/// recent rejection. The time-based recovery exists because the original
/// reset triggers don't fire when the position is capped + the close-side
/// itself is what's blocked — without it, a brief burst of `-5022` cross
/// rejections during a fast move can strand the position for hours.
const MAX_FAILS_PER_SIDE: u32 = 3;

/// Time after which a side-lockout auto-recovers even without a fill or
/// `CancelAll`. Picked to be long enough that a continuing burst of
/// rejections still produces back-pressure (each new rejection re-stamps
/// the timestamp), but short enough that a transient venue hiccup doesn't
/// strand a position.
const SIDE_FAILS_RESET_AFTER: Duration = Duration::from_secs(10);

/// REST `userTrades` reconciliation look-back overlap. Each reconciliation
/// tick re-fetches trades back to `last_seen − this` and relies on the
/// [`TradeDedup`] set to skip the ones already applied, so an out-of-order or
/// boundary-straddling fill within this window is never missed. Also the
/// retention horizon for the dedup set (entries older than this are pruned).
const RECONCILE_LOOKBACK: Duration = Duration::from_secs(120);

/// Current wall-clock time in nanoseconds since the UNIX epoch. Live-only
/// helper for the fill-reconciliation window; the backtest path never calls
/// it (sim time is driven by recorded event timestamps). See issue #57 for
/// the planned unified Clock abstraction.
fn wall_clock_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Bounded trade-id dedup set for live fill reconciliation. Tracks the venue
/// `trade_id`s already applied to the position tracker — from EITHER the WS
/// user-data stream OR the REST `userTrades` gap-fill path — so a fill that
/// arrives over both channels is applied exactly once. Insertion order is kept
/// so entries older than [`RECONCILE_LOOKBACK`] can be pruned, bounding memory
/// to roughly one look-back window of trades.
#[derive(Default)]
struct TradeDedup {
    seen: HashSet<u64>,
    order: VecDeque<(u64, u64)>, // (ts_ms, trade_id), oldest at the front
}

impl TradeDedup {
    /// Record `id` (observed at `ts_ms`). Returns `true` if newly inserted,
    /// `false` if it was already present (i.e. a duplicate to be skipped).
    fn insert(&mut self, id: u64, ts_ms: u64) -> bool {
        if !self.seen.insert(id) {
            return false;
        }
        self.order.push_back((ts_ms, id));
        true
    }

    /// Drop entries observed strictly before `cutoff_ms` to bound memory.
    fn prune(&mut self, cutoff_ms: u64) {
        while let Some(&(ts, id)) = self.order.front() {
            if ts >= cutoff_ms {
                break;
            }
            self.order.pop_front();
            self.seen.remove(&id);
        }
    }
}

/// Runtime configuration for [`run`].
#[derive(Debug, Clone)]
pub struct RunnerConfig {
    /// Directory where state snapshots land. Default `./paper_state`.
    pub state_dir: PathBuf,
    /// Snapshot cadence in events. Default 100.
    pub snapshot_every_n_events: u32,
    /// Optional profit-skim accounting: every time perp realized P&L (net
    /// of fees + prior skims) grows by `budget × skim_pct`, that chunk is
    /// "moved" to a spot wallet that buys the base asset at the last seen
    /// mid. `None` disables. See [`SkimConfig`] for semantics.
    pub skim: Option<SkimConfig>,
    /// Optional perp funding model. When `Some`, the runner accrues funding
    /// continuously against the open position at each event:
    /// `Δfunding = −position × mark × rate × (dt_secs / 28800)`.
    /// `None` disables (back-compat: no funding cost on backtests/paper).
    pub funding: Option<FundingConfig>,
    /// Optional live snapshot publication target. When `Some`, the runner
    /// writes a fresh [`PaperReport`] into the lock on the same cadence
    /// as on-disk snapshots (`snapshot_every_n_events`). Lets a dashboard
    /// or supervisor read live PnL/position state without polling
    /// `state_dir`. `None` (default) disables — runner behaves as before.
    pub snapshot_tap: Option<std::sync::Arc<std::sync::RwLock<Option<crate::PaperReport>>>>,
    /// Optional fine-grained live state publication target. When `Some`,
    /// the runner writes a fresh [`crate::LiveSnapshot`] on every fill
    /// AND every regular snapshot tick — much higher update frequency
    /// than [`Self::snapshot_tap`], which fires only at
    /// `snapshot_every_n_events`. Holds position size, open-order
    /// counts, buy/sell split, last fill, and the best bid/ask/mid for
    /// dashboards that need to refresh on every fill.
    pub live_tap: Option<std::sync::Arc<std::sync::RwLock<Option<crate::LiveSnapshot>>>>,
    /// Optional live per-order notional updates derived from account wallet
    /// balance. Strategies decide how to refresh resting orders.
    pub notional_rx: Option<watch::Receiver<Decimal>>,
    /// Optional live per-bot position-cap updates derived from the same
    /// wallet poll as `notional_rx`. Strategies that gate adds against
    /// `max_position_usdt` update their config via `on_max_position_updated`.
    /// `None` (default) = static cap (set once at spawn).
    pub max_position_rx: Option<watch::Receiver<Decimal>>,
    /// Rolling-window length (seconds) for `StrategyContext::recent_liqs`.
    /// `0` (default) disables the liq window — the slice exposed to
    /// strategies is always empty even when `external_liqs` is set.
    /// LiqFade defaults to 60-300s in its config; the runner just
    /// prunes the buffer to this window so per-event memory stays
    /// bounded. Non-LiqFade strategies leave this 0.
    pub liq_window_secs: u32,
    /// Optional pre-existing position to seed the PositionTracker with.
    /// Used by the `tikr` supervisor when `--clear` is off so the
    /// tracker mirrors the venue's actual inventory (queried via
    /// `position_risk`) — otherwise the bot starts believing it's
    /// flat while the venue holds inherited inventory, and the
    /// strategy + cap + risk gates all reason against the wrong state.
    /// `None` (default) = fresh-flat start.
    pub seed_position: Option<Position>,
    /// Optional CSV file to append an equity-curve row to on every
    /// snapshot tick. Format: `ts_ns,sim_secs,fills,pos_size,realized,unrealized,fees,funding,net`.
    /// File is created + header-written on the first tick; subsequent
    /// ticks append. `None` (default) disables the curve export. Used by
    /// `compare` to dump per-preset PnL timelines for drawdown analysis.
    pub equity_csv_path: Option<PathBuf>,
    /// Initial wallet balance for backtest compounding (USDT). When
    /// `> 0` AND `order_balance_pct > 0`, the runner overrides any
    /// caller-supplied `notional_rx`/`max_position_rx` with internal
    /// watch channels driven by running balance =
    /// `initial_balance + realized - fees`. Each fill recomputes
    /// notional = `balance × order_balance_pct / 100` and max-pos =
    /// `balance × max_position_pct / 100`, sending updates that fire
    /// `Strategy::on_notional_updated` / `on_max_position_updated`.
    /// `0` (default) disables — sizing stays static from spawn.
    pub initial_balance: Decimal,
    /// Percent of running balance allocated per order (0-100). Drives
    /// notional updates when `initial_balance > 0`. Mirrors the live
    /// account poller's formula in `apps/tikr/src/main.rs`.
    pub order_balance_pct: Decimal,
    /// Percent of running balance used as the per-bot position cap
    /// (0-100). Drives max_position_usdt updates when
    /// `initial_balance > 0`.
    pub max_position_pct: Decimal,
    /// Venue-side minimum order notional (USDT). When `> 0`, the live
    /// dispatch path drops any `Action::Quote` whose `size × price`
    /// falls below this floor BEFORE calling `venue.quote()`. Defense
    /// against strategy emit paths that don't bump dust qty to meet
    /// the exchange filter (close-side pinned to residual position,
    /// risk module TP/SL on a near-empty position, etc.). `0` (default)
    /// disables the guard.
    pub min_notional: Decimal,
    /// Strategy's expected maximum open orders. The 30s reconcile
    /// sweep nukes everything when `venue_open` exceeds this — an
    /// orphan-accumulation safety net. SS quotes at most 1/side = 2.
    /// Grid strategies like Tide with N levels per side want
    /// `2 * N` (or more if they keep deeper historical levels). `0`
    /// (default) disables the sweep entirely — useful when the
    /// strategy intentionally keeps a lot of resting orders.
    pub max_expected_open_orders: usize,
    /// Optional isolated-margin liquidation model (paper / backtest only).
    /// When `Some` AND not in live mode, the runner force-closes the open
    /// position when the mark (book-mid proxy) breaches the computed
    /// liquidation price — realizing the loss and cancelling resting orders,
    /// mirroring a real leveraged-perp blowup. Ignored in live mode (the
    /// venue performs its own liquidation). `None` (default) disables.
    pub liquidation: Option<LiquidationConfig>,
    /// Optional recorded perp mark-price series (backtest). When `Some`, the
    /// runner marks unrealized PnL, funding, and the liquidation trigger
    /// against the recorded mark at each sim timestamp instead of the order-
    /// book mid. `None` (default) → mark falls back to book mid (prior
    /// behaviour). Loaded from `mark_<BASE>_*.parquet` via
    /// [`tikr_backtest::mark::MarkSeries`].
    pub mark_series: Option<tikr_backtest::mark::MarkSeries>,
    /// Optional inventory-aware order-size boost (runner-side, applies to
    /// every strategy). When `Some`, the reducing side's order size is scaled
    /// up on a curve as inventory approaches the per-bot cap. `None` (default)
    /// = no boost; sizes pass through untouched. See [`InventoryBoostConfig`].
    pub inventory_boost: Option<InventoryBoostConfig>,
}

/// Inventory-aware order-size boost. Scales the *inventory-reducing* side's
/// order size up on a curve as |position| approaches the cap, so the book
/// leans harder toward flattening when inventory is heavy. The inventory-
/// *growing* side is never boosted (and stays bounded by the worst-case gate
/// in [`apply_bot_inventory_cap`]). Example: short → buys (Bid) are boosted;
/// long → sells (Ask) are boosted.
///
/// The multiplier on the reducing side is
/// `1 + (max_boost_pct/100) × (|pos_notional|/cap)^curve_exponent`,
/// clamping the inventory ratio to `[0, 1]`. The denominator is the live
/// per-bot cap (`max_position_usdt`); when the cap is non-positive the boost
/// is disabled (no reference to scale against).
#[derive(Debug, Clone, Copy)]
pub struct InventoryBoostConfig {
    /// Extra order size at full inventory (|pos| == cap), as a percent of the
    /// base size. `100` ≈ up to 2× the base size when maxed; `0` disables.
    pub max_boost_pct: Decimal,
    /// Curve exponent applied to the inventory ratio (clamped `0..=1`).
    /// `1` = linear; `>1` = slow start then steep (boost concentrated near the
    /// cap); `<1` = fast early ramp. Non-positive is treated as `1`.
    pub curve_exponent: Decimal,
}

/// Perp funding accrual parameters. Binance USD-M typically pays/charges
/// every 8h at 00:00 / 08:00 / 16:00 UTC; this model is continuous (smooth
/// over time) for backtest simplicity. Positive rate = longs pay shorts.
#[derive(Debug, Clone, Copy)]
pub struct FundingConfig {
    /// Funding interval in seconds (Binance USD-M = 8h = 28800). The
    /// per-event accrual prorates `rate_per_interval` by `dt / interval_secs`.
    pub interval_secs: u64,
    /// Funding rate per interval, as a signed fraction (e.g. `0.0001` =
    /// 1 bp = 0.01%). Positive = longs pay shorts. Binance caps at ±0.0075
    /// (±75 bp) but typical mid-cap pairs sit near ±0.0001.
    pub rate_per_interval: Decimal,
}

/// Profit-skim parameters. When enabled the runner reports `base_stacked`,
/// `skim_count`, `skim_total_usdt`, and the final mark-to-market account
/// value alongside the regular PnL fields.
#[derive(Debug, Clone, Copy)]
pub struct SkimConfig {
    /// Starting USDT budget held in the perp account.
    pub budget: Decimal,
    /// Skim threshold as a fraction of `budget`. `0.05` = skim every 5%
    /// gain. Each gain chunk consumes exactly `budget × skim_pct` of
    /// `profit_since_skim`.
    pub skim_pct: Decimal,
    /// Fraction of each skim chunk that moves to spot. `1.0` (classic):
    /// all profit chunk → spot. `0.5`: half → spot, half stays in perp
    /// account (compounds trading capital). `0.0`: never skim, all stays.
    pub skim_ratio: Decimal,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            state_dir: PathBuf::from("./paper_state"),
            snapshot_every_n_events: 100,
            skim: None,
            funding: None,
            snapshot_tap: None,
            live_tap: None,
            notional_rx: None,
            max_position_rx: None,
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
            inventory_boost: None,
        }
    }
}

/// Rolling-window buffer of forced-liquidation events, drained from an
/// optional [`tokio::sync::mpsc::UnboundedReceiver`] on every market
/// event. Exposes a contiguous slice via [`Self::observe`] for
/// inclusion in [`StrategyContext::recent_liqs`].
///
/// Backtest path: caller pre-loads the channel with sorted events from
/// `LiqEventStream` (or feeds them through during replay).
/// Live path: a background task subscribes to `@forceOrder` and forwards.
///
/// The buffer holds BOTH past-and-visible events (`ts <= now`) AND
/// future events (`ts > now`, only possible in the backtest path where
/// all liqs are loaded upfront). The visible slice returned by
/// `observe` is the prefix whose timestamps are within
/// `[now - window_ns, now]`. Future events stay in the back of the
/// buffer until `now` advances past them.
struct LiqWindow {
    rx: Option<mpsc::UnboundedReceiver<LiqEvent>>,
    buffer: VecDeque<LiqEvent>,
    window_ns: u64,
}

impl LiqWindow {
    fn new(rx: Option<mpsc::UnboundedReceiver<LiqEvent>>, window_secs: u32) -> Self {
        Self {
            rx,
            buffer: VecDeque::new(),
            window_ns: (window_secs as u64).saturating_mul(1_000_000_000),
        }
    }

    /// Drain pending events from `rx` into the buffer, prune events
    /// outside the rolling window relative to `now_ns`, and return a
    /// contiguous slice of currently-visible events
    /// (`now - window_ns <= ts <= now_ns`).
    ///
    /// Returns `&[]` when the window is disabled (`window_ns == 0`) or
    /// no events are visible.
    fn observe(&mut self, now_ns: u64) -> &[LiqEvent] {
        if self.window_ns == 0 {
            return &[];
        }
        if let Some(rx) = self.rx.as_mut() {
            while let Ok(ev) = rx.try_recv() {
                self.buffer.push_back(ev);
            }
        }
        // Prune events older than the window.
        let cutoff = now_ns.saturating_sub(self.window_ns);
        while let Some(front) = self.buffer.front() {
            if front.ts.0 < cutoff {
                self.buffer.pop_front();
            } else {
                break;
            }
        }
        // Contiguousify so we can return a single slice. Cheap — the
        // buffer is bounded by `window_ns × event_rate` (tens of entries
        // typical for a 60-300s liq window).
        self.buffer.make_contiguous();
        let (head, _tail) = self.buffer.as_slices();
        // Visible prefix = events whose ts <= now. Backtest pre-loads
        // all events upfront, so the tail may hold future liqs; those
        // stay in the buffer until `now` advances.
        let visible = head.partition_point(|e| e.ts.0 <= now_ns);
        &head[..visible]
    }
}

/// Drive `strategy` against `venue.subscribe(symbol)`, returning the final
/// [`PaperReport`] when the stream ends or `shutdown` fires.
///
/// Thin wrapper over [`run_with_resume`] with no prior report and no risk gate.
///
/// # v0 limitations
///
/// - `StrategyContext.recent_fills` is always empty
/// - `StrategyContext.open_quotes` is always empty
/// - Single-symbol per call
/// - `last_mid` is zero if no `BookUpdate` ever arrived
pub async fn run<V, S>(
    venue: V,
    strategy: S,
    fill_sim: FillSim,
    symbol: Symbol,
    shutdown: watch::Receiver<bool>,
    config: RunnerConfig,
) -> PaperReport
where
    V: Venue,
    S: Strategy,
{
    run_inner(
        venue, strategy, fill_sim, symbol, shutdown, config, None, None, None, None, None,
    )
    .await
}

/// Drive `strategy` like [`run`], optionally seeding aggregate state from a
/// prior [`PaperReport`] and/or layering a [`RiskGate`] between the strategy
/// and the fill simulator.
///
/// # Resume semantics (v0 limitation)
///
/// When `resume` is `Some(prior)`, the new runner seeds:
///
/// - `realized`, `fees`, `funding` — aggregated forward from `prior`
/// - `events_processed`, `fills_emitted` — counters carry over
/// - `runtime_secs` — final report adds the new wall-clock to `prior.runtime_secs`
///
/// **Position size is reset to zero.** [`PaperReport`] only carries aggregate
/// P&L, not the raw `Position { size, avg_entry }`, so v0 resume cannot
/// reconstruct an open position. Operators must close all positions before
/// restart; otherwise unrealized P&L attribution is wrong post-resume.
/// (Position-state persistence is a future enhancement; see #32 follow-ups.)
///
/// # Risk gate
///
/// When `risk_gate` is `Some(gate)`:
/// 1. Every strategy-emitted [`tikr_strategy::Action`] is run through
///    [`RiskGate::check`] **before** the runner hands it to [`FillSim`].
///    `Allow` forwards; `Reject` logs + drops; `Halt` logs + drops (the gate
///    flips its sticky-halt state so subsequent checks return `Reject`).
/// 2. After each fill is applied to the tracker, [`RiskGate::record_fill`] is
///    called with the fill timestamp so the rolling fills-per-minute window
///    stays current.
/// 3. The final report's `risk_state` contains a clone of
///    [`RiskGate::state`].
///
/// # Alerting (#33)
///
/// When `alert_sink` is `Some(sink)`, the runner emits:
///
/// - [`Alert::Fill`] after each fill is applied to the tracker
/// - [`Alert::Halt`] when the risk gate returns `RiskDecision::Halt`
/// - [`Alert::Drawdown`] in addition to `Halt` if the halt reason contains
///   `"max_drawdown"`. The `threshold` field is a sentinel `Notional(ZERO)`
///   in v0 — the runner does not have direct access to the gate's configured
///   threshold value (a future enhancement once `RiskGate` exposes its limits
///   on the trait surface).
///
/// `ReconnectStorm`, `PositionMismatch`, and `Restart` are NOT emitted by the
/// runner in v0 — see the [`Alert`] enum rustdoc for the rationale.
///
/// Sink failures are intentionally swallowed (logged inside [`crate::alerts::MultiSink`]
/// but never propagated back to the runner) — operational alerting MUST NOT
/// crash the trading loop. See #30 for the failure-isolation decision.
///
/// # Panics
///
/// Hard-fails on `prior.schema_version != SCHEMA_VERSION`. v0 snapshots that
/// pre-date `schema_version` (i.e. from #26) are not resumable.
#[allow(clippy::too_many_arguments)]
pub async fn run_with_resume<V, S>(
    venue: V,
    strategy: S,
    fill_sim: FillSim,
    symbol: Symbol,
    shutdown: watch::Receiver<bool>,
    config: RunnerConfig,
    resume: Option<PaperReport>,
    risk_gate: Option<Box<dyn RiskGate>>,
    alert_sink: Option<Box<dyn AlertSink>>,
    external_fills: Option<mpsc::UnboundedReceiver<Fill>>,
    external_liqs: Option<mpsc::UnboundedReceiver<LiqEvent>>,
) -> PaperReport
where
    V: Venue,
    S: Strategy,
{
    if let Some(ref prior) = resume
        && prior.schema_version != SCHEMA_VERSION
    {
        panic!(
            "unsupported PaperReport schema_version {}; tikr-paper supports {}",
            prior.schema_version, SCHEMA_VERSION
        );
    }
    run_inner(
        venue,
        strategy,
        fill_sim,
        symbol,
        shutdown,
        config,
        resume,
        risk_gate,
        alert_sink,
        external_fills,
        external_liqs,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_inner<V, S>(
    venue: V,
    mut strategy: S,
    mut fill_sim: FillSim,
    symbol: Symbol,
    mut shutdown: watch::Receiver<bool>,
    config: RunnerConfig,
    resume: Option<PaperReport>,
    mut risk_gate: Option<Box<dyn RiskGate>>,
    alert_sink: Option<Box<dyn AlertSink>>,
    mut external_fills: Option<mpsc::UnboundedReceiver<Fill>>,
    external_liqs: Option<mpsc::UnboundedReceiver<LiqEvent>>,
) -> PaperReport
where
    V: Venue,
    S: Strategy,
{
    let mut liq_window = LiqWindow::new(external_liqs, config.liq_window_secs);

    // Reconstruct tracker from resume + optional seed_position.
    // - PaperReport `resume` carries running aggregates (realized/fees/
    //   funding) but NOT position size/avg_entry.
    // - `config.seed_position` carries the actual position to start
    //   from (e.g. fetched from venue.position_risk on `tikr` startup
    //   when --clear is OFF). When both present, seed_position's
    //   size + avg_entry win; resume's aggregates still apply.
    let mut tracker = match (resume.as_ref(), config.seed_position.as_ref()) {
        (Some(prior), Some(seed)) => {
            let pos = Position {
                symbol: symbol.clone(),
                size: seed.size,
                avg_entry: seed.avg_entry,
                realized_pnl: prior.realized,
            };
            PositionTracker::from_snapshot(
                symbol.clone(),
                pos,
                prior.realized,
                prior.fees,
                prior.funding,
            )
        }
        (Some(prior), None) => {
            // Legacy resume — aggregates only, position reset (v0).
            let pos = Position {
                symbol: symbol.clone(),
                size: tikr_core::SignedSize(Decimal::ZERO),
                avg_entry: Price(Decimal::ZERO),
                realized_pnl: prior.realized,
            };
            PositionTracker::from_snapshot(
                symbol.clone(),
                pos,
                prior.realized,
                prior.fees,
                prior.funding,
            )
        }
        (None, Some(seed)) => {
            // Fresh start but with an inherited position from the venue
            // (the `--clear`-off path: bot resumes against existing
            // live inventory).
            let pos = Position {
                symbol: symbol.clone(),
                size: seed.size,
                avg_entry: seed.avg_entry,
                realized_pnl: tikr_core::Notional(Decimal::ZERO),
            };
            PositionTracker::from_snapshot(
                symbol.clone(),
                pos,
                tikr_core::Notional(Decimal::ZERO),
                tikr_core::Notional(Decimal::ZERO),
                tikr_core::Notional(Decimal::ZERO),
            )
        }
        (None, None) => PositionTracker::new(symbol.clone()),
    };

    // Seed counters from resume.
    let mut events_processed: u64 = resume.as_ref().map(|r| r.events_processed).unwrap_or(0);
    let mut fills_emitted: u64 = resume.as_ref().map(|r| r.fills_emitted).unwrap_or(0);
    // Buy/sell fill counters drive the periodic status line. Not yet persisted
    // to PaperReport — resume always starts these at 0.
    let mut buy_fills: u64 = 0;
    let mut sell_fills: u64 = 0;
    // Full vs partial fill split (Fill.is_full). fills_emitted = full + partial.
    let mut full_fills: u64 = 0;
    let mut partial_fills: u64 = 0;
    let mut buy_volume: Decimal = Decimal::ZERO;
    let mut sell_volume: Decimal = Decimal::ZERO;
    // Peak absolute position notional (|size| × mid) seen during the
    // run. Sampled on BookUpdate events so a strategy that grew to its
    // cap then traded out still shows the high-water mark in the
    // final report.
    let mut peak_position_usdt: Decimal = Decimal::ZERO;
    // Running accumulators for MEAN absolute position notional (same sample
    // cadence as the peak). Mean shows typical inventory load, not just the
    // high-water mark — a lower mean at equal net = the algo carried less risk.
    let mut position_usdt_sum: Decimal = Decimal::ZERO;
    let mut position_samples: u64 = 0;
    // Peak fills-per-minute via a 60s sliding window. The window state itself
    // can't carry across a resume, but the high-water mark does.
    let mut fill_rate = FillRateTracker {
        peak_per_min: resume.as_ref().map(|r| r.peak_fills_per_min).unwrap_or(0),
        ..FillRateTracker::default()
    };
    // Order rejections observed (predominantly post-only -5022 would-cross).
    let mut rejected_orders: u64 = resume.as_ref().map(|r| r.rejected_orders).unwrap_or(0);
    let resumed_runtime_secs: u64 = resume.as_ref().map(|r| r.runtime_secs).unwrap_or(0);
    let resumed_sim_duration_secs: u64 = resume.as_ref().map(|r| r.sim_duration_secs).unwrap_or(0);

    // Skim-mode state. Tracks profit accumulated since the last skim. When it
    // crosses `budget × skim_pct`, that chunk is split into a spot buy (size
    // = chunk × skim_ratio) plus an amount that stays in the perp account
    // (chunk × (1 − skim_ratio)) — the retained piece compounds trading
    // capital. skim_ratio=1 keeps the original "skim all" behavior.
    let skim_cfg = config.skim;
    let mut skim_threshold = Decimal::ZERO;
    let mut skim_ratio = Decimal::ONE;
    if let Some(sc) = skim_cfg {
        skim_threshold = sc.budget * sc.skim_pct;
        skim_ratio = sc.skim_ratio;
    }
    let mut profit_since_skim = Decimal::ZERO;
    let mut last_net_seen = Decimal::ZERO;
    let mut skim_count: u64 = resume.as_ref().map(|r| r.skim_count).unwrap_or(0);
    let mut skim_total_usdt: Decimal = resume
        .as_ref()
        .map(|r| r.skim_total_usdt.0)
        .unwrap_or(Decimal::ZERO);
    let mut base_stacked: Decimal = resume
        .as_ref()
        .map(|r| r.base_stacked.0)
        .unwrap_or(Decimal::ZERO);

    let mut current_book = empty_snapshot(&symbol);
    // The strategy's open-quote view comes from `fill_sim.open_quotes(&symbol)`,
    // which serves a borrowed slice from an internal cache that only rebuilds
    // (and re-clones symbols) when the resting-order set actually changes.
    let mut last_mid = Price(Decimal::ZERO);
    // Perp mark price for unrealized PnL, funding, and liquidation. Sourced
    // from `config.mark_series` when present; otherwise falls back to
    // `last_mid` so behaviour is unchanged when no mark stream is supplied.
    let mut last_mark = Price(Decimal::ZERO);
    let mut last_fill: Option<Fill> = None;
    // Per-symbol side-failure tracker. When one side's venue.quote() fails
    // MAX_FAILS_PER_SIDE times consecutively, that side is skipped until the
    // next CancelAll (full requote cycle) resets the counters.
    let mut side_fails: HashMap<String, (u32, u32)> = HashMap::new();
    // Most-recent rejection timestamp per symbol. Drives the
    // `SIDE_FAILS_RESET_AFTER` time-based auto-recovery so a brief
    // burst of `-5022` cross rejections during a fast move doesn't
    // strand a position when the natural reset triggers
    // (fill + CancelAll) don't fire.
    let mut side_fails_last: HashMap<String, Instant> = HashMap::new();
    // Fill reconciliation (live): dedup trade ids already applied to the
    // tracker (via WS or REST) + the REST `userTrades` look-back window start.
    // Initialised to "now" so the gap-fill path only ever replays fills from
    // the current live session, never historical trades already reflected in a
    // resumed snapshot or the seeded starting position.
    let mut trade_dedup = TradeDedup::default();
    let mut reconcile_from_ns: u64 = wall_clock_ns();
    let started = Instant::now();
    // Decouple in-memory `snapshot_tap` updates from event-count disk
    // writes so the dashboard sidebar refreshes every ~250ms regardless
    // of how chatty the symbol's book is. Disk persistence still rides
    // on `snapshot_every_n_events`.
    let mut last_tap_publish = started;
    const TAP_MIN_INTERVAL: Duration = Duration::from_millis(250);
    // Track sim-time span from event timestamps so backtest reports show
    // data-time duration, not wall-clock replay speed.
    let mut first_event_ts: Option<Timestamp> = None;
    let mut last_event_ts: Option<Timestamp> = None;
    // Funding accrual state: timestamp of the last event we accrued through.
    // On each subsequent event we apply `position × mark × rate × dt/28800s`
    // continuously, then advance.
    let funding_cfg = config.funding;
    // Isolated-margin liquidation model (paper/backtest only — see field doc).
    let mut liq_model = config.liquidation.map(LiquidationModel::new);
    // Optional recorded mark-price series (backtest). Queried per event by
    // sim-time; `None` → mark falls back to book mid.
    let mut mark_series = config.mark_series;
    let mut notional_rx = config.notional_rx;
    let mut max_position_rx = config.max_position_rx;
    // Balance-compounding channels (backtest mode). When enabled,
    // overrides any caller-supplied notional/max_position channels.
    let compounding_enabled =
        config.initial_balance > Decimal::ZERO && config.order_balance_pct > Decimal::ZERO;
    let initial_balance = config.initial_balance;
    let order_balance_pct = config.order_balance_pct;
    let max_position_pct = config.max_position_pct;
    let (balance_notional_tx, balance_maxpos_tx) = if compounding_enabled {
        let init_notional = initial_balance * order_balance_pct / Decimal::from(100);
        let init_maxpos = initial_balance * max_position_pct / Decimal::from(100);
        let (ntx, nrx) = watch::channel(init_notional);
        notional_rx = Some(nrx);
        let mtx_opt = if max_position_pct > Decimal::ZERO {
            let (mtx, mrx) = watch::channel(init_maxpos);
            max_position_rx = Some(mrx);
            Some(mtx)
        } else {
            None
        };
        (Some(ntx), mtx_opt)
    } else {
        (None, None)
    };
    // Runner-side wallet inventory cap (USDT notional). Mirrors what an
    // inventory-aware strategy (e.g. Wave) self-limits, but enforced centrally
    // so it binds for ANY algo — strategies with no inventory logic (SimpleGap,
    // MMR) otherwise run to the venue margin backstop. Seeded from the initial
    // `max_position_rx` value, refreshed whenever that channel ticks. `0` =
    // disabled (no cap configured).
    let mut current_max_position = max_position_rx
        .as_ref()
        .map(|rx| *rx.borrow())
        .unwrap_or(Decimal::ZERO);
    // Runner-side inventory-aware order-size boost (applies to every
    // strategy). Static config; the live cap above is the curve denominator.
    let inventory_boost = config.inventory_boost;
    let mut last_funding_ts: Option<Timestamp> = None;
    let run_id = make_run_id(&symbol);

    // Equity-curve CSV writer. Lazy-opened on the first snapshot tick
    // (header gets written then). `None` when the feature is disabled or
    // the path fails to open — open failure is logged but does not abort
    // the run, since the CSV is purely an introspection aid.
    let mut equity_csv_writer: Option<std::io::BufWriter<std::fs::File>> = None;
    let equity_csv_path = config.equity_csv_path.clone();

    info!(
        symbol = %symbol.base.0,
        run_id = %run_id,
        resumed = resume.is_some(),
        risk_gate = risk_gate.is_some(),
        "paper runner starting"
    );

    // First-connect: if subscribe fails synchronously, return a zero report.
    let mut stream = match venue.subscribe(&symbol).await {
        Ok(s) => s,
        Err(e) => {
            warn!("subscribe failed: {}", e);
            let mut report = finalize(
                &tracker,
                last_mark,
                started,
                events_processed,
                fills_emitted,
                &risk_gate,
                first_event_ts,
                last_event_ts,
                skim_cfg,
                skim_count,
                skim_total_usdt,
                base_stacked,
                &symbol,
                buy_volume,
                sell_volume,
                peak_position_usdt,
                position_usdt_sum,
                position_samples,
                full_fills,
                partial_fills,
                liq_model.as_ref().map(|m| m.count()).unwrap_or(0),
                fill_rate.peak_per_min,
                rejected_orders,
            );
            report.runtime_secs = resumed_runtime_secs.saturating_add(report.runtime_secs);
            report.sim_duration_secs =
                resumed_sim_duration_secs.saturating_add(report.sim_duration_secs);
            // Non-blocking publish: if a reader (dashboard TUI) is mid-
            // draw and holds the read lock, skip this tick rather than
            // blocking the event loop. The next snapshot interval will
            // refresh — eventual consistency is fine for a dashboard.
            if let Some(ref tap) = config.snapshot_tap
                && let Ok(mut guard) = tap.try_write()
            {
                *guard = Some(report.clone());
            }
            return report;
        }
    };

    // Whether we are in live mode (external fills) or paper mode (FillSim).
    // In live mode the FillSim is still driven by actions for state tracking
    // but its synthesized fills are discarded; real fills come from `external_fills`.
    let live_mode = external_fills.is_some();

    // 1 Hz status sampler. Only emits when something changed since the last
    // print — fills, open-quote counts, or position size. Idle ticks are
    // silent so logs don't fill with redundant lines.
    let mut status_tick = tokio::time::interval(Duration::from_secs(1));
    status_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    status_tick.tick().await;
    let mut last_status_fingerprint: Option<(u64, u32, u32, Decimal)> = None;

    // 30 s order-reconciliation tick (live mode only). Polls
    // `venue.open_orders` and drops any FillSim ghosts — orders that
    // were silently cancelled / expired by the venue, or lost across a
    // listenKey reconnect. Skip in paper mode (FillSim is authoritative).
    let mut recon_tick = tokio::time::interval(Duration::from_secs(30));
    recon_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    recon_tick.tick().await;

    // Initial reconciliation: on startup, cancel ANY existing orders on
    // this symbol. The bot can't have created them in this incarnation,
    // so they're stale from a prior run (crash, supervisor respawn, code
    // reload) — at prices that no longer match the strategy's current
    // state. Better to wipe + let the strategy place fresh quotes than
    // adopt orphan orders the strategy didn't intend.
    //
    // Skipped in paper mode (FillSim is authoritative; nothing to clean).
    if live_mode {
        match venue.open_orders(&symbol).await {
            Ok(orders) if !orders.is_empty() => {
                info!(
                    venue_open = orders.len(),
                    "initial reconciliation: cancelling {} pre-existing order(s) on {} — bot did not create them",
                    orders.len(),
                    symbol.base.0
                );
                match venue.cancel_all(&symbol).await {
                    Ok(()) => {
                        info!("initial reconciliation: cancel_all OK — clean slate")
                    }
                    Err(e) => warn!(
                        error = ?e,
                        "initial reconciliation: cancel_all failed — strategy may collide with stale orders"
                    ),
                }
            }
            Ok(_) => {
                // No pre-existing orders — nothing to clean.
            }
            Err(e) => {
                warn!(error = ?e, "initial reconciliation: venue.open_orders failed (cannot check for stale orders)");
            }
        }
    }

    loop {
        // Poll the external fill receiver when in live mode. We use an async
        // block that resolves to `Option<Fill>` so the select! can be unified.
        //
        // `biased`: poll branches top-to-bottom in a FIXED order instead of
        // tokio's default random selection. This is load-bearing for backtest
        // determinism — after a fill the event branch sends on the
        // balance-compounding watch channels (notional/max_position), so the
        // next poll has BOTH those branches and the next event ready. Random
        // selection applied the size/cap update before-or-after the next event
        // inconsistently, changing quote sizes run-to-run (amplified into large
        // PnL swings on volatile symbols). Fixed order makes replays
        // reproducible. The watch/timer branches are only briefly ready (just
        // after a fill / once a second), never perpetually, so they cannot
        // starve the event stream — safe for live too.
        tokio::select! {
            biased;
            changed = async {
                match notional_rx.as_mut() {
                    Some(rx) => rx.changed().await.is_ok(),
                    None => std::future::pending().await,
                }
            } => {
                if changed
                    && let Some(rx) = notional_rx.as_ref()
                {
                    let next_notional = *rx.borrow();
                    let pos = tracker.snapshot();
                    let open_quotes = fill_sim.live_quotes_for(&symbol);
                    let liqs = liq_window.observe(last_event_ts.unwrap_or(Timestamp(0)).0);
                    let ctx = StrategyContext {
                        symbol: &symbol,
                        now: last_event_ts.unwrap_or(Timestamp(0)),
                        position: &pos,
                        recent_fills: &[],
                        latest_book: &current_book,
                        open_quotes: &open_quotes,
                        recent_liqs: liqs,
                    };
                    let actions = strategy.on_notional_updated(&ctx, next_notional);
                    if !actions.is_empty() {
                        info!(notional_per_order = %next_notional, "order notional updated");
                        dispatch_post_fill_actions(
                            actions,
                            &venue,
                            &mut fill_sim,
                            &mut strategy,
                            &symbol,
                            last_event_ts.unwrap_or(Timestamp(0)),
                            &pos,
                            &current_book,
                            live_mode,
                            &mut side_fails,
                            config.min_notional,
                            current_max_position,
                            inventory_boost,
                        ).await;
                    }
                }
            }
            changed = async {
                match max_position_rx.as_mut() {
                    Some(rx) => rx.changed().await.is_ok(),
                    None => std::future::pending().await,
                }
            } => {
                if changed
                    && let Some(rx) = max_position_rx.as_ref()
                {
                    let next_max_pos = *rx.borrow();
                    current_max_position = next_max_pos;
                    let pos = tracker.snapshot();
                    let open_quotes = fill_sim.live_quotes_for(&symbol);
                    let liqs = liq_window.observe(last_event_ts.unwrap_or(Timestamp(0)).0);
                    let ctx = StrategyContext {
                        symbol: &symbol,
                        now: last_event_ts.unwrap_or(Timestamp(0)),
                        position: &pos,
                        recent_fills: &[],
                        latest_book: &current_book,
                        open_quotes: &open_quotes,
                        recent_liqs: liqs,
                    };
                    let actions = strategy.on_max_position_updated(&ctx, next_max_pos);
                    if !actions.is_empty() {
                        info!(max_position_usdt = %next_max_pos, "position cap updated");
                        dispatch_post_fill_actions(
                            actions,
                            &venue,
                            &mut fill_sim,
                            &mut strategy,
                            &symbol,
                            last_event_ts.unwrap_or(Timestamp(0)),
                            &pos,
                            &current_book,
                            live_mode,
                            &mut side_fails,
                            config.min_notional,
                            current_max_position,
                            inventory_boost,
                        ).await;
                    }
                }
            }
            ev = stream.next() => {
                let Some(event) = ev else {
                    info!("event stream ended");
                    break;
                };
                let ts = event_ts(&event);
                if first_event_ts.is_none() {
                    first_event_ts = Some(ts);
                }
                last_event_ts = Some(ts);

                if let MarketEvent::BookUpdate { snapshot } = &event {
                    // Refresh in place rather than cloning the whole Snapshot:
                    // the symbol is constant (single-symbol runner), so reuse
                    // its (and the Vecs') allocation and skip the per-event
                    // Arc<str> clone/drop churn that dominated the profile.
                    current_book.bids.clear();
                    current_book.bids.extend_from_slice(&snapshot.bids);
                    current_book.asks.clear();
                    current_book.asks.extend_from_slice(&snapshot.asks);
                    current_book.ts = snapshot.ts;
                    if let (Some(b), Some(a)) = (snapshot.bids.first(), snapshot.asks.first()) {
                        last_mid = Price((b.price.0 + a.price.0) / Decimal::from(2));
                        // Sample peak position notional at this fresh
                        // mid. tracker.snapshot() is cheap (struct copy)
                        // so once-per-book is fine even on chatty syms.
                        let pos_notional = tracker.snapshot().size.0.abs() * last_mid.0;
                        if pos_notional > peak_position_usdt {
                            peak_position_usdt = pos_notional;
                        }
                        position_usdt_sum += pos_notional;
                        position_samples += 1;
                    }
                }

                // Refresh the perp mark from the recorded series (if any) at
                // the current sim time; otherwise track book mid. Drives
                // unrealized PnL, funding, and the liquidation trigger below.
                last_mark = mark_series
                    .as_mut()
                    .and_then(|s| s.mark_at(ts.0))
                    .unwrap_or(last_mid);

                // Funding accrual: continuous model. On each event, charge
                // (or credit) `position × mark × rate × (dt / 28800s)`. The
                // sign convention: positive funding rate → longs pay,
                // shorts receive (`amount = −size × mark × rate × dt/8h`).
                // Rate source: the recorded funding rate from the mark series
                // at this timestamp when present (real, time-varying), else
                // the flat configured `rate_per_interval` fallback.
                if let Some(fcfg) = funding_cfg
                    && last_mark.0 > Decimal::ZERO
                {
                    if let Some(prev_ts) = last_funding_ts {
                        let dt_ns = ts.0.saturating_sub(prev_ts.0);
                        if dt_ns > 0 {
                            let rate = mark_series
                                .as_mut()
                                .and_then(|s| s.funding_at(ts.0))
                                .unwrap_or(fcfg.rate_per_interval);
                            let dt_secs = Decimal::from(dt_ns) / Decimal::from(1_000_000_000u64);
                            let interval = Decimal::from(fcfg.interval_secs.max(1));
                            let pos_size = tracker.snapshot().size.0;
                            let amount =
                                -pos_size * last_mark.0 * rate * (dt_secs / interval);
                            tracker.accrue_funding(amount);
                        }
                    }
                    last_funding_ts = Some(ts);
                }

                // Forced liquidation (paper/backtest only — live venue runs
                // its own). Checked against the current mark (book-mid proxy)
                // BEFORE the strategy reacts, so the strategy sees a freshly
                // flat position on this same event. The realized loss lands in
                // the tracker via the synthetic close fill.
                if !live_mode
                    && last_mark.0 > Decimal::ZERO
                    && let Some(model) = liq_model.as_mut()
                {
                    let pos_now = tracker.snapshot();
                    if let Some(fill) = model.check(&pos_now, last_mark, ts) {
                        warn!(
                            liq_price = %fill.price.0,
                            mark = %last_mark.0,
                            size = %fill.size.0,
                            side = ?fill.side,
                            "LIQUIDATION — mark breached liq price; force-closing position"
                        );
                        // A real liquidation cancels the account's resting orders.
                        fill_sim.drop_quotes_for(&symbol);
                        apply_fill(
                            fill,
                            &mut tracker,
                            &mut risk_gate,
                            &mut fills_emitted,
                            &mut full_fills,
                            &mut partial_fills,
                            &mut buy_fills,
                            &mut sell_fills,
                            &mut buy_volume,
                            &mut sell_volume,
                            &mut fill_rate,                            alert_sink.as_deref(),
                            &symbol,
                        )
                        .await;
                    }
                }

                let pos = tracker.snapshot();
                // Committed (resting + in-flight) same-side exposure for the
                // bot inventory gate below — counts in-flight orders so submit
                // latency can't let placements pile up past the cap.
                let (resting_bid_notional, resting_ask_notional) =
                    fill_sim.committed_notional_by_side(&symbol);
                let open_quotes = fill_sim.open_quotes(&symbol);
                let liqs = liq_window.observe(ts.0);
                let ctx = StrategyContext {
                    symbol: &symbol,
                    now: ts,
                    position: &pos,
                    recent_fills: &[],
                    latest_book: &current_book,
                    open_quotes,
                    recent_liqs: liqs,
                };

                let actions = strategy.on_event(&ctx, &event);

                // Bot-side worst-case inventory cap (order-placement logic for
                // the bot's own wallet limit; the exchange margin backstop is
                // simulated separately in FillSim). Value the position at COST
                // BASIS (avg_entry) so the cap bounds capital actually DEPLOYED,
                // not its mark-to-market value. With mark, a losing long marked
                // down shrinks the notional → releases the cap → the bot buys
                // deeper into the drop, over-accumulating the loser. Cost basis
                // binds on what was paid. Fall back to mark only when the entry
                // is unknown (shouldn't happen with a non-zero position).
                let mark_price = if last_mark.0 > Decimal::ZERO {
                    last_mark.0
                } else {
                    snapshot_mid(&current_book).unwrap_or(last_mid.0)
                };
                let cap_price = if pos.avg_entry.0 > Decimal::ZERO {
                    pos.avg_entry.0
                } else {
                    mark_price
                };
                let signed_pos_notional = pos.size.0 * cap_price;
                // Inventory-aware order-size boost runs BEFORE the cap: it only
                // enlarges the reducing side, which the cap never blocks, so
                // the worst-case growing-side bound below is unaffected.
                let actions = match inventory_boost {
                    Some(boost) => apply_inventory_size_boost(
                        actions,
                        signed_pos_notional,
                        current_max_position,
                        boost,
                    ),
                    None => actions,
                };
                let actions = apply_bot_inventory_cap(
                    actions,
                    signed_pos_notional,
                    resting_bid_notional,
                    resting_ask_notional,
                    current_max_position,
                );

                // Risk-gate filter — same `risk_ctx` for every action in this
                // batch (tracker state can't change mid-loop since fills
                // arrive via a separate select arm), so doing it up-front
                // lets the dispatch loop below batch quotes without
                // re-entering the gate per action.
                let mut filtered: Vec<tikr_strategy::Action> = Vec::with_capacity(actions.len());
                for action in actions {
                    if let Some(gate) = risk_gate.as_mut() {
                        let pnl_now = tracker.report(last_mark);
                        let risk_ctx = RiskContext {
                            position: &pos,
                            pnl: pnl_now,
                            now: ts,
                        };
                        match gate.check(&action, &risk_ctx) {
                            RiskDecision::Allow => {}
                            RiskDecision::Reject(reason) => {
                                warn!(?action, reason = %reason, "risk: action rejected");
                                continue;
                            }
                            RiskDecision::Halt(reason) => {
                                warn!(?action, reason = %reason, "risk: HALT — action dropped, sticky");
                                if let Some(sink) = alert_sink.as_ref() {
                                    let halt_msg = reason.clone();
                                    let _ = sink
                                        .send(Alert::Halt {
                                            reason: halt_msg.clone(),
                                        })
                                        .await;
                                    if reason.contains("max_drawdown") {
                                        let report = tracker.report(last_mark);
                                        let _ = sink
                                            .send(Alert::Drawdown {
                                                net: report.net,
                                                threshold: Notional(Decimal::ZERO),
                                            })
                                            .await;
                                    }
                                }
                                continue;
                            }
                        }
                    }
                    filtered.push(action);
                }

                // Live mode: dispatch the action to the real venue.
                // Fills come back via the external_fills channel.
                // Paper mode: skip the venue call (fill_sim simulates).
                //
                // Quote handling is special — we feed the venue's returned
                // QuoteId into FillSim so subsequent strategy `Cancel(id)`
                // actions reference ids the venue recognizes.
                //
                // Consecutive `Quote` actions in `filtered` are dispatched
                // concurrently via `join_all` so the venue sees them as
                // close together in time as possible. The strategy already
                // emits them inner-out (`sort_inside_out` in StaticGrid),
                // but with concurrent dispatch they all leave the host at
                // roughly the same instant — a fast market move can't fill
                // the early orders on a single side before later orders
                // even leave the box. Non-Quote actions still execute
                // sequentially (Cancel/Requote/CancelAll have ordering
                // semantics relative to neighbouring quotes).
                if live_mode {
                    let mut i = 0;
                    while i < filtered.len() {
                        if matches!(filtered[i], tikr_strategy::Action::Quote(_)) {
                            let mut run_intents: Vec<QuoteIntent> = Vec::new();
                            // Time-based lockout auto-recovery: if the
                            // last rejection was long enough ago, treat
                            // the side as healthy again. Stops a brief
                            // burst of `-5022` cross rejections from
                            // permanently locking a side when the
                            // natural reset triggers (fill, CancelAll)
                            // can't fire (e.g. cap-pinned position with
                            // close-side itself blocked).
                            let key = symbol.base.0.to_string();
                            if let Some(last) = side_fails_last.get(&key)
                                && last.elapsed() >= SIDE_FAILS_RESET_AFTER
                            {
                                side_fails.remove(&key);
                                side_fails_last.remove(&key);
                            }
                            while i < filtered.len() {
                                if let tikr_strategy::Action::Quote(intent) = &filtered[i] {
                                    let state = side_fails
                                        .entry(symbol.base.0.to_string())
                                        .or_insert((0, 0));
                                    let skip = match intent.side {
                                        Side::Bid => state.0 >= MAX_FAILS_PER_SIDE,
                                        Side::Ask => state.1 >= MAX_FAILS_PER_SIDE,
                                    };
                                    // Sub-min-notional guard — drop dust emits
                                    // before the venue rejects them. Catches
                                    // residual close-side qty, TP/SL on
                                    // near-empty positions, etc.
                                    let below_min = config.min_notional > Decimal::ZERO
                                        && intent.size.0 * intent.price.0
                                            < config.min_notional;
                                    if below_min {
                                        debug!(
                                            side = ?intent.side,
                                            qty = %intent.size.0,
                                            price = %intent.price.0,
                                            notional = %(intent.size.0 * intent.price.0),
                                            min = %config.min_notional,
                                            "live: dropping sub-min-notional quote"
                                        );
                                    } else if !skip {
                                        run_intents.push(intent.clone());
                                    } else {
                                        debug!(
                                            side = ?intent.side, bid_fails = state.0, ask_fails = state.1,
                                            "live: skipping quote — side exceeded max failures"
                                        );
                                    }
                                    i += 1;
                                } else {
                                    break;
                                }
                            }
                            if run_intents.is_empty() {
                                debug!("live: all quote intents filtered — skipping dispatch");
                                continue;
                            }
                            let results = futures::future::join_all(
                                run_intents
                                    .iter()
                                    .map(|intent| venue.quote(intent.clone())),
                            )
                            .await;
                            for (intent, r) in run_intents.into_iter().zip(results.into_iter()) {
                                let state = side_fails
                                    .entry(symbol.base.0.to_string())
                                    .or_insert((0, 0));
                                match r {
                                    Ok(qid) => {
                                        info!(
                                            side = ?intent.side, price = %intent.price.0, size = %intent.size.0,
                                            quote_id = ?qid, "live: order placed"
                                        );
                                        match intent.side {
                                            Side::Bid => state.0 = 0,
                                            Side::Ask => state.1 = 0,
                                        }
                                        fill_sim.enqueue_place_with_id(intent, ts, qid);
                                    }
                                    Err(e) => {
                                        let msg = format!("{e:?}");
                                        let is_transient = msg.contains("-5022")
                                            || msg.contains("Post Only")
                                            || msg.contains("RateLimited")
                                            || msg.contains("-4400")
                                            || msg.contains("Quantitative Rules");
                                        warn!(error = ?e, "live: venue.quote failed");
                                        // Post-only races are market jitter, not a
                                        // strategy/config bug — don't burn the
                                        // side_fails budget on them.
                                        if !is_transient {
                                            match intent.side {
                                                Side::Bid => state.0 += 1,
                                                Side::Ask => state.1 += 1,
                                            }
                                            side_fails_last
                                                .insert(symbol.base.0.to_string(), Instant::now());
                                        }
                                    }
                                }
                            }
                            continue;
                        }
                        match &filtered[i] {
                            tikr_strategy::Action::Requote { id, intent } => {
                                match venue.requote(*id, intent.clone()).await {
                                    Ok(()) => info!(
                                        side = ?intent.side, price = %intent.price.0, size = %intent.size.0,
                                        old_id = ?id, "live: order requoted"
                                    ),
                                    Err(e) => warn!(error = ?e, "live: venue.requote failed"),
                                }
                            }
                            tikr_strategy::Action::Cancel(id) => {
                                match venue.cancel(*id).await {
                                    // UnknownQuote = the order is already gone on
                                    // the venue (filled / canceled / lost across a
                                    // reconnect). Drop it from the local mirror too,
                                    // else we re-issue the cancel every event and
                                    // spin forever on a phantom order.
                                    Ok(()) | Err(VenueError::UnknownQuote) => {
                                        fill_sim.drop_quote(*id)
                                    }
                                    Err(e) => warn!(error = ?e, "live: venue.cancel failed"),
                                }
                            }
                            tikr_strategy::Action::CancelAll => {
                                side_fails.remove(symbol.base.0.as_ref());
                                match venue.cancel_all(&symbol).await {
                                    Ok(()) | Err(VenueError::UnknownQuote) => {
                                        fill_sim.drop_quotes_for(&symbol)
                                    }
                                    Err(e) => warn!(error = ?e, "live: venue.cancel_all failed"),
                                }
                            }
                            tikr_strategy::Action::NoOp => {}
                            tikr_strategy::Action::Quote(_) => unreachable!(),
                        }
                        if matches!(filtered[i], tikr_strategy::Action::Requote { .. }) {
                            // Requotes still use FillSim's delayed replace path.
                            fill_sim.on_action(filtered[i].clone(), ts);
                        }
                        i += 1;
                    }
                } else {
                    // Paper mode: all actions feed FillSim; no venue calls.
                    for action in filtered {
                        fill_sim.on_action(action, ts);
                    }
                }

                // In paper mode: synthesize fills from FillSim.
                // In live mode: discard synthesized fills (real fills come from
                //               the `external_fills` arm below).
                if !live_mode {
                    let fills = fill_sim.on_market_event(&event, ts);
                    for fill in fills {
                        let fill_clone = fill.clone();
                        let fill_is_full = fill_clone.is_full;
                        apply_fill(
                            fill,
                            &mut tracker,
                            &mut risk_gate,
                            &mut fills_emitted,
                            &mut full_fills,
                            &mut partial_fills,
                            &mut buy_fills,
                            &mut sell_fills,
                            &mut buy_volume,
                            &mut sell_volume,
                            &mut fill_rate,                            alert_sink.as_deref(),
                            &symbol,
                        )
                        .await;
                        // Balance-compounding: republish notional + max-pos
                        // derived from running balance = initial + realized - fees.
                        if compounding_enabled {
                            let balance = initial_balance + tracker.realized().0 - tracker.fees().0;
                            let new_notional = (balance * order_balance_pct / Decimal::from(100))
                                .max(Decimal::ZERO);
                            if let Some(tx) = balance_notional_tx.as_ref() {
                                let cur = *tx.borrow();
                                if (new_notional - cur).abs()
                                    > Decimal::from_str_exact("0.01").unwrap()
                                {
                                    let _ = tx.send(new_notional);
                                }
                            }
                            if let Some(mtx) = balance_maxpos_tx.as_ref() {
                                let new_maxpos =
                                    (balance * max_position_pct / Decimal::from(100))
                                        .max(Decimal::ZERO);
                                let cur = *mtx.borrow();
                                if (new_maxpos - cur).abs()
                                    > Decimal::from_str_exact("0.01").unwrap()
                                {
                                    let _ = mtx.send(new_maxpos);
                                }
                            }
                        }
                        last_fill = Some(fill_clone.clone());
                        publish_live(
                            &config.live_tap,
                            &tracker,
                            &fill_sim,
                            &symbol,
                            last_mid,
                            &current_book,
                            buy_fills,
                            sell_fills,
                            buy_volume,
                            sell_volume,
                            &last_fill,
                        );
                        // Partial: the LiveQuote is still on the book (FillSim
                        // keeps it around with reduced size_remaining). Don't
                        // notify the strategy so it won't move/cancel a still-
                        // valid resting order.
                        if !fill_is_full {
                            continue;
                        }
                        // Notify strategy of its own fill so re-entry / SAR /
                        // ladder-rebuild logic can react synchronously. Action
                        // results queue through fill_sim and process on the
                        // NEXT market event (no infinite-recursion risk).
                        let post_fill_pos = tracker.snapshot();
                        let post_fill_quotes = fill_sim.live_quotes_for(&symbol);
                        let fill_event = MarketEvent::Fill(fill_clone.clone());
                        let liqs = liq_window.observe(ts.0);
                        let fill_ctx = StrategyContext {
                            symbol: &symbol,
                            now: ts,
                            position: &post_fill_pos,
                            recent_fills: std::slice::from_ref(&fill_clone),
                            latest_book: &current_book,
                            open_quotes: &post_fill_quotes,
                            recent_liqs: liqs,
                        };
                        let fill_actions = strategy.on_event(&fill_ctx, &fill_event);
                        dispatch_post_fill_actions(
                            fill_actions,
                            &venue,
                            &mut fill_sim,
                            &mut strategy,
                            &symbol,
                            ts,
                            &post_fill_pos,
                            &current_book,
                            false,
                            &mut side_fails,
                            config.min_notional,
                            current_max_position,
                            inventory_boost,
                        )
                        .await;
                    }
                } else {
                    // Still call on_market_event so FillSim internal state advances.
                    let _ = fill_sim.on_market_event(&event, ts);
                }

                // Paper-mode post-only rejections: FillSim collected any
                // PostOnly intent whose price would cross the touch this
                // tick. Route each through `strategy.on_quote_rejected` so
                // the strategy's recovery path is exercised in backtests
                // (mirrors the live-mode recovery loop in
                // dispatch_post_fill_actions). Resulting actions queue
                // through fill_sim and apply on the NEXT event.
                //
                // Bounded by MAX_RECOVERY_ROUNDS to match live mode
                // (dispatch_post_fill_actions:1514). Without the cap,
                // strategies whose `on_quote_rejected` re-emits a full
                // ladder (LG/SG) can blow up exponentially when the
                // position cap rejects every grid level — each rejection
                // spawns N new placements which all reject again.
                if !live_mode {
                    const MAX_RECOVERY_ROUNDS: usize = 5;
                    let mut round = 0;
                    loop {
                        let rejections = fill_sim.drain_rejections();
                        if rejections.is_empty() || round >= MAX_RECOVERY_ROUNDS {
                            break;
                        }
                        round += 1;
                        rejected_orders += rejections.len() as u64;
                        let rec_pos = tracker.snapshot();
                        for (rej_intent, rej_reason) in rejections {
                            // Skip recovery for margin-insufficient rejections.
                            // The cap is binding until position decays via TP/SL
                            // or external fills — re-emitting the same intent
                            // (or worse, the whole grid via LG's
                            // `on_quote_rejected`) will just bounce again,
                            // burning CPU for zero progress.
                            if rej_reason.starts_with("margin insufficient") {
                                continue;
                            }
                            let rec_quotes = fill_sim.live_quotes_for(&symbol);
                            let liqs = liq_window.observe(ts.0);
                            let rec_ctx = StrategyContext {
                                symbol: &symbol,
                                now: ts,
                                position: &rec_pos,
                                recent_fills: &[],
                                latest_book: &current_book,
                                open_quotes: &rec_quotes,
                                recent_liqs: liqs,
                            };
                            let recovery_actions =
                                strategy.on_quote_rejected(&rec_ctx, &rej_intent, &rej_reason);
                            for action in recovery_actions {
                                fill_sim.on_action(action, ts);
                            }
                        }
                    }
                }

                // Skim-mode: after fills land this tick, check whether net
                // realized P&L (excluding skimmed dollars) crossed the next
                // threshold and convert that chunk to base asset at last_mid.
                if skim_cfg.is_some() && last_mid.0 > Decimal::ZERO {
                    let rep = tracker.report(last_mark);
                    let net_now = rep.realized.0 + rep.funding.0 - rep.fees.0 - skim_total_usdt;
                    let gain = net_now - last_net_seen;
                    if gain > Decimal::ZERO {
                        profit_since_skim += gain;
                    }
                    while profit_since_skim >= skim_threshold && skim_threshold > Decimal::ZERO {
                        let spot_amount = skim_threshold * skim_ratio;
                        if spot_amount > Decimal::ZERO {
                            let btc_bought = spot_amount / last_mid.0;
                            base_stacked += btc_bought;
                            skim_total_usdt += spot_amount;
                        }
                        // Retained piece (chunk × (1 − skim_ratio)) stays in
                        // perp by NOT being subtracted from realized. The
                        // full chunk is consumed from profit_since_skim so we
                        // don't re-trigger on the same gain.
                        skim_count += 1;
                        profit_since_skim -= skim_threshold;
                    }
                    last_net_seen = rep.realized.0 + rep.funding.0 - rep.fees.0 - skim_total_usdt;
                }
                events_processed += 1;

                // Live tap is published EVERY event — cheap try_write on
                // an Arc<RwLock> with one small clone — so dashboards
                // see open-order counts / position / last fill within a
                // frame of the runner observing it. The PaperReport
                // snapshot (with finalize() cost + disk write) stays
                // at the configured cadence below.
                publish_live(
                    &config.live_tap,
                    &tracker,
                    &fill_sim,
                    &symbol,
                    last_mid,
                    &current_book,
                    buy_fills,
                    sell_fills,
                    buy_volume,
                    sell_volume,
                    &last_fill,
                );

                // PaperReport snapshot: fire on the very FIRST event so
                // the dashboard's bot-detail panel populates instantly
                // (otherwise quiet symbols can take 30-60s to hit the
                // first multiple of snapshot_every_n_events). After
                // that, regular interval cadence applies.
                let first_paper_snapshot = events_processed == 1;
                let interval_due = config.snapshot_every_n_events > 0
                    && events_processed.is_multiple_of(config.snapshot_every_n_events as u64);
                let tap_due = config.snapshot_tap.is_some()
                    && last_tap_publish.elapsed() >= TAP_MIN_INTERVAL;
                if first_paper_snapshot || interval_due || tap_due {
                    let mut report = finalize(
                        &tracker,
                        last_mark,
                        started,
                        events_processed,
                        fills_emitted,
                        &risk_gate,
                        first_event_ts,
                        last_event_ts,
                        skim_cfg,
                        skim_count,
                        skim_total_usdt,
                        base_stacked,
                        &symbol,
                        buy_volume,
                        sell_volume,
                        peak_position_usdt,
                        position_usdt_sum,
                        position_samples,
                        full_fills,
                        partial_fills,
                        liq_model.as_ref().map(|m| m.count()).unwrap_or(0),
                fill_rate.peak_per_min,
                rejected_orders,
                    );
                    report.runtime_secs = resumed_runtime_secs.saturating_add(report.runtime_secs);
                    report.sim_duration_secs =
                        resumed_sim_duration_secs.saturating_add(report.sim_duration_secs);
                    // Skip the disk write on the first-event publish —
                    // there's no meaningful state to persist before any
                    // events have accumulated, and we don't want every
                    // bot to spam the filesystem on startup. ALSO skip
                    // disk writes on time-only `tap_due` ticks; only the
                    // event-count cadence (interval_due) earns persistence.
                    if !first_paper_snapshot
                        && interval_due
                        && let Err(e) = state::write_snapshot(&report, &config.state_dir, &run_id)
                    {
                        warn!("snapshot write failed: {}", e);
                    }
                    if let Some(ref tap) = config.snapshot_tap
                        && let Ok(mut guard) = tap.try_write()
                    {
                        *guard = Some(report.clone());
                        last_tap_publish = Instant::now();
                    }
                    // Equity-curve CSV append. Open + header on first
                    // tick; bounded buffered writer so we don't fsync on
                    // every snapshot. Errors are warned but never fatal.
                    if let Some(ref path) = equity_csv_path {
                        if equity_csv_writer.is_none() {
                            match std::fs::File::create(path) {
                                Ok(f) => {
                                    let mut w = std::io::BufWriter::new(f);
                                    use std::io::Write;
                                    let _ = writeln!(
                                        w,
                                        "ts_ns,sim_secs,fills,pos_size,realized,unrealized,fees,funding,net"
                                    );
                                    equity_csv_writer = Some(w);
                                }
                                Err(e) => warn!(
                                    path = %path.display(),
                                    error = %e,
                                    "equity csv open failed; curve disabled for this run"
                                ),
                            }
                        }
                        if let Some(w) = equity_csv_writer.as_mut() {
                            use std::io::Write;
                            let ts_ns = last_event_ts.map(|t| t.0).unwrap_or(0);
                            let pos_size = tracker.snapshot().size.0;
                            let _ = writeln!(
                                w,
                                "{},{},{},{},{},{},{},{},{}",
                                ts_ns,
                                report.sim_duration_secs,
                                report.fills_emitted,
                                pos_size,
                                report.realized.0,
                                report.unrealized.0,
                                report.fees.0,
                                report.funding.0,
                                report.net.0,
                            );
                        }
                    }
                }
            }
            // Live mode: process real exchange fills.
            // When external_fills is None this arm always returns Poll::Pending
            // (the future never resolves), so it never fires in paper mode.
            fill = async {
                match external_fills.as_mut() {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                let Some(fill) = fill else {
                    // Channel closed unexpectedly — either planned
                    // shutdown (cancellable user_stream tasks dropped
                    // the sender) or the WS pump exited mid-session.
                    // Drop out so the supervisor can re-spawn with a
                    // fresh subscription.
                    warn!("external fill channel closed; runner exiting for respawn");
                    break;
                };
                // Dedup vs the REST gap-fill path: if reconciliation already
                // applied this trade id (it beat the WS delivery), skip —
                // applying twice would double-count position + PnL. A real
                // Binance fill always carries a trade id; a `None` here is
                // unexpected, so fall through and apply (legacy behaviour)
                // rather than silently drop a fill.
                if let Some(tid) = fill.trade_id
                    && !trade_dedup.insert(tid, fill.ts.0 / 1_000_000)
                {
                    debug!(
                        trade_id = tid,
                        "WS fill already applied via REST reconciliation; skipping duplicate"
                    );
                    continue;
                }
                let fill_clone = fill.clone();
                let fill_is_full = fill_clone.is_full;
                apply_fill(
                    fill,
                    &mut tracker,
                    &mut risk_gate,
                    &mut fills_emitted,
                    &mut full_fills,
                    &mut partial_fills,
                    &mut buy_fills,
                    &mut sell_fills,
                    &mut buy_volume,
                    &mut sell_volume,
                    &mut fill_rate,                    alert_sink.as_deref(),
                    &symbol,
                )
                .await;
                // A fill is proof the venue is reachable + responsive on
                // both sides. Clear any accumulated per-side failure
                // counter so a prior burst of `-5022` (PostOnly cross)
                // rejections doesn't permanently block the close-side
                // quote from re-placing — the bug that stranded a
                // carried-over short for 59m on 2026-05-24 because the
                // close-side BID had hit `MAX_FAILS_PER_SIDE` and
                // `side_fails` only auto-reset on `CancelAll` (which
                // SS's close-side path never emits).
                side_fails.remove(symbol.base.0.as_ref());
                last_fill = Some(fill_clone.clone());
                if let Some(ref tap) = config.snapshot_tap {
                    let mut report = finalize(
                        &tracker,
                        last_mark,
                        started,
                        events_processed,
                        fills_emitted,
                        &risk_gate,
                        first_event_ts,
                        last_event_ts,
                        skim_cfg,
                        skim_count,
                        skim_total_usdt,
                        base_stacked,
                        &symbol,
                        buy_volume,
                        sell_volume,
                        peak_position_usdt,
                        position_usdt_sum,
                        position_samples,
                        full_fills,
                        partial_fills,
                        liq_model.as_ref().map(|m| m.count()).unwrap_or(0),
                fill_rate.peak_per_min,
                rejected_orders,
                    );
                    report.runtime_secs = resumed_runtime_secs.saturating_add(report.runtime_secs);
                    report.sim_duration_secs =
                        resumed_sim_duration_secs.saturating_add(report.sim_duration_secs);
                    if let Ok(mut guard) = tap.try_write() {
                        *guard = Some(report);
                    }
                }
                // Partial fill: order is still resting on the book, do not
                // drop from FillSim and do not notify the strategy — the
                // strategy should keep waiting for the remaining size.
                if !fill_is_full {
                publish_live(
                    &config.live_tap,
                    &tracker,
                    &fill_sim,
                    &symbol,
                    last_mid,
                    &current_book,
                    buy_fills,
                    sell_fills,
                    buy_volume,
                    sell_volume,
                    &last_fill,
                );
                    continue;
                }
                // Sync FillSim: drop the consumed quote so `live_quotes_for`
                // returns the correct open count for the strategy.
                fill_sim.drop_quote(fill_clone.quote_id);

                // Notify the strategy of its own fill so re-entry / rolling
                // ladder logic fires. Dispatch resulting actions to the venue
                // (Quote => place + record id in fill_sim; Cancel => cancel).
                // Recovery: rejected Quotes are bounced to on_quote_rejected
                // for one recovery round inside dispatch_post_fill_actions.
                let post_fill_pos = tracker.snapshot();
                let post_fill_quotes = fill_sim.live_quotes_for(&symbol);
                let fill_event = MarketEvent::Fill(fill_clone.clone());
                let liqs = liq_window.observe(fill_clone.ts.0);
                let fill_ctx = StrategyContext {
                    symbol: &symbol,
                    now: fill_clone.ts,
                    position: &post_fill_pos,
                    recent_fills: std::slice::from_ref(&fill_clone),
                    latest_book: &current_book,
                    open_quotes: &post_fill_quotes,
                    recent_liqs: liqs,
                };
                let fill_actions = strategy.on_event(&fill_ctx, &fill_event);
                dispatch_post_fill_actions(
                    fill_actions,
                    &venue,
                    &mut fill_sim,
                    &mut strategy,
                    &symbol,
                    fill_clone.ts,
                    &post_fill_pos,
                    &current_book,
                    true,
                    &mut side_fails,
                    config.min_notional,
                    current_max_position,
                    inventory_boost,
                )
                .await;
                // Publish AFTER local mirror has dropped the filled order and
                // recorded the newly-created pair. Publishing earlier showed
                // transient/stale open b/s counts under fill bursts.
                publish_live(
                    &config.live_tap,
                    &tracker,
                    &fill_sim,
                    &symbol,
                    last_mid,
                    &current_book,
                    buy_fills,
                    sell_fills,
                    buy_volume,
                    sell_volume,
                    &last_fill,
                );
            }
            _ = status_tick.tick() => {
                let pos = tracker.snapshot();
                let quotes = fill_sim.live_quotes_for(&symbol);
                let (open_buys, open_sells) = quotes.iter().fold((0u32, 0u32), |(b, s), (_, q)| {
                    match q.side {
                        tikr_core::Side::Bid => (b + 1, s),
                        tikr_core::Side::Ask => (b, s + 1),
                    }
                });
                // Always refresh live_tap + snapshot_tap so the TUI's
                // uptime timer stays live (uptime isn't in the
                // fingerprint dedup below). Without this, idle bots
                // appear frozen because snapshot_tap is otherwise only
                // refreshed on Fill / BookUpdate events.
                publish_live(
                    &config.live_tap,
                    &tracker,
                    &fill_sim,
                    &symbol,
                    last_mid,
                    &current_book,
                    buy_fills,
                    sell_fills,
                    buy_volume,
                    sell_volume,
                    &last_fill,
                );
                if let Some(ref tap) = config.snapshot_tap {
                    let mut heartbeat = finalize(
                        &tracker,
                        last_mark,
                        started,
                        events_processed,
                        fills_emitted,
                        &risk_gate,
                        first_event_ts,
                        last_event_ts,
                        skim_cfg,
                        skim_count,
                        skim_total_usdt,
                        base_stacked,
                        &symbol,
                        buy_volume,
                        sell_volume,
                        peak_position_usdt,
                        position_usdt_sum,
                        position_samples,
                        full_fills,
                        partial_fills,
                        liq_model.as_ref().map(|m| m.count()).unwrap_or(0),
                fill_rate.peak_per_min,
                rejected_orders,
                    );
                    heartbeat.runtime_secs =
                        resumed_runtime_secs.saturating_add(heartbeat.runtime_secs);
                    heartbeat.sim_duration_secs =
                        resumed_sim_duration_secs.saturating_add(heartbeat.sim_duration_secs);
                    if let Ok(mut guard) = tap.try_write() {
                        *guard = Some(heartbeat);
                    }
                }
                let fingerprint = (fills_emitted, open_buys, open_sells, pos.size.0);
                if last_status_fingerprint.as_ref() == Some(&fingerprint) {
                    continue;
                }
                last_status_fingerprint = Some(fingerprint);
                let pnl = tracker.report(last_mark);
                let elapsed = started.elapsed().as_secs() + resumed_runtime_secs;
                let fills_per_min = if elapsed > 0 {
                    (fills_emitted as f64) * 60.0 / (elapsed as f64)
                } else {
                    0.0
                };
                let base_value = pos.size.0 * last_mid.0;
                let acct = if let Some(sc) = skim_cfg.as_ref() {
                    sc.budget + pnl.net.0 + skim_total_usdt + base_stacked * last_mid.0
                } else {
                    pnl.net.0
                };
                info!(
                    target: "tikr_paper::status",
                    symbol = %symbol.base.0,
                    runtime_s = elapsed,
                    fills = fills_emitted,
                    buy = buy_fills,
                    sell = sell_fills,
                    fpm = format!("{:.1}", fills_per_min),
                    open_b = open_buys,
                    open_s = open_sells,
                    pos = %pos.size.0,
                    base_usdt = %base_value.round_dp(2),
                    last = %last_mid.0,
                    realized = %pnl.realized.0.round_dp(4),
                    fees = %pnl.fees.0.round_dp(4),
                    mtm = %pnl.net.0.round_dp(4),
                    acct = %acct.round_dp(4),
                    skims = skim_count,
                    skim_usd = %skim_total_usdt.round_dp(2),
                    base_stk = %base_stacked.round_dp(6),
                    "status"
                );
            }
            _ = recon_tick.tick(), if live_mode => {
                // ── 1) REST gap-fill: replay fills the WS stream missed. ──────
                // Authoritative source of truth for missed executions. Each
                // fetched trade carries its venue trade id; the dedup set skips
                // the ones already applied (WS or a prior tick), so only the
                // genuinely-missed fills are replayed — through the SAME
                // apply_fill path as live fills, preserving realized PnL, fees,
                // and fill counts. This is what makes a WS gap fully
                // attributable, unlike the force_reconcile fallback below which
                // only snaps the net size. The resting-order side is handled by
                // the open_orders reconciliation further down (a filled order's
                // FillSim ghost is dropped there).
                match venue.fills_since(&symbol, reconcile_from_ns).await {
                    Ok(trades) => {
                        let mut replayed = 0u32;
                        let mut max_ts_ms = reconcile_from_ns / 1_000_000;
                        for fill in trades {
                            let ts_ms = fill.ts.0 / 1_000_000;
                            if ts_ms > max_ts_ms {
                                max_ts_ms = ts_ms;
                            }
                            match fill.trade_id {
                                // Already applied (WS delivered it, or an
                                // earlier reconciliation tick) — skip.
                                Some(id) if !trade_dedup.insert(id, ts_ms) => continue,
                                // No trade id: can't dedup safely against the WS
                                // stream, so skip rather than risk double-apply.
                                None => continue,
                                Some(_) => {}
                            }
                            apply_fill(
                                fill,
                                &mut tracker,
                                &mut risk_gate,
                                &mut fills_emitted,
                                &mut full_fills,
                                &mut partial_fills,
                                &mut buy_fills,
                                &mut sell_fills,
                                &mut buy_volume,
                                &mut sell_volume,
                                &mut fill_rate,                                alert_sink.as_deref(),
                                &symbol,
                            )
                            .await;
                            replayed += 1;
                        }
                        if replayed > 0 {
                            warn!(
                                replayed,
                                "fill reconciliation: replayed missed fills from REST userTrades (WS gap) with full PnL/fee attribution"
                            );
                        }
                        // Advance the look-back window + prune the dedup set,
                        // keeping a RECONCILE_LOOKBACK overlap so a boundary-
                        // straddling / out-of-order fill is still caught next
                        // tick.
                        let lookback_ms = RECONCILE_LOOKBACK.as_millis() as u64;
                        // Advance the window to (newest seen − lookback), but
                        // NEVER regress: on idle ticks `max_ts_ms` is stale, so
                        // an unconditional subtract would walk the window
                        // backward every tick and re-fetch an ever-growing
                        // range. Clamp to the current start to keep it
                        // monotonic while preserving the look-back overlap.
                        let cur_from_ms = reconcile_from_ns / 1_000_000;
                        let new_from_ms = cur_from_ms.max(max_ts_ms.saturating_sub(lookback_ms));
                        reconcile_from_ns = new_from_ms.saturating_mul(1_000_000);
                        trade_dedup.prune(new_from_ms);
                    }
                    Err(e) => {
                        warn!(error = ?e, "fill reconciliation: fills_since failed; will retry next tick")
                    }
                }

                // ── 2) Position-drift fallback: ground-truth via venue.position.
                // After the REST replay above, any remaining drift is a gap the
                // trade history could NOT explain (e.g. a brief resume window,
                // or a venue-side adjustment). Snap the net size as a last
                // resort — PnL for an unexplained gap stays unattributable, but
                // the explained common case (WS missed fills) is now handled by
                // step 1 with full attribution.
                match venue.position(&symbol).await {
                    Ok(venue_pos) => {
                        let tracker_size = tracker.snapshot().size.0;
                        let venue_size = venue_pos.size.0;
                        let drift = (tracker_size - venue_size).abs();
                        // Float-noise threshold. Real drift (missed fill) is
                        // always much larger than this.
                        let threshold = Decimal::from_str_exact("0.00000001").unwrap();
                        if drift > threshold {
                            warn!(
                                tracker_size = %tracker_size,
                                venue_size = %venue_size,
                                drift = %drift,
                                "position drift persists after REST trade replay — unexplained gap; force-reconciling tracker size to venue as last resort (PnL for this residual gap is not attributable)"
                            );
                            tracker.force_reconcile(
                                SignedSize(venue_size),
                                venue_pos.avg_entry,
                            );
                        }
                    }
                    Err(e) => warn!(error = ?e, "position drift check: venue.position failed"),
                }
                // Order reconciliation: ground-truth via venue.open_orders.
                // Drop any FillSim ghosts (silent cancel / expiry / lost WS
                // events). One REST call every 30 s per bot — cheap relative
                // to event rate.
                match venue.open_orders(&symbol).await {
                    Ok(orders) => {
                        let venue_open = orders.len();
                        // Orphan sweep: when the strategy's expected max
                        // open-order count is set and exceeded, wipe +
                        // let strategy re-emit on next event. Disabled
                        // (max=0) for grid strategies that deliberately
                        // keep many resting orders.
                        if config.max_expected_open_orders > 0
                            && venue_open > config.max_expected_open_orders
                        {
                            warn!(
                                venue_open,
                                max_expected = config.max_expected_open_orders,
                                "order reconciliation: open count > max_expected, cancelling all"
                            );
                            if let Err(e) = venue.cancel_all(&symbol).await {
                                warn!(error = ?e, "orphan sweep: cancel_all failed");
                            } else {
                                fill_sim.drop_quotes_for(&symbol);
                            }
                        }
                        let (removed, added) = fill_sim.reconcile_quotes_for(&symbol, &orders);
                        if removed > 0 || added > 0 {
                            warn!(
                                ghosts = removed,
                                missing = added,
                                venue_open,
                                "order reconciliation: synced FillSim mirror to venue"
                            );
                publish_live(
                    &config.live_tap,
                    &tracker,
                    &fill_sim,
                    &symbol,
                    last_mid,
                    &current_book,
                    buy_fills,
                    sell_fills,
                    buy_volume,
                    sell_volume,
                    &last_fill,
                );
                        }
                    }
                    Err(e) => {
                        warn!(error = ?e, "order reconciliation: venue.open_orders failed");
                    }
                }
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("shutdown signal received");
                    break;
                }
            }
        }
    }

    // Drain pending FillSim actions and emit strategy on_shutdown actions.
    // Shutdown actions skip the risk gate — cancel-all is always allowed.
    let pos = tracker.snapshot();
    let now_ts = current_book.ts;
    let shutdown_quotes = fill_sim.live_quotes_for(&symbol);
    let liqs = liq_window.observe(now_ts.0);
    let ctx = StrategyContext {
        symbol: &symbol,
        now: now_ts,
        position: &pos,
        recent_fills: &[],
        latest_book: &current_book,
        open_quotes: &shutdown_quotes,
        recent_liqs: liqs,
    };
    let shutdown_actions = strategy.on_shutdown(&ctx);
    for action in shutdown_actions {
        fill_sim.on_action(action, now_ts);
    }

    let mut report = finalize(
        &tracker,
        last_mark,
        started,
        events_processed,
        fills_emitted,
        &risk_gate,
        first_event_ts,
        last_event_ts,
        skim_cfg,
        skim_count,
        skim_total_usdt,
        base_stacked,
        &symbol,
        buy_volume,
        sell_volume,
        peak_position_usdt,
        position_usdt_sum,
        position_samples,
        full_fills,
        partial_fills,
        liq_model.as_ref().map(|m| m.count()).unwrap_or(0),
        fill_rate.peak_per_min,
        rejected_orders,
    );
    report.runtime_secs = resumed_runtime_secs.saturating_add(report.runtime_secs);
    report.sim_duration_secs = resumed_sim_duration_secs.saturating_add(report.sim_duration_secs);
    if let Err(e) = state::write_snapshot(&report, &config.state_dir, &run_id) {
        warn!("final snapshot write failed: {}", e);
    }
    if let Some(ref tap) = config.snapshot_tap
        && let Ok(mut guard) = tap.try_write()
    {
        *guard = Some(report.clone());
    }
    info!(
        runtime_secs = report.runtime_secs,
        events = events_processed,
        fills = fills_emitted,
        net = %report.net.0,
        "paper runner done"
    );
    report
}

/// Best-effort valuation price (top-of-book mid) from a snapshot, for position
/// notional checks. `None` when either side is empty.
fn snapshot_mid(book: &tikr_core::Snapshot) -> Option<Decimal> {
    let bid = book.bids.first()?.price.0;
    let ask = book.asks.first()?.price.0;
    Some((bid + ask) / Decimal::from(2))
}

/// Bot-side worst-case inventory cap, applied at order placement (the same
/// shape as the exchange margin check, but at the bot's own wallet limit
/// rather than `balance × leverage`). For each add-side Quote, the worst-case
/// fill is `current position + all resting same-side notional + everything
/// accepted earlier in this batch + this order`. Drop the order if that would
/// breach `cap`; reducing-side orders and Cancel/Requote/CancelAll always
/// pass. Checking worst-case (not just current position) is what bounds the
/// PEAK — otherwise a flat strategy can pre-place a deep ladder that later
/// sweeps far past the cap before any single order looks over-limit.
///
/// `signed_pos_notional` is the current signed position notional (+ = long);
/// `resting_bid`/`resting_ask` are the summed notionals of orders already
/// resting on each side. A non-positive `cap` disables the gate.
fn apply_bot_inventory_cap(
    actions: Vec<tikr_strategy::Action>,
    signed_pos_notional: Decimal,
    mut resting_bid: Decimal,
    mut resting_ask: Decimal,
    cap: Decimal,
) -> Vec<tikr_strategy::Action> {
    if cap <= Decimal::ZERO {
        return actions;
    }
    let mut out = Vec::with_capacity(actions.len());
    for action in actions {
        if let tikr_strategy::Action::Quote(intent) = &action {
            let n = intent.price.0 * intent.size.0;
            let breaches = match intent.side {
                // Worst case: all bids fill → long grows by resting_bids + n.
                Side::Bid => signed_pos_notional + resting_bid + n > cap,
                // Worst case: all asks fill → short grows → must stay > -cap.
                Side::Ask => signed_pos_notional - resting_ask - n < -cap,
            };
            if breaches {
                continue;
            }
            match intent.side {
                Side::Bid => resting_bid += n,
                Side::Ask => resting_ask += n,
            }
        }
        out.push(action);
    }
    out
}

/// Scale the inventory-*reducing* side's Quote sizes up on a curve as the
/// signed position approaches the per-bot cap, leaving the growing side and
/// all non-Quote actions untouched. See [`InventoryBoostConfig`].
///
/// `signed_pos_notional` is the current signed position notional (+ = long);
/// `cap` is the per-bot position cap (the same value fed to
/// [`apply_bot_inventory_cap`]). A non-positive cap or `max_boost_pct`
/// disables the boost. The boosted size is rounded back to the original
/// size's decimal scale to stay on the venue lot grid, and never shrinks a
/// quote below its original size.
fn apply_inventory_size_boost(
    actions: Vec<tikr_strategy::Action>,
    signed_pos_notional: Decimal,
    cap: Decimal,
    cfg: InventoryBoostConfig,
) -> Vec<tikr_strategy::Action> {
    if cfg.max_boost_pct <= Decimal::ZERO || cap <= Decimal::ZERO {
        return actions;
    }
    // Reducing side: a long (+) is reduced by Ask sells; a short (-) by Bid
    // buys. Flat → nothing to reduce, pass through.
    let reducing_side = match signed_pos_notional.cmp(&Decimal::ZERO) {
        std::cmp::Ordering::Greater => Side::Ask,
        std::cmp::Ordering::Less => Side::Bid,
        std::cmp::Ordering::Equal => return actions,
    };
    let ratio = (signed_pos_notional.abs() / cap).min(Decimal::ONE);
    let exponent = if cfg.curve_exponent > Decimal::ZERO {
        cfg.curve_exponent
    } else {
        Decimal::ONE
    };
    let curved = if exponent == Decimal::ONE {
        ratio
    } else {
        ratio.powd(exponent)
    };
    let mult = Decimal::ONE + (cfg.max_boost_pct / Decimal::from(100)) * curved;
    if mult <= Decimal::ONE {
        return actions;
    }
    actions
        .into_iter()
        .map(|action| {
            if let tikr_strategy::Action::Quote(mut intent) = action {
                if intent.side == reducing_side {
                    let scale = intent.size.0.scale();
                    let boosted = (intent.size.0 * mult).round_dp(scale);
                    if boosted > intent.size.0 {
                        intent.size = tikr_core::Size(boosted);
                    }
                }
                tikr_strategy::Action::Quote(intent)
            } else {
                action
            }
        })
        .collect()
}

/// Dispatch a list of post-fill strategy actions into `fill_sim` and,
/// optionally, the live venue.
///
/// **Paper mode** (`live_mode == false`): every action is fed directly to
/// `fill_sim.on_action`; no venue calls are made.
///
/// **Live mode** (`live_mode == true`): Quote actions are submitted to the
/// venue and the returned `QuoteId` is threaded back into `fill_sim` via
/// `enqueue_place_with_id`. Requote/Cancel/CancelAll also hit the venue.
/// Any Quote that the venue rejects is collected and handed to
/// `strategy.on_quote_rejected` for one recovery round; if the recovery
/// quotes also fail, the error is logged and we move on (no recursion).
#[allow(clippy::too_many_arguments)]
async fn dispatch_post_fill_actions<V, S>(
    actions: Vec<tikr_strategy::Action>,
    venue: &V,
    fill_sim: &mut FillSim,
    strategy: &mut S,
    symbol: &Symbol,
    ts: Timestamp,
    post_fill_pos: &tikr_core::Position,
    current_book: &tikr_core::Snapshot,
    live_mode: bool,
    side_fails: &mut HashMap<String, (u32, u32)>,
    min_notional: Decimal,
    max_position: Decimal,
    inventory_boost: Option<InventoryBoostConfig>,
) where
    V: Venue,
    S: Strategy,
{
    // Inventory-aware order-size boost + bot-side worst-case inventory cap:
    // SimpleGap/MMR-style refills flow through this path, so apply the same
    // size boost (reducing side) and order-placement gate as the main loop.
    // Resting same-side exposure comes from FillSim's live quotes; sum it
    // before dispatching (drops the borrow before the `&mut fill_sim` use
    // below).
    let actions: Vec<tikr_strategy::Action> = match snapshot_mid(current_book) {
        Some(mid) if max_position > Decimal::ZERO => {
            let signed_pos_notional = post_fill_pos.size.0 * mid;
            let actions = match inventory_boost {
                Some(boost) => {
                    apply_inventory_size_boost(actions, signed_pos_notional, max_position, boost)
                }
                None => actions,
            };
            let (resting_bid, resting_ask) = fill_sim.committed_notional_by_side(symbol);
            apply_bot_inventory_cap(
                actions,
                signed_pos_notional,
                resting_bid,
                resting_ask,
                max_position,
            )
        }
        _ => actions,
    };
    if !live_mode {
        // Paper mode: pipe all actions straight into FillSim.
        for action in actions {
            fill_sim.on_action(action, ts);
        }
        return;
    }

    // Live mode: dispatch to venue; track rejected Quote intents for recovery.
    let mut rejected_intents: Vec<(QuoteIntent, String)> = Vec::new();
    for action in actions {
        match &action {
            tikr_strategy::Action::Quote(intent) => {
                let state = side_fails
                    .entry(symbol.base.0.to_string())
                    .or_insert((0, 0));
                let skip = match intent.side {
                    Side::Bid => state.0 >= MAX_FAILS_PER_SIDE,
                    Side::Ask => state.1 >= MAX_FAILS_PER_SIDE,
                };
                if skip {
                    warn!(
                        side = ?intent.side, bid_fails = state.0, ask_fails = state.1,
                        "live: skipping post-fill quote — side exceeded max failures"
                    );
                    continue;
                }
                if min_notional > Decimal::ZERO && intent.size.0 * intent.price.0 < min_notional {
                    debug!(
                        side = ?intent.side,
                        qty = %intent.size.0,
                        price = %intent.price.0,
                        notional = %(intent.size.0 * intent.price.0),
                        min = %min_notional,
                        "live: dropping sub-min-notional quote (post-fill)"
                    );
                    continue;
                }
                match venue.quote(intent.clone()).await {
                    Ok(qid) => {
                        info!(
                            side = ?intent.side, price = %intent.price.0, size = %intent.size.0,
                            quote_id = ?qid, "live: order placed (post-fill)"
                        );
                        match intent.side {
                            Side::Bid => state.0 = 0,
                            Side::Ask => state.1 = 0,
                        }
                        fill_sim.enqueue_place_with_id(intent.clone(), ts, qid);
                    }
                    Err(e) => {
                        let msg = format!("{e:?}");
                        let is_transient = msg.contains("-5022")
                            || msg.contains("Post Only")
                            || msg.contains("RateLimited")
                            || msg.contains("-4400")
                            || msg.contains("Quantitative Rules");
                        warn!(error = ?e, "live: venue.quote failed (post-fill)");
                        if !is_transient {
                            match intent.side {
                                Side::Bid => state.0 += 1,
                                Side::Ask => state.1 += 1,
                            }
                        }
                        rejected_intents.push((intent.clone(), msg));
                    }
                }
            }
            tikr_strategy::Action::Requote { id, intent } => {
                if let Err(e) = venue.requote(*id, intent.clone()).await {
                    warn!(error = ?e, "live: venue.requote failed (post-fill)");
                }
            }
            tikr_strategy::Action::Cancel(id) => match venue.cancel(*id).await {
                Ok(()) => fill_sim.drop_quote(*id),
                Err(e) => warn!(error = ?e, "live: venue.cancel failed (post-fill)"),
            },
            tikr_strategy::Action::CancelAll => {
                side_fails.remove(symbol.base.0.as_ref());
                match venue.cancel_all(symbol).await {
                    Ok(()) => fill_sim.drop_quotes_for(symbol),
                    Err(e) => warn!(error = ?e, "live: venue.cancel_all failed (post-fill)"),
                }
            }
            tikr_strategy::Action::NoOp => {}
        }
    }

    // Recovery: any Quote actions the venue rejected get bounced back to
    // the strategy via `on_quote_rejected`. The strategy typically
    // re-anchors on current book mid and emits a fresh pair. We iterate
    // until no Quote actions remain rejected or `MAX_RECOVERY_ROUNDS` is
    // hit (defensive cap against an infinite reject loop in a fast move).
    // Raised from 5 to 20 — at offset=0 on a 1-tick-spread book, the
    // post-only race against book moves is the dominant rejection mode
    // and the bot needs to chase the touch through 10+ price ticks
    // before giving up. Each round refetches the book.
    const MAX_RECOVERY_ROUNDS: usize = 20;
    let mut round = 0;
    while !rejected_intents.is_empty() && round < MAX_RECOVERY_ROUNDS {
        round += 1;
        // Refresh the book BEFORE each recovery round. The cached
        // `current_book` was last updated whenever the runner saw a
        // `MarketEvent::BookUpdate` — by the time on_quote_rejected
        // fires, that snapshot is often hundreds of ms stale (which
        // is exactly what caused the reject). Pull a fresh top-of-book
        // from the venue so the strategy's mid is current.
        let fresh_book = match venue.snapshot(symbol).await {
            Ok(s) => s,
            Err(e) => {
                warn!(error = ?e, round, "live: venue.snapshot failed (recovery) — using stale book");
                current_book.clone()
            }
        };
        let pending = std::mem::take(&mut rejected_intents);
        for (rej_intent, rej_reason) in pending {
            let recovery_quotes = fill_sim.live_quotes_for(symbol);
            let rec_ctx = StrategyContext {
                symbol,
                now: ts,
                position: post_fill_pos,
                recent_fills: &[],
                latest_book: &fresh_book,
                open_quotes: &recovery_quotes,
                recent_liqs: &[],
            };
            let recovery_actions = strategy.on_quote_rejected(&rec_ctx, &rej_intent, &rej_reason);
            for action in recovery_actions {
                match &action {
                    tikr_strategy::Action::Quote(intent) => {
                        let state = side_fails
                            .entry(symbol.base.0.to_string())
                            .or_insert((0, 0));
                        let skip = match intent.side {
                            Side::Bid => state.0 >= MAX_FAILS_PER_SIDE,
                            Side::Ask => state.1 >= MAX_FAILS_PER_SIDE,
                        };
                        if skip {
                            warn!(
                                side = ?intent.side, bid_fails = state.0, ask_fails = state.1, round,
                                "live: skipping recovery quote — side exceeded max failures"
                            );
                            continue;
                        }
                        if min_notional > Decimal::ZERO
                            && intent.size.0 * intent.price.0 < min_notional
                        {
                            debug!(
                                side = ?intent.side,
                                qty = %intent.size.0,
                                price = %intent.price.0,
                                notional = %(intent.size.0 * intent.price.0),
                                min = %min_notional,
                                round,
                                "live: dropping sub-min-notional quote (recovery)"
                            );
                            continue;
                        }
                        match venue.quote(intent.clone()).await {
                            Ok(qid) => {
                                info!(
                                    side = ?intent.side, price = %intent.price.0, size = %intent.size.0,
                                    quote_id = ?qid, round, "live: order placed (recovery)"
                                );
                                match intent.side {
                                    Side::Bid => state.0 = 0,
                                    Side::Ask => state.1 = 0,
                                }
                                fill_sim.enqueue_place_with_id(intent.clone(), ts, qid);
                            }
                            Err(e) => {
                                let msg = format!("{e:?}");
                                let is_transient = msg.contains("-5022")
                                    || msg.contains("Post Only")
                                    || msg.contains("RateLimited")
                                    || msg.contains("-4400")
                                    || msg.contains("Quantitative Rules");
                                warn!(error = ?e, round, "live: venue.quote failed (recovery)");
                                if !is_transient {
                                    match intent.side {
                                        Side::Bid => state.0 += 1,
                                        Side::Ask => state.1 += 1,
                                    }
                                }
                                rejected_intents.push((intent.clone(), msg));
                            }
                        }
                    }
                    tikr_strategy::Action::Cancel(id) => match venue.cancel(*id).await {
                        Ok(()) => fill_sim.drop_quote(*id),
                        Err(e) => warn!(error = ?e, round, "live: venue.cancel failed (recovery)"),
                    },
                    tikr_strategy::Action::CancelAll => {
                        side_fails.remove(symbol.base.0.as_ref());
                        match venue.cancel_all(symbol).await {
                            Ok(()) => fill_sim.drop_quotes_for(symbol),
                            Err(e) => {
                                warn!(error = ?e, round, "live: venue.cancel_all failed (recovery)")
                            }
                        }
                    }
                    tikr_strategy::Action::Requote { .. } | tikr_strategy::Action::NoOp => {}
                }
            }
        }
    }
    if !rejected_intents.is_empty() {
        warn!(
            remaining = rejected_intents.len(),
            rounds = MAX_RECOVERY_ROUNDS,
            "recovery cap hit — some sides may be empty on the book"
        );
    }
}

/// Tracks the peak fills-per-minute via a 60s sliding window over fill
/// sim-timestamps (nanoseconds). The average rate (`fills_emitted` /
/// `sim_duration_secs`) hides bursts; this exposes the worst-case minute.
#[derive(Default)]
struct FillRateTracker {
    /// Fill timestamps (ns) within the trailing 60s window, oldest first.
    window: std::collections::VecDeque<u64>,
    /// Largest window size seen = peak fills in any 60s span.
    peak_per_min: u64,
}

impl FillRateTracker {
    /// Record a fill at `ts_ns` and update the running peak.
    fn observe(&mut self, ts_ns: u64) {
        const WINDOW_NS: u64 = 60 * 1_000_000_000;
        self.window.push_back(ts_ns);
        let cutoff = ts_ns.saturating_sub(WINDOW_NS);
        while let Some(&front) = self.window.front() {
            if front < cutoff {
                self.window.pop_front();
            } else {
                break;
            }
        }
        let n = self.window.len() as u64;
        if n > self.peak_per_min {
            self.peak_per_min = n;
        }
    }
}

/// Apply a fill to the tracker, update the risk gate, emit alerts.
///
/// Shared by paper mode (FillSim-synthesized fills) and live mode (external
/// venue fills). Keeping this as a standalone async fn avoids code duplication
/// in the two `select!` arms.
#[allow(clippy::too_many_arguments)]
async fn apply_fill(
    fill: Fill,
    tracker: &mut PositionTracker,
    risk_gate: &mut Option<Box<dyn RiskGate>>,
    fills_emitted: &mut u64,
    full_fills: &mut u64,
    partial_fills: &mut u64,
    buy_fills: &mut u64,
    sell_fills: &mut u64,
    buy_volume: &mut Decimal,
    sell_volume: &mut Decimal,
    fill_rate: &mut FillRateTracker,
    alert_sink: Option<&dyn AlertSink>,
    symbol: &Symbol,
) {
    info!(
        price = %fill.price.0,
        size = %fill.size.0,
        side = ?fill.side,
        fee_asset = %fill.fee_asset.0,
        fee_amount = %fill.fee_amount,
        fee_quote = %fill.fee_quote.0,
        "fill"
    );
    tracker.apply(&fill);
    if let Some(gate) = risk_gate.as_mut() {
        gate.record_fill(fill.ts);
    }
    // Display/report fill counters track every execution, including partials.
    *fills_emitted += 1;
    fill_rate.observe(fill.ts.0);
    if fill.is_full {
        *full_fills += 1;
    } else {
        *partial_fills += 1;
    }
    match fill.side {
        tikr_core::Side::Bid => {
            *buy_fills += 1;
            *buy_volume += fill.price.0 * fill.size.0;
        }
        tikr_core::Side::Ask => {
            *sell_fills += 1;
            *sell_volume += fill.price.0 * fill.size.0;
        }
    }
    if let Some(sink) = alert_sink {
        let _ = sink
            .send(Alert::Fill {
                quote_id: fill.quote_id,
                price: fill.price,
                size: fill.size,
                side: fill.side,
                symbol: symbol.clone(),
            })
            .await;
    }
}

#[allow(clippy::too_many_arguments)]
fn finalize(
    tracker: &PositionTracker,
    last_mark: Price,
    started: Instant,
    events_processed: u64,
    fills_emitted: u64,
    risk_gate: &Option<Box<dyn RiskGate>>,
    first_event_ts: Option<Timestamp>,
    last_event_ts: Option<Timestamp>,
    skim_cfg: Option<SkimConfig>,
    skim_count: u64,
    skim_total_usdt: Decimal,
    base_stacked: Decimal,
    symbol: &Symbol,
    buy_volume: Decimal,
    sell_volume: Decimal,
    peak_position_usdt: Decimal,
    position_usdt_sum: Decimal,
    position_samples: u64,
    full_fills: u64,
    partial_fills: u64,
    liquidations: u64,
    peak_fills_per_min: u64,
    rejected_orders: u64,
) -> PaperReport {
    let base = tracker.report(last_mark);
    let sim_duration_secs = match (first_event_ts, last_event_ts) {
        (Some(a), Some(b)) if b.0 >= a.0 => (b.0 - a.0) / 1_000_000_000,
        _ => 0,
    };
    let final_perp_balance = match skim_cfg {
        Some(sc) => {
            sc.budget + base.realized.0 + base.funding.0 - base.fees.0 - skim_total_usdt
                + base.unrealized.0
        }
        None => Decimal::ZERO,
    };
    let final_base_value = base_stacked * last_mark.0;
    let mean_position_usdt = if position_samples > 0 {
        (position_usdt_sum / Decimal::from(position_samples)).round_dp(8)
    } else {
        Decimal::ZERO
    };
    PaperReport {
        schema_version: SCHEMA_VERSION,
        realized: base.realized,
        unrealized: base.unrealized,
        fees: base.fees,
        funding: base.funding,
        net: base.net,
        runtime_secs: started.elapsed().as_secs(),
        sim_duration_secs,
        events_processed,
        fills_emitted,
        risk_state: risk_gate.as_ref().map(|g| g.state().clone()),
        skim_count,
        skim_total_usdt: Notional(skim_total_usdt),
        base_stacked: Notional(base_stacked),
        final_perp_balance: Notional(final_perp_balance),
        final_base_value: Notional(final_base_value),
        base_asset: if skim_cfg.is_some() {
            symbol.base.0.as_ref().to_string()
        } else {
            String::new()
        },
        buy_volume_usdt: Notional(buy_volume),
        sell_volume_usdt: Notional(sell_volume),
        peak_position_usdt: Notional(peak_position_usdt),
        mean_position_usdt: Notional(mean_position_usdt),
        full_fills,
        partial_fills,
        liquidations,
        peak_fills_per_min,
        rejected_orders,
    }
}

fn empty_snapshot(symbol: &Symbol) -> Snapshot {
    Snapshot {
        symbol: symbol.clone(),
        bids: Vec::new(),
        asks: Vec::new(),
        ts: Timestamp(0),
    }
}

/// Publish a fresh [`LiveSnapshot`] into `tap` if present.
///
/// Called on every fill and at every regular snapshot tick so dashboards
/// see position + open-order + last-fill state with sub-second latency.
#[allow(clippy::too_many_arguments)]
fn publish_live(
    tap: &Option<std::sync::Arc<std::sync::RwLock<Option<LiveSnapshot>>>>,
    tracker: &PositionTracker,
    fill_sim: &FillSim,
    symbol: &Symbol,
    last_mid: Price,
    last_book: &Snapshot,
    buy_fills: u64,
    sell_fills: u64,
    buy_volume: Decimal,
    sell_volume: Decimal,
    last_fill: &Option<Fill>,
) {
    let Some(tap) = tap.as_ref() else {
        return;
    };
    let pos = tracker.snapshot();
    let quotes = fill_sim.live_quotes_for(symbol);
    let mut open_buys: u32 = 0;
    let mut open_sells: u32 = 0;
    for (_, q) in &quotes {
        match q.side {
            tikr_core::Side::Bid => open_buys = open_buys.saturating_add(1),
            tikr_core::Side::Ask => open_sells = open_sells.saturating_add(1),
        }
    }
    let last_bid = last_book
        .bids
        .first()
        .map(|l| l.price.0)
        .unwrap_or_default();
    let last_ask = last_book
        .asks
        .first()
        .map(|l| l.price.0)
        .unwrap_or_default();
    let snap = LiveSnapshot {
        position_size: pos.size.0,
        avg_entry: pos.avg_entry.0,
        last_mid: last_mid.0,
        last_bid,
        last_ask,
        buy_fills,
        sell_fills,
        buy_volume,
        sell_volume,
        open_quotes: open_buys.saturating_add(open_sells),
        open_buys,
        open_sells,
        last_fill_ts: last_fill.as_ref().map(|f| f.ts.0),
        last_fill_side: last_fill.as_ref().map(|f| f.side),
        last_fill_price: last_fill.as_ref().map(|f| f.price.0).unwrap_or_default(),
        last_fill_size: last_fill.as_ref().map(|f| f.size.0).unwrap_or_default(),
        inventory_usdt: pos.size.0 * last_mid.0,
    };
    // Non-blocking: dashboard reader can be holding the read lock
    // during a draw; the next fill / snapshot tick will refresh.
    if let Ok(mut guard) = tap.try_write() {
        *guard = Some(snap);
    }
}

fn event_ts(event: &MarketEvent) -> Timestamp {
    match event {
        MarketEvent::BookUpdate { snapshot } => snapshot.ts,
        MarketEvent::Trade { ts, .. } => *ts,
        MarketEvent::Fill(f) => f.ts,
        MarketEvent::Heartbeat { ts } => *ts,
    }
}

fn make_run_id(symbol: &Symbol) -> String {
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let uuid_short = Uuid::new_v4().simple().to_string();
    let uuid_short = &uuid_short[..8];
    let base = symbol.base.0.to_string();
    format!("{base}_{now_secs}_{uuid_short}")
}

// --- mock Venue + tests below ---

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trade_dedup_applies_each_id_once() {
        let mut d = TradeDedup::default();
        // First sight of a trade id is "new"; a repeat is a duplicate.
        assert!(d.insert(100, 1_000), "first insert is new");
        assert!(!d.insert(100, 1_000), "repeat is a duplicate");
        assert!(d.insert(101, 1_010), "different id is new");
        // Models the WS-then-REST race: REST re-offers 100 → must be skipped.
        assert!(!d.insert(100, 1_000));
    }

    #[test]
    fn trade_dedup_prune_bounds_memory_and_forgets_old() {
        let mut d = TradeDedup::default();
        d.insert(1, 1_000);
        d.insert(2, 2_000);
        d.insert(3, 3_000);
        // Prune everything observed strictly before ts=3000.
        d.prune(3_000);
        assert_eq!(d.order.len(), 1, "only the ts=3000 entry survives");
        assert!(d.seen.contains(&3));
        assert!(!d.seen.contains(&1));
        assert!(!d.seen.contains(&2));
        // A pruned id is treated as new again (acceptable: it's far outside the
        // reconciliation look-back window, so it can never be re-offered).
        assert!(d.insert(1, 4_000));
    }

    use crate::multi::{MultiSymbolRun, run_multi};
    use async_trait::async_trait;
    use futures::stream::{self, BoxStream};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;
    use tikr_backtest::fill_sim::{FillSim, FillSimConfig, VenueFees};
    use tikr_core::{Asset, Level, MarketKind, Notional, Size, VenueId};
    use tikr_strategy::{LayeredGrid, LayeredGridConfig, Strategy};
    use tikr_venue::{QuoteId, QuoteIntent, Venue, VenueError};
    use tokio::sync::watch;

    fn cap_test_symbol() -> Symbol {
        Symbol {
            base: Asset::new("SOL"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("test"),
            kind: MarketKind::Perp,
        }
    }

    fn cap_quote(side: Side, notional: i64) -> tikr_strategy::Action {
        // price 100 × size = notional → size = notional / 100.
        tikr_strategy::Action::Quote(QuoteIntent {
            symbol: cap_test_symbol(),
            side,
            price: Price(Decimal::from(100)),
            size: Size(Decimal::from(notional) / Decimal::from(100)),
            tif: tikr_core::TimeInForce::PostOnly,
            kind: tikr_core::QuoteKind::Point,
        })
    }

    fn count_quotes(actions: &[tikr_strategy::Action]) -> usize {
        actions
            .iter()
            .filter(|a| matches!(a, tikr_strategy::Action::Quote(_)))
            .count()
    }

    #[test]
    fn bot_inventory_cap_bounds_worst_case_batch() {
        // Flat, no resting, cap 600: ten $100 bids → only 6 survive
        // (6×100 = 600; the 7th would push worst-case to 700 > 600).
        let actions: Vec<_> = (0..10).map(|_| cap_quote(Side::Bid, 100)).collect();
        let out = apply_bot_inventory_cap(
            actions,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::from(600),
        );
        assert_eq!(count_quotes(&out), 6);
    }

    #[test]
    fn bot_inventory_cap_counts_resting_and_position() {
        // Already long $400 with $150 resting bids: only $50 of new bid
        // headroom remains (400 + 150 + 50 = 600), so one $100 bid is blocked
        // but a $100 ASK (reducing) always passes.
        let actions = vec![cap_quote(Side::Bid, 100), cap_quote(Side::Ask, 100)];
        let out = apply_bot_inventory_cap(
            actions,
            Decimal::from(400),
            Decimal::from(150),
            Decimal::ZERO,
            Decimal::from(600),
        );
        assert_eq!(count_quotes(&out), 1);
        assert!(matches!(
            out[0],
            tikr_strategy::Action::Quote(ref q) if q.side == Side::Ask
        ));
    }

    #[test]
    fn bot_inventory_cap_disabled_when_zero() {
        let actions: Vec<_> = (0..10).map(|_| cap_quote(Side::Bid, 100)).collect();
        let out = apply_bot_inventory_cap(
            actions,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
        );
        assert_eq!(count_quotes(&out), 10);
    }

    fn quote_size(action: &tikr_strategy::Action) -> Decimal {
        match action {
            tikr_strategy::Action::Quote(q) => q.size.0,
            _ => Decimal::ZERO,
        }
    }

    // Quote with an explicit scale-3 lot size (1.000), so the boost's
    // round-to-original-scale leaves it on a 0.001 grid (realistic lot step).
    fn boost_quote(side: Side) -> tikr_strategy::Action {
        tikr_strategy::Action::Quote(QuoteIntent {
            symbol: cap_test_symbol(),
            side,
            price: Price(Decimal::from(100)),
            size: Size(Decimal::new(1000, 3)), // 1.000
            tif: tikr_core::TimeInForce::PostOnly,
            kind: tikr_core::QuoteKind::Point,
        })
    }

    #[test]
    fn inventory_boost_scales_reducing_side_only() {
        // Short -$300 against a $600 cap → ratio 0.5, linear, 100% max boost
        // → reducing side (Bid) ×1.5; growing side (Ask) untouched.
        let boost = InventoryBoostConfig {
            max_boost_pct: Decimal::from(100),
            curve_exponent: Decimal::ONE,
        };
        let actions = vec![boost_quote(Side::Bid), boost_quote(Side::Ask)];
        let out =
            apply_inventory_size_boost(actions, Decimal::from(-300), Decimal::from(600), boost);
        assert_eq!(quote_size(&out[0]), Decimal::new(1500, 3)); // 1.000 × 1.5
        assert_eq!(quote_size(&out[1]), Decimal::new(1000, 3)); // ask unchanged
    }

    #[test]
    fn inventory_boost_long_scales_sells() {
        // Long at full cap → ratio 1.0, 50% boost → Ask ×1.5, Bid untouched.
        let boost = InventoryBoostConfig {
            max_boost_pct: Decimal::from(50),
            curve_exponent: Decimal::ONE,
        };
        let actions = vec![boost_quote(Side::Bid), boost_quote(Side::Ask)];
        let out =
            apply_inventory_size_boost(actions, Decimal::from(600), Decimal::from(600), boost);
        assert_eq!(quote_size(&out[0]), Decimal::new(1000, 3)); // bid unchanged
        assert_eq!(quote_size(&out[1]), Decimal::new(1500, 3)); // 1.000 × 1.5
    }

    #[test]
    fn inventory_boost_curve_exponent_dampens_midrange() {
        // Ratio 0.5, exponent 2 → curve = 0.25, 100% max → ×1.25 (vs ×1.5
        // linear). Verifies the curve concentrates boost near the cap.
        let boost = InventoryBoostConfig {
            max_boost_pct: Decimal::from(100),
            curve_exponent: Decimal::from(2),
        };
        let actions = vec![boost_quote(Side::Bid)];
        let out =
            apply_inventory_size_boost(actions, Decimal::from(-300), Decimal::from(600), boost);
        assert_eq!(quote_size(&out[0]), Decimal::new(1250, 3)); // 1.000 × 1.25
    }

    #[test]
    fn inventory_boost_flat_and_disabled_are_noops() {
        let boost = InventoryBoostConfig {
            max_boost_pct: Decimal::from(100),
            curve_exponent: Decimal::ONE,
        };
        // Flat position: nothing to reduce.
        let flat = apply_inventory_size_boost(
            vec![boost_quote(Side::Bid)],
            Decimal::ZERO,
            Decimal::from(600),
            boost,
        );
        assert_eq!(quote_size(&flat[0]), Decimal::new(1000, 3));
        // Zero max_boost_pct disables.
        let off = apply_inventory_size_boost(
            vec![boost_quote(Side::Bid)],
            Decimal::from(-600),
            Decimal::from(600),
            InventoryBoostConfig {
                max_boost_pct: Decimal::ZERO,
                curve_exponent: Decimal::ONE,
            },
        );
        assert_eq!(quote_size(&off[0]), Decimal::new(1000, 3));
    }

    struct MockVenue {
        events: Mutex<Option<Vec<MarketEvent>>>,
        infinite: bool,
        // Recording of write-side venue calls (for live-mode tests).
        // Arc<Mutex<_>> so tests can keep a clone after the runner moves
        // the venue. When `live_mode` is off, these stay empty because
        // the runner never invokes the write methods.
        quote_calls: Arc<Mutex<Vec<QuoteIntent>>>,
        cancel_calls: Arc<Mutex<Vec<QuoteId>>>,
        cancel_all_calls: Arc<Mutex<u32>>,
        requote_calls: Arc<Mutex<Vec<(QuoteId, QuoteIntent)>>>,
    }

    impl MockVenue {
        fn finite(events: Vec<MarketEvent>) -> Self {
            Self {
                events: Mutex::new(Some(events)),
                infinite: false,
                quote_calls: Arc::new(Mutex::new(Vec::new())),
                cancel_calls: Arc::new(Mutex::new(Vec::new())),
                cancel_all_calls: Arc::new(Mutex::new(0)),
                requote_calls: Arc::new(Mutex::new(Vec::new())),
            }
        }
        fn infinite_heartbeats() -> Self {
            Self {
                events: Mutex::new(Some(Vec::new())),
                infinite: true,
                quote_calls: Arc::new(Mutex::new(Vec::new())),
                cancel_calls: Arc::new(Mutex::new(Vec::new())),
                cancel_all_calls: Arc::new(Mutex::new(0)),
                requote_calls: Arc::new(Mutex::new(Vec::new())),
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
        async fn subscribe(
            &self,
            _symbol: &Symbol,
        ) -> Result<BoxStream<'_, MarketEvent>, VenueError> {
            if self.infinite {
                // Yield between items so the select! loop can poll the
                // shutdown branch; a purely-synchronous `repeat_with` would
                // hog the executor and the shutdown task would never run.
                let s = stream::unfold((), |()| async {
                    tokio::task::yield_now().await;
                    Some((MarketEvent::Heartbeat { ts: Timestamp(0) }, ()))
                });
                Ok(Box::pin(s))
            } else {
                let events = self.events.lock().unwrap().take().unwrap_or_default();
                Ok(Box::pin(stream::iter(events)))
            }
        }
        async fn quote(&self, intent: QuoteIntent) -> Result<QuoteId, VenueError> {
            self.quote_calls.lock().unwrap().push(intent);
            Ok(QuoteId::new())
        }
        async fn requote(&self, id: QuoteId, intent: QuoteIntent) -> Result<(), VenueError> {
            self.requote_calls.lock().unwrap().push((id, intent));
            Ok(())
        }
        async fn cancel(&self, id: QuoteId) -> Result<(), VenueError> {
            self.cancel_calls.lock().unwrap().push(id);
            Ok(())
        }
        async fn cancel_all(&self, _symbol: &Symbol) -> Result<(), VenueError> {
            *self.cancel_all_calls.lock().unwrap() += 1;
            Ok(())
        }
        async fn position(&self, _symbol: &Symbol) -> Result<tikr_core::Position, VenueError> {
            unimplemented!()
        }
    }

    fn make_symbol() -> Symbol {
        Symbol {
            base: Asset::new("BTC"),
            quote: Asset::new("USDC"),
            venue: VenueId::new("mock"),
            kind: MarketKind::Perp,
        }
    }

    fn layered_grid() -> LayeredGrid {
        LayeredGrid::new(LayeredGridConfig {
            notional_per_order: Decimal::from(25),
            levels_per_side: 1,
            inner_bps: 20,
            max_position_usdt: Decimal::ZERO,
            take_profit_bps: 0,
            stop_loss_bps: 0,
        })
    }

    fn fill_sim() -> FillSim {
        FillSim::new(FillSimConfig {
            submit_latency_ms: 0,
            cancel_latency_ms: 0,
            fees: VenueFees {
                maker_bps: 0,
                taker_bps: 0,
            },
            max_position_notional_usdt: None,
            silent_cancel_rate_per_min: 0.0,
            rng_seed: 0,
            latency_jitter_ms: 0,
            max_open_orders: None,
            queue_cancel_decay_per_sec: 0.0,
        })
    }

    fn test_config(state_dir: PathBuf) -> RunnerConfig {
        RunnerConfig {
            state_dir,
            snapshot_every_n_events: 100,
            skim: None,
            funding: None,
            snapshot_tap: None,
            live_tap: None,
            notional_rx: None,
            max_position_rx: None,
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
            inventory_boost: None,
        }
    }

    #[tokio::test]
    async fn liquidation_force_closes_seeded_long() {
        let temp = TempDir::new().unwrap();
        let symbol = make_symbol();
        // Seed a 10× long, size 1 @ entry 100. With mmr=0 the liquidation
        // price is 100 × (1 − 0.1) = 90.
        let seed = Position {
            symbol: symbol.clone(),
            size: tikr_core::SignedSize(Decimal::from(1)),
            avg_entry: Price(Decimal::from(100)),
            realized_pnl: Notional(Decimal::ZERO),
        };
        let mut config = test_config(temp.path().into());
        config.seed_position = Some(seed);
        config.liquidation = Some(LiquidationConfig {
            leverage: Decimal::from(10),
            maint_margin_rate: Decimal::ZERO,
            close_fee_bps: 0,
        });
        // Book mid = (88 + 90) / 2 = 89 < liq 90 → liquidation fires.
        let book = MarketEvent::BookUpdate {
            snapshot: Snapshot {
                symbol: symbol.clone(),
                bids: vec![Level {
                    price: Price(Decimal::from(88)),
                    size: Size(Decimal::from(10)),
                }],
                asks: vec![Level {
                    price: Price(Decimal::from(90)),
                    size: Size(Decimal::from(10)),
                }],
                ts: Timestamp(1_000),
            },
        };
        let venue = MockVenue::finite(vec![book]);
        let (_tx, rx) = watch::channel(false);
        let report = run_with_resume(
            venue,
            layered_grid(),
            fill_sim(),
            symbol,
            rx,
            config,
            None,
            None,
            None,
            None, // paper mode
            None,
        )
        .await;
        assert_eq!(report.liquidations, 1, "seeded long must be liquidated");
        // Closed 1 @ liq 90 from entry 100 → realized −10.
        assert_eq!(report.realized.0, Decimal::from(-10));
    }

    #[tokio::test]
    async fn no_liquidation_when_mark_holds_above_liq() {
        let temp = TempDir::new().unwrap();
        let symbol = make_symbol();
        let seed = Position {
            symbol: symbol.clone(),
            size: tikr_core::SignedSize(Decimal::from(1)),
            avg_entry: Price(Decimal::from(100)),
            realized_pnl: Notional(Decimal::ZERO),
        };
        let mut config = test_config(temp.path().into());
        config.seed_position = Some(seed);
        config.liquidation = Some(LiquidationConfig {
            leverage: Decimal::from(10),
            maint_margin_rate: Decimal::ZERO,
            close_fee_bps: 0,
        });
        // Mid = 95.5 > liq 90 → no liquidation.
        let book = MarketEvent::BookUpdate {
            snapshot: Snapshot {
                symbol: symbol.clone(),
                bids: vec![Level {
                    price: Price(Decimal::from(95)),
                    size: Size(Decimal::from(10)),
                }],
                asks: vec![Level {
                    price: Price(Decimal::from(96)),
                    size: Size(Decimal::from(10)),
                }],
                ts: Timestamp(1_000),
            },
        };
        let venue = MockVenue::finite(vec![book]);
        let (_tx, rx) = watch::channel(false);
        let report = run_with_resume(
            venue,
            layered_grid(),
            fill_sim(),
            symbol,
            rx,
            config,
            None,
            None,
            None,
            None,
            None,
        )
        .await;
        assert_eq!(report.liquidations, 0, "mark above liq must not liquidate");
    }

    #[tokio::test]
    async fn mark_series_drives_liquidation_over_mid() {
        let temp = TempDir::new().unwrap();
        let symbol = make_symbol();
        let seed = Position {
            symbol: symbol.clone(),
            size: tikr_core::SignedSize(Decimal::from(1)),
            avg_entry: Price(Decimal::from(100)),
            realized_pnl: Notional(Decimal::ZERO),
        };
        let mut config = test_config(temp.path().into());
        config.seed_position = Some(seed);
        config.liquidation = Some(LiquidationConfig {
            leverage: Decimal::from(10),
            maint_margin_rate: Decimal::ZERO,
            close_fee_bps: 0,
        });
        // Recorded mark dips to 89 (below liq 90) at t=500, while the book
        // mid stays at 95 — proving the trigger marks against the mark
        // series, not the order-book mid (which would NOT liquidate).
        config.mark_series = Some(tikr_backtest::mark::MarkSeries::from_points(vec![(
            500,
            Price(Decimal::from(89)),
        )]));
        let book = MarketEvent::BookUpdate {
            snapshot: Snapshot {
                symbol: symbol.clone(),
                bids: vec![Level {
                    price: Price(Decimal::from(94)),
                    size: Size(Decimal::from(10)),
                }],
                asks: vec![Level {
                    price: Price(Decimal::from(96)),
                    size: Size(Decimal::from(10)),
                }],
                ts: Timestamp(1_000),
            },
        };
        let venue = MockVenue::finite(vec![book]);
        let (_tx, rx) = watch::channel(false);
        let report = run_with_resume(
            venue,
            layered_grid(),
            fill_sim(),
            symbol,
            rx,
            config,
            None,
            None,
            None,
            None,
            None,
        )
        .await;
        // Mid (95) alone could never breach liq 90 — so a liquidation here
        // proves the mark series (89) drove the trigger.
        assert_eq!(report.liquidations, 1, "mark below liq must liquidate");
        // Forced close executes at the liq price (90), not the mark: closed
        // 1 @ 90 from entry 100 → realized −10.
        assert_eq!(report.realized.0, Decimal::from(-10));
    }

    #[tokio::test]
    async fn recorded_funding_rate_overrides_flat_config() {
        let temp = TempDir::new().unwrap();
        let symbol = make_symbol();
        // Long 1 @ 100.
        let seed = Position {
            symbol: symbol.clone(),
            size: tikr_core::SignedSize(Decimal::from(1)),
            avg_entry: Price(Decimal::from(100)),
            realized_pnl: Notional(Decimal::ZERO),
        };
        let mut config = test_config(temp.path().into());
        config.seed_position = Some(seed);
        // Flat fallback rate is ZERO — so any funding accrual must come from
        // the recorded series.
        config.funding = Some(FundingConfig {
            interval_secs: 28_800,
            rate_per_interval: Decimal::ZERO,
        });
        // Recorded mark 100 + funding 0.0001 (1 bp) per 8h interval, from t=0.
        config.mark_series = Some(tikr_backtest::mark::MarkSeries::from_points_with_funding(
            vec![(0, Price(Decimal::from(100)), Some(Decimal::new(1, 4)))],
        ));
        // Two heartbeats exactly one funding interval apart.
        let interval_ns = 28_800u64 * 1_000_000_000;
        let events = vec![
            MarketEvent::Heartbeat { ts: Timestamp(0) },
            MarketEvent::Heartbeat {
                ts: Timestamp(interval_ns),
            },
        ];
        let venue = MockVenue::finite(events);
        let (_tx, rx) = watch::channel(false);
        let report = run_with_resume(
            venue,
            layered_grid(),
            fill_sim(),
            symbol,
            rx,
            config,
            None,
            None,
            None,
            None,
            None,
        )
        .await;
        // dt = one full interval → funding = −pos × mark × rate × 1
        //                                  = −1 × 100 × 0.0001 = −0.01.
        // The flat config rate (0) would have produced 0, so −0.01 proves the
        // recorded rate drove the accrual.
        assert_eq!(report.funding.0, Decimal::new(-1, 2));
    }

    fn make_book_event(symbol: &Symbol, i: u64) -> MarketEvent {
        MarketEvent::BookUpdate {
            snapshot: Snapshot {
                symbol: symbol.clone(),
                bids: vec![Level {
                    price: Price(Decimal::from(100)),
                    size: Size(Decimal::from(1)),
                }],
                asks: vec![Level {
                    price: Price(Decimal::from(101)),
                    size: Size(Decimal::from(1)),
                }],
                ts: Timestamp(i * 1_000_000),
            },
        }
    }

    #[tokio::test]
    async fn runner_handles_empty_event_stream() {
        let temp = TempDir::new().unwrap();
        let venue = MockVenue::finite(Vec::new());
        let (_tx, rx) = watch::channel(false);
        let report = run(
            venue,
            layered_grid(),
            fill_sim(),
            make_symbol(),
            rx,
            test_config(temp.path().into()),
        )
        .await;
        assert_eq!(report.events_processed, 0);
        assert_eq!(report.fills_emitted, 0);
    }

    #[tokio::test]
    async fn runner_shutdown_signal_exits_promptly() {
        let temp = TempDir::new().unwrap();
        let venue = MockVenue::infinite_heartbeats();
        let (tx, rx) = watch::channel(false);
        let cfg = test_config(temp.path().into());

        // Trigger shutdown after a brief delay.
        let shutdown_handle = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let _ = tx.send(true);
        });

        let report = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            run(venue, layered_grid(), fill_sim(), make_symbol(), rx, cfg),
        )
        .await
        .expect("runner did not exit within 2s");

        shutdown_handle.await.unwrap();
        // Should have processed at least some heartbeats before shutdown.
        assert!(report.events_processed > 0);
    }

    #[tokio::test]
    async fn snapshot_writes_to_disk() {
        let temp = TempDir::new().unwrap();
        // Build 100+ BookUpdate events to trigger one snapshot write.
        let symbol = make_symbol();
        let events: Vec<MarketEvent> = (0..120u64)
            .map(|i| MarketEvent::BookUpdate {
                snapshot: Snapshot {
                    symbol: symbol.clone(),
                    bids: vec![Level {
                        price: Price(Decimal::from(100)),
                        size: Size(Decimal::from(1)),
                    }],
                    asks: vec![Level {
                        price: Price(Decimal::from(101)),
                        size: Size(Decimal::from(1)),
                    }],
                    ts: Timestamp(i * 1_000_000),
                },
            })
            .collect();

        let venue = MockVenue::finite(events);
        let (_tx, rx) = watch::channel(false);
        let cfg = test_config(temp.path().into());
        let report = run(venue, layered_grid(), fill_sim(), symbol, rx, cfg).await;

        assert_eq!(report.events_processed, 120);
        assert_eq!(report.schema_version, SCHEMA_VERSION);
        // Check that at least one snapshot was written to disk.
        let entries: Vec<_> = std::fs::read_dir(temp.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("json"))
            .collect();
        assert!(
            !entries.is_empty(),
            "expected at least one JSON snapshot file"
        );

        // Verify the file parses back, including schema_version round-trip.
        let path = entries[0].path();
        let txt = std::fs::read_to_string(&path).unwrap();
        let parsed: PaperReport = serde_json::from_str(&txt).unwrap();
        assert!(parsed.events_processed >= 100);
        assert_eq!(parsed.schema_version, 1);
    }

    #[tokio::test]
    async fn run_with_resume_seeds_position_tracker() {
        let temp = TempDir::new().unwrap();
        let prior = PaperReport {
            schema_version: SCHEMA_VERSION,
            realized: Notional(Decimal::from(5)),
            unrealized: Notional(Decimal::ZERO),
            fees: Notional(Decimal::from(1)),
            funding: Notional(Decimal::ZERO),
            net: Notional(Decimal::from(4)),
            runtime_secs: 100,
            sim_duration_secs: 100,
            events_processed: 50,
            fills_emitted: 3,
            risk_state: None,
            skim_count: 0,
            skim_total_usdt: Notional(Decimal::ZERO),
            base_stacked: Notional(Decimal::ZERO),
            final_perp_balance: Notional(Decimal::ZERO),
            final_base_value: Notional(Decimal::ZERO),
            base_asset: String::new(),
            buy_volume_usdt: Notional(Decimal::ZERO),
            sell_volume_usdt: Notional(Decimal::ZERO),
            peak_position_usdt: Notional(Decimal::ZERO),
            mean_position_usdt: Notional(Decimal::ZERO),
            full_fills: 0,
            partial_fills: 0,
            liquidations: 0,
            peak_fills_per_min: 0,
            rejected_orders: 0,
        };
        let venue = MockVenue::finite(Vec::new());
        let (_tx, rx) = watch::channel(false);
        let report = run_with_resume(
            venue,
            layered_grid(),
            fill_sim(),
            make_symbol(),
            rx,
            test_config(temp.path().into()),
            Some(prior),
            None,
            None,
            None, // no external fills (paper mode)
            None, // no external liqs
        )
        .await;
        assert_eq!(report.realized.0, Decimal::from(5));
        assert_eq!(report.events_processed, 50);
        assert_eq!(report.fills_emitted, 3);
        // runtime accumulates: prior 100 + (~0 for empty stream)
        assert!(report.runtime_secs >= 100);
        assert_eq!(report.schema_version, SCHEMA_VERSION);
    }

    #[tokio::test]
    #[should_panic(expected = "unsupported PaperReport schema_version")]
    async fn run_with_resume_rejects_unknown_schema_version() {
        let temp = TempDir::new().unwrap();
        let prior = PaperReport {
            schema_version: 999,
            realized: Notional(Decimal::ZERO),
            unrealized: Notional(Decimal::ZERO),
            fees: Notional(Decimal::ZERO),
            funding: Notional(Decimal::ZERO),
            net: Notional(Decimal::ZERO),
            runtime_secs: 0,
            sim_duration_secs: 0,
            events_processed: 0,
            fills_emitted: 0,
            risk_state: None,
            skim_count: 0,
            skim_total_usdt: Notional(Decimal::ZERO),
            base_stacked: Notional(Decimal::ZERO),
            final_perp_balance: Notional(Decimal::ZERO),
            final_base_value: Notional(Decimal::ZERO),
            base_asset: String::new(),
            buy_volume_usdt: Notional(Decimal::ZERO),
            sell_volume_usdt: Notional(Decimal::ZERO),
            peak_position_usdt: Notional(Decimal::ZERO),
            mean_position_usdt: Notional(Decimal::ZERO),
            full_fills: 0,
            partial_fills: 0,
            liquidations: 0,
            peak_fills_per_min: 0,
            rejected_orders: 0,
        };
        let venue = MockVenue::finite(Vec::new());
        let (_tx, rx) = watch::channel(false);
        let _ = run_with_resume(
            venue,
            layered_grid(),
            fill_sim(),
            make_symbol(),
            rx,
            test_config(temp.path().into()),
            Some(prior),
            None,
            None,
            None, // no external fills (paper mode)
            None, // no external liqs
        )
        .await;
    }

    #[tokio::test]
    async fn run_multi_aggregates_per_symbol() {
        let symbol_a = Symbol {
            base: Asset::new("BTC"),
            quote: Asset::new("USDC"),
            venue: VenueId::new("mock"),
            kind: MarketKind::Perp,
        };
        let symbol_b = Symbol {
            base: Asset::new("ETH"),
            quote: Asset::new("USDC"),
            venue: VenueId::new("mock"),
            kind: MarketKind::Perp,
        };

        let (_tx, rx) = watch::channel(false);
        let rx_a = rx.clone();
        let rx_b = rx.clone();
        let temp = TempDir::new().unwrap();
        let cfg_a = test_config(temp.path().join("a"));
        let cfg_b = test_config(temp.path().join("b"));

        // 4 BookUpdate events per symbol.
        let events_a: Vec<MarketEvent> = (0..4u64).map(|i| make_book_event(&symbol_a, i)).collect();
        let events_b: Vec<MarketEvent> = (0..4u64).map(|i| make_book_event(&symbol_b, i)).collect();

        let symbol_a_clone = symbol_a.clone();
        let symbol_b_clone = symbol_b.clone();
        let fut_a: Pin<Box<dyn Future<Output = PaperReport> + Send>> = Box::pin(async move {
            run(
                MockVenue::finite(events_a),
                layered_grid(),
                fill_sim(),
                symbol_a_clone,
                rx_a,
                cfg_a,
            )
            .await
        });
        let fut_b: Pin<Box<dyn Future<Output = PaperReport> + Send>> = Box::pin(async move {
            run(
                MockVenue::finite(events_b),
                layered_grid(),
                fill_sim(),
                symbol_b_clone,
                rx_b,
                cfg_b,
            )
            .await
        });

        let runs = vec![
            MultiSymbolRun {
                symbol: symbol_a.clone(),
                future: fut_a,
            },
            MultiSymbolRun {
                symbol: symbol_b.clone(),
                future: fut_b,
            },
        ];

        let report = run_multi(runs, rx).await;
        assert_eq!(report.per_symbol.len(), 2);
        assert_eq!(report.sum.events_processed, 8);
        assert!(report.per_symbol.contains_key(&symbol_a));
        assert!(report.per_symbol.contains_key(&symbol_b));
        assert_eq!(report.sum.schema_version, SCHEMA_VERSION);
    }

    /// Live mode (external_fills = Some) dispatches strategy actions to the
    /// venue's quote / cancel_all methods. Paper mode skips them.
    ///
    /// Regression: pre-fix, the runner only called fill_sim.on_action and
    /// never invoked venue.quote() — Hyperliquid + DODO + Binance "live"
    /// runs all processed market events but placed zero real orders.
    #[tokio::test(flavor = "multi_thread")]
    async fn live_mode_dispatches_actions_to_venue() {
        let symbol = make_symbol();
        // Two BookUpdates so LayeredGrid emits a cold-start Quote on the first full book.
        let events = vec![
            MarketEvent::BookUpdate {
                snapshot: Snapshot {
                    symbol: symbol.clone(),
                    bids: vec![Level {
                        price: Price(Decimal::from(100)),
                        size: Size(Decimal::from(1)),
                    }],
                    asks: vec![Level {
                        price: Price(Decimal::from(101)),
                        size: Size(Decimal::from(1)),
                    }],
                    ts: Timestamp(1_000),
                },
            },
            MarketEvent::BookUpdate {
                snapshot: Snapshot {
                    symbol: symbol.clone(),
                    bids: vec![Level {
                        price: Price(Decimal::from(102)),
                        size: Size(Decimal::from(1)),
                    }],
                    asks: vec![Level {
                        price: Price(Decimal::from(103)),
                        size: Size(Decimal::from(1)),
                    }],
                    ts: Timestamp(2_000),
                },
            },
        ];
        let venue = MockVenue::finite(events);
        // Clone the recording handles before moving the venue.
        let quote_log = venue.quote_calls.clone();
        let cancel_all_log = venue.cancel_all_calls.clone();

        let strategy = LayeredGrid::new(LayeredGridConfig {
            notional_per_order: Decimal::from(25),
            levels_per_side: 1,
            inner_bps: 20,
            max_position_usdt: Decimal::ZERO,
            take_profit_bps: 0,
            stop_loss_bps: 0,
        });
        let fill_sim = FillSim::new(FillSimConfig {
            submit_latency_ms: 0,
            cancel_latency_ms: 0,
            fees: VenueFees {
                maker_bps: 0,
                taker_bps: 0,
            },
            max_position_notional_usdt: None,
            silent_cancel_rate_per_min: 0.0,
            rng_seed: 0,
            latency_jitter_ms: 0,
            max_open_orders: None,
            queue_cancel_decay_per_sec: 0.0,
        });

        let temp = TempDir::new().unwrap();
        let config = RunnerConfig {
            state_dir: temp.path().to_path_buf(),
            snapshot_every_n_events: 100,
            skim: None,
            funding: None,
            snapshot_tap: None,
            live_tap: None,
            notional_rx: None,
            max_position_rx: None,
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
            inventory_boost: None,
        };
        // External fills channel: empty, never sends — but `Some` activates
        // live_mode.
        let (_fill_tx, fill_rx) = mpsc::unbounded_channel::<Fill>();
        let (_tx, rx) = watch::channel(false);

        let report = run_with_resume(
            venue,
            strategy,
            fill_sim,
            symbol,
            rx,
            config,
            None,
            None,
            None,
            Some(fill_rx),
            None,
        )
        .await;

        assert_eq!(report.events_processed, 2);

        // LayeredGrid emits Quotes on the first book update.
        // Verify live mode dispatched Quotes to the venue.
        let _ = cancel_all_log;
        let quote_count = quote_log.lock().unwrap().len();
        assert!(
            quote_count > 0,
            "live mode must dispatch Quote to venue (got {quote_count})"
        );
    }

    /// Paper mode (external_fills = None) does NOT call venue write methods.
    /// Strategy actions go to fill_sim only.
    #[tokio::test(flavor = "multi_thread")]
    async fn paper_mode_does_not_dispatch_to_venue() {
        let symbol = make_symbol();
        let events = vec![MarketEvent::BookUpdate {
            snapshot: Snapshot {
                symbol: symbol.clone(),
                bids: vec![Level {
                    price: Price(Decimal::from(100)),
                    size: Size(Decimal::from(1)),
                }],
                asks: vec![Level {
                    price: Price(Decimal::from(101)),
                    size: Size(Decimal::from(1)),
                }],
                ts: Timestamp(1_000),
            },
        }];
        let venue = MockVenue::finite(events);
        let quote_log = venue.quote_calls.clone();
        let cancel_all_log = venue.cancel_all_calls.clone();

        let strategy = LayeredGrid::new(LayeredGridConfig {
            notional_per_order: Decimal::from(25),
            levels_per_side: 1,
            inner_bps: 20,
            max_position_usdt: Decimal::ZERO,
            take_profit_bps: 0,
            stop_loss_bps: 0,
        });
        let fill_sim = FillSim::new(FillSimConfig {
            submit_latency_ms: 0,
            cancel_latency_ms: 0,
            fees: VenueFees {
                maker_bps: 0,
                taker_bps: 0,
            },
            max_position_notional_usdt: None,
            silent_cancel_rate_per_min: 0.0,
            rng_seed: 0,
            latency_jitter_ms: 0,
            max_open_orders: None,
            queue_cancel_decay_per_sec: 0.0,
        });

        let temp = TempDir::new().unwrap();
        let config = RunnerConfig {
            state_dir: temp.path().to_path_buf(),
            snapshot_every_n_events: 100,
            skim: None,
            funding: None,
            snapshot_tap: None,
            live_tap: None,
            notional_rx: None,
            max_position_rx: None,
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
            inventory_boost: None,
        };
        let (_tx, rx) = watch::channel(false);

        let _report = run_with_resume(
            venue, strategy, fill_sim, symbol, rx, config, None, None, None,
            None, // paper mode
            None,
        )
        .await;

        assert_eq!(
            *cancel_all_log.lock().unwrap(),
            0,
            "paper mode must not dispatch CancelAll to venue"
        );
        assert_eq!(
            quote_log.lock().unwrap().len(),
            0,
            "paper mode must not dispatch Quote to venue"
        );
    }
}
