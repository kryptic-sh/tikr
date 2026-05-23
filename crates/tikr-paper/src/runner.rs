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
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tikr_backtest::fill_sim::FillSim;
use tikr_backtest::pnl::PositionTracker;
use tikr_core::{
    Decimal, Fill, MarketEvent, Notional, Position, Price, Side, Snapshot, Symbol, Timestamp,
};
use tikr_risk::{RiskContext, RiskDecision, RiskGate};
use tikr_strategy::{Strategy, StrategyContext};
use tikr_venue::{QuoteIntent, Venue};
use tokio::sync::{mpsc, watch};
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Max consecutive venue rejections per side before switching to single-sided quoting.
/// When one side hits this threshold, that side is skipped in subsequent quote rounds
/// until a full requote cycle resets the counters.
const MAX_FAILS_PER_SIDE: u32 = 3;

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
}

/// Perp funding accrual parameters. Binance USD-M typically pays/charges
/// every 8h at 00:00 / 08:00 / 16:00 UTC; this model is continuous (smooth
/// over time) for backtest simplicity. Positive rate = longs pay shorts.
#[derive(Debug, Clone, Copy)]
pub struct FundingConfig {
    /// Funding rate per 8h interval, as a signed bps value. Binance default
    /// cap is ±75 bps but typical mid-cap pairs sit at ±1 bps (~0.01%).
    pub rate_bps_per_8h: i32,
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
        }
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
        venue, strategy, fill_sim, symbol, shutdown, config, None, None, None, None,
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
) -> PaperReport
where
    V: Venue,
    S: Strategy,
{
    // Reconstruct tracker from resume if provided, else fresh.
    // v0 limitation: position size is reset to zero — see run_with_resume docs.
    let mut tracker = if let Some(ref prior) = resume {
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
    } else {
        PositionTracker::new(symbol.clone())
    };

    // Seed counters from resume.
    let mut events_processed: u64 = resume.as_ref().map(|r| r.events_processed).unwrap_or(0);
    let mut fills_emitted: u64 = resume.as_ref().map(|r| r.fills_emitted).unwrap_or(0);
    // Buy/sell fill counters drive the periodic status line. Not yet persisted
    // to PaperReport — resume always starts these at 0.
    let mut buy_fills: u64 = 0;
    let mut sell_fills: u64 = 0;
    let mut buy_volume: Decimal = Decimal::ZERO;
    let mut sell_volume: Decimal = Decimal::ZERO;
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
    let mut last_mid = Price(Decimal::ZERO);
    let mut last_fill: Option<Fill> = None;
    // Per-symbol side-failure tracker. When one side's venue.quote() fails
    // MAX_FAILS_PER_SIDE times consecutively, that side is skipped until the
    // next CancelAll (full requote cycle) resets the counters.
    let mut side_fails: HashMap<String, (u32, u32)> = HashMap::new();
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
    let mut notional_rx = config.notional_rx;
    let mut last_funding_ts: Option<Timestamp> = None;
    let run_id = make_run_id(&symbol);

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
                last_mid,
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

    loop {
        // Poll the external fill receiver when in live mode. We use an async
        // block that resolves to `Option<Fill>` so the select! can be unified.
        tokio::select! {
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
                    let ctx = StrategyContext {
                        symbol: &symbol,
                        now: last_event_ts.unwrap_or(Timestamp(0)),
                        position: &pos,
                        recent_fills: &[],
                        latest_book: &current_book,
                        open_quotes: &open_quotes,
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
                    current_book = snapshot.clone();
                    if let (Some(b), Some(a)) = (snapshot.bids.first(), snapshot.asks.first()) {
                        last_mid = Price((b.price.0 + a.price.0) / Decimal::from(2));
                    }
                }

                // Funding accrual: continuous model. On each event, charge
                // (or credit) `position × mark × rate × (dt / 28800s)`. The
                // sign convention: positive funding rate → longs pay,
                // shorts receive (`amount = −size × mark × rate × dt/8h`).
                if let Some(fcfg) = funding_cfg
                    && last_mid.0 > Decimal::ZERO
                {
                    if let Some(prev_ts) = last_funding_ts {
                        let dt_ns = ts.0.saturating_sub(prev_ts.0);
                        if dt_ns > 0 {
                            let dt_secs = Decimal::from(dt_ns) / Decimal::from(1_000_000_000u64);
                            let rate_per_8h =
                                Decimal::from(fcfg.rate_bps_per_8h) / Decimal::from(10_000);
                            let interval = Decimal::from(28_800u64);
                            let pos_size = tracker.snapshot().size.0;
                            let amount = -pos_size * last_mid.0 * rate_per_8h * (dt_secs / interval);
                            tracker.accrue_funding(amount);
                        }
                    }
                    last_funding_ts = Some(ts);
                }

                let pos = tracker.snapshot();
                let open_quotes = fill_sim.live_quotes_for(&symbol);
                let ctx = StrategyContext {
                    symbol: &symbol,
                    now: ts,
                    position: &pos,
                    recent_fills: &[],
                    latest_book: &current_book,
                    open_quotes: &open_quotes,
                };

                let actions = strategy.on_event(&ctx, &event);

                // Risk-gate filter — same `risk_ctx` for every action in this
                // batch (tracker state can't change mid-loop since fills
                // arrive via a separate select arm), so doing it up-front
                // lets the dispatch loop below batch quotes without
                // re-entering the gate per action.
                let mut filtered: Vec<tikr_strategy::Action> = Vec::with_capacity(actions.len());
                for action in actions {
                    if let Some(gate) = risk_gate.as_mut() {
                        let pnl_now = tracker.report(last_mid);
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
                                        let report = tracker.report(last_mid);
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
                            while i < filtered.len() {
                                if let tikr_strategy::Action::Quote(intent) = &filtered[i] {
                                    let state = side_fails
                                        .entry(symbol.base.0.to_string())
                                        .or_insert((0, 0));
                                    let skip = match intent.side {
                                        Side::Bid => state.0 >= MAX_FAILS_PER_SIDE,
                                        Side::Ask => state.1 >= MAX_FAILS_PER_SIDE,
                                    };
                                    if !skip {
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
                                        warn!(error = ?e, "live: venue.quote failed");
                                        match intent.side {
                                            Side::Bid => state.0 += 1,
                                            Side::Ask => state.1 += 1,
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
                                    Ok(()) => fill_sim.drop_quote(*id),
                                    Err(e) => warn!(error = ?e, "live: venue.cancel failed"),
                                }
                            }
                            tikr_strategy::Action::CancelAll => {
                                side_fails.remove(symbol.base.0.as_ref());
                                match venue.cancel_all(&symbol).await {
                                    Ok(()) => fill_sim.drop_quotes_for(&symbol),
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
                            &mut buy_fills,
                            &mut sell_fills,
                            &mut buy_volume,
                            &mut sell_volume,
                            alert_sink.as_deref(),
                            &symbol,
                        )
                        .await;
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
                        let fill_ctx = StrategyContext {
                            symbol: &symbol,
                            now: ts,
                            position: &post_fill_pos,
                            recent_fills: std::slice::from_ref(&fill_clone),
                            latest_book: &current_book,
                            open_quotes: &post_fill_quotes,
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
                if !live_mode {
                    let rejections = fill_sim.drain_rejections();
                    if !rejections.is_empty() {
                        let rec_pos = tracker.snapshot();
                        for (rej_intent, rej_reason) in rejections {
                            let rec_quotes = fill_sim.live_quotes_for(&symbol);
                            let rec_ctx = StrategyContext {
                                symbol: &symbol,
                                now: ts,
                                position: &rec_pos,
                                recent_fills: &[],
                                latest_book: &current_book,
                                open_quotes: &rec_quotes,
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
                    let rep = tracker.report(last_mid);
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
                        last_mid,
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
                let fill_clone = fill.clone();
                let fill_is_full = fill_clone.is_full;
                apply_fill(
                    fill,
                    &mut tracker,
                    &mut risk_gate,
                    &mut fills_emitted,
                    &mut buy_fills,
                    &mut sell_fills,
                    &mut buy_volume,
                    &mut sell_volume,
                    alert_sink.as_deref(),
                    &symbol,
                )
                .await;
                last_fill = Some(fill_clone.clone());
                if let Some(ref tap) = config.snapshot_tap {
                    let mut report = finalize(
                        &tracker,
                        last_mid,
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
                let fill_ctx = StrategyContext {
                    symbol: &symbol,
                    now: fill_clone.ts,
                    position: &post_fill_pos,
                    recent_fills: std::slice::from_ref(&fill_clone),
                    latest_book: &current_book,
                    open_quotes: &post_fill_quotes,
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
                let fingerprint = (fills_emitted, open_buys, open_sells, pos.size.0);
                if last_status_fingerprint.as_ref() == Some(&fingerprint) {
                    continue;
                }
                last_status_fingerprint = Some(fingerprint);
                let pnl = tracker.report(last_mid);
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
                // Order reconciliation: ground-truth via venue.open_orders.
                // Drop any FillSim ghosts (silent cancel / expiry / lost WS
                // events). One REST call every 30 s per bot — cheap relative
                // to event rate.
                match venue.open_orders(&symbol).await {
                    Ok(orders) => {
                        let venue_open = orders.len();
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
    let ctx = StrategyContext {
        symbol: &symbol,
        now: now_ts,
        position: &pos,
        recent_fills: &[],
        latest_book: &current_book,
        open_quotes: &shutdown_quotes,
    };
    let shutdown_actions = strategy.on_shutdown(&ctx);
    for action in shutdown_actions {
        fill_sim.on_action(action, now_ts);
    }

    let mut report = finalize(
        &tracker,
        last_mid,
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
) where
    V: Venue,
    S: Strategy,
{
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
                        warn!(error = ?e, "live: venue.quote failed (post-fill)");
                        match intent.side {
                            Side::Bid => state.0 += 1,
                            Side::Ask => state.1 += 1,
                        }
                        rejected_intents.push((intent.clone(), format!("{e:?}")));
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
    const MAX_RECOVERY_ROUNDS: usize = 5;
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
                                warn!(error = ?e, round, "live: venue.quote failed (recovery)");
                                match intent.side {
                                    Side::Bid => state.0 += 1,
                                    Side::Ask => state.1 += 1,
                                }
                                rejected_intents.push((intent.clone(), format!("{e:?}")));
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
    buy_fills: &mut u64,
    sell_fills: &mut u64,
    buy_volume: &mut Decimal,
    sell_volume: &mut Decimal,
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
    last_mid: Price,
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
) -> PaperReport {
    let base = tracker.report(last_mid);
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
    let final_base_value = base_stacked * last_mid.0;
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
        async fn fills_since(&self, _since_ts: u64) -> Result<Vec<tikr_core::Fill>, VenueError> {
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
        }
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
        };
        let (_tx, rx) = watch::channel(false);

        let _report = run_with_resume(
            venue, strategy, fill_sim, symbol, rx, config, None, None, None,
            None, // paper mode
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
