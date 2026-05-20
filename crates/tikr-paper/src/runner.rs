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
use crate::report::{PaperReport, SCHEMA_VERSION};
use crate::state;
use futures::StreamExt;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tikr_backtest::fill_sim::FillSim;
use tikr_backtest::pnl::PositionTracker;
use tikr_core::{
    Decimal, Fill, MarketEvent, Notional, Position, Price, Snapshot, Symbol, Timestamp,
};
use tikr_risk::{RiskContext, RiskDecision, RiskGate};
use tikr_strategy::{Strategy, StrategyContext};
use tikr_venue::Venue;
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};
use uuid::Uuid;

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
    let started = Instant::now();
    // Track sim-time span from event timestamps so backtest reports show
    // data-time duration, not wall-clock replay speed.
    let mut first_event_ts: Option<Timestamp> = None;
    let mut last_event_ts: Option<Timestamp> = None;
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
            return report;
        }
    };

    // Whether we are in live mode (external fills) or paper mode (FillSim).
    // In live mode the FillSim is still driven by actions for state tracking
    // but its synthesized fills are discarded; real fills come from `external_fills`.
    let live_mode = external_fills.is_some();

    // 1 Hz status-line ticker. First tick fires immediately; we skip it so
    // the loop only emits status starting at +1s.
    let mut status_tick = tokio::time::interval(Duration::from_secs(1));
    status_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    status_tick.tick().await;

    loop {
        // Poll the external fill receiver when in live mode. We use an async
        // block that resolves to `Option<Fill>` so the select! can be unified.
        tokio::select! {
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
                for action in actions {
                    // Risk-gate check happens BEFORE the action is dispatched.
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

                    // Live mode: dispatch the action to the real venue.
                    // Fills come back via the external_fills channel.
                    // Paper mode: skip the venue call (fill_sim simulates).
                    //
                    // Quote handling is special — we feed the venue's returned
                    // QuoteId into FillSim so subsequent strategy `Cancel(id)`
                    // actions reference ids the venue recognizes.
                    if live_mode {
                        match &action {
                            tikr_strategy::Action::Quote(intent) => {
                                match venue.quote(intent.clone()).await {
                                    Ok(qid) => {
                                        info!(
                                            side = ?intent.side, price = %intent.price.0, size = %intent.size.0,
                                            quote_id = ?qid, "live: order placed"
                                        );
                                        fill_sim.enqueue_place_with_id(
                                            intent.clone(),
                                            ts,
                                            qid,
                                        );
                                    }
                                    Err(e) => warn!(error = ?e, "live: venue.quote failed"),
                                }
                                continue;
                            }
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
                                if let Err(e) = venue.cancel(*id).await {
                                    warn!(error = ?e, "live: venue.cancel failed");
                                }
                            }
                            tikr_strategy::Action::CancelAll => {
                                if let Err(e) = venue.cancel_all(&symbol).await {
                                    warn!(error = ?e, "live: venue.cancel_all failed");
                                }
                            }
                            tikr_strategy::Action::NoOp => {}
                        }
                    }
                    fill_sim.on_action(action, ts);
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
                            alert_sink.as_deref(),
                            &symbol,
                        )
                        .await;
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
                        for action in fill_actions {
                            fill_sim.on_action(action, ts);
                        }
                    }
                } else {
                    // Still call on_market_event so FillSim internal state advances.
                    let _ = fill_sim.on_market_event(&event, ts);
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

                if events_processed > 0
                    && config.snapshot_every_n_events > 0
                    && events_processed.is_multiple_of(config.snapshot_every_n_events as u64)
                {
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
                    if let Err(e) = state::write_snapshot(&report, &config.state_dir, &run_id) {
                        warn!("snapshot write failed: {}", e);
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
                    info!("external fill channel closed");
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
                    alert_sink.as_deref(),
                    &symbol,
                )
                .await;
                // Partial fill: order is still resting on the book, do not
                // drop from FillSim and do not notify the strategy — the
                // strategy should keep waiting for the remaining size.
                if !fill_is_full {
                    continue;
                }
                // Sync FillSim: drop the consumed quote so `live_quotes_for`
                // returns the correct open count for the strategy.
                fill_sim.drop_quote(fill_clone.quote_id);

                // Notify the strategy of its own fill so re-entry / rolling
                // ladder logic fires. Dispatch resulting actions to the venue
                // (Quote => place + record id in fill_sim; Cancel => cancel).
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
                for action in fill_actions {
                    match &action {
                        tikr_strategy::Action::Quote(intent) => {
                            match venue.quote(intent.clone()).await {
                                Ok(qid) => {
                                    info!(
                                        side = ?intent.side, price = %intent.price.0, size = %intent.size.0,
                                        quote_id = ?qid, "live: order placed (post-fill)"
                                    );
                                    fill_sim.enqueue_place_with_id(
                                        intent.clone(),
                                        fill_clone.ts,
                                        qid,
                                    );
                                }
                                Err(e) => warn!(error = ?e, "live: venue.quote failed (post-fill)"),
                            }
                        }
                        tikr_strategy::Action::Requote { id, intent } => {
                            if let Err(e) = venue.requote(*id, intent.clone()).await {
                                warn!(error = ?e, "live: venue.requote failed (post-fill)");
                            }
                        }
                        tikr_strategy::Action::Cancel(id) => {
                            if let Err(e) = venue.cancel(*id).await {
                                warn!(error = ?e, "live: venue.cancel failed (post-fill)");
                            }
                            fill_sim.on_action(action, fill_clone.ts);
                        }
                        tikr_strategy::Action::CancelAll => {
                            if let Err(e) = venue.cancel_all(&symbol).await {
                                warn!(error = ?e, "live: venue.cancel_all failed (post-fill)");
                            }
                            fill_sim.on_action(action, fill_clone.ts);
                        }
                        tikr_strategy::Action::NoOp => {}
                    }
                }
            }
            _ = status_tick.tick() => {
                let pos = tracker.snapshot();
                let pnl = tracker.report(last_mid);
                let quotes = fill_sim.live_quotes_for(&symbol);
                let (open_buys, open_sells) = quotes.iter().fold((0u32, 0u32), |(b, s), (_, q)| {
                    match q.side {
                        tikr_core::Side::Bid => (b + 1, s),
                        tikr_core::Side::Ask => (b, s + 1),
                    }
                });
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
    info!(
        runtime_secs = report.runtime_secs,
        events = events_processed,
        fills = fills_emitted,
        net = %report.net.0,
        "paper runner done"
    );
    report
}

/// Apply a fill to the tracker, update the risk gate, emit alerts.
///
/// Shared by paper mode (FillSim-synthesized fills) and live mode (external
/// venue fills). Keeping this as a standalone async fn avoids code duplication
/// in the two `select!` arms.
async fn apply_fill(
    fill: Fill,
    tracker: &mut PositionTracker,
    risk_gate: &mut Option<Box<dyn RiskGate>>,
    fills_emitted: &mut u64,
    buy_fills: &mut u64,
    sell_fills: &mut u64,
    alert_sink: Option<&dyn AlertSink>,
    symbol: &Symbol,
) {
    info!(price = %fill.price.0, size = %fill.size.0, side = ?fill.side, "fill");
    tracker.apply(&fill);
    if let Some(gate) = risk_gate.as_mut() {
        gate.record_fill(fill.ts);
    }
    *fills_emitted += 1;
    match fill.side {
        tikr_core::Side::Bid => *buy_fills += 1,
        tikr_core::Side::Ask => *sell_fills += 1,
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
    use tikr_strategy::{NaiveGrid, NaiveGridConfig, Strategy};
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

    fn naive_grid() -> NaiveGrid {
        NaiveGrid::new(NaiveGridConfig {
            levels_per_side: 1,
            base_spread_bps: 50,
            level_step_bps: 10,
            size_per_quote: Size(Decimal::from(1)),
            min_requote_interval_ms: 100_000,
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
        })
    }

    fn test_config(state_dir: PathBuf) -> RunnerConfig {
        RunnerConfig {
            state_dir,
            snapshot_every_n_events: 100,
            skim: None,
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
            naive_grid(),
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
            run(venue, naive_grid(), fill_sim(), make_symbol(), rx, cfg),
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
        let report = run(venue, naive_grid(), fill_sim(), symbol, rx, cfg).await;

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
            naive_grid(),
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
            naive_grid(),
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
                naive_grid(),
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
                naive_grid(),
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
        // Two BookUpdates so NaiveGrid emits CancelAll + Quote on the second.
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

        let strategy = NaiveGrid::new(NaiveGridConfig {
            levels_per_side: 1,
            base_spread_bps: 50,
            level_step_bps: 10,
            size_per_quote: Size(Decimal::from(1)),
            min_requote_interval_ms: 0,
        });
        let fill_sim = FillSim::new(FillSimConfig {
            submit_latency_ms: 0,
            cancel_latency_ms: 0,
            fees: VenueFees {
                maker_bps: 0,
                taker_bps: 0,
            },
        });

        let temp = TempDir::new().unwrap();
        let config = RunnerConfig {
            state_dir: temp.path().to_path_buf(),
            snapshot_every_n_events: 100,
            skim: None,
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

        // NaiveGrid emits Quotes on every book update (Cancel(id) appears
        // from the second update onward once `ctx.open_quotes` is populated).
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

        let strategy = NaiveGrid::new(NaiveGridConfig {
            levels_per_side: 1,
            base_spread_bps: 50,
            level_step_bps: 10,
            size_per_quote: Size(Decimal::from(1)),
            min_requote_interval_ms: 0,
        });
        let fill_sim = FillSim::new(FillSimConfig {
            submit_latency_ms: 0,
            cancel_latency_ms: 0,
            fees: VenueFees {
                maker_bps: 0,
                taker_bps: 0,
            },
        });

        let temp = TempDir::new().unwrap();
        let config = RunnerConfig {
            state_dir: temp.path().to_path_buf(),
            snapshot_every_n_events: 100,
            skim: None,
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
