//! Spread scalping / liquidity-provision strategy.
//!
//! When the market spread exceeds a configurable bps threshold, places passive
//! limit orders one tick inside the best bid/ask. Requotes on a fixed interval
//! unless quotes are already at the best market prices. Inventory-aware sizing
//! increases the reducing-side order size.

pub mod adverse_tracker;
pub mod book_state;
pub mod policy;
pub mod resting_orders;

/// Risk policy lives at the crate root so SG/LG share the same TP/SL
/// + bps-of-notional evaluation as SS. Re-exported here so existing
/// `spread_scalp::risk::*` paths continue to compile.
pub use crate::risk;

use tikr_core::{
    Decimal, MarketEvent, Price, QuoteKind, Side, Size, Snapshot, TimeInForce, Timestamp,
};
use tikr_venue::QuoteIntent;

use crate::{Action, Strategy, StrategyContext};
use adverse_tracker::{AdverseConfig, AdverseTracker};
use book_state::{Top, quote_size, size_at_least_min_notional};
use policy::{DiffDecision, apply as policy_apply, diff as policy_diff};
use resting_orders::RestingOrders;
use risk::{RiskConfig, RiskDecision};

/// Configuration for [`SpreadScalp`].
#[derive(Debug, Clone)]
pub struct SpreadScalpConfig {
    /// Fiat notional per order.
    pub notional_per_order: Decimal,
    /// Venue tick size (price increment).
    pub tick_size: Decimal,
    /// Venue lot step size (quantity rounding).
    pub step_size: Decimal,
    /// Minimum order notional (price × size) required by the venue.
    pub min_notional: Decimal,
    /// Minimum market spread in bps required to quote.
    pub min_spread_bps: Decimal,
    /// Fixed requote interval in ms.
    pub requote_interval_ms: u64,
    /// Max position in quote currency before one-sided quoting kicks in.
    /// 0 = disabled.
    pub max_position_usdt: Decimal,
    /// Unrealized PnL threshold in quote currency to activate take-profit.
    /// When exceeded, fires an IOC on the reducing side at the opposing
    /// touch to close immediately as taker. 0 = disabled.
    pub take_profit_usdt: Decimal,
    /// Cooldown after a venue rejection (per side) before another rebuild
    /// is allowed. Prevents -5022 / -2019 hot loops on fast markets.
    /// 0 disables the gate (legacy behaviour).
    pub reject_cooldown_ms: u64,
    /// Per-side requote price tolerance, in ticks. Stage-4 `policy::diff`
    /// skips a requote when the new target is within `±N ticks` of the
    /// resting quote, killing churn on micro-mid jitter. 0 = exact
    /// match required; 1-2 is sensible for most venues.
    pub price_tolerance_ticks: u32,
    /// Take-profit threshold in bps of position notional (entry × qty).
    /// `0` disables — falls back to `take_profit_usdt`. Stage 5: when
    /// non-zero, position closes via IOC at the opposing touch as soon
    /// as unrealized PnL ≥ this many bps of notional, checked on every
    /// event (not just at requote time).
    pub take_profit_bps: u32,
    /// Stop-loss threshold in bps of position notional. `0` disables.
    /// Same trigger shape as `take_profit_bps` — IOC at opposing touch
    /// on every event. Bounds the bad-tail when a position is grinding
    /// against the strategy.
    pub stop_loss_bps: u32,
    /// Adverse-selection tracker config (Stage 6). When non-zero
    /// `max_widen_bps`, `min_spread_bps` is bumped dynamically by
    /// `current_widen_bps` whenever rolling post-fill adverse drift
    /// exceeds `threshold_bps`. Set fields all to zero / disabled to
    /// keep the legacy fixed-threshold behaviour.
    pub adverse: AdverseConfig,
    /// When `true` (default), the close-side passive quote stays alive
    /// even when book spread is below `min_spread_bps`. Lets a held
    /// position close at maker fee instead of drifting unhedged once
    /// the cascade event that triggered the entry cools off.
    /// Add-side (the side that would deepen inventory) still respects
    /// the spread gate. `false` restores the legacy behaviour where
    /// BOTH sides cancel when targets unavailable — useful for backtest
    /// A/B but not recommended for live trading.
    pub close_side_always_quotes: bool,
}

/// Spread scalping strategy state.
pub struct SpreadScalp {
    config: SpreadScalpConfig,
    last_bid: Option<Price>,
    last_ask: Option<Price>,
    last_requote_ts: Option<Timestamp>,
    quotes_live: bool,
    /// Per-side timestamp (ns) of the last venue rejection. Used by
    /// `should_emit_side` to gate the next rebuild attempt by
    /// `reject_cooldown_ms`, matching the SG pattern.
    last_reject_bid_ts: Option<Timestamp>,
    last_reject_ask_ts: Option<Timestamp>,
    /// Strategy-owned view of resting orders. Stage 3 introduces this
    /// as the authoritative quote book; Stage 4 will retire
    /// `last_bid`/`last_ask`/`quotes_live` in favour of it.
    resting: RestingOrders,
    /// Adverse-selection tracker (Stage 6). Disabled when
    /// `config.adverse.snapshot_window_ms == 0`.
    adverse: AdverseTracker,
}

impl SpreadScalp {
    fn compute_targets(&self, snapshot: &Snapshot) -> Option<(Price, Price)> {
        let top = Top::from_snapshot(snapshot)?;
        let tick = self.config.tick_size;
        if tick <= Decimal::ZERO || top.ask.0 <= top.bid.0 {
            return None;
        }
        let spread_bps = top.spread_bps()?;
        // Stage 6: effective threshold = baseline + adverse widen.
        // When adverse tracker is disabled, widen is 0 and behaviour
        // collapses to the legacy fixed threshold.
        let effective_min = self.config.min_spread_bps + self.adverse.current_widen_bps();
        if spread_bps < effective_min {
            return None;
        }
        // Quote 1 tick inside the best level so PostOnly orders don't get
        // rejected (-5022) when the market moves between snapshot and
        // placement.
        let bid = Price(top.bid.0 + tick);
        let ask = Price(top.ask.0 - tick);
        if bid.0 >= ask.0 {
            return None;
        }
        Some((bid, ask))
    }

    fn quote_size(&self, price: Price, size_multiplier: Decimal) -> Decimal {
        quote_size(
            self.config.notional_per_order,
            price,
            size_multiplier,
            self.config.step_size,
        )
    }

    fn make_quote(
        &self,
        ctx: &StrategyContext<'_>,
        side: Side,
        price: Price,
        size_multiplier: Decimal,
    ) -> Action {
        let size = self.size_at_least_min_notional(price, size_multiplier);
        Action::Quote(QuoteIntent {
            symbol: ctx.symbol.clone(),
            side,
            price,
            size: Size(size),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        })
    }

    /// Risk-policy config snapshot. Lets the risk module stay stateless
    /// while the strategy owns the configurable knobs.
    fn risk_cfg(&self) -> RiskConfig {
        RiskConfig {
            take_profit_bps: self.config.take_profit_bps,
            stop_loss_bps: self.config.stop_loss_bps,
            take_profit_usdt_legacy: self.config.take_profit_usdt,
        }
    }

    /// Build a fresh intent for `side` at `price` with inventory-bias
    /// `size_multiplier`, then run it through [`policy::diff`] against
    /// the tracked resting quote. Emits the minimum action set
    /// (`[]` / `[Quote]` / `[Cancel, Quote]`) and updates the tracker
    /// on emit. Stage 4 entry point.
    fn diff_emit(
        &mut self,
        ctx: &StrategyContext<'_>,
        side: Side,
        price: Price,
        size_multiplier: Decimal,
    ) -> Vec<Action> {
        let action = self.make_quote(ctx, side, price, size_multiplier);
        let Action::Quote(intent) = action else {
            // make_quote always returns Action::Quote.
            unreachable!()
        };
        let decision = policy_diff(
            self.resting.current_for(side),
            intent.price,
            intent.size,
            Decimal::from(self.config.price_tolerance_ticks),
            self.config.tick_size,
        );
        if matches!(decision, DiffDecision::Unchanged) {
            return Vec::new();
        }
        // Update tracker BEFORE emitting so the next reconcile pass
        // sees the new intent (id will get stamped by reconcile once
        // the runner places it).
        self.resting.record_place(&intent);
        policy_apply(decision, intent)
    }

    /// Emit a side-cancel for a tracked-but-no-longer-wanted side.
    /// Drops the tracker entry too.
    fn drop_tracked_side(&mut self, side: Side) -> Vec<Action> {
        let actions = policy::drop_side(self.resting.current_for(side));
        if !actions.is_empty() || self.resting.current_for(side).is_some() {
            self.resting.drop_side(side);
        }
        actions
    }

    /// Delegates to [`book_state::size_at_least_min_notional`].
    fn size_at_least_min_notional(&self, price: Price, size_multiplier: Decimal) -> Decimal {
        let raw = self.quote_size(price, size_multiplier);
        size_at_least_min_notional(raw, price, self.config.min_notional, self.config.step_size)
    }
    /// Whether the strategy is allowed to (re-)place orders on `side`
    /// right now given the per-side reject cooldown.
    fn side_in_cooldown(&self, side: Side, now: Timestamp) -> bool {
        let cooldown_ms = self.config.reject_cooldown_ms;
        if cooldown_ms == 0 {
            return false;
        }
        let last = match side {
            Side::Bid => self.last_reject_bid_ts,
            Side::Ask => self.last_reject_ask_ts,
        };
        let Some(last) = last else {
            return false;
        };
        let elapsed_ns = now.0.saturating_sub(last.0);
        let cooldown_ns = cooldown_ms.saturating_mul(1_000_000);
        elapsed_ns < cooldown_ns
    }

    fn mark_reject(&mut self, side: Side, ts: Timestamp) {
        match side {
            Side::Bid => self.last_reject_bid_ts = Some(ts),
            Side::Ask => self.last_reject_ask_ts = Some(ts),
        }
    }

    fn clear_reject(&mut self, side: Side) {
        match side {
            Side::Bid => self.last_reject_bid_ts = None,
            Side::Ask => self.last_reject_ask_ts = None,
        }
    }

    fn should_requote(&self, bid: Price, ask: Price, ts: Timestamp) -> bool {
        if let (Some(last_bid), Some(last_ask)) = (self.last_bid, self.last_ask)
            && last_bid.0 == bid.0
            && last_ask.0 == ask.0
        {
            return false;
        }
        let Some(last_ts) = self.last_requote_ts else {
            return true;
        };
        let elapsed_ns = ts.0.saturating_sub(last_ts.0);
        let interval_ns = self.config.requote_interval_ms.saturating_mul(1_000_000);
        elapsed_ns >= interval_ns
    }

    fn emit_requote(
        &mut self,
        ctx: &StrategyContext<'_>,
        bid: Price,
        ask: Price,
        ts: Timestamp,
    ) -> Vec<Action> {
        let size_mult = self.inventory_size_multiplier(ctx);
        self.last_bid = Some(bid);
        self.last_ask = Some(ask);
        self.last_requote_ts = Some(ts);
        self.quotes_live = true;
        // Per-side diff — emit only the deltas instead of nuking both
        // sides every cycle. CancelAll is reserved for `cancel_if_live`
        // (spread narrowed below threshold) and TP closes.
        let mut actions: Vec<Action> = Vec::new();
        let mid = (bid.0 + ask.0) / Decimal::from(2);
        let position_value = ctx.position.size.0.abs() * mid;
        let cap = self.config.max_position_usdt;
        let capped = cap > Decimal::ZERO && position_value >= cap;

        // Stage 5: TP/SL moved to the `risk` module and evaluated at
        // the top of `on_event` — no inline check needed here.

        let want_bid = (!capped || ctx.position.size.0 <= Decimal::ZERO)
            && !self.side_in_cooldown(Side::Bid, ts);
        let want_ask = (!capped || ctx.position.size.0 >= Decimal::ZERO)
            && !self.side_in_cooldown(Side::Ask, ts);
        if want_bid {
            actions.extend(self.diff_emit(ctx, Side::Bid, bid, size_mult.0));
        } else {
            // Side not wanted — drop the resting quote on it.
            actions.extend(self.drop_tracked_side(Side::Bid));
        }
        if want_ask {
            actions.extend(self.diff_emit(ctx, Side::Ask, ask, size_mult.1));
        } else {
            actions.extend(self.drop_tracked_side(Side::Ask));
        }
        if actions.is_empty() {
            actions.push(Action::NoOp);
        }
        actions
    }

    fn inventory_size_multiplier(&self, ctx: &StrategyContext<'_>) -> (Decimal, Decimal) {
        let size = ctx.position.size.0;
        if size > Decimal::ZERO {
            (Decimal::ONE, Decimal::from(2))
        } else if size < Decimal::ZERO {
            (Decimal::from(2), Decimal::ONE)
        } else {
            (Decimal::ONE, Decimal::ONE)
        }
    }

    fn cancel_if_live(&mut self, _ts: Timestamp) -> Vec<Action> {
        // Resetting `last_requote_ts = None` (instead of advancing it to
        // `ts`) lets us re-enter immediately when targets become valid
        // again. Stamping it would have parked the next requote behind
        // `requote_interval_ms` even though we just cancelled — a quiet
        // stall on spread re-widening.
        self.last_bid = None;
        self.last_ask = None;
        self.last_requote_ts = None;
        if self.quotes_live {
            self.quotes_live = false;
            self.resting.drop_all();
            vec![Action::CancelAll]
        } else {
            Vec::new()
        }
    }

    /// Which side is the "closing" side for the current position. Long
    /// → Ask reduces. Short → Bid reduces. Flat → None (nothing to
    /// close). Used by `try_keep_close_side` to filter the cancel-when-
    /// targets-unavailable path.
    fn close_side_for(position_size: Decimal) -> Option<Side> {
        if position_size > Decimal::ZERO {
            Some(Side::Ask)
        } else if position_size < Decimal::ZERO {
            Some(Side::Bid)
        } else {
            None
        }
    }

    /// When `compute_targets` returned None (spread below threshold)
    /// AND we hold inventory AND `close_side_always_quotes` is on,
    /// keep the close-side quote alive at the ORIGINAL profit target
    /// (avg_entry ± min_spread_bps) so the position drains at maker
    /// fee for the spread we originally tried to capture — not at
    /// whatever the current touch happens to be (which can be a
    /// loss when the market cools off in the direction we hold).
    ///
    /// Two cases:
    /// - **Market moved against us** (mark closer to or past entry):
    ///   target sits above touch (long) / below touch (short).
    ///   PostOnly happily rests there until a rebound or taker fills.
    /// - **Market moved past our target in our favour**: we'd be
    ///   leaving money on the table by posting at the original
    ///   target. Use the more aggressive touch quote
    ///   (best_ask − 1 tick for long, best_bid + 1 tick for short)
    ///   so we capture the bonus instead of sitting deep behind the
    ///   queue.
    ///
    /// Returns `Some(actions)` when the close-side path engaged (a
    /// quote was posted or kept alive + add-side cancelled). `None`
    /// when the gate is off, the position is flat, or the book is
    /// unusable — caller falls back to `cancel_if_live`.
    fn try_keep_close_side(
        &mut self,
        ctx: &StrategyContext<'_>,
        snapshot: &Snapshot,
        ts: Timestamp,
    ) -> Option<Vec<Action>> {
        if !self.config.close_side_always_quotes {
            return None;
        }
        let close_side = Self::close_side_for(ctx.position.size.0)?;
        let top = Top::from_snapshot(snapshot)?;
        let entry = ctx.position.avg_entry.0;
        if entry <= Decimal::ZERO {
            return None;
        }
        let tick = self.config.tick_size;
        let bp = self.config.min_spread_bps / Decimal::from(10_000);
        // Profit-target price relative to entry. The min_spread_bps
        // value already encodes the operator's intended round-trip
        // capture, so re-using it here keeps "what we were trying to
        // earn" consistent with "what we hold out for on the exit".
        let target_from_entry = match close_side {
            Side::Ask => Price(entry * (Decimal::ONE + bp)),
            Side::Bid => Price(entry * (Decimal::ONE - bp)),
        };
        // Aggressive touch fallback for the case where the market
        // moved PAST our target — use 1 tick inside the touch so a
        // natural taker closes us at the bonus price.
        let aggressive_touch = match close_side {
            Side::Ask => Price(top.ask.0 - tick),
            Side::Bid => Price(top.bid.0 + tick),
        };
        let price = match close_side {
            // Long → take the HIGHER ask of (target_from_entry,
            // aggressive_touch). If market rallied past target,
            // aggressive_touch wins.
            Side::Ask => {
                if aggressive_touch.0 > target_from_entry.0 {
                    aggressive_touch
                } else {
                    target_from_entry
                }
            }
            // Short → take the LOWER bid.
            Side::Bid => {
                if aggressive_touch.0 < target_from_entry.0 {
                    aggressive_touch
                } else {
                    target_from_entry
                }
            }
        };
        // Safety: refuse to post if it would cross the opposite side
        // (PostOnly would reject anyway; cheaper to skip). Long's Ask
        // must be > best_bid; short's Bid must be < best_ask.
        let crosses = match close_side {
            Side::Ask => price.0 <= top.bid.0,
            Side::Bid => price.0 >= top.ask.0,
        };
        if crosses {
            // Falling through to cancel_if_live is wrong here — we'd
            // strand the position. Return empty so the existing
            // resting close-side quote (if any) stays in place; no
            // new placement attempt.
            return Some(Vec::new());
        }
        // Drop the add-side regardless of whether we already have one.
        let add_side = match close_side {
            Side::Ask => Side::Bid,
            Side::Bid => Side::Ask,
        };
        let mut actions = self.drop_tracked_side(add_side);
        // Mirror cancel_if_live's per-side bookkeeping for the side we
        // just cancelled so the next refresh re-enters cleanly.
        match add_side {
            Side::Bid => self.last_bid = None,
            Side::Ask => self.last_ask = None,
        }
        // Place / refresh the close-side quote.
        let close_actions = self.diff_emit(ctx, close_side, price, Decimal::ONE);
        actions.extend(close_actions);
        // Stamp state: we still have at least the close side live.
        // Reset last_requote_ts so the next spread-widening event can
        // immediately switch back to full two-sided quoting without
        // sitting behind requote_interval_ms.
        self.last_requote_ts = Some(ts);
        match close_side {
            Side::Ask => self.last_ask = Some(price),
            Side::Bid => self.last_bid = Some(price),
        }
        self.quotes_live = self.resting.current_for(close_side).is_some();
        Some(actions)
    }
}

impl Strategy for SpreadScalp {
    type Config = SpreadScalpConfig;

    fn new(config: Self::Config) -> Self {
        let adverse = AdverseTracker::new(config.adverse);
        Self {
            config,
            last_bid: None,
            last_ask: None,
            last_requote_ts: None,
            quotes_live: false,
            last_reject_bid_ts: None,
            last_reject_ask_ts: None,
            resting: RestingOrders::new(),
            adverse,
        }
    }

    fn name(&self) -> &str {
        "spread-scalp"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        // Pull venue truth into the tracker so in-flight quotes get
        // their ids stamped + ghosts (silently cancelled / expired)
        // drop out of our view. Cheap — `ctx.open_quotes` is a slice,
        // reconcile is O(N).
        self.resting.reconcile(ctx.open_quotes);

        // Stage 6: fold any due post-fill snapshots into the adverse
        // tracker. Cheap, runs every event but no-op when nothing's
        // pending or the tracker is disabled.
        if let Some(top_for_adverse) = Top::from_snapshot(ctx.latest_book) {
            self.adverse
                .process_due_snapshots(ctx.now, top_for_adverse.mid());
        }

        // Stage 6: record fills for adverse-drift tracking. We do
        // this BEFORE the risk gate so a TP-triggered close itself
        // doesn't poison the EMA (TP fires at a favourable moment by
        // construction). NOTE: this fires on EVERY Fill, including
        // partials — adverse drift is the same regardless of fill
        // fraction.
        if let MarketEvent::Fill(fill) = event {
            self.adverse.record_fill(fill.ts, fill.side, fill.price);
        }

        // Stage 5 risk gate: TP/SL checked every event, before any
        // requote logic. Fires regardless of `min_spread_bps` — we want
        // to close on a favourable spike even when the book is too
        // tight to scalp. Reducing-side IOC at the opposing touch is
        // a guaranteed taker fill (subject to venue liquidity at touch).
        let mut cap_cancels: Vec<Action> = Vec::new();
        if let Some(top) = Top::from_snapshot(ctx.latest_book) {
            let mid = top.mid();
            if let RiskDecision::Close { side, qty, .. } =
                risk::evaluate(ctx.position, mid, self.risk_cfg())
            {
                // Stage 5 fires close + cancels any resting quotes so
                // the runner doesn't keep two intents alive on the
                // same side. CancelAll is justified here — we're
                // exiting the position, not refining quotes.
                self.resting.drop_all();
                self.quotes_live = false;
                self.last_bid = None;
                self.last_ask = None;
                return vec![
                    Action::CancelAll,
                    risk::build_close(ctx.symbol, side, qty, top.bid, top.ask),
                ];
            }

            // Stage 4 bug-fix: position-cap-driven side cancel must
            // fire on EVERY event, not only when `should_requote`
            // returns true. The previous design put this gate inside
            // `emit_requote` — which is skipped when prices haven't
            // moved enough. In a stable-book live setting that meant
            // the bid kept resting + filling past the cap (the live
            // BTC bot accumulated 36× over its 100-USDT cap that way).
            //
            // Now: on every event, check position vs cap; if breached
            // on a side, drop that side's resting quote (which emits
            // Cancel(id) if the runner has stamped the venue id).
            let position_value = ctx.position.size.0.abs() * mid.0;
            let cap = self.config.max_position_usdt;
            let capped = cap > Decimal::ZERO && position_value >= cap;
            if capped {
                if ctx.position.size.0 > Decimal::ZERO {
                    // Long over cap — kill the resting bid that keeps
                    // adding to inventory.
                    cap_cancels.extend(self.drop_tracked_side(Side::Bid));
                } else if ctx.position.size.0 < Decimal::ZERO {
                    cap_cancels.extend(self.drop_tracked_side(Side::Ask));
                }
            }
        }

        let (snapshot, ts) = match event {
            MarketEvent::BookUpdate { snapshot } => (snapshot, snapshot.ts),
            MarketEvent::Heartbeat { ts } => (ctx.latest_book, *ts),
            MarketEvent::Trade { .. } => return Vec::new(),
            MarketEvent::Fill(fill) => {
                let ts = ctx.now;
                // Fill = inventory just moved; whatever rejection state
                // was tracked for the filled side is stale. The tracked
                // resting quote on the filled side is also gone now.
                self.clear_reject(fill.side);
                self.resting.drop_side(fill.side);
                let Some((bid, ask)) = self.compute_targets(ctx.latest_book) else {
                    // Spread too tight for normal entry. If we hold
                    // inventory + close_side_always_quotes is on, keep
                    // the close-side passive quote alive so the
                    // position can drain at maker fee; otherwise drop
                    // everything.
                    if let Some(actions) = self.try_keep_close_side(ctx, ctx.latest_book, ts) {
                        return actions;
                    }
                    return self.cancel_if_live(ts);
                };
                let size_mult = self.inventory_size_multiplier(ctx);
                let fill_side = fill.side;
                let opp_side = if fill_side == Side::Bid {
                    Side::Ask
                } else {
                    Side::Bid
                };
                self.last_bid = Some(bid);
                self.last_ask = Some(ask);
                self.last_requote_ts = Some(ts);
                self.quotes_live = true;

                // Position-cap gate — `emit_requote` honoured it, the
                // fill path did not. A Bid fill drives us long; if we
                // already passed the cap, suppress the replacement Bid
                // (and any opp-side top-up that would lean further in
                // the same direction). Same logic mirrored for Ask /
                // short. `<= 0` / `>= 0` keeps the inclusive flat case.
                let mid = (bid.0 + ask.0) / Decimal::from(2);
                let position_value = ctx.position.size.0.abs() * mid;
                let cap = self.config.max_position_usdt;
                let capped = cap > Decimal::ZERO && position_value >= cap;
                let allow_bid = (!capped || ctx.position.size.0 <= Decimal::ZERO)
                    && !self.side_in_cooldown(Side::Bid, ts);
                let allow_ask = (!capped || ctx.position.size.0 >= Decimal::ZERO)
                    && !self.side_in_cooldown(Side::Ask, ts);

                // Filled side replacement.
                let mut actions = Vec::new();
                let allow_filled_side = match fill_side {
                    Side::Bid => allow_bid,
                    Side::Ask => allow_ask,
                };
                if allow_filled_side {
                    let action = self.make_quote(
                        ctx,
                        fill_side,
                        if fill_side == Side::Bid { bid } else { ask },
                        if fill_side == Side::Bid {
                            size_mult.0
                        } else {
                            size_mult.1
                        },
                    );
                    if let Action::Quote(intent) = &action {
                        self.resting.record_place(intent);
                    }
                    actions.push(action);
                }

                // Opp-side top-up — only if cap allows growing that side.
                let allow_opp_side = match opp_side {
                    Side::Bid => allow_bid,
                    Side::Ask => allow_ask,
                };
                if allow_opp_side {
                    let opp_mult = if opp_side == Side::Bid {
                        size_mult.0
                    } else {
                        size_mult.1
                    };
                    let opp_price = if opp_side == Side::Bid { bid } else { ask };
                    let existing_opp: Decimal = ctx
                        .open_quotes
                        .iter()
                        .filter(|q| q.1.side == opp_side)
                        .map(|q| q.1.size.0)
                        .sum();
                    let desired_total = self.quote_size(opp_price, opp_mult);
                    if desired_total > existing_opp {
                        let extra = desired_total - existing_opp;
                        let step = self.config.step_size;
                        let extra = if step > Decimal::ZERO {
                            (extra / step).floor() * step
                        } else {
                            extra
                        };
                        if extra > Decimal::ZERO {
                            let intent = QuoteIntent {
                                symbol: ctx.symbol.clone(),
                                side: opp_side,
                                price: opp_price,
                                size: Size(extra),
                                tif: TimeInForce::PostOnly,
                                kind: QuoteKind::Point,
                            };
                            // Opp-side top-up adds to whatever's already
                            // tracked at that side. We overwrite the
                            // entry with the latest intent — Stage 4's
                            // multi-quote support will replace this
                            // with proper additive bookkeeping.
                            self.resting.record_place(&intent);
                            actions.push(Action::Quote(intent));
                        }
                    }
                }
                // Prepend cap-driven cancels collected at the top of
                // on_event so a Fill that puts us over the cap also
                // tears down the exposing side immediately.
                let mut out = cap_cancels;
                out.extend(actions);
                return out;
            }
        };
        let Some((bid, ask)) = self.compute_targets(snapshot) else {
            // Spread below threshold. Try to keep the close-side
            // passive quote alive (when configured + holding inventory)
            // so the position doesn't sit naked once the cascade event
            // that triggered the entry cools off; fall back to
            // cancel_if_live when not applicable. cap_cancels merge
            // preserved either way.
            let mut actions = cap_cancels;
            if let Some(close_actions) = self.try_keep_close_side(ctx, snapshot, ts) {
                actions.extend(close_actions);
            } else {
                actions.extend(self.cancel_if_live(ts));
            }
            return actions;
        };
        if !self.should_requote(bid, ask, ts) {
            // CRITICAL: do NOT return a bare NoOp here — that swallows
            // any cap-driven cancels we built at the top of on_event.
            if cap_cancels.is_empty() {
                return vec![Action::NoOp];
            }
            return cap_cancels;
        }
        let mut actions = cap_cancels;
        actions.extend(self.emit_requote(ctx, bid, ask, ts));
        actions
    }

    fn on_quote_rejected(
        &mut self,
        ctx: &StrategyContext<'_>,
        intent: &tikr_venue::QuoteIntent,
        _reason: &str,
    ) -> Vec<Action> {
        // Stamp the cooldown on the side that just bounced so the next
        // rebuild attempt within `reject_cooldown_ms` skips this side
        // (see SG `last_refill_*_ts` for the parent pattern). Without
        // this, fast moves can produce a -5022 → rebuild → -5022 hot
        // loop because every rejection nukes the price cache and
        // re-emits both sides.
        let ts = ctx.now;
        self.mark_reject(intent.side, ts);
        self.last_bid = None;
        self.last_ask = None;
        self.last_requote_ts = None;
        let Some((bid, ask)) = self.compute_targets(ctx.latest_book) else {
            if let Some(actions) = self.try_keep_close_side(ctx, ctx.latest_book, ts) {
                return actions;
            }
            return self.cancel_if_live(ts);
        };
        self.emit_requote(ctx, bid, ask, ts)
    }

    fn on_notional_updated(
        &mut self,
        _ctx: &StrategyContext<'_>,
        notional_per_order: Decimal,
    ) -> Vec<Action> {
        if notional_per_order > Decimal::ZERO {
            self.config.notional_per_order = notional_per_order;
        }
        Vec::new()
    }

    fn on_max_position_updated(
        &mut self,
        _ctx: &StrategyContext<'_>,
        max_position_usdt: Decimal,
    ) -> Vec<Action> {
        if max_position_usdt > Decimal::ZERO {
            self.config.max_position_usdt = max_position_usdt;
        }
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::{Asset, Level, MarketKind, Notional, Position, SignedSize, Symbol, VenueId};

    fn sym() -> Symbol {
        Symbol {
            base: Asset::new("BTC"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("test"),
            kind: MarketKind::Perp,
        }
    }

    fn book(symbol: &Symbol, bid: i64, ask: i64, ts: u64) -> Snapshot {
        Snapshot {
            symbol: symbol.clone(),
            bids: vec![Level {
                price: Price(Decimal::from(bid)),
                size: Size(Decimal::ONE),
            }],
            asks: vec![Level {
                price: Price(Decimal::from(ask)),
                size: Size(Decimal::ONE),
            }],
            ts: Timestamp(ts),
        }
    }

    fn pos(symbol: &Symbol) -> Position {
        Position {
            symbol: symbol.clone(),
            size: SignedSize(Decimal::ZERO),
            avg_entry: Price(Decimal::ZERO),
            realized_pnl: Notional(Decimal::ZERO),
        }
    }

    fn pos_with_size(symbol: &Symbol, size: Decimal) -> Position {
        Position {
            symbol: symbol.clone(),
            size: SignedSize(size),
            avg_entry: Price(Decimal::ZERO),
            realized_pnl: Notional(Decimal::ZERO),
        }
    }

    fn ctx<'a>(
        symbol: &'a Symbol,
        snapshot: &'a Snapshot,
        position: &'a Position,
    ) -> StrategyContext<'a> {
        StrategyContext {
            symbol,
            now: snapshot.ts,
            position,
            recent_fills: &[],
            latest_book: snapshot,
            open_quotes: &[],
            recent_liqs: &[],
        }
    }

    fn strategy() -> SpreadScalp {
        SpreadScalp::new(SpreadScalpConfig {
            notional_per_order: Decimal::from(100),
            tick_size: Decimal::from(1),
            step_size: Decimal::from(1),
            min_notional: Decimal::ZERO,
            min_spread_bps: Decimal::from(5),
            requote_interval_ms: 1000,
            max_position_usdt: Decimal::ZERO,
            take_profit_usdt: Decimal::ZERO,
            reject_cooldown_ms: 0,
            price_tolerance_ticks: 0,
            take_profit_bps: 0,
            stop_loss_bps: 0,
            adverse: AdverseConfig::disabled(),
            // Default-on in production; default-OFF in unit tests so
            // existing flat-position cancel-on-tight-spread assertions
            // don't regress. Tests that need the new behaviour can opt
            // in by mutating this field on the config.
            close_side_always_quotes: false,
        })
    }

    #[test]
    fn wide_spread_quotes_at_best() {
        let symbol = sym();
        let snapshot = book(&symbol, 100, 110, 1);
        let position = pos(&symbol);
        let mut strategy = strategy();
        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        assert_eq!(actions.len(), 2);
        match (&actions[0], &actions[1]) {
            (Action::Quote(bid), Action::Quote(ask)) => {
                // 1 tick inside best bid/ask
                assert_eq!(bid.price.0, Decimal::from(101));
                assert_eq!(ask.price.0, Decimal::from(109));
            }
            _ => panic!("expected quotes"),
        }
    }

    #[test]
    fn narrow_spread_does_not_quote() {
        let symbol = sym();
        let snapshot = book(&symbol, 100, 100, 1);
        let position = pos(&symbol);
        let mut strategy = strategy();
        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        assert!(
            actions.is_empty(),
            "narrow spread should produce no actions, got {:?}",
            actions
        );
    }

    #[test]
    fn does_not_requote_when_already_at_best() {
        let symbol = sym();
        let snapshot = book(&symbol, 100, 110, 1);
        let position = pos(&symbol);
        let mut strategy = strategy();
        let first = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        assert_eq!(first.len(), 2);

        let second = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        assert!(matches!(second.as_slice(), [Action::NoOp]));
    }

    #[test]
    fn requotes_when_market_moves() {
        let symbol = sym();
        let first = book(&symbol, 100, 110, 1);
        let moved = book(&symbol, 102, 112, 2_000_000_000);
        let position = pos(&symbol);
        let mut strategy = strategy();
        let _ = strategy.on_event(
            &ctx(&symbol, &first, &position),
            &MarketEvent::BookUpdate {
                snapshot: first.clone(),
            },
        );
        let actions = strategy.on_event(
            &ctx(&symbol, &moved, &position),
            &MarketEvent::BookUpdate {
                snapshot: moved.clone(),
            },
        );
        assert_eq!(actions.len(), 2);
        match (&actions[0], &actions[1]) {
            (Action::Quote(bid), Action::Quote(ask)) => {
                // 1 tick inside best bid/ask
                assert_eq!(bid.price.0, Decimal::from(103));
                assert_eq!(ask.price.0, Decimal::from(111));
            }
            _ => panic!("expected quotes"),
        }
    }

    #[test]
    fn long_inventory_sizes_ask_larger() {
        let symbol = sym();
        let snapshot = book(&symbol, 50, 60, 1);
        let position = pos_with_size(&symbol, Decimal::new(5, 1));
        let mut strategy = strategy();
        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        assert_eq!(actions.len(), 2);
        match (&actions[0], &actions[1]) {
            (Action::Quote(bid), Action::Quote(ask)) => {
                assert!(
                    ask.size.0 > bid.size.0,
                    "ask={} bid={}",
                    ask.size.0,
                    bid.size.0
                );
            }
            _ => panic!("expected quotes"),
        }
    }

    #[test]
    fn short_inventory_sizes_bid_larger() {
        let symbol = sym();
        let snapshot = book(&symbol, 100, 110, 1);
        let position = pos_with_size(&symbol, Decimal::new(-5, 1));
        let mut strategy = strategy();
        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        assert_eq!(actions.len(), 2);
        match (&actions[0], &actions[1]) {
            (Action::Quote(bid), Action::Quote(ask)) => {
                assert!(bid.size.0 > ask.size.0);
            }
            _ => panic!("expected quotes"),
        }
    }

    #[test]
    fn cancel_when_spread_narrows() {
        let symbol = sym();
        let wide = book(&symbol, 100, 110, 1);
        let narrow = book(&symbol, 100, 100, 2);
        let position = pos(&symbol);
        let mut strategy = strategy();
        let _ = strategy.on_event(
            &ctx(&symbol, &wide, &position),
            &MarketEvent::BookUpdate {
                snapshot: wide.clone(),
            },
        );
        let actions = strategy.on_event(
            &ctx(&symbol, &narrow, &position),
            &MarketEvent::BookUpdate {
                snapshot: narrow.clone(),
            },
        );
        assert!(matches!(actions.as_slice(), [Action::CancelAll]));
    }

    #[test]
    fn fill_triggers_requote() {
        let symbol = sym();
        let snapshot = book(&symbol, 100, 110, 1);
        let position = pos(&symbol);
        let mut strategy = strategy();
        let _ = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::Fill(tikr_core::Fill {
                quote_id: tikr_venue::QuoteId::new(),
                price: Price(Decimal::from(101)),
                size: Size(Decimal::ONE),
                fee_asset: Asset::new("USDT"),
                fee_amount: Decimal::ZERO,
                fee_quote: Notional(Decimal::ZERO),
                side: Side::Bid,
                ts: Timestamp(2),
                is_full: true,
            }),
        );
        // Fill replaces only the filled side; opposite side stays live.
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::Quote(q) => assert_eq!(q.side, Side::Bid),
            other => panic!("expected Quote, got {:?}", other),
        }
    }
}
