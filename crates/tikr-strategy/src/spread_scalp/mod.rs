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
/// and bps-of-notional evaluation as SS. Re-exported here so existing
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
    /// Time-decay step 1 (seconds). After this many seconds holding the
    /// current cycle's position, multiply the close-target distance by
    /// `close_decay_factor_1` (e.g. 0.7) to ratchet the TP closer.
    /// Captures partial wins when reversion stalls. `0` disables.
    pub close_decay_after_secs_1: u64,
    /// Multiplier applied after `close_decay_after_secs_1`. Sensible
    /// range 0.3-0.9. `1.0` is a no-op (no decay).
    pub close_decay_factor_1: Decimal,
    /// Time-decay step 2 (seconds). After this many seconds, multiply
    /// by `close_decay_factor_2` (tighter than step 1). `0` disables.
    pub close_decay_after_secs_2: u64,
    /// Multiplier applied after `close_decay_after_secs_2`. Typically
    /// smaller than `close_decay_factor_1` (e.g. 0.5).
    pub close_decay_factor_2: Decimal,
    /// Adverse-drift hard stop: after `adverse_stop_after_secs` of
    /// holding, if mid has drifted >= `adverse_stop_drift_bps` against
    /// the position direction (long → mid below avg_entry), IOC close
    /// at opposing touch. Caps the bad-tail when reversion never comes.
    /// `0` disables.
    pub adverse_stop_after_secs: u64,
    /// Bps drift threshold from `avg_entry` that triggers the adverse
    /// stop (only after `adverse_stop_after_secs` elapsed). `0`
    /// disables even when the time gate is set.
    pub adverse_stop_drift_bps: u32,
    /// Tick offset from touch for quote placement.
    ///
    /// - `-1` (default) — legacy SS: quote 1 tick INSIDE the touch
    ///   (`bid = top.bid + 1 tick`, `ask = top.ask − 1 tick`). Captures
    ///   dislocation cascades when book widens past `min_spread_bps`.
    ///   Requires book spread ≥ 2 ticks (otherwise quotes would cross).
    ///
    /// - `0` — join the queue AT the touches (`bid = top.bid`,
    ///   `ask = top.ask`). Works even on 1-tick-wide books. Sits at
    ///   the back of the queue at each touch level.
    ///
    /// - `+1`, `+2`, … — quote N ticks OUTSIDE the touches
    ///   (`bid = top.bid − N×tick`, `ask = top.ask + N×tick`). Tick-floor
    ///   sitter mode: we own our own level, no queue competition. Each
    ///   RT captures `(2N+1) × tick_bps` gross. Best fit for wide-tick
    ///   symbols (WIF/ADA/SAGA/ESPORTS at 4-20bps per tick) where book
    ///   spread is structurally 1 tick.
    ///
    /// When `>= 0`, the "would-cross" guard in compute_targets is a
    /// no-op (we're at or outside touches, never inside).
    pub quote_offset_ticks: i32,
    /// Close-side target distance in TICKS from `avg_entry`, used only
    /// when `quote_offset_ticks >= 0` (tick mode). Default `0` means
    /// "auto" — derives `quote_offset_ticks + 1` so a (2N+1)-tick RT
    /// is the natural exit symmetric to the entry placement.
    ///
    /// Tick mode close logic: place the closing quote at
    ///   long  → ask @ max(avg_entry + N×tick, touch_based_ask)
    ///   short → bid @ min(avg_entry − N×tick, touch_based_bid)
    /// This guarantees we never close at a loss (the avg-anchored
    /// floor) while still capturing the bonus when the market has
    /// moved past target (the touch-based fallback). Pure tick math —
    /// no bps involved.
    ///
    /// Has no effect in legacy SS mode (`quote_offset_ticks = -1`):
    /// that path still uses `min_spread_bps` via `try_keep_close_side`.
    pub close_target_ticks: u32,
    /// When `true`, **bypasses the close-side avg-anchored pin** so the
    /// close-side quote always sits at touch (same as the entry side).
    /// Removes the "1-tick gap above touch_ask when holding long" symptom
    /// when avg_entry has drifted above the current touch. Tradeoff: the
    /// close quote can now realize a per-cycle loss if the touch is below
    /// avg + close_target × tick — the strategy gives up the avg-anchored
    /// floor in exchange for always being at the front of the book.
    /// Default `false` preserves the protective pin behavior.
    pub strict_touch_quotes: bool,
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
    /// Timestamp of the first fill of the current inventory cycle.
    /// Set when position transitions zero → non-zero; cleared when it
    /// returns to zero. Drives time-decay close target + adverse stop.
    cycle_start_ts: Option<Timestamp>,
    /// Cached signed position size at the previous event. Used to
    /// detect zero ↔ non-zero transitions for cycle_start_ts tracking
    /// without needing a separate Fill-hook path.
    prev_pos_size: Decimal,
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
        // Quote placement is parameterised by `quote_offset_ticks`:
        //   -1 (default) — 1 tick INSIDE touches (legacy SS, captures
        //       cascade widenings; requires book >= 2 ticks wide).
        //    0           — AT touches (joins queue at top bid/ask).
        //   +1, +2…      — N ticks OUTSIDE touches (tick-floor sitter:
        //       owns its own level, captures (2N+1) ticks per RT).
        //
        // The would-cross guard below only fires when offset == -1 and
        // book is at the 1-tick floor; for offset >= 0 the quotes are
        // at or outside touches, never crossing.
        let off = self.config.quote_offset_ticks;
        let (bid, ask) = if off >= 0 {
            let n = Decimal::from(off);
            (Price(top.bid.0 - n * tick), Price(top.ask.0 + n * tick))
        } else {
            // off < 0 → inside touches by |off| ticks. Negate to get
            // positive distance; for off=-1, inside by 1 tick.
            let n = Decimal::from(-off);
            (Price(top.bid.0 + n * tick), Price(top.ask.0 - n * tick))
        };
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

    /// Close-side emit with a pinned `qty` (= position size to flatten).
    /// Skips notional-derived sizing so price jitter within
    /// `price_tolerance_ticks` no longer triggers Replace from a 1-lot
    /// size recompute. `qty` must already be lot-step-compliant (venue
    /// fill aggregation guarantees this).
    fn diff_emit_close(
        &mut self,
        ctx: &StrategyContext<'_>,
        side: Side,
        price: Price,
        qty: Decimal,
    ) -> Vec<Action> {
        // Skip if qty falls below min-notional (residual dust). Adverse
        // stop or natural drift will mop up; emitting would just earn
        // -2010 reject + cooldown churn.
        if self.config.min_notional > Decimal::ZERO && qty * price.0 < self.config.min_notional {
            return Vec::new();
        }
        let intent = QuoteIntent {
            symbol: ctx.symbol.clone(),
            side,
            price,
            size: Size(qty),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
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
        self.resting.record_place(&intent);
        policy_apply(decision, intent)
    }

    /// Emit a side-cancel for a tracked-but-no-longer-wanted side.
    ///
    /// Only drops the tracker entry when a Cancel was actually emitted
    /// (i.e. the resting quote has a venue id). When the resting entry
    /// is in-flight (no id yet), keep it — otherwise the in-flight
    /// place lands successfully AFTER the tracker forgets it, leaving
    /// an orphan on the venue forever. The next `reconcile` will stamp
    /// the id and the subsequent cycle will emit the Cancel cleanly.
    fn drop_tracked_side(&mut self, side: Side) -> Vec<Action> {
        let actions = policy::drop_side(self.resting.current_for(side));
        if !actions.is_empty() {
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
        // Tick mode close-side override. When in tick mode (offset >= 0)
        // and holding inventory, the close-side quote from compute_targets
        // is touch-anchored — it follows the market and can sit BELOW
        // avg_entry (covering at a loss). Replace it with an avg-anchored
        // target that takes max(avg+N_tick, touch) for long close,
        // min(avg-N_tick, touch) for short close. Pure tick math.
        let (bid, ask) = self.apply_tick_mode_close_target(ctx, bid, ask);
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
        // Close-side qty pin: when holding inventory, the side that
        // reduces it (long→Ask, short→Bid) is sized to abs(position.size)
        // rather than notional/price. Eliminates lot-step jitter from
        // recomputed sizes when price moves within price_tolerance_ticks.
        let close_side = Self::close_side_for(ctx.position.size.0);
        let close_qty = ctx.position.size.0.abs();
        if want_bid {
            if close_side == Some(Side::Bid) && close_qty > Decimal::ZERO {
                actions.extend(self.diff_emit_close(ctx, Side::Bid, bid, close_qty));
            } else {
                actions.extend(self.diff_emit(ctx, Side::Bid, bid, size_mult.0));
            }
        } else {
            // Side not wanted — drop the resting quote on it.
            actions.extend(self.drop_tracked_side(Side::Bid));
        }
        if want_ask {
            if close_side == Some(Side::Ask) && close_qty > Decimal::ZERO {
                actions.extend(self.diff_emit_close(ctx, Side::Ask, ask, close_qty));
            } else {
                actions.extend(self.diff_emit(ctx, Side::Ask, ask, size_mult.1));
            }
        } else {
            actions.extend(self.drop_tracked_side(Side::Ask));
        }
        if actions.is_empty() {
            actions.push(Action::NoOp);
        }
        actions
    }

    /// In tick mode (`quote_offset_ticks >= 0`) with non-zero inventory,
    /// override the close-side quote price with an avg-anchored target
    /// using pure tick math (no bps). Returns possibly-modified `(bid, ask)`.
    ///
    /// Long  → ask close: max(avg_entry + N×tick, touch_ask) — never
    ///   close below profitable target; capture bonus if market is past.
    /// Short → bid close: min(avg_entry − N×tick, touch_bid) — never
    ///   cover above profitable target; capture bonus if market is past.
    ///
    /// N = `close_target_ticks` config, defaulting to `quote_offset_ticks + 1`
    /// (so a 2N+1 tick RT is the natural exit symmetric to entry).
    fn apply_tick_mode_close_target(
        &self,
        ctx: &StrategyContext<'_>,
        bid: Price,
        ask: Price,
    ) -> (Price, Price) {
        // strict_touch_quotes bypass: caller wants close-side to sit at
        // touch (entry-side parity) even when that means closing at a
        // loss relative to avg_entry. Skip the pin entirely.
        if self.config.strict_touch_quotes {
            return (bid, ask);
        }
        if self.config.quote_offset_ticks < 0 {
            return (bid, ask);
        }
        let pos_size = ctx.position.size.0;
        if pos_size == Decimal::ZERO {
            return (bid, ask);
        }
        let avg = ctx.position.avg_entry.0;
        if avg <= Decimal::ZERO {
            return (bid, ask);
        }
        let n_ticks = if self.config.close_target_ticks > 0 {
            self.config.close_target_ticks as i32
        } else {
            self.config.quote_offset_ticks + 1
        };
        let tick = self.config.tick_size;
        let target_distance = Decimal::from(n_ticks) * tick;
        if pos_size > Decimal::ZERO {
            // Long: close is ASK. Target = avg + N*tick (above entry).
            // Use max(target, touch-based ask) so we never sit below
            // target but DO capture bonus when market is past.
            let target_ask = Price(avg + target_distance);
            let safe_ask = if target_ask.0 > ask.0 {
                target_ask
            } else {
                ask
            };
            (bid, safe_ask)
        } else {
            // Short: close is BID. Target = avg - N*tick (below entry).
            // Use min(target, touch-based bid) so we never sit above
            // target but DO capture bonus when market is past.
            let target_bid = Price(avg - target_distance);
            let safe_bid = if target_bid.0 < bid.0 {
                target_bid
            } else {
                bid
            };
            (safe_bid, ask)
        }
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
        // Time-decay close target: ratchet bp distance down after
        // configured hold thresholds. 0-step config = no decay (factor
        // = 1.0). Decay step 2 supersedes step 1 once its threshold
        // is reached.
        let decay_factor = self
            .cycle_start_ts
            .map(|cycle_ts| {
                let held_ns = ts.0.saturating_sub(cycle_ts.0);
                let secs_1 = self.config.close_decay_after_secs_1;
                let secs_2 = self.config.close_decay_after_secs_2;
                let f1 = self.config.close_decay_factor_1;
                let f2 = self.config.close_decay_factor_2;
                if secs_2 > 0
                    && held_ns >= secs_2.saturating_mul(1_000_000_000)
                    && f2 > Decimal::ZERO
                {
                    f2
                } else if secs_1 > 0
                    && held_ns >= secs_1.saturating_mul(1_000_000_000)
                    && f1 > Decimal::ZERO
                {
                    f1
                } else {
                    Decimal::ONE
                }
            })
            .unwrap_or(Decimal::ONE);
        let bp = self.config.min_spread_bps * decay_factor / Decimal::from(10_000);
        // Profit-target price relative to entry. The min_spread_bps
        // value already encodes the operator's intended round-trip
        // capture, so re-using it here keeps "what we were trying to
        // earn" consistent with "what we hold out for on the exit".
        // After decay, the target ratchets closer so partial wins
        // bank when reversion stalls (vs holding out forever).
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
            cycle_start_ts: None,
            prev_pos_size: Decimal::ZERO,
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

        // Cycle tracking: detect zero ↔ non-zero transitions to anchor
        // the time-decay close target + adverse stop. Set when entering
        // a new cycle (Flat → Holding), cleared on close (Holding → Flat).
        let pos_size = ctx.position.size.0;
        if self.prev_pos_size == Decimal::ZERO && pos_size != Decimal::ZERO {
            self.cycle_start_ts = Some(ctx.now);
        } else if self.prev_pos_size != Decimal::ZERO && pos_size == Decimal::ZERO {
            self.cycle_start_ts = None;
        }
        self.prev_pos_size = pos_size;

        // Adverse-drift hard stop. Fires before normal flow when:
        //  - configured (both bps + secs > 0)
        //  - position non-zero
        //  - cycle held >= adverse_stop_after_secs
        //  - mid drifted >= adverse_stop_drift_bps against position direction
        // Returns an IOC at the opposing touch. Bounds the bad-tail when
        // reversion never comes.
        if self.config.adverse_stop_drift_bps > 0
            && self.config.adverse_stop_after_secs > 0
            && pos_size != Decimal::ZERO
            && let Some(cycle_ts) = self.cycle_start_ts
            && let Some(top) = Top::from_snapshot(ctx.latest_book)
        {
            let held_ns = ctx.now.0.saturating_sub(cycle_ts.0);
            let stop_ns = self
                .config
                .adverse_stop_after_secs
                .saturating_mul(1_000_000_000);
            if held_ns >= stop_ns {
                let mid = top.mid().0;
                let avg = ctx.position.avg_entry.0;
                if avg > Decimal::ZERO {
                    let drift_bps = if pos_size > Decimal::ZERO {
                        // Long: adverse = mid below avg.
                        (avg - mid) / avg * Decimal::from(10_000)
                    } else {
                        // Short: adverse = mid above avg.
                        (mid - avg) / avg * Decimal::from(10_000)
                    };
                    let threshold = Decimal::from(self.config.adverse_stop_drift_bps);
                    if drift_bps >= threshold {
                        let close_side = if pos_size > Decimal::ZERO {
                            Side::Ask
                        } else {
                            Side::Bid
                        };
                        let touch = match close_side {
                            Side::Bid => top.ask,
                            Side::Ask => top.bid,
                        };
                        // Lot-step-round the position size directly. DO NOT
                        // call quote_size here — that helper divides by
                        // price to convert NOTIONAL→qty; we already have
                        // the qty (contracts) and just need it on the lot
                        // grid for the venue to accept the IOC.
                        let qty = pos_size.abs();
                        let step = self.config.step_size;
                        let stop_qty = if step > Decimal::ZERO {
                            (qty / step).floor() * step
                        } else {
                            qty
                        };
                        if stop_qty > Decimal::ZERO {
                            self.cycle_start_ts = None;
                            let mut actions: Vec<Action> = Vec::with_capacity(3);
                            actions.extend(self.drop_tracked_side(Side::Bid));
                            actions.extend(self.drop_tracked_side(Side::Ask));
                            actions.push(Action::Quote(tikr_venue::QuoteIntent {
                                symbol: ctx.symbol.clone(),
                                side: close_side,
                                price: touch,
                                size: Size(stop_qty),
                                tif: TimeInForce::IOC,
                                kind: QuoteKind::Point,
                            }));
                            return actions;
                        }
                    }
                }
            }
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
        reason: &str,
    ) -> Vec<Action> {
        // Post-only races on offset=0 (-5022 "would not be maker") are
        // not real errors — the book just moved a tick while our place
        // was in flight. Re-emit IMMEDIATELY at the fresh touch without
        // stamping side cooldown. Other rejection classes (insufficient
        // margin, lot/tick filter) get the cooldown so we don't hammer
        // a known-bad config.
        let ts = ctx.now;
        let is_post_only_race = reason.contains("-5022") || reason.contains("Post Only");
        if !is_post_only_race {
            self.mark_reject(intent.side, ts);
        }
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
            close_decay_after_secs_1: 0,
            close_decay_factor_1: Decimal::ONE,
            close_decay_after_secs_2: 0,
            close_decay_factor_2: Decimal::ONE,
            adverse_stop_after_secs: 0,
            adverse_stop_drift_bps: 0,
            quote_offset_ticks: -1,
            close_target_ticks: 0,
            strict_touch_quotes: false,
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
    fn long_inventory_pins_ask_to_position_size() {
        // Close-side (Ask for long) is now pinned to abs(position.size)
        // rather than notional/price × multiplier. This prevents
        // lot-step jitter from triggering Replace when price moves
        // within `price_tolerance_ticks`.
        let symbol = sym();
        let snapshot = book(&symbol, 50, 60, 1);
        let pos_size = Decimal::new(5, 1);
        let position = pos_with_size(&symbol, pos_size);
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
                assert_eq!(ask.size.0, pos_size.abs(), "ask pinned to position size");
                // Bid (entry side) still notional-derived. Default
                // strategy() has quote_offset_ticks=-1 → bid price =
                // best_bid + 1 tick = 51, size = floor(100/51) = 1.
                assert_eq!(bid.size.0, Decimal::ONE);
            }
            _ => panic!("expected quotes"),
        }
    }

    #[test]
    fn short_inventory_pins_bid_to_position_size() {
        let symbol = sym();
        let snapshot = book(&symbol, 100, 110, 1);
        let pos_size = Decimal::new(-5, 1);
        let position = pos_with_size(&symbol, pos_size);
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
                assert_eq!(bid.size.0, pos_size.abs(), "bid pinned to position size");
                // Ask (entry side) still notional-derived: 100/110 floor
                // to step=1 = 0. min_notional is ZERO in tests so the
                // zero-size emit is allowed.
                assert_eq!(ask.size.0, Decimal::ZERO);
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
