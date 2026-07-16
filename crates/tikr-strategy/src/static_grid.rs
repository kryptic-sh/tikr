//! Static grid — place once, then rebuild the full grid when one side is consumed.
//!
//! Differs from [`LayeredGrid`] in that there is NO rolling, NO TP, NO mid-tracking
//! after cold start. Each order is a passive limit that sits until filled or
//! the batch is consumed enough to warrant a fresh placement around the new mid.
//!
//! # Layout
//!
//! For `levels_per_side = N`, `inner_bps`, `step_bps`, cold start places `2N`
//! orders symmetrically around the current mid:
//!
//! ```text
//! sell @ mid + (inner + 2·step) bps   (outer)
//! sell @ mid + (inner + 1·step) bps
//! sell @ mid +  inner bps             (inner)
//!                MID
//! buy  @ mid −  inner bps             (inner)
//! buy  @ mid − (inner + 1·step) bps
//! buy  @ mid − (inner + 2·step) bps   (outer)
//! ```
//!
//! Default `inner_bps = 3, step_bps = 3, N = 3` gives the user's example
//! layout: orders at ±3, ±6, ±9 bps from mid (gap-between-inner-pair = 6bps,
//! adjacent levels = 3bps apart).
//!
//! # Rebuild rule
//!
//! After cold start, on each fully-filled fill the strategy counts the
//! remaining open quotes. Rebuilds when one side is empty (i.e. all
//! remaining orders are on the same side, or both sides were externally wiped).
//!
//! Rebuild = `CancelAll` + fresh `2N` orders around the latest book mid.
//! Anchored on the current book mid each time — the grid "follows" big
//! moves in a coarse step-wise way, not smoothly like [`LayeredGrid`].

use std::collections::VecDeque;

use tikr_core::{Decimal, MarketEvent, Price, QuoteKind, Side, Size, Symbol, TimeInForce};
use tikr_venue::{QuoteId, QuoteIntent};

use crate::risk::{self, RiskConfig, RiskDecision};
use crate::{Action, Strategy, StrategyContext};

/// True iff posting on `side` would deepen inventory past `cap`.
/// `cap == 0` disables the gate (always returns false). Shared
/// helper used by SG/LG build_batch loops.
#[inline]
fn add_side_capped(pos_usdt: Decimal, cap: Decimal, side: Side) -> bool {
    if cap <= Decimal::ZERO {
        return false;
    }
    match side {
        Side::Bid => pos_usdt >= cap,
        Side::Ask => pos_usdt <= -cap,
    }
}

/// Rolling-window directional-drift tracker for regime classification.
///
/// Keeps `(ts_ns, mid)` samples within the configured window. On
/// query, returns the signed bps drift from the *oldest* sample still
/// in-window to the *newest*. Used by `StaticGrid` to gate inventory
/// skew: skew engages only when |drift_bps| > `trend_threshold_bps`
/// (trending regime). Below threshold the strategy quotes
/// symmetrically, which empirically captures 30-50% more spread on
/// reverting/chop days (see 24h backtest A/B in commit history).
#[derive(Debug, Clone, Default)]
struct RegimeTracker {
    window_ns: u64,
    /// `(ts_ns, mid_price)` samples, oldest first. Trimmed on every
    /// `observe` call.
    samples: VecDeque<(u64, Decimal)>,
}

impl RegimeTracker {
    fn new(window_secs: u64) -> Self {
        Self {
            window_ns: window_secs.saturating_mul(1_000_000_000),
            samples: VecDeque::new(),
        }
    }

    /// Push a mid observation. No-op when the tracker is disabled
    /// (`window_ns == 0`).
    fn observe(&mut self, ts_ns: u64, mid: Price) {
        if self.window_ns == 0 {
            return;
        }
        // Drop samples older than the rolling window.
        let cutoff = ts_ns.saturating_sub(self.window_ns);
        while let Some(&(ts, _)) = self.samples.front() {
            if ts < cutoff {
                self.samples.pop_front();
            } else {
                break;
            }
        }
        self.samples.push_back((ts_ns, mid.0));
    }

    /// Signed drift from oldest in-window sample to newest, in bps.
    /// `None` when fewer than 2 samples in window (not enough to
    /// classify regime yet).
    fn drift_bps(&self) -> Option<Decimal> {
        if self.samples.len() < 2 {
            return None;
        }
        let oldest = self.samples.front()?.1;
        let newest = self.samples.back()?.1;
        if oldest <= Decimal::ZERO {
            return None;
        }
        Some((newest - oldest) / oldest * Decimal::from(10_000))
    }

    /// Whether the current regime is "trending" (|drift| > threshold).
    /// When tracker is disabled or has insufficient data, returns the
    /// `legacy_fallback` value so callers preserve their pre-regime
    /// behaviour (typically: assume trending, leave skew engaged).
    fn is_trending(&self, threshold_bps: u32, legacy_fallback: bool) -> bool {
        if self.window_ns == 0 {
            return legacy_fallback;
        }
        match self.drift_bps() {
            Some(d) => d.abs() >= Decimal::from(threshold_bps),
            None => legacy_fallback,
        }
    }

    /// **Directional efficiency** (a.k.a. Kaufman's efficiency ratio):
    /// `signed_drift / sum(|tick_returns|)`. Range `[-1, +1]`.
    ///
    /// `+1` = perfectly monotonic up move (no retracements).
    /// `-1` = perfectly monotonic down move.
    /// `0`  = pure chop (every move cancelled by an opposite move).
    ///
    /// Self-scaling per symbol — DOGE's 10-bps spread-flicker and
    /// BTC's 1-bps tick-noise both produce low efficiency unless the
    /// drift is genuinely directional. Replaces the brittle
    /// `|drift| > N bps` heuristic. Returns `None` when fewer than 2
    /// samples or total movement is zero.
    fn efficiency(&self) -> Option<Decimal> {
        if self.samples.len() < 2 {
            return None;
        }
        let oldest = self.samples.front()?.1;
        let newest = self.samples.back()?.1;
        let signed = newest - oldest;
        // Sum absolute consecutive deltas — proxy for total path length.
        let mut total_abs = Decimal::ZERO;
        let mut prev: Option<Decimal> = None;
        for &(_, mid) in self.samples.iter() {
            if let Some(p) = prev {
                let d = mid - p;
                total_abs += if d < Decimal::ZERO { -d } else { d };
            }
            prev = Some(mid);
        }
        if total_abs <= Decimal::ZERO {
            return None;
        }
        Some(signed / total_abs)
    }

    /// Vol-normalised "is trending" using directional efficiency.
    /// `efficiency_threshold` is a Decimal in `[0, 1]` — e.g. `0.3`
    /// means "30% of the price path moved in one direction" → trending.
    /// Above the threshold the regime is flagged trending; below is
    /// chop. Falls back to `legacy_fallback` when the tracker has
    /// insufficient data.
    fn is_trending_efficiency(&self, efficiency_threshold: Decimal, legacy_fallback: bool) -> bool {
        if self.window_ns == 0 {
            return legacy_fallback;
        }
        match self.efficiency() {
            Some(e) => e.abs() >= efficiency_threshold,
            None => legacy_fallback,
        }
    }
}

/// Configuration for [`StaticGrid`].
#[derive(Debug, Clone)]
pub struct StaticGridConfig {
    /// Fixed fiat notional per order.
    pub notional_per_order: Decimal,
    /// Number of orders per side (total open at start = `2 × levels_per_side`).
    pub levels_per_side: u32,
    /// Inner spread from mid in bps (closest level on each side).
    pub inner_bps: u32,
    /// Step between consecutive levels on the same side in bps.
    pub step_bps: u32,
    /// Venue lot step size for quantity rounding.
    pub step_size: Decimal,
    /// Minimum order notional required by the venue.
    pub min_notional: Decimal,
    /// Adaptive fill-rate scaler. Widens `inner_bps`/`step_bps` when
    /// realised fills/min exceed `target_fills_per_min`. `0` disables
    /// (no scaling applied). Default `5.0` fills/min target — tune to
    /// match the symbol's natural taker-flow rate.
    pub target_fills_per_min: Decimal,
    /// Rolling window (seconds) over which fills/min is measured.
    /// Default `60` — one minute of memory.
    pub fillrate_window_secs: u32,
    /// Lower bound on the adaptive scale multiplier. `1.0` = never
    /// tighten below configured bps (safer); `<1.0` allows tightening
    /// when flow is slow. Default `1.0`.
    pub scale_min: Decimal,
    /// Upper bound on the adaptive scale multiplier. Default `4.0` =
    /// up to 4× widening under heavy fill pressure.
    pub scale_max: Decimal,
    /// Enable inventory-driven asymmetric skew. When `true`:
    /// the side accumulating inventory (weak side) joins the book at
    /// best bid/ask, while the opposite side widens by `(1 + |ratio|)`.
    /// When `false`: symmetric ladder at `inner_bps + k·step_bps` from
    /// mid regardless of position — useful for testing baseline grid
    /// behaviour or when an external risk layer manages inventory.
    ///
    /// When `regime_window_secs > 0`, this knob becomes the *upper
    /// limit*: skew is engaged ONLY when the regime tracker flags a
    /// trending market AND `auto_skew` is true. On reverting/chop
    /// regimes the strategy quotes symmetrically even when `auto_skew
    /// = true`. Empirically the 24h sweep showed skew costs 30-50%
    /// of spread capture on reverting days and saves the same on
    /// trending ones — regime-gating gets you both behaviours from
    /// one config.
    pub auto_skew: bool,
    /// Rolling-window length (seconds) for the regime tracker. `0`
    /// disables regime gating — `auto_skew` then takes effect
    /// unconditionally (legacy behaviour). Sensible default: `300`
    /// (5 minutes) — long enough to smooth tick-by-tick noise, short
    /// enough to react to a regime change inside one trading session.
    pub regime_window_secs: u64,
    /// **Legacy** absolute-bps trend threshold. Used only when
    /// `regime_efficiency_threshold == 0`. Brittle on illiquid pairs
    /// (DOGE 10 bps = bid/ask flicker, not a trend). Prefer
    /// `regime_efficiency_threshold` for new configs.
    ///
    /// Threshold (bps) on the signed mid-drift over `regime_window_secs`
    /// at which the regime flips from "chop" to "trending". Below
    /// this (`|drift| <= threshold`) the strategy is in chop mode →
    /// skew is suppressed even when `auto_skew = true`. Sensible
    /// default: `10` (a tenth of a percent over 5 minutes).
    pub regime_trend_threshold_bps: u32,
    /// **Vol-normalised** trend threshold via Kaufman's directional
    /// efficiency: `signed_drift / sum(|tick_returns|)` ∈ `[-1, +1]`.
    /// Above this (`|efficiency| >= threshold`) the regime is flagged
    /// trending; below is chop. Self-scales per symbol — DOGE's
    /// 10-bps spread-flicker and BTC's 1-bps tick-noise both register
    /// low efficiency unless drift is genuinely directional.
    /// `0` (default) disables the efficiency path → falls back to the
    /// legacy `regime_trend_threshold_bps`. Sensible value: `"0.3"`
    /// (30% of total path moved in one direction).
    pub regime_efficiency_threshold: Decimal,
    /// Hard inventory cap in USDT notional. When `|position × mid| ≥
    /// cap`, the add-side quote (Bid for longs, Ask for shorts) is
    /// suppressed in `build_batch` so existing rest-orders can drain
    /// the position without piling on more inventory. `0` (default)
    /// disables the cap. Sensible match for the per-bot config:
    /// `max_position_usdt = notional × levels_per_side` so the bot
    /// can fill its full ladder but never deepen further.
    pub max_position_usdt: Decimal,
    /// Take-profit threshold in bps of position notional (entry × |qty|).
    /// On breach the bot fires an IOC at the opposing touch to flatten
    /// the position. `0` (default) disables. Self-scaling — same number
    /// works whether the position is $10 or $10k notional.
    pub take_profit_bps: u32,
    /// Stop-loss threshold in bps of position notional. On adverse
    /// drift past `-sl_bps` the bot IOC-flattens. `0` (default)
    /// disables. Pair with `take_profit_bps` to bound both wings.
    pub stop_loss_bps: u32,
}

/// Static grid state.
pub struct StaticGrid {
    config: StaticGridConfig,
    /// `true` once the initial batch has been placed.
    placed: bool,
    /// Last seen book mid — used to rebuild around the current mid when the
    /// rebuild trigger fires (since rebuild fires inside `on_event(Fill)` which
    /// doesn't carry a fresh book snapshot of its own).
    last_mid: Option<Price>,
    /// Best bid + ask from the most recent book snapshot. Used by the
    /// weak side of an asymmetric skew to JOIN the book at the best
    /// price (most aggressive maker placement) instead of pricing
    /// off the configured `inner_bps` distance from mid.
    last_bid: Option<Price>,
    last_ask: Option<Price>,
    /// Rolling window of fill timestamps (ns since epoch). Pruned to
    /// `fillrate_window_secs` on every fill; used to compute the
    /// adaptive bps scale.
    fill_ts: VecDeque<u64>,
    /// Timestamp of the first event seen (ns). Used for the adaptive
    /// scale ramp-up: during the first `fillrate_window_secs` the
    /// effective window is `now - session_start`, not the full window
    /// — so a fast-fill open isn't masked by dividing by a 60s window
    /// when only 5s have elapsed.
    session_start_ts: Option<u64>,
    /// Latest event timestamp seen. Used as "now" by `adaptive_scale`
    /// when computing effective window length (since the scaler is
    /// called from `build_batch` which doesn't take ts directly).
    last_event_ts: Option<u64>,
    /// Per-side timestamp (ns) of the last refill we EMITTED. Used to
    /// rate-limit BookUpdate-driven self-heal so a venue rejection
    /// (insufficient margin, lot-size violation, etc.) doesn't kick
    /// off a hot loop of place→reject→next-tick→place→reject at the
    /// BookUpdate cadence (~10/sec).
    ///
    /// `[Bid, Ask]` indexed by `side as usize` doesn't work cleanly
    /// since `Side` isn't a known repr — store as two Options.
    last_refill_bid_ts: Option<u64>,
    last_refill_ask_ts: Option<u64>,
    /// Rolling-window directional drift tracker for regime gating.
    /// `RegimeTracker::observe(ts, mid)` runs on every BookUpdate;
    /// `make_level` consults `is_trending()` to decide whether to
    /// engage `auto_skew`.
    regime: RegimeTracker,
    /// Timestamp of the last TP/SL flatten emission. While the IOC is
    /// in flight the position report still shows the old size, so
    /// without this latch the risk gate would re-fire a full-size
    /// close on every event until the fill lands — two fills flip the
    /// position. Doubles as a retry timer: if the IOC missed, the
    /// close re-fires after the window instead of never.
    last_risk_close_ts_ns: u64,
    /// Whether the most recent `build_batch` call intentionally left
    /// the Bid / Ask side empty because `max_position_usdt` was
    /// breached. Consulted by `rebuild_decision` so a cap-suppressed
    /// empty side isn't mistaken for a broken one — without this, an
    /// inventory-capped grid CancelAll + re-places both sides every
    /// ~2s forever, losing queue priority on the still-healthy closing
    /// side.
    last_bid_suppressed: bool,
    last_ask_suppressed: bool,
}

/// How long to wait before re-attempting a side refill after the
/// strategy already emitted one. Resets implicitly when a real fill
/// arrives (the position changed → rebuild_decision now computes
/// against a different state, so the cooldown becomes irrelevant).
const REFILL_COOLDOWN_NS: u64 = 2_000_000_000; // 2 seconds

/// Min elapsed time between TP/SL flatten emissions (see
/// `last_risk_close_ts_ns`).
const RISK_CLOSE_RETRY_NS: u64 = 5_000_000_000;

/// Sort an action batch by `|price - mid|` ascending so the venue sees
/// inner orders first. Non-quote actions (CancelAll, Cancel(id)) keep
/// distance `0` and stay at the front. Stable so same-distance entries
/// preserve insertion order — important when a bid and ask are
/// equidistant under symmetric skew (both fire same tick).
fn sort_inside_out(actions: &mut [Action], mid: Price) {
    actions.sort_by_key(|a| match a {
        Action::Quote(q) => {
            let d = q.price.0 - mid.0;
            if d < Decimal::ZERO { -d } else { d }
        }
        _ => Decimal::ZERO,
    });
}

impl StaticGrid {
    /// Snapshot the risk policy from the current config. Stateless —
    /// rebuilt each tick so a hot-reloaded config picks up immediately.
    fn risk_cfg(&self) -> RiskConfig {
        RiskConfig {
            take_profit_bps: self.config.take_profit_bps,
            stop_loss_bps: self.config.stop_loss_bps,
            take_profit_usdt_legacy: Decimal::ZERO,
        }
    }

    /// Evaluate the risk gate; on breach, return CancelAll + IOC close
    /// at the opposing touch. None when both thresholds are disabled
    /// or position is flat. Caller short-circuits the normal quote loop
    /// when Some is returned — including `Some(vec![])` while a
    /// previously-emitted close IOC is still in flight (the position
    /// report lags it, so re-evaluating would re-fire a full-size
    /// close and flip the position).
    fn try_risk_close(
        &mut self,
        ctx: &StrategyContext<'_>,
        mid: Price,
        best_bid: Price,
        best_ask: Price,
    ) -> Option<Vec<Action>> {
        if self.config.take_profit_bps == 0 && self.config.stop_loss_bps == 0 {
            return None;
        }
        if ctx.position.size.0 == Decimal::ZERO {
            // Flat — any in-flight flatten landed; re-arm the latch.
            self.last_risk_close_ts_ns = 0;
            return None;
        }
        match risk::evaluate(ctx.position, mid, self.risk_cfg()) {
            RiskDecision::Hold => None,
            RiskDecision::Close { side, qty, .. } => {
                if self.last_risk_close_ts_ns > 0
                    && ctx.now.0.saturating_sub(self.last_risk_close_ts_ns) < RISK_CLOSE_RETRY_NS
                {
                    return Some(Vec::new());
                }
                self.last_risk_close_ts_ns = ctx.now.0;
                Some(vec![
                    Action::CancelAll,
                    risk::build_close(ctx.symbol, side, qty, best_bid, best_ask),
                ])
            }
        }
    }

    fn make_quote(&self, symbol: &Symbol, side: Side, price: Price) -> Action {
        let step = self.config.step_size;
        let raw_size = self.config.notional_per_order / price.0;
        let mut qty = if step > Decimal::ZERO {
            (raw_size / step).floor() * step
        } else {
            raw_size
        };
        if self.config.min_notional > Decimal::ZERO {
            if step > Decimal::ZERO {
                // Ceil to the step: the smallest on-lot qty at or above
                // the min-notional floor. No extra step on top — ceil
                // already lands at-or-above, so adding one more lot
                // was inflating every constrained order by a full step.
                let min_qty = (self.config.min_notional / price.0 / step).ceil() * step;
                if qty < min_qty {
                    qty = min_qty;
                }
            } else {
                // No lot step to round to (fractional-qty venue) —
                // enforce min_notional via direct division so a
                // zero-step config can't quote below the venue floor.
                let min_qty = self.config.min_notional / price.0;
                if qty < min_qty {
                    qty = min_qty;
                }
            }
        }
        Action::Quote(QuoteIntent {
            symbol: symbol.clone(),
            side,
            price,
            size: Size(qty),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        })
    }

    /// Prune fill_ts to entries within the rolling window of `now_ns`.
    fn prune_fills(&mut self, now_ns: u64) {
        let window_ns = (self.config.fillrate_window_secs as u64).saturating_mul(1_000_000_000);
        let cutoff = now_ns.saturating_sub(window_ns);
        while let Some(&front) = self.fill_ts.front() {
            if front < cutoff {
                self.fill_ts.pop_front();
            } else {
                break;
            }
        }
    }

    /// Adaptive scale multiplier from rolling fill rate vs target.
    ///
    /// `scale = (actual_fpm / target_fpm).clamp(scale_min, scale_max)`.
    /// Returns `1.0` when adaptive is disabled (target = 0), when the
    /// window is empty, or when no events have been seen yet (neutral
    /// boot — let the strategy start at configured bps).
    ///
    /// During the first `fillrate_window_secs` of a session the
    /// effective denominator is `now - session_start` (capped at the
    /// window) — so a hot open with 5 fills in 3 seconds reports
    /// `5 / (3/60) = 100 fpm`, not `5 / 1.0 = 5 fpm`.
    fn adaptive_scale(&self) -> Decimal {
        if self.config.target_fills_per_min <= Decimal::ZERO {
            return Decimal::ONE;
        }
        let count = self.fill_ts.len();
        if count == 0 {
            return Decimal::ONE;
        }
        let (Some(start), Some(now)) = (self.session_start_ts, self.last_event_ts) else {
            return Decimal::ONE;
        };
        let window_ns = (self.config.fillrate_window_secs as u64).saturating_mul(1_000_000_000);
        if window_ns == 0 {
            return Decimal::ONE;
        }
        let elapsed_ns = now.saturating_sub(start);
        let effective_ns = elapsed_ns.min(window_ns).max(1);
        let effective_min = Decimal::from(effective_ns) / Decimal::from(60u64 * 1_000_000_000u64);
        let actual_fpm = Decimal::from(count as u64) / effective_min;
        let raw = actual_fpm / self.config.target_fills_per_min;
        raw.clamp(self.config.scale_min, self.config.scale_max)
    }

    /// True when the BookUpdate-driven self-heal recently emitted a
    /// refill for `side` and the cooldown hasn't expired yet. Prevents
    /// the place→reject→retry hot loop on terminal venue rejections
    /// (margin insufficient, etc.) that the strategy can't recover
    /// from until inventory shifts.
    fn side_in_cooldown(&self, side: Side, now_ns: u64) -> bool {
        let last = match side {
            Side::Bid => self.last_refill_bid_ts,
            Side::Ask => self.last_refill_ask_ts,
        };
        let Some(last) = last else { return false };
        now_ns.saturating_sub(last) < REFILL_COOLDOWN_NS
    }

    /// Record a refill emission timestamp for the given side.
    fn mark_refill(&mut self, side: Side, now_ns: u64) {
        match side {
            Side::Bid => self.last_refill_bid_ts = Some(now_ns),
            Side::Ask => self.last_refill_ask_ts = Some(now_ns),
        }
    }

    /// Clear both side cooldowns. Called after an actual Fill so that
    /// any subsequent refill decision starts fresh — a fill changed
    /// the position and the prior cooldown's reasoning is stale.
    fn clear_refill_cooldowns(&mut self) {
        self.last_refill_bid_ts = None;
        self.last_refill_ask_ts = None;
    }

    /// Compute position ratio in `[-1, 1]`, **balance-agnostic**: measures
    /// imbalance as "filled orders relative to the grid's own size", not
    /// against an external dollar target.
    ///
    /// Net fills in this side = `position_usdt / notional_per_order`.
    /// Ratio = `net_fills / levels_per_side`. Saturates at ±1 when the
    /// bot has filled every level on one side (max grid imbalance).
    ///
    /// Positive = long, negative = short. Zero notional or zero
    /// levels short-circuits to flat (defensive guard).
    fn pos_ratio(&self, pos_usdt: Decimal) -> Decimal {
        if self.config.notional_per_order <= Decimal::ZERO || self.config.levels_per_side == 0 {
            return Decimal::ZERO;
        }
        let net_fills = pos_usdt / self.config.notional_per_order;
        let raw = net_fills / Decimal::from(self.config.levels_per_side);
        raw.clamp(-Decimal::ONE, Decimal::ONE)
    }

    /// Emit `2N` orders skewed by current inventory.
    ///
    /// **Asymmetric, with a weak-side "join the book" mode.**
    ///
    /// `pos_ratio = 0` (flat): symmetric ladder, both sides at
    /// `inner_bps + k·step_bps` from mid (the configured normal).
    ///
    /// `pos_ratio > 0` (long, buys filling faster):
    /// - **Strong side (Bid)**: widened to `(inner + k·step) × (1 + |ratio|)` bps
    ///   from mid → first buy is up to 2× normal distance below mid at full
    ///   saturation. Slows further accumulation.
    /// - **Weak side (Ask)**: *joins the book* at `best_ask` for k=0, then
    ///   stacks back by `k·step_bps`. Most aggressive maker placement
    ///   possible — first sell sits at the same price as the best ask,
    ///   maximising odds of a buy aggressor filling it.
    ///
    /// `pos_ratio < 0` (short): mirrored — Ask widens from mid, Bid joins
    /// the book at `best_bid`.
    ///
    /// Returns `(actions, bid_suppressed, ask_suppressed)` — the two
    /// bools flag whether the cap left that side intentionally empty
    /// this build. Callers persist them into `last_bid_suppressed` /
    /// `last_ask_suppressed` for `rebuild_decision` to consult.
    fn build_batch(
        &self,
        symbol: &Symbol,
        mid: Price,
        best_bid: Price,
        best_ask: Price,
        pos_ratio: Decimal,
        pos_usdt: Decimal,
    ) -> (Vec<Action>, bool, bool) {
        let mut actions = Vec::with_capacity(self.config.levels_per_side as usize * 2);
        let cap = self.config.max_position_usdt;
        let bid_capped = add_side_capped(pos_usdt, cap, Side::Bid);
        let ask_capped = add_side_capped(pos_usdt, cap, Side::Ask);
        for k in 0..self.config.levels_per_side {
            if !bid_capped {
                actions.push(self.make_level(
                    symbol,
                    mid,
                    best_bid,
                    best_ask,
                    pos_ratio,
                    Side::Bid,
                    k,
                ));
            }
            if !ask_capped {
                actions.push(self.make_level(
                    symbol,
                    mid,
                    best_bid,
                    best_ask,
                    pos_ratio,
                    Side::Ask,
                    k,
                ));
            }
        }
        // Submit innermost-first regardless of side. The naive
        // `for k { bid, ask }` loop emits k=1 bid BEFORE k=0 ask when
        // auto-skew puts the ask weak-side at the touch and pushes bids
        // out by `(1 + |ratio|)`. On a one-sided market move that
        // means the bot fires a stack of distant bids before its
        // closest ask is even on the book — exactly the asymmetry we
        // want to avoid. Sorting by |price - mid| ascending sends the
        // nearest-to-mid quote first whatever its side.
        sort_inside_out(&mut actions, mid);
        (actions, bid_capped, ask_capped)
    }

    #[allow(clippy::too_many_arguments)]
    fn make_level(
        &self,
        symbol: &Symbol,
        mid: Price,
        best_bid: Price,
        best_ask: Price,
        pos_ratio: Decimal,
        side: Side,
        k: u32,
    ) -> Action {
        let bp_unit = Decimal::from(10_000);
        let adaptive = self.adaptive_scale();
        let base_bps = Decimal::from(self.config.inner_bps + self.config.step_bps * k) * adaptive;

        // Identify weak side from inventory direction. When auto_skew
        // is off both legs fall through to the symmetric branches —
        // `is_weak = false` and `abs_ratio = 0` collapse the math back
        // to plain `inner_bps + k·step_bps` from mid.
        //
        // Regime gate: skew engages only when the rolling-window
        // drift tracker flags a trending market. On chop/reverting
        // regimes (drift within ±`regime_trend_threshold_bps` over
        // `regime_window_secs`) skew is suppressed even when
        // `auto_skew = true`, because symmetric quoting captures
        // 30-50% more round-trips on those days (24h backtest A/B).
        // Tracker disabled (`regime_window_secs == 0`) falls back to
        // the legacy "always skew" behaviour.
        // Prefer the vol-normalised efficiency metric when configured.
        // Falls back to the legacy `|drift| > N bps` heuristic when
        // `regime_efficiency_threshold == 0`. Both default to
        // `legacy_fallback = true` so configs without regime gating
        // (window == 0) skew unconditionally — matches pre-regime
        // behaviour.
        let regime_ok = if self.config.regime_efficiency_threshold > Decimal::ZERO {
            self.regime.is_trending_efficiency(
                self.config.regime_efficiency_threshold,
                /* legacy_fallback = */ true,
            )
        } else {
            self.regime.is_trending(
                self.config.regime_trend_threshold_bps,
                /* legacy_fallback = */ true,
            )
        };
        let (is_weak, abs_ratio) = if self.config.auto_skew && regime_ok {
            let weak_side = if pos_ratio > Decimal::ZERO {
                Some(Side::Ask) // long → closing side is Ask
            } else if pos_ratio < Decimal::ZERO {
                Some(Side::Bid) // short → closing side is Bid
            } else {
                None // flat → no asymmetry
            };
            let abs = if pos_ratio < Decimal::ZERO {
                -pos_ratio
            } else {
                pos_ratio
            };
            (weak_side == Some(side), abs)
        } else {
            (false, Decimal::ZERO)
        };

        let price = match side {
            Side::Bid if is_weak => {
                // Join at best_bid for k=0; stack back by k·step_bps below.
                let offset_bps = Decimal::from(self.config.step_bps * k) * adaptive;
                Price(best_bid.0 * (Decimal::ONE - offset_bps / bp_unit))
            }
            Side::Ask if is_weak => {
                // Join at best_ask for k=0; stack back by k·step_bps above.
                let offset_bps = Decimal::from(self.config.step_bps * k) * adaptive;
                Price(best_ask.0 * (Decimal::ONE + offset_bps / bp_unit))
            }
            Side::Bid => {
                // Strong (or neutral) Bid — widen from mid by (1 + |ratio|).
                let bps = (base_bps * (Decimal::ONE + abs_ratio)).max(Decimal::ONE);
                let mid_price = mid.0 * (Decimal::ONE - bps / bp_unit);
                // If market spread is wider than our grid spread (2×inner_bps),
                // our k=0 order would land inside the market spread.
                // Place 1bp below best_bid instead.
                let grid_spread_bps = Decimal::from(self.config.inner_bps * 2) * adaptive;
                let market_spread_bps = (best_ask.0 - best_bid.0) / mid.0 * bp_unit;
                if k == 0 && market_spread_bps > grid_spread_bps {
                    Price(best_bid.0 * (Decimal::ONE - Decimal::ONE / bp_unit))
                } else {
                    Price(mid_price)
                }
            }
            Side::Ask => {
                // Strong (or neutral) Ask — widen from mid by (1 + |ratio|).
                let bps = (base_bps * (Decimal::ONE + abs_ratio)).max(Decimal::ONE);
                let mid_price = mid.0 * (Decimal::ONE + bps / bp_unit);
                // Same logic as Bid side.
                let grid_spread_bps = Decimal::from(self.config.inner_bps * 2) * adaptive;
                let market_spread_bps = (best_ask.0 - best_bid.0) / mid.0 * bp_unit;
                if k == 0 && market_spread_bps > grid_spread_bps {
                    Price(best_ask.0 * (Decimal::ONE + Decimal::ONE / bp_unit))
                } else {
                    Price(mid_price)
                }
            }
        };
        self.make_quote(symbol, side, price)
    }

    /// What the bot should do on a full fill.
    ///
    /// `bid_suppressed` / `ask_suppressed` reflect whether the LAST
    /// `build_batch` call intentionally left that side empty because
    /// the inventory cap bound. An empty side only counts as "broken"
    /// (worth a rebuild) when it was NOT intentionally suppressed —
    /// otherwise a capped grid would CancelAll + re-place both sides
    /// every cycle, losing queue priority on the still-healthy closing
    /// side for as long as the position sits at the cap.
    fn rebuild_decision(
        &self,
        open_quotes: &[(QuoteId, QuoteIntent)],
        cur_pos_ratio: Decimal,
        bid_suppressed: bool,
        ask_suppressed: bool,
    ) -> RebuildDecision {
        let buys = open_quotes
            .iter()
            .filter(|(_, q)| q.side == Side::Bid)
            .count();
        let sells = open_quotes.len() - buys;

        // One side empty: rebuild the whole grid so the surviving side
        // follows the current price instead of lagging in fast trends.
        // Skip when the empty side is the cap-suppressed one — that
        // side is SUPPOSED to be empty right now.
        let bid_broken = buys == 0 && !bid_suppressed;
        let ask_broken = sells == 0 && !ask_suppressed;
        if bid_broken || ask_broken {
            return RebuildDecision::FullRebuild;
        }

        // Both sides healthy. Nothing to do — the next fill may empty
        // a side and route through FullRebuild, which re-prices both
        // sides with the current inventory skew.
        let _ = cur_pos_ratio;
        RebuildDecision::None
    }

    /// Check if the grid is down to its last order on each side.
    /// Called only from the Fill handler — NOT from BookUpdate self-heal
    /// to avoid premature rebuilds during normal trading.
    fn is_last_per_side(&self, open_quotes: &[(QuoteId, QuoteIntent)]) -> bool {
        let buys = open_quotes
            .iter()
            .filter(|(_, q)| q.side == Side::Bid)
            .count();
        let sells = open_quotes.len() - buys;
        buys == 1 && sells == 1
    }
}

/// Outcome of `rebuild_decision`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RebuildDecision {
    None,
    /// Wipe and re-place both sides around current mid.
    FullRebuild,
}

impl Strategy for StaticGrid {
    type Config = StaticGridConfig;

    fn new(config: Self::Config) -> Self {
        assert!(
            config.scale_min <= config.scale_max,
            "StaticGridConfig: scale_min ({}) must be <= scale_max ({})",
            config.scale_min,
            config.scale_max
        );
        let regime = RegimeTracker::new(config.regime_window_secs);
        Self {
            config,
            placed: false,
            last_mid: None,
            last_bid: None,
            last_ask: None,
            fill_ts: VecDeque::new(),
            session_start_ts: None,
            last_event_ts: None,
            last_refill_bid_ts: None,
            last_refill_ask_ts: None,
            regime,
            last_risk_close_ts_ns: 0,
            last_bid_suppressed: false,
            last_ask_suppressed: false,
        }
    }

    fn name(&self) -> &str {
        "static-grid"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        // Session timestamps: anchor session_start on first event, refresh
        // last_event_ts on every event. Both feed adaptive_scale's
        // effective-window calc so a fast-fill open isn't masked by
        // dividing observed fills by the full configured window.
        let event_ts = match event {
            MarketEvent::BookUpdate { snapshot } => Some(snapshot.ts.0),
            MarketEvent::Trade { ts, .. } => Some(ts.0),
            MarketEvent::Fill(f) => Some(f.ts.0),
            MarketEvent::Heartbeat { ts } => Some(ts.0),
        };
        if let Some(ts) = event_ts {
            self.session_start_ts.get_or_insert(ts);
            self.last_event_ts = Some(ts);
        }

        match event {
            MarketEvent::BookUpdate { snapshot } => {
                let bid = snapshot.bids.first().map(|l| l.price.0);
                let ask = snapshot.asks.first().map(|l| l.price.0);
                let (Some(b), Some(a)) = (bid, ask) else {
                    return Vec::new();
                };
                let mid = Price((b + a) / Decimal::from(2));
                self.last_mid = Some(mid);
                // Feed the regime tracker. No-op when window=0.
                self.regime.observe(snapshot.ts.0, mid);
                let best_bid = Price(b);
                let best_ask = Price(a);
                self.last_bid = Some(best_bid);
                self.last_ask = Some(best_ask);
                // Risk gate runs FIRST so an adverse spike between
                // requote intervals still trips TP/SL even when the
                // rest of the handler would short-circuit to NoOp.
                if let Some(close) = self.try_risk_close(ctx, mid, best_bid, best_ask) {
                    return close;
                }
                let pos_usdt = ctx.position.size.0 * mid.0;
                if !self.placed {
                    let ratio = self.pos_ratio(pos_usdt);
                    self.placed = true;
                    let (actions, bid_suppressed, ask_suppressed) =
                        self.build_batch(ctx.symbol, mid, best_bid, best_ask, ratio, pos_usdt);
                    self.last_bid_suppressed = bid_suppressed;
                    self.last_ask_suppressed = ask_suppressed;
                    return actions;
                }
                // Self-heal on book updates: if a side has gone empty
                // since the last fill arrived (e.g. a fill event was
                // dropped, or the reconciliation tick cleaned up a
                // ghost that the venue silently cancelled / expired)
                // the strategy is otherwise blind to that state — it
                // only acts on Fill events. Re-running rebuild_decision
                // on every BookUpdate makes the bot self-correct within
                // one book tick (~100ms on a busy symbol).
                let cur_ratio = self.pos_ratio(pos_usdt);
                let now_ns = self.last_event_ts.unwrap_or(0);
                match self.rebuild_decision(
                    ctx.open_quotes,
                    cur_ratio,
                    self.last_bid_suppressed,
                    self.last_ask_suppressed,
                ) {
                    RebuildDecision::None => vec![Action::NoOp],
                    RebuildDecision::FullRebuild => {
                        // Both sides need re-placement; rate-limit on
                        // EITHER side being in cooldown to avoid the
                        // hot loop on venue errors.
                        if self.side_in_cooldown(Side::Bid, now_ns)
                            || self.side_in_cooldown(Side::Ask, now_ns)
                        {
                            return vec![Action::NoOp];
                        }
                        self.mark_refill(Side::Bid, now_ns);
                        self.mark_refill(Side::Ask, now_ns);
                        let mut actions =
                            Vec::with_capacity(1 + self.config.levels_per_side as usize * 2);
                        actions.push(Action::CancelAll);
                        let (batch, bid_suppressed, ask_suppressed) = self
                            .build_batch(ctx.symbol, mid, best_bid, best_ask, cur_ratio, pos_usdt);
                        self.last_bid_suppressed = bid_suppressed;
                        self.last_ask_suppressed = ask_suppressed;
                        actions.extend(batch);
                        actions
                    }
                }
            }
            MarketEvent::Fill(f) => {
                // Count EVERY fill (partial + full) in the fpm window —
                // partials are real toxic-flow signal too. Only full fills
                // trigger the rebuild check below.
                self.fill_ts.push_back(f.ts.0);
                self.prune_fills(f.ts.0);
                // A real fill means inventory just changed — the prior
                // refill cooldown reasoning is stale, allow fresh
                // refill decisions immediately.
                self.clear_refill_cooldowns();
                if !f.is_full {
                    return Vec::new();
                }
                let (Some(mid), Some(best_bid), Some(best_ask)) =
                    (self.last_mid, self.last_bid, self.last_ask)
                else {
                    return Vec::new();
                };
                let pos_usdt = ctx.position.size.0 * mid.0;
                // Risk gate: fills move the position into TP/SL range
                // most often, so checking right after a fill catches the
                // breach with the tightest possible latency.
                if let Some(close) = self.try_risk_close(ctx, mid, best_bid, best_ask) {
                    return close;
                }
                let cur_ratio = self.pos_ratio(pos_usdt);
                // Rebuild when a side is empty OR when down to the last
                // order on each side. The "last per side" check is done
                // here (Fill handler only) and NOT in the BookUpdate
                // self-heal to avoid premature rebuilds during normal trading.
                if self.is_last_per_side(ctx.open_quotes) {
                    let mut actions =
                        Vec::with_capacity(1 + self.config.levels_per_side as usize * 2);
                    actions.push(Action::CancelAll);
                    let (batch, bid_suppressed, ask_suppressed) =
                        self.build_batch(ctx.symbol, mid, best_bid, best_ask, cur_ratio, pos_usdt);
                    self.last_bid_suppressed = bid_suppressed;
                    self.last_ask_suppressed = ask_suppressed;
                    actions.extend(batch);
                    return actions;
                }
                match self.rebuild_decision(
                    ctx.open_quotes,
                    cur_ratio,
                    self.last_bid_suppressed,
                    self.last_ask_suppressed,
                ) {
                    RebuildDecision::None => Vec::new(),
                    RebuildDecision::FullRebuild => {
                        let mut actions =
                            Vec::with_capacity(1 + self.config.levels_per_side as usize * 2);
                        actions.push(Action::CancelAll);
                        let (batch, bid_suppressed, ask_suppressed) = self
                            .build_batch(ctx.symbol, mid, best_bid, best_ask, cur_ratio, pos_usdt);
                        self.last_bid_suppressed = bid_suppressed;
                        self.last_ask_suppressed = ask_suppressed;
                        actions.extend(batch);
                        actions
                    }
                }
            }
            _ => Vec::new(),
        }
    }

    fn on_quote_rejected(
        &mut self,
        ctx: &StrategyContext<'_>,
        intent: &QuoteIntent,
        _reason: &str,
    ) -> Vec<Action> {
        // Recovery path: a single Quote we emitted was rejected. Emit
        // ONE replacement at the innermost level (k=0) of the same
        // side, anchored on the freshest book. Do NOT rebuild here —
        // recovery rounds compound and can balloon open counts.
        //
        // The deeper levels on this side already exist from the prior
        // batch placement; this hook only patches the specific failed
        // intent.
        let bid = ctx.latest_book.bids.first().map(|l| l.price.0);
        let ask = ctx.latest_book.asks.first().map(|l| l.price.0);
        let (Some(b), Some(a)) = (bid, ask) else {
            return Vec::new();
        };
        let mid = Price((b + a) / Decimal::from(2));
        let best_bid = Price(b);
        let best_ask = Price(a);
        self.last_mid = Some(mid);
        self.last_bid = Some(best_bid);
        self.last_ask = Some(best_ask);
        let pos_usdt = ctx.position.size.0 * mid.0;
        // Respect the inventory cap on recovery too — replacing a
        // rejected add-side quote would just refill the bleed during
        // a trend.
        if add_side_capped(pos_usdt, self.config.max_position_usdt, intent.side) {
            return Vec::new();
        }
        // Rate-limit the rejected side too. Deterministic rejections
        // (insufficient margin, min-notional, etc.) would otherwise
        // hot-loop place→reject→place at the venue round-trip rate —
        // every rejection re-emits immediately, which just re-triggers
        // the same rejection. Reuses the BookUpdate self-heal's per-
        // side cooldown state so a rejection and a self-heal refill on
        // the same side share one clock.
        let now_ns = ctx.now.0;
        if self.side_in_cooldown(intent.side, now_ns) {
            return Vec::new();
        }
        let ratio = self.pos_ratio(pos_usdt);
        self.mark_refill(intent.side, now_ns);
        vec![self.make_level(ctx.symbol, mid, best_bid, best_ask, ratio, intent.side, 0)]
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
    use std::str::FromStr;

    fn cfg(target_fpm: &str, window_secs: u32, sc_min: &str, sc_max: &str) -> StaticGridConfig {
        StaticGridConfig {
            notional_per_order: Decimal::from(100),
            levels_per_side: 2,
            inner_bps: 3,
            step_bps: 2,
            step_size: Decimal::from(1),
            min_notional: Decimal::ZERO,
            target_fills_per_min: Decimal::from_str(target_fpm).unwrap(),
            fillrate_window_secs: window_secs,
            scale_min: Decimal::from_str(sc_min).unwrap(),
            scale_max: Decimal::from_str(sc_max).unwrap(),
            auto_skew: true,
            regime_window_secs: 0,
            regime_trend_threshold_bps: 10,
            regime_efficiency_threshold: Decimal::ZERO,
            max_position_usdt: Decimal::ZERO,
            take_profit_bps: 0,
            stop_loss_bps: 0,
        }
    }

    fn quote(side: Side) -> (QuoteId, QuoteIntent) {
        let symbol = Symbol {
            base: tikr_core::Asset::new("BTC"),
            quote: tikr_core::Asset::new("USDT"),
            venue: tikr_core::VenueId::new("test"),
            kind: tikr_core::MarketKind::Perp,
        };
        (
            QuoteId::new(),
            QuoteIntent {
                symbol,
                side,
                price: Price(Decimal::from(100)),
                size: Size(Decimal::ONE),
                tif: TimeInForce::PostOnly,
                kind: QuoteKind::Point,
            },
        )
    }

    #[test]
    fn side_empty_triggers_full_rebuild() {
        let g = StaticGrid::new(cfg("0", 60, "1", "4"));
        assert_eq!(
            g.rebuild_decision(
                &[quote(Side::Ask), quote(Side::Ask)],
                Decimal::ZERO,
                false,
                false
            ),
            RebuildDecision::FullRebuild
        );
        assert_eq!(
            g.rebuild_decision(
                &[quote(Side::Bid), quote(Side::Bid)],
                Decimal::ZERO,
                false,
                false
            ),
            RebuildDecision::FullRebuild
        );
    }

    #[test]
    fn suppressed_empty_side_does_not_trigger_rebuild() {
        // Cap suppression intentionally leaves a side empty — that
        // must NOT be treated as "broken" (see fix for the FullRebuild
        // hot-loop when max_position_usdt binds).
        let g = StaticGrid::new(cfg("0", 60, "1", "4"));
        assert_eq!(
            g.rebuild_decision(
                &[quote(Side::Ask), quote(Side::Ask)],
                Decimal::ZERO,
                /* bid_suppressed */ true,
                false
            ),
            RebuildDecision::None
        );
        // But an ask_suppressed flag doesn't excuse an empty BID side.
        assert_eq!(
            g.rebuild_decision(
                &[quote(Side::Ask), quote(Side::Ask)],
                Decimal::ZERO,
                false,
                /* ask_suppressed */ true
            ),
            RebuildDecision::FullRebuild
        );
    }

    #[test]
    fn single_per_side_does_not_trigger_rebuild_decision() {
        let g = StaticGrid::new(cfg("0", 60, "1", "4"));
        assert_eq!(
            g.rebuild_decision(
                &[quote(Side::Bid), quote(Side::Ask)],
                Decimal::ZERO,
                false,
                false
            ),
            RebuildDecision::None
        );
    }

    #[test]
    fn single_per_side_triggers_is_last_per_side() {
        let g = StaticGrid::new(cfg("0", 60, "1", "4"));
        assert!(g.is_last_per_side(&[quote(Side::Bid), quote(Side::Ask)]));
    }

    #[test]
    fn both_sides_healthy_does_not_rebuild() {
        let g = StaticGrid::new(cfg("0", 60, "1", "4"));
        assert_eq!(
            g.rebuild_decision(
                &[
                    quote(Side::Bid),
                    quote(Side::Bid),
                    quote(Side::Ask),
                    quote(Side::Ask),
                ],
                Decimal::ZERO,
                false,
                false
            ),
            RebuildDecision::None
        );
        assert!(!g.is_last_per_side(&[
            quote(Side::Bid),
            quote(Side::Bid),
            quote(Side::Ask),
            quote(Side::Ask),
        ]));
    }

    #[test]
    fn empty_both_sides_triggers_full_rebuild() {
        let g = StaticGrid::new(cfg("0", 60, "1", "4"));
        assert_eq!(
            g.rebuild_decision(&[], Decimal::ZERO, false, false),
            RebuildDecision::FullRebuild
        );
    }

    #[test]
    fn regime_disabled_acts_as_legacy_always_trending() {
        let r = RegimeTracker::new(0);
        // window == 0 → tracker disabled; always reports trending.
        assert!(r.is_trending(10, /* legacy */ true));
    }

    #[test]
    fn regime_below_threshold_is_chop() {
        let mut r = RegimeTracker::new(60);
        r.observe(0, Price(Decimal::from(100)));
        // Move from 100 → 100.05 over the window = +5 bps.
        r.observe(
            30 * 1_000_000_000,
            Price(Decimal::from_str("100.05").unwrap()),
        );
        assert!(!r.is_trending(10, true));
    }

    #[test]
    fn regime_above_threshold_is_trending() {
        let mut r = RegimeTracker::new(60);
        r.observe(0, Price(Decimal::from(100)));
        // 100 → 100.2 over the window = +20 bps > 10 bps threshold.
        r.observe(
            30 * 1_000_000_000,
            Price(Decimal::from_str("100.2").unwrap()),
        );
        assert!(r.is_trending(10, true));
    }

    #[test]
    fn regime_drops_old_samples() {
        let mut r = RegimeTracker::new(60);
        r.observe(0, Price(Decimal::from(100)));
        // Sample 70s later — first one drops out of window.
        r.observe(
            70 * 1_000_000_000,
            Price(Decimal::from_str("100.2").unwrap()),
        );
        // Only one sample left in window → no drift available → fallback.
        assert!(r.is_trending(10, /* legacy */ true));
    }

    #[test]
    fn efficiency_is_one_on_monotonic_walk() {
        // Strictly increasing prices → efficiency = +1.
        let mut r = RegimeTracker::new(600);
        for i in 0..10 {
            r.observe(
                i * 1_000_000_000,
                Price(Decimal::from(100) + Decimal::from(i)),
            );
        }
        let e = r.efficiency().unwrap();
        // Allow rounding slack — should be very close to 1.
        assert!(
            (e - Decimal::ONE).abs() < Decimal::from_str("0.001").unwrap(),
            "expected efficiency ≈ 1, got {e}"
        );
        assert!(r.is_trending_efficiency(Decimal::from_str("0.3").unwrap(), true));
    }

    #[test]
    fn efficiency_is_zero_on_pure_chop() {
        // Alternating up/down — net drift = 0 → efficiency = 0.
        let mut r = RegimeTracker::new(600);
        let vals = [100, 101, 100, 101, 100, 101, 100];
        for (i, v) in vals.iter().enumerate() {
            r.observe(i as u64 * 1_000_000_000, Price(Decimal::from(*v)));
        }
        let e = r.efficiency().unwrap();
        assert!(
            e.abs() < Decimal::from_str("0.001").unwrap(),
            "expected efficiency ≈ 0, got {e}"
        );
        // Below 0.3 threshold → not trending.
        assert!(!r.is_trending_efficiency(Decimal::from_str("0.3").unwrap(), false));
    }

    #[test]
    fn scaler_disabled_returns_one() {
        let g = StaticGrid::new(cfg("0", 60, "1", "4"));
        assert_eq!(g.adaptive_scale(), Decimal::ONE);
    }

    #[test]
    fn scaler_empty_window_returns_one() {
        let g = StaticGrid::new(cfg("5", 60, "1", "4"));
        assert_eq!(g.adaptive_scale(), Decimal::ONE);
    }

    #[test]
    fn scaler_no_session_returns_one() {
        // Has a fill but no session ts (shouldn't happen via on_event,
        // but the guard exists).
        let mut g = StaticGrid::new(cfg("5", 60, "1", "4"));
        g.fill_ts.push_back(1_000_000_000);
        assert_eq!(g.adaptive_scale(), Decimal::ONE);
    }

    #[test]
    fn scaler_at_target_returns_one() {
        let mut g = StaticGrid::new(cfg("5", 60, "1", "4"));
        // Session spans 60s (full window). 5 fills = 5 fpm = target.
        g.session_start_ts = Some(0);
        g.last_event_ts = Some(60_000_000_000);
        for i in 0..5 {
            g.fill_ts.push_back(i * 12_000_000_000);
        }
        assert_eq!(g.adaptive_scale(), Decimal::ONE);
    }

    #[test]
    fn scaler_above_target_widens() {
        let mut g = StaticGrid::new(cfg("5", 60, "1", "4"));
        // 20 fills in 60s = 20 fpm, target 5 → raw 4.0 → clamped at 4.0.
        g.session_start_ts = Some(0);
        g.last_event_ts = Some(60_000_000_000);
        for _ in 0..20 {
            g.fill_ts.push_back(60_000_000_000);
        }
        assert_eq!(g.adaptive_scale(), Decimal::from(4));
    }

    #[test]
    fn scaler_clamps_at_max() {
        let mut g = StaticGrid::new(cfg("5", 60, "1", "4"));
        // 200 fills → 40fpm → raw 8.0 → clamped at 4.0.
        g.session_start_ts = Some(0);
        g.last_event_ts = Some(60_000_000_000);
        for _ in 0..200 {
            g.fill_ts.push_back(60_000_000_000);
        }
        assert_eq!(g.adaptive_scale(), Decimal::from(4));
    }

    #[test]
    fn scaler_clamps_at_min_below_one() {
        let mut g = StaticGrid::new(cfg("10", 60, "0.5", "4"));
        // 1 fill in 60s = 1 fpm, target 10 → raw 0.1 → clamped at 0.5.
        g.session_start_ts = Some(0);
        g.last_event_ts = Some(60_000_000_000);
        g.fill_ts.push_back(60_000_000_000);
        assert_eq!(g.adaptive_scale(), Decimal::from_str("0.5").unwrap());
    }

    #[test]
    fn scaler_rampup_uses_elapsed_not_window() {
        let mut g = StaticGrid::new(cfg("5", 60, "1", "4"));
        // Only 6 seconds elapsed (10% of window), 5 fills.
        // Naive (count/window): 5/1.0 = 5fpm → scale 1.0
        // Correct (count/elapsed): 5/(6/60) = 50fpm → scale 4.0 (clamped)
        g.session_start_ts = Some(0);
        g.last_event_ts = Some(6_000_000_000);
        for _ in 0..5 {
            g.fill_ts.push_back(6_000_000_000);
        }
        assert_eq!(g.adaptive_scale(), Decimal::from(4));
    }

    #[test]
    #[should_panic(expected = "scale_min")]
    fn invalid_scale_bounds_panic_on_new() {
        StaticGrid::new(cfg("5", 60, "5", "2"));
    }

    #[test]
    fn build_batch_emits_inside_out() {
        let mut c = cfg("0", 60, "1", "4");
        c.levels_per_side = 3;
        c.inner_bps = 5;
        c.step_bps = 5;
        c.auto_skew = false;
        let g = StaticGrid::new(c);
        let sym = Symbol {
            base: tikr_core::Asset::new("BTC"),
            quote: tikr_core::Asset::new("USDT"),
            venue: tikr_core::VenueId::new("test"),
            kind: tikr_core::MarketKind::Perp,
        };
        let mid = Price(Decimal::from(100_000));
        let (actions, bid_suppressed, ask_suppressed) =
            g.build_batch(&sym, mid, mid, mid, Decimal::ZERO, Decimal::ZERO);
        assert!(
            !bid_suppressed && !ask_suppressed,
            "cap disabled — nothing suppressed"
        );
        // Extract |price - mid| sequence; must be non-decreasing.
        let dists: Vec<Decimal> = actions
            .iter()
            .map(|a| match a {
                Action::Quote(q) => {
                    let d = q.price.0 - mid.0;
                    if d < Decimal::ZERO { -d } else { d }
                }
                _ => panic!("non-quote action in batch"),
            })
            .collect();
        for w in dists.windows(2) {
            assert!(w[0] <= w[1], "batch not sorted inside-out: {dists:?}");
        }
        // Innermost pair = inner_bps = 5bp distance on a 100_000 mid = 50.
        assert_eq!(dists[0], Decimal::from(50));
        assert_eq!(dists[1], Decimal::from(50));
    }

    #[test]
    fn wide_spread_uses_best_bid_ask_minus_1bp() {
        let mut c = cfg("0", 60, "1", "4");
        c.levels_per_side = 3;
        c.inner_bps = 3;
        c.step_bps = 3;
        c.auto_skew = false;
        let g = StaticGrid::new(c);
        let sym = Symbol {
            base: tikr_core::Asset::new("BTC"),
            quote: tikr_core::Asset::new("USDT"),
            venue: tikr_core::VenueId::new("test"),
            kind: tikr_core::MarketKind::Perp,
        };
        // Market spread = 200bps (best_bid=99000, best_ask=101000, mid=100000).
        // inner_bps=3 → mid-based bid = 99700, which is INSIDE the spread.
        // Should use best_bid - 1bp instead.
        let mid = Price(Decimal::from(100_000));
        let best_bid = Price(Decimal::from(99_000));
        let best_ask = Price(Decimal::from(101_000));
        let (actions, _, _) =
            g.build_batch(&sym, mid, best_bid, best_ask, Decimal::ZERO, Decimal::ZERO);
        let bids: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Bid => Some(q.price.0),
                _ => None,
            })
            .collect();
        let asks: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Ask => Some(q.price.0),
                _ => None,
            })
            .collect();
        // k=0 bid is closest to best_bid (spread-aware places at best_bid - 1bp).
        let inner_bid = bids
            .iter()
            .min_by_key(|p| (**p - best_bid.0).abs())
            .unwrap();
        let expected_inner_bid = Decimal::from(99_000) * (Decimal::ONE - Decimal::new(1, 4));
        assert_eq!(*inner_bid, expected_inner_bid);
        // k=0 ask is closest to best_ask.
        let inner_ask = asks
            .iter()
            .min_by_key(|p| (**p - best_ask.0).abs())
            .unwrap();
        let expected_inner_ask = Decimal::from(101_000) * (Decimal::ONE + Decimal::new(1, 4));
        assert_eq!(*inner_ask, expected_inner_ask);
    }

    #[test]
    fn narrow_spread_uses_inner_bps() {
        let mut c = cfg("0", 60, "1", "4");
        c.levels_per_side = 3;
        c.inner_bps = 5;
        c.step_bps = 5;
        c.auto_skew = false;
        let g = StaticGrid::new(c);
        let sym = Symbol {
            base: tikr_core::Asset::new("BTC"),
            quote: tikr_core::Asset::new("USDT"),
            venue: tikr_core::VenueId::new("test"),
            kind: tikr_core::MarketKind::Perp,
        };
        // Market spread = 2bps (best_bid=99990, best_ask=100010, mid=100000).
        // inner_bps=5 → mid-based bid = 99950, which is OUTSIDE the spread (below best_bid).
        // Should use normal inner_bps.
        let mid = Price(Decimal::from(100_000));
        let best_bid = Price(Decimal::from(99_990));
        let best_ask = Price(Decimal::from(100_010));
        let (actions, _, _) =
            g.build_batch(&sym, mid, best_bid, best_ask, Decimal::ZERO, Decimal::ZERO);
        let bids: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Bid => Some(q.price.0),
                _ => None,
            })
            .collect();
        let asks: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Ask => Some(q.price.0),
                _ => None,
            })
            .collect();
        // Innermost bid = mid * (1 - 5bps) = 100000 * 0.9995 = 99950
        let expected_inner_bid = Decimal::from(100_000) * (Decimal::ONE - Decimal::new(5, 4));
        assert_eq!(bids[0], expected_inner_bid);
        // Innermost ask = mid * (1 + 5bps) = 100000 * 1.0005 = 100050
        let expected_inner_ask = Decimal::from(100_000) * (Decimal::ONE + Decimal::new(5, 4));
        assert_eq!(asks[0], expected_inner_ask);
    }
}
