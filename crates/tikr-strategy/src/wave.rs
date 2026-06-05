//! Wave: fixed-lattice band-refill market-making (pure form).
//!
//! A price lattice (origin + step) frozen at bot start. The active `levels`-slot
//! band on each side is a WINDOW over that grid that SLIDES up and down to
//! bracket the current touch: when the market rises the window rises (bids may
//! sit above the bid origin), when it falls the window falls (asks may sit below
//! the ask origin). Only which slots are active moves; the prices orders land on
//! stay on the lattice, so fills happen at consistent grid prices.
//!
//! The grid is frozen in ABSOLUTE price, so as mid drifts the step measured in
//! bps of the current mid drifts off `steps_bps`. A **relattice** event rebuilds
//! the grid (new step + origins) at the current touch when that drift exceeds
//! [`relattice_drift`] — bounded, infrequent, and self-arresting (after a
//! relattice the effective bps is back at `steps_bps`, so it won't re-fire until
//! mid drifts another 10%).
//!
//! ## Knobs
//! - `steps_bps` — bps of mid per lattice step (snapped to tick, min 1 tick).
//!   `0` = a 1-tick lattice.
//! - `steps_inner` — lattice slots to skip between mid and the first order on
//!   each side (the inner dead-zone / self-spread). `0` = first order at the
//!   touch.
//! - `levels` — orders per side.
//! - `round_trips` — completed round-trips (a bid AND an ask both drained by
//!   this many slots) needed to trigger a refill. One whole side draining
//!   refills regardless (re-arm after a one-sided sweep).
//!
//! ## Behavior
//! 1. **Init (first usable book event):** freeze step + origins. Step =
//!    `steps_bps` of mid (snapped to tick), else 1 tick. Origins sit
//!    `steps_inner × step` off mid on each side, clamped to the touch.
//! 2. **Refill** fires when EITHER `round_trips` round-trips completed (≥
//!    `round_trips` bids AND ≥ `round_trips` asks drained → captured spread) OR
//!    one whole side is empty. On refill, re-emit every empty band slot on each
//!    side at the current-touch window and prune the tail (resting orders that
//!    fell outside the slid window). Between refills: nothing.
//!
//! Inventory is bounded by `steps_bps` width (wider step = slower one-sided
//! accumulation) and per-order size — run on small-min-notional markets so
//! accumulated fills stay survivable.

use std::collections::HashSet;

use tikr_core::{Decimal, MarketEvent, Price, QuoteKind, Side, Size, Symbol, TimeInForce};
use tikr_venue::QuoteIntent;

use crate::{Action, Strategy, StrategyContext};

/// Relattice when the frozen lattice step, expressed in bps of the CURRENT mid,
/// drifts more than this fraction (10%) from the configured `steps_bps`.
fn relattice_drift() -> Decimal {
    Decimal::new(1, 1) // 0.10
}

/// Auto-inner: number of trailing 1-second candles averaged to size the inner
/// dead-zone.
const CANDLE_WINDOW: usize = 60;
/// Auto-inner: candle bucket width in nanoseconds (1 second).
const CANDLE_NANOS: u64 = 1_000_000_000;
/// Auto-inner: hard cap on the auto-sized inner (steps), so a volatility spike
/// can't push the first order absurdly far from the touch.
const MAX_AUTO_INNER: u32 = 50;

/// Auto-inner target as a fraction of the average candle range: the inner
/// dead-zone (half-spread) aims for ~half the mean high→low candle gap.
fn auto_inner_fraction() -> Decimal {
    Decimal::new(5, 1) // 0.5
}

/// Configuration for [`Wave`].
#[derive(Debug, Clone)]
pub struct WaveConfig {
    /// Notional in quote currency per order.
    pub notional_per_order: Decimal,
    /// Venue tick size.
    pub tick_size: Decimal,
    /// Venue lot step.
    pub step_size: Decimal,
    /// Venue min order notional.
    pub min_notional: Decimal,

    /// Orders per side. Default 12.
    pub levels: u32,

    /// Level spacing in bps of mid — the gap between consecutive lattice
    /// levels. Snapped to tick (min 1 tick). `0` = 1-tick lattice.
    pub steps_bps: u32,

    /// Inner dead-zone in STEPS: the first order on each side sits
    /// `steps_inner × step` from mid (where the frozen origins are anchored).
    /// e.g. `steps_inner=2, steps_bps=5` → first order 10bps off mid, levels
    /// 5bps apart. `0` (default) = origins at the touch. Snapped to tick.
    /// Used only when `auto_inner == false`.
    pub steps_inner: u32,

    /// Auto-size the inner dead-zone from recent volatility. When `true`
    /// (default behavior), `steps_inner` is ignored: the inner starts at 0 and
    /// tracks ~half the average high→low gap of the last [`CANDLE_WINDOW`]
    /// one-second candles (in bps), divided by `steps_bps` to convert to steps.
    /// When `false`, the fixed `steps_inner` is used.
    pub auto_inner: bool,

    /// Completed round-trips needed to trigger a refill: refill once ≥ this
    /// many slots have drained on the bid AND ≥ this many on the ask (each
    /// drained pair = a captured spread). `1` = refill on any completed
    /// round-trip. A whole side emptying refills regardless of this. Default
    /// `1`.
    pub round_trips: u32,

    /// Slow-market safety valve: if this many seconds pass since the last refill
    /// AND any band slot is vacant, refill the empty slots — short-circuiting
    /// the `round_trips` gate. On a fast market the round-trip / side-empty
    /// triggers always fire first, so this never bites; on a slow one it stops
    /// half-drained bands sitting idle. `0` = disabled. Default `300` (5 min).
    pub force_refill_secs: u64,

    /// Auto-size the lattice step from recent volatility, mirroring `auto_inner`.
    /// When `true` (and `steps_bps > 0`), the live step tracks
    /// `auto_step_k × mean(1s candle high→low gap, bps)`, clamped to
    /// `[floor, steps_bps]` where the floor is the round-trip break-even
    /// (`2 × maker_fee_bps`) and `steps_bps` is the ceiling/anchor. The change is
    /// delivered by the existing relattice (>10% drift) path. `false` (default)
    /// uses the fixed `steps_bps`.
    pub auto_step: bool,

    /// Fraction of the mean candle range one step targets when `auto_step` is on
    /// (e.g. `0.5` → each step banks ~half a typical 1-second swing). The main
    /// tuning lever; default `0.5`.
    pub auto_step_k: Decimal,

    /// Maker fee in bps for this symbol, used only to floor the auto-step at the
    /// round-trip break-even (`2 × maker_fee_bps`). A 0-fee pair → no fee floor
    /// (the 1-tick min in `compute_step` still applies). Live: from
    /// `/fapi/v1/commissionRate`; backtest: the sim's `maker_bps`.
    pub maker_fee_bps: Decimal,
}

#[derive(Debug, Clone, Copy)]
struct WindowRange {
    /// Lowest k index in the window (inclusive).
    low_k: i64,
    /// Highest k index in the window (inclusive).
    high_k: i64,
}

/// Wave strategy state.
pub struct Wave {
    config: WaveConfig,
    /// Frozen on first usable book event.
    bid_lattice_origin: Option<Decimal>,
    ask_lattice_origin: Option<Decimal>,
    /// Frozen lattice step (price) — uniform spacing between levels.
    lattice_step: Option<Decimal>,
    /// Per-event dedupe (in case Quote action sequence has duplicates).
    emitted_this_event_bid: HashSet<i64>,
    emitted_this_event_ask: HashSet<i64>,

    // --- Auto-inner volatility tracking (1-second candles of mid) ---
    /// Current candle's second bucket (`now_ns / CANDLE_NANOS`).
    cur_candle_sec: Option<u64>,
    /// Current candle's running high/low of mid.
    cur_candle_high: Decimal,
    cur_candle_low: Decimal,
    /// Closed-candle high→low gaps in bps, newest at the back, capped at
    /// [`CANDLE_WINDOW`].
    candle_bps: std::collections::VecDeque<Decimal>,
    /// Auto-sized inner dead-zone in steps. Starts at 0, recomputed on each
    /// candle close. Ignored when `config.auto_inner == false`.
    dyn_inner_steps: u32,
    /// Set when `dyn_inner_steps` changed on a candle close; consumed by
    /// `on_event` to force a refill so the resting lattice re-places at the new
    /// inner offset immediately (not on the next round-trip/side-empty).
    inner_dirty: bool,
    /// Auto-sized lattice step in bps. Seeded to `config.steps_bps` (the
    /// ceiling), recomputed on each candle close when `auto_step`. Ignored when
    /// `config.auto_step == false`. Delivered to the live grid by relattice.
    dyn_step_bps: u32,
    /// Event-time (ns) of the last refill, for the `force_refill_secs` valve.
    last_refill_ns: Option<u64>,
}

impl Wave {
    /// Order size for `price`: notional / price, rounded to the lot step and
    /// floored at `min_notional`.
    fn quote_size(&self, price: Price) -> Size {
        if price.0 <= Decimal::ZERO {
            return Size(Decimal::ZERO);
        }
        let raw = self.config.notional_per_order / price.0;
        let stepped = if self.config.step_size > Decimal::ZERO {
            (raw / self.config.step_size).floor() * self.config.step_size
        } else {
            raw
        };
        let min = self.config.min_notional;
        if min > Decimal::ZERO && stepped * price.0 < min && self.config.step_size > Decimal::ZERO {
            let mut needed = (min / price.0 / self.config.step_size).ceil() * self.config.step_size;
            // Guard: the chained Decimal divisions above can truncate the ratio a
            // hair below its true value, so `ceil` lands one lot short and the
            // notional ends up just under min (e.g. 4.9998 < 5 → exchange reject).
            // Bump by whole lots until the notional actually clears min.
            while needed * price.0 < min {
                needed += self.config.step_size;
            }
            Size(needed)
        } else {
            Size(stepped)
        }
    }

    fn make_quote(&self, symbol: &Symbol, side: Side, price: Price) -> Action {
        Action::Quote(QuoteIntent {
            symbol: symbol.clone(),
            side,
            price,
            size: self.quote_size(price),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        })
    }

    /// The inner dead-zone in steps actually used this event: the auto-sized
    /// value when `auto_inner`, else the fixed config knob.
    fn effective_inner(&self) -> u32 {
        if self.config.auto_inner {
            self.dyn_inner_steps
        } else {
            self.config.steps_inner
        }
    }

    /// The lattice step in bps actually used this event: the auto-sized value
    /// when `auto_step` (and `steps_bps > 0`), else the fixed config knob.
    fn effective_step_bps(&self) -> u32 {
        if self.config.auto_step && self.config.steps_bps > 0 {
            self.dyn_step_bps
        } else {
            self.config.steps_bps
        }
    }

    /// Round-trip break-even floor for the auto-step, in bps: `2 × maker_fee`
    /// (one maker buy + one maker sell). A 0-fee pair → 0 (no fee floor; the
    /// 1-tick min in `compute_step` still bounds the step).
    fn floor_step_bps(&self) -> u32 {
        let floor = self.config.maker_fee_bps * Decimal::from(2);
        floor.ceil().to_string().parse::<i64>().unwrap_or(0).max(0) as u32
    }

    /// Recompute the auto-sized step: `auto_step_k × mean candle gap (bps)`,
    /// clamped to `[floor, steps_bps]` (floor = round-trip break-even, ceil =
    /// the configured `steps_bps` anchor). No-op when `auto_step` is off,
    /// `steps_bps == 0`, or no candles yet — `dyn_step_bps` stays at its seed.
    /// The change reaches the live grid via the relattice (>10% drift) path; no
    /// dirty flag needed.
    fn recompute_dyn_step(&mut self) {
        if !self.config.auto_step || self.config.steps_bps == 0 || self.candle_bps.is_empty() {
            return;
        }
        let sum: Decimal = self.candle_bps.iter().copied().sum();
        let avg = sum / Decimal::from(self.candle_bps.len() as u64);
        let raw = avg * self.config.auto_step_k;
        let floor = self.floor_step_bps();
        // Ceil is the configured anchor; if the fee floor exceeds it (a misconfig
        // where the configured step can't clear break-even), the floor wins.
        let ceil = self.config.steps_bps.max(floor);
        self.dyn_step_bps = raw
            .round()
            .to_string()
            .parse::<i64>()
            .unwrap_or(self.config.steps_bps as i64)
            .clamp(floor as i64, ceil as i64) as u32;
    }

    /// Fold the current mid into the 1-second candle tracker. On a second
    /// rollover the just-closed candle's high→low gap (bps) is pushed into the
    /// rolling window and the auto-inner is recomputed. No-op unless
    /// `auto_inner` (avoids the work when the feature is off).
    fn update_candles(&mut self, mid: Decimal, now_ns: u64) {
        if !(self.config.auto_inner || self.config.auto_step) || mid <= Decimal::ZERO {
            return;
        }
        let sec = now_ns / CANDLE_NANOS;
        match self.cur_candle_sec {
            Some(cur) if cur == sec => {
                if mid > self.cur_candle_high {
                    self.cur_candle_high = mid;
                }
                if mid < self.cur_candle_low {
                    self.cur_candle_low = mid;
                }
            }
            Some(_) => {
                // Second rolled over → close the prior candle.
                let cmid = (self.cur_candle_high + self.cur_candle_low) / Decimal::from(2);
                if cmid > Decimal::ZERO {
                    let gap_bps =
                        (self.cur_candle_high - self.cur_candle_low) / cmid * Decimal::from(10_000);
                    if self.candle_bps.len() == CANDLE_WINDOW {
                        self.candle_bps.pop_front();
                    }
                    self.candle_bps.push_back(gap_bps);
                    // Step first: the inner divides by the effective step.
                    self.recompute_dyn_step();
                    self.recompute_dyn_inner();
                }
                self.cur_candle_sec = Some(sec);
                self.cur_candle_high = mid;
                self.cur_candle_low = mid;
            }
            None => {
                self.cur_candle_sec = Some(sec);
                self.cur_candle_high = mid;
                self.cur_candle_low = mid;
            }
        }
    }

    /// Recompute the auto-sized inner: ~half the mean candle gap (bps) over the
    /// window, divided by `steps_bps` to convert bps → steps, clamped to
    /// `[0, MAX_AUTO_INNER]`. No-op when `steps_bps == 0` (no bps-per-step to
    /// divide by — inner stays 0).
    fn recompute_dyn_inner(&mut self) {
        // Divide by the EFFECTIVE step (the auto-sized one when `auto_step`), so
        // the inner dead-zone is measured in current steps. Must run AFTER
        // `recompute_dyn_step` on a candle close.
        let sbps = self.effective_step_bps();
        let steps = if sbps == 0 || self.candle_bps.is_empty() {
            0
        } else {
            let sum: Decimal = self.candle_bps.iter().copied().sum();
            let avg = sum / Decimal::from(self.candle_bps.len() as u64);
            let target_bps = avg * auto_inner_fraction();
            (target_bps / Decimal::from(sbps))
                .round()
                .to_string()
                .parse::<i64>()
                .unwrap_or(0)
                .clamp(0, MAX_AUTO_INNER as i64) as u32
        };
        if steps != self.dyn_inner_steps {
            self.dyn_inner_steps = steps;
            // Force a refill this event so the lattice re-places at the new
            // inner offset immediately.
            self.inner_dirty = true;
        }
    }

    /// Compute `(top_bid_override, top_ask_override)`, pushing the origins
    /// apart to honor the inner dead-zone (`steps_inner × step` off mid each
    /// side, clamped to the touch).
    fn top_overrides(
        &self,
        best_bid: Option<Price>,
        best_ask: Option<Price>,
    ) -> (Option<Price>, Option<Price>) {
        let tick = self.config.tick_size;
        let inner = self.effective_inner();
        let spread_active = self.config.steps_bps > 0 || inner > 0;
        if let (Some(bp), Some(ap)) = (best_bid, best_ask)
            && bp.0 > Decimal::ZERO
            && ap.0 > bp.0
            && tick > Decimal::ZERO
            && spread_active
        {
            let mid = (bp.0 + ap.0) / Decimal::from(2);
            // Distance from mid to the first order on each side =
            // `inner × step`. `inner=0` → offset 0 → origins clamp to the touch
            // via the .min(bp)/.max(ap) below.
            let required_half = Decimal::from(inner) * self.compute_step(mid);
            let raw_top_bid = mid - required_half;
            let raw_top_ask = mid + required_half;
            let snapped_bid = (raw_top_bid / tick).floor() * tick;
            let snapped_ask = (raw_top_ask / tick).ceil() * tick;
            (
                Some(Price(snapped_bid.min(bp.0))),
                Some(Price(snapped_ask.max(ap.0))),
            )
        } else {
            (best_bid, best_ask)
        }
    }

    /// Base lattice gap = `steps_bps` of mid, snapped up to tick (min 1 tick).
    /// `steps_bps = 0` → 1-tick gap. This is the distance from origin to the
    /// first level.
    fn compute_step(&self, mid: Decimal) -> Decimal {
        let tick = self.config.tick_size;
        let sbps = self.effective_step_bps();
        if sbps > 0 && mid > Decimal::ZERO && tick > Decimal::ZERO {
            let target = mid * Decimal::from(sbps) / Decimal::from(10_000);
            return if target > tick {
                (target / tick).ceil() * tick
            } else {
                tick
            };
        }
        tick
    }

    /// If the frozen lattice step has drifted more than [`relattice_drift`] from
    /// the step we'd freshly install at the current mid, rebuild the grid (fresh
    /// step + origins) at the current touch and cancel every resting order so the
    /// grid is a clean slate. Returns `true` if it relatticed.
    ///
    /// The comparison target is `compute_step(mid)` — the step that encodes
    /// `steps_bps` of the CURRENT mid, snapped to tick. In the normal regime
    /// (`steps_bps` of mid ≫ tick) this is exactly "effective bps drifted >10%
    /// from `steps_bps`", but using the install-able step makes it
    /// self-arresting (diff is 0 right after a relattice, so it can't re-fire
    /// until mid drifts another 10%) and a correct no-op in the tick-dominated
    /// regime where the bps target is unreachable (comparing raw effective bps
    /// there would thrash every event). No-op when `steps_bps == 0` — a 1-tick
    /// lattice has no bps target.
    fn maybe_relattice(
        &mut self,
        ctx: &StrategyContext<'_>,
        top_b: Option<Price>,
        top_a: Option<Price>,
        actions: &mut Vec<Action>,
    ) -> bool {
        let sbps = self.effective_step_bps();
        let tick = self.config.tick_size;
        if sbps == 0 || tick <= Decimal::ZERO {
            return false;
        }
        let (Some(step), Some(b), Some(a)) = (self.lattice_step, top_b, top_a) else {
            return false;
        };
        if !(b.0 > Decimal::ZERO && a.0 > b.0) {
            return false;
        }
        let mid = (b.0 + a.0) / Decimal::from(2);
        if mid <= Decimal::ZERO {
            return false;
        }
        let fresh = self.compute_step(mid);
        if fresh <= Decimal::ZERO {
            return false;
        }
        let drift = (step - fresh).abs() / fresh;
        if drift <= relattice_drift() {
            return false;
        }
        // Relattice: cancel everything on the stale grid, then re-freeze at the
        // current touch. Re-seeding happens via the normal refill path this same
        // event (the new grid reads as fully drained).
        for (id, _) in ctx.open_quotes {
            actions.push(Action::Cancel(*id));
        }
        let eff_bps = step / mid * Decimal::from(10_000);
        self.lattice_step = Some(fresh);
        self.bid_lattice_origin = Some(b.0);
        self.ask_lattice_origin = Some(a.0);
        tracing::info!(
            symbol = %ctx.symbol.base.0,
            mid = %mid,
            old_step = %step,
            new_step = %fresh,
            eff_bps = %eff_bps.round_dp(2),
            steps_bps = sbps,
            drift_pct = %(drift * Decimal::from(100)).round_dp(1),
            cancelled = ctx.open_quotes.len(),
            "wave: relattice"
        );
        true
    }

    /// BID slot price at index k (k=0 is the top/origin, larger k = lower).
    /// Uniform lattice: slots are `step` apart.
    fn bid_price(&self, k: i64) -> Option<Decimal> {
        let origin = self.bid_lattice_origin?;
        let step = self.lattice_step?;
        let p = origin - Decimal::from(k) * step;
        if p > Decimal::ZERO { Some(p) } else { None }
    }

    /// ASK slot price at index k (k=0 is the top/origin, larger k = higher).
    fn ask_price(&self, k: i64) -> Option<Decimal> {
        let origin = self.ask_lattice_origin?;
        let step = self.lattice_step?;
        let p = origin + Decimal::from(k) * step;
        if p > Decimal::ZERO { Some(p) } else { None }
    }

    /// k index of the BID slot at or below `price` = `ceil((origin - price) / step)`.
    /// `price >= origin` → `k <= 0`.
    fn bid_k_at_or_below(&self, price: Decimal) -> Option<i64> {
        let origin = self.bid_lattice_origin?;
        let step = self.lattice_step?;
        if step <= Decimal::ZERO {
            return None;
        }
        ((origin - price) / step)
            .ceil()
            .to_string()
            .parse::<i64>()
            .ok()
    }

    /// k index of the ASK slot at or above `price` = `ceil((price - origin) / step)`.
    fn ask_k_at_or_above(&self, price: Decimal) -> Option<i64> {
        let origin = self.ask_lattice_origin?;
        let step = self.lattice_step?;
        if step <= Decimal::ZERO {
            return None;
        }
        ((price - origin) / step)
            .ceil()
            .to_string()
            .parse::<i64>()
            .ok()
    }

    /// Cancel only the TRAILING tail on `side` — resting orders past the deep
    /// edge of the window, i.e. the ones price has moved AWAY from. Orders
    /// between the window and the touch (the LEADING side, where price is
    /// heading) are deliberately left resting so price fills them as it passes
    /// each rung, instead of the window sliding the order out from under an
    /// incoming fill. Used by the price-tracking refill.
    ///
    /// BID: the deep edge is the lowest active price (`origin - high_k·step`);
    /// bids BELOW it are the tail left behind when price rose away — prune
    /// those. Shallower bids (price falling toward them) are kept.
    /// ASK: mirror — prune only asks ABOVE the highest active price
    /// (`origin + high_k·step`); keep the lower asks price is rising toward.
    fn prune_trailing_tail(
        &self,
        ctx: &StrategyContext<'_>,
        side: Side,
        band: WindowRange,
        actions: &mut Vec<Action>,
    ) {
        let edge = match side {
            Side::Bid => self.bid_price(band.high_k), // deepest (lowest) active bid
            Side::Ask => self.ask_price(band.high_k), // deepest (highest) active ask
        };
        let Some(edge) = edge else {
            return;
        };
        for (id, q) in ctx.open_quotes {
            if q.side != side {
                continue;
            }
            let is_trailing_tail = match side {
                Side::Bid => q.price.0 < edge, // below the deepest active bid
                Side::Ask => q.price.0 > edge, // above the deepest active ask
            };
            if is_trailing_tail {
                actions.push(Action::Cancel(*id));
            }
        }
    }

    /// Cancel EVERY resting order on `side` outside the band on either edge.
    /// Used only for a deliberate reposition (auto-inner dead-zone resize),
    /// where near-touch orders genuinely need to move — unlike the price-
    /// tracking refill, which keeps leading orders via [`Self::prune_trailing_tail`].
    fn prune_full_window(
        &self,
        ctx: &StrategyContext<'_>,
        side: Side,
        band: WindowRange,
        actions: &mut Vec<Action>,
    ) {
        let (lo, hi) = match side {
            Side::Bid => {
                let (Some(deep), Some(shallow)) =
                    (self.bid_price(band.high_k), self.bid_price(band.low_k))
                else {
                    return;
                };
                (deep, shallow)
            }
            Side::Ask => {
                let (Some(shallow), Some(deep)) =
                    (self.ask_price(band.low_k), self.ask_price(band.high_k))
                else {
                    return;
                };
                (shallow, deep)
            }
        };
        for (id, q) in ctx.open_quotes {
            if q.side == side && (q.price.0 < lo || q.price.0 > hi) {
                actions.push(Action::Cancel(*id));
            }
        }
    }

    /// Count band slots on `side` with no matching resting order in
    /// `ctx.open_quotes` (= empty/filled). Used to gate batched refill.
    fn band_missing(&self, ctx: &StrategyContext<'_>, side: Side, band: WindowRange) -> u32 {
        let mut missing = 0u32;
        for k in band.low_k..=band.high_k {
            let Some(p) = (match side {
                Side::Bid => self.bid_price(k),
                Side::Ask => self.ask_price(k),
            }) else {
                continue;
            };
            let present = ctx
                .open_quotes
                .iter()
                .any(|(_, q)| q.side == side && q.price.0 == p);
            if !present {
                missing = missing.saturating_add(1);
            }
        }
        missing
    }

    /// Issue Quote actions for every slot in `[low_k, high_k]` on `side`
    /// that's not already present in `ctx.open_quotes`. Updates the
    /// in-event dedupe set as it emits.
    fn emit_window_slots(
        &mut self,
        ctx: &StrategyContext<'_>,
        side: Side,
        window: WindowRange,
        actions: &mut Vec<Action>,
    ) {
        let cross_guard_ask = ctx.latest_book.asks.first().map(|l| l.price);
        let cross_guard_bid = ctx.latest_book.bids.first().map(|l| l.price);
        let tick = self.config.tick_size;
        for k in window.low_k..=window.high_k {
            let Some(price_raw) = (match side {
                Side::Bid => self.bid_price(k),
                Side::Ask => self.ask_price(k),
            }) else {
                continue;
            };
            // Cross-guard: never emit BID >= best_ask, never emit ASK <= best_bid.
            let safe_price = match side {
                Side::Bid => {
                    if let Some(ap) = cross_guard_ask
                        && ap.0 > Decimal::ZERO
                        && tick > Decimal::ZERO
                    {
                        let cap = ap.0 - tick;
                        if price_raw > cap {
                            continue; // skip — would cross
                        }
                    }
                    price_raw
                }
                Side::Ask => {
                    if let Some(bp) = cross_guard_bid
                        && bp.0 > Decimal::ZERO
                        && tick > Decimal::ZERO
                    {
                        let floor = bp.0 + tick;
                        if price_raw < floor {
                            continue;
                        }
                    }
                    price_raw
                }
            };
            if safe_price <= Decimal::ZERO {
                continue;
            }
            // Dedupe within this event + against open_quotes.
            let emitted = match side {
                Side::Bid => self.emitted_this_event_bid.contains(&k),
                Side::Ask => self.emitted_this_event_ask.contains(&k),
            };
            if emitted {
                continue;
            }
            let present = ctx
                .open_quotes
                .iter()
                .any(|(_, q)| q.side == side && q.price.0 == safe_price);
            if present {
                continue;
            }
            actions.push(self.make_quote(ctx.symbol, side, Price(safe_price)));
            match side {
                Side::Bid => {
                    self.emitted_this_event_bid.insert(k);
                }
                Side::Ask => {
                    self.emitted_this_event_ask.insert(k);
                }
            }
        }
    }
}

impl Strategy for Wave {
    type Config = WaveConfig;

    fn new(config: Self::Config) -> Self {
        // Seed the auto-step at the configured `steps_bps` (the ceiling/anchor),
        // so the bot starts at the configured spacing and adapts down as candles
        // arrive.
        let dyn_step_bps = config.steps_bps;
        Self {
            config,
            bid_lattice_origin: None,
            ask_lattice_origin: None,
            lattice_step: None,
            emitted_this_event_bid: HashSet::new(),
            emitted_this_event_ask: HashSet::new(),
            cur_candle_sec: None,
            cur_candle_high: Decimal::ZERO,
            cur_candle_low: Decimal::ZERO,
            candle_bps: std::collections::VecDeque::with_capacity(CANDLE_WINDOW),
            dyn_inner_steps: 0,
            inner_dirty: false,
            dyn_step_bps,
            last_refill_ns: None,
        }
    }

    fn name(&self) -> &str {
        "wave"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, _event: &MarketEvent) -> Vec<Action> {
        self.emitted_this_event_bid.clear();
        self.emitted_this_event_ask.clear();
        let mut actions: Vec<Action> = Vec::new();

        let best_bid = ctx.latest_book.bids.first().map(|l| l.price);
        let best_ask = ctx.latest_book.asks.first().map(|l| l.price);

        // Auto-inner: fold this event's mid into the 1s candle tracker before
        // computing the window so the inner dead-zone reflects current vol.
        if let (Some(bp), Some(ap)) = (best_bid, best_ask)
            && bp.0 > Decimal::ZERO
            && ap.0 > bp.0
        {
            let mid = (bp.0 + ap.0) / Decimal::from(2);
            self.update_candles(mid, ctx.now.0);
        }

        let (top_b, top_a) = self.top_overrides(best_bid, best_ask);
        let tick = self.config.tick_size;

        // 1) Lattice init (one-shot): freeze step + origins on first usable book.
        if self.lattice_step.is_none()
            && let (Some(b), Some(a)) = (top_b, top_a)
            && b.0 > Decimal::ZERO
            && a.0 > b.0
            && tick > Decimal::ZERO
        {
            let mid = (b.0 + a.0) / Decimal::from(2);
            let base = self.compute_step(mid);
            self.lattice_step = Some(base);
            self.bid_lattice_origin = Some(b.0);
            self.ask_lattice_origin = Some(a.0);
            tracing::info!(
                symbol = %ctx.symbol.base.0,
                mid = %mid,
                tick = %self.config.tick_size,
                steps_bps = self.config.steps_bps,
                auto_inner = self.config.auto_inner,
                inner_steps = self.effective_inner(),
                step = %base,
                inner_offset = %(self.bid_lattice_origin.map(|o| mid - o).unwrap_or_default()),
                "wave: lattice frozen"
            );
        }

        let lattice_ready = self.lattice_step.is_some()
            && self.bid_lattice_origin.is_some()
            && self.ask_lattice_origin.is_some();
        if !lattice_ready {
            return actions;
        }

        // 1b) Relattice: if the frozen step has drifted >10% (in bps of the
        // current mid) from steps_bps, rebuild the grid at the current touch and
        // cancel the stale orders. The re-seed happens via the refill path below
        // (the fresh grid reads fully drained).
        let relatticed = self.maybe_relattice(ctx, top_b, top_a, &mut actions);

        // Auto-inner just changed → re-place the lattice at the new offset this
        // event (consume the flag regardless of whether a band is computable).
        let inner_dirty = std::mem::take(&mut self.inner_dirty);

        // 2) Round-trip refill on the FIXED lattice.
        //
        // Refill fires when BOTH sides of the band have drained by
        // ≥ round_trips slots since the last refill — i.e. ≥ round_trips bids
        // AND ≥ round_trips asks filled. Each drained pair is a completed
        // round-trip (bought low + sold high), so every refill cycle banks the
        // captured spread. OR when one whole side is empty — re-arming the grid
        // after a one-sided sweep instead of going dormant.
        //
        // On refill: re-emit every empty slot on both sides at their
        // current-touch band prices, then prune the tail (orders left outside
        // the new band). Between refills: do nothing.
        let levels = self.config.levels.max(1) as i64;

        // Compute both bands around the cross-guarded touch. The active window
        // SLIDES along the frozen grid to bracket the current touch: the
        // shallowest bid sits at the grid slot at-or-below the (inner-offset)
        // touch and extends `levels` deeper; the shallowest ask at the slot
        // at-or-above and extends `levels` higher. `top_k` may go negative —
        // i.e. the window tracks the market PAST the origin (bids above the bid
        // origin / asks below the ask origin) — because the grid is frozen but
        // the window that gets filled follows price up and down. The grid prices
        // themselves never move; only which `levels` slots are active.
        let bid_band = top_b.filter(|t| t.0 > Decimal::ZERO).and_then(|top| {
            let mut cap = top.0;
            if let Some(ap) = best_ask
                && ap.0 > Decimal::ZERO
                && tick > Decimal::ZERO
            {
                cap = cap.min(ap.0 - tick);
            }
            self.bid_k_at_or_below(cap).map(|top_k| WindowRange {
                low_k: top_k,
                high_k: top_k + levels - 1,
            })
        });
        let ask_band = top_a.filter(|t| t.0 > Decimal::ZERO).and_then(|top| {
            let mut cap = top.0;
            if let Some(bp) = best_bid
                && bp.0 > Decimal::ZERO
                && tick > Decimal::ZERO
            {
                cap = cap.max(bp.0 + tick);
            }
            self.ask_k_at_or_above(cap).map(|top_k| WindowRange {
                low_k: top_k,
                high_k: top_k + levels - 1,
            })
        });

        if let (Some(bb), Some(ab)) = (bid_band, ask_band) {
            let bid_drained = self.band_missing(ctx, Side::Bid, bb);
            let ask_drained = self.band_missing(ctx, Side::Ask, ab);
            let thr = self.config.round_trips.max(1);
            let full = self.config.levels.max(1);
            let round_trip = bid_drained >= thr && ask_drained >= thr;
            let side_empty = bid_drained >= full || ask_drained >= full;
            // Slow-market valve: any vacant slot + `force_refill_secs` elapsed
            // since the last refill → refill anyway (short-circuit round_trips).
            // Never bites on a fast market — round_trip/side_empty fire first.
            let force_refill = self.config.force_refill_secs > 0
                && (bid_drained > 0 || ask_drained > 0)
                && self.last_refill_ns.is_none_or(|last| {
                    ctx.now.0.saturating_sub(last)
                        >= self.config.force_refill_secs.saturating_mul(1_000_000_000)
                });
            // After a relattice the stale orders are already queued for cancel,
            // so seed the fresh grid directly and skip prune (avoids a redundant
            // double-cancel of those same ids).
            if relatticed {
                self.emit_window_slots(ctx, Side::Bid, bb, &mut actions);
                self.emit_window_slots(ctx, Side::Ask, ab, &mut actions);
                self.last_refill_ns = Some(ctx.now.0);
            } else if inner_dirty {
                // Auto-inner changed → deliberate reposition to the new offset:
                // re-emit and FULL-prune (near-touch orders inside the new dead
                // zone must move).
                self.emit_window_slots(ctx, Side::Bid, bb, &mut actions);
                self.emit_window_slots(ctx, Side::Ask, ab, &mut actions);
                self.prune_full_window(ctx, Side::Bid, bb, &mut actions);
                self.prune_full_window(ctx, Side::Ask, ab, &mut actions);
                self.last_refill_ns = Some(ctx.now.0);
            } else if round_trip || side_empty || force_refill {
                // Price-tracking refill: re-emit empty slots and prune only the
                // TRAILING tail. Orders price is moving INTO are left to fill —
                // never slid out from under an incoming price. `force_refill`
                // adds the slow-market valve on top of the round-trip triggers.
                self.emit_window_slots(ctx, Side::Bid, bb, &mut actions);
                self.emit_window_slots(ctx, Side::Ask, ab, &mut actions);
                self.prune_trailing_tail(ctx, Side::Bid, bb, &mut actions);
                self.prune_trailing_tail(ctx, Side::Ask, ab, &mut actions);
                self.last_refill_ns = Some(ctx.now.0);
            }
        }

        actions
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::{
        Asset, Level, MarketKind, Notional, Position, SignedSize, Snapshot, Timestamp, VenueId,
    };
    use tikr_venue::QuoteId;

    fn sym() -> Symbol {
        Symbol {
            base: Asset::new("BTC"),
            quote: Asset::new("USDC"),
            venue: VenueId::new("test"),
            kind: MarketKind::Perp,
        }
    }

    fn cfg() -> WaveConfig {
        WaveConfig {
            notional_per_order: Decimal::from(50),
            tick_size: Decimal::new(1, 1),
            step_size: Decimal::new(1, 3),
            min_notional: Decimal::from(5),
            levels: 6,
            steps_bps: 10,
            steps_inner: 0,
            // Existing tests exercise the fixed-inner (manual) path; auto-inner
            // has its own dedicated tests.
            auto_inner: false,
            round_trips: 1,
            // Off by default in tests; the force-refill valve has its own test.
            force_refill_secs: 0,
            // Auto-step off by default; it has its own dedicated tests.
            auto_step: false,
            auto_step_k: Decimal::new(5, 1),
            maker_fee_bps: Decimal::from(2),
        }
    }

    fn pos_flat() -> Position {
        Position {
            symbol: sym(),
            size: SignedSize(Decimal::ZERO),
            avg_entry: Price(Decimal::ZERO),
            realized_pnl: Notional(Decimal::ZERO),
        }
    }

    fn snap(bid: Decimal, ask: Decimal) -> Snapshot {
        Snapshot {
            symbol: sym(),
            bids: vec![Level {
                price: Price(bid),
                size: Size(Decimal::from(100)),
            }],
            asks: vec![Level {
                price: Price(ask),
                size: Size(Decimal::from(100)),
            }],
            ts: Timestamp(1),
        }
    }

    fn ctx<'a>(
        symbol: &'a Symbol,
        s: &'a Snapshot,
        p: &'a Position,
        open: &'a [(QuoteId, QuoteIntent)],
    ) -> StrategyContext<'a> {
        ctx_at(symbol, s, p, open, 1)
    }

    fn ctx_at<'a>(
        symbol: &'a Symbol,
        s: &'a Snapshot,
        p: &'a Position,
        open: &'a [(QuoteId, QuoteIntent)],
        now_ns: u64,
    ) -> StrategyContext<'a> {
        StrategyContext {
            symbol,
            now: Timestamp(now_ns),
            position: p,
            recent_fills: &[],
            latest_book: s,
            open_quotes: open,
            recent_liqs: &[],
        }
    }

    #[test]
    fn steps_inner_pushes_first_order_out_in_step_multiples() {
        let s = snap(Decimal::from(100), Decimal::new(10002, 2)); // 100 / 100.02
        let mid = Decimal::new(10001, 2);
        let sm = sym();
        let p = pos_flat();
        let freeze = |c: WaveConfig| {
            let mut w = Wave::new(c);
            let _ = w.on_event(
                &ctx(&sm, &s, &p, &[]),
                &MarketEvent::BookUpdate {
                    snapshot: s.clone(),
                },
            );
            let b0 = w.bid_price(0).unwrap();
            let b1 = w.bid_price(1).unwrap();
            (mid - b0, b0 - b1) // (inner gap from mid, step gap)
        };
        // steps_inner=2 with steps_bps=10: first order ~2 steps (20bps) off mid.
        let mut wide = cfg();
        wide.tick_size = Decimal::new(1, 2);
        wide.steps_bps = 10;
        wide.steps_inner = 2;
        let (inner_wide, step_wide) = freeze(wide);
        // steps_inner=1: first order ~1 step (10bps) off mid.
        let mut narrow = cfg();
        narrow.tick_size = Decimal::new(1, 2);
        narrow.steps_bps = 10;
        narrow.steps_inner = 1;
        let (inner_narrow, step_narrow) = freeze(narrow);
        // More steps_inner ⇒ first order FARTHER from mid.
        assert!(
            inner_wide > inner_narrow,
            "steps_inner=2 ({inner_wide}) must push out farther than steps_inner=1 ({inner_narrow})"
        );
        // Step spacing is unchanged by steps_inner.
        assert_eq!(
            step_wide, step_narrow,
            "step spacing independent of steps_inner"
        );
    }

    #[test]
    fn uniform_lattice_has_equal_gaps() {
        let mut w = Wave::new(cfg());
        let s = snap(Decimal::from(100), Decimal::new(1001, 1));
        let p = pos_flat();
        let sm = sym();
        let _ = w.on_event(
            &ctx(&sm, &s, &p, &[]),
            &MarketEvent::BookUpdate {
                snapshot: s.clone(),
            },
        );
        let b: Vec<Decimal> = (0..4).map(|k| w.bid_price(k).unwrap()).collect();
        assert_eq!(b[0] - b[1], b[1] - b[2], "uniform gaps must be equal");
        assert_eq!(b[1] - b[2], b[2] - b[3]);
    }

    #[test]
    fn quote_size_always_meets_min_notional() {
        // The min-notional bump must never leave the order a hair under min
        // (the 4.9998 < 5 reject). Whole-lot step = worst case for the chained-
        // division truncation; sweep awkward repeating-decimal prices.
        let mut c = cfg();
        c.min_notional = Decimal::from(5);
        c.step_size = Decimal::ONE; // whole-lot step
        c.notional_per_order = Decimal::ONE; // tiny → forces the min-notional path
        let w = Wave::new(c);
        for i in 1..=500u32 {
            let price = Decimal::from(i) / Decimal::from(133); // 133 = 7×19 → repeating
            let sz = w.quote_size(Price(price)).0;
            assert!(
                sz * price >= Decimal::from(5),
                "notional {} < min 5 at price {price} (size {sz})",
                sz * price
            );
        }
    }

    #[test]
    fn seeds_full_window_on_first_event() {
        let mut w = Wave::new(cfg());
        let s = snap(Decimal::from(100), Decimal::new(1001, 1));
        let p = pos_flat();
        let sm = sym();
        let c = ctx(&sm, &s, &p, &[]);
        let actions = w.on_event(
            &c,
            &MarketEvent::BookUpdate {
                snapshot: s.clone(),
            },
        );
        // 6 bids + 6 asks
        assert_eq!(actions.len(), 12);
    }

    #[test]
    fn quiet_event_emits_nothing_when_band_intact() {
        let mut w = Wave::new(cfg());
        let s = snap(Decimal::from(100), Decimal::new(1001, 1));
        let p = pos_flat();
        let sm = sym();
        let c = ctx(&sm, &s, &p, &[]);
        // First event seeds the band — capture every emitted quote.
        let seeded = w.on_event(
            &c,
            &MarketEvent::BookUpdate {
                snapshot: s.clone(),
            },
        );
        assert!(!seeded.is_empty(), "first event should place the band");
        let open: Vec<(QuoteId, QuoteIntent)> = seeded
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) => Some((QuoteId::new(), q.clone())),
                _ => None,
            })
            .collect();
        // Replay the same book with those orders resting → band is intact,
        // refill should emit nothing (no slot is empty).
        let c2 = ctx(&sm, &s, &p, &open);
        let actions = w.on_event(
            &c2,
            &MarketEvent::BookUpdate {
                snapshot: s.clone(),
            },
        );
        assert!(actions.is_empty(), "no churn when band intact: {actions:?}");
    }

    #[test]
    fn side_empty_refills_regardless_of_round_trips() {
        // round_trips set high so the round-trip trigger can't fire; drain the
        // whole bid side → side-empty must still refill it.
        let mut c = cfg();
        c.round_trips = 100; // round-trip trigger effectively disabled
        let mut w = Wave::new(c);
        let s = snap(Decimal::from(100), Decimal::new(1001, 1));
        let p = pos_flat();
        let sm = sym();
        let seeded = w.on_event(
            &ctx(&sm, &s, &p, &[]),
            &MarketEvent::BookUpdate {
                snapshot: s.clone(),
            },
        );
        // Keep ONLY the ask orders resting → the whole bid side is empty.
        let open: Vec<(QuoteId, QuoteIntent)> = seeded
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Ask => Some((QuoteId::new(), q.clone())),
                _ => None,
            })
            .collect();
        let actions = w.on_event(
            &ctx(&sm, &s, &p, &open),
            &MarketEvent::BookUpdate {
                snapshot: s.clone(),
            },
        );
        let new_bids = actions
            .iter()
            .filter(|a| matches!(a, Action::Quote(q) if q.side == Side::Bid))
            .count();
        assert_eq!(
            new_bids, 6,
            "empty bid side must refill regardless of round_trips: {actions:?}"
        );
    }

    #[test]
    fn window_slides_to_track_market_both_ways() {
        // Freeze at mid ~100, then move the book far UP: the bid window must
        // slide up PAST the bid origin (bids quoted above 100, near the new
        // touch), not stay pinned at the frozen origin. Then move far DOWN: the
        // ask window must slide below the ask origin.
        let mut w = Wave::new(cfg());
        let s0 = snap(Decimal::from(100), Decimal::new(1001, 1)); // bid 100 / ask 100.1
        let p = pos_flat();
        let sm = sym();
        let seeded = w.on_event(
            &ctx(&sm, &s0, &p, &[]),
            &MarketEvent::BookUpdate {
                snapshot: s0.clone(),
            },
        );
        let open: Vec<(QuoteId, QuoteIntent)> = seeded
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) => Some((QuoteId::new(), q.clone())),
                _ => None,
            })
            .collect();
        let bid_origin = w.bid_lattice_origin.unwrap();
        let ask_origin = w.ask_lattice_origin.unwrap();

        // Market jumps UP to ~110. The whole prior band is now stale (drained) →
        // refill; emitted bids must bracket the NEW touch, i.e. above the origin.
        let s_up = snap(Decimal::from(110), Decimal::new(1101, 1)); // bid 110 / ask 110.1
        let a_up = w.on_event(
            &ctx(&sm, &s_up, &p, &open),
            &MarketEvent::BookUpdate {
                snapshot: s_up.clone(),
            },
        );
        let max_bid = a_up
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Bid => Some(q.price.0),
                _ => None,
            })
            .max()
            .expect("must re-emit bids after the up move");
        assert!(
            max_bid > bid_origin,
            "bid window must slide UP past origin {bid_origin} to track the 110 touch, got {max_bid}"
        );

        // Market drops to ~90: ask window must slide below the ask origin.
        let s_dn = snap(Decimal::from(90), Decimal::new(901, 1)); // bid 90 / ask 90.1
        let a_dn = w.on_event(
            &ctx(&sm, &s_dn, &p, &open),
            &MarketEvent::BookUpdate {
                snapshot: s_dn.clone(),
            },
        );
        let min_ask = a_dn
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Ask => Some(q.price.0),
                _ => None,
            })
            .min()
            .expect("must re-emit asks after the down move");
        assert!(
            min_ask < ask_origin,
            "ask window must slide DOWN past origin {ask_origin} to track the 90 touch, got {min_ask}"
        );
    }

    #[test]
    fn refill_prunes_only_trailing_tail_not_leading_orders() {
        // Seed at ~100, jump the book UP to ~110. Price rose AWAY from the bids
        // (trailing tail) and rose THROUGH the asks (leading — price passed
        // them). Only the trailing bids may be cancelled; the asks price moved
        // into must be LEFT resting so they fill instead of being slid out from
        // under the incoming price.
        let mut w = Wave::new(cfg());
        let s0 = snap(Decimal::from(100), Decimal::new(1001, 1));
        let p = pos_flat();
        let sm = sym();
        let seeded = w.on_event(
            &ctx(&sm, &s0, &p, &[]),
            &MarketEvent::BookUpdate {
                snapshot: s0.clone(),
            },
        );
        let open: Vec<(QuoteId, QuoteIntent)> = seeded
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) => Some((QuoteId::new(), q.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(open.len(), 12, "seed should rest 12 orders");
        let bid_ids: std::collections::HashSet<QuoteId> = open
            .iter()
            .filter(|(_, q)| q.side == Side::Bid)
            .map(|(id, _)| *id)
            .collect();
        let ask_ids: std::collections::HashSet<QuoteId> = open
            .iter()
            .filter(|(_, q)| q.side == Side::Ask)
            .map(|(id, _)| *id)
            .collect();

        let s_up = snap(Decimal::from(110), Decimal::new(1101, 1));
        let a_up = w.on_event(
            &ctx(&sm, &s_up, &p, &open),
            &MarketEvent::BookUpdate {
                snapshot: s_up.clone(),
            },
        );
        let cancelled: std::collections::HashSet<QuoteId> = a_up
            .iter()
            .filter_map(|a| match a {
                Action::Cancel(id) => Some(*id),
                _ => None,
            })
            .collect();
        assert_eq!(
            cancelled, bid_ids,
            "only the trailing bids (price moved away) may be cancelled: {a_up:?}"
        );
        assert!(
            cancelled.is_disjoint(&ask_ids),
            "asks price rose through must NOT be cancelled — let them fill: {a_up:?}"
        );
    }

    #[test]
    fn round_trips_threshold_gates_refill() {
        // round_trips=2: one bid + one ask drained (1 round-trip) must NOT
        // refill; two of each (2 round-trips) must.
        let mut c = cfg();
        c.round_trips = 2;
        let mut w = Wave::new(c);
        let s = snap(Decimal::from(100), Decimal::new(1001, 1));
        let p = pos_flat();
        let sm = sym();
        let seeded = w.on_event(
            &ctx(&sm, &s, &p, &[]),
            &MarketEvent::BookUpdate {
                snapshot: s.clone(),
            },
        );
        let all: Vec<(QuoteId, QuoteIntent)> = seeded
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) => Some((QuoteId::new(), q.clone())),
                _ => None,
            })
            .collect();
        // Drop exactly 1 bid + 1 ask (1 round-trip) → below threshold → no refill.
        let drop_one = |side: Side, open: &[(QuoteId, QuoteIntent)]| {
            let mut dropped = false;
            open.iter()
                .filter(|(_, q)| {
                    if q.side == side && !dropped {
                        dropped = true;
                        false
                    } else {
                        true
                    }
                })
                .cloned()
                .collect::<Vec<_>>()
        };
        let one_gap = drop_one(Side::Ask, &drop_one(Side::Bid, &all));
        let a1 = w.on_event(
            &ctx(&sm, &s, &p, &one_gap),
            &MarketEvent::BookUpdate {
                snapshot: s.clone(),
            },
        );
        assert!(
            a1.is_empty(),
            "1 round-trip below round_trips=2 must not refill: {a1:?}"
        );
        // Drop 2 bids + 2 asks (2 round-trips) → meets threshold → refill.
        let two_gap = drop_one(
            Side::Ask,
            &drop_one(Side::Ask, &drop_one(Side::Bid, &drop_one(Side::Bid, &all))),
        );
        let a2 = w.on_event(
            &ctx(&sm, &s, &p, &two_gap),
            &MarketEvent::BookUpdate {
                snapshot: s.clone(),
            },
        );
        assert!(
            !a2.is_empty(),
            "2 round-trips at round_trips=2 must refill: {a2:?}"
        );
    }

    fn quotes_as_open(actions: &[Action]) -> Vec<(QuoteId, QuoteIntent)> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) => Some((QuoteId::new(), q.clone())),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn force_refill_fills_vacant_slots_after_timeout_only() {
        // round_trips high so the round-trip trigger can't fire; one vacant slot
        // must NOT refill before force_refill_secs, but MUST after.
        let mut c = cfg();
        c.round_trips = 100;
        c.force_refill_secs = 60;
        let mut w = Wave::new(c);
        let s = snap(Decimal::from(100), Decimal::new(1001, 1));
        let p = pos_flat();
        let sm = sym();
        let seeded = w.on_event(
            &ctx(&sm, &s, &p, &[]),
            &MarketEvent::BookUpdate {
                snapshot: s.clone(),
            },
        );
        // Drop exactly ONE bid (partial drain, below round_trips=100).
        let mut open = quotes_as_open(&seeded);
        let drop_idx = open
            .iter()
            .position(|(_, q)| q.side == Side::Bid)
            .expect("a bid to drop");
        open.remove(drop_idx);

        // 30s later (< 60s): no refill — round_trips not met, valve not open.
        let a30 = w.on_event(
            &ctx_at(&sm, &s, &p, &open, 30 * 1_000_000_000),
            &MarketEvent::BookUpdate {
                snapshot: s.clone(),
            },
        );
        assert!(
            a30.is_empty(),
            "no refill before force_refill_secs elapses: {a30:?}"
        );

        // 61s later (> 60s): the valve opens → the vacant bid is refilled.
        let a61 = w.on_event(
            &ctx_at(&sm, &s, &p, &open, 61 * 1_000_000_000),
            &MarketEvent::BookUpdate {
                snapshot: s.clone(),
            },
        );
        assert!(
            a61.iter()
                .any(|a| matches!(a, Action::Quote(q) if q.side == Side::Bid)),
            "force-refill fills the vacant slot after the timeout: {a61:?}"
        );
    }

    #[test]
    fn relattice_rebuilds_grid_when_bps_drifts_and_self_arrests() {
        // Freeze at mid ~100 (step = 10 bps). Double the market to ~200: the
        // frozen step is now ~5 bps of the new mid → >10% drift → relattice must
        // rebuild the grid at the new touch, cancel the stale orders, and re-seed.
        let mut w = Wave::new(cfg());
        let s0 = snap(Decimal::from(100), Decimal::new(1001, 1));
        let p = pos_flat();
        let sm = sym();
        let seeded = w.on_event(
            &ctx(&sm, &s0, &p, &[]),
            &MarketEvent::BookUpdate {
                snapshot: s0.clone(),
            },
        );
        let open = quotes_as_open(&seeded);
        let seeded_ids: std::collections::HashSet<QuoteId> =
            open.iter().map(|(id, _)| *id).collect();
        let step0 = w.lattice_step.unwrap();

        let s_up = snap(Decimal::from(200), Decimal::new(2001, 1));
        let a = w.on_event(
            &ctx(&sm, &s_up, &p, &open),
            &MarketEvent::BookUpdate {
                snapshot: s_up.clone(),
            },
        );
        let step1 = w.lattice_step.unwrap();
        assert!(
            step1 > step0,
            "relattice must grow the absolute step (held bps at the higher mid): {step0} -> {step1}"
        );
        let cancelled: std::collections::HashSet<QuoteId> = a
            .iter()
            .filter_map(|x| match x {
                Action::Cancel(id) => Some(*id),
                _ => None,
            })
            .collect();
        assert_eq!(
            cancelled, seeded_ids,
            "relattice must cancel every order on the stale grid: {a:?}"
        );
        assert!(
            a.iter()
                .any(|x| matches!(x, Action::Quote(q) if q.side == Side::Bid)),
            "relattice must re-seed the fresh grid: {a:?}"
        );

        // Self-arresting: replay the same book with the freshly seeded orders
        // resting → no further relattice (step stays put, nothing cancelled).
        let open2 = quotes_as_open(&a);
        let a2 = w.on_event(
            &ctx(&sm, &s_up, &p, &open2),
            &MarketEvent::BookUpdate {
                snapshot: s_up.clone(),
            },
        );
        assert_eq!(
            w.lattice_step.unwrap(),
            step1,
            "relattice must not re-fire at the same mid (self-arresting)"
        );
        assert!(
            !a2.iter().any(|x| matches!(x, Action::Cancel(_))),
            "no churn once relatticed and the band is intact: {a2:?}"
        );
    }

    #[test]
    fn relattice_skips_when_tick_limited() {
        // tick=0.1, steps_bps=10 → bps target is one tick at mid 100 and below
        // it for any lower mid. Freeze at mid ~50 (step already pinned to the 0.1
        // tick) and drop to mid ~25: the ideal step stays below one tick, so the
        // install-able step is still 0.1 — no improvement possible → no
        // relattice (grid step stays fixed).
        let mut w = Wave::new(cfg());
        let s0 = snap(Decimal::from(50), Decimal::new(501, 1));
        let p = pos_flat();
        let sm = sym();
        let seeded = w.on_event(
            &ctx(&sm, &s0, &p, &[]),
            &MarketEvent::BookUpdate {
                snapshot: s0.clone(),
            },
        );
        let open = quotes_as_open(&seeded);
        let step0 = w.lattice_step.unwrap();
        assert_eq!(step0, Decimal::new(1, 1), "freeze should pin to the tick");

        let s_dn = snap(Decimal::from(25), Decimal::new(251, 1));
        let _ = w.on_event(
            &ctx(&sm, &s_dn, &p, &open),
            &MarketEvent::BookUpdate {
                snapshot: s_dn.clone(),
            },
        );
        assert_eq!(
            w.lattice_step.unwrap(),
            step0,
            "tick-limited step must not relattice (cannot tighten below tick)"
        );
    }

    /// Feed `secs` one-second candles each spanning `[low, high]` (two ticks per
    /// second forces the high then the low), so the prior candle closes.
    fn feed_candles(w: &mut Wave, high: Decimal, low: Decimal, secs: u64) {
        for s in 0..secs {
            let t = s * CANDLE_NANOS + 1;
            w.update_candles(high, t);
            w.update_candles(low, t);
        }
    }

    #[test]
    fn auto_inner_starts_zero_and_tracks_half_candle_gap() {
        // steps_bps=10. A 20bps avg candle gap → inner target = half = 10bps =
        // 1 step. A 60bps gap → 30bps = 3 steps.
        let mut c = cfg();
        c.auto_inner = true;
        c.steps_bps = 10;
        let mut w = Wave::new(c.clone());
        assert_eq!(w.dyn_inner_steps, 0, "auto-inner must start at 0");
        assert_eq!(w.effective_inner(), 0);

        // 20 bps gap at mid 100 → 100.1 / 99.9.
        feed_candles(&mut w, Decimal::new(1001, 1), Decimal::new(999, 1), 5);
        assert_eq!(
            w.dyn_inner_steps, 1,
            "20bps gap → half=10bps / 10bps = 1 step"
        );

        // 60 bps gap at mid 100 → 100.3 / 99.7. Refill the window with the wider
        // range; the rolling mean climbs toward 60bps → 3 steps.
        let mut w2 = Wave::new(c);
        feed_candles(&mut w2, Decimal::new(1003, 1), Decimal::new(997, 1), 70);
        assert_eq!(
            w2.dyn_inner_steps, 3,
            "60bps gap → half=30bps / 10bps = 3 steps"
        );
    }

    #[test]
    fn auto_inner_noop_when_steps_bps_zero() {
        // No bps-per-step to divide by → inner stays 0 (1-tick lattice).
        let mut c = cfg();
        c.auto_inner = true;
        c.steps_bps = 0;
        let mut w = Wave::new(c);
        feed_candles(&mut w, Decimal::new(1010, 1), Decimal::new(990, 1), 10);
        assert_eq!(w.dyn_inner_steps, 0);
        assert_eq!(w.effective_inner(), 0);
    }

    #[test]
    fn fixed_inner_ignores_candles_when_auto_off() {
        // auto_inner=false → effective_inner is the fixed knob, candle tracking
        // is a no-op.
        let mut c = cfg();
        c.auto_inner = false;
        c.steps_inner = 4;
        c.steps_bps = 10;
        let mut w = Wave::new(c);
        feed_candles(&mut w, Decimal::new(1005, 1), Decimal::new(995, 1), 10);
        assert_eq!(
            w.dyn_inner_steps, 0,
            "auto tracker idle when auto_inner off"
        );
        assert_eq!(
            w.effective_inner(),
            4,
            "fixed knob used when auto_inner off"
        );
    }

    #[test]
    fn auto_inner_caps_at_max() {
        // A huge candle gap must clamp the inner to MAX_AUTO_INNER.
        let mut c = cfg();
        c.auto_inner = true;
        c.steps_bps = 1; // tiny step → bps/step ratio explodes the step count
        let mut w = Wave::new(c);
        // ~2000bps gap at mid 100 → half=1000bps / 1bps = 1000 steps, capped.
        feed_candles(&mut w, Decimal::from(110), Decimal::from(90), 70);
        assert_eq!(w.dyn_inner_steps, MAX_AUTO_INNER);
    }

    #[test]
    fn auto_inner_change_forces_refill() {
        // When the auto-sized inner changes, the lattice must re-place at the new
        // offset that same event (cancel the old band + emit the new), not wait
        // for a round-trip / side-empty.
        let mut c = cfg();
        c.auto_inner = true;
        c.steps_bps = 10;
        c.levels = 6;
        let sm = sym();
        let p = pos_flat();
        let mut w = Wave::new(c);

        // Seed at mid ~100.05 with inner still 0 (no candle has closed yet).
        let s = snap(Decimal::from(100), Decimal::new(1001, 1));
        let seeded = w.on_event(
            &ctx_at(&sm, &s, &p, &[], 1),
            &MarketEvent::BookUpdate {
                snapshot: s.clone(),
            },
        );
        let open = quotes_as_open(&seeded);
        assert!(!open.is_empty(), "seed should place a band");
        assert_eq!(w.effective_inner(), 0);

        // Roll 60bps candles → inner jumps to 3 and marks dirty.
        feed_candles(&mut w, Decimal::new(1003, 1), Decimal::new(997, 1), 5);
        assert_eq!(w.dyn_inner_steps, 3);
        assert!(w.inner_dirty, "inner change must mark the lattice dirty");

        // Same book, same second as the last candle (no new close) → the refill
        // is driven purely by the inner change, not a slide or round-trip.
        let now = 4 * CANDLE_NANOS + 5;
        let acted = w.on_event(
            &ctx_at(&sm, &s, &p, &open, now),
            &MarketEvent::BookUpdate {
                snapshot: s.clone(),
            },
        );
        assert!(!w.inner_dirty, "inner_dirty must be consumed");
        assert!(
            acted.iter().any(|a| matches!(a, Action::Cancel(_))),
            "must prune the old inner=0 band: {acted:?}"
        );
        assert!(
            acted.iter().any(|a| matches!(a, Action::Quote(_))),
            "must re-emit at the new inner=3 offset: {acted:?}"
        );
    }

    #[test]
    fn auto_step_tracks_k_times_candle_gap() {
        // steps_bps=30 ceiling, k=0.5, maker=2bps → floor=4bps. A 40bps avg
        // candle gap → 0.5×40 = 20bps (within [4,30]).
        let mut c = cfg();
        c.auto_step = true;
        c.steps_bps = 30;
        c.auto_step_k = Decimal::new(5, 1); // 0.5
        c.maker_fee_bps = Decimal::from(2);
        let mut w = Wave::new(c);
        assert_eq!(w.dyn_step_bps, 30, "auto-step seeds at the ceiling");
        assert_eq!(w.effective_step_bps(), 30);

        // 40bps gap at mid 100 → 100.2 / 99.8.
        feed_candles(&mut w, Decimal::new(1002, 1), Decimal::new(998, 1), 5);
        assert_eq!(w.dyn_step_bps, 20, "0.5 × 40bps = 20bps");
        assert_eq!(w.effective_step_bps(), 20);
    }

    #[test]
    fn auto_step_floors_at_round_trip_breakeven() {
        // Quiet market (4bps gap) with k=0.5 → raw 2bps, below the 2×maker=4bps
        // floor → clamps up to 4bps. The floor protects against sub-break-even.
        let mut c = cfg();
        c.auto_step = true;
        c.steps_bps = 30;
        c.auto_step_k = Decimal::new(5, 1);
        c.maker_fee_bps = Decimal::from(2); // floor = 4bps
        let mut w = Wave::new(c);
        // 4bps gap at mid 100 → 100.02 / 99.98.
        feed_candles(&mut w, Decimal::new(10002, 2), Decimal::new(9998, 2), 5);
        assert_eq!(w.dyn_step_bps, 4, "raw 2bps floored to 2×maker = 4bps");
    }

    #[test]
    fn auto_step_zero_fee_has_no_floor() {
        // USDC 0-fee promo (maker=0) → floor=0. A near-flat market computes a
        // tiny step (clamped only by the ceiling), no break-even floor.
        let mut c = cfg();
        c.auto_step = true;
        c.steps_bps = 30;
        c.auto_step_k = Decimal::new(5, 1);
        c.maker_fee_bps = Decimal::ZERO; // 0-fee → floor 0
        let mut w = Wave::new(c);
        assert_eq!(w.floor_step_bps(), 0);
        // 6bps gap → 0.5×6 = 3bps; no fee floor lifts it.
        feed_candles(&mut w, Decimal::new(10003, 2), Decimal::new(9997, 2), 5);
        assert_eq!(w.dyn_step_bps, 3, "0-fee: step follows k×gap with no floor");
    }

    #[test]
    fn auto_step_caps_at_configured_ceiling() {
        // A violent market must not blow the step past the configured steps_bps.
        let mut c = cfg();
        c.auto_step = true;
        c.steps_bps = 15; // ceiling
        c.auto_step_k = Decimal::new(5, 1);
        c.maker_fee_bps = Decimal::from(2);
        let mut w = Wave::new(c);
        // ~2000bps gap → 0.5× = 1000bps, capped at the 15bps ceiling.
        feed_candles(&mut w, Decimal::from(110), Decimal::from(90), 5);
        assert_eq!(
            w.dyn_step_bps, 15,
            "auto-step clamps to the steps_bps ceiling"
        );
    }

    #[test]
    fn fixed_step_ignores_candles_when_auto_off() {
        // auto_step=false → effective step is the fixed steps_bps regardless of
        // candle volatility.
        let mut c = cfg();
        c.auto_step = false;
        c.steps_bps = 10;
        c.maker_fee_bps = Decimal::from(2);
        let mut w = Wave::new(c);
        feed_candles(&mut w, Decimal::new(1003, 1), Decimal::new(997, 1), 10);
        assert_eq!(
            w.effective_step_bps(),
            10,
            "fixed steps_bps used when auto_step off"
        );
    }
}
