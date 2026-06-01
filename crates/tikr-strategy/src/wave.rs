//! Wave: fixed-lattice band-refill market-making.
//!
//! A frozen price lattice (origin + step, set once at init). The active
//! `grid_levels`-slot band is a window over that fixed grid; it slides to
//! track the touch but the grid prices never move (no recenter/relattice).
//!
//! ## Behavior
//! 1. **Init (first usable book event):** freeze lattice. Step = `step_bps`
//!    of mid (snapped to tick), else 1 tick. `step_bps` also sets the inner
//!    self-spread, so origins sit `step_bps/2` off mid on each side.
//! 2. **Refill** fires when EITHER a both-sides round-trip completes (bid
//!    AND ask each drained ≥ `refill_threshold` → captured spread) OR one
//!    whole side is empty (re-arm after a one-sided sweep). On refill,
//!    re-emit every empty band slot on each side and prune the tail (resting
//!    orders that fell outside the slid window). Between refills: nothing.
//! 3. **Position cap** (`max_position_usdt`, account-derived via
//!    [`Strategy::on_max_position_updated`]): when over the cap, the adding
//!    side stops emitting while resting orders stay to catch the reversion.
//!
//! Inventory is otherwise bounded by `step_bps` width (wider step = slower
//! one-sided accumulation) and per-order size — run on small-min-notional
//! markets so accumulated fills stay survivable.

use std::collections::{HashSet, VecDeque};

use tikr_core::{Decimal, MarketEvent, Price, QuoteKind, Side, Size, Symbol, TimeInForce};
use tikr_venue::QuoteIntent;

use crate::{Action, Strategy, StrategyContext};

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
    /// Lattice slots per side. Default 12.
    pub grid_levels: u32,

    /// Level spacing in bps of mid — the gap between consecutive lattice
    /// levels. Snapped to tick (min 1 tick). `0` = 1-tick lattice. Required;
    /// also the default for `inner_bps` when that is unset.
    pub step_bps: u32,

    /// Inner dead-zone in STEPS: the first order on each side sits
    /// `inner_steps × step` from mid (where the frozen origins are anchored),
    /// matching Tide's `inner_steps`. e.g. `inner_steps=2, step_bps=5` → first
    /// order 10bps off mid, levels 5bps apart (10,15,20,…). `0` (default) =
    /// origins at the touch (1-tick spread). Snapped to tick.
    pub inner_steps: u32,

    /// Refill batching: only refill a side once ≥ this many of its band
    /// slots are empty (filled). `1` = refill on any single gap (most
    /// reactive). Higher = wait for N fills then refill them together,
    /// cutting re-emit churn. Default `1`.
    pub refill_threshold: u32,

    /// Hard position cap in quote notional. When `|position notional|`
    /// exceeds this, the *adding* side stops emitting (longs → no more
    /// bids, shorts → no more asks) while resting orders stay put to catch
    /// the reversion. Updated live via [`Strategy::on_max_position_updated`]
    /// from the account-derived cap. `0` (default) = uncapped.
    pub max_position_usdt: Decimal,

    /// Chase the reducing side, but only as far as cost basis (Tide semantics).
    /// When long, the ASK band chases DOWN past the origin to follow price but
    /// is floored at `avg_entry + gap` — it sells inventory near cost on a
    /// bounce, never below what was paid. When short, the BID band chases UP
    /// but is ceilinged at `avg_entry − gap`. The accumulating side stays
    /// one-sided/frozen. `gap = max(inner_steps,1) × step`. `false` = off
    /// (pure one-sided lattice).
    pub chase_to_avg: bool,

    /// Market-chase: when `true`, the lattice window follows the touch in BOTH
    /// directions — bids may sit ABOVE the bid origin, asks BELOW the ask
    /// origin — abandoning the one-sided clamp. Mirrors Tide's `chase`. This is
    /// the proven LOSING mode (buys high / sells low into a trend → realized
    /// losses); the one-sided clamp exists to prevent exactly this. `false`
    /// (default) = frozen one-sided. Overrides `chase_to_avg` when set.
    pub chase: bool,

    /// Adaptive lattice: number of 1-minute candle ranges to average for the
    /// volatility estimate. `0`/unset → 10.
    pub candle_count: u32,
    /// Adaptive lattice: re-evaluate + (if the step changed) re-lattice this
    /// often, in seconds. `0` (default) = adaptive OFF (static `step_bps`).
    pub lattice_adjust_secs: u32,
    /// Adaptive lattice: effective step bps = `step_volatility_mult × average
    /// 1-minute candle range (bps)`, floored at `step_bps`. `0` (default) = off.
    pub step_volatility_mult: Decimal,
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
    /// Completed 1-minute candle ranges, in bps ((high−low)/low×1e4), newest at back.
    candles: VecDeque<Decimal>,
    /// In-progress candle: (minute_bucket, high, low).
    cur_candle: Option<(u64, Decimal, Decimal)>,
    /// ctx.now.0 (ns) of the last adaptive re-evaluation.
    last_adjust_ns: Option<u64>,
    /// Current volatility-derived step bps (None until first adjust → uses config.step_bps).
    adaptive_step_bps: Option<u32>,
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
            let needed = (min / price.0 / self.config.step_size).ceil() * self.config.step_size;
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

    /// Compute `(top_bid_override, top_ask_override)`, pushing the origins
    /// apart to honor the inner self-spread (`step_bps` of mid, half each
    /// side). Mirror of tide's logic.
    fn top_overrides(
        &self,
        best_bid: Option<Price>,
        best_ask: Option<Price>,
    ) -> (Option<Price>, Option<Price>) {
        let tick = self.config.tick_size;
        let spread_active = self.effective_step_bps() > 0 || self.config.inner_steps > 0;
        if let (Some(bp), Some(ap)) = (best_bid, best_ask)
            && bp.0 > Decimal::ZERO
            && ap.0 > bp.0
            && tick > Decimal::ZERO
            && spread_active
        {
            let mid = (bp.0 + ap.0) / Decimal::from(2);
            // Distance from mid to the first order on each side =
            // `inner_steps × step` (Tide semantics). `inner_steps=0` → offset 0
            // → origins clamp to the touch via the .min(bp)/.max(ap) below.
            let required_half = Decimal::from(self.config.inner_steps) * self.compute_step(mid);
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

    /// Returns the effective step bps: adaptive when set, else config.step_bps.
    fn effective_step_bps(&self) -> u32 {
        self.adaptive_step_bps.unwrap_or(self.config.step_bps)
    }

    /// Base lattice gap = `effective_step_bps` of mid, snapped up to tick (min 1 tick).
    /// `step_bps = 0` → 1-tick gap. This is the distance from origin to the
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

    /// Cancel resting orders on `side` whose price is outside the band's
    /// price range — the tail left behind as price travels. Holds the
    /// resting-order count to ~`grid_levels` per side.
    ///
    /// BID band `[low_k, high_k]` → price band
    /// `[origin - high_k·step, origin - low_k·step]` (high_k = deeper =
    /// lower price). ASK → `[origin + low_k·step, origin + high_k·step]`.
    fn prune_outside_band(
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
    ///
    /// `force = true` skips the `open_quotes` presence check — used right
    /// after a `CancelAll` (relattice), where `ctx.open_quotes` still
    /// reflects the pre-cancel venue state and would wrongly suppress emits.
    fn emit_window_slots(
        &mut self,
        ctx: &StrategyContext<'_>,
        side: Side,
        window: WindowRange,
        force: bool,
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
            if !force {
                let present = ctx
                    .open_quotes
                    .iter()
                    .any(|(_, q)| q.side == side && q.price.0 == safe_price);
                if present {
                    continue;
                }
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
        Self {
            config,
            bid_lattice_origin: None,
            ask_lattice_origin: None,
            lattice_step: None,
            emitted_this_event_bid: HashSet::new(),
            emitted_this_event_ask: HashSet::new(),
            candles: VecDeque::new(),
            cur_candle: None,
            last_adjust_ns: None,
            adaptive_step_bps: None,
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
        let (top_b, top_a) = self.top_overrides(best_bid, best_ask);
        let tick = self.config.tick_size;

        // 0) Candle tracking + adaptive re-lattice.
        //
        // Compute mid (requires both sides usable).
        if let (Some(b), Some(a)) = (best_bid, best_ask)
            && b.0 > Decimal::ZERO
            && a.0 > b.0
        {
            let mid = (b.0 + a.0) / Decimal::from(2);

            // Track 1-minute candle ranges.
            let minute = ctx.now.0 / 60_000_000_000;
            match &mut self.cur_candle {
                None => self.cur_candle = Some((minute, mid, mid)),
                Some((m, hi, lo)) if minute > *m => {
                    // close the finished candle
                    if *lo > Decimal::ZERO {
                        let range_bps = (*hi - *lo) / *lo * Decimal::from(10_000);
                        self.candles.push_back(range_bps);
                        let keep = self.config.candle_count.max(1) as usize;
                        while self.candles.len() > keep {
                            self.candles.pop_front();
                        }
                    }
                    self.cur_candle = Some((minute, mid, mid));
                }
                Some((_, hi, lo)) => {
                    if mid > *hi {
                        *hi = mid;
                    }
                    if mid < *lo {
                        *lo = mid;
                    }
                }
            }

            // Adaptive re-lattice: only when enabled and candles are available.
            let due = self.last_adjust_ns.is_none_or(|t| {
                ctx.now.0.saturating_sub(t)
                    >= u64::from(self.config.lattice_adjust_secs) * 1_000_000_000
            });
            if self.config.lattice_adjust_secs > 0
                && self.config.step_volatility_mult > Decimal::ZERO
                && due
                && !self.candles.is_empty()
            {
                self.last_adjust_ns = Some(ctx.now.0);
                let sum: Decimal = self.candles.iter().copied().sum();
                let avg = sum / Decimal::from(self.candles.len() as u64);
                // effective step bps = mult × avg candle bps, floored at config.step_bps,
                // clamped to a sane ceiling.
                let raw = (self.config.step_volatility_mult * avg).round();
                let floor = self.config.step_bps.max(1);
                let new_bps = raw
                    .to_string()
                    .parse::<f64>()
                    .ok()
                    .map(|f| (f as i64).clamp(floor as i64, 5_000))
                    .unwrap_or(floor as i64) as u32;
                // Re-lattice ONLY when the rounded step actually changes (no churn in
                // stable vol): cancel all + reset the lattice so the next event
                // re-freezes at the new step around the current mid.
                if Some(new_bps) != self.adaptive_step_bps {
                    self.adaptive_step_bps = Some(new_bps);
                    if self.lattice_step.is_some() {
                        actions.push(Action::CancelAll);
                        self.lattice_step = None;
                        self.bid_lattice_origin = None;
                        self.ask_lattice_origin = None;
                    }
                }
            }
        }

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
                step_bps = self.config.step_bps,
                inner_steps = self.config.inner_steps,
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

        // 2) Round-trip refill on the FIXED lattice.
        //
        // Refill fires ONLY when BOTH sides of the band have drained by
        // ≥ refill_threshold slots since the last refill — i.e. at least
        // one bid AND one ask filled. That pair is a completed round-trip
        // (bought low + sold high), so every refill cycle banks the
        // captured spread. On a one-way trend only one side fills → the
        // both-sides trigger never fires → no refill → inventory is
        // capped at the initial band (no chasing, no runaway).
        //
        // On refill: re-emit every empty slot on both sides at their
        // current-touch band prices, then prune the tail (orders left
        // outside the new band). Between refills: do nothing.
        let levels = self.config.grid_levels.max(1) as i64;

        // chase_to_avg: gap from cost basis = max(inner_steps,1) × step. The
        // reducing side may chase past the origin (top_k < 0) but no further
        // than this gap from avg_entry, so it never realizes a loss.
        let pos_size = ctx.position.size.0;
        let avg_entry = ctx.position.avg_entry.0;
        let chase_gap = self
            .lattice_step
            .map(|s| Decimal::from(self.config.inner_steps.max(1)) * s)
            .unwrap_or(Decimal::ZERO);

        // Compute both bands around the cross-guarded touch.
        let bid_band = top_b.filter(|t| t.0 > Decimal::ZERO).and_then(|top| {
            let mut cap = top.0;
            if let Some(ap) = best_ask
                && ap.0 > Decimal::ZERO
                && tick > Decimal::ZERO
            {
                cap = cap.min(ap.0 - tick);
            }
            self.bid_k_at_or_below(cap).map(|top_k| {
                // One-sided: bids never above bid_origin (k >= 0). When price
                // rises past the origin, top_k goes <= 0 — clamp to 0 so the
                // shallowest bid sits AT the origin, never above it. Without
                // this the band chases up and buys high, breaking the fixed-grid
                // invariant (avg buy < avg sell) and bleeding realized on trends.
                //
                // chase_to_avg + SHORT: bids are the reducing side. Let them
                // chase UP (k < 0) to cover, but floor the k at the avg_entry −
                // gap slot so they never buy back above what was shorted.
                let floor_k = if self.config.chase_to_avg
                    && pos_size < Decimal::ZERO
                    && avg_entry > Decimal::ZERO
                {
                    self.bid_k_at_or_below(avg_entry - chase_gap).unwrap_or(0)
                } else {
                    0
                };
                // `chase`: market-chase — follow the touch unclamped (bids may
                // sit ABOVE the origin), abandoning the one-sided invariant. The
                // losing mode (buys high on trends); off by default.
                let top_k = if self.config.chase {
                    top_k
                } else {
                    top_k.max(floor_k)
                };
                WindowRange {
                    low_k: top_k,
                    high_k: top_k + levels - 1,
                }
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
            self.ask_k_at_or_above(cap).map(|top_k| {
                // One-sided mirror: asks never below ask_origin (k >= 0). When
                // price falls past the origin, top_k goes <= 0 — clamp to 0 so
                // the shallowest ask sits AT the origin, never below it (no
                // chasing down / selling low).
                //
                // chase_to_avg + LONG: asks are the reducing side. Let them
                // chase DOWN (k < 0) to follow price, but floor the k at the
                // avg_entry + gap slot so they never sell inventory below cost.
                let floor_k = if self.config.chase_to_avg
                    && pos_size > Decimal::ZERO
                    && avg_entry > Decimal::ZERO
                {
                    self.ask_k_at_or_above(avg_entry + chase_gap).unwrap_or(0)
                } else {
                    0
                };
                // `chase`: market-chase mirror — asks may sit BELOW the origin
                // (sells low on trends). Off by default.
                let top_k = if self.config.chase {
                    top_k
                } else {
                    top_k.max(floor_k)
                };
                WindowRange {
                    low_k: top_k,
                    high_k: top_k + levels - 1,
                }
            })
        });

        if let (Some(bb), Some(ab)) = (bid_band, ask_band) {
            let bid_drained = self.band_missing(ctx, Side::Bid, bb);
            let ask_drained = self.band_missing(ctx, Side::Ask, ab);
            let thr = self.config.refill_threshold.max(1);
            let full = self.config.grid_levels.max(1);
            // Refill when a round-trip completed (both sides drained ≥
            // threshold = a bid AND an ask filled → captured spread), OR
            // when one whole side is empty — re-arming the grid after a
            // one-sided sweep instead of going dormant. The side-empty
            // re-arm is integral to keeping the bot live through one-way
            // moves, not an option.
            let round_trip = bid_drained >= thr && ask_drained >= thr;
            let side_empty = bid_drained >= full || ask_drained >= full;
            if round_trip || side_empty {
                let mid = match (best_bid, best_ask) {
                    (Some(b), Some(a)) if a.0 > b.0 => (b.0 + a.0) / Decimal::from(2),
                    _ => Decimal::ZERO,
                };
                // Hard position cap: when over the cap, suppress the side
                // that would add to inventory (longs → no bids, shorts → no
                // asks). Resting orders stay put to catch the reversion.
                // Value the bag at COST BASIS (avg_entry), not mark, so the cap
                // bounds capital deployed — a losing bag marked down must not
                // release the cap and let the adding side over-accumulate.
                let pos = ctx.position.size.0;
                let cap = self.config.max_position_usdt;
                let cap_price = if avg_entry > Decimal::ZERO {
                    avg_entry
                } else {
                    mid
                };
                let pos_notional = pos * cap_price;
                let suppress_bids = cap > Decimal::ZERO && pos_notional > cap;
                let suppress_asks = cap > Decimal::ZERO && pos_notional < -cap;
                if !suppress_bids {
                    self.emit_window_slots(ctx, Side::Bid, bb, false, &mut actions);
                }
                if !suppress_asks {
                    self.emit_window_slots(ctx, Side::Ask, ab, false, &mut actions);
                }
                self.prune_outside_band(ctx, Side::Bid, bb, &mut actions);
                self.prune_outside_band(ctx, Side::Ask, ab, &mut actions);
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

    fn on_max_position_updated(
        &mut self,
        _ctx: &StrategyContext<'_>,
        max_position_usdt: Decimal,
    ) -> Vec<Action> {
        self.config.max_position_usdt = max_position_usdt.max(Decimal::ZERO);
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
            grid_levels: 6,
            step_bps: 10,
            inner_steps: 0,
            refill_threshold: 1,
            max_position_usdt: Decimal::ZERO,
            chase_to_avg: false,
            chase: false,
            candle_count: 10,
            lattice_adjust_secs: 0,
            step_volatility_mult: Decimal::ZERO,
        }
    }

    /// Build a StrategyContext with a configurable timestamp.
    fn make_ctx<'a>(
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
        StrategyContext {
            symbol,
            now: Timestamp(1),
            position: p,
            recent_fills: &[],
            latest_book: s,
            open_quotes: open,
            recent_liqs: &[],
        }
    }

    #[test]
    fn inner_steps_pushes_first_order_out_in_step_multiples() {
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
        // inner_steps=2 with step_bps=10: first order ~2 steps (20bps) off mid.
        let mut wide = cfg();
        wide.tick_size = Decimal::new(1, 2);
        wide.step_bps = 10;
        wide.inner_steps = 2;
        let (inner_wide, step_wide) = freeze(wide);
        // inner_steps=1: first order ~1 step (10bps) off mid.
        let mut narrow = cfg();
        narrow.tick_size = Decimal::new(1, 2);
        narrow.step_bps = 10;
        narrow.inner_steps = 1;
        let (inner_narrow, step_narrow) = freeze(narrow);
        // More inner_steps ⇒ first order FARTHER from mid (Tide semantics).
        assert!(
            inner_wide > inner_narrow,
            "inner_steps=2 ({inner_wide}) must push out farther than inner_steps=1 ({inner_narrow})"
        );
        // Step spacing is unchanged by inner_steps (both == step_bps geometry).
        assert_eq!(
            step_wide, step_narrow,
            "step spacing independent of inner_steps"
        );
    }

    #[test]
    fn uniform_lattice_has_equal_gaps() {
        let mut w = Wave::new(cfg()); // step_increment_bps defaults 0
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
    fn adaptive_off_by_default() {
        // lattice_adjust_secs=0 → adaptive disabled. Feed events across multiple
        // minutes and verify no CancelAll is ever emitted and adaptive_step_bps
        // stays None.
        let mut c = cfg();
        c.lattice_adjust_secs = 0;
        c.step_volatility_mult = Decimal::from(2);
        c.candle_count = 3;
        let mut w = Wave::new(c);
        let sm = sym();
        let p = pos_flat();

        // Simulate 5 minutes of events, advancing by 1 minute each time.
        let minute_ns = 60_000_000_000u64;
        for i in 0..5u64 {
            // Swing mid slightly each minute to create candle ranges
            let bid = if i % 2 == 0 {
                Decimal::from(100)
            } else {
                Decimal::from(102)
            };
            let ask = bid + Decimal::new(1, 1);
            let s_i = snap(bid, ask);
            let c_i = make_ctx(&sm, &s_i, &p, &[], i * minute_ns + 30_000_000_000);
            let actions = w.on_event(
                &c_i,
                &MarketEvent::BookUpdate {
                    snapshot: s_i.clone(),
                },
            );
            // No CancelAll should ever fire
            assert!(
                !actions.iter().any(|a| matches!(a, Action::CancelAll)),
                "adaptive off: no CancelAll at minute {i}"
            );
        }
        assert_eq!(
            w.adaptive_step_bps, None,
            "adaptive_step_bps must stay None when lattice_adjust_secs=0"
        );
        // Lattice must be frozen at some point (first event seeds it)
        assert!(
            w.lattice_step.is_some(),
            "lattice must freeze on first usable event"
        );
        // Lattice step must be derived from static step_bps, not adaptive.
        // The step is frozen from the first event (bid=100, ask=100.1, mid=100.05).
        // compute_step(100.05): target = 100.05 * 10 / 10000 = 0.1005
        // 0.1005 > 0.1 → ceil(0.1005/0.1)*0.1 = ceil(1.005)*0.1 = 2*0.1 = 0.2
        let frozen = w.lattice_step.unwrap();
        assert_eq!(
            frozen,
            Decimal::new(2, 1),
            "static step_bps=10 at mid=100.05 should yield 0.2 (tick-snapped)"
        );
    }

    #[test]
    fn adaptive_resizes_step_on_volatility() {
        // step_bps=10 (floor), step_volatility_mult=1.0, lattice_adjust_secs=60,
        // candle_count=1. Feed a 1-minute candle with a KNOWN range of ≈ 200 bps
        // (hi=101, lo=99, range_bps=(101-99)/99×10000≈202). After feeding that
        // candle and waiting ≥60s, verify:
        //  - a CancelAll is emitted (re-lattice fired)
        //  - adaptive_step_bps > 100 (well above the 10 bps floor)
        //  - lattice_step re-frozen at the new (larger) step in the SAME event
        //  - some Quote actions follow in the same Vec (re-seed)
        let mut c = cfg();
        c.step_bps = 10; // floor
        c.step_volatility_mult = Decimal::ONE;
        c.lattice_adjust_secs = 60;
        c.candle_count = 1; // keep only 1 candle → avg = that candle's range
        c.tick_size = Decimal::new(1, 1); // 0.1
        let mut w = Wave::new(c);
        let sm = sym();
        let p = pos_flat();

        let minute_ns = 60_000_000_000u64;
        // t=0 in minute 0: open candle at hi=101, lo=99 (mid≈100)
        // Use wide bid/ask so mid swings across 99 and 101 within minute 0.
        let s_hi = snap(Decimal::new(10095, 2), Decimal::new(10105, 2)); // mid=101
        let c_first = make_ctx(&sm, &s_hi, &p, &[], 10_000_000_000u64); // t=10s, min=0
        let a_first = w.on_event(
            &c_first,
            &MarketEvent::BookUpdate {
                snapshot: s_hi.clone(),
            },
        );
        // First event seeds the lattice.
        assert!(!a_first.is_empty(), "first event must seed the lattice");
        let initial_step = w.lattice_step.unwrap();

        // Still in minute 0: lo tick sets lo=99
        let s_lo = snap(Decimal::new(9895, 2), Decimal::new(9905, 2)); // mid=99
        let c_lo = make_ctx(&sm, &s_lo, &p, &[], 30_000_000_000u64); // t=30s, min=0
        let _ = w.on_event(
            &c_lo,
            &MarketEvent::BookUpdate {
                snapshot: s_lo.clone(),
            },
        );

        // Advance to minute 1: closes minute-0 candle (hi=101, lo=99 → ≈202 bps)
        // last_adjust_ns=None → due=true. candles=[≈202] (non-empty). Fires adjust!
        // new_bps = round(1.0 × 202) = 202. floor=10. 202 != None → CancelAll + re-seed.
        let s_mid = snap(Decimal::new(9995, 2), Decimal::new(10005, 2)); // mid=100
        let c_adj = make_ctx(&sm, &s_mid, &p, &[], minute_ns + 5_000_000_000u64); // t=65s, min=1
        let actions = w.on_event(
            &c_adj,
            &MarketEvent::BookUpdate {
                snapshot: s_mid.clone(),
            },
        );

        // CancelAll must be emitted
        assert!(
            actions.iter().any(|a| matches!(a, Action::CancelAll)),
            "adaptive: CancelAll must be emitted on first volatility resize. actions={actions:?}"
        );
        // adaptive_step_bps must be set and >> 10
        let abps = w
            .adaptive_step_bps
            .expect("adaptive_step_bps must be Some after adjust");
        assert!(
            abps > 10,
            "adaptive step bps {abps} must be well above floor 10"
        );
        assert!(
            abps > 100,
            "adaptive step bps {abps} should be ~200 for ≈202bps candle"
        );

        // lattice_step must be re-frozen at the new step (larger than initial)
        let new_step = w
            .lattice_step
            .expect("lattice must re-freeze in same event");
        assert!(
            new_step > initial_step,
            "new lattice step {new_step} must be larger than initial {initial_step}"
        );
        // Some Quote actions must also be present (the re-seed)
        assert!(
            actions.iter().any(|a| matches!(a, Action::Quote(_))),
            "re-seed quotes must follow CancelAll in same event: {actions:?}"
        );
    }

    #[test]
    fn adaptive_no_churn_when_stable() {
        // Once the first adjust sets adaptive_step_bps, subsequent adjusts with
        // the SAME rounded step must emit NO further CancelAll.
        // Use candle_count=1 so the rolling avg always equals the single latest
        // candle — stable candle range → stable rounded bps → no churn.
        let mut c = cfg();
        c.step_bps = 10;
        c.step_volatility_mult = Decimal::ONE;
        c.lattice_adjust_secs = 60;
        c.candle_count = 1; // only 1 candle in the buffer → avg = that candle
        c.tick_size = Decimal::new(1, 1);
        let mut w = Wave::new(c);
        let sm = sym();
        let p = pos_flat();

        let minute_ns = 60_000_000_000u64;

        // Minute 0: open candle with hi=101, lo=99
        let s_hi = snap(Decimal::new(10095, 2), Decimal::new(10105, 2));
        let c0_hi = make_ctx(&sm, &s_hi, &p, &[], 10_000_000_000u64);
        let _ = w.on_event(
            &c0_hi,
            &MarketEvent::BookUpdate {
                snapshot: s_hi.clone(),
            },
        );
        let s_lo = snap(Decimal::new(9895, 2), Decimal::new(9905, 2));
        let c0_lo = make_ctx(&sm, &s_lo, &p, &[], 30_000_000_000u64);
        let _ = w.on_event(
            &c0_lo,
            &MarketEvent::BookUpdate {
                snapshot: s_lo.clone(),
            },
        );

        // Minute 1: closes min-0 candle (≈202bps), first adjust fires → CancelAll.
        // We enter minute 1 via a hi tick so minute 1's candle starts at hi=101.
        let s1_hi = snap(Decimal::new(10095, 2), Decimal::new(10105, 2));
        let c1_hi = make_ctx(&sm, &s1_hi, &p, &[], minute_ns + 5_000_000_000u64);
        let first_actions = w.on_event(
            &c1_hi,
            &MarketEvent::BookUpdate {
                snapshot: s1_hi.clone(),
            },
        );
        assert!(
            first_actions.iter().any(|a| matches!(a, Action::CancelAll)),
            "first adjust must fire a CancelAll"
        );
        let first_abps = w.adaptive_step_bps.unwrap();

        // Still minute 1: add a lo tick so the candle has hi=101, lo=99 → ≈202bps.
        let s1_lo = snap(Decimal::new(9895, 2), Decimal::new(9905, 2));
        let c1_lo = make_ctx(&sm, &s1_lo, &p, &[], minute_ns + 30_000_000_000u64);
        let _ = w.on_event(
            &c1_lo,
            &MarketEvent::BookUpdate {
                snapshot: s1_lo.clone(),
            },
        );

        // Now feed identical-range candles in subsequent minutes (same hi/lo →
        // same range → same rounded bps). Each "stable" minute: hi tick + lo tick
        // WITHIN the minute, then a close event in the NEXT minute (which records
        // the previous candle ≈202 bps). The adaptive check fires on the close
        // event (>60s from last_adjust_ns) and sees candle=[202] → new_bps=202 =
        // first_abps → guard `Some(202) != Some(202)` is FALSE → no CancelAll.
        let mut cancel_count = 0usize;
        for i in 2u64..=7 {
            // Hi tick within minute i
            let s_h = snap(Decimal::new(10095, 2), Decimal::new(10105, 2));
            let c_h = make_ctx(&sm, &s_h, &p, &[], i * minute_ns + 10_000_000_000u64);
            let a = w.on_event(
                &c_h,
                &MarketEvent::BookUpdate {
                    snapshot: s_h.clone(),
                },
            );
            cancel_count += a.iter().filter(|x| matches!(x, Action::CancelAll)).count();

            // Lo tick within minute i (same minute bucket)
            let s_l = snap(Decimal::new(9895, 2), Decimal::new(9905, 2));
            let c_l = make_ctx(&sm, &s_l, &p, &[], i * minute_ns + 30_000_000_000u64);
            let b = w.on_event(
                &c_l,
                &MarketEvent::BookUpdate {
                    snapshot: s_l.clone(),
                },
            );
            cancel_count += b.iter().filter(|x| matches!(x, Action::CancelAll)).count();

            // Close event: enter minute i+1 (records minute i's candle: ≈202bps)
            // Also >60s from last adjust_ns → adjust fires with candle=[202] →
            // new_bps=202 = first_abps → guard holds → no CancelAll.
            let s_c = snap(Decimal::new(9995, 2), Decimal::new(10005, 2));
            let c_c = make_ctx(&sm, &s_c, &p, &[], (i + 1) * minute_ns + 5_000_000_000u64);
            let d = w.on_event(
                &c_c,
                &MarketEvent::BookUpdate {
                    snapshot: s_c.clone(),
                },
            );
            cancel_count += d.iter().filter(|x| matches!(x, Action::CancelAll)).count();
        }
        assert_eq!(
            cancel_count, 0,
            "no CancelAll after step stabilises (same rounded bps {first_abps})"
        );
        // adaptive_step_bps must be unchanged
        assert_eq!(
            w.adaptive_step_bps,
            Some(first_abps),
            "adaptive_step_bps must not change when candle range is stable"
        );
    }
}
