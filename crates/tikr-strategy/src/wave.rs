//! Wave: lazy-recenter lattice market-making.
//!
//! Same frozen-lattice idea as [`crate::tide::Tide`] but the placement
//! WINDOW only moves when the inner section of the lattice gets drained
//! by fills. Between trigger events the strategy does NOTHING — no
//! cancels, no refills, no re-emits. This preserves venue queue
//! priority for every order that survives a recenter event.
//!
//! ## Behavior
//! 1. **Init (first usable book event):** freeze lattice (origin + step)
//!    using the same `*_ticks` / `*_bps` knobs as Tide.
//! 2. **Seed:** place the initial active window — `grid_levels` slots
//!    per side around the current mid.
//! 3. **Monitor:** each event, count how many slots in the current
//!    active window are missing from `open_quotes` (= filled). When
//!    `max(missing_bids, missing_asks) >= recenter_drain_slots`,
//!    trigger a recenter.
//! 4. **Recenter:** compute new center from current mid. Apply mild
//!    inventory skew (more asks if long, more bids if short, capped
//!    at `skew_max_pct`). Emit any slot in the new window that does
//!    not already exist in `open_quotes`. Update tracked window.
//!    **Never cancel** — old orders outside the new window stay
//!    resting and will fill if price reverts.

use std::collections::{HashSet, VecDeque};

use tikr_core::{
    Decimal, MarketEvent, Price, QuoteKind, Side, Size, Symbol, TimeInForce, Timestamp,
};
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

    /// Minimum self-spread in bps (small-tick markets). `0` = disabled.
    pub min_self_spread_bps: u32,
    /// Tick override for min_self_spread. `> 0` wins over bps.
    pub min_self_spread_ticks: u32,
    /// Grid step in bps (small-tick markets). `0` = 1-tick lattice.
    pub grid_step_bps: u32,
    /// Tick override for grid_step. `> 0` wins over bps.
    pub grid_step_ticks: u32,

    /// Recenter when ≥ this many slots are missing from the active
    /// window on either side. `0` = `grid_levels / 3` (auto).
    pub recenter_drain_slots: u32,
    /// Max ±skew on recenter as fraction of `grid_levels`. `0.25` =
    /// long-100% → bid_count = grid_levels × 0.75, ask_count =
    /// grid_levels × 1.25 (clamped to [1, grid_levels]).
    pub skew_max_pct: Decimal,
    /// Per-bot peak position cap in quote notional. Used as the
    /// denominator for inventory_pct = position / cap. `0` =
    /// inventory skew disabled (always symmetric).
    pub max_position_usdt: Decimal,

    // ----- adaptive step sizing -----
    /// Bar interval (seconds) for ATR aggregation. Default 60 = 1m.
    pub bar_interval_secs: u64,
    /// Max closed bars retained for ATR.
    pub max_bars: usize,
    /// ATR lookback. Default 14.
    pub atr_period: u32,
    /// When `> 0`, lattice step at init / relattice = `ATR × mult`
    /// snapped to tick. Replaces the bps/ticks knobs. `0` (default)
    /// = use bps/ticks as configured.
    pub step_atr_mult: Decimal,
    /// Wait this many closed bars before seeding the first lattice
    /// when `step_atr_mult > 0`. Default = `atr_period`. Ignored when
    /// auto-step is disabled.
    pub bar_warmup_bars: u32,
    /// Recompute lattice step from current ATR every Nth recenter
    /// event. `0` = never (lattice frozen at init). Default `10`
    /// (typical: relattice ~hourly if recenters are ~6/hour).
    pub relattice_every_n_recenters: u32,
    /// Minimum wall-clock gap (ms) between recenter events. Guards
    /// against the runaway loop where a tight grid + low drain trigger
    /// recenters every event → re-emit cascade → inventory explosion.
    /// Default `1000`. `0` = no cooldown (unsafe with tight params).
    pub recenter_cooldown_ms: u64,
    /// When `true` (default), cancel any resting order that falls
    /// OUTSIDE the active window after a recenter. Bounds inventory to
    /// ~grid_levels per side — without this the window-shift trail
    /// accumulates unbounded as price travels. Trade-off: loses queue
    /// priority + reversion-catch on the cancelled far orders.
    pub prune_trail: bool,
}

/// Closed OHLCV bar.
#[derive(Debug, Clone, Copy)]
struct Bar {
    #[allow(dead_code)]
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    #[allow(dead_code)]
    volume: Decimal,
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
    lattice_step: Option<Decimal>,
    /// Active window per side. k indices: BID slot price =
    /// `bid_origin - k × step`; ASK slot price = `ask_origin + k × step`.
    bid_window: Option<WindowRange>,
    ask_window: Option<WindowRange>,
    /// Per-event dedupe (in case Quote action sequence has duplicates).
    emitted_this_event_bid: HashSet<i64>,
    emitted_this_event_ask: HashSet<i64>,
    // ----- adaptive step state -----
    /// Bar buffer for ATR computation.
    closed_bars: VecDeque<Bar>,
    open_bar: Option<Bar>,
    current_bucket: Option<u64>,
    /// Recenter events since last relattice. Used with
    /// `relattice_every_n_recenters` to decide when to recompute step.
    recenters_since_relattice: u32,
    /// Timestamp (ns) of the last recenter, for cooldown gating.
    last_recenter_ts: Option<u64>,
}

impl Wave {
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

    /// Compute `(top_bid_override, top_ask_override)` honoring
    /// `min_self_spread_{ticks,bps}` — mirror of tide's logic.
    fn top_overrides(
        &self,
        best_bid: Option<Price>,
        best_ask: Option<Price>,
    ) -> (Option<Price>, Option<Price>) {
        let tick = self.config.tick_size;
        let spread_active =
            self.config.min_self_spread_ticks > 0 || self.config.min_self_spread_bps > 0;
        if let (Some(bp), Some(ap)) = (best_bid, best_ask)
            && bp.0 > Decimal::ZERO
            && ap.0 > bp.0
            && tick > Decimal::ZERO
            && spread_active
        {
            let mid = (bp.0 + ap.0) / Decimal::from(2);
            let required_half = if self.config.min_self_spread_ticks > 0 {
                Decimal::from(self.config.min_self_spread_ticks) * tick / Decimal::from(2)
            } else {
                mid * Decimal::from(self.config.min_self_spread_bps) / Decimal::from(20_000)
            };
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

    /// Compute lattice step. Priority order:
    /// 1. ATR mode (`step_atr_mult > 0`): step = `ATR × mult` snapped to tick.
    ///    Returns `None` if buffer warming.
    /// 2. Tick mode (`grid_step_ticks > 0`).
    /// 3. Bps mode (`grid_step_bps > 0`).
    /// 4. Default: 1 tick.
    fn compute_step(&self, mid: Decimal) -> Option<Decimal> {
        let tick = self.config.tick_size;
        if self.config.step_atr_mult > Decimal::ZERO {
            let atr = self.atr()?;
            if atr <= Decimal::ZERO || tick <= Decimal::ZERO {
                return None;
            }
            let target = atr * self.config.step_atr_mult;
            let snapped = if target > tick {
                (target / tick).ceil() * tick
            } else {
                tick
            };
            return Some(snapped);
        }
        if self.config.grid_step_ticks > 0 {
            return Some(Decimal::from(self.config.grid_step_ticks) * tick);
        }
        if self.config.grid_step_bps > 0 && mid > Decimal::ZERO && tick > Decimal::ZERO {
            let target = mid * Decimal::from(self.config.grid_step_bps) / Decimal::from(10_000);
            return Some(if target > tick {
                (target / tick).ceil() * tick
            } else {
                tick
            });
        }
        Some(tick)
    }

    // ----- bar aggregation -----

    fn bucket_of(&self, ts: Timestamp) -> u64 {
        let interval_ns = self.config.bar_interval_secs.saturating_mul(1_000_000_000);
        ts.0.checked_div(interval_ns).unwrap_or(0)
    }

    /// Roll the open bar if the timestamp bucket changed.
    fn maybe_roll_bar(&mut self, ts: Timestamp, price: Decimal) {
        let bucket = self.bucket_of(ts);
        match self.current_bucket {
            None => {
                self.current_bucket = Some(bucket);
                self.open_bar = Some(Bar {
                    open: price,
                    high: price,
                    low: price,
                    close: price,
                    volume: Decimal::ZERO,
                });
            }
            Some(b) if b == bucket => {}
            Some(_) => {
                if let Some(bar) = self.open_bar.take() {
                    self.closed_bars.push_back(bar);
                    while self.closed_bars.len() > self.config.max_bars {
                        self.closed_bars.pop_front();
                    }
                }
                self.current_bucket = Some(bucket);
                self.open_bar = Some(Bar {
                    open: price,
                    high: price,
                    low: price,
                    close: price,
                    volume: Decimal::ZERO,
                });
            }
        }
    }

    fn update_open_bar_price(&mut self, price: Decimal) {
        if let Some(bar) = self.open_bar.as_mut() {
            bar.close = price;
            if price > bar.high {
                bar.high = price;
            }
            if price < bar.low {
                bar.low = price;
            }
        }
    }

    fn add_trade_volume(&mut self, size: Decimal) {
        if let Some(bar) = self.open_bar.as_mut() {
            bar.volume += size;
        }
    }

    /// Simple ATR over `atr_period` closed bars. None until enough data.
    fn atr(&self) -> Option<Decimal> {
        let n = self.config.atr_period as usize;
        if self.closed_bars.len() < n + 1 {
            return None;
        }
        let bars: Vec<&Bar> = self.closed_bars.iter().rev().take(n + 1).collect();
        let mut tr_sum = Decimal::ZERO;
        for w in bars.windows(2) {
            let h_l = w[0].high - w[0].low;
            let h_pc = (w[0].high - w[1].close).abs();
            let l_pc = (w[0].low - w[1].close).abs();
            let tr = h_l.max(h_pc).max(l_pc);
            tr_sum += tr;
        }
        Some(tr_sum / Decimal::from(n as u64))
    }

    /// `true` when auto-step is enabled AND the bar buffer hasn't
    /// reached the warmup threshold yet.
    fn warming_up(&self) -> bool {
        if self.config.step_atr_mult <= Decimal::ZERO {
            return false;
        }
        let need = self.config.bar_warmup_bars.max(self.config.atr_period) as usize;
        self.closed_bars.len() < need
    }

    /// BID slot price at index k (k=0 is the top, increases descending).
    fn bid_price(&self, k: i64) -> Option<Decimal> {
        let origin = self.bid_lattice_origin?;
        let step = self.lattice_step?;
        let p = origin - Decimal::from(k) * step;
        if p > Decimal::ZERO { Some(p) } else { None }
    }

    /// ASK slot price at index k (k=0 is the top, increases ascending).
    fn ask_price(&self, k: i64) -> Option<Decimal> {
        let origin = self.ask_lattice_origin?;
        let step = self.lattice_step?;
        let p = origin + Decimal::from(k) * step;
        if p > Decimal::ZERO { Some(p) } else { None }
    }

    /// k index of the BID slot at or below the given price.
    fn bid_k_at_or_below(&self, price: Decimal) -> Option<i64> {
        let origin = self.bid_lattice_origin?;
        let step = self.lattice_step?;
        if step <= Decimal::ZERO {
            return None;
        }
        // k = ceil((origin - price) / step). If price >= origin then k <= 0.
        let raw = (origin - price) / step;
        let k = raw.ceil();
        k.to_string().parse::<i64>().ok()
    }

    /// k index of the ASK slot at or above the given price.
    fn ask_k_at_or_above(&self, price: Decimal) -> Option<i64> {
        let origin = self.ask_lattice_origin?;
        let step = self.lattice_step?;
        if step <= Decimal::ZERO {
            return None;
        }
        let raw = (price - origin) / step;
        let k = raw.ceil();
        k.to_string().parse::<i64>().ok()
    }

    /// Configured (or auto) drain trigger threshold.
    fn recenter_threshold(&self) -> u32 {
        if self.config.recenter_drain_slots > 0 {
            self.config.recenter_drain_slots
        } else {
            (self.config.grid_levels / 3).max(1)
        }
    }

    /// Count slots in `window` whose price has no matching open quote
    /// on `side` in `ctx.open_quotes`.
    fn count_missing(&self, ctx: &StrategyContext<'_>, side: Side, window: WindowRange) -> u32 {
        let mut missing = 0u32;
        for k in window.low_k..=window.high_k {
            let price_opt = match side {
                Side::Bid => self.bid_price(k),
                Side::Ask => self.ask_price(k),
            };
            let Some(p) = price_opt else { continue };
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

    /// Compute (bid_count, ask_count) for a recenter.
    ///
    /// When `max_position_usdt > 0`:
    /// - **Hard cap:** once `|position notional| ≥ cap`, the
    ///   accumulating side is suppressed entirely (bid_count = 0 when
    ///   long-over-cap, ask_count = 0 when short-over-cap). Bounds
    ///   inventory.
    /// - **Mild skew below the cap:** long → fewer bids, more asks
    ///   (drain bias), scaled by `skew_max_pct`.
    ///
    /// When `max_position_usdt == 0`: symmetric, no cap, no skew.
    fn skewed_counts(&self, pos_notional: Decimal) -> (u32, u32) {
        let levels = self.config.grid_levels.max(1);
        let cap = self.config.max_position_usdt;
        if cap <= Decimal::ZERO {
            return (levels, levels);
        }
        // Hard cap: suppress the side that would grow the position past cap.
        if pos_notional >= cap {
            return (0, levels); // long-capped: asks only (drain)
        }
        if pos_notional <= -cap {
            return (levels, 0); // short-capped: bids only (drain)
        }
        if self.config.skew_max_pct <= Decimal::ZERO {
            return (levels, levels);
        }
        let raw_pct = pos_notional / cap;
        let clamped = raw_pct.max(Decimal::from(-1)).min(Decimal::ONE);
        let skew = clamped * self.config.skew_max_pct;
        let levels_dec = Decimal::from(levels);
        let bid_dec = (levels_dec * (Decimal::ONE - skew)).round();
        let ask_dec = (levels_dec * (Decimal::ONE + skew)).round();
        let to_u32 = |d: Decimal| -> u32 {
            let n = d.to_string().parse::<f64>().unwrap_or(0.0) as i64;
            n.clamp(0, levels as i64) as u32
        };
        (to_u32(bid_dec), to_u32(ask_dec))
    }

    /// Cancel any open order on `side` whose price lies OUTSIDE the
    /// active window's price band. Bounds the window-shift trail.
    ///
    /// BID window `[low_k, high_k]` → price band
    /// `[origin - high_k·step, origin - low_k·step]` (high_k = deeper =
    /// lower price). ASK → `[origin + low_k·step, origin + high_k·step]`.
    fn prune_outside_window(
        &self,
        ctx: &StrategyContext<'_>,
        side: Side,
        window: WindowRange,
        actions: &mut Vec<Action>,
    ) {
        let (lo, hi) = match side {
            Side::Bid => {
                let Some(deep) = self.bid_price(window.high_k) else {
                    return;
                };
                let Some(shallow) = self.bid_price(window.low_k) else {
                    return;
                };
                (deep, shallow)
            }
            Side::Ask => {
                let Some(shallow) = self.ask_price(window.low_k) else {
                    return;
                };
                let Some(deep) = self.ask_price(window.high_k) else {
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
            bid_window: None,
            ask_window: None,
            emitted_this_event_bid: HashSet::new(),
            emitted_this_event_ask: HashSet::new(),
            closed_bars: VecDeque::new(),
            open_bar: None,
            current_bucket: None,
            recenters_since_relattice: 0,
            last_recenter_ts: None,
        }
    }

    fn name(&self) -> &str {
        "wave"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        self.emitted_this_event_bid.clear();
        self.emitted_this_event_ask.clear();
        let mut actions: Vec<Action> = Vec::new();

        let best_bid = ctx.latest_book.bids.first().map(|l| l.price);
        let best_ask = ctx.latest_book.asks.first().map(|l| l.price);
        let (top_b, top_a) = self.top_overrides(best_bid, best_ask);
        let tick = self.config.tick_size;

        // 0) Drive bar aggregation (for ATR if enabled).
        let bar_price = match event {
            MarketEvent::BookUpdate { .. } => match (best_bid, best_ask) {
                (Some(b), Some(a)) if a.0 > b.0 => Some((b.0 + a.0) / Decimal::from(2)),
                _ => None,
            },
            MarketEvent::Trade { price, .. } => Some(price.0),
            _ => None,
        };
        if let Some(p) = bar_price
            && p > Decimal::ZERO
        {
            self.maybe_roll_bar(ctx.now, p);
            self.update_open_bar_price(p);
        }
        if let MarketEvent::Trade { size, .. } = event {
            self.add_trade_volume(size.0);
        }

        // 1) Lattice init (one-shot). When auto-step is on, defer until
        // ATR buffer is warm.
        if self.lattice_step.is_none()
            && !self.warming_up()
            && let (Some(b), Some(a)) = (top_b, top_a)
            && b.0 > Decimal::ZERO
            && a.0 > b.0
            && tick > Decimal::ZERO
        {
            let mid = (b.0 + a.0) / Decimal::from(2);
            if let Some(step) = self.compute_step(mid) {
                self.lattice_step = Some(step);
                self.bid_lattice_origin = Some(b.0);
                self.ask_lattice_origin = Some(a.0);
            }
        }

        let lattice_ready = self.lattice_step.is_some()
            && self.bid_lattice_origin.is_some()
            && self.ask_lattice_origin.is_some();
        if !lattice_ready {
            return actions;
        }

        // Position notional for skew calc.
        let mid_for_pos = match (best_bid, best_ask) {
            (Some(b), Some(a)) if a.0 > b.0 && b.0 > Decimal::ZERO => {
                (b.0 + a.0) / Decimal::from(2)
            }
            _ => Decimal::ZERO,
        };
        let pos_notional = ctx.position.size.0 * mid_for_pos;

        // 2) Seed initial window if not yet placed.
        if self.bid_window.is_none() || self.ask_window.is_none() {
            let (bid_count, ask_count) = self.skewed_counts(pos_notional);
            // BID window: k=0 (top) through k=bid_count-1 (deepest).
            // We anchor at the actual current top (k = bid_k_at_or_below(top_b)).
            let bid_top_k = self
                .bid_k_at_or_below(top_b.map(|p| p.0).unwrap_or(Decimal::ZERO))
                .unwrap_or(0);
            let bw = WindowRange {
                low_k: bid_top_k,
                high_k: bid_top_k + (bid_count as i64) - 1,
            };
            let ask_top_k = self
                .ask_k_at_or_above(top_a.map(|p| p.0).unwrap_or(Decimal::ZERO))
                .unwrap_or(0);
            let aw = WindowRange {
                low_k: ask_top_k,
                high_k: ask_top_k + (ask_count as i64) - 1,
            };
            self.bid_window = Some(bw);
            self.ask_window = Some(aw);
            self.emit_window_slots(ctx, Side::Bid, bw, false, &mut actions);
            self.emit_window_slots(ctx, Side::Ask, aw, false, &mut actions);
            return actions;
        }

        // 3) Trigger detection — count missing slots in current windows.
        let bw = self.bid_window.unwrap();
        let aw = self.ask_window.unwrap();
        let missing_bids = self.count_missing(ctx, Side::Bid, bw);
        let missing_asks = self.count_missing(ctx, Side::Ask, aw);
        let threshold = self.recenter_threshold();
        if missing_bids < threshold && missing_asks < threshold {
            return actions; // quiet — do nothing
        }

        // Cooldown gate: refuse to recenter if the last recenter was
        // within `recenter_cooldown_ms`. Prevents the every-event
        // recenter cascade (tight grid + low drain → inventory blowup).
        let now_ns = ctx.now.0;
        if self.config.recenter_cooldown_ms > 0
            && let Some(last) = self.last_recenter_ts
        {
            let gap_ms = now_ns.saturating_sub(last) / 1_000_000;
            if gap_ms < self.config.recenter_cooldown_ms {
                return actions; // still cooling down
            }
        }
        self.last_recenter_ts = Some(now_ns);

        // 4) Recenter — count event + maybe relattice.
        self.recenters_since_relattice = self.recenters_since_relattice.saturating_add(1);
        let relattice = self.config.relattice_every_n_recenters > 0
            && self.recenters_since_relattice >= self.config.relattice_every_n_recenters
            && self.config.step_atr_mult > Decimal::ZERO
            && !self.warming_up();
        let mut did_relattice = false;
        if relattice
            && let (Some(b), Some(a)) = (top_b, top_a)
            && b.0 > Decimal::ZERO
            && a.0 > b.0
            && tick > Decimal::ZERO
        {
            let mid = (b.0 + a.0) / Decimal::from(2);
            if let Some(new_step) = self.compute_step(mid) {
                self.lattice_step = Some(new_step);
                self.bid_lattice_origin = Some(b.0);
                self.ask_lattice_origin = Some(a.0);
                self.recenters_since_relattice = 0;
                did_relattice = true;
            }
        }

        // On relattice the lattice identity changed — every resting order
        // is now on a dead lattice and can never be reused (count_missing
        // keys off the new lattice's prices). Cancel them all so they
        // don't accumulate as orphans, then force-seed the fresh window.
        if did_relattice {
            actions.push(Action::CancelAll);
        }

        // Compute new windows around current top using (possibly new) lattice.
        let (bid_count, ask_count) = self.skewed_counts(pos_notional);
        let new_bid_top = self
            .bid_k_at_or_below(top_b.map(|p| p.0).unwrap_or(Decimal::ZERO))
            .unwrap_or(bw.low_k);
        let new_ask_top = self
            .ask_k_at_or_above(top_a.map(|p| p.0).unwrap_or(Decimal::ZERO))
            .unwrap_or(aw.low_k);
        let new_bw = WindowRange {
            low_k: new_bid_top,
            high_k: new_bid_top + (bid_count as i64) - 1,
        };
        let new_aw = WindowRange {
            low_k: new_ask_top,
            high_k: new_ask_top + (ask_count as i64) - 1,
        };
        self.bid_window = Some(new_bw);
        self.ask_window = Some(new_aw);
        // Prune the window-shift trail: cancel resting orders now outside
        // the new window. Skipped on relattice (CancelAll already wiped).
        if self.config.prune_trail && !did_relattice {
            self.prune_outside_window(ctx, Side::Bid, new_bw, &mut actions);
            self.prune_outside_window(ctx, Side::Ask, new_aw, &mut actions);
        }
        // After CancelAll the ctx.open_quotes view is stale → force emit.
        self.emit_window_slots(ctx, Side::Bid, new_bw, did_relattice, &mut actions);
        self.emit_window_slots(ctx, Side::Ask, new_aw, did_relattice, &mut actions);
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
        if max_position_usdt > Decimal::ZERO {
            self.config.max_position_usdt = max_position_usdt;
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
            grid_levels: 6,
            min_self_spread_bps: 0,
            min_self_spread_ticks: 0,
            grid_step_bps: 0,
            grid_step_ticks: 2,
            recenter_drain_slots: 2,
            skew_max_pct: Decimal::new(25, 2),
            max_position_usdt: Decimal::ZERO,
            bar_interval_secs: 60,
            max_bars: 64,
            atr_period: 14,
            step_atr_mult: Decimal::ZERO,
            bar_warmup_bars: 14,
            relattice_every_n_recenters: 10,
            recenter_cooldown_ms: 1000,
            prune_trail: true,
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
    fn quiet_event_emits_nothing_after_seed() {
        let mut w = Wave::new(cfg());
        let s = snap(Decimal::from(100), Decimal::new(1001, 1));
        let p = pos_flat();
        let sm = sym();
        let c = ctx(&sm, &s, &p, &[]);
        // First event seeds.
        let _ = w.on_event(
            &c,
            &MarketEvent::BookUpdate {
                snapshot: s.clone(),
            },
        );
        // Build open_quotes from emitted-window state — full window alive.
        let bw = w.bid_window.unwrap();
        let aw = w.ask_window.unwrap();
        let mut open: Vec<(QuoteId, QuoteIntent)> = Vec::new();
        for k in bw.low_k..=bw.high_k {
            open.push((
                QuoteId::new(),
                QuoteIntent {
                    symbol: sm.clone(),
                    side: Side::Bid,
                    price: Price(w.bid_price(k).unwrap()),
                    size: Size(Decimal::from(1)),
                    tif: TimeInForce::PostOnly,
                    kind: QuoteKind::Point,
                },
            ));
        }
        for k in aw.low_k..=aw.high_k {
            open.push((
                QuoteId::new(),
                QuoteIntent {
                    symbol: sm.clone(),
                    side: Side::Ask,
                    price: Price(w.ask_price(k).unwrap()),
                    size: Size(Decimal::from(1)),
                    tif: TimeInForce::PostOnly,
                    kind: QuoteKind::Point,
                },
            ));
        }
        let c2 = ctx(&sm, &s, &p, &open);
        let actions = w.on_event(
            &c2,
            &MarketEvent::BookUpdate {
                snapshot: s.clone(),
            },
        );
        assert!(actions.is_empty(), "no churn when window intact");
    }
}
