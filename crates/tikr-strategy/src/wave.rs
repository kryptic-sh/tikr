//! Wave: fixed-lattice band-refill market-making.
//!
//! A frozen price lattice (origin + step, set once at init). Each event
//! the strategy keeps `grid_levels` slots filled on each side around the
//! current touch. When a slot fills, it's re-emitted at its OWN lattice
//! price next event (**refill in place**). The lattice never moves — no
//! recenter, no relattice. Orders that drift outside the active band as
//! price travels stay resting forever (**never cancelled**), so a
//! reversion fills them.
//!
//! ## Behavior
//! 1. **Init (first usable book event, after ATR warmup if enabled):**
//!    freeze lattice. Step = `ATR × step_atr_mult` if set, else the
//!    `*_ticks` / `*_bps` knobs, else 1 tick. Origins = current
//!    top_bid / top_ask (after min_self_spread).
//! 2. **Band refill (every event):** for each side, compute the active
//!    band = `grid_levels` lattice slots from the current cross-guarded
//!    top. Emit any band slot not already resting in `open_quotes`.
//!    Filled slots get refilled in place; far slots keep resting.
//!
//! Inventory is bounded only by per-order size (no position cap) — run
//! on small-min-notional markets so accumulated fills stay survivable.

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
    /// Refill batching: only refill a side once ≥ this many of its band
    /// slots are empty (filled). `1` = refill on any single gap (most
    /// reactive). Higher = wait for N fills then refill them together,
    /// cutting re-emit churn. Default `1`.
    pub refill_threshold: u32,
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
    /// Per-event dedupe (in case Quote action sequence has duplicates).
    emitted_this_event_bid: HashSet<i64>,
    emitted_this_event_ask: HashSet<i64>,
    // ----- adaptive step state (ATR for one-shot lattice init) -----
    /// Bar buffer for ATR computation.
    closed_bars: VecDeque<Bar>,
    open_bar: Option<Bar>,
    current_bucket: Option<u64>,
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
            closed_bars: VecDeque::new(),
            open_bar: None,
            current_bucket: None,
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

        // Compute both bands around the cross-guarded touch.
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
            let thr = self.config.refill_threshold.max(1);
            // Refill only when a round-trip completed: both sides drained
            // ≥ threshold (a bid AND an ask filled → captured spread).
            if bid_drained >= thr && ask_drained >= thr {
                self.emit_window_slots(ctx, Side::Bid, bb, false, &mut actions);
                self.emit_window_slots(ctx, Side::Ask, ab, false, &mut actions);
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
            bar_interval_secs: 60,
            max_bars: 64,
            atr_period: 14,
            step_atr_mult: Decimal::ZERO,
            bar_warmup_bars: 14,
            refill_threshold: 1,
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
}
