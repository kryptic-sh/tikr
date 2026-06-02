//! Wave: fixed-lattice band-refill market-making (pure form).
//!
//! A frozen price lattice (origin + step) computed ONCE at bot start — the grid
//! prices never move (no recenter, no relattice, no adaptive resize). The
//! active `levels`-slot band on each side is a WINDOW over that fixed grid that
//! SLIDES up and down to bracket the current touch: when the market rises the
//! window rises (bids may sit above the bid origin), when it falls the window
//! falls (asks may sit below the ask origin). Only which slots are active
//! moves; the discrete prices orders land on stay on the original lattice, so
//! fills always happen at consistent grid prices.
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
    pub steps_inner: u32,

    /// Completed round-trips needed to trigger a refill: refill once ≥ this
    /// many slots have drained on the bid AND ≥ this many on the ask (each
    /// drained pair = a captured spread). `1` = refill on any completed
    /// round-trip. A whole side emptying refills regardless of this. Default
    /// `1`.
    pub round_trips: u32,
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

    /// Compute `(top_bid_override, top_ask_override)`, pushing the origins
    /// apart to honor the inner dead-zone (`steps_inner × step` off mid each
    /// side, clamped to the touch).
    fn top_overrides(
        &self,
        best_bid: Option<Price>,
        best_ask: Option<Price>,
    ) -> (Option<Price>, Option<Price>) {
        let tick = self.config.tick_size;
        let spread_active = self.config.steps_bps > 0 || self.config.steps_inner > 0;
        if let (Some(bp), Some(ap)) = (best_bid, best_ask)
            && bp.0 > Decimal::ZERO
            && ap.0 > bp.0
            && tick > Decimal::ZERO
            && spread_active
        {
            let mid = (bp.0 + ap.0) / Decimal::from(2);
            // Distance from mid to the first order on each side =
            // `steps_inner × step`. `steps_inner=0` → offset 0 → origins clamp
            // to the touch via the .min(bp)/.max(ap) below.
            let required_half = Decimal::from(self.config.steps_inner) * self.compute_step(mid);
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
        let sbps = self.config.steps_bps;
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
    /// resting-order count to ~`levels` per side.
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
        Self {
            config,
            bid_lattice_origin: None,
            ask_lattice_origin: None,
            lattice_step: None,
            emitted_this_event_bid: HashSet::new(),
            emitted_this_event_ask: HashSet::new(),
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
                steps_inner = self.config.steps_inner,
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
            if round_trip || side_empty {
                self.emit_window_slots(ctx, Side::Bid, bb, &mut actions);
                self.emit_window_slots(ctx, Side::Ask, ab, &mut actions);
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
            levels: 6,
            steps_bps: 10,
            steps_inner: 0,
            round_trips: 1,
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
    fn refill_prunes_orders_outside_window() {
        // After the window slides and refills, every resting order now outside
        // the new window must be cancelled (no orphaned stragglers). Seed at
        // ~100, jump the book to ~110 → all 12 old orders fall outside the new
        // window → all 12 must get a Cancel.
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
        let seeded_ids: std::collections::HashSet<QuoteId> =
            open.iter().map(|(id, _)| *id).collect();

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
            cancelled, seeded_ids,
            "every order outside the slid window must be cancelled: {a_up:?}"
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
}
