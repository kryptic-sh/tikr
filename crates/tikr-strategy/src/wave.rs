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
    /// Lattice slots per side. Default 12.
    pub grid_levels: u32,

    /// Level spacing in bps of mid — the gap between consecutive lattice
    /// levels. Snapped to tick (min 1 tick). `0` = 1-tick lattice. Required;
    /// also the default for `inner_bps` when that is unset.
    pub step_bps: u32,

    /// Inner self-spread in bps of mid: the distance from mid to the FIRST
    /// order on each side (where the frozen origins sit), independent of the
    /// `step_bps` level spacing. e.g. `inner_bps=2, step_bps=5` → first order
    /// 2bps off mid, then levels 5bps apart (2,7,12,…). `0` (default/unset) =
    /// inner defaults to the full `step_bps` each side. Snapped to tick.
    pub inner_bps: u32,

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

    /// Inventory skew, in lattice slots. As `|position notional|` grows toward
    /// `max_position_usdt`, the *overloaded* side's band is shifted to deeper
    /// frozen slots — long → bids move lower (buy slower), short → asks move
    /// higher (sell slower) — while the reducing side stays at the touch to
    /// actively flatten. Offset scales linearly from 0 (flat) to this many
    /// slots (at/over the cap). Requires `max_position_usdt > 0`. `0`
    /// (default) = no skew (symmetric lattice, original behavior).
    pub inventory_skew_slots: u32,
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
        let spread_active = self.config.step_bps > 0 || self.config.inner_bps > 0;
        if let (Some(bp), Some(ap)) = (best_bid, best_ask)
            && bp.0 > Decimal::ZERO
            && ap.0 > bp.0
            && tick > Decimal::ZERO
            && spread_active
        {
            let mid = (bp.0 + ap.0) / Decimal::from(2);
            // Distance from mid to the first order on each side. Explicit
            // `inner_bps` when set (>0); otherwise default to the full
            // `step_bps` each side (step_bps is required, inner_bps optional).
            let inner_bps = if self.config.inner_bps > 0 {
                self.config.inner_bps
            } else {
                self.config.step_bps
            };
            let required_half = mid * Decimal::from(inner_bps) / Decimal::from(10_000);
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

    /// Base lattice gap = `step_bps` of mid, snapped up to tick (min 1 tick).
    /// `step_bps = 0` → 1-tick gap. This is the distance from origin to the
    /// first level.
    fn compute_step(&self, mid: Decimal) -> Decimal {
        let tick = self.config.tick_size;
        if self.config.step_bps > 0 && mid > Decimal::ZERO && tick > Decimal::ZERO {
            let target = mid * Decimal::from(self.config.step_bps) / Decimal::from(10_000);
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

    /// Per-side band slot offset from inventory skew: `(bid_skew, ask_skew)`.
    /// Only the overloaded side is offset (long → bids deeper, short → asks
    /// deeper); the reducing side stays at the touch. Offset scales linearly
    /// from 0 (flat) to `inventory_skew_slots` (|position notional| ≥ cap).
    /// Returns `(0, 0)` when skew is disabled or no cap is set.
    fn inventory_skew(
        &self,
        ctx: &StrategyContext<'_>,
        best_bid: Option<Price>,
        best_ask: Option<Price>,
    ) -> (i64, i64) {
        let skew_max = self.config.inventory_skew_slots as i64;
        let cap = self.config.max_position_usdt;
        if skew_max <= 0 || cap <= Decimal::ZERO {
            return (0, 0);
        }
        let mid = match (best_bid, best_ask) {
            (Some(b), Some(a)) if a.0 > b.0 => (b.0 + a.0) / Decimal::from(2),
            _ => return (0, 0),
        };
        let pos_notional = ctx.position.size.0 * mid;
        let ratio = (pos_notional.abs() / cap).min(Decimal::ONE);
        let skew = (ratio * Decimal::from(skew_max))
            .round()
            .to_string()
            .parse::<i64>()
            .unwrap_or(0)
            .clamp(0, skew_max);
        if pos_notional > Decimal::ZERO {
            (skew, 0)
        } else if pos_notional < Decimal::ZERO {
            (0, skew)
        } else {
            (0, 0)
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
                step_bps = self.config.step_bps,
                inner_bps = self.config.inner_bps,
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

        // Inventory skew: shift the overloaded side's band to deeper frozen
        // slots so it quotes further from the touch (long → bids lower, short
        // → asks higher), throttling the side that grows inventory while the
        // reducing side stays at the touch to flatten. Offset scales 0..N
        // slots by `|position notional| / cap`.
        let (bid_skew, ask_skew) = self.inventory_skew(ctx, best_bid, best_ask);

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
                low_k: top_k + bid_skew,
                high_k: top_k + bid_skew + levels - 1,
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
                low_k: top_k + ask_skew,
                high_k: top_k + ask_skew + levels - 1,
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
                let pos = ctx.position.size.0;
                let cap = self.config.max_position_usdt;
                let pos_notional = pos * mid;
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
            inner_bps: 0,
            refill_threshold: 1,
            max_position_usdt: Decimal::ZERO,
            inventory_skew_slots: 0,
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

    fn pos_size(size: i64) -> Position {
        Position {
            symbol: sym(),
            size: SignedSize(Decimal::from(size)),
            avg_entry: Price(Decimal::ZERO),
            realized_pnl: Notional(Decimal::ZERO),
        }
    }

    #[test]
    fn inventory_skew_offsets_overloaded_side() {
        let mut c = cfg();
        c.inventory_skew_slots = 8;
        c.max_position_usdt = Decimal::from(600);
        let w = Wave::new(c);
        let s = snap(Decimal::from(100), Decimal::from(101)); // mid 100.5
        let bb = s.bids.first().map(|l| l.price);
        let ba = s.asks.first().map(|l| l.price);
        let sy = sym();

        // Flat → no skew either side.
        let flat = pos_flat();
        assert_eq!(w.inventory_skew(&ctx(&sy, &s, &flat, &[]), bb, ba), (0, 0));

        // Long at/over cap (6 × 100.5 = 603 ≥ 600) → full skew on bids only.
        let long = pos_size(6);
        assert_eq!(w.inventory_skew(&ctx(&sy, &s, &long, &[]), bb, ba), (8, 0));

        // Short at/over cap → full skew on asks only.
        let short = pos_size(-6);
        assert_eq!(w.inventory_skew(&ctx(&sy, &s, &short, &[]), bb, ba), (0, 8));

        // Half cap long (3 × 100.5 = 301.5 ≈ 0.5×600) → ~half skew on bids.
        let half = pos_size(3);
        assert_eq!(w.inventory_skew(&ctx(&sy, &s, &half, &[]), bb, ba), (4, 0));
    }

    #[test]
    fn inner_bps_decouples_first_order_from_step_spacing() {
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
        // inner_bps=2 with step_bps=10: first order ~2bps off mid, levels ~10bps.
        let mut tight = cfg();
        tight.tick_size = Decimal::new(1, 2);
        tight.step_bps = 10;
        tight.inner_bps = 2;
        let (inner_tight, step_tight) = freeze(tight);
        // Default (inner_bps=0): first order defaults to full step_bps = 10bps off mid.
        let mut legacy = cfg();
        legacy.tick_size = Decimal::new(1, 2);
        legacy.step_bps = 10;
        legacy.inner_bps = 0;
        let (inner_legacy, step_legacy) = freeze(legacy);
        // Explicit inner_bps=2 puts the first order CLOSER to mid than the
        // step_bps default (10bps).
        assert!(
            inner_tight < inner_legacy,
            "inner_bps=2 ({inner_tight}) must be tighter than the step_bps default ({inner_legacy})"
        );
        // Step spacing is unchanged by inner_bps (both == step_bps geometry).
        assert_eq!(
            step_tight, step_legacy,
            "step spacing independent of inner_bps"
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
    fn inventory_skew_disabled_without_cap_or_slots() {
        let s = snap(Decimal::from(100), Decimal::from(101));
        let bb = s.bids.first().map(|l| l.price);
        let ba = s.asks.first().map(|l| l.price);
        let sy = sym();
        let long = pos_size(6);
        // slots=0 → off even with a cap.
        let mut c1 = cfg();
        c1.inventory_skew_slots = 0;
        c1.max_position_usdt = Decimal::from(600);
        assert_eq!(
            Wave::new(c1).inventory_skew(&ctx(&sy, &s, &long, &[]), bb, ba),
            (0, 0)
        );
        // cap=0 → off even with slots (no ratio to scale).
        let mut c2 = cfg();
        c2.inventory_skew_slots = 8;
        c2.max_position_usdt = Decimal::ZERO;
        assert_eq!(
            Wave::new(c2).inventory_skew(&ctx(&sy, &s, &long, &[]), bb, ba),
            (0, 0)
        );
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
