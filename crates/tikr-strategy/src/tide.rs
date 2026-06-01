//! Center-tracking, timer-reconciled ladder market-making strategy.
//!
//! Tide maintains a fixed-step ladder anchored to the last full fill price
//! (or the first book mid when no fills have occurred). A timer-driven
//! reconcile patches the resting set every ≥1 second: adds slots that are
//! absent and cancels orders that have drifted outside the re-centered
//! target lattice.

use std::collections::VecDeque;

use tikr_core::{Decimal, MarketEvent, Price, QuoteKind, Side, Size, Symbol, TimeInForce};
use tikr_venue::QuoteIntent;

use crate::{Action, Strategy, StrategyContext};

/// Configuration for [`Tide`].
#[derive(Debug, Clone)]
pub struct TideConfig {
    /// Notional USDT per order. Quantity = `notional / price`, floored
    /// to `step_size`, bumped to meet `min_notional`.
    pub notional_per_order: Decimal,
    /// Venue tick size. Used for snapping spread and grid step
    /// computations to the nearest tick.
    pub tick_size: Decimal,
    /// Venue lot step.
    pub step_size: Decimal,
    /// Venue min order notional.
    pub min_notional: Decimal,
    /// Grid depth per side. `1` (default) = one order per side.
    /// `N > 1` places N orders per side, separated by `step`.
    pub grid_levels: u32,
    /// Lattice step in bps of mid. `0` (default) = 1-tick spacing.
    pub step_bps: u32,
    /// Per-bot peak position cap in USDT notional. `0` = no cap.
    pub max_position_usdt: Decimal,
    /// Unused. Kept for config compat.
    pub prune_stragglers: bool,
    /// Unused. Kept for config compat.
    pub recenter_bps: u32,
    /// Unused. Kept for config compat.
    pub recenter_secs: u32,
    /// Skip the inner rungs: the innermost order on each side is placed at
    /// `(inner_steps + 1) × lattice_step` from center. `0` (default) = top
    /// order at one step from center.
    pub inner_steps: u32,
    /// Unused. Kept for config compat.
    pub chase: bool,
    /// Unused. Kept for config compat.
    pub chase_to_avg: bool,
    /// Unused. Kept for config compat.
    pub relattice_timeout_secs: u32,
    /// Persistence skew gain: lattice STEPS to back the bag side off per
    /// order-size of the ROLLING-AVERAGE signed bag. `0` (default) = off.
    pub inventory_skew: Decimal,
    /// Window (number of per-reconcile snapshots, ~1/sec) for the rolling
    /// signed-bag average that drives the skew. `0`/unset → 60.
    pub inventory_skew_window: u32,
}

/// Center-tracking, timer-reconciled ladder strategy state.
pub struct Tide {
    config: TideConfig,
    /// STATIC lattice anchor — tick-aligned, frozen on the first reconcile. The
    /// price grid is `lattice_origin + n·lattice_step` for all integers n and
    /// NEVER moves. Only the active window (which slots carry orders) slides.
    lattice_origin: Option<Decimal>,
    /// Frozen lattice step. Set on the first reconcile.
    lattice_step: Option<Decimal>,
    /// Window anchor (the "filled-step center"): the last full fill price,
    /// bootstrapped to the first book mid. Snapped onto the static lattice in
    /// `reconcile` so the grid stays put and only the window slides.
    center: Option<Decimal>,
    /// `ctx.now.0` at the last reconcile, in nanoseconds.
    last_reconcile_ns: Option<u64>,
    /// Rolling window of signed bag in order-sizes (newest at back), used to
    /// compute the persistence-driven inventory skew.
    bag_window: VecDeque<Decimal>,
}

impl Tide {
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

    /// Best-bid/best-ask mid. Returns `None` if either side empty, non-positive,
    /// or ask ≤ bid.
    fn book_mid(book: &tikr_core::Snapshot) -> Option<Decimal> {
        let bid = book.bids.first()?.price.0;
        let ask = book.asks.first()?.price.0;
        if bid <= Decimal::ZERO || ask <= Decimal::ZERO || ask <= bid {
            return None;
        }
        Some((bid + ask) / Decimal::from(2))
    }

    /// Compute the step from mid. If `step_bps > 0`, compute as bps of mid
    /// rounded up to the next tick. Otherwise return one tick.
    fn compute_step(&self, mid: Decimal) -> Decimal {
        let tick = self.config.tick_size;
        if self.config.step_bps > 0 && tick > Decimal::ZERO {
            let t = mid * Decimal::from(self.config.step_bps) / Decimal::from(10_000);
            if t > tick {
                (t / tick).ceil() * tick
            } else {
                tick
            }
        } else {
            tick
        }
    }

    /// Reconcile the resting order set against the target lattice anchored at
    /// `center`. Emits adds first, then cancels — the runner dispatches them
    /// in that order (adds land before any cancel-triggered gaps).
    fn reconcile(&mut self, ctx: &StrategyContext<'_>, center_raw: Decimal) -> Vec<Action> {
        // Freeze the STATIC lattice on the first reconcile: a fixed step and a
        // tick-aligned origin. The price grid `origin + n·step` NEVER moves.
        let step = match self.lattice_step {
            Some(s) => s,
            None => {
                let s = self.compute_step(center_raw);
                self.lattice_step = Some(s);
                s
            }
        };
        let origin = match self.lattice_origin {
            Some(o) => o,
            None => {
                let tick = self.config.tick_size;
                let o = if tick > Decimal::ZERO {
                    (center_raw / tick).round() * tick
                } else {
                    center_raw
                };
                self.lattice_origin = Some(o);
                o
            }
        };
        // Snap the anchor onto the static lattice — only the active window slides
        // (in whole steps); the grid itself stays put. This is what keeps the
        // fixed lattice honored even when the anchor is an off-grid book mid.
        let center = if step > Decimal::ZERO {
            origin + ((center_raw - origin) / step).round() * step
        } else {
            center_raw
        };

        let levels = self.config.grid_levels.max(1);
        let inner = Decimal::from(self.config.inner_steps + 1);

        // Snapshot the current SIGNED bag (in order-sizes) once per reconcile
        // (~1/sec) into a rolling window; its average drives a one-sided widen.
        let mark = Self::book_mid(ctx.latest_book).unwrap_or(center);
        let window = self.config.inventory_skew_window.max(1) as usize;
        let bag_rungs = if self.config.notional_per_order > Decimal::ZERO {
            ctx.position.size.0 * mark / self.config.notional_per_order
        } else {
            Decimal::ZERO
        };
        self.bag_window.push_back(bag_rungs);
        while self.bag_window.len() > window {
            self.bag_window.pop_front();
        }
        let (bid_skew, ask_skew) =
            if self.config.inventory_skew > Decimal::ZERO && !self.bag_window.is_empty() {
                let sum: Decimal = self.bag_window.iter().copied().sum();
                let avg = sum / Decimal::from(self.bag_window.len() as u64);
                // round() gives a free deadband: near-zero (chop) avg → 0 shift.
                let shift = (self.config.inventory_skew * avg.abs())
                    .clamp(Decimal::ZERO, Decimal::from(levels))
                    .round();
                if avg < Decimal::ZERO {
                    (Decimal::ZERO, shift) // persistently SHORT → push ASKS away (up)
                } else if avg > Decimal::ZERO {
                    (shift, Decimal::ZERO) // persistently LONG → push BIDS away (down)
                } else {
                    (Decimal::ZERO, Decimal::ZERO)
                }
            } else {
                (Decimal::ZERO, Decimal::ZERO)
            };

        // Compute target prices.
        let mut target_bids: Vec<Decimal> = Vec::with_capacity(levels as usize);
        let mut target_asks: Vec<Decimal> = Vec::with_capacity(levels as usize);
        for k in 0..levels {
            let k_dec = Decimal::from(k);
            let bid_price = center - (inner + k_dec + bid_skew) * step;
            let ask_price = center + (inner + k_dec + ask_skew) * step;
            if bid_price > Decimal::ZERO {
                target_bids.push(bid_price);
            }
            target_asks.push(ask_price);
        }

        // Position-cap guard.
        let cap = self.config.max_position_usdt;
        let pos_notional = ctx.position.size.0 * center;
        let suppress_bids = cap > Decimal::ZERO && pos_notional >= cap;
        let suppress_asks = cap > Decimal::ZERO && -pos_notional >= cap;

        // Split resting orders by side.
        let resting_bids: Vec<(tikr_venue::QuoteId, Decimal)> = ctx
            .open_quotes
            .iter()
            .filter(|(_, q)| q.side == Side::Bid)
            .map(|(id, q)| (*id, q.price.0))
            .collect();
        let resting_asks: Vec<(tikr_venue::QuoteId, Decimal)> = ctx
            .open_quotes
            .iter()
            .filter(|(_, q)| q.side == Side::Ask)
            .map(|(id, q)| (*id, q.price.0))
            .collect();

        let tol = step / Decimal::from(2);

        let mut adds: Vec<Action> = Vec::new();
        let mut cancels: Vec<Action> = Vec::new();

        // Adds: target slots with no matching resting order.
        if !suppress_bids {
            for &tgt in &target_bids {
                let covered = resting_bids.iter().any(|(_, p)| (*p - tgt).abs() <= tol);
                if !covered {
                    adds.push(self.make_quote(ctx.symbol, Side::Bid, Price(tgt)));
                }
            }
        }
        if !suppress_asks {
            for &tgt in &target_asks {
                let covered = resting_asks.iter().any(|(_, p)| (*p - tgt).abs() <= tol);
                if !covered {
                    adds.push(self.make_quote(ctx.symbol, Side::Ask, Price(tgt)));
                }
            }
        }

        // Cancels: resting orders not within tol of any target.
        for (id, p) in &resting_bids {
            let on_lattice = target_bids.iter().any(|t| (*t - *p).abs() <= tol);
            if !on_lattice {
                cancels.push(Action::Cancel(*id));
            }
        }
        for (id, p) in &resting_asks {
            let on_lattice = target_asks.iter().any(|t| (*t - *p).abs() <= tol);
            if !on_lattice {
                cancels.push(Action::Cancel(*id));
            }
        }

        adds.extend(cancels);
        adds
    }
}

impl Strategy for Tide {
    type Config = TideConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            lattice_origin: None,
            lattice_step: None,
            center: None,
            last_reconcile_ns: None,
            bag_window: VecDeque::new(),
        }
    }

    fn name(&self) -> &str {
        "tide"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        if let MarketEvent::Fill(f) = event
            && f.is_full
        {
            // Update center; all order work deferred to the timer.
            self.center = Some(f.price.0);
            return Vec::new();
        }

        // Non-fill events: bootstrap center if needed.
        if self.center.is_none() {
            match Self::book_mid(ctx.latest_book) {
                Some(mid) => self.center = Some(mid),
                None => return Vec::new(),
            }
        }

        // Timer gate: reconcile at most once per second.
        let due = match self.last_reconcile_ns {
            None => true,
            Some(last) => ctx.now.0.saturating_sub(last) >= 1_000_000_000,
        };

        if due && let Some(c) = self.center {
            // Fallback to mid: if the live mid has drifted beyond the grid's
            // reach from the last-fill center, the lattice no longer brackets
            // price (it would sit entirely on one side and never fill). Re-anchor
            // the center on the current mid so the next reconcile rebuilds around
            // price. Reach = outermost order distance = (inner_steps+grid_levels)·step.
            let effective_center = match Self::book_mid(ctx.latest_book) {
                Some(mid) => {
                    let step = self.lattice_step.unwrap_or_else(|| self.compute_step(c));
                    let reach =
                        Decimal::from(self.config.inner_steps + self.config.grid_levels.max(1))
                            * step;
                    if (mid - c).abs() > reach { mid } else { c }
                }
                None => c,
            };
            self.center = Some(effective_center);
            let actions = self.reconcile(ctx, effective_center);
            self.last_reconcile_ns = Some(ctx.now.0);
            return actions;
        }

        Vec::new()
    }

    fn on_quote_rejected(
        &mut self,
        _ctx: &StrategyContext<'_>,
        _intent: &QuoteIntent,
        _reason: &str,
    ) -> Vec<Action> {
        Vec::new()
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
        Asset, Fill, Level, MarketKind, Notional, Position, SignedSize, Snapshot, Timestamp,
        VenueId,
    };
    use tikr_venue::QuoteId;

    fn sym() -> Symbol {
        Symbol {
            base: Asset::new("ESPORTS"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("test"),
            kind: MarketKind::Perp,
        }
    }

    fn book(bid: Decimal, ask: Decimal) -> Snapshot {
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

    fn pos() -> Position {
        Position {
            symbol: sym(),
            size: SignedSize(Decimal::ZERO),
            avg_entry: Price(Decimal::ZERO),
            realized_pnl: Notional(Decimal::ZERO),
        }
    }

    /// Default config: tick=0.0001, step_bps=0 (→step=tick), grid_levels=3,
    /// inner_steps=1, notional_per_order=10, min_notional=5, step_size=1.
    fn cfg() -> TideConfig {
        TideConfig {
            notional_per_order: Decimal::from(10),
            tick_size: Decimal::new(1, 4), // 0.0001
            step_size: Decimal::ONE,
            min_notional: Decimal::from(5),
            grid_levels: 3,
            step_bps: 0,
            max_position_usdt: Decimal::ZERO,
            prune_stragglers: true,
            recenter_bps: 0,
            recenter_secs: 0,
            inner_steps: 1,
            chase: false,
            chase_to_avg: false,
            relattice_timeout_secs: 300,
            inventory_skew: Decimal::ZERO,
            inventory_skew_window: 60,
        }
    }

    /// Build a StrategyContext with a configurable `now` timestamp.
    fn make_ctx<'a>(
        symbol: &'a Symbol,
        snap: &'a Snapshot,
        position: &'a Position,
        open: &'a [(QuoteId, QuoteIntent)],
        now_ns: u64,
    ) -> StrategyContext<'a> {
        StrategyContext {
            symbol,
            now: Timestamp(now_ns),
            position,
            recent_fills: &[],
            latest_book: snap,
            open_quotes: open,
            recent_liqs: &[],
        }
    }

    fn mk_fill(side: Side, price: Decimal, is_full: bool) -> Fill {
        Fill {
            quote_id: QuoteId::new(),
            price: Price(price),
            size: Size(Decimal::ONE),
            fee_asset: Asset::new("USDT"),
            fee_amount: Decimal::ZERO,
            fee_quote: Notional(Decimal::ZERO),
            side,
            ts: Timestamp(1),
            is_full,
            trade_id: None,
        }
    }

    fn make_intent(symbol: &Symbol, side: Side, price: Decimal) -> QuoteIntent {
        QuoteIntent {
            symbol: symbol.clone(),
            side,
            price: Price(price),
            size: Size(Decimal::ONE),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        }
    }

    // -----------------------------------------------------------------------
    // Test 1: initial reconcile seeds the full grid
    // book 0.0100/0.0102 → mid = 0.0101 (bootstrap center)
    // step = tick = 0.0001, inner_steps=1, grid_levels=3
    // inner = inner_steps+1 = 2
    // bid_k = 0.0101 − (2+k)·0.0001  k=0→0.0099, k=1→0.0098, k=2→0.0097
    // ask_k = 0.0101 + (2+k)·0.0001  k=0→0.0103, k=1→0.0104, k=2→0.0105
    // -----------------------------------------------------------------------
    #[test]
    fn initial_reconcile_seeds_full_grid() {
        let mut s = Tide::new(cfg());
        let snap = book(Decimal::new(100, 4), Decimal::new(102, 4));
        let p = pos();
        let symbol = sym();
        let ctx = make_ctx(&symbol, &snap, &p, &[], 0);

        let actions = s.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );

        // All actions must be Quote (no Cancels on a fresh grid).
        for a in &actions {
            assert!(matches!(a, Action::Quote(_)), "expected Quote, got {a:?}");
        }

        let bids: Vec<Decimal> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Bid => Some(q.price.0),
                _ => None,
            })
            .collect();
        let asks: Vec<Decimal> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Ask => Some(q.price.0),
                _ => None,
            })
            .collect();

        assert_eq!(bids.len(), 3, "expected 3 bids: {bids:?}");
        assert_eq!(asks.len(), 3, "expected 3 asks: {asks:?}");

        // center=0.0101, step=0.0001, inner=2
        for (k, expected) in [(0u32, 99i64), (1, 98), (2, 97)] {
            let px = Decimal::new(expected, 4);
            assert!(bids.contains(&px), "bid k={k} at {px} missing: {bids:?}");
        }
        for (k, expected) in [(0u32, 103i64), (1, 104), (2, 105)] {
            let px = Decimal::new(expected, 4);
            assert!(asks.contains(&px), "ask k={k} at {px} missing: {asks:?}");
        }
    }

    // -----------------------------------------------------------------------
    // Test 2: throttle — second event within 1 s returns empty;
    // event at ≥1000 ms reconciles again.
    // -----------------------------------------------------------------------
    #[test]
    fn throttle_within_one_second() {
        let mut s = Tide::new(cfg());
        let snap = book(Decimal::new(100, 4), Decimal::new(102, 4));
        let p = pos();
        let symbol = sym();

        // t=0: first BookUpdate → seeds (due = true, last_reconcile=None)
        let ctx0 = make_ctx(&symbol, &snap, &p, &[], 0);
        let a0 = s.on_event(
            &ctx0,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        assert!(!a0.is_empty(), "t=0 should reconcile");

        // t=500ms: not yet due (500_000_000 < 1_000_000_000)
        let ctx1 = make_ctx(&symbol, &snap, &p, &[], 500_000_000);
        let a1 = s.on_event(
            &ctx1,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        assert!(a1.is_empty(), "t=500ms must be throttled: {a1:?}");

        // t=1000ms: exactly 1 s elapsed → due again
        let ctx2 = make_ctx(&symbol, &snap, &p, &[], 1_000_000_000);
        let a2 = s.on_event(
            &ctx2,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        assert!(!a2.is_empty(), "t=1000ms should reconcile again: {a2:?}");
    }

    // -----------------------------------------------------------------------
    // Test 3: hole refill — one slot absent from resting set triggers exactly
    // one add for that slot, no spurious cancels.
    // -----------------------------------------------------------------------
    #[test]
    fn hole_refill() {
        let mut s = Tide::new(cfg());
        let snap = book(Decimal::new(100, 4), Decimal::new(102, 4));
        let p = pos();
        let symbol = sym();

        // Bootstrap center at t=0.
        let ctx0 = make_ctx(&symbol, &snap, &p, &[], 0);
        let _ = s.on_event(
            &ctx0,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );

        // Build resting set representing the full grid MINUS the innermost bid
        // (0.0099 = center−2·step with center=0.0101).
        // Bids present: 0.0098, 0.0097 (missing 0.0099)
        // Asks present: 0.0103, 0.0104, 0.0105
        let mut open: Vec<(QuoteId, QuoteIntent)> = Vec::new();
        for px in [98i64, 97] {
            open.push((
                QuoteId::new(),
                make_intent(&symbol, Side::Bid, Decimal::new(px, 4)),
            ));
        }
        for px in [103i64, 104, 105] {
            open.push((
                QuoteId::new(),
                make_intent(&symbol, Side::Ask, Decimal::new(px, 4)),
            ));
        }

        // Advance ≥1000ms so the timer fires.
        let ctx1 = make_ctx(&symbol, &snap, &p, &open, 1_000_000_000);
        let actions = s.on_event(
            &ctx1,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );

        let quotes: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) => Some(q.clone()),
                _ => None,
            })
            .collect();
        let cancels: Vec<_> = actions
            .iter()
            .filter(|a| matches!(a, Action::Cancel(_)))
            .collect();

        // Exactly one add: the missing bid at 0.0099.
        assert_eq!(quotes.len(), 1, "expected one hole-fill Quote: {quotes:?}");
        assert_eq!(quotes[0].side, Side::Bid);
        assert_eq!(quotes[0].price.0, Decimal::new(99, 4));

        // No spurious cancels.
        assert!(cancels.is_empty(), "no cancels expected: {cancels:?}");
    }

    // -----------------------------------------------------------------------
    // Test 4: fill re-centers the lattice.
    // Seed at center=0.0101. Full Ask fill at 0.0103 → center becomes 0.0103.
    // After ≥1000ms with the original grid as open_quotes, reconcile should
    // cancel orders outside the re-centered lattice and add the new slots.
    //
    // New target (center=0.0103, step=0.0001, inner=2):
    //   bids: 0.0101, 0.0100, 0.0099
    //   asks: 0.0105, 0.0106, 0.0107
    // Old resting (seeded at 0.0101):
    //   bids: 0.0099, 0.0098, 0.0097
    //   asks: 0.0103, 0.0104, 0.0105
    //
    // Adds expected: bid 0.0101, bid 0.0100, ask 0.0106, ask 0.0107
    // Cancels expected: bid 0.0098, bid 0.0097, ask 0.0103
    // (bid 0.0099 and ask 0.0105 appear in BOTH old and new lattice — no action)
    // -----------------------------------------------------------------------
    #[test]
    fn fill_recenters_lattice() {
        let mut s = Tide::new(cfg());
        let snap = book(Decimal::new(100, 4), Decimal::new(102, 4));
        let p = pos();
        let symbol = sym();

        // Seed at t=0 (center bootstraps to 0.0101).
        let ctx0 = make_ctx(&symbol, &snap, &p, &[], 0);
        let _ = s.on_event(
            &ctx0,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );

        // Full Ask fill at 0.0103 → center = 0.0103, no order actions.
        let fill = mk_fill(Side::Ask, Decimal::new(103, 4), true);
        let ctx_fill = make_ctx(&symbol, &snap, &p, &[], 500_000_000);
        let fill_actions = s.on_event(&ctx_fill, &MarketEvent::Fill(fill));
        assert!(
            fill_actions.is_empty(),
            "fill must return empty: {fill_actions:?}"
        );

        // Build old resting set (seeded at center=0.0101).
        let mut open: Vec<(QuoteId, QuoteIntent)> = Vec::new();
        let bid_98_id = QuoteId::new();
        let bid_97_id = QuoteId::new();
        let ask_103_id = QuoteId::new();
        open.push((
            bid_98_id,
            make_intent(&symbol, Side::Bid, Decimal::new(98, 4)),
        ));
        open.push((
            bid_97_id,
            make_intent(&symbol, Side::Bid, Decimal::new(97, 4)),
        ));
        open.push((
            QuoteId::new(),
            make_intent(&symbol, Side::Bid, Decimal::new(99, 4)),
        ));
        open.push((
            ask_103_id,
            make_intent(&symbol, Side::Ask, Decimal::new(103, 4)),
        ));
        open.push((
            QuoteId::new(),
            make_intent(&symbol, Side::Ask, Decimal::new(104, 4)),
        ));
        open.push((
            QuoteId::new(),
            make_intent(&symbol, Side::Ask, Decimal::new(105, 4)),
        ));

        // Reconcile at t=1001ms (≥1s after t=0).
        let ctx1 = make_ctx(&symbol, &snap, &p, &open, 1_001_000_000);
        let actions = s.on_event(
            &ctx1,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );

        // Adds must precede cancels.
        let first_cancel = actions.iter().position(|a| matches!(a, Action::Cancel(_)));
        let last_quote = actions.iter().rposition(|a| matches!(a, Action::Quote(_)));
        if let (Some(cp), Some(qp)) = (first_cancel, last_quote) {
            assert!(qp < cp, "Quotes must precede Cancels: {actions:?}");
        }

        let add_bids: Vec<Decimal> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Bid => Some(q.price.0),
                _ => None,
            })
            .collect();
        let add_asks: Vec<Decimal> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Ask => Some(q.price.0),
                _ => None,
            })
            .collect();
        let cancelled: Vec<QuoteId> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Cancel(id) => Some(*id),
                _ => None,
            })
            .collect();

        // Adds: new slots at 0.0101, 0.0100 (bids) and 0.0106, 0.0107 (asks).
        assert!(
            add_bids.contains(&Decimal::new(101, 4)),
            "add bid 0.0101: {add_bids:?}"
        );
        assert!(
            add_bids.contains(&Decimal::new(100, 4)),
            "add bid 0.0100: {add_bids:?}"
        );
        assert!(
            add_asks.contains(&Decimal::new(106, 4)),
            "add ask 0.0106: {add_asks:?}"
        );
        assert!(
            add_asks.contains(&Decimal::new(107, 4)),
            "add ask 0.0107: {add_asks:?}"
        );

        // Cancels: old orders now off-lattice.
        assert!(
            cancelled.contains(&bid_98_id),
            "cancel bid 0.0098: {cancelled:?}"
        );
        assert!(
            cancelled.contains(&bid_97_id),
            "cancel bid 0.0097: {cancelled:?}"
        );
        assert!(
            cancelled.contains(&ask_103_id),
            "cancel ask 0.0103: {cancelled:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Test 5: cancel-outside — extra resting order far from any target gets
    // cancelled on reconcile.
    // -----------------------------------------------------------------------
    #[test]
    fn cancel_outside_lattice() {
        let mut s = Tide::new(cfg());
        let snap = book(Decimal::new(100, 4), Decimal::new(102, 4));
        let p = pos();
        let symbol = sym();

        // Bootstrap at t=0.
        let ctx0 = make_ctx(&symbol, &snap, &p, &[], 0);
        let _ = s.on_event(
            &ctx0,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );

        // One stray bid far below any target.
        let stray_id = QuoteId::new();
        let open = vec![(
            stray_id,
            make_intent(&symbol, Side::Bid, Decimal::new(50, 4)), // 0.0050 — way off lattice
        )];

        let ctx1 = make_ctx(&symbol, &snap, &p, &open, 1_000_000_000);
        let actions = s.on_event(
            &ctx1,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );

        let cancelled: Vec<QuoteId> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Cancel(id) => Some(*id),
                _ => None,
            })
            .collect();

        assert!(
            cancelled.contains(&stray_id),
            "stray order must be cancelled: {cancelled:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Test 6: position cap suppresses bid Quotes but still adds/keeps asks
    // and cancels out-of-lattice orders as normal.
    // -----------------------------------------------------------------------
    #[test]
    fn fallback_to_mid_when_price_drifts_beyond_grid() {
        // inner_steps=1, grid_levels=3, step=tick=0.0001 → reach=(1+3)·0.0001=0.0004.
        let mut c = cfg();
        c.inner_steps = 1;
        c.grid_levels = 3;
        let mut s = Tide::new(c);
        let symbol = sym();
        let p = pos();

        // First event seeds center at mid 0.0101 (book 0.0100/0.0102), t=0.
        let snap1 = book(Decimal::new(100, 4), Decimal::new(102, 4));
        let ctx1 = make_ctx(&symbol, &snap1, &p, &[], 0);
        let _ = s.on_event(
            &ctx1,
            &MarketEvent::BookUpdate {
                snapshot: snap1.clone(),
            },
        );

        // Book mid jumps to 0.0120 (|0.0120−0.0101| = 0.0019 > reach 0.0004) at +2s.
        // The grid must re-anchor on the new mid: innermost bid 0.0118, ask 0.0122.
        let snap2 = book(Decimal::new(119, 4), Decimal::new(121, 4));
        let ctx2 = make_ctx(&symbol, &snap2, &p, &[], 2_000_000_000);
        let actions = s.on_event(
            &ctx2,
            &MarketEvent::BookUpdate {
                snapshot: snap2.clone(),
            },
        );
        let bids: Vec<Decimal> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Bid => Some(q.price.0),
                _ => None,
            })
            .collect();
        let asks: Vec<Decimal> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Ask => Some(q.price.0),
                _ => None,
            })
            .collect();
        assert!(
            bids.contains(&Decimal::new(118, 4)),
            "re-anchored innermost bid 0.0118: {bids:?}"
        );
        assert!(
            asks.contains(&Decimal::new(122, 4)),
            "re-anchored innermost ask 0.0122: {asks:?}"
        );
    }

    #[test]
    fn static_lattice_honored_when_anchor_off_grid() {
        // step_bps=30 at mid 1.0 → step 0.0030 (30 ticks); origin snaps to 1.0000.
        // The grid is 1.0000 + n·0.0030 and must NEVER move. inner_steps=0,
        // grid_levels=2.
        let mut c = cfg();
        c.step_bps = 30;
        c.inner_steps = 0;
        c.grid_levels = 2;
        let mut s = Tide::new(c);
        let symbol = sym();
        let p = pos();

        // Seed at mid 1.0000 (book 0.9999/1.0001), t=0.
        let snap1 = book(Decimal::new(9999, 4), Decimal::new(10001, 4));
        let ctx1 = make_ctx(&symbol, &snap1, &p, &[], 0);
        let _ = s.on_event(
            &ctx1,
            &MarketEvent::BookUpdate {
                snapshot: snap1.clone(),
            },
        );

        // Book mid jumps to 1.0123 — OFF the 0.0030 grid (|Δ| 0.0123 > reach
        // 0.0060). Fallback snaps the anchor to the nearest lattice point 1.0120,
        // so the innermost ask is 1.0150 (= 1.0000 + 5·0.0030), NOT the off-grid
        // 1.0153 (= mid + step). The fixed lattice is honored.
        let snap2 = book(Decimal::new(10122, 4), Decimal::new(10124, 4));
        let ctx2 = make_ctx(&symbol, &snap2, &p, &[], 2_000_000_000);
        let actions = s.on_event(
            &ctx2,
            &MarketEvent::BookUpdate {
                snapshot: snap2.clone(),
            },
        );
        let prices: Vec<Decimal> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) => Some(q.price.0),
                _ => None,
            })
            .collect();
        assert!(
            prices.contains(&Decimal::new(10150, 4)),
            "innermost ask on the static grid (1.0150): {prices:?}"
        );
        assert!(
            prices.contains(&Decimal::new(10090, 4)),
            "innermost bid on the static grid (1.0090): {prices:?}"
        );
        assert!(
            !prices.contains(&Decimal::new(10153, 4)),
            "must NOT place the off-grid 1.0153: {prices:?}"
        );
        // Every emitted price sits exactly on origin + n·step.
        let origin = Decimal::new(10000, 4);
        let step = Decimal::new(30, 4);
        for p in &prices {
            let n = (*p - origin) / step;
            assert_eq!(n, n.round(), "price {p} must be on the lattice");
        }
    }

    // -----------------------------------------------------------------------
    // Test: persistence_skew_off_in_chop
    // gain=0.5, window=4, inner_steps=0, grid_levels=3, step_bps=0
    // (step=tick=0.0001), notional_per_order=10, mid 1.0000.
    // Alternating short/long (−40, +40, −40, +40) → signed avg ≈ 0 → no skew.
    // Innermost ask stays at 1.0001, innermost bid at 0.9999.
    // -----------------------------------------------------------------------
    #[test]
    fn persistence_skew_off_in_chop() {
        let mut c = cfg();
        c.inner_steps = 0;
        c.grid_levels = 3;
        c.step_bps = 0;
        c.notional_per_order = Decimal::from(10);
        c.inventory_skew = Decimal::new(5, 1); // 0.5
        c.inventory_skew_window = 4;
        c.tick_size = Decimal::new(1, 4); // 0.0001
        let mut s = Tide::new(c);
        let symbol = sym();

        // mid = 1.0000 (book 0.9999/1.0001)
        let snap = book(Decimal::new(9999, 4), Decimal::new(10001, 4));

        // Positions alternating short/long: size × mark / notional = ±4 rungs
        // avg over 4 samples → 0
        let positions = [
            Position {
                symbol: symbol.clone(),
                size: SignedSize(Decimal::from(-40)),
                avg_entry: Price(Decimal::new(9990, 4)),
                realized_pnl: Notional(Decimal::ZERO),
            },
            Position {
                symbol: symbol.clone(),
                size: SignedSize(Decimal::from(40)),
                avg_entry: Price(Decimal::new(10010, 4)),
                realized_pnl: Notional(Decimal::ZERO),
            },
            Position {
                symbol: symbol.clone(),
                size: SignedSize(Decimal::from(-40)),
                avg_entry: Price(Decimal::new(9990, 4)),
                realized_pnl: Notional(Decimal::ZERO),
            },
            Position {
                symbol: symbol.clone(),
                size: SignedSize(Decimal::from(40)),
                avg_entry: Price(Decimal::new(10010, 4)),
                realized_pnl: Notional(Decimal::ZERO),
            },
        ];

        let mut last_actions = Vec::new();
        for (i, p) in positions.iter().enumerate() {
            let now_ns = (i as u64 + 1) * 1_000_000_000; // 1s apart
            let ctx = make_ctx(&symbol, &snap, p, &[], now_ns);
            last_actions = s.on_event(
                &ctx,
                &MarketEvent::BookUpdate {
                    snapshot: snap.clone(),
                },
            );
        }

        let asks: Vec<Decimal> = last_actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Ask => Some(q.price.0),
                _ => None,
            })
            .collect();
        let bids: Vec<Decimal> = last_actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Bid => Some(q.price.0),
                _ => None,
            })
            .collect();

        // inner=1 (inner_steps=0 → inner=1), step=0.0001
        // innermost ask = center + 1*step = 1.0000 + 0.0001 = 1.0001
        // innermost bid = center − 1*step = 1.0000 − 0.0001 = 0.9999
        assert!(
            asks.contains(&Decimal::new(10001, 4)),
            "innermost ask must be 1.0001 (no skew): {asks:?}"
        );
        assert!(
            bids.contains(&Decimal::new(9999, 4)),
            "innermost bid must be 0.9999 (no skew): {bids:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Test: persistence_skew_engages_on_sustained_short
    // Same cfg as above. Feed ≥window reconciles with persistent short (−40).
    // avg ≈ −4 rungs → shift = round(0.5×4) = 2
    // Innermost ASK pushed out to 1.0003; innermost BID stays 0.9999.
    // -----------------------------------------------------------------------
    #[test]
    fn persistence_skew_engages_on_sustained_short() {
        let mut c = cfg();
        c.inner_steps = 0;
        c.grid_levels = 3;
        c.step_bps = 0;
        c.notional_per_order = Decimal::from(10);
        c.inventory_skew = Decimal::new(5, 1); // 0.5
        c.inventory_skew_window = 4;
        c.tick_size = Decimal::new(1, 4); // 0.0001
        let mut s = Tide::new(c);
        let symbol = sym();

        // mid = 1.0000
        let snap = book(Decimal::new(9999, 4), Decimal::new(10001, 4));

        // Persistent short: size=−40, mark=1.0000, notional=10 → bag_rungs=−4
        let short_pos = Position {
            symbol: symbol.clone(),
            size: SignedSize(Decimal::from(-40)),
            avg_entry: Price(Decimal::new(9990, 4)),
            realized_pnl: Notional(Decimal::ZERO),
        };

        let mut last_actions = Vec::new();
        for i in 0..5usize {
            // 5 > window=4 so window is fully filled with −4
            let now_ns = (i as u64 + 1) * 1_000_000_000;
            let ctx = make_ctx(&symbol, &snap, &short_pos, &[], now_ns);
            last_actions = s.on_event(
                &ctx,
                &MarketEvent::BookUpdate {
                    snapshot: snap.clone(),
                },
            );
        }

        let asks: Vec<Decimal> = last_actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Ask => Some(q.price.0),
                _ => None,
            })
            .collect();
        let bids: Vec<Decimal> = last_actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Bid => Some(q.price.0),
                _ => None,
            })
            .collect();

        // shift = round(0.5 × 4) = 2
        // ask_skew = 2 → innermost ask = center + (1 + 2)*step = 1.0003
        // bid_skew = 0 → innermost bid = center − 1*step = 0.9999 (unchanged)
        assert!(
            asks.contains(&Decimal::new(10003, 4)),
            "innermost ask must be 1.0003 (skew=2 on short): {asks:?}"
        );
        assert!(
            bids.contains(&Decimal::new(9999, 4)),
            "innermost bid must be 0.9999 (no bid skew on short): {bids:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Test: persistence_skew_window_bounds_history
    // window=2. Two longs then verify bid side is skewed; an earlier short
    // (outside the window) is forgotten.
    // -----------------------------------------------------------------------
    #[test]
    fn persistence_skew_window_bounds_history() {
        let mut c = cfg();
        c.inner_steps = 0;
        c.grid_levels = 3;
        c.step_bps = 0;
        c.notional_per_order = Decimal::from(10);
        c.inventory_skew = Decimal::new(5, 1); // 0.5
        c.inventory_skew_window = 2;
        c.tick_size = Decimal::new(1, 4); // 0.0001
        let mut s = Tide::new(c);
        let symbol = sym();

        let snap = book(Decimal::new(9999, 4), Decimal::new(10001, 4));

        let short_pos = Position {
            symbol: symbol.clone(),
            size: SignedSize(Decimal::from(-40)),
            avg_entry: Price(Decimal::new(9990, 4)),
            realized_pnl: Notional(Decimal::ZERO),
        };
        let long_pos = Position {
            symbol: symbol.clone(),
            size: SignedSize(Decimal::from(40)),
            avg_entry: Price(Decimal::new(10010, 4)),
            realized_pnl: Notional(Decimal::ZERO),
        };

        // t=1s: short (−4 rungs)
        let ctx0 = make_ctx(&symbol, &snap, &short_pos, &[], 1_000_000_000);
        let _ = s.on_event(
            &ctx0,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        // t=2s: long (+4 rungs) — window now [−4, +4]
        let ctx1 = make_ctx(&symbol, &snap, &long_pos, &[], 2_000_000_000);
        let _ = s.on_event(
            &ctx1,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        // t=3s: long (+4 rungs) — window evicts short, now [+4, +4]
        let ctx2 = make_ctx(&symbol, &snap, &long_pos, &[], 3_000_000_000);
        let last_actions = s.on_event(
            &ctx2,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );

        let bids: Vec<Decimal> = last_actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Bid => Some(q.price.0),
                _ => None,
            })
            .collect();
        let asks: Vec<Decimal> = last_actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Ask => Some(q.price.0),
                _ => None,
            })
            .collect();

        // avg over window [+4, +4] = +4 → shift = round(0.5×4) = 2
        // bid_skew = 2 → innermost bid = center − (1+2)*step = 0.9997
        // ask_skew = 0 → innermost ask = center + 1*step = 1.0001
        assert!(
            bids.contains(&Decimal::new(9997, 4)),
            "innermost bid must be 0.9997 (bid skew=2 on sustained long): {bids:?}"
        );
        assert!(
            asks.contains(&Decimal::new(10001, 4)),
            "innermost ask must be 1.0001 (no ask skew when long): {asks:?}"
        );
    }

    #[test]
    fn position_cap_suppresses_bids() {
        let mut c = cfg();
        // Small cap: 1 USDT. Long position well above it.
        c.max_position_usdt = Decimal::from(1);
        let mut s = Tide::new(c);

        let snap = book(Decimal::new(100, 4), Decimal::new(102, 4));
        let symbol = sym();

        // Long position: 20000 units × 0.0101 ≈ 202 USDT > 1 USDT cap.
        let long_pos = Position {
            symbol: symbol.clone(),
            size: SignedSize(Decimal::from(20_000)),
            avg_entry: Price(Decimal::new(100, 4)),
            realized_pnl: Notional(Decimal::ZERO),
        };

        // Stray ask far above lattice — should still be cancelled.
        let stray_ask_id = QuoteId::new();
        let open = vec![(
            stray_ask_id,
            make_intent(&symbol, Side::Ask, Decimal::new(200, 4)), // way off lattice
        )];

        let ctx = make_ctx(&symbol, &snap, &long_pos, &open, 0);
        let actions = s.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );

        // No bid Quotes emitted (suppressed by cap).
        let bid_adds: Vec<_> = actions
            .iter()
            .filter(|a| matches!(a, Action::Quote(q) if q.side == Side::Bid))
            .collect();
        assert!(bid_adds.is_empty(), "bids suppressed by cap: {bid_adds:?}");

        // Ask Quotes ARE emitted (reducing side not suppressed).
        let ask_adds: Vec<_> = actions
            .iter()
            .filter(|a| matches!(a, Action::Quote(q) if q.side == Side::Ask))
            .collect();
        assert!(
            !ask_adds.is_empty(),
            "asks still added when long-capped: {ask_adds:?}"
        );

        // Stray ask still cancelled.
        let cancelled: Vec<QuoteId> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Cancel(id) => Some(*id),
                _ => None,
            })
            .collect();
        assert!(
            cancelled.contains(&stray_ask_id),
            "stray ask must be cancelled: {cancelled:?}"
        );
    }
}
