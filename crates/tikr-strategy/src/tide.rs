//! Fill-driven sliding ladder market-making strategy.
//!
//! Tide maintains a fixed-step ladder that slides on each full fill.
//! No book-tick re-quoting after seeding — grid only moves when an order
//! fills. On side-exhaustion (one side wiped out), cancels everything and
//! re-seeds around the current mid.

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
    /// Unused in fill-driven lattice. Kept for config compat.
    pub prune_stragglers: bool,
    /// Unused in fill-driven lattice. Kept for config compat.
    pub recenter_bps: u32,
    /// Unused in fill-driven lattice. Kept for config compat.
    pub recenter_secs: u32,
    /// Skip the inner rungs: the top order on each side is held at least
    /// `inner_steps × lattice_step` away from the current mid (a dead zone
    /// around mid). `0` (default) = top order at the self-spread.
    pub inner_steps: u32,
    /// Unused in fill-driven lattice. Kept for config compat.
    pub chase: bool,
    /// Unused in fill-driven lattice. Kept for config compat.
    pub chase_to_avg: bool,
    /// Unused in fill-driven lattice. Kept for config compat.
    pub relattice_timeout_secs: u32,
}

/// Fill-driven sliding ladder strategy state.
pub struct Tide {
    config: TideConfig,
    /// Frozen lattice step. Set on first event with a usable book;
    /// cleared on side-exhaustion rebuild to force recompute.
    lattice_step: Option<Decimal>,
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

    /// Seed the ladder around `mid`: set `lattice_step`, emit `grid_levels`
    /// buys and `grid_levels` asks at geometry prices.
    ///
    /// Geometry: center = round(mid/tick)*tick
    ///   bid[k] = center − (inner_steps+1+k)·step  for k in 0..grid_levels
    ///   ask[k] = center + (inner_steps+1+k)·step  for k in 0..grid_levels
    fn seed(&mut self, ctx: &StrategyContext<'_>, mid: Decimal) -> Vec<Action> {
        let step = self.compute_step(mid);
        self.lattice_step = Some(step);

        let tick = self.config.tick_size;
        let center = if tick > Decimal::ZERO {
            (mid / tick).round() * tick
        } else {
            mid
        };

        let levels = self.config.grid_levels.max(1);
        let inner = Decimal::from(self.config.inner_steps + 1);

        let pos_size = ctx.position.size.0;
        let cap = self.config.max_position_usdt;
        let pos_notional = pos_size * mid;
        let suppress_bids = cap > Decimal::ZERO && pos_notional >= cap;
        let suppress_asks = cap > Decimal::ZERO && -pos_notional >= cap;

        let mut actions = Vec::with_capacity((levels * 2) as usize);

        for k in 0..levels {
            let offset = (inner + Decimal::from(k)) * step;
            let bid_price = center - offset;
            let ask_price = center + offset;

            if !suppress_bids && bid_price > Decimal::ZERO {
                actions.push(self.make_quote(ctx.symbol, Side::Bid, Price(bid_price)));
            }
            if !suppress_asks && ask_price > Decimal::ZERO {
                actions.push(self.make_quote(ctx.symbol, Side::Ask, Price(ask_price)));
            }
        }

        actions
    }

    /// Handle a full fill: slide the ladder one step in the fill direction,
    /// or rebuild if one side is exhausted.
    fn on_full_fill(&mut self, ctx: &StrategyContext<'_>, fill: &tikr_core::Fill) -> Vec<Action> {
        let mid_opt = Self::book_mid(ctx.latest_book);
        let step = match self.lattice_step {
            Some(s) => s,
            None => return Vec::new(),
        };

        // Collect resting bid and ask prices + ids from open_quotes.
        let bid_orders: Vec<(tikr_venue::QuoteId, Decimal)> = ctx
            .open_quotes
            .iter()
            .filter(|(_, q)| q.side == Side::Bid)
            .map(|(id, q)| (*id, q.price.0))
            .collect();
        let ask_orders: Vec<(tikr_venue::QuoteId, Decimal)> = ctx
            .open_quotes
            .iter()
            .filter(|(_, q)| q.side == Side::Ask)
            .map(|(id, q)| (*id, q.price.0))
            .collect();

        // Side-exhaustion rebuild: if either side is empty, cancel all and re-seed.
        if bid_orders.is_empty() || ask_orders.is_empty() {
            self.lattice_step = None;
            let Some(mid) = mid_opt else {
                return vec![Action::CancelAll];
            };
            let mut actions = vec![Action::CancelAll];
            actions.extend(self.seed(ctx, mid));
            return actions;
        }

        let mid = mid_opt.unwrap_or(fill.price.0);

        let pos_size = ctx.position.size.0;
        let cap = self.config.max_position_usdt;
        let pos_notional = pos_size * mid;
        let suppress_bids = cap > Decimal::ZERO && pos_notional >= cap;
        let suppress_asks = cap > Decimal::ZERO && -pos_notional >= cap;

        let mut actions = Vec::with_capacity(3);

        match fill.side {
            Side::Ask => {
                // SELL fill — slide UP.
                // (a) new sell at far edge = max(ask prices) + step
                let max_ask = ask_orders
                    .iter()
                    .map(|(_, p)| *p)
                    .fold(Decimal::MIN, Decimal::max);
                let new_ask_price = max_ask + step;
                if !suppress_asks && new_ask_price > Decimal::ZERO {
                    actions.push(self.make_quote(ctx.symbol, Side::Ask, Price(new_ask_price)));
                }
                // (b) new buy = max(bid prices) + step
                let max_bid = bid_orders
                    .iter()
                    .map(|(_, p)| *p)
                    .fold(Decimal::MIN, Decimal::max);
                let new_bid_price = max_bid + step;
                if !suppress_bids && new_bid_price > Decimal::ZERO {
                    actions.push(self.make_quote(ctx.symbol, Side::Bid, Price(new_bid_price)));
                }
                // (c) cancel furthest buy = bid with MIN price
                let min_bid_id = bid_orders
                    .iter()
                    .min_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
                    .map(|(id, _)| *id);
                if let Some(id) = min_bid_id {
                    actions.push(Action::Cancel(id));
                }
            }
            Side::Bid => {
                // BUY fill — slide DOWN.
                // (a) new buy at far edge = min(bid prices) − step
                let min_bid = bid_orders
                    .iter()
                    .map(|(_, p)| *p)
                    .fold(Decimal::MAX, Decimal::min);
                let new_bid_price = min_bid - step;
                if !suppress_bids && new_bid_price > Decimal::ZERO {
                    actions.push(self.make_quote(ctx.symbol, Side::Bid, Price(new_bid_price)));
                }
                // (b) new sell = min(ask prices) − step
                let min_ask = ask_orders
                    .iter()
                    .map(|(_, p)| *p)
                    .fold(Decimal::MAX, Decimal::min);
                let new_ask_price = min_ask - step;
                if !suppress_asks && new_ask_price > Decimal::ZERO {
                    actions.push(self.make_quote(ctx.symbol, Side::Ask, Price(new_ask_price)));
                }
                // (c) cancel furthest sell = ask with MAX price
                let max_ask_id = ask_orders
                    .iter()
                    .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
                    .map(|(id, _)| *id);
                if let Some(id) = max_ask_id {
                    actions.push(Action::Cancel(id));
                }
            }
        }

        actions
    }
}

impl Strategy for Tide {
    type Config = TideConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            lattice_step: None,
        }
    }

    fn name(&self) -> &str {
        "tide"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        match event {
            MarketEvent::Fill(fill) if fill.is_full && self.lattice_step.is_some() => {
                self.on_full_fill(ctx, fill)
            }
            _ => {
                // Not a full fill: seed if we haven't yet.
                if self.lattice_step.is_none()
                    && let Some(mid) = Self::book_mid(ctx.latest_book)
                {
                    return self.seed(ctx, mid);
                }
                Vec::new()
            }
        }
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

    fn cfg() -> TideConfig {
        TideConfig {
            notional_per_order: Decimal::from(10),
            tick_size: Decimal::new(1, 4), // 0.0001
            step_size: Decimal::ONE,
            min_notional: Decimal::from(5),
            grid_levels: 1,
            step_bps: 0,
            max_position_usdt: Decimal::ZERO,
            prune_stragglers: true,
            recenter_bps: 0,
            recenter_secs: 0,
            inner_steps: 0,
            chase: false,
            chase_to_avg: false,
            relattice_timeout_secs: 300,
        }
    }

    fn make_ctx<'a>(
        symbol: &'a Symbol,
        snap: &'a Snapshot,
        position: &'a Position,
        open: &'a [(QuoteId, QuoteIntent)],
    ) -> StrategyContext<'a> {
        StrategyContext {
            symbol,
            now: Timestamp(1),
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

    // -----------------------------------------------------------------------
    // Test 1: seed geometry
    // inner_steps=1, grid_levels=3, book 0.0100/0.0102
    // center = round(0.0101/0.0001)*0.0001 = 0.0101
    // innermost offset = (inner_steps+1)*step = 2*0.0001 = 0.0002
    // bids: 0.0101-0.0002=0.0099, 0.0101-0.0003=0.0098, 0.0101-0.0004=0.0097
    //   wait — k=0: center-(1+0)*step=0.0101-0.0002=0.0099
    //          k=1: 0.0101-0.0003=0.0098
    //          k=2: 0.0101-0.0004=0.0097  ... hmm but spec says 0.0098,0.0097,0.0096
    // Let me re-read: inner_steps=1 → inner_steps+1=2 → innermost bid = center-2*step
    // center=0.0101, step=0.0001: innermost bid = 0.0101-2*0.0001 = 0.0099
    // k=0: 0.0099, k=1: 0.0098, k=2: 0.0097
    // But spec says bids at 0.0098,0.0097,0.0096 (innermost at ±(inner_steps+1)=±3 steps)
    // Re-reading spec: "inner_steps=1, grid_levels=3" → innermost at ±(inner_steps+1) = ±2 steps
    // "assert exactly 3 bids at 0.0098,0.0097,0.0096 and 3 asks at 0.0104,0.0105,0.0106
    //  (innermost at ±(inner_steps+1)=±3 steps; gap 6 steps)"
    // So spec says ±3 steps for inner_steps=1... that would be inner_steps+2 = 3? Doesn't match.
    // Wait: spec parenthetical says "(innermost at ±(inner_steps+1)=±3 steps)" with inner_steps=1
    // ±(inner_steps+1) = ±(1+1) = ±2, not ±3. But the expected prices are at distance 3 from center.
    // 0.0101 - 0.0098 = 0.0003 = 3 steps. So innermost is at inner_steps+2=3 steps?
    // That doesn't match the formula. Let's re-read the spec formula:
    // "bid prices = center − (inner_steps+1+k)·step for k in 0..grid_levels"
    // inner_steps=1, k=0: center-(1+1+0)*step = center-2*step = 0.0101-0.0002=0.0099
    // But spec says 0.0098 is the INNERMOST bid. 0.0101-0.0098=0.0003=3 steps.
    // So the expected values in test 1 conflict with the formula unless inner_steps=2 is intended.
    // The spec test 1 says: "inner_steps=1, grid_levels=3" but bids at 0.0098/0.0097/0.0096 implies
    // innermost at 3 steps from center. The spec also says "innermost at ±(inner_steps+1)=±3 steps"
    // which would mean inner_steps=2. This appears to be a typo in the spec — the parenthetical
    // "(innermost at ±(inner_steps+1)=±3 steps)" with inner_steps=1 would give ±2, not ±3.
    // RESOLUTION: The spec's existing test `inner_steps_2_lattice_geometry` (inner_steps=2, grid_levels=3)
    // produces bids at 0.0098,0.0097,0.0096 which is correct for the formula (center=0.0101,
    // innermost at (2+1)*step=3 steps). The test 1 spec text "inner_steps=1" combined with those prices
    // is inconsistent. I'll implement test 1 with inner_steps=2 to match the stated prices.
    // -----------------------------------------------------------------------

    #[test]
    fn seed_geometry() {
        // inner_steps=2, grid_levels=3, book 0.0100/0.0102
        // center = 0.0101, step = 0.0001
        // k=0: bid=0.0101-(2+1+0)*0.0001=0.0098, ask=0.0104
        // k=1: bid=0.0097, ask=0.0105
        // k=2: bid=0.0096, ask=0.0106
        let mut c = cfg();
        c.inner_steps = 2;
        c.grid_levels = 3;
        let mut s = Tide::new(c);
        let snap = book(Decimal::new(100, 4), Decimal::new(102, 4));
        let p = pos();
        let symbol = sym();
        let ctx = make_ctx(&symbol, &snap, &p, &[]);
        let actions = s.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        // All actions are Quote
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
        assert_eq!(bids.len(), 3, "3 bids: {bids:?}");
        assert_eq!(asks.len(), 3, "3 asks: {asks:?}");
        for px in [98u32, 97, 96] {
            assert!(
                bids.contains(&Decimal::new(px as i64, 4)),
                "bid {px}: {bids:?}"
            );
        }
        for px in [104u32, 105, 106] {
            assert!(
                asks.contains(&Decimal::new(px as i64, 4)),
                "ask {px}: {asks:?}"
            );
        }
        // Dead zone: 99..103 must have no orders
        for vacant in [99u32, 100, 101, 102, 103] {
            let v = Decimal::new(vacant as i64, 4);
            assert!(
                !bids.contains(&v) && !asks.contains(&v),
                "slot {vacant} must be empty"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Test 2: no re-quote on book ticks after seed
    // -----------------------------------------------------------------------

    #[test]
    fn no_requote_on_book_tick_after_seed() {
        let mut s = Tide::new(cfg());
        let snap = book(Decimal::new(100, 4), Decimal::new(102, 4));
        let p = pos();
        let symbol = sym();
        // First BookUpdate — seeds
        let ctx1 = make_ctx(&symbol, &snap, &p, &[]);
        let a1 = s.on_event(
            &ctx1,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        assert!(!a1.is_empty(), "first event should seed");
        // Second BookUpdate — lattice_step set, no fill → empty
        let snap2 = book(Decimal::new(101, 4), Decimal::new(103, 4));
        let ctx2 = make_ctx(&symbol, &snap2, &p, &[]);
        let a2 = s.on_event(
            &ctx2,
            &MarketEvent::BookUpdate {
                snapshot: snap2.clone(),
            },
        );
        assert!(a2.is_empty(), "no actions on second book tick: {a2:?}");
    }

    // -----------------------------------------------------------------------
    // Test 3: sell-fill slides up
    // Seed inner_steps=2, grid_levels=3 at center 0.0101
    // bids: 0.0098, 0.0097, 0.0096 (bids[0]=0.0098, bids[2]=0.0096)
    // asks: 0.0104, 0.0105, 0.0106
    // SELL fill → slide up:
    //   new ask = max(asks)+step = 0.0106+0.0001 = 0.0107
    //   new bid = max(bids)+step = 0.0098+0.0001 = 0.0099
    //   cancel  = bid with min price = 0.0096's id
    // -----------------------------------------------------------------------

    fn make_open_quotes_for_seeded_ladder(symbol: &Symbol) -> Vec<(QuoteId, QuoteIntent)> {
        // bids at 0.0098, 0.0097, 0.0096 / asks at 0.0104, 0.0105, 0.0106
        let mut open = Vec::new();
        for px in [98i64, 97, 96] {
            open.push((
                QuoteId::new(),
                QuoteIntent {
                    symbol: symbol.clone(),
                    side: Side::Bid,
                    price: Price(Decimal::new(px, 4)),
                    size: Size(Decimal::ONE),
                    tif: TimeInForce::PostOnly,
                    kind: QuoteKind::Point,
                },
            ));
        }
        for px in [104i64, 105, 106] {
            open.push((
                QuoteId::new(),
                QuoteIntent {
                    symbol: symbol.clone(),
                    side: Side::Ask,
                    price: Price(Decimal::new(px, 4)),
                    size: Size(Decimal::ONE),
                    tif: TimeInForce::PostOnly,
                    kind: QuoteKind::Point,
                },
            ));
        }
        open
    }

    #[test]
    fn sell_fill_slides_up() {
        let mut c = cfg();
        c.inner_steps = 2;
        c.grid_levels = 3;
        let mut s = Tide::new(c);
        // Force lattice_step to be set (pretend already seeded)
        s.lattice_step = Some(Decimal::new(1, 4));

        let symbol = sym();
        let snap = book(Decimal::new(100, 4), Decimal::new(102, 4));
        let p = pos();
        let open = make_open_quotes_for_seeded_ladder(&symbol);
        // Id of the bid at 0.0096 (should be cancelled)
        let min_bid_id = open
            .iter()
            .find(|(_, q)| q.side == Side::Bid && q.price.0 == Decimal::new(96, 4))
            .map(|(id, _)| *id)
            .expect("bid at 0.0096 must be in open_quotes");

        let ctx = make_ctx(&symbol, &snap, &p, &open);
        let fill = mk_fill(Side::Ask, Decimal::new(104, 4), true);
        let actions = s.on_event(&ctx, &MarketEvent::Fill(fill));

        // Quote actions must come before Cancel
        let first_cancel_pos = actions.iter().position(|a| matches!(a, Action::Cancel(_)));
        let last_quote_pos = actions.iter().rposition(|a| matches!(a, Action::Quote(_)));
        if let (Some(cpos), Some(qpos)) = (first_cancel_pos, last_quote_pos) {
            assert!(
                qpos < cpos,
                "all Quote actions must precede Cancel: {actions:?}"
            );
        }

        // New ask at 0.0107
        let new_asks: Vec<Decimal> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Ask => Some(q.price.0),
                _ => None,
            })
            .collect();
        assert!(
            new_asks.contains(&Decimal::new(107, 4)),
            "new ask at 0.0107: {new_asks:?}"
        );

        // New bid at 0.0099
        let new_bids: Vec<Decimal> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Bid => Some(q.price.0),
                _ => None,
            })
            .collect();
        assert!(
            new_bids.contains(&Decimal::new(99, 4)),
            "new bid at 0.0099: {new_bids:?}"
        );

        // Cancel of min bid (0.0096)
        let cancelled: Vec<QuoteId> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Cancel(id) => Some(*id),
                _ => None,
            })
            .collect();
        assert!(
            cancelled.contains(&min_bid_id),
            "cancel of bid 0.0096: {actions:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Test 4: buy-fill slides down (mirror of test 3)
    // BUY fill → slide down:
    //   new bid = min(bids)−step = 0.0096−0.0001 = 0.0095
    //   new ask = min(asks)−step = 0.0104−0.0001 = 0.0103
    //   cancel  = ask with max price = 0.0106's id
    // -----------------------------------------------------------------------

    #[test]
    fn buy_fill_slides_down() {
        let mut c = cfg();
        c.inner_steps = 2;
        c.grid_levels = 3;
        let mut s = Tide::new(c);
        s.lattice_step = Some(Decimal::new(1, 4));

        let symbol = sym();
        let snap = book(Decimal::new(100, 4), Decimal::new(102, 4));
        let p = pos();
        let open = make_open_quotes_for_seeded_ladder(&symbol);
        let max_ask_id = open
            .iter()
            .find(|(_, q)| q.side == Side::Ask && q.price.0 == Decimal::new(106, 4))
            .map(|(id, _)| *id)
            .expect("ask at 0.0106 must be in open_quotes");

        let ctx = make_ctx(&symbol, &snap, &p, &open);
        let fill = mk_fill(Side::Bid, Decimal::new(98, 4), true);
        let actions = s.on_event(&ctx, &MarketEvent::Fill(fill));

        // New bid at 0.0095
        let new_bids: Vec<Decimal> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Bid => Some(q.price.0),
                _ => None,
            })
            .collect();
        assert!(
            new_bids.contains(&Decimal::new(95, 4)),
            "new bid at 0.0095: {new_bids:?}"
        );

        // New ask at 0.0103
        let new_asks: Vec<Decimal> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Ask => Some(q.price.0),
                _ => None,
            })
            .collect();
        assert!(
            new_asks.contains(&Decimal::new(103, 4)),
            "new ask at 0.0103: {new_asks:?}"
        );

        // Cancel of max ask (0.0106)
        let cancelled: Vec<QuoteId> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Cancel(id) => Some(*id),
                _ => None,
            })
            .collect();
        assert!(
            cancelled.contains(&max_ask_id),
            "cancel of ask 0.0106: {actions:?}"
        );

        // Quote actions before Cancel
        let first_cancel_pos = actions.iter().position(|a| matches!(a, Action::Cancel(_)));
        let last_quote_pos = actions.iter().rposition(|a| matches!(a, Action::Quote(_)));
        if let (Some(cpos), Some(qpos)) = (first_cancel_pos, last_quote_pos) {
            assert!(qpos < cpos, "Quotes before Cancel: {actions:?}");
        }
    }

    // -----------------------------------------------------------------------
    // Test 5: side-exhaustion rebuild
    // open_quotes has bids but ZERO asks; full Fill{side:Ask} → rebuild
    // -----------------------------------------------------------------------

    #[test]
    fn side_exhaustion_rebuild() {
        let mut c = cfg();
        c.inner_steps = 0;
        c.grid_levels = 1;
        let mut s = Tide::new(c);
        s.lattice_step = Some(Decimal::new(1, 4));

        let symbol = sym();
        let snap = book(Decimal::new(100, 4), Decimal::new(102, 4));
        let p = pos();

        // Only bids, no asks
        let open = vec![(
            QuoteId::new(),
            QuoteIntent {
                symbol: symbol.clone(),
                side: Side::Bid,
                price: Price(Decimal::new(99, 4)),
                size: Size(Decimal::ONE),
                tif: TimeInForce::PostOnly,
                kind: QuoteKind::Point,
            },
        )];

        let ctx = make_ctx(&symbol, &snap, &p, &open);
        let fill = mk_fill(Side::Ask, Decimal::new(101, 4), true);
        let actions = s.on_event(&ctx, &MarketEvent::Fill(fill));

        // First action must be CancelAll
        assert!(
            matches!(actions.first(), Some(Action::CancelAll)),
            "first action must be CancelAll: {actions:?}"
        );

        // Remaining actions are Quotes (re-seed)
        let quotes: Vec<_> = actions
            .iter()
            .skip(1)
            .filter(|a| matches!(a, Action::Quote(_)))
            .collect();
        assert!(!quotes.is_empty(), "re-seed must emit quotes: {actions:?}");
    }

    // -----------------------------------------------------------------------
    // Test 6: position cap stops the growing side
    // Long position >= max_position_usdt → no new bid quotes on sell-fill slide.
    // The far-sell quote and the buy-cancel still happen (cancel is always allowed).
    // -----------------------------------------------------------------------

    #[test]
    fn position_cap_stops_growing_side() {
        let mut c = cfg();
        c.inner_steps = 2;
        c.grid_levels = 3;
        // Cap at 1 USDT. pos_size * mid must be >= 1.
        c.max_position_usdt = Decimal::from(1);
        let mut s = Tide::new(c);
        s.lattice_step = Some(Decimal::new(1, 4));

        let symbol = sym();
        // mid = (0.0100+0.0102)/2 = 0.0101
        let snap = book(Decimal::new(100, 4), Decimal::new(102, 4));

        // pos_size = 20000 → pos_notional = 20000 * 0.0101 = 202 >= 1 (capped long)
        let long_pos = Position {
            symbol: symbol.clone(),
            size: SignedSize(Decimal::from(20_000)),
            avg_entry: Price(Decimal::new(100, 4)),
            realized_pnl: Notional(Decimal::ZERO),
        };

        let open = make_open_quotes_for_seeded_ladder(&symbol);
        let ctx = make_ctx(&symbol, &snap, &long_pos, &open);
        let fill = mk_fill(Side::Ask, Decimal::new(104, 4), true);
        let actions = s.on_event(&ctx, &MarketEvent::Fill(fill));

        // No new bid quotes (long capped)
        let new_bids: Vec<Decimal> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Bid => Some(q.price.0),
                _ => None,
            })
            .collect();
        assert!(
            new_bids.is_empty(),
            "long capped → no new bid: {new_bids:?}"
        );

        // New sell quote at far edge IS emitted (reducing side not suppressed)
        let new_asks: Vec<Decimal> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Ask => Some(q.price.0),
                _ => None,
            })
            .collect();
        assert!(
            new_asks.contains(&Decimal::new(107, 4)),
            "far-sell quote still emitted: {new_asks:?}"
        );

        // Cancel of furthest bid still present
        let cancels: Vec<_> = actions
            .iter()
            .filter(|a| matches!(a, Action::Cancel(_)))
            .collect();
        assert!(
            !cancels.is_empty(),
            "cancel still emitted when capped: {actions:?}"
        );
    }
}
