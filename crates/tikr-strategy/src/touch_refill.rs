//! Minimal at-touch market-making strategy. Two rules:
//!
//! 1. **Best-price maintenance**: always have ≥1 order at the current
//!    `top.bid` and ≥1 at `top.ask`. If either side is missing such an
//!    order, place one.
//! 2. **Close-on-fill**: when a fill lands at price `P`, immediately
//!    place an opposite-side order at `P ± 1 tick` (the 1-tick profit
//!    target for that just-opened position).
//!
//! **NEVER cancels.** Each placed order is left alone until it fills
//! or is canceled externally. Stale orders just sit in the book
//! (post-only, no fee while resting). This means inventory CAN grow
//! unbounded if the market trends — there's no cap or stop.
//!
//! Suited to wide-tick perps where `tick_bps > 2 × maker_fee_bps` so
//! each completed round-trip clears fees. See ESPORTS (~20bps tick).
//!
//! Inventory risk: when your bid fills repeatedly during a down-move,
//! you'll accumulate longs. The close orders at fill+1 tick will not
//! fill until the market reverts. This strategy is a pure
//! "spread > 2×fees" bet — the operator owns the inventory risk.

use std::collections::BTreeSet;

use tikr_core::{Decimal, MarketEvent, Price, QuoteKind, Side, Size, Symbol, TimeInForce};
use tikr_venue::QuoteIntent;

use crate::{Action, Strategy, StrategyContext};

/// Configuration for [`TouchRefill`].
#[derive(Debug, Clone)]
pub struct TouchRefillConfig {
    /// Notional USDT per order. Quantity = `notional / price`, floored
    /// to `step_size`, bumped to meet `min_notional`.
    pub notional_per_order: Decimal,
    /// Venue tick size. Used for the close-on-fill +/- 1 tick offset
    /// and for grid level spacing.
    pub tick_size: Decimal,
    /// Venue lot step.
    pub step_size: Decimal,
    /// Venue min order notional.
    pub min_notional: Decimal,
    /// Grid depth per side. `1` (default) = classic single-level
    /// at-touch. `N > 1` places orders at `best_bid − i × tick` for
    /// `i ∈ [0, N)` on the bid side, and `best_ask + i × tick` on the
    /// ask side. Defends against price jumps that would otherwise leave
    /// the bot unfilled and chasing — with N=12, a 10-tick jump still
    /// leaves the bot with orders in the path. Inventory cap scales
    /// linearly: max position = N × notional_per_order per side.
    pub grid_levels: u32,
    /// Minimum required spread (in bps of mid) between the top of the
    /// bid grid and the top of the ask grid. When the book spread is
    /// wider than this, both tops sit at their touches (no change).
    /// When the book spread is narrower, BOTH tops are pushed apart
    /// (bid down, ask up) symmetrically around mid so the gap meets
    /// the requirement. `0` (default) = disabled (always at touch).
    ///
    /// Use to make TouchRefill viable on tight-spread / narrow-tick
    /// markets where the natural book spread alone wouldn't cover
    /// 2× maker fees (~3.6 bps RT on BNB-discount Binance USD-M).
    pub min_self_spread_bps: u32,
    /// Profit target for close-on-fill orders, in bps of fill price.
    /// When `> 0`, every close order placed in response to a full
    /// fill sits exactly this many bps away from the fill price
    /// (snapped up to nearest tick, minimum 1 tick). When `0`, the
    /// close distance falls back to `min_self_spread_bps` (or 1 tick,
    /// whichever is larger). Set higher than `min_self_spread_bps` to
    /// capture more profit per round-trip at the cost of slower fills.
    pub close_profit_bps: u32,
    /// Spacing between grid levels in bps of mid. Effective spacing =
    /// max(1 tick, ceil(grid_step_bps × mid / 10000 / tick) × tick).
    /// `0` = legacy 1-tick spacing. On tight-tick markets (e.g.
    /// ETHUSDC where 1 tick ≈ 0.005 bps), 1-tick spacing piles dozens
    /// of orders within sub-bps; setting `grid_step_bps = 4` spaces
    /// them ~4 bps apart for meaningful fill independence.
    pub grid_step_bps: u32,
    /// Per-bot peak position cap in USDT notional. When long
    /// notional > cap, BID emits are suppressed (no more accumulation
    /// on the long side); when short notional > cap, ASK emits are
    /// suppressed. Close-on-fill orders are NEVER suppressed since
    /// they reduce position. `0` = no cap (legacy behavior).
    pub max_position_usdt: Decimal,
}

/// Strategy state. Tracks intents emitted but not yet confirmed via
/// `ctx.open_quotes` to avoid double-emitting in a single cycle.
pub struct TouchRefill {
    config: TouchRefillConfig,
    /// Prices we've emitted Quote intents for this cycle, used to
    /// dedupe within `on_event` before the runner has dispatched +
    /// fill_sim has registered them. Cleared at the start of each
    /// `on_event` call.
    pending_bid_prices: BTreeSet<Decimal>,
    pending_ask_prices: BTreeSet<Decimal>,
    /// Historic deepest BID grid floor. When the book drops, the grid
    /// extends down to follow it; when it rises, the floor stays put
    /// (existing deep bids keep resting). `None` until the first event.
    bid_grid_floor: Option<Decimal>,
    /// Historic highest ASK grid ceiling — mirror of `bid_grid_floor`.
    ask_grid_ceiling: Option<Decimal>,
}

impl TouchRefill {
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
        // Bump to min_notional if needed.
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

    /// Does the venue (per fill_sim) OR this cycle's pending set
    /// already hold an order on `side` within `tolerance` of `price`?
    ///
    /// Exact-price matching breaks on tight-tick markets: prior
    /// close-on-fill orders sit at arbitrary fill prices, and grid
    /// emits at step boundaries. A fresh emit one tick off an
    /// existing order would create a duplicate. With tolerance set
    /// to `step / 2`, any existing order within half a step "covers"
    /// the requested level and the emit is skipped.
    fn already_have_order(
        &self,
        ctx: &StrategyContext<'_>,
        side: Side,
        price: Price,
        tolerance: Decimal,
    ) -> bool {
        let pending = match side {
            Side::Bid => &self.pending_bid_prices,
            Side::Ask => &self.pending_ask_prices,
        };
        let lo = price.0 - tolerance;
        let hi = price.0 + tolerance;
        if pending.range(lo..=hi).next().is_some() {
            return true;
        }
        ctx.open_quotes
            .iter()
            .any(|(_, q)| q.side == side && q.price.0 >= lo && q.price.0 <= hi)
    }

    fn emit(&mut self, symbol: &Symbol, side: Side, price: Price) -> Action {
        match side {
            Side::Bid => {
                self.pending_bid_prices.insert(price.0);
            }
            Side::Ask => {
                self.pending_ask_prices.insert(price.0);
            }
        }
        self.make_quote(symbol, side, price)
    }
}

impl Strategy for TouchRefill {
    type Config = TouchRefillConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            pending_bid_prices: BTreeSet::new(),
            pending_ask_prices: BTreeSet::new(),
            bid_grid_floor: None,
            ask_grid_ceiling: None,
        }
    }

    fn name(&self) -> &str {
        "touch-refill"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        // Pending sets are per-event dedupe only; clear at the top.
        self.pending_bid_prices.clear();
        self.pending_ask_prices.clear();

        let mut actions: Vec<Action> = Vec::new();

        // Rule 1: maintain a grid that extends in both directions as the
        // book moves. Each side's grid spans from the current touch
        // outward by at least `grid_levels` ticks, AND keeps any deeper
        // levels it has accumulated from past book moves.
        //
        // - BID floor: min(historic floor, current best_bid − (N−1)×tick).
        //   When best_bid drops, the floor drops with it (extend down).
        //   When best_bid rises, the floor stays put (existing deep bids
        //   keep resting).
        // - ASK ceiling: max(historic ceiling, current best_ask + (N−1)×tick).
        //   Mirror behavior for the ask side.
        //
        // Then place at every tick from current touch out to the
        // historic extreme. Existing orders are deduped via
        // `already_have_order`. Never cancels — stale levels just
        // sit in the book until they fill.
        let levels = self.config.grid_levels.max(1);
        let tick = self.config.tick_size;
        let best_bid = ctx.latest_book.bids.first().map(|l| l.price);
        let best_ask = ctx.latest_book.asks.first().map(|l| l.price);

        // Per-side cap: when long notional > cap, no more BID emits
        // (close-side ASK emits still fire). Mirror for shorts.
        let pos_size = ctx.position.size.0;
        let cap = self.config.max_position_usdt;
        let mid_for_pos = match (best_bid, best_ask) {
            (Some(b), Some(a)) if a.0 > Decimal::ZERO && b.0 > Decimal::ZERO => {
                (b.0 + a.0) / Decimal::from(2)
            }
            (Some(b), _) if b.0 > Decimal::ZERO => b.0,
            (_, Some(a)) if a.0 > Decimal::ZERO => a.0,
            _ => Decimal::ZERO,
        };
        let pos_notional = pos_size * mid_for_pos;
        let suppress_bids = cap > Decimal::ZERO && pos_notional > cap;
        let suppress_asks = cap > Decimal::ZERO && pos_notional < -cap;

        // Effective grid step = max(1 tick, ceil(grid_step_bps × mid /
        // 10000 / tick) × tick). On tight-tick markets, 1-tick spacing
        // piles dozens of orders within sub-bps; grid_step_bps spaces
        // them meaningfully apart.
        let mid_for_step = match (best_bid, best_ask) {
            (Some(b), Some(a)) if a.0 > b.0 && b.0 > Decimal::ZERO => {
                (b.0 + a.0) / Decimal::from(2)
            }
            (Some(b), _) if b.0 > Decimal::ZERO => b.0,
            (_, Some(a)) if a.0 > Decimal::ZERO => a.0,
            _ => Decimal::ZERO,
        };
        let step = if self.config.grid_step_bps > 0
            && mid_for_step > Decimal::ZERO
            && tick > Decimal::ZERO
        {
            let target =
                mid_for_step * Decimal::from(self.config.grid_step_bps) / Decimal::from(10_000);
            if target > tick {
                (target / tick).ceil() * tick
            } else {
                tick
            }
        } else {
            tick
        };
        let outward = Decimal::from(levels.saturating_sub(1)) * step;

        // Min-self-spread enforcement: when the book spread is tighter
        // than `min_self_spread_bps`, push the grid tops apart so that
        // top_ask − top_bid ≥ min_self_spread × mid / 10000. Both tops
        // shift symmetrically around mid, snapped to tick boundaries
        // (bid floor down, ask ceil up).
        let (top_bid_override, top_ask_override) = if let (Some(bp), Some(ap)) =
            (best_bid, best_ask)
            && bp.0 > Decimal::ZERO
            && ap.0 > bp.0
            && tick > Decimal::ZERO
            && self.config.min_self_spread_bps > 0
        {
            let mid = (bp.0 + ap.0) / Decimal::from(2);
            let required_half =
                mid * Decimal::from(self.config.min_self_spread_bps) / Decimal::from(20_000);
            let raw_top_bid = mid - required_half;
            let raw_top_ask = mid + required_half;
            // Snap to tick grid: bid floor, ask ceil.
            let snapped_bid = (raw_top_bid / tick).floor() * tick;
            let snapped_ask = (raw_top_ask / tick).ceil() * tick;
            (
                Some(Price(snapped_bid.min(bp.0))),
                Some(Price(snapped_ask.max(ap.0))),
            )
        } else {
            (best_bid, best_ask)
        };

        if let Some(bp_orig) = best_bid
            && let Some(bp) = top_bid_override
            && bp_orig.0 > Decimal::ZERO
            && tick > Decimal::ZERO
            && !suppress_bids
        {
            let target_floor = bp.0 - outward;
            self.bid_grid_floor = Some(match self.bid_grid_floor {
                Some(f) if f <= target_floor => f,
                _ => target_floor,
            });
            let floor = self.bid_grid_floor.unwrap();
            // Cross-guard: never emit a BID at or above best_ask
            // (snapshot may be stale vs current venue book — post-only
            // would reject with -5022). Cap starting price at best_ask − tick.
            let mut price = bp.0;
            if let Some(ap) = best_ask
                && ap.0 > Decimal::ZERO
            {
                let max_bid = ap.0 - tick;
                if price > max_bid {
                    price = max_bid;
                }
            }
            let tolerance = step / Decimal::from(2);
            while price >= floor && price > Decimal::ZERO {
                let p = Price(price);
                if !self.already_have_order(ctx, Side::Bid, p, tolerance) {
                    actions.push(self.emit(ctx.symbol, Side::Bid, p));
                }
                price -= step;
            }
        }

        if let Some(ap_orig) = best_ask
            && let Some(ap) = top_ask_override
            && ap_orig.0 > Decimal::ZERO
            && tick > Decimal::ZERO
            && !suppress_asks
        {
            let target_ceiling = ap.0 + outward;
            self.ask_grid_ceiling = Some(match self.ask_grid_ceiling {
                Some(c) if c >= target_ceiling => c,
                _ => target_ceiling,
            });
            let ceiling = self.ask_grid_ceiling.unwrap();
            // Cross-guard: never emit ASK at or below best_bid.
            let mut price = ap.0;
            if let Some(bp) = best_bid
                && bp.0 > Decimal::ZERO
            {
                let min_ask = bp.0 + tick;
                if price < min_ask {
                    price = min_ask;
                }
            }
            let tolerance = step / Decimal::from(2);
            while price <= ceiling {
                let p = Price(price);
                if !self.already_have_order(ctx, Side::Ask, p, tolerance) {
                    actions.push(self.emit(ctx.symbol, Side::Ask, p));
                }
                price += step;
            }
        }

        // Rule 2: on FULL fill, place opposite-side close at a
        // distance that satisfies min_self_spread_bps. Partial fills
        // (is_full=false) skip the close — there's still residual
        // size on the same side that'll catch the rest of the flow;
        // we don't want to pre-place a close for a position that's
        // still being built.
        //
        // Close distance = max(1 tick, ceil(min_self_spread × fill_price
        //   / 10000 / tick) × tick). Always ≥ 1 tick; bumped to whatever
        // satisfies min_self_spread_bps on tight-tick markets.
        if let MarketEvent::Fill(fill) = event
            && fill.is_full
        {
            let tick = self.config.tick_size;
            if tick > Decimal::ZERO && fill.price.0 > Decimal::ZERO {
                // close_profit_bps overrides min_self_spread_bps when set.
                // Otherwise the close distance falls back to the
                // self-spread requirement (whatever keeps the grid tops
                // apart). Either way, always ≥ 1 tick.
                let close_bps = if self.config.close_profit_bps > 0 {
                    self.config.close_profit_bps
                } else {
                    self.config.min_self_spread_bps
                };
                let target_distance =
                    fill.price.0 * Decimal::from(close_bps) / Decimal::from(10_000);
                let close_distance = if target_distance > tick {
                    (target_distance / tick).ceil() * tick
                } else {
                    tick
                };
                let (close_side, close_price) = match fill.side {
                    Side::Bid => (Side::Ask, Price(fill.price.0 + close_distance)),
                    Side::Ask => (Side::Bid, Price(fill.price.0 - close_distance)),
                };
                // Close uses exact match (tolerance = 0) — each fill
                // gets its own close target. Tolerating overlap here
                // would skip legitimate close emits when grid orders
                // happen to sit near the close price.
                if close_price.0 > Decimal::ZERO
                    && !self.already_have_order(ctx, close_side, close_price, Decimal::ZERO)
                {
                    actions.push(self.emit(ctx.symbol, close_side, close_price));
                }
            }
        }

        actions
    }

    fn on_quote_rejected(
        &mut self,
        _ctx: &StrategyContext<'_>,
        _intent: &QuoteIntent,
        _reason: &str,
    ) -> Vec<Action> {
        // The next on_event for any book update will re-check the
        // best-price invariant and re-emit if needed. No special path
        // — strategy is stateless across events.
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

    fn cfg() -> TouchRefillConfig {
        TouchRefillConfig {
            notional_per_order: Decimal::from(10),
            tick_size: Decimal::new(1, 4), // 0.0001
            step_size: Decimal::ONE,
            min_notional: Decimal::from(5),
            grid_levels: 1,
            min_self_spread_bps: 0,
            close_profit_bps: 0,
            grid_step_bps: 0,
            max_position_usdt: Decimal::ZERO,
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
        }
    }

    #[test]
    fn first_event_places_both_sides_at_touch() {
        let mut s = TouchRefill::new(cfg());
        let snap = book(Decimal::new(10, 4), Decimal::new(11, 4));
        let p = pos();
        let symbol = sym();
        let ctx = make_ctx(&symbol, &snap, &p, &[]);
        let actions = s.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        assert_eq!(actions.len(), 2);
        let prices: Vec<_> = actions
            .iter()
            .map(|a| match a {
                Action::Quote(q) => (q.side, q.price.0),
                _ => panic!("expected Quote"),
            })
            .collect();
        assert!(prices.contains(&(Side::Bid, Decimal::new(10, 4))));
        assert!(prices.contains(&(Side::Ask, Decimal::new(11, 4))));
    }

    #[test]
    fn does_not_re_emit_when_orders_already_at_best() {
        let mut s = TouchRefill::new(cfg());
        let snap = book(Decimal::new(10, 4), Decimal::new(11, 4));
        let p = pos();
        let symbol = sym();
        let bid_intent = QuoteIntent {
            symbol: symbol.clone(),
            side: Side::Bid,
            price: Price(Decimal::new(10, 4)),
            size: Size(Decimal::ONE),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        };
        let ask_intent = QuoteIntent {
            symbol: symbol.clone(),
            side: Side::Ask,
            price: Price(Decimal::new(11, 4)),
            size: Size(Decimal::ONE),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        };
        let open = vec![(QuoteId::new(), bid_intent), (QuoteId::new(), ask_intent)];
        let ctx = make_ctx(&symbol, &snap, &p, &open);
        let actions = s.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        assert!(actions.is_empty(), "no emit when at best: {actions:?}");
    }

    #[test]
    fn full_fill_creates_opposite_close_at_one_tick() {
        let mut s = TouchRefill::new(cfg());
        let snap = book(Decimal::new(10, 4), Decimal::new(11, 4));
        let p = pos();
        let symbol = sym();
        let fill = mk_fill(Side::Bid, Decimal::new(10, 4), true);
        let ctx = make_ctx(&symbol, &snap, &p, &[]);
        let actions = s.on_event(&ctx, &MarketEvent::Fill(fill));
        // 1-tick book → close ASK at fill+1tick coincides with best ask.
        let asks: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Ask => Some(q.price.0),
                _ => None,
            })
            .collect();
        assert_eq!(asks.len(), 1);
        assert_eq!(asks[0], Decimal::new(11, 4));
    }

    #[test]
    fn full_fill_creates_separate_close_when_book_is_wide() {
        let mut s = TouchRefill::new(cfg());
        let snap = book(Decimal::new(10, 4), Decimal::new(20, 4));
        let p = pos();
        let symbol = sym();
        let fill = mk_fill(Side::Bid, Decimal::new(10, 4), true);
        let ctx = make_ctx(&symbol, &snap, &p, &[]);
        let actions = s.on_event(&ctx, &MarketEvent::Fill(fill));
        // BID at 0.0010 (best), ASK at 0.0011 (close), ASK at 0.0020 (best).
        assert_eq!(actions.len(), 3);
        let asks: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Ask => Some(q.price.0),
                _ => None,
            })
            .collect();
        assert_eq!(asks.len(), 2);
        assert!(asks.contains(&Decimal::new(11, 4)));
        assert!(asks.contains(&Decimal::new(20, 4)));
    }

    #[test]
    fn partial_fill_does_not_create_close_order() {
        let mut s = TouchRefill::new(cfg());
        let snap = book(Decimal::new(10, 4), Decimal::new(20, 4));
        let p = pos();
        let symbol = sym();
        let fill = mk_fill(Side::Bid, Decimal::new(10, 4), false);
        let ctx = make_ctx(&symbol, &snap, &p, &[]);
        let actions = s.on_event(&ctx, &MarketEvent::Fill(fill));
        // Only the best-price maintenance emits — no close-on-fill.
        let asks: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Ask => Some(q.price.0),
                _ => None,
            })
            .collect();
        assert_eq!(asks.len(), 1, "no close emit on partial fill: {asks:?}");
        assert_eq!(asks[0], Decimal::new(20, 4));
    }

    #[test]
    fn full_fill_close_honors_min_self_spread_on_tight_tick() {
        // Setup: price ~ 0.1 (so 10bps of price = 1e-4 = 1 tick), but
        // pretend tick is smaller so 1 tick alone wouldn't satisfy
        // min_self_spread_bps. Use price=0.100, tick=0.00001 → 1 tick
        // = 1 bps. min_self_spread_bps = 10 → required = 0.0001 = 10
        // ticks. Close ASK at fill+10 ticks, not fill+1 tick.
        let mut c = TouchRefillConfig {
            notional_per_order: Decimal::from(10),
            tick_size: Decimal::new(1, 5), // 0.00001
            step_size: Decimal::ONE,
            min_notional: Decimal::from(5),
            grid_levels: 1,
            min_self_spread_bps: 10,
            close_profit_bps: 0,
            grid_step_bps: 0,
            max_position_usdt: Decimal::ZERO,
        };
        c.tick_size = Decimal::new(1, 5);
        let mut s = TouchRefill::new(c);
        let symbol = sym();
        let p = pos();
        let snap = book(Decimal::new(99999, 6), Decimal::new(100001, 6)); // 0.099999 / 0.100001
        let fill = mk_fill(Side::Bid, Decimal::new(99999, 6), true);
        let ctx = make_ctx(&symbol, &snap, &p, &[]);
        let actions = s.on_event(&ctx, &MarketEvent::Fill(fill));
        // The close-on-fill ASK should sit at fill + close_distance where
        // close_distance >= 10 ticks (to satisfy 10 bps). Several ASKs
        // may be emitted (rule 1 top-of-grid + rule 2 close); assert at
        // least one is far enough out.
        let fill_p = Decimal::new(99999, 6);
        let min_distance = Decimal::new(1, 4); // 0.0001 = 10 ticks
        let has_close = actions.iter().any(|a| match a {
            Action::Quote(q) if q.side == Side::Ask => q.price.0 - fill_p >= min_distance,
            _ => false,
        });
        assert!(
            has_close,
            "close ASK should sit ≥ 10 ticks from fill; got: {:?}",
            actions
                .iter()
                .filter_map(|a| match a {
                    Action::Quote(q) if q.side == Side::Ask => Some(q.price.0),
                    _ => None,
                })
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn close_profit_bps_overrides_min_self_spread() {
        // Setup: price=$100, tick=$0.01, min_self_spread=10, close_profit=50.
        // min_self_spread distance = 100 × 10 / 10000 = $0.10 = 10 ticks.
        // close_profit distance = 100 × 50 / 10000 = $0.50 = 50 ticks.
        // Expect close to use 50 (the larger override), not 10.
        let mut c = TouchRefillConfig {
            notional_per_order: Decimal::from(10),
            tick_size: Decimal::new(1, 2), // 0.01
            step_size: Decimal::ONE,
            min_notional: Decimal::from(5),
            grid_levels: 1,
            min_self_spread_bps: 10,
            close_profit_bps: 50,
            grid_step_bps: 0,
            max_position_usdt: Decimal::ZERO,
        };
        c.tick_size = Decimal::new(1, 2);
        let mut s = TouchRefill::new(c);
        let symbol = sym();
        let p = pos();
        let snap = book(Decimal::from(100), Decimal::new(10001, 2));
        let fill = mk_fill(Side::Bid, Decimal::from(100), true);
        let ctx = make_ctx(&symbol, &snap, &p, &[]);
        let actions = s.on_event(&ctx, &MarketEvent::Fill(fill));
        let fill_p = Decimal::from(100);
        let expected_distance = Decimal::new(50, 2); // 0.50 = 50 ticks
        let has_close = actions.iter().any(|a| match a {
            Action::Quote(q) if q.side == Side::Ask => q.price.0 - fill_p >= expected_distance,
            _ => false,
        });
        assert!(
            has_close,
            "close ASK should sit ≥ 50 ticks from fill (close_profit_bps=50); got: {:?}",
            actions
                .iter()
                .filter_map(|a| match a {
                    Action::Quote(q) if q.side == Side::Ask => Some(q.price.0),
                    _ => None,
                })
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn grid_places_levels_outward_from_touch() {
        let mut c = cfg();
        c.grid_levels = 3;
        let mut s = TouchRefill::new(c);
        let snap = book(Decimal::new(10, 4), Decimal::new(11, 4));
        let p = pos();
        let symbol = sym();
        let ctx = make_ctx(&symbol, &snap, &p, &[]);
        let actions = s.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        // 3 BIDs at 0.0010, 0.0009, 0.0008 + 3 ASKs at 0.0011, 0.0012, 0.0013.
        let bids: BTreeSet<Decimal> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Bid => Some(q.price.0),
                _ => None,
            })
            .collect();
        assert_eq!(bids.len(), 3);
        assert!(bids.contains(&Decimal::new(10, 4)));
        assert!(bids.contains(&Decimal::new(9, 4)));
        assert!(bids.contains(&Decimal::new(8, 4)));
        let asks: BTreeSet<Decimal> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Ask => Some(q.price.0),
                _ => None,
            })
            .collect();
        assert_eq!(asks.len(), 3);
        assert!(asks.contains(&Decimal::new(11, 4)));
        assert!(asks.contains(&Decimal::new(12, 4)));
        assert!(asks.contains(&Decimal::new(13, 4)));
    }

    #[test]
    fn grid_extends_down_when_best_bid_falls() {
        let mut c = cfg();
        c.grid_levels = 3;
        let mut s = TouchRefill::new(c);
        let symbol = sym();
        let p = pos();

        // Initial book: 0.0010 / 0.0011. Grid: BIDs at 10, 9, 8.
        let snap1 = book(Decimal::new(10, 4), Decimal::new(11, 4));
        let ctx1 = make_ctx(&symbol, &snap1, &p, &[]);
        let _ = s.on_event(
            &ctx1,
            &MarketEvent::BookUpdate {
                snapshot: snap1.clone(),
            },
        );

        // Book moves down: 0.0008 / 0.0009. New bid grid should cover
        // 0.0008 (already there), 0.0007 (NEW — extension), 0.0006 (NEW).
        // Existing 0.0010, 0.0009 stay (orphans we don't cancel).
        let snap2 = book(Decimal::new(8, 4), Decimal::new(9, 4));
        // Simulate open orders from first cycle (10, 9, 8 BIDs).
        let open: Vec<(QuoteId, QuoteIntent)> = [10, 9, 8]
            .iter()
            .map(|p| {
                (
                    QuoteId::new(),
                    QuoteIntent {
                        symbol: symbol.clone(),
                        side: Side::Bid,
                        price: Price(Decimal::new(*p, 4)),
                        size: Size(Decimal::ONE),
                        tif: TimeInForce::PostOnly,
                        kind: QuoteKind::Point,
                    },
                )
            })
            .collect();
        let ctx2 = make_ctx(&symbol, &snap2, &p, &open);
        let actions = s.on_event(
            &ctx2,
            &MarketEvent::BookUpdate {
                snapshot: snap2.clone(),
            },
        );
        let new_bids: BTreeSet<Decimal> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Bid => Some(q.price.0),
                _ => None,
            })
            .collect();
        // 0.0008 was already open. 0.0007 + 0.0006 are new extensions.
        assert_eq!(new_bids.len(), 2, "expect 2 new bid levels: {new_bids:?}");
        assert!(new_bids.contains(&Decimal::new(7, 4)));
        assert!(new_bids.contains(&Decimal::new(6, 4)));
    }
}
