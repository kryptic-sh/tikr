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
    /// Venue tick size. Used for the close-on-fill +/- 1 tick offset.
    pub tick_size: Decimal,
    /// Venue lot step.
    pub step_size: Decimal,
    /// Venue min order notional.
    pub min_notional: Decimal,
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
    /// already hold an order on `side` at exactly `price`?
    fn already_have_order(&self, ctx: &StrategyContext<'_>, side: Side, price: Price) -> bool {
        let pending = match side {
            Side::Bid => &self.pending_bid_prices,
            Side::Ask => &self.pending_ask_prices,
        };
        if pending.contains(&price.0) {
            return true;
        }
        ctx.open_quotes
            .iter()
            .any(|(_, q)| q.side == side && q.price.0 == price.0)
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

        // Rule 1: ensure ≥1 order at best on each side.
        let best_bid = ctx.latest_book.bids.first().map(|l| l.price);
        let best_ask = ctx.latest_book.asks.first().map(|l| l.price);
        if let Some(bp) = best_bid
            && bp.0 > Decimal::ZERO
            && !self.already_have_order(ctx, Side::Bid, bp)
        {
            actions.push(self.emit(ctx.symbol, Side::Bid, bp));
        }
        if let Some(ap) = best_ask
            && ap.0 > Decimal::ZERO
            && !self.already_have_order(ctx, Side::Ask, ap)
        {
            actions.push(self.emit(ctx.symbol, Side::Ask, ap));
        }

        // Rule 2: on FULL fill, place opposite-side close at
        // fill_price ± 1 tick. Partial fills (is_full=false) skip
        // the close — there's still residual size on the same side
        // that'll catch the rest of the flow; we don't want to
        // pre-place a close for a position that's still being built.
        if let MarketEvent::Fill(fill) = event
            && fill.is_full
        {
            let tick = self.config.tick_size;
            if tick > Decimal::ZERO {
                let (close_side, close_price) = match fill.side {
                    // Filled BID = long → close with SELL at fill+1tick.
                    Side::Bid => (Side::Ask, Price(fill.price.0 + tick)),
                    // Filled ASK = short → close with BUY at fill-1tick.
                    Side::Ask => (Side::Bid, Price(fill.price.0 - tick)),
                };
                if close_price.0 > Decimal::ZERO
                    && !self.already_have_order(ctx, close_side, close_price)
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
}
