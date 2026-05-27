//! Minimal joker (join-the-touch) market-making strategy.
//!
//! Single rule: on every event, if we don't already have a post-only
//! order on the current `best_bid` and `best_ask`, place one. Never
//! cancels. No close-on-fill. No grid, no inventory cap, no risk gate.
//!
//! Designed for zero-fee venues (USDC promo) where any fill at touch
//! collects pure spread with no cost floor. Inventory risk is the
//! operator's to manage via `max_position_pct` at the account layer.
//!
//! Dedupe: a fresh emit at price P on side S is suppressed if
//! `ctx.open_quotes` already contains an order on S at exactly P.
//! Price moves a tick → new emit at the new touch; the old one sits
//! forever at its original price (never cancelled).
//!
//! Strategy state is empty — it's a pure function of the current book
//! + open-orders view.
//!
//! Cross-guard: BID emit capped at `best_ask - tick`, ASK emit floored
//! at `best_bid + tick`, to avoid post-only-would-cross rejections on
//! 1-tick books.

use std::collections::HashMap;

use tikr_core::{
    Decimal, MarketEvent, Price, QuoteKind, Side, Size, Symbol, TimeInForce, Timestamp,
};
use tikr_venue::QuoteIntent;

use crate::{Action, Strategy, StrategyContext};

/// Configuration for [`Joker`].
#[derive(Debug, Clone)]
pub struct JokerConfig {
    /// Notional in quote currency per order. Quantity =
    /// `notional / price`, floored to `step_size`, bumped to
    /// `min_notional` when below.
    pub notional_per_order: Decimal,
    /// Venue tick size. Used only for the cross-guard math.
    pub tick_size: Decimal,
    /// Venue lot step.
    pub step_size: Decimal,
    /// Venue min order notional in quote currency.
    pub min_notional: Decimal,
    /// Cancel any open order older than this many seconds since its emit.
    /// Forces the joiner to keep its book fresh — stale orders that sat
    /// through book moves get reaped instead of pinning margin. `0`
    /// disables the age sweep (orders rest forever).
    pub max_order_age_secs: u64,
}

/// Joker (join-the-touch) state. Tracks emit timestamps per (side, price)
/// so the on_event loop can cancel orders older than `MAX_ORDER_AGE_SECS`.
pub struct Joker {
    config: JokerConfig,
    /// `(side, price.0) → ts (ns) at emit`. Inserted on every emit;
    /// removed when the corresponding open quote disappears (filled
    /// or cancelled). Used to gate the age-based cancel pass.
    placement_ts: HashMap<(Side, Decimal), Timestamp>,
}

impl Joker {
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

    fn already_at(&self, ctx: &StrategyContext<'_>, side: Side, price: Price) -> bool {
        ctx.open_quotes
            .iter()
            .any(|(_, q)| q.side == side && q.price.0 == price.0)
    }
}

impl Strategy for Joker {
    type Config = JokerConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            placement_ts: HashMap::new(),
        }
    }

    fn name(&self) -> &str {
        "joker"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, _event: &MarketEvent) -> Vec<Action> {
        let mut actions = Vec::new();
        let tick = self.config.tick_size;
        let best_bid = ctx.latest_book.bids.first().map(|l| l.price);
        let best_ask = ctx.latest_book.asks.first().map(|l| l.price);

        // Age-based cancel sweep. For every open quote, look up its emit
        // timestamp in `placement_ts`; if older than `max_order_age_secs`,
        // emit Cancel(id) and drop the tracker entry. Quotes we have no
        // record of (orphans from a prior session) are left alone.
        let max_age_secs = self.config.max_order_age_secs;
        if max_age_secs > 0 {
            let max_age_ns = max_age_secs.saturating_mul(1_000_000_000);
            let now_ns = ctx.now.0;
            let open_keys: std::collections::HashSet<(Side, Decimal)> = ctx
                .open_quotes
                .iter()
                .map(|(_, q)| (q.side, q.price.0))
                .collect();
            // Drop tracker entries that no longer have a matching open
            // quote (filled, externally cancelled).
            self.placement_ts.retain(|k, _| open_keys.contains(k));
            for (id, q) in ctx.open_quotes {
                let key = (q.side, q.price.0);
                if let Some(ts) = self.placement_ts.get(&key)
                    && now_ns.saturating_sub(ts.0) > max_age_ns
                {
                    actions.push(Action::Cancel(*id));
                    self.placement_ts.remove(&key);
                }
            }
        }

        if let Some(bp) = best_bid
            && bp.0 > Decimal::ZERO
        {
            // Cross-guard: never emit BID >= best_ask.
            let mut price = bp;
            if let Some(ap) = best_ask
                && ap.0 > Decimal::ZERO
                && tick > Decimal::ZERO
            {
                let cap = Price(ap.0 - tick);
                if price.0 > cap.0 {
                    price = cap;
                }
            }
            if price.0 > Decimal::ZERO && !self.already_at(ctx, Side::Bid, price) {
                actions.push(self.make_quote(ctx.symbol, Side::Bid, price));
                self.placement_ts.insert((Side::Bid, price.0), ctx.now);
            }
        }

        if let Some(ap) = best_ask
            && ap.0 > Decimal::ZERO
        {
            // Cross-guard: never emit ASK <= best_bid.
            let mut price = ap;
            if let Some(bp) = best_bid
                && bp.0 > Decimal::ZERO
                && tick > Decimal::ZERO
            {
                let floor = Price(bp.0 + tick);
                if price.0 < floor.0 {
                    price = floor;
                }
            }
            if price.0 > Decimal::ZERO && !self.already_at(ctx, Side::Ask, price) {
                actions.push(self.make_quote(ctx.symbol, Side::Ask, price));
                self.placement_ts.insert((Side::Ask, price.0), ctx.now);
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
        Asset, Level, MarketKind, Position, SignedSize, Snapshot, Timestamp, VenueId,
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

    fn book(bid: Decimal, ask: Decimal) -> Snapshot {
        Snapshot {
            symbol: sym(),
            bids: vec![Level {
                price: Price(bid),
                size: Size(Decimal::from(1)),
            }],
            asks: vec![Level {
                price: Price(ask),
                size: Size(Decimal::from(1)),
            }],
            ts: Timestamp(1),
        }
    }

    fn pos() -> Position {
        Position {
            symbol: sym(),
            size: SignedSize(Decimal::ZERO),
            avg_entry: Price(Decimal::ZERO),
            realized_pnl: tikr_core::Notional(Decimal::ZERO),
        }
    }

    fn cfg() -> JokerConfig {
        JokerConfig {
            notional_per_order: Decimal::from(100),
            tick_size: Decimal::new(1, 1), // 0.1
            step_size: Decimal::new(1, 3), // 0.001
            min_notional: Decimal::from(5),
            max_order_age_secs: 0,
        }
    }

    fn ctx<'a>(
        symbol: &'a Symbol,
        snap: &'a Snapshot,
        p: &'a Position,
        open: &'a [(QuoteId, QuoteIntent)],
    ) -> StrategyContext<'a> {
        StrategyContext {
            symbol,
            now: Timestamp(1),
            position: p,
            recent_fills: &[],
            latest_book: snap,
            open_quotes: open,
            recent_liqs: &[],
        }
    }

    #[test]
    fn first_event_emits_both_touches() {
        let mut s = Joker::new(cfg());
        let snap = book(Decimal::from(100), Decimal::new(1001, 1));
        let p = pos();
        let symbol = sym();
        let c = ctx(&symbol, &snap, &p, &[]);
        let actions = s.on_event(
            &c,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        assert_eq!(actions.len(), 2);
    }

    #[test]
    fn skips_emit_when_already_at_touch() {
        let mut s = Joker::new(cfg());
        let snap = book(Decimal::from(100), Decimal::new(1001, 1));
        let p = pos();
        let symbol = sym();
        let open = vec![(
            QuoteId::new(),
            QuoteIntent {
                symbol: symbol.clone(),
                side: Side::Bid,
                price: Price(Decimal::from(100)),
                size: Size(Decimal::ONE),
                tif: TimeInForce::PostOnly,
                kind: QuoteKind::Point,
            },
        )];
        let c = ctx(&symbol, &snap, &p, &open);
        let actions = s.on_event(
            &c,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        // Only the ASK side should emit; BID already covered.
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::Quote(q) => assert_eq!(q.side, Side::Ask),
            _ => panic!("expected Quote"),
        }
    }
}
