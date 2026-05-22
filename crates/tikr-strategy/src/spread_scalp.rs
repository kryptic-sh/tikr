//! Spread scalping / liquidity-provision strategy.
//!
//! When the market spread exceeds a configurable bps threshold, places passive
//! limit orders one tick inside the best bid/ask. Requotes on a fixed interval
//! unless quotes are already at the best market prices. Inventory-aware sizing
//! increases the reducing-side order size.

use tikr_core::{
    Decimal, MarketEvent, Price, QuoteKind, Side, Size, Snapshot, TimeInForce, Timestamp,
};
use tikr_venue::QuoteIntent;

use crate::{Action, Strategy, StrategyContext};

/// Configuration for [`SpreadScalp`].
#[derive(Debug, Clone)]
pub struct SpreadScalpConfig {
    /// Fiat notional per order.
    pub notional_per_order: Decimal,
    /// Venue tick size (price increment).
    pub tick_size: Decimal,
    /// Venue lot step size (quantity rounding).
    pub step_size: Decimal,
    /// Minimum order notional (price × size) required by the venue.
    pub min_notional: Decimal,
    /// Minimum market spread in bps required to quote.
    pub min_spread_bps: Decimal,
    /// Fixed requote interval in ms.
    pub requote_interval_ms: u64,
}

/// Spread scalping strategy state.
pub struct SpreadScalp {
    config: SpreadScalpConfig,
    last_bid: Option<Price>,
    last_ask: Option<Price>,
    last_requote_ts: Option<Timestamp>,
    quotes_live: bool,
}

impl SpreadScalp {
    fn compute_targets(&self, snapshot: &Snapshot) -> Option<(Price, Price)> {
        let best_bid = snapshot.bids.first()?.price;
        let best_ask = snapshot.asks.first()?.price;
        let tick = self.config.tick_size;
        if tick <= Decimal::ZERO || best_ask.0 <= best_bid.0 {
            return None;
        }
        let mid = (best_bid.0 + best_ask.0) / Decimal::from(2);
        if mid <= Decimal::ZERO {
            return None;
        }
        let spread_bps = (best_ask.0 - best_bid.0) / mid * Decimal::from(10_000);
        if spread_bps < self.config.min_spread_bps {
            return None;
        }
        let bid = Price(best_bid.0 + tick);
        let ask = Price(best_ask.0 - tick);
        if bid.0 >= ask.0 {
            return None;
        }
        let edge_bps = (ask.0 - bid.0) / mid * Decimal::from(10_000);
        if edge_bps < self.config.min_spread_bps {
            return None;
        }
        Some((bid, ask))
    }

    fn make_quote(
        &self,
        ctx: &StrategyContext<'_>,
        side: Side,
        price: Price,
        size_multiplier: Decimal,
    ) -> Action {
        let raw_size = self.config.notional_per_order / price.0 * size_multiplier;
        let step = self.config.step_size;
        let size = if step > Decimal::ZERO {
            (raw_size / step).floor() * step
        } else {
            raw_size
        };
        let size = if self.config.min_notional > Decimal::ZERO
            && size * price.0 < self.config.min_notional
            && step > Decimal::ZERO
        {
            size + step
        } else {
            size
        };
        Action::Quote(QuoteIntent {
            symbol: ctx.symbol.clone(),
            side,
            price,
            size: Size(size),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        })
    }

    fn should_requote(&self, bid: Price, ask: Price, ts: Timestamp) -> bool {
        if let (Some(last_bid), Some(last_ask)) = (self.last_bid, self.last_ask)
            && last_bid.0 == bid.0
            && last_ask.0 == ask.0
        {
            return false;
        }
        let Some(last_ts) = self.last_requote_ts else {
            return true;
        };
        let elapsed_ns = ts.0.saturating_sub(last_ts.0);
        let interval_ns = self.config.requote_interval_ms.saturating_mul(1_000_000);
        elapsed_ns >= interval_ns
    }

    fn emit_requote(
        &mut self,
        ctx: &StrategyContext<'_>,
        bid: Price,
        ask: Price,
        ts: Timestamp,
    ) -> Vec<Action> {
        let size_mult = self.inventory_size_multiplier(ctx);
        self.last_bid = Some(bid);
        self.last_ask = Some(ask);
        self.last_requote_ts = Some(ts);
        self.quotes_live = true;
        vec![
            Action::CancelAll,
            self.make_quote(ctx, Side::Bid, bid, size_mult.0),
            self.make_quote(ctx, Side::Ask, ask, size_mult.1),
        ]
    }

    fn inventory_size_multiplier(&self, ctx: &StrategyContext<'_>) -> (Decimal, Decimal) {
        let size = ctx.position.size.0;
        if size > Decimal::ZERO {
            (Decimal::from(2), Decimal::ONE)
        } else if size < Decimal::ZERO {
            (Decimal::ONE, Decimal::from(2))
        } else {
            (Decimal::ONE, Decimal::ONE)
        }
    }

    fn cancel_if_live(&mut self, ts: Timestamp) -> Vec<Action> {
        self.last_bid = None;
        self.last_ask = None;
        self.last_requote_ts = Some(ts);
        if self.quotes_live {
            self.quotes_live = false;
            vec![Action::CancelAll]
        } else {
            Vec::new()
        }
    }
}

impl Strategy for SpreadScalp {
    type Config = SpreadScalpConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            last_bid: None,
            last_ask: None,
            last_requote_ts: None,
            quotes_live: false,
        }
    }

    fn name(&self) -> &str {
        "spread-scalp"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        let (snapshot, ts) = match event {
            MarketEvent::BookUpdate { snapshot } => (snapshot, snapshot.ts),
            MarketEvent::Heartbeat { ts } => (ctx.latest_book, *ts),
            MarketEvent::Trade { .. } => return Vec::new(),
            MarketEvent::Fill(_) => {
                let ts = ctx.now;
                let Some((bid, ask)) = self.compute_targets(ctx.latest_book) else {
                    return self.cancel_if_live(ts);
                };
                return self.emit_requote(ctx, bid, ask, ts);
            }
        };
        let Some((bid, ask)) = self.compute_targets(snapshot) else {
            return self.cancel_if_live(ts);
        };
        if !self.should_requote(bid, ask, ts) {
            return vec![Action::NoOp];
        }
        self.emit_requote(ctx, bid, ask, ts)
    }

    fn on_quote_rejected(
        &mut self,
        ctx: &StrategyContext<'_>,
        _intent: &tikr_venue::QuoteIntent,
        _reason: &str,
    ) -> Vec<Action> {
        self.last_bid = None;
        self.last_ask = None;
        self.last_requote_ts = None;
        let ts = ctx.now;
        let Some((bid, ask)) = self.compute_targets(ctx.latest_book) else {
            return self.cancel_if_live(ts);
        };
        self.emit_requote(ctx, bid, ask, ts)
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
    use tikr_core::{Asset, Level, MarketKind, Notional, Position, SignedSize, Symbol, VenueId};

    fn sym() -> Symbol {
        Symbol {
            base: Asset::new("BTC"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("test"),
            kind: MarketKind::Perp,
        }
    }

    fn book(symbol: &Symbol, bid: i64, ask: i64, ts: u64) -> Snapshot {
        Snapshot {
            symbol: symbol.clone(),
            bids: vec![Level {
                price: Price(Decimal::from(bid)),
                size: Size(Decimal::ONE),
            }],
            asks: vec![Level {
                price: Price(Decimal::from(ask)),
                size: Size(Decimal::ONE),
            }],
            ts: Timestamp(ts),
        }
    }

    fn pos(symbol: &Symbol) -> Position {
        Position {
            symbol: symbol.clone(),
            size: SignedSize(Decimal::ZERO),
            avg_entry: Price(Decimal::ZERO),
            realized_pnl: Notional(Decimal::ZERO),
        }
    }

    fn pos_with_size(symbol: &Symbol, size: Decimal) -> Position {
        Position {
            symbol: symbol.clone(),
            size: SignedSize(size),
            avg_entry: Price(Decimal::ZERO),
            realized_pnl: Notional(Decimal::ZERO),
        }
    }

    fn ctx<'a>(
        symbol: &'a Symbol,
        snapshot: &'a Snapshot,
        position: &'a Position,
    ) -> StrategyContext<'a> {
        StrategyContext {
            symbol,
            now: snapshot.ts,
            position,
            recent_fills: &[],
            latest_book: snapshot,
            open_quotes: &[],
        }
    }

    fn strategy() -> SpreadScalp {
        SpreadScalp::new(SpreadScalpConfig {
            notional_per_order: Decimal::from(100),
            tick_size: Decimal::from(1),
            step_size: Decimal::from(1),
            min_notional: Decimal::ZERO,
            min_spread_bps: Decimal::from(5),
            requote_interval_ms: 1000,
        })
    }

    #[test]
    fn wide_spread_quotes_one_tick_inside() {
        let symbol = sym();
        let snapshot = book(&symbol, 100, 110, 1);
        let position = pos(&symbol);
        let mut strategy = strategy();
        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        assert_eq!(actions.len(), 3);
        match (&actions[1], &actions[2]) {
            (Action::Quote(bid), Action::Quote(ask)) => {
                assert_eq!(bid.price.0, Decimal::from(101));
                assert_eq!(ask.price.0, Decimal::from(109));
            }
            _ => panic!("expected quotes"),
        }
    }

    #[test]
    fn narrow_spread_does_not_quote() {
        let symbol = sym();
        let snapshot = book(&symbol, 100, 102, 1);
        let position = pos(&symbol);
        let mut strategy = strategy();
        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        assert!(
            actions.is_empty(),
            "narrow spread should produce no actions, got {:?}",
            actions
        );
    }

    #[test]
    fn does_not_requote_when_already_at_best() {
        let symbol = sym();
        let snapshot = book(&symbol, 100, 110, 1);
        let position = pos(&symbol);
        let mut strategy = strategy();
        let first = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        assert_eq!(first.len(), 3);

        let second = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        assert!(matches!(second.as_slice(), [Action::NoOp]));
    }

    #[test]
    fn requotes_when_market_moves() {
        let symbol = sym();
        let first = book(&symbol, 100, 110, 1);
        let moved = book(&symbol, 102, 112, 2_000_000_000);
        let position = pos(&symbol);
        let mut strategy = strategy();
        let _ = strategy.on_event(
            &ctx(&symbol, &first, &position),
            &MarketEvent::BookUpdate {
                snapshot: first.clone(),
            },
        );
        let actions = strategy.on_event(
            &ctx(&symbol, &moved, &position),
            &MarketEvent::BookUpdate {
                snapshot: moved.clone(),
            },
        );
        assert_eq!(actions.len(), 3);
        match (&actions[1], &actions[2]) {
            (Action::Quote(bid), Action::Quote(ask)) => {
                assert_eq!(bid.price.0, Decimal::from(103));
                assert_eq!(ask.price.0, Decimal::from(111));
            }
            _ => panic!("expected quotes"),
        }
    }

    #[test]
    fn long_inventory_sizes_bid_larger() {
        let symbol = sym();
        let snapshot = book(&symbol, 100, 110, 1);
        let position = pos_with_size(&symbol, Decimal::new(5, 1));
        let mut strategy = strategy();
        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        assert_eq!(actions.len(), 3);
        match (&actions[1], &actions[2]) {
            (Action::Quote(bid), Action::Quote(ask)) => {
                assert!(bid.size.0 > ask.size.0);
            }
            _ => panic!("expected quotes"),
        }
    }

    #[test]
    fn short_inventory_sizes_ask_larger() {
        let symbol = sym();
        let snapshot = book(&symbol, 100, 110, 1);
        let position = pos_with_size(&symbol, Decimal::new(-5, 1));
        let mut strategy = strategy();
        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        assert_eq!(actions.len(), 3);
        match (&actions[1], &actions[2]) {
            (Action::Quote(bid), Action::Quote(ask)) => {
                assert!(ask.size.0 > bid.size.0);
            }
            _ => panic!("expected quotes"),
        }
    }

    #[test]
    fn cancel_when_spread_narrows() {
        let symbol = sym();
        let wide = book(&symbol, 100, 110, 1);
        let narrow = book(&symbol, 100, 102, 2);
        let position = pos(&symbol);
        let mut strategy = strategy();
        let _ = strategy.on_event(
            &ctx(&symbol, &wide, &position),
            &MarketEvent::BookUpdate {
                snapshot: wide.clone(),
            },
        );
        let actions = strategy.on_event(
            &ctx(&symbol, &narrow, &position),
            &MarketEvent::BookUpdate {
                snapshot: narrow.clone(),
            },
        );
        assert!(matches!(actions.as_slice(), [Action::CancelAll]));
    }

    #[test]
    fn fill_triggers_requote() {
        let symbol = sym();
        let snapshot = book(&symbol, 100, 110, 1);
        let position = pos(&symbol);
        let mut strategy = strategy();
        let _ = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::Fill(tikr_core::Fill {
                quote_id: tikr_venue::QuoteId::new(),
                price: Price(Decimal::from(101)),
                size: Size(Decimal::ONE),
                fee_asset: Asset::new("USDT"),
                fee_amount: Decimal::ZERO,
                fee_quote: Notional(Decimal::ZERO),
                side: Side::Bid,
                ts: Timestamp(2),
                is_full: true,
            }),
        );
        assert_eq!(actions.len(), 3);
    }
}
