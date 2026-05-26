//! Minimal at-touch refill strategy for wide-tick markets.
//!
//! The whole idea: post a BUY at `top.bid` and a SELL at `top.ask`,
//! both post-only. Whenever the book moves, re-quote so both sides
//! sit at the current touches. On any fill, refill the filled side
//! at the fresh touch. That's it — no inventory cap, no adverse
//! stop, no close-side avg pin.
//!
//! The economics:
//!   gross RT capture ≈ tick_size_bps (you buy at bid, sell at ask)
//!   maker fees       ≈ 1.8 bps × 2  = 3.6 bps RT (Binance USD-M + BNB)
//!   net RT           ≈ tick_bps − 3.6 bps
//!
//! Profitable when tick_bps > ~4 (e.g. ESPORTS 20bps → +16 bps RT,
//! XPL 11bps → +7 bps, OP 7.5bps → +4 bps). Below that you're paying
//! fees out of net.
//!
//! Risk: adverse selection. When your BID fills, market is often
//! moving down (you got picked off). You'll either bag-hold or close
//! at a loss. The strategy is a pure "is the spread wider than 2×
//! fees" bet — no inventory management or trend protection.

use tikr_core::{Decimal, MarketEvent, Price, QuoteKind, Side, Size, Symbol, TimeInForce};
use tikr_venue::QuoteIntent;

use crate::{Action, Strategy, StrategyContext};

/// Configuration for [`TouchRefill`].
#[derive(Debug, Clone)]
pub struct TouchRefillConfig {
    /// Notional USDT per order. Quantity = `notional_per_order / price`,
    /// floored to `step_size`.
    pub notional_per_order: Decimal,
    /// Venue lot step. Quote sizes are floored to this.
    pub step_size: Decimal,
    /// Venue min notional. Sizes are bumped up so `size × price ≥ min`.
    pub min_notional: Decimal,
}

/// Strategy state. Holds the last-emitted bid/ask prices so the diff
/// path can skip emits when targets haven't moved.
pub struct TouchRefill {
    config: TouchRefillConfig,
    last_bid: Option<Price>,
    last_ask: Option<Price>,
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
        // Bump up to meet min_notional if configured.
        let min = self.config.min_notional;
        if min > Decimal::ZERO && stepped * price.0 < min {
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

    /// Emit the action set to bring both sides to (top.bid, top.ask).
    /// Uses `CancelAll` + two fresh `Quote`s — simplest path that's
    /// guaranteed to leave the venue in the intended state.
    fn requote_to_touch(&mut self, ctx: &StrategyContext<'_>) -> Vec<Action> {
        let snapshot = ctx.latest_book;
        let bid = snapshot.bids.first().map(|l| l.price);
        let ask = snapshot.asks.first().map(|l| l.price);
        let (Some(bid), Some(ask)) = (bid, ask) else {
            return Vec::new();
        };
        if bid.0 >= ask.0 {
            return Vec::new();
        }
        // Skip if nothing moved since the last emit. The runner's
        // 30s reconcile will catch orphans if any.
        if self.last_bid == Some(bid) && self.last_ask == Some(ask) {
            return Vec::new();
        }
        self.last_bid = Some(bid);
        self.last_ask = Some(ask);
        vec![
            Action::CancelAll,
            self.make_quote(ctx.symbol, Side::Bid, bid),
            self.make_quote(ctx.symbol, Side::Ask, ask),
        ]
    }
}

impl Strategy for TouchRefill {
    type Config = TouchRefillConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            last_bid: None,
            last_ask: None,
        }
    }

    fn name(&self) -> &str {
        "touch-refill"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        match event {
            MarketEvent::BookUpdate { .. } => self.requote_to_touch(ctx),
            MarketEvent::Fill(_) => {
                // Force re-emit on fill: clear the last-emit cache so
                // requote_to_touch always emits a fresh pair even when
                // the book hasn't moved.
                self.last_bid = None;
                self.last_ask = None;
                self.requote_to_touch(ctx)
            }
            MarketEvent::Heartbeat { .. } | MarketEvent::Trade { .. } => Vec::new(),
        }
    }

    fn on_quote_rejected(
        &mut self,
        ctx: &StrategyContext<'_>,
        _intent: &QuoteIntent,
        _reason: &str,
    ) -> Vec<Action> {
        // Re-emit at the fresh touch. Forces cache invalidation.
        self.last_bid = None;
        self.last_ask = None;
        self.requote_to_touch(ctx)
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

    fn sym() -> Symbol {
        Symbol {
            base: Asset::new("ESPORTS"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("test"),
            kind: MarketKind::Perp,
        }
    }

    fn book(bid: Decimal, ask: Decimal) -> Snapshot {
        let symbol = sym();
        Snapshot {
            symbol,
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
            step_size: Decimal::ONE,
            min_notional: Decimal::from(5),
        }
    }

    #[test]
    fn first_book_event_emits_pair() {
        let mut s = TouchRefill::new(cfg());
        let snapshot = book(Decimal::new(1, 1), Decimal::new(2, 1));
        let position = pos();
        let ctx = StrategyContext {
            symbol: &sym(),
            now: Timestamp(1),
            position: &position,
            recent_fills: &[],
            latest_book: &snapshot,
            open_quotes: &[],
            recent_liqs: &[],
        };
        let actions = s.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        assert_eq!(actions.len(), 3); // CancelAll + Quote x 2
        assert!(matches!(actions[0], Action::CancelAll));
    }

    #[test]
    fn same_book_no_re_emit() {
        let mut s = TouchRefill::new(cfg());
        let snapshot = book(Decimal::new(1, 1), Decimal::new(2, 1));
        let position = pos();
        let ctx = StrategyContext {
            symbol: &sym(),
            now: Timestamp(1),
            position: &position,
            recent_fills: &[],
            latest_book: &snapshot,
            open_quotes: &[],
            recent_liqs: &[],
        };
        let _ = s.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        let actions = s.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        assert!(actions.is_empty());
    }

    #[test]
    fn book_move_triggers_re_emit() {
        let mut s = TouchRefill::new(cfg());
        let pos = pos();
        let snap1 = book(Decimal::new(1, 1), Decimal::new(2, 1));
        let ctx1 = StrategyContext {
            symbol: &sym(),
            now: Timestamp(1),
            position: &pos,
            recent_fills: &[],
            latest_book: &snap1,
            open_quotes: &[],
            recent_liqs: &[],
        };
        let _ = s.on_event(
            &ctx1,
            &MarketEvent::BookUpdate {
                snapshot: snap1.clone(),
            },
        );

        let snap2 = book(Decimal::new(2, 1), Decimal::new(3, 1));
        let ctx2 = StrategyContext {
            symbol: &sym(),
            now: Timestamp(2),
            position: &pos,
            recent_fills: &[],
            latest_book: &snap2,
            open_quotes: &[],
            recent_liqs: &[],
        };
        let actions = s.on_event(
            &ctx2,
            &MarketEvent::BookUpdate {
                snapshot: snap2.clone(),
            },
        );
        assert_eq!(actions.len(), 3);
    }
}
