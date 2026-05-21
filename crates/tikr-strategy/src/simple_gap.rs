//! Simple fixed-gap pair strategy.
//!
//! Places one post-only bid and one post-only ask at a fixed bps gap from
//! book mid. On every fill, it places another fresh pair at the latest mid.
//! It does not cancel, skew, requote, or inventory-manage.

use tikr_core::{Decimal, MarketEvent, Price, QuoteKind, Side, Size, Symbol, TimeInForce};
use tikr_venue::QuoteIntent;

use crate::{Action, Strategy, StrategyContext, compute_mid_strict};

/// Configuration for [`SimpleGap`].
#[derive(Debug, Clone)]
pub struct SimpleGapConfig {
    /// Fiat notional per order. Quantity is `notional_per_order / price`.
    pub notional_per_order: Decimal,
    /// Distance from mid for each side, in basis points. Default intended value: `4`.
    pub gap_bps: u32,
}

/// Fixed-gap pair strategy.
pub struct SimpleGap {
    /// Strategy configuration.
    config: SimpleGapConfig,
    /// Whether the first pair has been placed.
    seeded: bool,
}

impl SimpleGap {
    fn make_pair(&self, symbol: &Symbol, mid: Price) -> Vec<Action> {
        vec![
            self.make_side(symbol, mid, Side::Bid),
            self.make_side(symbol, mid, Side::Ask),
        ]
    }

    fn make_side(&self, symbol: &Symbol, mid: Price, side: Side) -> Action {
        let gap = Decimal::from(self.config.gap_bps) / Decimal::from(10_000);
        let price = match side {
            Side::Bid => Price(mid.0 * (Decimal::ONE - gap)),
            Side::Ask => Price(mid.0 * (Decimal::ONE + gap)),
        };
        self.make_quote(symbol, side, price)
    }

    fn make_quote(&self, symbol: &Symbol, side: Side, price: Price) -> Action {
        Action::Quote(QuoteIntent {
            symbol: symbol.clone(),
            side,
            price,
            size: Size(self.config.notional_per_order / price.0),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        })
    }
}

impl Strategy for SimpleGap {
    type Config = SimpleGapConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            seeded: false,
        }
    }

    fn name(&self) -> &str {
        "simple-gap"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        match event {
            MarketEvent::BookUpdate { snapshot } if !self.seeded => {
                let Some(mid) = compute_mid_strict(snapshot) else {
                    return Vec::new();
                };
                self.seeded = true;
                self.make_pair(ctx.symbol, mid)
            }
            MarketEvent::Fill(_) => {
                let Some(mid) = compute_mid_strict(ctx.latest_book) else {
                    return Vec::new();
                };
                self.make_pair(ctx.symbol, mid)
            }
            MarketEvent::BookUpdate { .. }
            | MarketEvent::Heartbeat { .. }
            | MarketEvent::Trade { .. } => Vec::new(),
        }
    }

    fn on_quote_rejected(
        &mut self,
        ctx: &StrategyContext<'_>,
        intent: &QuoteIntent,
        _reason: &str,
    ) -> Vec<Action> {
        let Some(mid) = compute_mid_strict(ctx.latest_book) else {
            return Vec::new();
        };
        vec![self.make_side(ctx.symbol, mid, intent.side)]
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
            base: Asset::new("BTC"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("test"),
            kind: MarketKind::Perp,
        }
    }

    fn book(symbol: &Symbol) -> Snapshot {
        Snapshot {
            symbol: symbol.clone(),
            bids: vec![Level {
                price: Price(Decimal::from(99_000)),
                size: Size(Decimal::ONE),
            }],
            asks: vec![Level {
                price: Price(Decimal::from(101_000)),
                size: Size(Decimal::ONE),
            }],
            ts: Timestamp(1),
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

    fn ctx<'a>(
        symbol: &'a Symbol,
        snapshot: &'a Snapshot,
        position: &'a Position,
    ) -> StrategyContext<'a> {
        StrategyContext {
            symbol,
            now: Timestamp(1),
            position,
            recent_fills: &[],
            latest_book: snapshot,
            open_quotes: &[],
        }
    }

    #[test]
    fn first_book_update_places_gap_pair() {
        let symbol = sym();
        let snapshot = book(&symbol);
        let position = pos(&symbol);
        let mut strategy = SimpleGap::new(SimpleGapConfig {
            notional_per_order: Decimal::from(100),
            gap_bps: 4,
        });

        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );

        assert_eq!(actions.len(), 2);
        match (&actions[0], &actions[1]) {
            (Action::Quote(bid), Action::Quote(ask)) => {
                assert_eq!(bid.side, Side::Bid);
                assert_eq!(ask.side, Side::Ask);
                assert_eq!(bid.price.0, Decimal::from(99_960));
                assert_eq!(ask.price.0, Decimal::from(100_040));
            }
            _ => panic!("expected two quote actions"),
        }
    }

    #[test]
    fn fill_places_another_pair_without_cancel() {
        let symbol = sym();
        let snapshot = book(&symbol);
        let position = pos(&symbol);
        let mut strategy = SimpleGap::new(SimpleGapConfig {
            notional_per_order: Decimal::from(100),
            gap_bps: 4,
        });

        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::Fill(Fill {
                quote_id: QuoteId::new(),
                price: Price(Decimal::from(99_960)),
                size: Size(Decimal::new(1, 3)),
                fee_asset: Asset::new("USDT"),
                fee_amount: Decimal::ZERO,
                fee_quote: Notional(Decimal::ZERO),
                side: Side::Bid,
                ts: Timestamp(2),
                is_full: true,
            }),
        );

        assert!(matches!(
            actions.as_slice(),
            [Action::Quote(_), Action::Quote(_)]
        ));
    }

    #[test]
    fn rejected_leg_retries_same_side_only() {
        let symbol = sym();
        let snapshot = book(&symbol);
        let position = pos(&symbol);
        let mut strategy = SimpleGap::new(SimpleGapConfig {
            notional_per_order: Decimal::from(100),
            gap_bps: 4,
        });
        let rejected = QuoteIntent {
            symbol: symbol.clone(),
            side: Side::Ask,
            price: Price(Decimal::from(100_040)),
            size: Size(Decimal::new(1, 3)),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        };

        let actions = strategy.on_quote_rejected(
            &ctx(&symbol, &snapshot, &position),
            &rejected,
            "post-only would cross",
        );

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::Quote(q) => assert_eq!(q.side, Side::Ask),
            _ => panic!("expected quote action"),
        }
    }
}
