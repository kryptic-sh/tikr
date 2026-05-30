//! Micro mean-reversion / overshoot capture strategy.
//!
//! Watches the last trade against the current book mid. When the trade is far
//! enough beyond mid, places one passive order on the opposite side expecting a
//! short-term snapback. A fill then places one passive exit on the opposite side.

use tikr_core::{Decimal, MarketEvent, Price, QuoteKind, Side, Size, Symbol, TimeInForce};
use tikr_venue::QuoteIntent;

use crate::{Action, Strategy, StrategyContext, compute_mid_strict};

/// Configuration for [`MicroMeanReversion`].
#[derive(Debug, Clone)]
pub struct MicroMeanReversionConfig {
    /// Fiat notional per order. Quantity is `notional_per_order / price`.
    pub notional_per_order: Decimal,
    /// Trade distance from mid required before entering, in bps.
    pub trigger_bps: u32,
    /// Passive entry distance from mid, in bps.
    pub entry_bps: u32,
    /// Exit distance from fill price, in bps.
    pub exit_bps: u32,
    /// Maximum same-side entry quotes to keep open.
    pub max_open_entries: u32,
}

/// Micro mean-reversion strategy state.
pub struct MicroMeanReversion {
    config: MicroMeanReversionConfig,
}

impl MicroMeanReversion {
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

    fn entry_quote(&self, symbol: &Symbol, mid: Price, side: Side) -> Action {
        let gap = Decimal::from(self.config.entry_bps) / Decimal::from(10_000);
        let price = match side {
            Side::Bid => Price(mid.0 * (Decimal::ONE - gap)),
            Side::Ask => Price(mid.0 * (Decimal::ONE + gap)),
        };
        self.make_quote(symbol, side, price)
    }

    fn exit_quote(&self, symbol: &Symbol, fill_price: Price, fill_side: Side) -> Action {
        let gap = Decimal::from(self.config.exit_bps) / Decimal::from(10_000);
        let (side, price) = match fill_side {
            Side::Bid => (Side::Ask, Price(fill_price.0 * (Decimal::ONE + gap))),
            Side::Ask => (Side::Bid, Price(fill_price.0 * (Decimal::ONE - gap))),
        };
        self.make_quote(symbol, side, price)
    }

    fn open_entries(&self, ctx: &StrategyContext<'_>, side: Side) -> u32 {
        ctx.open_quotes
            .iter()
            .filter(|(_, q)| q.side == side)
            .count() as u32
    }
}

impl Strategy for MicroMeanReversion {
    type Config = MicroMeanReversionConfig;

    fn new(config: Self::Config) -> Self {
        Self { config }
    }

    fn name(&self) -> &str {
        "micro-mean-reversion"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        match event {
            MarketEvent::Trade { price, .. } => {
                let Some(mid) = compute_mid_strict(ctx.latest_book) else {
                    return Vec::new();
                };
                if mid.0 <= Decimal::ZERO {
                    return Vec::new();
                }
                let move_bps = (price.0 - mid.0) / mid.0 * Decimal::from(10_000);
                let trigger = Decimal::from(self.config.trigger_bps);
                if move_bps >= trigger {
                    if self.open_entries(ctx, Side::Ask) >= self.config.max_open_entries {
                        return Vec::new();
                    }
                    vec![self.entry_quote(ctx.symbol, mid, Side::Ask)]
                } else if move_bps <= -trigger {
                    if self.open_entries(ctx, Side::Bid) >= self.config.max_open_entries {
                        return Vec::new();
                    }
                    vec![self.entry_quote(ctx.symbol, mid, Side::Bid)]
                } else {
                    Vec::new()
                }
            }
            MarketEvent::Fill(fill) if fill.is_full => {
                vec![self.exit_quote(ctx.symbol, fill.price, fill.side)]
            }
            MarketEvent::BookUpdate { .. }
            | MarketEvent::Heartbeat { .. }
            | MarketEvent::Fill(_) => Vec::new(),
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
        vec![self.entry_quote(ctx.symbol, mid, intent.side)]
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
            quote: Asset::new("USDT"),
            venue: VenueId::new("test"),
            kind: MarketKind::Perp,
        }
    }

    fn book(symbol: &Symbol) -> Snapshot {
        Snapshot {
            symbol: symbol.clone(),
            bids: vec![Level {
                price: Price(Decimal::from(99_900)),
                size: Size(Decimal::ONE),
            }],
            asks: vec![Level {
                price: Price(Decimal::from(100_100)),
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
        open_quotes: &'a [(QuoteId, QuoteIntent)],
    ) -> StrategyContext<'a> {
        StrategyContext {
            symbol,
            now: Timestamp(1),
            position,
            recent_fills: &[],
            latest_book: snapshot,
            open_quotes,
            recent_liqs: &[],
        }
    }

    fn strategy() -> MicroMeanReversion {
        MicroMeanReversion::new(MicroMeanReversionConfig {
            notional_per_order: Decimal::from(100),
            trigger_bps: 10,
            entry_bps: 2,
            exit_bps: 6,
            max_open_entries: 1,
        })
    }

    #[test]
    fn high_overshoot_places_passive_ask() {
        let symbol = sym();
        let snapshot = book(&symbol);
        let position = pos(&symbol);
        let mut strategy = strategy();
        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position, &[]),
            &MarketEvent::Trade {
                symbol: symbol.clone(),
                price: Price(Decimal::from(100_200)),
                size: Size(Decimal::ONE),
                side: Side::Bid,
                ts: Timestamp(2),
            },
        );
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::Quote(q) => assert_eq!(q.side, Side::Ask),
            _ => panic!("expected quote"),
        }
    }

    #[test]
    fn fill_places_opposite_exit() {
        let symbol = sym();
        let snapshot = book(&symbol);
        let position = pos(&symbol);
        let mut strategy = strategy();
        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position, &[]),
            &MarketEvent::Fill(tikr_core::Fill {
                quote_id: QuoteId::new(),
                price: Price(Decimal::from(99_980)),
                size: Size(Decimal::new(1, 3)),
                fee_asset: Asset::new("USDT"),
                fee_amount: Decimal::ZERO,
                fee_quote: Notional(Decimal::ZERO),
                side: Side::Bid,
                ts: Timestamp(2),
                is_full: true,
                trade_id: None,
            }),
        );
        match &actions[0] {
            Action::Quote(q) => assert_eq!(q.side, Side::Ask),
            _ => panic!("expected quote"),
        }
    }
}
