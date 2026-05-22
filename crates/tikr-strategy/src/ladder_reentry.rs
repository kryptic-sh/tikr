//! Ladder re-entry strategy.
//!
//! Seeds a symmetric ladder around mid, then adds two orders after each full
//! fill: one same-side continuation farther from the fill, and one opposite-
//! side reentry closer to the fill.

use tikr_core::{Decimal, Fill, MarketEvent, Price, QuoteKind, Side, Size, Symbol, TimeInForce};
use tikr_venue::QuoteIntent;

use crate::{Action, Strategy, StrategyContext, compute_mid_strict};

/// Configuration for [`LadderReentry`].
#[derive(Debug, Clone)]
pub struct LadderReentryConfig {
    /// Fiat notional per order. Quantity is `notional_per_order / price`.
    pub notional_per_order: Decimal,
    /// Orders per side at seed time.
    pub levels_per_side: u32,
    /// Inner distance from mid, in bps.
    pub inner_bps: u32,
    /// Bps between adjacent seed levels on the same side.
    pub step_bps: u32,
    /// Distance for the opposite-side order after a full fill.
    pub reentry_bps: u32,
    /// Distance for the same-side continuation order after a full fill.
    pub continuation_bps: u32,
}

/// Fixed seed ladder with opposite-side reentry after each full fill.
pub struct LadderReentry {
    /// Strategy configuration.
    config: LadderReentryConfig,
    /// Whether the initial ladder was placed.
    seeded: bool,
}

impl LadderReentry {
    fn seed_ladder(&self, symbol: &Symbol, mid: Price) -> Vec<Action> {
        let mut actions = Vec::with_capacity((self.config.levels_per_side * 2) as usize);
        for k in 0..self.config.levels_per_side {
            let bps = self.config.inner_bps + self.config.step_bps * k;
            actions.push(self.make_seed_side(symbol, mid, Side::Bid, bps));
            actions.push(self.make_seed_side(symbol, mid, Side::Ask, bps));
        }
        actions
    }

    fn make_seed_side(&self, symbol: &Symbol, mid: Price, side: Side, bps: u32) -> Action {
        let gap = Decimal::from(bps) / Decimal::from(10_000);
        let price = match side {
            Side::Bid => Price(mid.0 * (Decimal::ONE - gap)),
            Side::Ask => Price(mid.0 * (Decimal::ONE + gap)),
        };
        self.make_quote(symbol, side, price)
    }

    fn make_reentries(&self, ctx: &StrategyContext<'_>, fill: &Fill) -> Vec<Action> {
        let reentry_gap = Decimal::from(self.config.reentry_bps) / Decimal::from(10_000);
        let continuation_gap = Decimal::from(self.config.continuation_bps) / Decimal::from(10_000);
        match fill.side {
            Side::Bid => {
                let mut actions = vec![
                    self.make_quote(
                        ctx.symbol,
                        Side::Bid,
                        Price(fill.price.0 * (Decimal::ONE - continuation_gap)),
                    ),
                    self.make_quote(
                        ctx.symbol,
                        Side::Ask,
                        Price(fill.price.0 * (Decimal::ONE + reentry_gap)),
                    ),
                ];
                if let Some((id, _)) = ctx
                    .open_quotes
                    .iter()
                    .filter(|(_, q)| q.side == Side::Ask)
                    .max_by_key(|(_, q)| q.price.0)
                {
                    actions.push(Action::Cancel(*id));
                }
                actions
            }
            Side::Ask => {
                let mut actions = vec![
                    self.make_quote(
                        ctx.symbol,
                        Side::Ask,
                        Price(fill.price.0 * (Decimal::ONE + continuation_gap)),
                    ),
                    self.make_quote(
                        ctx.symbol,
                        Side::Bid,
                        Price(fill.price.0 * (Decimal::ONE - reentry_gap)),
                    ),
                ];
                if let Some((id, _)) = ctx
                    .open_quotes
                    .iter()
                    .filter(|(_, q)| q.side == Side::Bid)
                    .min_by_key(|(_, q)| q.price.0)
                {
                    actions.push(Action::Cancel(*id));
                }
                actions
            }
        }
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

    fn make_rebalance(&self, ctx: &StrategyContext<'_>) -> Vec<Action> {
        let pos = ctx.position.size.0;
        if pos == Decimal::ZERO {
            return Vec::new();
        }
        let (side, price) = if pos > Decimal::ZERO {
            let Some(best_ask) = ctx.latest_book.asks.first().map(|l| l.price) else {
                return Vec::new();
            };
            (Side::Ask, best_ask)
        } else {
            let Some(best_bid) = ctx.latest_book.bids.first().map(|l| l.price) else {
                return Vec::new();
            };
            (Side::Bid, best_bid)
        };
        if price.0 <= Decimal::ZERO {
            return Vec::new();
        }
        let max_order_size = self.config.notional_per_order / price.0;
        let size = pos.abs().min(max_order_size);
        if size <= Decimal::ZERO {
            return Vec::new();
        }
        vec![Action::Quote(QuoteIntent {
            symbol: ctx.symbol.clone(),
            side,
            price,
            size: Size(size),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        })]
    }
}

impl Strategy for LadderReentry {
    type Config = LadderReentryConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            seeded: false,
        }
    }

    fn name(&self) -> &str {
        "ladder-reentry"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        match event {
            MarketEvent::BookUpdate { snapshot } if !self.seeded => {
                let Some(mid) = compute_mid_strict(snapshot) else {
                    return Vec::new();
                };
                self.seeded = true;
                self.seed_ladder(ctx.symbol, mid)
            }
            MarketEvent::Fill(fill) if fill.is_full => self.make_reentries(ctx, fill),
            MarketEvent::Fill(_)
            | MarketEvent::BookUpdate { .. }
            | MarketEvent::Heartbeat { .. }
            | MarketEvent::Trade { .. } => Vec::new(),
        }
    }

    fn on_quote_rejected(
        &mut self,
        ctx: &StrategyContext<'_>,
        intent: &QuoteIntent,
        reason: &str,
    ) -> Vec<Action> {
        if reason.contains("-2019") || reason.contains("margin insufficient") {
            return self.make_rebalance(ctx);
        }
        let Some(mid) = compute_mid_strict(ctx.latest_book) else {
            return Vec::new();
        };
        vec![self.make_seed_side(ctx.symbol, mid, intent.side, self.config.inner_bps)]
    }

    fn on_notional_updated(
        &mut self,
        _ctx: &StrategyContext<'_>,
        notional_per_order: Decimal,
    ) -> Vec<Action> {
        if notional_per_order <= Decimal::ZERO
            || notional_per_order == self.config.notional_per_order
        {
            return Vec::new();
        }
        self.config.notional_per_order = notional_per_order;
        self.seeded = false;
        vec![Action::CancelAll]
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
        pos_with_size(symbol, Decimal::ZERO)
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
        open_quotes: &'a [(QuoteId, QuoteIntent)],
    ) -> StrategyContext<'a> {
        StrategyContext {
            symbol,
            now: Timestamp(1),
            position,
            recent_fills: &[],
            latest_book: snapshot,
            open_quotes,
        }
    }

    fn cfg() -> LadderReentryConfig {
        LadderReentryConfig {
            notional_per_order: Decimal::from(100),
            levels_per_side: 10,
            inner_bps: 5,
            step_bps: 1,
            reentry_bps: 5,
            continuation_bps: 11,
        }
    }

    #[test]
    fn first_book_update_places_twenty_level_ladder() {
        let symbol = sym();
        let snapshot = book(&symbol);
        let position = pos(&symbol);
        let mut strategy = LadderReentry::new(cfg());

        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position, &[]),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );

        assert_eq!(actions.len(), 20);
        match (&actions[0], &actions[1], &actions[18], &actions[19]) {
            (Action::Quote(b0), Action::Quote(a0), Action::Quote(b9), Action::Quote(a9)) => {
                assert_eq!(b0.side, Side::Bid);
                assert_eq!(a0.side, Side::Ask);
                assert_eq!(b0.price.0, Decimal::from(99_950));
                assert_eq!(a0.price.0, Decimal::from(100_050));
                assert_eq!(b9.price.0, Decimal::from(99_860));
                assert_eq!(a9.price.0, Decimal::from(100_140));
            }
            _ => panic!("expected quote actions"),
        }
    }

    #[test]
    fn sell_fill_opens_sell_continuation_and_buy_reentry() {
        let symbol = sym();
        let snapshot = book(&symbol);
        let position = pos(&symbol);
        let mut strategy = LadderReentry::new(cfg());
        let fill_price = Decimal::from(100_000);

        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position, &[]),
            &MarketEvent::Fill(Fill {
                quote_id: QuoteId::new(),
                price: Price(fill_price),
                size: Size(Decimal::new(1, 3)),
                fee_asset: Asset::new("USDT"),
                fee_amount: Decimal::ZERO,
                fee_quote: Notional(Decimal::ZERO),
                side: Side::Ask,
                ts: Timestamp(2),
                is_full: true,
            }),
        );

        assert_eq!(actions.len(), 2);
        match (&actions[0], &actions[1]) {
            (Action::Quote(sell), Action::Quote(buy)) => {
                assert_eq!(sell.side, Side::Ask);
                assert_eq!(sell.price.0, Decimal::from(100_110));
                assert_eq!(buy.side, Side::Bid);
                assert_eq!(buy.price.0, Decimal::from(99_950));
            }
            _ => panic!("expected quote actions"),
        }
    }

    #[test]
    fn buy_fill_opens_buy_continuation_and_sell_reentry() {
        let symbol = sym();
        let snapshot = book(&symbol);
        let position = pos(&symbol);
        let mut strategy = LadderReentry::new(cfg());
        let fill_price = Decimal::from(100_000);

        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position, &[]),
            &MarketEvent::Fill(Fill {
                quote_id: QuoteId::new(),
                price: Price(fill_price),
                size: Size(Decimal::new(1, 3)),
                fee_asset: Asset::new("USDT"),
                fee_amount: Decimal::ZERO,
                fee_quote: Notional(Decimal::ZERO),
                side: Side::Bid,
                ts: Timestamp(2),
                is_full: true,
            }),
        );

        assert_eq!(actions.len(), 2);
        match (&actions[0], &actions[1]) {
            (Action::Quote(buy), Action::Quote(sell)) => {
                assert_eq!(buy.side, Side::Bid);
                assert_eq!(buy.price.0, Decimal::from(99_890));
                assert_eq!(sell.side, Side::Ask);
                assert_eq!(sell.price.0, Decimal::from(100_050));
            }
            _ => panic!("expected quote actions"),
        }
    }

    #[test]
    fn buy_fill_cancels_furthest_sell_after_new_pair() {
        let symbol = sym();
        let snapshot = book(&symbol);
        let position = pos(&symbol);
        let mut strategy = LadderReentry::new(cfg());
        let near = QuoteId::new();
        let far = QuoteId::new();
        let bid = QuoteId::new();
        let open_quotes = vec![
            (
                near,
                QuoteIntent {
                    symbol: symbol.clone(),
                    side: Side::Ask,
                    price: Price(Decimal::from(100_100)),
                    size: Size(Decimal::ONE),
                    tif: TimeInForce::PostOnly,
                    kind: QuoteKind::Point,
                },
            ),
            (
                far,
                QuoteIntent {
                    symbol: symbol.clone(),
                    side: Side::Ask,
                    price: Price(Decimal::from(100_300)),
                    size: Size(Decimal::ONE),
                    tif: TimeInForce::PostOnly,
                    kind: QuoteKind::Point,
                },
            ),
            (
                bid,
                QuoteIntent {
                    symbol: symbol.clone(),
                    side: Side::Bid,
                    price: Price(Decimal::from(99_900)),
                    size: Size(Decimal::ONE),
                    tif: TimeInForce::PostOnly,
                    kind: QuoteKind::Point,
                },
            ),
        ];

        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position, &open_quotes),
            &MarketEvent::Fill(Fill {
                quote_id: QuoteId::new(),
                price: Price(Decimal::from(100_000)),
                size: Size(Decimal::new(1, 3)),
                fee_asset: Asset::new("USDT"),
                fee_amount: Decimal::ZERO,
                fee_quote: Notional(Decimal::ZERO),
                side: Side::Bid,
                ts: Timestamp(2),
                is_full: true,
            }),
        );

        assert_eq!(actions.len(), 3);
        assert!(matches!(actions[0], Action::Quote(_)));
        assert!(matches!(actions[1], Action::Quote(_)));
        match actions[2] {
            Action::Cancel(id) => assert_eq!(id, far),
            _ => panic!("expected furthest sell cancel"),
        }
    }

    #[test]
    fn sell_fill_cancels_furthest_buy_after_new_pair() {
        let symbol = sym();
        let snapshot = book(&symbol);
        let position = pos(&symbol);
        let mut strategy = LadderReentry::new(cfg());
        let near = QuoteId::new();
        let far = QuoteId::new();
        let ask = QuoteId::new();
        let open_quotes = vec![
            (
                near,
                QuoteIntent {
                    symbol: symbol.clone(),
                    side: Side::Bid,
                    price: Price(Decimal::from(99_900)),
                    size: Size(Decimal::ONE),
                    tif: TimeInForce::PostOnly,
                    kind: QuoteKind::Point,
                },
            ),
            (
                far,
                QuoteIntent {
                    symbol: symbol.clone(),
                    side: Side::Bid,
                    price: Price(Decimal::from(99_700)),
                    size: Size(Decimal::ONE),
                    tif: TimeInForce::PostOnly,
                    kind: QuoteKind::Point,
                },
            ),
            (
                ask,
                QuoteIntent {
                    symbol: symbol.clone(),
                    side: Side::Ask,
                    price: Price(Decimal::from(100_100)),
                    size: Size(Decimal::ONE),
                    tif: TimeInForce::PostOnly,
                    kind: QuoteKind::Point,
                },
            ),
        ];

        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position, &open_quotes),
            &MarketEvent::Fill(Fill {
                quote_id: QuoteId::new(),
                price: Price(Decimal::from(100_000)),
                size: Size(Decimal::new(1, 3)),
                fee_asset: Asset::new("USDT"),
                fee_amount: Decimal::ZERO,
                fee_quote: Notional(Decimal::ZERO),
                side: Side::Ask,
                ts: Timestamp(2),
                is_full: true,
            }),
        );

        assert_eq!(actions.len(), 3);
        assert!(matches!(actions[0], Action::Quote(_)));
        assert!(matches!(actions[1], Action::Quote(_)));
        match actions[2] {
            Action::Cancel(id) => assert_eq!(id, far),
            _ => panic!("expected furthest buy cancel"),
        }
    }

    #[test]
    fn margin_reject_long_places_position_reducing_sell() {
        let symbol = sym();
        let snapshot = book(&symbol);
        let position = pos_with_size(&symbol, Decimal::new(2, 3));
        let mut strategy = LadderReentry::new(cfg());
        let rejected = QuoteIntent {
            symbol: symbol.clone(),
            side: Side::Bid,
            price: Price(Decimal::from(99_950)),
            size: Size(Decimal::new(1, 3)),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        };

        let actions = strategy.on_quote_rejected(
            &ctx(&symbol, &snapshot, &position, &[]),
            &rejected,
            "binance error (code -2019): Margin is insufficient.",
        );

        match &actions[0] {
            Action::Quote(q) => {
                assert_eq!(q.side, Side::Ask);
                assert_eq!(q.price.0, Decimal::from(101_000));
                assert!(q.size.0 <= position.size.0);
            }
            _ => panic!("expected rebalance quote"),
        }
    }

    #[test]
    fn margin_reject_short_places_position_reducing_buy() {
        let symbol = sym();
        let snapshot = book(&symbol);
        let position = pos_with_size(&symbol, Decimal::new(-2, 3));
        let mut strategy = LadderReentry::new(cfg());
        let rejected = QuoteIntent {
            symbol: symbol.clone(),
            side: Side::Ask,
            price: Price(Decimal::from(100_050)),
            size: Size(Decimal::new(1, 3)),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        };

        let actions = strategy.on_quote_rejected(
            &ctx(&symbol, &snapshot, &position, &[]),
            &rejected,
            "margin insufficient (paper -2019)",
        );

        match &actions[0] {
            Action::Quote(q) => {
                assert_eq!(q.side, Side::Bid);
                assert_eq!(q.price.0, Decimal::from(99_000));
                assert!(q.size.0 <= position.size.0.abs());
            }
            _ => panic!("expected rebalance quote"),
        }
    }
}
