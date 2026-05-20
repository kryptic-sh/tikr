//! Layered fixed-fiat grid with re-entry scalping.
//!
//! Always maintains `2 × levels_per_side` open limit orders (3 buys + 3
//! sells by default) at geometrically-spaced prices around the current mid:
//!
//! ```text
//! sell @ mid + 12 bps  (outer)
//! sell @ mid +  9 bps
//! sell @ mid +  6 bps  (inner)
//!                    MID
//! buy  @ mid −  6 bps  (inner)
//! buy  @ mid −  9 bps
//! buy  @ mid − 12 bps  (outer)
//! ```
//!
//! Each order has a fixed **fiat notional** (e.g. `$100`); coin quantity =
//! `notional / price`. Cheaper buys naturally accumulate more coin per
//! dollar, higher sells release less coin per dollar — a built-in long
//! bias before any price movement.
//!
//! **Re-entry**: when a fill lands the strategy emits a fresh order on
//! the OPPOSITE side at the filled price ± `reentry_bps`. The 6-order
//! count stays constant.
//!
//! The strategy is fill-driven: after the cold-start placement on the
//! first BookUpdate, it only emits new orders in response to its own
//! [`MarketEvent::Fill`] events. Requires the runner to deliver fill
//! events to `on_event` (added in #TBD).
//!
//! See `crates/tikr-backtest/src/bin/backtest_layered_grid.rs` for the
//! standalone candle-based version of the same logic.

use tikr_core::{
    Decimal, MarketEvent, Price, QuoteKind, Side, Size, Symbol, TimeInForce,
};
use tikr_venue::QuoteIntent;

use crate::{Action, Strategy, StrategyContext};

/// Configuration for [`LayeredGrid`].
#[derive(Debug, Clone)]
pub struct LayeredGridConfig {
    /// Fixed fiat notional per order (e.g. `Decimal::from(100)` for $100).
    pub notional_per_order: Decimal,
    /// Number of orders per side. Total open orders = `2 × levels_per_side`.
    pub levels_per_side: u32,
    /// Inner spread from mid in bps. First buy at `mid × (1 − inner/10000)`,
    /// first sell at `mid × (1 + inner/10000)`.
    pub inner_bps: u32,
    /// Step between levels in bps. Each successive outer order is `step`
    /// further from the inner pair.
    pub step_bps: u32,
    /// Re-entry spread in bps. When a buy fills at `P`, a new sell is
    /// placed at `P × (1 + reentry/10000)`. Mirror for sell fills.
    pub reentry_bps: u32,
}

/// Layered-grid strategy state.
///
/// `placed` flips to true on the first BookUpdate (cold-start placement).
/// `orders` mirrors the prices+sides we believe are resting; we identify
/// fills by matching `(side, price)` since [`Strategy`] doesn't observe
/// the `QuoteId` assigned by the venue / FillSim.
pub struct LayeredGrid {
    config: LayeredGridConfig,
    placed: bool,
    orders: Vec<(Side, Price)>,
}

impl LayeredGrid {
    fn place_initial(&mut self, symbol: &Symbol, mid: Price) -> Vec<Action> {
        let mut actions: Vec<Action> = Vec::with_capacity(self.config.levels_per_side as usize * 2 + 1);
        // Cancel any stray quotes first (defensive — usually a no-op on cold start).
        actions.push(Action::CancelAll);
        self.orders.clear();

        let bps_to_decimal = |b: u32| Decimal::from(b) / Decimal::from(10_000);
        for k in 0..self.config.levels_per_side {
            let bps = self.config.inner_bps + self.config.step_bps * k;
            let bp_dec = bps_to_decimal(bps);

            let buy_price = Price(mid.0 * (Decimal::ONE - bp_dec));
            let sell_price = Price(mid.0 * (Decimal::ONE + bp_dec));

            actions.push(self.make_quote(symbol, Side::Bid, buy_price));
            actions.push(self.make_quote(symbol, Side::Ask, sell_price));
            self.orders.push((Side::Bid, buy_price));
            self.orders.push((Side::Ask, sell_price));
        }
        self.placed = true;
        actions
    }

    fn make_quote(&self, symbol: &Symbol, side: Side, price: Price) -> Action {
        let qty = Size(self.config.notional_per_order / price.0);
        Action::Quote(QuoteIntent {
            symbol: symbol.clone(),
            side,
            price,
            size: qty,
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        })
    }

    fn on_fill(&mut self, symbol: &Symbol, fill_side: Side, fill_price: Price) -> Vec<Action> {
        // Drop the matching order from our local mirror (closest matching price
        // on the filled side). Use closest match since fill_price may equal the
        // resting limit price exactly OR be the touch where a marketable order
        // crossed — both should resolve to the inner-most matching order.
        if let Some(pos) = self.orders.iter().position(|(s, p)| *s == fill_side && *p == fill_price) {
            self.orders.remove(pos);
        } else if let Some(pos) = self
            .orders
            .iter()
            .enumerate()
            .filter(|(_, (s, _))| *s == fill_side)
            .min_by(|(_, (_, a)), (_, (_, b))| {
                let da = (a.0 - fill_price.0).abs();
                let db = (b.0 - fill_price.0).abs();
                da.cmp(&db)
            })
            .map(|(i, _)| i)
        {
            self.orders.remove(pos);
        }

        // Re-enter on the opposite side at fill_price ± reentry_bps.
        let reentry_dec = Decimal::from(self.config.reentry_bps) / Decimal::from(10_000);
        let (new_side, new_price) = match fill_side {
            Side::Bid => (Side::Ask, Price(fill_price.0 * (Decimal::ONE + reentry_dec))),
            Side::Ask => (Side::Bid, Price(fill_price.0 * (Decimal::ONE - reentry_dec))),
        };
        self.orders.push((new_side, new_price));
        vec![self.make_quote(symbol, new_side, new_price)]
    }
}

impl Strategy for LayeredGrid {
    type Config = LayeredGridConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            placed: false,
            orders: Vec::new(),
        }
    }

    fn name(&self) -> &str {
        "layered-grid"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        match event {
            MarketEvent::BookUpdate { snapshot } if !self.placed => {
                let bid = snapshot.bids.first().map(|l| l.price.0);
                let ask = snapshot.asks.first().map(|l| l.price.0);
                let (Some(b), Some(a)) = (bid, ask) else {
                    return Vec::new();
                };
                let mid = Price((b + a) / Decimal::from(2));
                self.place_initial(ctx.symbol, mid)
            }
            MarketEvent::Fill(f) if f.symbol_matches(ctx.symbol) => {
                self.on_fill(ctx.symbol, f.side, f.price)
            }
            // After cold-start, BookUpdate / Trade / Heartbeat don't change
            // grid state. The strategy is purely fill-driven.
            _ => Vec::new(),
        }
    }
}

/// Helper: does the [`tikr_core::Fill`] belong to our symbol? FillSim
/// doesn't currently stamp Fill.symbol — it lives implicitly with the
/// runner — so this is a no-op stub that returns true. Kept as an
/// extension point if/when Fill gets a symbol field.
trait FillSymbolExt {
    fn symbol_matches(&self, sym: &Symbol) -> bool;
}

impl FillSymbolExt for tikr_core::Fill {
    fn symbol_matches(&self, _sym: &Symbol) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::{Asset, Fill, Level, MarketKind, Notional, Position, SignedSize, Snapshot, Timestamp, VenueId};
    use tikr_venue::QuoteId;

    fn sym() -> Symbol {
        Symbol {
            base: Asset::new("BTC"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("test"),
            kind: MarketKind::Perp,
        }
    }

    fn flat_pos(s: &Symbol) -> Position {
        Position {
            symbol: s.clone(),
            size: SignedSize(Decimal::ZERO),
            avg_entry: Price(Decimal::ZERO),
            realized_pnl: Notional(Decimal::ZERO),
        }
    }

    fn book(s: &Symbol, bid: i64, ask: i64) -> Snapshot {
        Snapshot {
            symbol: s.clone(),
            bids: vec![Level {
                price: Price(Decimal::from(bid)),
                size: Size(Decimal::from(1)),
            }],
            asks: vec![Level {
                price: Price(Decimal::from(ask)),
                size: Size(Decimal::from(1)),
            }],
            ts: Timestamp(0),
        }
    }

    fn cfg() -> LayeredGridConfig {
        LayeredGridConfig {
            notional_per_order: Decimal::from(100),
            levels_per_side: 3,
            inner_bps: 6,
            step_bps: 3,
            reentry_bps: 3,
        }
    }

    fn ctx<'a>(s: &'a Symbol, p: &'a Position, snap: &'a Snapshot) -> StrategyContext<'a> {
        StrategyContext {
            symbol: s,
            now: snap.ts,
            position: p,
            recent_fills: &[],
            latest_book: snap,
            open_quotes: &[],
        }
    }

    #[test]
    fn cold_start_places_six_orders() {
        let s = sym();
        let snap = book(&s, 100, 101); // mid = 100.5
        let p = flat_pos(&s);
        let c = ctx(&s, &p, &snap);
        let mut strat = LayeredGrid::new(cfg());
        let actions = strat.on_event(&c, &MarketEvent::BookUpdate { snapshot: snap.clone() });
        // 1 CancelAll + 6 Quote
        assert_eq!(actions.len(), 7);
        assert!(matches!(actions[0], Action::CancelAll));
        let quotes: Vec<&QuoteIntent> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) => Some(q),
                _ => None,
            })
            .collect();
        assert_eq!(quotes.len(), 6);
        let buys = quotes.iter().filter(|q| q.side == Side::Bid).count();
        let asks = quotes.iter().filter(|q| q.side == Side::Ask).count();
        assert_eq!(buys, 3);
        assert_eq!(asks, 3);
    }

    #[test]
    fn second_bookupdate_emits_nothing() {
        let s = sym();
        let snap = book(&s, 100, 101);
        let p = flat_pos(&s);
        let c = ctx(&s, &p, &snap);
        let mut strat = LayeredGrid::new(cfg());
        let _ = strat.on_event(&c, &MarketEvent::BookUpdate { snapshot: snap.clone() });
        // Second event should be no-op (grid is purely fill-driven after cold start).
        let actions = strat.on_event(&c, &MarketEvent::BookUpdate { snapshot: snap.clone() });
        assert!(actions.is_empty());
    }

    #[test]
    fn fill_emits_reentry_on_opposite_side() {
        let s = sym();
        let snap = book(&s, 1000, 1001);
        let p = flat_pos(&s);
        let c = ctx(&s, &p, &snap);
        let mut strat = LayeredGrid::new(cfg());
        let _ = strat.on_event(&c, &MarketEvent::BookUpdate { snapshot: snap.clone() });
        // Simulate a buy fill at the inner buy price ≈ 1000.5 × (1 − 6bps) = 999.9003
        // We compute it the same way the strategy did:
        let mid = Decimal::new(10005, 1); // 1000.5
        let inner_bp = Decimal::from(6) / Decimal::from(10_000);
        let buy_price = Price(mid * (Decimal::ONE - inner_bp));
        let fill = Fill {
            quote_id: QuoteId::new(),
            price: buy_price,
            size: Size(Decimal::new(1, 4)),
            fee_asset: s.quote.clone(),
            fee_amount: Decimal::ZERO,
            fee_quote: Notional(Decimal::ZERO),
            side: Side::Bid,
            ts: Timestamp(1000),
        };
        let actions = strat.on_event(&c, &MarketEvent::Fill(fill));
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::Quote(q) => {
                assert_eq!(q.side, Side::Ask);
                // Re-entry should be ~3bps above the fill price
                let expected = buy_price.0 * (Decimal::ONE + Decimal::from(3) / Decimal::from(10_000));
                assert_eq!(q.price.0, expected);
            }
            _ => panic!("expected Quote"),
        }
    }
}
