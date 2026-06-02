//! Volley — timed batched liquidity spray.
//!
//! Every `interval_secs`, fire a "volley": place `levels` bids and `levels` asks
//! as a fence, WITHOUT cancelling anything first. The first order on each side
//! sits `inner_ticks` off the touch (a dead-zone), and consecutive orders are
//! `step_ticks` apart:
//!
//! ```text
//!   bid[i] = best_bid - (inner_ticks + i*step_ticks) * tick   for i in 0..levels
//!   ask[i] = best_ask + (inner_ticks + i*step_ticks) * tick
//! ```
//!
//! Nothing is cancelled — each volley ADDS `levels` fresh orders per side, so
//! resting depth accumulates over time (a high-frequency liquidity provider that
//! keeps refreshing queue position with new orders rather than re-pricing old
//! ones). On a static book this stacks duplicate-price orders; as the touch
//! moves, new volleys land at the new touch.
//!
//! ⚠ With no cancel, open orders grow until the venue's per-symbol cap (~200 on
//! Binance USD-M) — past that, placements reject. Bound it with the account-layer
//! `max_position_pct` (inventory) and a tolerable `levels × interval` rate; an
//! age-based reaper can be added if you need the wall to self-trim.
//!
//! Post-only (maker). No inventory cap inside the strategy. Best on
//! zero/low-fee venues. State is just the last-volley timestamp.

use tikr_core::{Decimal, MarketEvent, Price, QuoteKind, Side, Size, Symbol, TimeInForce};
use tikr_venue::QuoteIntent;

use crate::{Action, Strategy, StrategyContext};

/// Configuration for [`Volley`].
#[derive(Debug, Clone)]
pub struct VolleyConfig {
    /// Notional in quote currency per order. Quantity = `notional / price`,
    /// floored to `step_size`, bumped to `min_notional` when below.
    pub notional_per_order: Decimal,
    /// Venue tick size — the unit for `inner_ticks` / `step_ticks`.
    pub tick_size: Decimal,
    /// Venue lot step.
    pub step_size: Decimal,
    /// Venue min order notional in quote currency.
    pub min_notional: Decimal,
    /// Orders per side per volley. Default 10.
    pub levels: u32,
    /// Fire a fresh volley (cancel all + re-place) this often, in seconds.
    /// `0` = every event. Default 1.
    pub interval_secs: u32,
    /// Tick gap between consecutive orders on a side. `1` = 1 tick apart.
    /// Default 1.
    pub step_ticks: u32,
    /// Dead-zone in ticks: the first order on each side sits this many ticks off
    /// the touch (bid below best_bid, ask above best_ask). `0` = start at the
    /// touch. Default 5.
    pub inner_ticks: u32,
}

/// Volley strategy state.
pub struct Volley {
    config: VolleyConfig,
    /// ctx.now.0 (ns) of the last volley; gates `interval_secs`.
    last_volley_ns: Option<u64>,
}

impl Volley {
    /// Order size for `price`: notional / price, floored to the lot step and
    /// bumped to `min_notional`.
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
            let mut needed = (min / price.0 / self.config.step_size).ceil() * self.config.step_size;
            // Chained Decimal divisions can land one lot short of min; bump up.
            while needed * price.0 < min {
                needed += self.config.step_size;
            }
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
}

impl Strategy for Volley {
    type Config = VolleyConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            last_volley_ns: None,
        }
    }

    fn name(&self) -> &str {
        "volley"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, _event: &MarketEvent) -> Vec<Action> {
        let tick = self.config.tick_size;
        let best_bid = ctx.latest_book.bids.first().map(|l| l.price);
        let best_ask = ctx.latest_book.asks.first().map(|l| l.price);

        // Gate on the volley interval.
        let interval_ns = u64::from(self.config.interval_secs) * 1_000_000_000;
        let due = self
            .last_volley_ns
            .is_none_or(|t| ctx.now.0.saturating_sub(t) >= interval_ns);
        if !due || tick <= Decimal::ZERO {
            return vec![Action::NoOp];
        }
        let (Some(bp), Some(ap)) = (best_bid, best_ask) else {
            return vec![Action::NoOp];
        };
        if bp.0 <= Decimal::ZERO || ap.0 <= bp.0 {
            return vec![Action::NoOp];
        }

        self.last_volley_ns = Some(ctx.now.0);
        let levels = self.config.levels.max(1) as i64;
        let inner = i64::from(self.config.inner_ticks);
        let step = i64::from(self.config.step_ticks.max(1));

        // Fresh volley: ADD `levels` orders per side at the current touch — no
        // cancel, so resting depth accumulates.
        let mut actions: Vec<Action> = Vec::new();
        for i in 0..levels {
            let off = Decimal::from(inner + i * step) * tick;
            // Bid below the touch; never cross to/above the ask.
            let bid_px = bp.0 - off;
            if bid_px > Decimal::ZERO && bid_px < ap.0 {
                actions.push(self.make_quote(ctx.symbol, Side::Bid, Price(bid_px)));
            }
            // Ask above the touch; never cross to/below the bid.
            let ask_px = ap.0 + off;
            if ask_px > bp.0 {
                actions.push(self.make_quote(ctx.symbol, Side::Ask, Price(ask_px)));
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
    use tikr_core::{Asset, Level, MarketKind, Position, SignedSize, Snapshot, Timestamp, VenueId};
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

    fn cfg() -> VolleyConfig {
        VolleyConfig {
            notional_per_order: Decimal::from(100),
            tick_size: Decimal::new(1, 1), // 0.1
            step_size: Decimal::new(1, 3), // 0.001
            min_notional: Decimal::from(5),
            levels: 10,
            interval_secs: 1,
            step_ticks: 1,
            inner_ticks: 5,
        }
    }

    fn ctx<'a>(
        symbol: &'a Symbol,
        snap: &'a Snapshot,
        p: &'a Position,
        open: &'a [(QuoteId, QuoteIntent)],
        now_ns: u64,
    ) -> StrategyContext<'a> {
        StrategyContext {
            symbol,
            now: Timestamp(now_ns),
            position: p,
            recent_fills: &[],
            latest_book: snap,
            open_quotes: open,
            recent_liqs: &[],
        }
    }

    #[test]
    fn volley_places_levels_per_side_without_cancelling() {
        let mut s = Volley::new(cfg());
        let snap = book(Decimal::from(100), Decimal::new(1001, 1)); // bid 100, ask 100.1
        let p = pos();
        let symbol = sym();
        let actions = s.on_event(
            &ctx(&symbol, &snap, &p, &[], 0),
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        // No cancel — pure adds.
        assert!(
            !actions.iter().any(|a| matches!(a, Action::CancelAll)),
            "volley must NOT cancel"
        );
        let bids = actions
            .iter()
            .filter(|a| matches!(a, Action::Quote(q) if q.side == Side::Bid))
            .count();
        let asks = actions
            .iter()
            .filter(|a| matches!(a, Action::Quote(q) if q.side == Side::Ask))
            .count();
        assert_eq!((bids, asks), (10, 10), "exactly `levels` per side");
    }

    #[test]
    fn inner_and_step_geometry() {
        // inner_ticks=5, step_ticks=1, tick=0.1 → first bid 0.5 below 100 = 99.5,
        // next 99.4, … ; first ask 0.5 above 100.1 = 100.6, next 100.7, …
        let mut s = Volley::new(cfg());
        let snap = book(Decimal::from(100), Decimal::new(1001, 1));
        let p = pos();
        let symbol = sym();
        let actions = s.on_event(
            &ctx(&symbol, &snap, &p, &[], 0),
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        let bid_prices: Vec<Decimal> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Bid => Some(q.price.0),
                _ => None,
            })
            .collect();
        assert_eq!(bid_prices[0], Decimal::new(995, 1)); // 99.5
        assert_eq!(bid_prices[1], Decimal::new(994, 1)); // 99.4
        let ask_prices: Vec<Decimal> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Ask => Some(q.price.0),
                _ => None,
            })
            .collect();
        assert_eq!(ask_prices[0], Decimal::new(1006, 1)); // 100.6
        assert_eq!(ask_prices[1], Decimal::new(1007, 1)); // 100.7
    }

    #[test]
    fn respects_interval() {
        let mut s = Volley::new(cfg());
        let snap = book(Decimal::from(100), Decimal::new(1001, 1));
        let p = pos();
        let symbol = sym();
        let fired = |a: &[Action]| a.iter().any(|x| matches!(x, Action::Quote(_)));
        // t=0: fires (places orders).
        let a0 = s.on_event(
            &ctx(&symbol, &snap, &p, &[], 0),
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        assert!(fired(&a0));
        // t=0.5s (< 1s interval): NoOp, no new volley.
        let a1 = s.on_event(
            &ctx(&symbol, &snap, &p, &[], 500_000_000),
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        assert!(!fired(&a1));
        // t=1.0s: fires again.
        let a2 = s.on_event(
            &ctx(&symbol, &snap, &p, &[], 1_000_000_000),
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        assert!(fired(&a2));
    }
}
