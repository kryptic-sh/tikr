//! Strangler: a plain tick-spaced lattice window that stays full.
//!
//! Dead simple, blind, place-only. Each side targets `levels` post-only orders
//! on the tick grid: the first `inner_ticks` from mid, then every `step_ticks`
//! deeper. There is NO skew, inventory logic, or refill threshold; it does NOT
//! inspect resting orders; fills are IGNORED; and it NEVER cancels. Once a
//! second it simply fires the FULL target grid off the current mid — the venue
//! (post-only / max-open-orders) handles any duplicate or over-cap orders.
//!
//! Reconcile fires at most once per second (gated on the event timestamp), on a
//! book update or heartbeat. `Fill` and `Trade` events are no-ops.
//!
//! NOTE: because it never cancels and re-fires blindly, resting orders
//! accumulate as the mid moves — size `levels` / cadence with the venue's
//! open-order cap in mind.
//!
//! ## Knobs
//! - `levels` — orders per side.
//! - `step_ticks` — ticks between consecutive levels (min 1).
//! - `inner_ticks` — ticks from mid to the first order on each side (`0` = at
//!   the mid tick).

use tikr_core::{Decimal, MarketEvent, Price, QuoteKind, Side, Size, Symbol, TimeInForce};
use tikr_venue::QuoteIntent;

use crate::{Action, Strategy, StrategyContext};

/// Reconcile cadence: rebuild the order batch at most once per second.
const RECONCILE_INTERVAL_NS: u64 = 1_000_000_000;

/// Configuration for [`Strangler`].
#[derive(Debug, Clone)]
pub struct StranglerConfig {
    /// Notional in quote currency per order.
    pub notional_per_order: Decimal,
    /// Venue tick size (price increment).
    pub tick_size: Decimal,
    /// Venue lot step (quantity rounding).
    pub step_size: Decimal,
    /// Venue min order notional.
    pub min_notional: Decimal,
    /// Orders per side.
    pub levels: u32,
    /// Ticks between consecutive levels. Treated as `max(1)`.
    pub step_ticks: u32,
    /// Ticks from mid to the first order on each side. `0` = at the mid tick.
    pub inner_ticks: u32,
}

/// Strangler strategy state. Reconcile reads only the latest book (mid);
/// `last_reconcile_ns` gates the 1 s cadence.
pub struct Strangler {
    config: StranglerConfig,
    /// Event-time (ns) of the last reconcile, for the once-per-second gate.
    last_reconcile_ns: Option<u64>,
}

impl Strangler {
    /// Order size for `price`: notional / price, rounded to the lot step and
    /// floored at `min_notional` (mirrors the other lattice strategies).
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

    /// The `(side, price)` levels the window wants right now, computed off the
    /// mid snapped to the tick grid. Cross-guarded: a bid is dropped if it would
    /// sit at/above `best_ask`, an ask if at/below `best_bid` (post-only safety).
    fn targets(&self, best_bid: Decimal, best_ask: Decimal) -> Vec<(Side, Decimal)> {
        let tick = self.config.tick_size;
        if tick <= Decimal::ZERO || best_bid <= Decimal::ZERO || best_ask <= best_bid {
            return Vec::new();
        }
        let mid = (best_bid + best_ask) / Decimal::from(2);
        // Snap mid to an integer number of ticks so every level lands on grid.
        let center_ticks = (mid / tick).round();
        let levels = self.config.levels.max(1) as i64;
        let inner = i64::from(self.config.inner_ticks);
        let step = i64::from(self.config.step_ticks.max(1));
        let mut out = Vec::with_capacity((levels * 2) as usize);
        for k in 0..levels {
            let off = inner + k * step;
            let bid = (center_ticks - Decimal::from(off)) * tick;
            if bid > Decimal::ZERO && bid < best_ask {
                out.push((Side::Bid, bid));
            }
            let ask = (center_ticks + Decimal::from(off)) * tick;
            if ask > best_bid {
                out.push((Side::Ask, ask));
            }
        }
        out
    }

    /// Blindly emit the full target grid. Does NOT inspect resting orders and
    /// NEVER cancels — every cycle just fires `make_quote` for every target
    /// level off the current mid. Duplicate-price / over-cap orders are left for
    /// the venue (post-only / max-orders) to reject.
    fn reconcile(&self, ctx: &StrategyContext<'_>) -> Vec<Action> {
        let Some(best_bid) = ctx.latest_book.bids.first().map(|l| l.price.0) else {
            return Vec::new();
        };
        let Some(best_ask) = ctx.latest_book.asks.first().map(|l| l.price.0) else {
            return Vec::new();
        };
        self.targets(best_bid, best_ask)
            .into_iter()
            .map(|(side, price)| self.make_quote(ctx.symbol, side, Price(price)))
            .collect()
    }
}

impl Strategy for Strangler {
    type Config = StranglerConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            last_reconcile_ns: None,
        }
    }

    fn name(&self) -> &str {
        "strangler"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        // Fills and trades are ignored — a filled slot is replenished by the
        // next 1 s reconcile, not reactively. Only book updates / heartbeats can
        // trigger a reconcile, and only once the 1 s cadence has elapsed.
        match event {
            MarketEvent::BookUpdate { .. } | MarketEvent::Heartbeat { .. } => {}
            MarketEvent::Fill(_) | MarketEvent::Trade { .. } => return Vec::new(),
        }
        let now = ctx.now.0;
        let due = self
            .last_reconcile_ns
            .is_none_or(|last| now.saturating_sub(last) >= RECONCILE_INTERVAL_NS);
        if !due {
            return Vec::new();
        }
        self.last_reconcile_ns = Some(now);
        self.reconcile(ctx)
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
            quote: Asset::new("USDC"),
            venue: VenueId::new("test"),
            kind: MarketKind::Perp,
        }
    }

    fn cfg() -> StranglerConfig {
        StranglerConfig {
            notional_per_order: Decimal::from(50),
            tick_size: Decimal::new(1, 1), // 0.1
            step_size: Decimal::new(1, 3),
            min_notional: Decimal::from(5),
            levels: 3,
            step_ticks: 2,
            inner_ticks: 1,
        }
    }

    fn snap(bid: Decimal, ask: Decimal) -> Snapshot {
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

    fn ctx<'a>(
        symbol: &'a Symbol,
        s: &'a Snapshot,
        p: &'a Position,
        open: &'a [(QuoteId, QuoteIntent)],
    ) -> StrategyContext<'a> {
        ctx_at(symbol, s, p, open, 1)
    }

    fn ctx_at<'a>(
        symbol: &'a Symbol,
        s: &'a Snapshot,
        p: &'a Position,
        open: &'a [(QuoteId, QuoteIntent)],
        now_ns: u64,
    ) -> StrategyContext<'a> {
        StrategyContext {
            symbol,
            now: Timestamp(now_ns),
            position: p,
            recent_fills: &[],
            latest_book: s,
            open_quotes: open,
            recent_liqs: &[],
        }
    }

    /// One second + 1 ns later — past the reconcile gate.
    const NEXT: u64 = super::RECONCILE_INTERVAL_NS + 1;

    fn quotes(actions: &[Action]) -> Vec<(QuoteId, QuoteIntent)> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) => Some((QuoteId::new(), q.clone())),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn seeds_full_window_on_grid() {
        // mid 100.05 → 1000.5 ticks → round-half-to-even → 1000 → center 100.0.
        // inner=1, step=2: bids 99.9 / 99.7 / 99.5, asks 100.1 / 100.3 / 100.5.
        let mut w = Strangler::new(cfg());
        let s = snap(Decimal::from(100), Decimal::new(1001, 1)); // 100 / 100.1
        let p = pos();
        let sm = sym();
        let actions = w.on_event(
            &ctx(&sm, &s, &p, &[]),
            &MarketEvent::BookUpdate {
                snapshot: s.clone(),
            },
        );
        let bids: Vec<Decimal> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Bid => Some(q.price.0),
                _ => None,
            })
            .collect();
        let asks: Vec<Decimal> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) if q.side == Side::Ask => Some(q.price.0),
                _ => None,
            })
            .collect();
        assert_eq!(
            bids,
            vec![
                Decimal::new(999, 1),
                Decimal::new(997, 1),
                Decimal::new(995, 1)
            ]
        );
        assert_eq!(
            asks,
            vec![
                Decimal::new(1001, 1),
                Decimal::new(1003, 1),
                Decimal::new(1005, 1)
            ]
        );
        assert_eq!(actions.len(), 6, "full window = 3 bids + 3 asks");
    }

    #[test]
    fn refires_full_grid_blindly_ignoring_resting() {
        // Even with the entire window already resting, the next cycle re-fires
        // the FULL grid (no dedup against open_quotes) and never cancels.
        let mut w = Strangler::new(cfg());
        let s = snap(Decimal::from(100), Decimal::new(1001, 1));
        let p = pos();
        let sm = sym();
        let seeded = w.on_event(
            &ctx(&sm, &s, &p, &[]),
            &MarketEvent::BookUpdate {
                snapshot: s.clone(),
            },
        );
        let open = quotes(&seeded);
        assert_eq!(open.len(), 6);
        let again = w.on_event(
            &ctx_at(&sm, &s, &p, &open, NEXT),
            &MarketEvent::BookUpdate {
                snapshot: s.clone(),
            },
        );
        assert!(
            !again.iter().any(|a| matches!(a, Action::Cancel(_))),
            "strangler must never cancel: {again:?}"
        );
        assert_eq!(
            again
                .iter()
                .filter(|a| matches!(a, Action::Quote(_)))
                .count(),
            6,
            "re-fires the full grid regardless of what's resting: {again:?}"
        );
    }

    #[test]
    fn recenter_places_new_and_never_cancels() {
        let mut w = Strangler::new(cfg());
        let s0 = snap(Decimal::from(100), Decimal::new(1001, 1));
        let p = pos();
        let sm = sym();
        let seeded = w.on_event(
            &ctx(&sm, &s0, &p, &[]),
            &MarketEvent::BookUpdate {
                snapshot: s0.clone(),
            },
        );
        let open = quotes(&seeded);
        // Market jumps up ~1 unit → window shifts → new slots placed, but the
        // drifted-out orders are LEFT resting (no cancels).
        let s1 = snap(Decimal::from(101), Decimal::new(1011, 1));
        let actions = w.on_event(
            &ctx_at(&sm, &s1, &p, &open, NEXT),
            &MarketEvent::BookUpdate {
                snapshot: s1.clone(),
            },
        );
        assert!(
            !actions.iter().any(|a| matches!(a, Action::Cancel(_))),
            "strangler must never cancel: {actions:?}"
        );
        assert!(
            actions.iter().any(|a| matches!(a, Action::Quote(_))),
            "new window slots must be placed: {actions:?}"
        );
    }

    #[test]
    fn throttled_within_one_second() {
        // A book change <1 s after the seed must NOT reconcile (1 Hz gate),
        // even though the window would otherwise move.
        let mut w = Strangler::new(cfg());
        let s0 = snap(Decimal::from(100), Decimal::new(1001, 1));
        let p = pos();
        let sm = sym();
        let seeded = w.on_event(
            &ctx_at(&sm, &s0, &p, &[], 1),
            &MarketEvent::BookUpdate {
                snapshot: s0.clone(),
            },
        );
        let open = quotes(&seeded);
        let s1 = snap(Decimal::from(101), Decimal::new(1011, 1));
        let half_sec = 1 + RECONCILE_INTERVAL_NS / 2;
        let actions = w.on_event(
            &ctx_at(&sm, &s1, &p, &open, half_sec),
            &MarketEvent::BookUpdate {
                snapshot: s1.clone(),
            },
        );
        assert!(
            actions.is_empty(),
            "must not reconcile within 1 s of the last batch: {actions:?}"
        );
    }

    #[test]
    fn ignores_fills() {
        // A Fill event never triggers a reconcile (even past the 1 s gate).
        let mut w = Strangler::new(cfg());
        let s = snap(Decimal::from(100), Decimal::new(1001, 1));
        let p = pos();
        let sm = sym();
        let seeded = w.on_event(
            &ctx_at(&sm, &s, &p, &[], 1),
            &MarketEvent::BookUpdate {
                snapshot: s.clone(),
            },
        );
        // Drop a slot, then deliver a Fill well past the gate → still no-op.
        let mut open = quotes(&seeded);
        open.remove(0);
        let fill = tikr_core::Fill {
            quote_id: QuoteId::new(),
            price: Price(Decimal::new(999, 1)),
            size: Size(Decimal::new(1, 3)),
            fee_asset: tikr_core::Asset::new("USDC"),
            fee_amount: Decimal::ZERO,
            fee_quote: Notional(Decimal::ZERO),
            side: Side::Bid,
            ts: Timestamp(NEXT),
            is_full: true,
            trade_id: None,
        };
        let actions = w.on_event(&ctx_at(&sm, &s, &p, &open, NEXT), &MarketEvent::Fill(fill));
        assert!(actions.is_empty(), "fills are ignored: {actions:?}");
    }
}
