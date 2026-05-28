//! Mantis — a symmetric touch scalper.
//!
//! Rests one post-only bid and one post-only ask at (or near) the book touch
//! whenever the book spread is wide enough, and waits — like a mantis — for
//! price to come to it. It does not chase: a side is only re-quoted when its
//! target price moves or the side fills, so resting orders fill with zero
//! submission latency (the latency-robust property of pre-placement).
//!
//! Placement is controlled by `tick_offset` (in ticks from the touch):
//! - `0` (default) — **join** the touch (`bid = best_bid`, `ask = best_ask`).
//!   Needs no tick size; the book's own prices are already tick-aligned.
//! - `-1` — **inside / outbid** (`bid = best_bid + 1 tick`,
//!   `ask = best_ask − 1 tick`). More aggressive; needs spread ≥ ~2 ticks.
//! - `+1`, `+2`, … — **outside** (`bid = best_bid − N tick`,
//!   `ask = best_ask + N tick`). Owns its own level, deeper in the queue.
//!
//! Only quotes when book spread ≥ `min_spread_bps`; otherwise it cancels both
//! sides and waits. An optional `max_position_usdt` cap suppresses the
//! inventory-deepening side once breached (one-sided).

use tikr_core::{Decimal, MarketEvent, Price, Side, Size};

use crate::{Action, Strategy, StrategyContext, make_post_only_intent};

/// Configuration for [`Mantis`].
#[derive(Debug, Clone)]
pub struct MantisConfig {
    /// Fiat notional per order.
    pub notional_per_order: Decimal,
    /// Venue tick size (price increment). Auto-detected from the symbol's
    /// exchange filters by the caller — operators don't hand-set it. Only
    /// used when `tick_offset != 0`; at the default join it's a non-factor.
    pub tick_size: Decimal,
    /// Venue lot step size (quantity rounding).
    pub step_size: Decimal,
    /// Minimum order notional (price × size) required by the venue.
    pub min_notional: Decimal,
    /// Minimum book spread in bps required to quote. Below this, both sides
    /// are cancelled and the strategy waits.
    pub min_spread_bps: Decimal,
    /// Tick offset from the touch. `0` = join, `-1` = inside/outbid,
    /// `+1` = one tick outside. See module docs.
    pub tick_offset: i32,
    /// Max signed position notional (quote currency) before the deepening
    /// side is suppressed. `0` = uncapped.
    pub max_position_usdt: Decimal,
}

/// Symmetric touch-scalping strategy. See module docs.
pub struct Mantis {
    config: MantisConfig,
}

impl Mantis {
    /// Round a notional into a venue-valid order size: `notional / price`
    /// floored to `step_size`, bumped up to meet `min_notional`.
    fn quote_size(&self, price: Price) -> Size {
        if price.0 <= Decimal::ZERO {
            return Size(Decimal::ZERO);
        }
        let raw = self.config.notional_per_order / price.0;
        let step = self.config.step_size;
        let mut sized = if step > Decimal::ZERO {
            (raw / step).floor() * step
        } else {
            raw
        };
        let min = self.config.min_notional;
        if min > Decimal::ZERO && sized * price.0 < min && step > Decimal::ZERO {
            sized = (min / price.0 / step).ceil() * step;
        }
        Size(sized)
    }

    /// Cancel every resting quote (used when the spread gate closes). Returns
    /// an empty vec when nothing is resting, so we don't spam CancelAll.
    fn cancel_all_if_resting(ctx: &StrategyContext<'_>) -> Vec<Action> {
        if ctx.open_quotes.is_empty() {
            Vec::new()
        } else {
            vec![Action::CancelAll]
        }
    }

    /// Maintain exactly one resting post-only order on `side` at `target`:
    /// keep an existing quote already at `target`, cancel any others on this
    /// side, and place a fresh one if none is at `target`. When `suppress`,
    /// cancel the side entirely (no placement).
    fn maintain_side(
        &self,
        ctx: &StrategyContext<'_>,
        side: Side,
        target: Price,
        suppress: bool,
        actions: &mut Vec<Action>,
    ) {
        let mut kept = false;
        for (id, intent) in ctx.open_quotes {
            if intent.side != side {
                continue;
            }
            if !suppress && !kept && intent.price == target {
                kept = true; // already resting at the target — leave it
            } else {
                actions.push(Action::Cancel(*id));
            }
        }
        if suppress || kept {
            return;
        }
        let size = self.quote_size(target);
        if size.0 > Decimal::ZERO {
            actions.push(Action::Quote(make_post_only_intent(
                ctx.symbol, side, target, size,
            )));
        }
    }
}

impl Strategy for Mantis {
    type Config = MantisConfig;

    fn new(config: Self::Config) -> Self {
        Self { config }
    }

    fn name(&self) -> &str {
        "mantis"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, _event: &MarketEvent) -> Vec<Action> {
        // Re-evaluated on every event against the latest book; idempotent when
        // nothing changed (targets unmoved → no actions). A Fill event drops
        // the filled side from open_quotes, so this naturally refills it.
        let (Some(best_bid), Some(best_ask)) = (
            ctx.latest_book.bids.first().map(|l| l.price),
            ctx.latest_book.asks.first().map(|l| l.price),
        ) else {
            return Vec::new();
        };
        if best_bid.0 <= Decimal::ZERO || best_ask.0 <= best_bid.0 {
            return Vec::new();
        }

        let mid = (best_bid.0 + best_ask.0) / Decimal::from(2);
        let spread_bps = (best_ask.0 - best_bid.0) / mid * Decimal::from(10_000);
        if spread_bps < self.config.min_spread_bps {
            return Self::cancel_all_if_resting(ctx);
        }

        // Targets: offset is in ticks, positive = deeper (away from mid).
        let off = Decimal::from(self.config.tick_offset) * self.config.tick_size;
        let bid_target = Price(best_bid.0 - off);
        let ask_target = Price(best_ask.0 + off);

        // Post-only cross guards: a bid must stay below best_ask, an ask above
        // best_bid, and the pair must not lock/cross each other. If the offset
        // makes the book too tight to honour this, cancel both and wait.
        let bid_ok = bid_target.0 > Decimal::ZERO && bid_target.0 < best_ask.0;
        let ask_ok = ask_target.0 > best_bid.0;
        if !bid_ok || !ask_ok || bid_target.0 >= ask_target.0 {
            return Self::cancel_all_if_resting(ctx);
        }

        // One-sided inventory suppression.
        let pos_notional = ctx.position.size.0 * mid;
        let cap = self.config.max_position_usdt;
        let suppress_bid = cap > Decimal::ZERO && pos_notional > cap;
        let suppress_ask = cap > Decimal::ZERO && pos_notional < -cap;

        let mut actions = Vec::new();
        self.maintain_side(ctx, Side::Bid, bid_target, suppress_bid, &mut actions);
        self.maintain_side(ctx, Side::Ask, ask_target, suppress_ask, &mut actions);
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

    fn on_max_position_updated(
        &mut self,
        _ctx: &StrategyContext<'_>,
        max_position_usdt: Decimal,
    ) -> Vec<Action> {
        if max_position_usdt > Decimal::ZERO {
            self.config.max_position_usdt = max_position_usdt;
        }
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::{
        Asset, Level, MarketKind, Notional, Position, QuoteKind, SignedSize, Snapshot, Symbol,
        TimeInForce, Timestamp, VenueId,
    };
    use tikr_venue::{QuoteId, QuoteIntent};

    fn sym() -> Symbol {
        Symbol {
            base: Asset::new("SOL"),
            quote: Asset::new("USDC"),
            venue: VenueId::new("test"),
            kind: MarketKind::Perp,
        }
    }

    fn cfg(min_spread_bps: i64, tick_offset: i32) -> MantisConfig {
        MantisConfig {
            notional_per_order: Decimal::from(10),
            tick_size: Decimal::new(1, 2), // 0.01
            step_size: Decimal::new(1, 2), // 0.01
            min_notional: Decimal::from(5),
            min_spread_bps: Decimal::from(min_spread_bps),
            tick_offset,
            max_position_usdt: Decimal::ZERO,
        }
    }

    fn book(bid: &str, ask: &str) -> Snapshot {
        use std::str::FromStr;
        Snapshot {
            symbol: sym(),
            bids: vec![Level {
                price: Price(Decimal::from_str(bid).unwrap()),
                size: Size(Decimal::from(100)),
            }],
            asks: vec![Level {
                price: Price(Decimal::from_str(ask).unwrap()),
                size: Size(Decimal::from(100)),
            }],
            ts: Timestamp(0),
        }
    }

    fn flat() -> Position {
        Position {
            symbol: sym(),
            size: SignedSize(Decimal::ZERO),
            avg_entry: Price(Decimal::ZERO),
            realized_pnl: Notional(Decimal::ZERO),
        }
    }

    fn ctx<'a>(
        s: &'a Symbol,
        pos: &'a Position,
        bk: &'a Snapshot,
        open: &'a [(QuoteId, QuoteIntent)],
    ) -> StrategyContext<'a> {
        StrategyContext {
            symbol: s,
            now: Timestamp(0),
            position: pos,
            recent_fills: &[],
            latest_book: bk,
            open_quotes: open,
            recent_liqs: &[],
        }
    }

    fn ev() -> MarketEvent {
        MarketEvent::Heartbeat { ts: Timestamp(0) }
    }

    #[test]
    fn join_places_at_touch_both_sides() {
        let s = sym();
        let p = flat();
        // mid 100, spread 0.1 → 10 bps ≥ 1 → quote. Join (offset 0).
        let bk = book("100.00", "100.10");
        let mut m = Mantis::new(cfg(1, 0));
        let acts = m.on_event(&ctx(&s, &p, &bk, &[]), &ev());
        let quotes: Vec<_> = acts
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) => Some(q),
                _ => None,
            })
            .collect();
        assert_eq!(quotes.len(), 2, "one bid + one ask");
        let bid = quotes.iter().find(|q| q.side == Side::Bid).unwrap();
        let ask = quotes.iter().find(|q| q.side == Side::Ask).unwrap();
        assert_eq!(bid.price.0, Decimal::from_str("100.00").unwrap()); // joins best bid
        assert_eq!(ask.price.0, Decimal::from_str("100.10").unwrap()); // joins best ask
        assert!(matches!(bid.tif, TimeInForce::PostOnly));
        assert!(matches!(bid.kind, QuoteKind::Point));
    }

    use std::str::FromStr;

    #[test]
    fn inside_offset_steps_one_tick_in() {
        let s = sym();
        let p = flat();
        let bk = book("100.00", "100.10"); // 10-tick-wide @ 0.01 tick
        let mut m = Mantis::new(cfg(1, -1));
        let acts = m.on_event(&ctx(&s, &p, &bk, &[]), &ev());
        let bid = acts.iter().find_map(|a| match a {
            Action::Quote(q) if q.side == Side::Bid => Some(q),
            _ => None,
        });
        let ask = acts.iter().find_map(|a| match a {
            Action::Quote(q) if q.side == Side::Ask => Some(q),
            _ => None,
        });
        // -1 = outbid: bid one tick above best bid, ask one tick below best ask.
        assert_eq!(bid.unwrap().price.0, Decimal::from_str("100.01").unwrap());
        assert_eq!(ask.unwrap().price.0, Decimal::from_str("100.09").unwrap());
    }

    #[test]
    fn below_min_spread_cancels_when_resting_else_noop() {
        let s = sym();
        let p = flat();
        // mid 100, spread 0.01 → 1 bp. Require 5 bps → gate closed.
        let bk = book("100.00", "100.01");
        let mut m = Mantis::new(cfg(5, 0));
        // No resting orders → nothing to do.
        assert!(m.on_event(&ctx(&s, &p, &bk, &[]), &ev()).is_empty());
        // With a resting order → CancelAll.
        let open = vec![(
            QuoteId::new(),
            QuoteIntent {
                symbol: s.clone(),
                side: Side::Bid,
                price: Price(Decimal::from_str("100.00").unwrap()),
                size: Size(Decimal::from_str("0.1").unwrap()),
                tif: TimeInForce::PostOnly,
                kind: QuoteKind::Point,
            },
        )];
        let acts = m.on_event(&ctx(&s, &p, &bk, &open), &ev());
        assert!(matches!(acts.as_slice(), [Action::CancelAll]));
    }

    #[test]
    fn keeps_resting_quote_at_target_no_churn() {
        let s = sym();
        let p = flat();
        let bk = book("100.00", "100.10");
        let mut m = Mantis::new(cfg(1, 0));
        // Both sides already resting at the join targets → no actions.
        let open = vec![
            (
                QuoteId::new(),
                QuoteIntent {
                    symbol: s.clone(),
                    side: Side::Bid,
                    price: Price(Decimal::from_str("100.00").unwrap()),
                    size: Size(Decimal::from_str("0.1").unwrap()),
                    tif: TimeInForce::PostOnly,
                    kind: QuoteKind::Point,
                },
            ),
            (
                QuoteId::new(),
                QuoteIntent {
                    symbol: s.clone(),
                    side: Side::Ask,
                    price: Price(Decimal::from_str("100.10").unwrap()),
                    size: Size(Decimal::from_str("0.1").unwrap()),
                    tif: TimeInForce::PostOnly,
                    kind: QuoteKind::Point,
                },
            ),
        ];
        assert!(m.on_event(&ctx(&s, &p, &bk, &open), &ev()).is_empty());
    }

    #[test]
    fn refills_missing_side_after_fill() {
        let s = sym();
        let p = flat();
        let bk = book("100.00", "100.10");
        let mut m = Mantis::new(cfg(1, 0));
        // Only the ask is resting (bid filled) → place a fresh bid, keep ask.
        let open = vec![(
            QuoteId::new(),
            QuoteIntent {
                symbol: s.clone(),
                side: Side::Ask,
                price: Price(Decimal::from_str("100.10").unwrap()),
                size: Size(Decimal::from_str("0.1").unwrap()),
                tif: TimeInForce::PostOnly,
                kind: QuoteKind::Point,
            },
        )];
        let acts = m.on_event(&ctx(&s, &p, &bk, &open), &ev());
        assert_eq!(acts.len(), 1);
        assert!(matches!(&acts[0], Action::Quote(q) if q.side == Side::Bid));
    }

    #[test]
    fn position_cap_suppresses_deepening_side() {
        let s = sym();
        // Long 1 @ mid 100 → pos_notional 100 > cap 50 → suppress bids.
        let p = Position {
            symbol: s.clone(),
            size: SignedSize(Decimal::from(1)),
            avg_entry: Price(Decimal::from(100)),
            realized_pnl: Notional(Decimal::ZERO),
        };
        let bk = book("100.00", "100.10");
        let mut c = cfg(1, 0);
        c.max_position_usdt = Decimal::from(50);
        let mut m = Mantis::new(c);
        let acts = m.on_event(&ctx(&s, &p, &bk, &[]), &ev());
        // Only the ask (reducing side) should be placed; no bid.
        let sides: Vec<Side> = acts
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) => Some(q.side),
                _ => None,
            })
            .collect();
        assert_eq!(sides, vec![Side::Ask]);
    }
}
