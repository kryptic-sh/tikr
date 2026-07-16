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

/// Grace window during which a just-emitted (side, price) placement counts
/// as coverage even though it hasn't shown up in `ctx.open_quotes` yet
/// (venue ack / runner reconciliation lags the emit by at least one
/// event). Used by both the dedupe check (`already_at`) and the age
/// sweep's `placement_ts` pruning — see their doc comments.
const IN_FLIGHT_GRACE_NS: u64 = 10_000_000_000; // 10s

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
    /// Tick offset from current best:
    /// - `-1` = improve (BID = best_bid + tick, ASK = best_ask - tick)
    /// - `0`  = join touch (BID = best_bid, ASK = best_ask)
    /// - `1+` = lag behind by N ticks (BID = best_bid - N*tick,
    ///   ASK = best_ask + N*tick)
    pub order_tick_offset: i32,
    /// Skip an emit if any open same-side order sits within this many
    /// ticks of the target price. `0` = exact-price dedupe only. `1+`
    /// stops the joker from spamming nearby orders when the book
    /// wiggles a tick — a single resting order covers an N-tick band.
    pub order_tick_tolerance: u32,
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

    /// True if `side` already has coverage near `price` — either a
    /// resting order in `ctx.open_quotes`, or a just-emitted placement
    /// (tracked in `placement_ts`) that hasn't shown up there yet.
    /// `open_quotes` lags in-flight placements by at least one event;
    /// checking it alone let the same touch get re-emitted every event
    /// until the venue ack landed — duplicate resting orders. A
    /// `placement_ts` entry counts as coverage until it either appears in
    /// `open_quotes` or `IN_FLIGHT_GRACE_NS` elapses (safety valve: if the
    /// order never lands — rejected, lost — don't suppress emits at that
    /// price forever).
    fn already_at(&self, ctx: &StrategyContext<'_>, side: Side, price: Price) -> bool {
        let tol = Decimal::from(self.config.order_tick_tolerance) * self.config.tick_size;
        let lo = price.0 - tol;
        let hi = price.0 + tol;
        let resting = ctx
            .open_quotes
            .iter()
            .any(|(_, q)| q.side == side && q.price.0 >= lo && q.price.0 <= hi);
        if resting {
            return true;
        }
        self.placement_ts.iter().any(|((s, p), ts)| {
            *s == side
                && *p >= lo
                && *p <= hi
                && ctx.now.0.saturating_sub(ts.0) < IN_FLIGHT_GRACE_NS
        })
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
            // quote (filled, externally cancelled) — EXCEPT ones still
            // inside the in-flight grace window: those are placements
            // just emitted this session that haven't shown up in
            // `open_quotes` yet. Dropping them unconditionally (as
            // before) permanently exempted them from this very age sweep
            // once they did land, since the sweep only checks quotes with
            // a `placement_ts` record.
            self.placement_ts.retain(|k, ts| {
                open_keys.contains(k) || now_ns.saturating_sub(ts.0) < IN_FLIGHT_GRACE_NS
            });
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

        // Per-side offset (signed ticks). Negative improves (in front of
        // best), 0 joins, positive lags behind best.
        let off = self.config.order_tick_offset;
        let offset_dec = Decimal::from(off.unsigned_abs() as u64) * tick;

        if let Some(bp) = best_bid
            && bp.0 > Decimal::ZERO
            && tick > Decimal::ZERO
        {
            // BID with offset O: price = best_bid - O*tick.
            // O=-1 → above best_bid (improve); O=0 → at; O=+1 → below.
            let mut price = if off < 0 {
                Price(bp.0 + offset_dec)
            } else {
                Price(bp.0 - offset_dec)
            };
            // Cross-guard: never emit BID >= best_ask.
            if let Some(ap) = best_ask
                && ap.0 > Decimal::ZERO
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
            && tick > Decimal::ZERO
        {
            // ASK with offset O: price = best_ask + O*tick.
            // O=-1 → below best_ask (improve); O=0 → at; O=+1 → above.
            let mut price = if off < 0 {
                Price(ap.0 - offset_dec)
            } else {
                Price(ap.0 + offset_dec)
            };
            // Cross-guard: never emit ASK <= best_bid.
            if let Some(bp) = best_bid
                && bp.0 > Decimal::ZERO
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

    fn cfg() -> JokerConfig {
        JokerConfig {
            notional_per_order: Decimal::from(100),
            tick_size: Decimal::new(1, 1), // 0.1
            step_size: Decimal::new(1, 3), // 0.001
            min_notional: Decimal::from(5),
            max_order_age_secs: 0,
            order_tick_offset: 0,
            order_tick_tolerance: 0,
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

    /// Regression: dedupe must cover in-flight placements the runner
    /// hasn't reconciled into `open_quotes` yet — checking `open_quotes`
    /// alone let the same touch get re-emitted every event.
    #[test]
    fn in_flight_placement_suppresses_duplicate_emit_before_open_quotes_catch_up() {
        let mut s = Joker::new(cfg());
        let snap = book(Decimal::from(100), Decimal::new(1001, 1));
        let p = pos();
        let symbol = sym();

        let c0 = StrategyContext {
            symbol: &symbol,
            now: Timestamp(1_000_000_000),
            position: &p,
            recent_fills: &[],
            latest_book: &snap,
            open_quotes: &[],
            recent_liqs: &[],
        };
        let first = s.on_event(
            &c0,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        assert_eq!(first.len(), 2, "first event emits both touches");

        // Same book, `open_quotes` still empty (runner hasn't reconciled
        // the in-flight placements yet), 1s later — well inside the
        // grace window. Before the fix this re-emitted duplicates.
        let c1 = StrategyContext {
            symbol: &symbol,
            now: Timestamp(2_000_000_000),
            position: &p,
            recent_fills: &[],
            latest_book: &snap,
            open_quotes: &[],
            recent_liqs: &[],
        };
        let second = s.on_event(
            &c1,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        assert!(
            second.is_empty(),
            "in-flight grace must suppress duplicate emit, got {second:?}"
        );

        // Well past the grace window, still no open_quotes (order truly
        // lost — rejected, dropped) → safety-valve retry re-emits.
        let c2 = StrategyContext {
            symbol: &symbol,
            now: Timestamp(1_000_000_000 + 11_000_000_000),
            position: &p,
            recent_fills: &[],
            latest_book: &snap,
            open_quotes: &[],
            recent_liqs: &[],
        };
        let third = s.on_event(
            &c2,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        assert_eq!(
            third.len(),
            2,
            "past the grace window, emit must retry, got {third:?}"
        );
    }

    /// Regression: the age sweep's `placement_ts.retain` used to drop
    /// entries for just-emitted, not-yet-reconciled orders unconditionally
    /// — permanently exempting them from the age sweep once they DID land
    /// in `open_quotes`, since the sweep only acts on quotes with a
    /// `placement_ts` record.
    #[test]
    fn placement_ts_survives_in_flight_gap_so_age_sweep_still_reaps_it() {
        let mut c = cfg();
        c.max_order_age_secs = 5;
        let mut s = Joker::new(c);
        let snap = book(Decimal::from(100), Decimal::new(1001, 1));
        let p = pos();
        let symbol = sym();

        let c0 = StrategyContext {
            symbol: &symbol,
            now: Timestamp(0),
            position: &p,
            recent_fills: &[],
            latest_book: &snap,
            open_quotes: &[],
            recent_liqs: &[],
        };
        let first = s.on_event(
            &c0,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        assert_eq!(first.len(), 2, "first event emits both touches");

        // 1s later, still not reconciled into open_quotes (runner lag).
        // Before the fix, this call's retain() unconditionally wiped the
        // placement_ts entries since open_keys was still empty.
        let c1 = StrategyContext {
            symbol: &symbol,
            now: Timestamp(1_000_000_000),
            position: &p,
            recent_fills: &[],
            latest_book: &snap,
            open_quotes: &[],
            recent_liqs: &[],
        };
        let _ = s.on_event(
            &c1,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );

        // 6s after the ORIGINAL emit (past max_order_age_secs=5), the
        // orders finally show up in open_quotes at their emitted prices.
        // The age sweep must reap them using the ORIGINAL emit
        // timestamp — which only survives if placement_ts wasn't wiped
        // at t=1s.
        let open = vec![
            (
                QuoteId::new(),
                QuoteIntent {
                    symbol: symbol.clone(),
                    side: Side::Bid,
                    price: Price(Decimal::from(100)),
                    size: Size(Decimal::ONE),
                    tif: TimeInForce::PostOnly,
                    kind: QuoteKind::Point,
                },
            ),
            (
                QuoteId::new(),
                QuoteIntent {
                    symbol: symbol.clone(),
                    side: Side::Ask,
                    price: Price(Decimal::new(1001, 1)),
                    size: Size(Decimal::ONE),
                    tif: TimeInForce::PostOnly,
                    kind: QuoteKind::Point,
                },
            ),
        ];
        let c2 = StrategyContext {
            symbol: &symbol,
            now: Timestamp(6_000_000_000),
            position: &p,
            recent_fills: &[],
            latest_book: &snap,
            open_quotes: &open,
            recent_liqs: &[],
        };
        let third = s.on_event(
            &c2,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        let cancels = third
            .iter()
            .filter(|a| matches!(a, Action::Cancel(_)))
            .count();
        assert_eq!(
            cancels, 2,
            "age sweep must reap both orders using the original emit ts, got {third:?}"
        );
    }
}
