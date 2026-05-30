//! Mantis — a symmetric touch scalper that trades one cycle at a time.
//!
//! While flat, it rests one post-only bid + one ask at (or near) the book
//! touch whenever the spread is wide enough, and waits — like a mantis — for
//! price to come to it. It does not chase: once a leg fills it freezes.
//!
//! Lifecycle:
//! 1. **Flat & ready** — rest a bid + ask pair at the touch (re-priced as the
//!    touch moves, since nothing is committed yet).
//! 2. **A leg fills** → now holding inventory. **Keep only the opposite (close)
//!    leg, at its original price** — that's the fixed take-profit; filling it
//!    books the captured spread. The filled side is NOT replaced and no new
//!    pair is opened.
//! 3. **Close leg fills** → flat again, spread booked → **cooldown**: don't
//!    open a new pair until price has moved `reopen_distance_ticks` away from
//!    the last fill, so we don't immediately re-quote the same level.
//!
//! Placement is controlled by `tick_offset` (in ticks from the touch):
//! `0` (default) = join, `-1` = inside/outbid, `+1` = one tick outside.
//! There is no stop-loss: if price runs against a held position, the fixed
//! close leg is the only exit (it sits until price reverts).

use tikr_core::{Decimal, MarketEvent, Price, Side, Size};

use crate::{Action, Strategy, StrategyContext, make_post_only_intent};

/// Configuration for [`Mantis`].
#[derive(Debug, Clone)]
pub struct MantisConfig {
    /// Fiat notional per order.
    pub notional_per_order: Decimal,
    /// Venue tick size (price increment). Auto-detected from the symbol's
    /// exchange filters by the caller — operators don't hand-set it.
    pub tick_size: Decimal,
    /// Venue lot step size (quantity rounding).
    pub step_size: Decimal,
    /// Minimum order notional (price × size) required by the venue.
    pub min_notional: Decimal,
    /// Minimum book spread in bps required to open a new pair. Below this,
    /// no new pair is opened (a held close leg is always kept regardless).
    pub min_spread_bps: Decimal,
    /// Tick offset from the touch. `0` = join, `-1` = inside/outbid,
    /// `+1` = one tick outside. See module docs.
    pub tick_offset: i32,
    /// After a fill, how far (in ticks) price must move away from the fill
    /// before a new pair is opened. Prevents immediately re-quoting the same
    /// level. `0` = reopen as soon as flat.
    pub reopen_distance_ticks: u32,
    /// Max signed position notional (quote currency). `0` = uncapped. Largely
    /// a backstop here: the one-cycle design already bounds inventory to a
    /// single fill (no adds while holding).
    pub max_position_usdt: Decimal,
}

/// Symmetric one-cycle touch-scalping strategy. See module docs.
pub struct Mantis {
    config: MantisConfig,
    /// `(bid, ask)` prices of the most recently opened pair. The held close
    /// leg's fixed take-profit price is read from here.
    active_pair: Option<(Price, Price)>,
    /// Price of the most recent fill. While set and price hasn't moved
    /// `reopen_distance_ticks` away, no new pair is opened.
    cooldown_anchor: Option<Price>,
}

impl Mantis {
    /// Round a notional into a venue-valid order size.
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

    /// Reconcile open quotes to exactly `desired` (side, price) orders: keep
    /// matching resting quotes, cancel everything else, place any missing.
    /// `desired = []` cancels all resting quotes.
    fn reconcile(
        &self,
        ctx: &StrategyContext<'_>,
        desired: &[(Side, Price)],
        actions: &mut Vec<Action>,
    ) {
        let mut matched = vec![false; desired.len()];
        for (id, intent) in ctx.open_quotes {
            let mut keep = false;
            for (i, (side, price)) in desired.iter().enumerate() {
                if !matched[i] && intent.side == *side && intent.price == *price {
                    matched[i] = true;
                    keep = true;
                    break;
                }
            }
            if !keep {
                actions.push(Action::Cancel(*id));
            }
        }
        for (i, (side, price)) in desired.iter().enumerate() {
            if matched[i] {
                continue;
            }
            let size = self.quote_size(*price);
            if size.0 > Decimal::ZERO {
                actions.push(Action::Quote(make_post_only_intent(
                    ctx.symbol, *side, *price, size,
                )));
            }
        }
    }
}

impl Strategy for Mantis {
    type Config = MantisConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            active_pair: None,
            cooldown_anchor: None,
        }
    }

    fn name(&self) -> &str {
        "mantis"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, _event: &MarketEvent) -> Vec<Action> {
        let (Some(best_bid), Some(best_ask)) = (
            ctx.latest_book.bids.first().map(|l| l.price),
            ctx.latest_book.asks.first().map(|l| l.price),
        ) else {
            return Vec::new();
        };
        if best_bid.0 <= Decimal::ZERO || best_ask.0 <= best_bid.0 {
            return Vec::new();
        }

        // Any fill this event arms the cooldown anchor at the fill price.
        if let Some(f) = ctx.recent_fills.last() {
            self.cooldown_anchor = Some(f.price);
        }

        let tick = self.config.tick_size;
        let off = Decimal::from(self.config.tick_offset) * tick;
        let bid_target = Price(best_bid.0 - off);
        let ask_target = Price(best_ask.0 + off);
        let mid = (best_bid.0 + best_ask.0) / Decimal::from(2);
        let pos = ctx.position.size.0;
        let mut actions = Vec::new();

        // HOLDING: keep only the close (inventory-reducing) leg at its fixed
        // take-profit price; cancel anything else; open nothing new.
        if pos != Decimal::ZERO {
            let long = pos > Decimal::ZERO;
            let close_side = if long { Side::Ask } else { Side::Bid };
            let close_price = match self.active_pair {
                Some((b, a)) => {
                    if long {
                        a
                    } else {
                        b
                    }
                }
                None => {
                    if long {
                        ask_target
                    } else {
                        bid_target
                    }
                }
            };
            self.reconcile(ctx, &[(close_side, close_price)], &mut actions);
            return actions;
        }

        // FLAT. Honor the post-fill cooldown: wait until price has moved
        // `reopen_distance_ticks` away from the last fill before reopening.
        if let Some(anchor) = self.cooldown_anchor {
            let dist = Decimal::from(self.config.reopen_distance_ticks) * tick;
            if (mid - anchor.0).abs() < dist {
                self.reconcile(ctx, &[], &mut actions); // cancel stragglers, wait
                return actions;
            }
            self.cooldown_anchor = None; // moved away → clear to reopen
        }

        // Flat, clear of cooldown → open a fresh pair if the gate allows.
        let spread_bps = (best_ask.0 - best_bid.0) / mid * Decimal::from(10_000);
        let gate_ok = spread_bps >= self.config.min_spread_bps;
        // Post-only cross guards (offset must not lock/cross the book).
        let placement_ok = bid_target.0 > Decimal::ZERO
            && bid_target.0 < best_ask.0
            && ask_target.0 > best_bid.0
            && bid_target.0 < ask_target.0;
        if !gate_ok || !placement_ok {
            self.reconcile(ctx, &[], &mut actions);
            return actions;
        }

        self.active_pair = Some((bid_target, ask_target));
        self.reconcile(
            ctx,
            &[(Side::Bid, bid_target), (Side::Ask, ask_target)],
            &mut actions,
        );
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
    use std::str::FromStr;
    use tikr_core::{
        Asset, Fill, Level, MarketKind, Notional, Position, QuoteKind, SignedSize, Snapshot,
        Symbol, TimeInForce, Timestamp, VenueId,
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

    fn cfg(tick_offset: i32, reopen: u32) -> MantisConfig {
        MantisConfig {
            notional_per_order: Decimal::from(10),
            tick_size: Decimal::new(1, 2), // 0.01
            step_size: Decimal::new(1, 2),
            min_notional: Decimal::from(5),
            min_spread_bps: Decimal::ONE,
            tick_offset,
            reopen_distance_ticks: reopen,
            max_position_usdt: Decimal::ZERO,
        }
    }

    fn book(bid: &str, ask: &str) -> Snapshot {
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

    fn pos(size: &str, entry: &str) -> Position {
        Position {
            symbol: sym(),
            size: SignedSize(Decimal::from_str(size).unwrap()),
            avg_entry: Price(Decimal::from_str(entry).unwrap()),
            realized_pnl: Notional(Decimal::ZERO),
        }
    }

    fn intent(side: Side, price: &str) -> QuoteIntent {
        QuoteIntent {
            symbol: sym(),
            side,
            price: Price(Decimal::from_str(price).unwrap()),
            size: Size(Decimal::from_str("0.1").unwrap()),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        }
    }

    fn fill(side: Side, price: &str) -> Fill {
        Fill {
            quote_id: QuoteId::new(),
            price: Price(Decimal::from_str(price).unwrap()),
            size: Size(Decimal::from_str("0.1").unwrap()),
            fee_asset: Asset::new("USDC"),
            fee_amount: Decimal::ZERO,
            fee_quote: Notional(Decimal::ZERO),
            side,
            ts: Timestamp(0),
            is_full: true,
            trade_id: None,
        }
    }

    struct Ctx {
        s: Symbol,
        p: Position,
        bk: Snapshot,
        open: Vec<(QuoteId, QuoteIntent)>,
        fills: Vec<Fill>,
    }
    impl Ctx {
        fn ctx(&self) -> StrategyContext<'_> {
            StrategyContext {
                symbol: &self.s,
                now: Timestamp(0),
                position: &self.p,
                recent_fills: &self.fills,
                latest_book: &self.bk,
                open_quotes: &self.open,
                recent_liqs: &[],
            }
        }
    }

    fn hb() -> MarketEvent {
        MarketEvent::Heartbeat { ts: Timestamp(0) }
    }

    fn quoted(acts: &[Action], side: Side) -> Option<Price> {
        acts.iter().find_map(|a| match a {
            Action::Quote(q) if q.side == side => Some(q.price),
            _ => None,
        })
    }

    #[test]
    fn flat_opens_pair_at_touch() {
        let mut m = Mantis::new(cfg(0, 1));
        let c = Ctx {
            s: sym(),
            p: pos("0", "0"),
            bk: book("100.00", "100.10"),
            open: vec![],
            fills: vec![],
        };
        let acts = m.on_event(&c.ctx(), &hb());
        assert_eq!(
            quoted(&acts, Side::Bid).unwrap().0,
            Decimal::from_str("100.00").unwrap()
        );
        assert_eq!(
            quoted(&acts, Side::Ask).unwrap().0,
            Decimal::from_str("100.10").unwrap()
        );
    }

    #[test]
    fn fill_holds_only_close_leg_no_refill() {
        let mut m = Mantis::new(cfg(0, 1));
        // First, flat → open the pair so active_pair is recorded.
        let c0 = Ctx {
            s: sym(),
            p: pos("0", "0"),
            bk: book("100.00", "100.10"),
            open: vec![],
            fills: vec![],
        };
        let _ = m.on_event(&c0.ctx(), &hb());
        // Bid fills → long. Only the ask (close) leg remains resting.
        let c1 = Ctx {
            s: sym(),
            p: pos("0.1", "100.00"),
            bk: book("100.00", "100.10"),
            open: vec![(QuoteId::new(), intent(Side::Ask, "100.10"))],
            fills: vec![fill(Side::Bid, "100.00")],
        };
        let acts = m.on_event(&c1.ctx(), &MarketEvent::Fill(fill(Side::Bid, "100.00")));
        // Close ask already resting at 100.10 → kept (no actions); crucially
        // NO new bid is placed (no refill of the filled side).
        assert!(acts.is_empty(), "close leg kept, no refill; got {acts:?}");
    }

    #[test]
    fn holding_replaces_missing_close_leg() {
        let mut m = Mantis::new(cfg(0, 1));
        let c0 = Ctx {
            s: sym(),
            p: pos("0", "0"),
            bk: book("100.00", "100.10"),
            open: vec![],
            fills: vec![],
        };
        let _ = m.on_event(&c0.ctx(), &hb()); // active_pair = (100.00, 100.10)
        // Long, but the close ask got silently cancelled → re-place it at the
        // original ask price (the fixed TP), not at the moved touch.
        let c1 = Ctx {
            s: sym(),
            p: pos("0.1", "100.00"),
            bk: book("100.05", "100.15"), // touch moved up
            open: vec![],
            fills: vec![],
        };
        let acts = m.on_event(&c1.ctx(), &hb());
        let ask = quoted(&acts, Side::Ask).expect("close ask replaced");
        assert_eq!(
            ask.0,
            Decimal::from_str("100.10").unwrap(),
            "TP stays at original price"
        );
        assert!(
            quoted(&acts, Side::Bid).is_none(),
            "no add-side bid while holding"
        );
    }

    #[test]
    fn cooldown_blocks_reopen_until_price_moves() {
        let mut m = Mantis::new(cfg(0, 5)); // 5 ticks × 0.01 = 0.05
        // Round trip done: flat, last fill at 100.10 (cooldown armed via fills).
        let c1 = Ctx {
            s: sym(),
            p: pos("0", "0"),
            bk: book("100.06", "100.12"), // mid 100.09; |100.09-100.10|=0.01 < 0.05
            open: vec![],
            fills: vec![fill(Side::Ask, "100.10")],
        };
        let acts = m.on_event(&c1.ctx(), &MarketEvent::Fill(fill(Side::Ask, "100.10")));
        assert!(acts.is_empty(), "in cooldown → no new pair; got {acts:?}");
        // Price moves away (mid 100.16, |100.16-100.10|=0.06 ≥ 0.05) → reopen.
        let c2 = Ctx {
            s: sym(),
            p: pos("0", "0"),
            bk: book("100.13", "100.19"),
            open: vec![],
            fills: vec![],
        };
        let acts2 = m.on_event(&c2.ctx(), &hb());
        assert!(
            quoted(&acts2, Side::Bid).is_some(),
            "reopened after move; got {acts2:?}"
        );
        assert!(quoted(&acts2, Side::Ask).is_some());
    }

    #[test]
    fn below_min_spread_opens_nothing() {
        let mut m = Mantis::new(cfg(0, 1));
        let mut c = Ctx {
            s: sym(),
            p: pos("0", "0"),
            bk: book("100.00", "100.001"), // ~0.1 bp spread
            open: vec![],
            fills: vec![],
        };
        // raise min_spread so the gate closes
        m.config.min_spread_bps = Decimal::from(5);
        let acts = m.on_event(&c.ctx(), &hb());
        assert!(acts.is_empty());
        // with a stale resting quote → cancel it
        c.open = vec![(QuoteId::new(), intent(Side::Bid, "100.00"))];
        let acts = m.on_event(&c.ctx(), &hb());
        assert!(matches!(acts.as_slice(), [Action::Cancel(_)]));
    }

    #[test]
    fn inside_offset_steps_one_tick_in() {
        let mut m = Mantis::new(cfg(-1, 1));
        let c = Ctx {
            s: sym(),
            p: pos("0", "0"),
            bk: book("100.00", "100.10"),
            open: vec![],
            fills: vec![],
        };
        let acts = m.on_event(&c.ctx(), &hb());
        assert_eq!(
            quoted(&acts, Side::Bid).unwrap().0,
            Decimal::from_str("100.01").unwrap()
        );
        assert_eq!(
            quoted(&acts, Side::Ask).unwrap().0,
            Decimal::from_str("100.09").unwrap()
        );
    }
}
