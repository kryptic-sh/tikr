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
//! **Re-entry on fill** (rolling ladder, keeps symmetric counts). All
//! spacing is controlled by the single `inner_bps` parameter:
//!
//! 1. Drop the filled order from the local mirror.
//! 2. Place a TP order on the OPPOSITE side at `fill_price ± inner_bps`.
//! 3. Extend the FILLED side outward — buys add a new entry at
//!    `lowest_existing_buy × (1 − inner_bps/10000)`; sells at
//!    `highest_existing_sell × (1 + inner_bps/10000)`.
//! 4. Drop the outermost opposite-side order (highest ask on buy fill,
//!    lowest bid on sell fill) so both sides stay at `levels_per_side`.
//!
//! Net effect: the whole ladder shifts ONE step in the direction the
//! market just moved (price came down → ladder shifts down; price went
//! up → ladder shifts up). Order count is invariant: always
//! `2 × levels_per_side`.
//!
//! Per-fill action set is the diff — `Cancel(outermost_opposite_id)` +
//! `Quote(tp)` + `Quote(extension)`. Surviving orders keep their venue-
//! assigned `QuoteId`s and queue priority. The strategy looks up the
//! outermost order's id via `ctx.open_quotes` (populated by the runner
//! from `FillSim::live_quotes_for`).
//!
//! The strategy is fill-driven: after the cold-start placement on the
//! first BookUpdate, it only emits new orders in response to its own
//! [`MarketEvent::Fill`] events. Requires the runner to deliver fill
//! events to `on_event` (added in #TBD).
//!
//! See `crates/tikr-backtest/src/bin/backtest_layered_grid.rs` for the
//! standalone candle-based version of the same logic.

use tikr_core::{Decimal, MarketEvent, Price, QuoteKind, Side, Size, Symbol, TimeInForce};
use tikr_venue::{QuoteId, QuoteIntent};

use crate::{Action, Strategy, StrategyContext};

/// Configuration for [`LayeredGrid`].
#[derive(Debug, Clone)]
pub struct LayeredGridConfig {
    /// Fixed fiat notional per order (e.g. `Decimal::from(100)` for $100).
    pub notional_per_order: Decimal,
    /// Number of orders per side. Total open orders = `2 × levels_per_side`.
    pub levels_per_side: u32,
    /// Single spacing parameter in bps. Controls all three layouts:
    /// - Cold-start level k (0-indexed): `mid × (1 ± (k+1) · inner/10000)`
    /// - TP after a fill at `P`: `P × (1 ± inner/10000)`
    /// - Extension on the filled side: `outermost_same_side × (1 ∓ inner/10000)`
    ///
    /// Each round-trip captures `inner_bps` minus `2 × maker_bps` in fees.
    pub inner_bps: u32,
}

/// Layered-grid strategy state.
///
/// `placed` flips to true on the first BookUpdate (cold-start placement).
/// `orders` mirrors the prices+sides we believe are resting for fill
/// matching. `QuoteId`s are looked up at use-time from
/// [`StrategyContext::open_quotes`].
pub struct LayeredGrid {
    config: LayeredGridConfig,
    placed: bool,
    orders: Vec<(Side, Price)>,
}

impl LayeredGrid {
    fn place_initial(
        &mut self,
        symbol: &Symbol,
        mid: Price,
        open_quotes: &[(QuoteId, QuoteIntent)],
    ) -> Vec<Action> {
        let mut actions: Vec<Action> =
            Vec::with_capacity(self.config.levels_per_side as usize * 2 + open_quotes.len());
        self.orders.clear();

        let bps_to_decimal = |b: u32| Decimal::from(b) / Decimal::from(10_000);
        // Place the new ladder FIRST so the venue sees fresh resting orders
        // before any cancels — avoids the naked-book gap. Level k (0-indexed)
        // sits at `(k+1) × inner_bps` from mid — uniform `inner_bps` spacing.
        for k in 0..self.config.levels_per_side {
            let bps = self.config.inner_bps * (k + 1);
            let bp_dec = bps_to_decimal(bps);

            let buy_price = Price(mid.0 * (Decimal::ONE - bp_dec));
            let sell_price = Price(mid.0 * (Decimal::ONE + bp_dec));

            actions.push(self.make_quote(symbol, Side::Bid, buy_price));
            actions.push(self.make_quote(symbol, Side::Ask, sell_price));
            self.orders.push((Side::Bid, buy_price));
            self.orders.push((Side::Ask, sell_price));
        }
        // Then cancel any prior open quotes by id. On cold start the
        // runner-supplied list is empty, so no cancels emit.
        for (id, _) in open_quotes {
            actions.push(Action::Cancel(*id));
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

    fn on_fill(
        &mut self,
        symbol: &Symbol,
        fill_side: Side,
        fill_price: Price,
        open_quotes: &[(QuoteId, QuoteIntent)],
    ) -> Vec<Action> {
        // 1. Drop the filled order from the mirror (exact match preferred,
        //    closest same-side match as fallback when fill_price is a touch
        //    price rather than the exact limit).
        if let Some(pos) = self
            .orders
            .iter()
            .position(|(s, p)| *s == fill_side && *p == fill_price)
        {
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

        // Single spacing parameter — TP distance and extension step are
        // both `inner_bps`.
        let inner_dec = Decimal::from(self.config.inner_bps) / Decimal::from(10_000);
        let opp_side = match fill_side {
            Side::Bid => Side::Ask,
            Side::Ask => Side::Bid,
        };

        // 2. Capture the existing outermost OPPOSITE before adding the new
        //    TP — otherwise the TP itself (which sits between the fill
        //    price and mid on the opp side) would be picked as "outermost"
        //    on small ladders and we'd cancel our just-placed TP.
        //    "Outermost" = furthest from mid → highest ask, lowest bid.
        let opp_outermost = self
            .orders
            .iter()
            .enumerate()
            .filter(|(_, (s, _))| *s == opp_side)
            .reduce(|acc, cur| {
                let acc_outer = match opp_side {
                    Side::Ask => acc.1.1.0 > cur.1.1.0,
                    Side::Bid => acc.1.1.0 < cur.1.1.0,
                };
                if acc_outer { acc } else { cur }
            })
            .map(|(i, (s, p))| (i, *s, *p));

        // 3. Place TP on opposite side at fill_price ± reentry_bps.
        let tp_price = match fill_side {
            Side::Bid => Price(fill_price.0 * (Decimal::ONE + inner_dec)),
            Side::Ask => Price(fill_price.0 * (Decimal::ONE - inner_dec)),
        };
        self.orders.push((opp_side, tp_price));

        // 4. Extend the FILLED side outward at the same step gap. Anchor
        //    on the outermost existing order on that side. Falls back to
        //    fill_price if no same-side orders remain (degenerate case).
        let same_side_extreme = match fill_side {
            Side::Bid => self
                .orders
                .iter()
                .filter(|(s, _)| *s == Side::Bid)
                .map(|(_, p)| p.0)
                .min(),
            Side::Ask => self
                .orders
                .iter()
                .filter(|(s, _)| *s == Side::Ask)
                .map(|(_, p)| p.0)
                .max(),
        };
        let anchor = same_side_extreme.unwrap_or(fill_price.0);
        let extension_price = match fill_side {
            Side::Bid => Price(anchor * (Decimal::ONE - inner_dec)),
            Side::Ask => Price(anchor * (Decimal::ONE + inner_dec)),
        };
        self.orders.push((fill_side, extension_price));

        let mut actions: Vec<Action> = Vec::with_capacity(3);

        // 5. Emit the TP + extension as fresh Quotes FIRST so the venue
        //    sees the new resting orders before any cancels arrive — no
        //    naked-book gap (mirrors NaiveGrid fix).
        actions.push(self.make_quote(symbol, opp_side, tp_price));
        actions.push(self.make_quote(symbol, fill_side, extension_price));

        // 6. Drop the outermost opposite by id (preserves queue priority
        //    on surviving orders). Fall back to CancelAll only when the
        //    QuoteId can't be resolved — in that fallback we replay the
        //    surviving mirror entries (which already include the
        //    just-pushed TP + extension), so DON'T re-emit those.
        if let Some((idx, drop_side, drop_price)) = opp_outermost {
            self.orders.remove(idx);
            if let Some(id) = open_quotes
                .iter()
                .find(|(_, q)| q.side == drop_side && q.price == drop_price)
                .map(|(id, _)| *id)
            {
                actions.push(Action::Cancel(id));
            } else {
                actions.clear();
                actions.push(Action::CancelAll);
                for (side, price) in self.orders.clone() {
                    actions.push(self.make_quote(symbol, side, price));
                }
            }
        }
        actions
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
                self.place_initial(ctx.symbol, mid, ctx.open_quotes)
            }
            MarketEvent::Fill(f) if f.symbol_matches(ctx.symbol) && f.is_full => {
                // Belt-and-suspenders: the runner already gates `is_full`,
                // but guard here too so a future caller (test, alt runner)
                // can't accidentally feed a partial fill — which would
                // cause `on_fill` to cancel the still-resting remainder.
                self.on_fill(ctx.symbol, f.side, f.price, ctx.open_quotes)
            }
            // After cold-start, BookUpdate / Trade / Heartbeat don't change
            // grid state. The strategy is purely fill-driven.
            _ => Vec::new(),
        }
    }

    fn on_quote_rejected(
        &mut self,
        ctx: &StrategyContext<'_>,
        _intent: &QuoteIntent,
        _reason: &str,
    ) -> Vec<Action> {
        // Recovery path: market moved so far that our re-quote couldn't be
        // posted as maker. Wipe everything with `CancelAll` (vs per-id
        // Cancels which race with the venue's still-in-flight ack) and
        // place a fresh symmetric ladder anchored on current book mid.
        // Preserves equal `inner_bps` spacing on both sides. If the new
        // pair also rejects (market still ripping), the runner re-invokes
        // this hook until either both sides land or the retry cap fires.
        // `ctx.latest_book` is refreshed by the runner via
        // `venue.snapshot` immediately before each recovery round so the
        // mid below reflects the venue's current state, not the stale
        // cached snapshot that caused the original reject.
        let bid = ctx.latest_book.bids.first().map(|l| l.price.0);
        let ask = ctx.latest_book.asks.first().map(|l| l.price.0);
        let (Some(b), Some(a)) = (bid, ask) else {
            return Vec::new();
        };
        let mid = Price((b + a) / Decimal::from(2));
        self.placed = false;
        self.orders.clear();

        let mut actions = vec![Action::CancelAll];
        let bps_to_decimal = |b: u32| Decimal::from(b) / Decimal::from(10_000);
        for k in 0..self.config.levels_per_side {
            let bps = self.config.inner_bps * (k + 1);
            let bp_dec = bps_to_decimal(bps);
            let buy_price = Price(mid.0 * (Decimal::ONE - bp_dec));
            let sell_price = Price(mid.0 * (Decimal::ONE + bp_dec));
            actions.push(self.make_quote(ctx.symbol, Side::Bid, buy_price));
            actions.push(self.make_quote(ctx.symbol, Side::Ask, sell_price));
            self.orders.push((Side::Bid, buy_price));
            self.orders.push((Side::Ask, sell_price));
        }
        self.placed = true;
        actions
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
        self.placed = false;
        self.orders.clear();
        vec![Action::CancelAll]
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
        let actions = strat.on_event(
            &c,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        // Cold start: no prior open quotes → 6 Quote actions (3 per side),
        // no Cancels (Quote-first ordering; no naked-book gap).
        assert_eq!(actions.len(), 6);
        for a in &actions {
            assert!(matches!(a, Action::Quote(_)));
        }
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
        let _ = strat.on_event(
            &c,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        // Second event should be no-op (grid is purely fill-driven after cold start).
        let actions = strat.on_event(
            &c,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        assert!(actions.is_empty());
    }

    #[test]
    fn fill_rolls_ladder_one_step() {
        let s = sym();
        let snap = book(&s, 1000, 1001);
        let p = flat_pos(&s);
        let mut strat = LayeredGrid::new(cfg());
        let cold_actions = strat.on_event(
            &ctx(&s, &p, &snap),
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        // Build a fake `open_quotes` from the cold-start placements so
        // the diff-Cancel path has QuoteIds to resolve.
        let open_quotes: Vec<(QuoteId, QuoteIntent)> = cold_actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(q) => Some((QuoteId::new(), q.clone())),
                _ => None,
            })
            .collect();
        let fill_ctx = StrategyContext {
            symbol: &s,
            now: Timestamp(1000),
            position: &p,
            recent_fills: &[],
            latest_book: &snap,
            open_quotes: &open_quotes,
        };

        let mid = Decimal::new(10005, 1);
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
            is_full: true,
        };
        let actions = strat.on_event(&fill_ctx, &MarketEvent::Fill(fill));
        // Quote-first ordering: Quote(TP) + Quote(extension) + Cancel(outermost_opp) = 3
        assert_eq!(actions.len(), 3);
        match &actions[0] {
            Action::Quote(q) => {
                assert_eq!(q.side, Side::Ask);
                // TP distance == inner_bps (collapsed-spacing model).
                assert_eq!(q.price.0, buy_price.0 * (Decimal::ONE + inner_bp));
            }
            _ => panic!("expected TP Quote on Ask"),
        }
        match &actions[1] {
            Action::Quote(q) => {
                assert_eq!(q.side, Side::Bid);
                // Extension == outermost surviving Bid × (1 − inner_bps).
                // Outermost = level 2 = mid × (1 − 18bps).
                let outer = mid * (Decimal::ONE - Decimal::from(18) / Decimal::from(10_000));
                assert_eq!(q.price.0, outer * (Decimal::ONE - inner_bp));
            }
            _ => panic!("expected extension Quote on Bid"),
        }
        assert!(matches!(actions[2], Action::Cancel(_)));
    }
}
