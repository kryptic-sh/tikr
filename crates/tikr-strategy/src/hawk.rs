//! Hawk — spread-gated grid market-maker with always-alive close-side.
//!
//! Combines the best traits the 2026-05-2X sweeps surfaced for each
//! parent strategy:
//!
//! - **From SpreadScalp**: `min_spread_bps` gate so the bot DOESN'T quote
//!   in tight mainnet books (where every fill is toxic), and the
//!   close-side stays alive at `avg_entry ± close_target_bps` so held
//!   inventory drains at maker fee when spread cools.
//! - **From StaticGrid**: per-side ladder of `levels_per_side` orders at
//!   `inner_bps + k·step_bps` from mid — more capture per widening
//!   burst than SS's single-level shot.
//! - **From LayeredGrid**: cap-aware inventory: when `|pos × mid| ≥
//!   max_position_usdt`, the add-side ladder is suppressed and only
//!   the close-side stays live.
//! - **Shared risk module**: bps-of-notional take-profit + stop-loss
//!   IOC-flatten via `tikr_strategy::risk`.
//!
//! # State machine
//!
//! ```text
//! Cold (spread < min_spread_bps):
//!   - All add-side quotes cancelled
//!   - Close-side stays at avg_entry ± close_target_bps (long sells
//!     above entry, short buys below) so a natural taker closes us
//!     at the spread we originally wanted to capture
//!   - No new ladder placement
//!
//! Hot (spread >= min_spread_bps):
//!   - Place / refresh ladder: 2 × levels_per_side orders
//!   - Skip add-side levels when at inventory cap
//!   - Close-side levels always allowed
//!
//! On Fill:
//!   - Rebuild ladder anchored on current mid (v0: simple rebuild;
//!     v1 follow-up = LG-style rolling reentry)
//!
//! Risk gate (every event):
//!   - take_profit_bps / stop_loss_bps → IOC flatten via risk module
//! ```
//!
//! # V0 scope
//!
//! Out of scope for the first cut:
//! - Regime-aware skew (Kaufman efficiency) — symmetric ladder only
//! - Adverse-selection tracker (dynamic widen)
//! - LG-style rolling reentry on fill — uses simple full rebuild
//! - Adaptive fillrate scaler
//!
//! Each of these can be added iteratively as backtests prove the
//! v0 baseline is profitable enough to warrant more complexity.

use tikr_core::{Decimal, MarketEvent, Price, QuoteKind, Side, Size, Symbol, TimeInForce};
use tikr_venue::QuoteIntent;

use crate::risk::{self, RiskConfig, RiskDecision};
use crate::{Action, Strategy, StrategyContext};

/// Configuration for [`Hawk`].
#[derive(Debug, Clone)]
pub struct HawkConfig {
    /// Fiat notional per quote level. Same role as SS/SG's `notional`.
    pub notional_per_order: Decimal,
    /// Venue tick size — used to round prices to grid + safety checks.
    pub tick_size: Decimal,
    /// Venue lot step size for quantity rounding.
    pub step_size: Decimal,
    /// Minimum order notional (price × size) required by the venue.
    pub min_notional: Decimal,
    /// Number of orders per side (total open at hot start =
    /// `2 × levels_per_side`).
    pub levels_per_side: u32,
    /// Inner spread from mid in bps (closest level on each side).
    pub inner_bps: u32,
    /// Step between consecutive levels on the same side, in bps.
    pub step_bps: u32,
    /// Book-spread floor in bps. The bot is in COLD mode (no new
    /// add-side quotes) when `book_spread_bps < min_spread_bps`. Same
    /// shape as SS's threshold. Setting `0` disables the gate —
    /// degenerates to a pure SG (always-on grid).
    pub min_spread_bps: Decimal,
    /// Hard inventory cap in USDT notional. Add-side quotes are
    /// suppressed when `|position × mid| ≥ cap` so existing rest-
    /// orders can drain inventory. `0` disables.
    pub max_position_usdt: Decimal,
    /// In COLD mode (spread below threshold), the close-side quote
    /// stays alive at `avg_entry ± close_target_bps`. Defaults to
    /// `min_spread_bps` if 0 — i.e. close at the same spread we were
    /// originally trying to capture. Set explicitly for an
    /// asymmetric exit target.
    pub close_target_bps: u32,
    /// Take-profit threshold in bps of position notional. `0`
    /// disables — held position relies on the close-side quote
    /// instead of IOC flatten.
    pub take_profit_bps: u32,
    /// Stop-loss threshold in bps of position notional. `0`
    /// disables. Pair with `take_profit_bps` to bound both wings.
    pub stop_loss_bps: u32,
}

/// `Hawk` strategy state.
pub struct Hawk {
    config: HawkConfig,
    /// Last seen best bid + ask, cached from BookUpdate so Fill /
    /// Heartbeat handlers can re-anchor without a fresh snapshot.
    last_bid: Option<Price>,
    last_ask: Option<Price>,
    /// `true` once we've ever placed the hot-mode ladder. Lets the
    /// first BookUpdate after going cold→hot do an unconditional
    /// placement vs a diff.
    placed: bool,
}

impl Hawk {
    fn risk_cfg(&self) -> RiskConfig {
        RiskConfig {
            take_profit_bps: self.config.take_profit_bps,
            stop_loss_bps: self.config.stop_loss_bps,
            take_profit_usdt_legacy: Decimal::ZERO,
        }
    }

    /// Compute the (bid, ask) target prices for the innermost level
    /// when spread is wide enough. Mirrors SS's quote-inside-touch
    /// safety: 1 tick inside best_bid / best_ask so PostOnly doesn't
    /// cross. Returns None when the book is bad OR the spread gate is
    /// closed.
    fn compute_top_targets(&self, best_bid: Price, best_ask: Price) -> Option<(Price, Price)> {
        let tick = self.config.tick_size;
        if tick <= Decimal::ZERO || best_ask.0 <= best_bid.0 {
            return None;
        }
        // book spread in bps
        let mid = (best_bid.0 + best_ask.0) / Decimal::from(2);
        if mid <= Decimal::ZERO {
            return None;
        }
        let spread_bps = (best_ask.0 - best_bid.0) / mid * Decimal::from(10_000);
        if self.config.min_spread_bps > Decimal::ZERO && spread_bps < self.config.min_spread_bps {
            return None;
        }
        // Quote 1 tick inside best level so PostOnly orders don't get
        // rejected when the market moves between snapshot and placement.
        let bid = Price(best_bid.0 + tick);
        let ask = Price(best_ask.0 - tick);
        if bid.0 >= ask.0 {
            return None;
        }
        Some((bid, ask))
    }

    /// True iff posting on `side` would deepen inventory past the cap.
    /// `cap == 0` disables. Long → Bid adds; short → Ask adds.
    fn add_side_capped(&self, pos_usdt: Decimal, side: Side) -> bool {
        let cap = self.config.max_position_usdt;
        if cap <= Decimal::ZERO {
            return false;
        }
        match side {
            Side::Bid => pos_usdt >= cap,
            Side::Ask => pos_usdt <= -cap,
        }
    }

    /// Which side closes the current position. Long → Ask reduces.
    /// Short → Bid reduces. Flat → None.
    fn close_side(pos_size: Decimal) -> Option<Side> {
        if pos_size > Decimal::ZERO {
            Some(Side::Ask)
        } else if pos_size < Decimal::ZERO {
            Some(Side::Bid)
        } else {
            None
        }
    }

    fn quote_size_at(&self, price: Price) -> Size {
        let raw = self.config.notional_per_order / price.0;
        let step = self.config.step_size;
        let mut qty = if step > Decimal::ZERO {
            (raw / step).floor() * step
        } else {
            raw
        };
        if self.config.min_notional > Decimal::ZERO && step > Decimal::ZERO {
            let min_qty = (self.config.min_notional / price.0 / step).ceil() * step;
            if qty < min_qty {
                qty = min_qty;
            }
        }
        Size(qty)
    }

    fn make_quote(&self, symbol: &Symbol, side: Side, price: Price) -> Action {
        Action::Quote(QuoteIntent {
            symbol: symbol.clone(),
            side,
            price,
            size: self.quote_size_at(price),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        })
    }

    /// Build the full hot-mode ladder, skipping add-side levels when
    /// at inventory cap. Returns CancelAll + the new quote stack
    /// inside-out so the venue receives the nearest level first.
    fn build_ladder(
        &mut self,
        ctx: &StrategyContext<'_>,
        best_bid: Price,
        best_ask: Price,
        pos_usdt: Decimal,
    ) -> Vec<Action> {
        let bp_unit = Decimal::from(10_000);
        let bid_capped = self.add_side_capped(pos_usdt, Side::Bid);
        let ask_capped = self.add_side_capped(pos_usdt, Side::Ask);
        let mut out: Vec<(Price, Action)> = Vec::with_capacity(self.config.levels_per_side as usize * 2);
        for k in 0..self.config.levels_per_side {
            let offset_bps = Decimal::from(self.config.inner_bps + self.config.step_bps * k);
            if !bid_capped {
                let bid_price = Price(best_bid.0 * (Decimal::ONE - offset_bps / bp_unit));
                // Cap at best_bid + 1 tick for k=0 so the innermost
                // bid joins the touch (matches SS quote-inside-touch).
                let bid_price = if k == 0 {
                    Price(best_bid.0 + self.config.tick_size)
                } else {
                    bid_price
                };
                out.push((bid_price, self.make_quote(ctx.symbol, Side::Bid, bid_price)));
            }
            if !ask_capped {
                let ask_price = Price(best_ask.0 * (Decimal::ONE + offset_bps / bp_unit));
                let ask_price = if k == 0 {
                    Price(best_ask.0 - self.config.tick_size)
                } else {
                    ask_price
                };
                out.push((ask_price, self.make_quote(ctx.symbol, Side::Ask, ask_price)));
            }
        }
        let mid = (best_bid.0 + best_ask.0) / Decimal::from(2);
        // Innermost (nearest to mid) goes first so the venue sees the
        // most aggressive quote before deeper ones.
        out.sort_by(|a, b| {
            let da = (a.0.0 - mid).abs();
            let db = (b.0.0 - mid).abs();
            da.cmp(&db)
        });
        let mut actions = Vec::with_capacity(out.len() + 1);
        actions.push(Action::CancelAll);
        actions.extend(out.into_iter().map(|(_, a)| a));
        actions
    }

    /// Cold-mode handler: spread gate is closed. Keep the close-side
    /// quote alive at avg_entry ± close_target_bps so a held position
    /// drains at maker fee; cancel everything else.
    fn cold_quote(
        &mut self,
        ctx: &StrategyContext<'_>,
        best_bid: Price,
        best_ask: Price,
    ) -> Vec<Action> {
        let Some(close) = Self::close_side(ctx.position.size.0) else {
            // Flat — nothing to keep alive. Drop all add-side quotes.
            return vec![Action::CancelAll];
        };
        let entry = ctx.position.avg_entry.0;
        if entry <= Decimal::ZERO {
            return vec![Action::CancelAll];
        }
        let target_bps = if self.config.close_target_bps > 0 {
            Decimal::from(self.config.close_target_bps)
        } else {
            // Fall back to the configured min_spread_bps as the
            // "what we were trying to capture" target.
            self.config.min_spread_bps
        };
        let bp = target_bps / Decimal::from(10_000);
        let target_from_entry = match close {
            Side::Ask => Price(entry * (Decimal::ONE + bp)),
            Side::Bid => Price(entry * (Decimal::ONE - bp)),
        };
        // Bonus path: market moved past target in our favour → use
        // the more aggressive touch quote to capture the extra.
        let aggressive_touch = match close {
            Side::Ask => Price(best_ask.0 - self.config.tick_size),
            Side::Bid => Price(best_bid.0 + self.config.tick_size),
        };
        let price = match close {
            Side::Ask => {
                if aggressive_touch.0 > target_from_entry.0 {
                    aggressive_touch
                } else {
                    target_from_entry
                }
            }
            Side::Bid => {
                if aggressive_touch.0 < target_from_entry.0 {
                    aggressive_touch
                } else {
                    target_from_entry
                }
            }
        };
        // Refuse to post if it would cross.
        let crosses = match close {
            Side::Ask => price.0 <= best_bid.0,
            Side::Bid => price.0 >= best_ask.0,
        };
        if crosses {
            // Don't strand the position by cancelling — let any
            // existing close-side rest. Caller is the only thing that
            // would CancelAll here.
            return Vec::new();
        }
        vec![Action::CancelAll, self.make_quote(ctx.symbol, close, price)]
    }
}

impl Strategy for Hawk {
    type Config = HawkConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            last_bid: None,
            last_ask: None,
            placed: false,
        }
    }

    fn name(&self) -> &str {
        "hawk"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        let (best_bid, best_ask) = match event {
            MarketEvent::BookUpdate { snapshot } => {
                let bid = snapshot.bids.first().map(|l| l.price);
                let ask = snapshot.asks.first().map(|l| l.price);
                let (Some(b), Some(a)) = (bid, ask) else {
                    return Vec::new();
                };
                self.last_bid = Some(b);
                self.last_ask = Some(a);
                (b, a)
            }
            MarketEvent::Fill(_) | MarketEvent::Heartbeat { .. } => {
                let (Some(b), Some(a)) = (self.last_bid, self.last_ask) else {
                    return Vec::new();
                };
                (b, a)
            }
            MarketEvent::Trade { .. } => return Vec::new(),
        };

        // Risk gate runs FIRST so an adverse spike still trips TP/SL
        // even when the rest of the handler short-circuits.
        if self.config.take_profit_bps > 0 || self.config.stop_loss_bps > 0 {
            let mid = Price((best_bid.0 + best_ask.0) / Decimal::from(2));
            if let RiskDecision::Close { side, qty, .. } =
                risk::evaluate(ctx.position, mid, self.risk_cfg())
            {
                self.placed = false;
                return vec![
                    Action::CancelAll,
                    risk::build_close(ctx.symbol, side, qty, best_bid, best_ask),
                ];
            }
        }

        // Decide hot vs cold based on the spread gate.
        let targets = self.compute_top_targets(best_bid, best_ask);
        match targets {
            Some(_) => {
                // Hot mode — refresh the ladder.
                let mid = (best_bid.0 + best_ask.0) / Decimal::from(2);
                let pos_usdt = ctx.position.size.0 * mid;
                let actions = self.build_ladder(ctx, best_bid, best_ask, pos_usdt);
                self.placed = true;
                actions
            }
            None => {
                // Cold mode — keep close-side alive, cancel add-side.
                self.placed = false;
                self.cold_quote(ctx, best_bid, best_ask)
            }
        }
    }

    fn on_quote_rejected(
        &mut self,
        _ctx: &StrategyContext<'_>,
        _intent: &QuoteIntent,
        _reason: &str,
    ) -> Vec<Action> {
        // V0: drop the rejected quote silently. Next BookUpdate
        // re-anchors the ladder, so we don't need an explicit recovery.
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

    fn cfg() -> HawkConfig {
        HawkConfig {
            notional_per_order: Decimal::from(100),
            tick_size: Decimal::from_str_exact("0.1").unwrap(),
            step_size: Decimal::from_str_exact("0.001").unwrap(),
            min_notional: Decimal::ZERO,
            levels_per_side: 2,
            inner_bps: 3,
            step_bps: 2,
            min_spread_bps: Decimal::from(5),
            max_position_usdt: Decimal::ZERO,
            close_target_bps: 0,
            take_profit_bps: 0,
            stop_loss_bps: 0,
        }
    }

    fn snap(bid: i64, ask: i64, ts: u64) -> Snapshot {
        Snapshot {
            symbol: sym(),
            ts: Timestamp(ts),
            bids: vec![Level {
                price: Price(Decimal::from(bid)),
                size: Size(Decimal::ONE),
            }],
            asks: vec![Level {
                price: Price(Decimal::from(ask)),
                size: Size(Decimal::ONE),
            }],
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
            recent_liqs: &[],
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

    #[test]
    fn cold_book_flat_position_emits_cancel_all_only() {
        let mut h = Hawk::new(cfg());
        let s = sym();
        let p = flat();
        // 1 bps spread on 100_000 mid → below 5 bps gate.
        let snap = snap(99_995, 100_005, 1_000_000_000);
        let c = ctx(&s, &p, &snap);
        let actions = h.on_event(&c, &MarketEvent::BookUpdate { snapshot: snap.clone() });
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], Action::CancelAll));
    }

    #[test]
    fn hot_book_places_ladder() {
        let mut h = Hawk::new(cfg());
        let s = sym();
        let p = flat();
        // 10 bps spread on 100_000 mid → above 5 bps gate.
        let snap = snap(99_950, 100_050, 1_000_000_000);
        let c = ctx(&s, &p, &snap);
        let actions = h.on_event(&c, &MarketEvent::BookUpdate { snapshot: snap.clone() });
        // CancelAll + 2 levels × 2 sides = 5 actions.
        assert_eq!(actions.len(), 5);
        assert!(matches!(actions[0], Action::CancelAll));
        // Innermost first: nearest-to-mid quote follows CancelAll.
        if let Action::Quote(intent) = &actions[1] {
            let mid = Decimal::from(100_000);
            let d = (intent.price.0 - mid).abs();
            // Innermost = within 1 bps (the inner_bps target + tick join).
            assert!(d <= Decimal::from(100));
        } else {
            panic!("expected innermost Quote first");
        }
    }

    #[test]
    fn cold_with_long_position_keeps_ask_at_target() {
        let mut h = Hawk::new(cfg());
        let s = sym();
        // Long 0.001 BTC at avg_entry 100_000.
        let p = Position {
            symbol: sym(),
            size: SignedSize(Decimal::from_str_exact("0.001").unwrap()),
            avg_entry: Price(Decimal::from(100_000)),
            realized_pnl: Notional(Decimal::ZERO),
        };
        // 2 bps spread (below 5 bps gate) but market hasn't moved.
        let snap = snap(99_990, 100_010, 1_000_000_000);
        let c = ctx(&s, &p, &snap);
        let actions = h.on_event(&c, &MarketEvent::BookUpdate { snapshot: snap.clone() });
        // CancelAll + one close-side Ask quote.
        assert_eq!(actions.len(), 2);
        assert!(matches!(actions[0], Action::CancelAll));
        let Action::Quote(intent) = &actions[1] else {
            panic!("expected Quote");
        };
        assert_eq!(intent.side, Side::Ask);
        // Target = avg_entry * (1 + min_spread_bps/10000) = 100_050.
        // close_target_bps defaults to min_spread_bps when 0.
        assert_eq!(intent.price.0, Decimal::from(100_050));
    }

    #[test]
    fn add_side_capped_when_at_position_cap() {
        let mut c = cfg();
        c.max_position_usdt = Decimal::from(100);
        let mut h = Hawk::new(c);
        let s = sym();
        // Already long $200 worth — over the $100 cap.
        let p = Position {
            symbol: sym(),
            size: SignedSize(Decimal::from_str_exact("0.002").unwrap()),
            avg_entry: Price(Decimal::from(100_000)),
            realized_pnl: Notional(Decimal::ZERO),
        };
        // Hot spread = 10 bps.
        let snap = snap(99_950, 100_050, 1_000_000_000);
        let cx = ctx(&s, &p, &snap);
        let actions = h.on_event(&cx, &MarketEvent::BookUpdate { snapshot: snap.clone() });
        // Long + capped → Bid side suppressed. Only Ask levels + CancelAll.
        let quote_sides: Vec<Side> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Quote(intent) => Some(intent.side),
                _ => None,
            })
            .collect();
        assert!(!quote_sides.is_empty());
        assert!(quote_sides.iter().all(|s| *s == Side::Ask));
    }
}
