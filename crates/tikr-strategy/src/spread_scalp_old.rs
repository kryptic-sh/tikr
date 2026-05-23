//! Spread scalping / liquidity-provision strategy.
//!
//! When the market spread exceeds a configurable bps threshold, places passive
//! limit orders one tick inside the best bid/ask. Requotes on a fixed interval
//! unless quotes are already at the best market prices. Inventory-aware sizing
//! increases the reducing-side order size.

use tikr_core::{
    Decimal, MarketEvent, Price, QuoteKind, Side, Size, Snapshot, TimeInForce, Timestamp,
};
use tikr_venue::QuoteIntent;

use crate::{Action, Strategy, StrategyContext};

/// Configuration for [`SpreadScalp`].
#[derive(Debug, Clone)]
pub struct SpreadScalpOldConfig {
    /// Fiat notional per order.
    pub notional_per_order: Decimal,
    /// Venue tick size (price increment).
    pub tick_size: Decimal,
    /// Venue lot step size (quantity rounding).
    pub step_size: Decimal,
    /// Minimum order notional (price × size) required by the venue.
    pub min_notional: Decimal,
    /// Minimum market spread in bps required to quote.
    pub min_spread_bps: Decimal,
    /// Fixed requote interval in ms.
    pub requote_interval_ms: u64,
    /// Max position in quote currency before one-sided quoting kicks in.
    /// 0 = disabled.
    pub max_position_usdt: Decimal,
    /// Unrealized PnL threshold in quote currency to activate take-profit.
    /// When exceeded, fires an IOC on the reducing side at the opposing
    /// touch to close immediately as taker. 0 = disabled.
    pub take_profit_usdt: Decimal,
    /// Cooldown after a venue rejection (per side) before another rebuild
    /// is allowed. Prevents -5022 / -2019 hot loops on fast markets.
    /// 0 disables the gate (legacy behaviour).
    pub reject_cooldown_ms: u64,
}

/// Spread scalping strategy state.
pub struct SpreadScalpOld {
    config: SpreadScalpOldConfig,
    last_bid: Option<Price>,
    last_ask: Option<Price>,
    last_requote_ts: Option<Timestamp>,
    quotes_live: bool,
    /// Per-side timestamp (ns) of the last venue rejection. Used by
    /// `should_emit_side` to gate the next rebuild attempt by
    /// `reject_cooldown_ms`, matching the SG pattern.
    last_reject_bid_ts: Option<Timestamp>,
    last_reject_ask_ts: Option<Timestamp>,
}

impl SpreadScalpOld {
    fn compute_targets(&self, snapshot: &Snapshot) -> Option<(Price, Price)> {
        let best_bid = snapshot.bids.first()?.price;
        let best_ask = snapshot.asks.first()?.price;
        let tick = self.config.tick_size;
        if tick <= Decimal::ZERO || best_ask.0 <= best_bid.0 {
            return None;
        }
        let mid = (best_bid.0 + best_ask.0) / Decimal::from(2);
        if mid <= Decimal::ZERO {
            return None;
        }
        let spread_bps = (best_ask.0 - best_bid.0) / mid * Decimal::from(10_000);
        if spread_bps < self.config.min_spread_bps {
            return None;
        }
        // Quote 1 tick inside the best level so PostOnly orders don't get
        // rejected (-5022) when the market moves between snapshot and
        // placement.
        let bid = Price(best_bid.0 + tick);
        let ask = Price(best_ask.0 - tick);
        if bid.0 >= ask.0 {
            return None;
        }
        Some((bid, ask))
    }

    fn quote_size(&self, price: Price, size_multiplier: Decimal) -> Decimal {
        let raw_size = self.config.notional_per_order / price.0 * size_multiplier;
        let step = self.config.step_size;
        if step > Decimal::ZERO {
            (raw_size / step).floor() * step
        } else {
            raw_size
        }
    }

    fn make_quote(
        &self,
        ctx: &StrategyContext<'_>,
        side: Side,
        price: Price,
        size_multiplier: Decimal,
    ) -> Action {
        let size = self.size_at_least_min_notional(price, size_multiplier);
        Action::Quote(QuoteIntent {
            symbol: ctx.symbol.clone(),
            side,
            price,
            size: Size(size),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        })
    }

    /// Round-up size so `size × price >= min_notional`. A single step
    /// bump can still leave the order under min_notional when the gap
    /// is larger than `step_size × price`, which then re-triggers the
    /// venue rejection → recovery hot loop. Compute the exact ceil.
    fn size_at_least_min_notional(&self, price: Price, size_multiplier: Decimal) -> Decimal {
        let raw = self.quote_size(price, size_multiplier);
        let min = self.config.min_notional;
        let step = self.config.step_size;
        if min <= Decimal::ZERO || price.0 <= Decimal::ZERO {
            return raw;
        }
        let current = raw * price.0;
        if current >= min {
            return raw;
        }
        if step <= Decimal::ZERO {
            // No lot step: bump straight to min_notional / price.
            return min / price.0;
        }
        let gap = min - current;
        // ceil(gap / (price × step)) steps to clear min_notional.
        let step_value = price.0 * step;
        if step_value <= Decimal::ZERO {
            return raw;
        }
        let mut needed_steps = (gap / step_value).floor();
        if needed_steps * step_value < gap {
            needed_steps += Decimal::ONE;
        }
        raw + needed_steps * step
    }

    /// Whether the strategy is allowed to (re-)place orders on `side`
    /// right now given the per-side reject cooldown.
    fn side_in_cooldown(&self, side: Side, now: Timestamp) -> bool {
        let cooldown_ms = self.config.reject_cooldown_ms;
        if cooldown_ms == 0 {
            return false;
        }
        let last = match side {
            Side::Bid => self.last_reject_bid_ts,
            Side::Ask => self.last_reject_ask_ts,
        };
        let Some(last) = last else {
            return false;
        };
        let elapsed_ns = now.0.saturating_sub(last.0);
        let cooldown_ns = cooldown_ms.saturating_mul(1_000_000);
        elapsed_ns < cooldown_ns
    }

    fn mark_reject(&mut self, side: Side, ts: Timestamp) {
        match side {
            Side::Bid => self.last_reject_bid_ts = Some(ts),
            Side::Ask => self.last_reject_ask_ts = Some(ts),
        }
    }

    fn clear_reject(&mut self, side: Side) {
        match side {
            Side::Bid => self.last_reject_bid_ts = None,
            Side::Ask => self.last_reject_ask_ts = None,
        }
    }

    fn should_requote(&self, bid: Price, ask: Price, ts: Timestamp) -> bool {
        if let (Some(last_bid), Some(last_ask)) = (self.last_bid, self.last_ask)
            && last_bid.0 == bid.0
            && last_ask.0 == ask.0
        {
            return false;
        }
        let Some(last_ts) = self.last_requote_ts else {
            return true;
        };
        let elapsed_ns = ts.0.saturating_sub(last_ts.0);
        let interval_ns = self.config.requote_interval_ms.saturating_mul(1_000_000);
        elapsed_ns >= interval_ns
    }

    fn emit_requote(
        &mut self,
        ctx: &StrategyContext<'_>,
        bid: Price,
        ask: Price,
        ts: Timestamp,
    ) -> Vec<Action> {
        let size_mult = self.inventory_size_multiplier(ctx);
        self.last_bid = Some(bid);
        self.last_ask = Some(ask);
        self.last_requote_ts = Some(ts);
        self.quotes_live = true;
        let mut actions = vec![Action::CancelAll];
        let mid = (bid.0 + ask.0) / Decimal::from(2);
        let position_value = ctx.position.size.0.abs() * mid;
        let cap = self.config.max_position_usdt;
        let capped = cap > Decimal::ZERO && position_value >= cap;

        // Take-profit: when unrealized PnL >= threshold, fire an IOC
        // taker on the reducing side at the opposing touch. PostOnly at
        // mid (the old behaviour) was passive — it sat in the queue
        // waiting for someone to cross, which contradicts the
        // "aggressively close" intent. IOC at the opposing best
        // crosses immediately as taker for the full position size.
        let tp_threshold = self.config.take_profit_usdt;
        let tp_triggered = tp_threshold > Decimal::ZERO
            && ctx.position.avg_entry.0 > Decimal::ZERO
            && ctx.position.size.0 != Decimal::ZERO;
        if tp_triggered {
            let long = ctx.position.size.0 > Decimal::ZERO;
            let pos_abs = ctx.position.size.0.abs();
            let profit = if long {
                mid - ctx.position.avg_entry.0
            } else {
                ctx.position.avg_entry.0 - mid
            };
            let unrealized = profit * pos_abs;
            if unrealized >= tp_threshold {
                // Reducing side + opposing touch = guaranteed taker.
                let (tp_side, tp_price) = if long {
                    (Side::Ask, bid)
                } else {
                    (Side::Bid, ask)
                };
                actions.push(Action::Quote(QuoteIntent {
                    symbol: ctx.symbol.clone(),
                    side: tp_side,
                    price: tp_price,
                    size: Size(pos_abs),
                    tif: TimeInForce::IOC,
                    kind: QuoteKind::Point,
                }));
                return actions;
            }
        }

        let want_bid = (!capped || ctx.position.size.0 <= Decimal::ZERO)
            && !self.side_in_cooldown(Side::Bid, ts);
        let want_ask = (!capped || ctx.position.size.0 >= Decimal::ZERO)
            && !self.side_in_cooldown(Side::Ask, ts);
        if want_bid {
            actions.push(self.make_quote(ctx, Side::Bid, bid, size_mult.0));
        }
        if want_ask {
            actions.push(self.make_quote(ctx, Side::Ask, ask, size_mult.1));
        }
        actions
    }

    fn inventory_size_multiplier(&self, ctx: &StrategyContext<'_>) -> (Decimal, Decimal) {
        let size = ctx.position.size.0;
        if size > Decimal::ZERO {
            (Decimal::ONE, Decimal::from(2))
        } else if size < Decimal::ZERO {
            (Decimal::from(2), Decimal::ONE)
        } else {
            (Decimal::ONE, Decimal::ONE)
        }
    }

    fn cancel_if_live(&mut self, _ts: Timestamp) -> Vec<Action> {
        // Resetting `last_requote_ts = None` (instead of advancing it to
        // `ts`) lets us re-enter immediately when targets become valid
        // again. Stamping it would have parked the next requote behind
        // `requote_interval_ms` even though we just cancelled — a quiet
        // stall on spread re-widening.
        self.last_bid = None;
        self.last_ask = None;
        self.last_requote_ts = None;
        if self.quotes_live {
            self.quotes_live = false;
            vec![Action::CancelAll]
        } else {
            Vec::new()
        }
    }
}

impl Strategy for SpreadScalpOld {
    type Config = SpreadScalpOldConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            last_bid: None,
            last_ask: None,
            last_requote_ts: None,
            quotes_live: false,
            last_reject_bid_ts: None,
            last_reject_ask_ts: None,
        }
    }

    fn name(&self) -> &str {
        "spread-scalp-old"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        let (snapshot, ts) = match event {
            MarketEvent::BookUpdate { snapshot } => (snapshot, snapshot.ts),
            MarketEvent::Heartbeat { ts } => (ctx.latest_book, *ts),
            MarketEvent::Trade { .. } => return Vec::new(),
            MarketEvent::Fill(fill) => {
                let ts = ctx.now;
                // Fill = inventory just moved; whatever rejection state
                // was tracked for the filled side is stale.
                self.clear_reject(fill.side);
                let Some((bid, ask)) = self.compute_targets(ctx.latest_book) else {
                    return self.cancel_if_live(ts);
                };
                let size_mult = self.inventory_size_multiplier(ctx);
                let fill_side = fill.side;
                let opp_side = if fill_side == Side::Bid {
                    Side::Ask
                } else {
                    Side::Bid
                };
                self.last_bid = Some(bid);
                self.last_ask = Some(ask);
                self.last_requote_ts = Some(ts);
                self.quotes_live = true;

                // Position-cap gate — `emit_requote` honoured it, the
                // fill path did not. A Bid fill drives us long; if we
                // already passed the cap, suppress the replacement Bid
                // (and any opp-side top-up that would lean further in
                // the same direction). Same logic mirrored for Ask /
                // short. `<= 0` / `>= 0` keeps the inclusive flat case.
                let mid = (bid.0 + ask.0) / Decimal::from(2);
                let position_value = ctx.position.size.0.abs() * mid;
                let cap = self.config.max_position_usdt;
                let capped = cap > Decimal::ZERO && position_value >= cap;
                let allow_bid = (!capped || ctx.position.size.0 <= Decimal::ZERO)
                    && !self.side_in_cooldown(Side::Bid, ts);
                let allow_ask = (!capped || ctx.position.size.0 >= Decimal::ZERO)
                    && !self.side_in_cooldown(Side::Ask, ts);

                // Filled side replacement.
                let mut actions = Vec::new();
                let allow_filled_side = match fill_side {
                    Side::Bid => allow_bid,
                    Side::Ask => allow_ask,
                };
                if allow_filled_side {
                    actions.push(self.make_quote(
                        ctx,
                        fill_side,
                        if fill_side == Side::Bid { bid } else { ask },
                        if fill_side == Side::Bid {
                            size_mult.0
                        } else {
                            size_mult.1
                        },
                    ));
                }

                // Opp-side top-up — only if cap allows growing that side.
                let allow_opp_side = match opp_side {
                    Side::Bid => allow_bid,
                    Side::Ask => allow_ask,
                };
                if allow_opp_side {
                    let opp_mult = if opp_side == Side::Bid {
                        size_mult.0
                    } else {
                        size_mult.1
                    };
                    let opp_price = if opp_side == Side::Bid { bid } else { ask };
                    let existing_opp: Decimal = ctx
                        .open_quotes
                        .iter()
                        .filter(|q| q.1.side == opp_side)
                        .map(|q| q.1.size.0)
                        .sum();
                    let desired_total = self.quote_size(opp_price, opp_mult);
                    if desired_total > existing_opp {
                        let extra = desired_total - existing_opp;
                        let step = self.config.step_size;
                        let extra = if step > Decimal::ZERO {
                            (extra / step).floor() * step
                        } else {
                            extra
                        };
                        if extra > Decimal::ZERO {
                            actions.push(Action::Quote(QuoteIntent {
                                symbol: ctx.symbol.clone(),
                                side: opp_side,
                                price: opp_price,
                                size: Size(extra),
                                tif: TimeInForce::PostOnly,
                                kind: QuoteKind::Point,
                            }));
                        }
                    }
                }
                return actions;
            }
        };
        let Some((bid, ask)) = self.compute_targets(snapshot) else {
            return self.cancel_if_live(ts);
        };
        if !self.should_requote(bid, ask, ts) {
            return vec![Action::NoOp];
        }
        self.emit_requote(ctx, bid, ask, ts)
    }

    fn on_quote_rejected(
        &mut self,
        ctx: &StrategyContext<'_>,
        intent: &tikr_venue::QuoteIntent,
        _reason: &str,
    ) -> Vec<Action> {
        // Stamp the cooldown on the side that just bounced so the next
        // rebuild attempt within `reject_cooldown_ms` skips this side
        // (see SG `last_refill_*_ts` for the parent pattern). Without
        // this, fast moves can produce a -5022 → rebuild → -5022 hot
        // loop because every rejection nukes the price cache and
        // re-emits both sides.
        let ts = ctx.now;
        self.mark_reject(intent.side, ts);
        self.last_bid = None;
        self.last_ask = None;
        self.last_requote_ts = None;
        let Some((bid, ask)) = self.compute_targets(ctx.latest_book) else {
            return self.cancel_if_live(ts);
        };
        self.emit_requote(ctx, bid, ask, ts)
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
    use tikr_core::{Asset, Level, MarketKind, Notional, Position, SignedSize, Symbol, VenueId};

    fn sym() -> Symbol {
        Symbol {
            base: Asset::new("BTC"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("test"),
            kind: MarketKind::Perp,
        }
    }

    fn book(symbol: &Symbol, bid: i64, ask: i64, ts: u64) -> Snapshot {
        Snapshot {
            symbol: symbol.clone(),
            bids: vec![Level {
                price: Price(Decimal::from(bid)),
                size: Size(Decimal::ONE),
            }],
            asks: vec![Level {
                price: Price(Decimal::from(ask)),
                size: Size(Decimal::ONE),
            }],
            ts: Timestamp(ts),
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
    ) -> StrategyContext<'a> {
        StrategyContext {
            symbol,
            now: snapshot.ts,
            position,
            recent_fills: &[],
            latest_book: snapshot,
            open_quotes: &[],
        }
    }

    fn strategy() -> SpreadScalp {
        SpreadScalpOld::new(SpreadScalpOldConfig {
            notional_per_order: Decimal::from(100),
            tick_size: Decimal::from(1),
            step_size: Decimal::from(1),
            min_notional: Decimal::ZERO,
            min_spread_bps: Decimal::from(5),
            requote_interval_ms: 1000,
            max_position_usdt: Decimal::ZERO,
            take_profit_usdt: Decimal::ZERO,
            reject_cooldown_ms: 0,
        })
    }

    #[test]
    fn wide_spread_quotes_at_best() {
        let symbol = sym();
        let snapshot = book(&symbol, 100, 110, 1);
        let position = pos(&symbol);
        let mut strategy = strategy();
        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        assert_eq!(actions.len(), 3);
        match (&actions[1], &actions[2]) {
            (Action::Quote(bid), Action::Quote(ask)) => {
                // 1 tick inside best bid/ask
                assert_eq!(bid.price.0, Decimal::from(101));
                assert_eq!(ask.price.0, Decimal::from(109));
            }
            _ => panic!("expected quotes"),
        }
    }

    #[test]
    fn narrow_spread_does_not_quote() {
        let symbol = sym();
        let snapshot = book(&symbol, 100, 100, 1);
        let position = pos(&symbol);
        let mut strategy = strategy();
        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        assert!(
            actions.is_empty(),
            "narrow spread should produce no actions, got {:?}",
            actions
        );
    }

    #[test]
    fn does_not_requote_when_already_at_best() {
        let symbol = sym();
        let snapshot = book(&symbol, 100, 110, 1);
        let position = pos(&symbol);
        let mut strategy = strategy();
        let first = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        assert_eq!(first.len(), 3);

        let second = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        assert!(matches!(second.as_slice(), [Action::NoOp]));
    }

    #[test]
    fn requotes_when_market_moves() {
        let symbol = sym();
        let first = book(&symbol, 100, 110, 1);
        let moved = book(&symbol, 102, 112, 2_000_000_000);
        let position = pos(&symbol);
        let mut strategy = strategy();
        let _ = strategy.on_event(
            &ctx(&symbol, &first, &position),
            &MarketEvent::BookUpdate {
                snapshot: first.clone(),
            },
        );
        let actions = strategy.on_event(
            &ctx(&symbol, &moved, &position),
            &MarketEvent::BookUpdate {
                snapshot: moved.clone(),
            },
        );
        assert_eq!(actions.len(), 3);
        match (&actions[1], &actions[2]) {
            (Action::Quote(bid), Action::Quote(ask)) => {
                // 1 tick inside best bid/ask
                assert_eq!(bid.price.0, Decimal::from(103));
                assert_eq!(ask.price.0, Decimal::from(111));
            }
            _ => panic!("expected quotes"),
        }
    }

    #[test]
    fn long_inventory_sizes_ask_larger() {
        let symbol = sym();
        let snapshot = book(&symbol, 50, 60, 1);
        let position = pos_with_size(&symbol, Decimal::new(5, 1));
        let mut strategy = strategy();
        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        assert_eq!(actions.len(), 3);
        match (&actions[1], &actions[2]) {
            (Action::Quote(bid), Action::Quote(ask)) => {
                assert!(
                    ask.size.0 > bid.size.0,
                    "ask={} bid={}",
                    ask.size.0,
                    bid.size.0
                );
            }
            _ => panic!("expected quotes"),
        }
    }

    #[test]
    fn short_inventory_sizes_bid_larger() {
        let symbol = sym();
        let snapshot = book(&symbol, 100, 110, 1);
        let position = pos_with_size(&symbol, Decimal::new(-5, 1));
        let mut strategy = strategy();
        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        assert_eq!(actions.len(), 3);
        match (&actions[1], &actions[2]) {
            (Action::Quote(bid), Action::Quote(ask)) => {
                assert!(bid.size.0 > ask.size.0);
            }
            _ => panic!("expected quotes"),
        }
    }

    #[test]
    fn cancel_when_spread_narrows() {
        let symbol = sym();
        let wide = book(&symbol, 100, 110, 1);
        let narrow = book(&symbol, 100, 100, 2);
        let position = pos(&symbol);
        let mut strategy = strategy();
        let _ = strategy.on_event(
            &ctx(&symbol, &wide, &position),
            &MarketEvent::BookUpdate {
                snapshot: wide.clone(),
            },
        );
        let actions = strategy.on_event(
            &ctx(&symbol, &narrow, &position),
            &MarketEvent::BookUpdate {
                snapshot: narrow.clone(),
            },
        );
        assert!(matches!(actions.as_slice(), [Action::CancelAll]));
    }

    #[test]
    fn fill_triggers_requote() {
        let symbol = sym();
        let snapshot = book(&symbol, 100, 110, 1);
        let position = pos(&symbol);
        let mut strategy = strategy();
        let _ = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::BookUpdate {
                snapshot: snapshot.clone(),
            },
        );
        let actions = strategy.on_event(
            &ctx(&symbol, &snapshot, &position),
            &MarketEvent::Fill(tikr_core::Fill {
                quote_id: tikr_venue::QuoteId::new(),
                price: Price(Decimal::from(101)),
                size: Size(Decimal::ONE),
                fee_asset: Asset::new("USDT"),
                fee_amount: Decimal::ZERO,
                fee_quote: Notional(Decimal::ZERO),
                side: Side::Bid,
                ts: Timestamp(2),
                is_full: true,
            }),
        );
        // Fill replaces only the filled side; opposite side stays live.
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::Quote(q) => assert_eq!(q.side, Side::Bid),
            other => panic!("expected Quote, got {:?}", other),
        }
    }
}
