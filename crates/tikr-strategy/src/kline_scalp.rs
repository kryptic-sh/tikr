//! Kline-aware spread scalping strategy.
//!
//! Like [`SpreadScalp`] but skews quotes based on mid-price momentum
//! detected from recent BookUpdate events. When the mid is trending up
//! (momentum), the ask side tightens (aggressive) and the bid side widens
//! (defensive). Down-trend flips the skew. Flat / choppy → symmetric.
//!
//! Momentum is measured as the mid-price change over the last N requote
//! cycles (not wall-clock seconds), making it adaptive to the venue's
//! actual event cadence.
//!
//! # Backtest
//!
//! A standalone kline-based backtester lives at
//! [`backtest_kline_scalp`][crate::bin::backtest_kline_scalp]. It uses
//! real OHLC data for both momentum detection and fill simulation.

use std::collections::VecDeque;

use tikr_core::{
    Decimal, MarketEvent, Price, QuoteKind, Side, Size, Snapshot, TimeInForce, Timestamp,
};
use tikr_venue::QuoteIntent;

use crate::{Action, Strategy, StrategyContext};

/// Configuration for [`KlineScalp`].
#[derive(Debug, Clone)]
pub struct KlineScalpConfig {
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
    /// When exceeded, quotes only the reducing side at mid price to close
    /// the position aggressively. 0 = disabled.
    pub take_profit_usdt: Decimal,
    /// Number of requote cycles to look back for momentum detection.
    /// 0 = disable momentum skew (behaves like plain SpreadScalp).
    pub momentum_lookback: u32,
    /// Minimum mid-price change in bps (over the lookback window) to
    /// classify as momentum. Default 10bps.
    pub momentum_bps_threshold: Decimal,
    /// Skew multiplier applied to the defensive side's bps distance
    /// when momentum is detected. E.g. 3.0 = defensive side is 3×
    /// further from mid than usual.
    pub momentum_skew_mult: Decimal,
}

/// Kline-aware spread scalping strategy state.
pub struct KlineScalp {
    config: KlineScalpConfig,
    last_bid: Option<Price>,
    last_ask: Option<Price>,
    last_requote_ts: Option<Timestamp>,
    quotes_live: bool,
    /// Rolling mid prices at requote time, newest at back.
    mid_prices: VecDeque<Price>,
    /// Current momentum signal: 1 = up, -1 = down, 0 = flat.
    momentum: i8,
}

/// The three momentum regimes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MomentumSignal {
    Up = 1,
    Down = -1,
    Flat = 0,
}

impl KlineScalp {
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
        let mut size = self.quote_size(price, size_multiplier);
        if self.config.min_notional > Decimal::ZERO
            && size * price.0 < self.config.min_notional
            && self.config.step_size > Decimal::ZERO
        {
            size += self.config.step_size;
        }
        Action::Quote(QuoteIntent {
            symbol: ctx.symbol.clone(),
            side,
            price,
            size: Size(size),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        })
    }

    /// Compute the current momentum signal from the rolling mid-price buffer.
    fn compute_momentum(&self) -> MomentumSignal {
        let lookback = self.config.momentum_lookback as usize;
        if lookback == 0 || self.mid_prices.len() <= lookback {
            return MomentumSignal::Flat;
        }
        let Some(current) = self.mid_prices.back() else {
            return MomentumSignal::Flat;
        };
        let Some(past) = self.mid_prices.get(self.mid_prices.len() - 1 - lookback) else {
            return MomentumSignal::Flat;
        };
        if past.0 <= Decimal::ZERO || current.0 <= Decimal::ZERO {
            return MomentumSignal::Flat;
        }
        let change_bps = (current.0 - past.0) / past.0 * Decimal::from(10_000);
        if change_bps > self.config.momentum_bps_threshold {
            MomentumSignal::Up
        } else if change_bps < -self.config.momentum_bps_threshold {
            MomentumSignal::Down
        } else {
            MomentumSignal::Flat
        }
    }

    /// Apply momentum skew to a base bps distance from mid.
    /// `side` is the side being quoted, `is_aggressive` means we tighten it.
    fn skewed_bps(&self, side: Side, base_bps: Decimal) -> Decimal {
        let m = self.momentum;
        if m == 0 || self.config.momentum_skew_mult <= Decimal::ONE {
            return base_bps;
        }
        match (side, m) {
            // Momentum up: tighten ask (aggressive), widen bid (defensive).
            (Side::Ask, 1) => (base_bps / self.config.momentum_skew_mult).max(Decimal::ONE),
            (Side::Bid, 1) => base_bps * self.config.momentum_skew_mult,
            // Momentum down: tighten bid (aggressive), widen ask (defensive).
            (Side::Bid, -1) => (base_bps / self.config.momentum_skew_mult).max(Decimal::ONE),
            (Side::Ask, -1) => base_bps * self.config.momentum_skew_mult,
            _ => base_bps,
        }
    }

    fn should_requote(&self, bid: Price, ask: Price, ts: Timestamp) -> bool {
        if let (Some(last_bid), Some(last_ask)) = (self.last_bid, self.last_ask)
            && last_bid.0 == bid.0
            && last_ask.0 == ask.0
            && self.mid_prices.len() <= 1
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
        self.last_bid = Some(bid);
        self.last_ask = Some(ask);
        self.last_requote_ts = Some(ts);
        self.quotes_live = true;

        let mid = (bid.0 + ask.0) / Decimal::from(2);
        self.mid_prices.push_back(Price(mid));
        let keep = (self.config.momentum_lookback as usize)
            .saturating_add(1)
            .max(2);
        while self.mid_prices.len() > keep {
            self.mid_prices.pop_front();
        }
        self.momentum = self.compute_momentum() as i8;

        let base_bps = self.config.min_spread_bps;
        let bid_bps = self.skewed_bps(Side::Bid, base_bps);
        let ask_bps = self.skewed_bps(Side::Ask, base_bps);

        let quoted_bid = Price(mid * (Decimal::ONE - bid_bps / Decimal::from(10_000)));
        let quoted_ask = Price(mid * (Decimal::ONE + ask_bps / Decimal::from(10_000)));

        let size_mult = self.inventory_size_multiplier(ctx);
        let mut actions = vec![Action::CancelAll];

        let position_value = ctx.position.size.0.abs() * mid;
        let cap = self.config.max_position_usdt;
        let capped = cap > Decimal::ZERO && position_value >= cap;

        let tp_threshold = self.config.take_profit_usdt;
        let tp_triggered = tp_threshold > Decimal::ZERO
            && ctx.position.avg_entry.0 > Decimal::ZERO
            && ctx.position.size.0 != Decimal::ZERO;
        if tp_triggered {
            let (long, pos_abs) = (
                ctx.position.size.0 > Decimal::ZERO,
                ctx.position.size.0.abs(),
            );
            let profit = if long {
                mid - ctx.position.avg_entry.0
            } else {
                ctx.position.avg_entry.0 - mid
            };
            let unrealized = profit * pos_abs;
            if unrealized >= tp_threshold {
                let tp_side = if long { Side::Ask } else { Side::Bid };
                actions.push(self.make_quote(ctx, tp_side, Price(mid), Decimal::ONE));
                return actions;
            }
        }

        if !capped || ctx.position.size.0 <= Decimal::ZERO {
            actions.push(self.make_quote(ctx, Side::Bid, quoted_bid, size_mult.0));
        }
        if !capped || ctx.position.size.0 >= Decimal::ZERO {
            actions.push(self.make_quote(ctx, Side::Ask, quoted_ask, size_mult.1));
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

    fn cancel_if_live(&mut self, ts: Timestamp) -> Vec<Action> {
        self.last_bid = None;
        self.last_ask = None;
        self.last_requote_ts = Some(ts);
        if self.quotes_live {
            self.quotes_live = false;
            vec![Action::CancelAll]
        } else {
            Vec::new()
        }
    }
}

impl Strategy for KlineScalp {
    type Config = KlineScalpConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            last_bid: None,
            last_ask: None,
            last_requote_ts: None,
            quotes_live: false,
            mid_prices: VecDeque::new(),
            momentum: 0,
        }
    }

    fn name(&self) -> &str {
        "kline-scalp"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        let (snapshot, ts) = match event {
            MarketEvent::BookUpdate { snapshot } => (snapshot, snapshot.ts),
            MarketEvent::Heartbeat { ts } => (ctx.latest_book, *ts),
            MarketEvent::Trade { .. } => return Vec::new(),
            MarketEvent::Fill(fill) => {
                let ts = ctx.now;
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
                let mut actions = vec![self.make_quote(
                    ctx,
                    fill_side,
                    if fill_side == Side::Bid { bid } else { ask },
                    if fill_side == Side::Bid {
                        size_mult.0
                    } else {
                        size_mult.1
                    },
                )];
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
        _intent: &tikr_venue::QuoteIntent,
        _reason: &str,
    ) -> Vec<Action> {
        self.last_bid = None;
        self.last_ask = None;
        self.last_requote_ts = None;
        let ts = ctx.now;
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
    use std::str::FromStr;
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

    fn strategy() -> KlineScalp {
        KlineScalp::new(KlineScalpConfig {
            notional_per_order: Decimal::from(100),
            tick_size: Decimal::from(1),
            step_size: Decimal::from(1),
            min_notional: Decimal::ZERO,
            min_spread_bps: Decimal::from(12),
            requote_interval_ms: 1000,
            max_position_usdt: Decimal::ZERO,
            take_profit_usdt: Decimal::ZERO,
            momentum_lookback: 3,
            momentum_bps_threshold: Decimal::new(10, 0),
            momentum_skew_mult: Decimal::from(3),
        })
    }

    #[test]
    fn wide_spread_quotes_symmetric_by_default() {
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
                // spacing around mid (105): 12bps = 0.126
                // bid at 105 * (1 - 12/10000) ≈ 104.874
                // ask at 105 * (1 + 12/10000) ≈ 105.126
                assert!(bid.price.0 < ask.price.0);
                assert!(bid.price.0 > Decimal::from(100));
                assert!(ask.price.0 < Decimal::from(110));
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
            "narrow spread should produce no actions"
        );
    }

    #[test]
    fn flat_momentum_on_startup() {
        let mut strategy = strategy();
        // Push one mid price — not enough for lookback
        strategy.mid_prices.push_back(Price(Decimal::from(105)));
        assert_eq!(strategy.compute_momentum(), MomentumSignal::Flat);
    }

    #[test]
    fn momentum_up_detected() {
        let mut strategy = strategy();
        let lookback = strategy.config.momentum_lookback as usize;
        for i in 0..=lookback {
            // Mid climbs 10 per step
            strategy
                .mid_prices
                .push_back(Price(Decimal::from(100 + i as i64 * 10)));
        }
        assert_eq!(strategy.compute_momentum(), MomentumSignal::Up);
    }

    #[test]
    fn momentum_down_detected() {
        let mut strategy = strategy();
        let lookback = strategy.config.momentum_lookback as usize;
        for i in 0..=lookback {
            strategy
                .mid_prices
                .push_back(Price(Decimal::from(200 - i as i64 * 10)));
        }
        assert_eq!(strategy.compute_momentum(), MomentumSignal::Down);
    }

    #[test]
    fn skewed_bps_tightens_aggressive_side() {
        // Test skewed_bps directly by constructing a strategy and manually
        // setting the momentum field via the only available path:
        // feed rising prices into mid_prices so compute_momentum() returns Up.
        let mut strategy = strategy();
        let base = Decimal::from(5);
        let skew = strategy.config.momentum_skew_mult;
        let tight = (base / skew).max(Decimal::ONE);
        let wide = base * skew;
        // Push prices where last - first > threshold → momentum Up
        for p in [100i64, 100, 101, 102] {
            strategy.mid_prices.push_back(Price(Decimal::from(p)));
        }
        assert_eq!(strategy.compute_momentum(), MomentumSignal::Up);
        // Set stored momentum so skewed_bps picks it up (normally done
        // inside emit_requote).
        strategy.momentum = MomentumSignal::Up as i8;
        assert_eq!(strategy.skewed_bps(Side::Ask, base), tight);
        assert_eq!(strategy.skewed_bps(Side::Bid, base), wide);

        // Down: reverse the trend
        strategy.mid_prices.clear();
        for p in [200i64, 200, 199, 198] {
            strategy.mid_prices.push_back(Price(Decimal::from(p)));
        }
        assert_eq!(strategy.compute_momentum(), MomentumSignal::Down);
        strategy.momentum = MomentumSignal::Down as i8;
        assert_eq!(strategy.skewed_bps(Side::Bid, base), tight);
        assert_eq!(strategy.skewed_bps(Side::Ask, base), wide);

        // Flat: need change < threshold over lookback
        strategy.mid_prices.clear();
        strategy.momentum = 0;
        for p in [100i64, 100, 100, 100] {
            strategy.mid_prices.push_back(Price(Decimal::from(p)));
        }
        assert_eq!(strategy.compute_momentum(), MomentumSignal::Flat);
        strategy.momentum = MomentumSignal::Flat as i8;
        assert_eq!(strategy.skewed_bps(Side::Bid, base), base);
        assert_eq!(strategy.skewed_bps(Side::Ask, base), base);
    }
}
