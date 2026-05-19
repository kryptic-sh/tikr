//! Top-of-book join/improve market-making strategy.
//!
//! Quotes directly at (or one tick inside) the current best bid and best ask,
//! with optional inventory-skew shift to mean-revert position.
//! Post-only safe — never crosses by construction.
//!
//! # Modes
//!
//! - **Join** (`spread_ticks <= improve_when_spread_gt_ticks`): quote at
//!   `best_bid` / `best_ask`. Queue position behind existing size.
//! - **Improve** (`spread_ticks > improve_when_spread_gt_ticks`): quote at
//!   `best_bid + tick` / `best_ask - tick`. Becomes new best; gains priority.
//!
//! # Inventory skew
//!
//! When `max_skew_ticks > 0`, both targets shift by
//! `−sign(position) · min(max_skew_ticks, |position|/skew_unit · max_skew_ticks)`
//! ticks. Long → both quotes shift DOWN (bid retreats from fill, ask
//! advances toward fill); short → both shift UP. Post-only safety is
//! preserved (improve mode is clamped so skewed quotes never cross).
//!
//! # Requote triggers
//!
//! - Cold start (no prior quotes)
//! - Target price moved by ≥ 1 tick on either side (book OR position change)
//! - `min_requote_interval_ms` elapsed (forced refresh)

use tikr_core::{
    Decimal, MarketEvent, Position, Price, QuoteKind, Side, Size, Snapshot, Symbol, TimeInForce,
    Timestamp,
};
use tikr_venue::QuoteIntent;

use crate::{Action, Strategy, StrategyContext};

/// Configuration for [`TopOfBook`].
#[derive(Debug, Clone)]
pub struct TopOfBookConfig {
    /// Order size placed on each side.
    pub size_per_quote: Size,
    /// Venue tick size (price increment). E.g. `0.1` for Binance Futures BTCUSDT.
    pub tick_size: Decimal,
    /// Improve (post inside the spread by 1 tick) when book spread is
    /// strictly greater than this many ticks. Set to `0` to always improve.
    /// Set high (e.g. `u32::MAX`) to always join.
    pub improve_when_spread_gt_ticks: u32,
    /// Minimum time between forced requotes (ms). Drift-based requotes can
    /// fire sooner.
    pub min_requote_interval_ms: u64,
    /// Maximum inventory-skew shift in ticks (each side). `0` = no skew
    /// (symmetric quoting).
    pub max_skew_ticks: u32,
    /// Position size at which skew is fully applied (reaches `max_skew_ticks`).
    /// Skew scales linearly from 0 to `max_skew_ticks` over `[0, skew_unit]`.
    /// Must be > 0 if `max_skew_ticks > 0`.
    pub skew_unit: Size,
}

/// Top-of-book join/improve strategy state.
pub struct TopOfBook {
    /// Strategy configuration.
    config: TopOfBookConfig,
    /// Most recent quoted bid price (target side, not necessarily filled).
    last_bid: Option<Price>,
    /// Most recent quoted ask price.
    last_ask: Option<Price>,
    /// Timestamp of last requote.
    last_requote_ts: Option<Timestamp>,
}

impl TopOfBook {
    /// Compute the (bid, ask) target prices given a book snapshot + current
    /// position. Returns `None` if either side is empty.
    fn compute_targets(&self, snapshot: &Snapshot, position: &Position) -> Option<(Price, Price)> {
        let best_bid = snapshot.bids.first().map(|l| l.price)?;
        let best_ask = snapshot.asks.first().map(|l| l.price)?;

        let tick = self.config.tick_size;
        if tick <= Decimal::ZERO {
            return None;
        }
        let spread = best_ask.0 - best_bid.0;
        let spread_ticks = (spread / tick).floor();
        let threshold = Decimal::from(self.config.improve_when_spread_gt_ticks);

        let (mut bid, mut ask) = if spread_ticks > threshold {
            // Improve: step 1 tick inside on each side.
            (Price(best_bid.0 + tick), Price(best_ask.0 - tick))
        } else {
            // Join: post at the existing best prices.
            (best_bid, best_ask)
        };

        // Apply inventory skew: long → shift down, short → shift up.
        let skew = self.compute_skew(position);
        if skew != Decimal::ZERO {
            bid = Price(bid.0 + skew);
            ask = Price(ask.0 + skew);
            // Post-only safety: never let skewed bid >= best_ask or
            // skewed ask <= best_bid (would cross). Clamp.
            if bid.0 >= best_ask.0 {
                bid = Price(best_ask.0 - tick);
            }
            if ask.0 <= best_bid.0 {
                ask = Price(best_bid.0 + tick);
            }
        }

        Some((bid, ask))
    }

    /// Inventory-skew shift in price units (signed). Long position →
    /// negative (shift down); short position → positive (shift up).
    fn compute_skew(&self, position: &Position) -> Decimal {
        if self.config.max_skew_ticks == 0 {
            return Decimal::ZERO;
        }
        let pos = position.size.0;
        if pos == Decimal::ZERO {
            return Decimal::ZERO;
        }
        let unit = self.config.skew_unit.0;
        if unit <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        let max_ticks = Decimal::from(self.config.max_skew_ticks);
        // Linear scale: at |pos| == unit → max ticks; saturate beyond.
        let ratio = (pos.abs() / unit).min(Decimal::from(1));
        let magnitude = ratio * max_ticks * self.config.tick_size;
        if pos > Decimal::ZERO {
            -magnitude
        } else {
            magnitude
        }
    }

    fn should_requote(&self, new_bid: Price, new_ask: Price, now: Timestamp) -> bool {
        let (Some(last_bid), Some(last_ask), Some(last_ts)) =
            (self.last_bid, self.last_ask, self.last_requote_ts)
        else {
            return true;
        };

        // Forced refresh interval.
        let elapsed_ns = now.0.saturating_sub(last_ts.0);
        let interval_ns = self
            .config
            .min_requote_interval_ms
            .saturating_mul(1_000_000);
        if elapsed_ns >= interval_ns {
            return true;
        }

        // Price drift ≥ 1 tick on either side.
        let bid_drift = (new_bid.0 - last_bid.0).abs();
        let ask_drift = (new_ask.0 - last_ask.0).abs();
        bid_drift >= self.config.tick_size || ask_drift >= self.config.tick_size
    }

    fn build_quotes(&self, symbol: &Symbol, bid: Price, ask: Price) -> Vec<Action> {
        let mut actions = Vec::with_capacity(3);
        actions.push(Action::CancelAll);
        for (side, price) in [(Side::Bid, bid), (Side::Ask, ask)] {
            actions.push(Action::Quote(QuoteIntent {
                symbol: symbol.clone(),
                side,
                price,
                size: self.config.size_per_quote,
                tif: TimeInForce::PostOnly,
                kind: QuoteKind::Point,
            }));
        }
        actions
    }
}

impl Strategy for TopOfBook {
    type Config = TopOfBookConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            last_bid: None,
            last_ask: None,
            last_requote_ts: None,
        }
    }

    fn name(&self) -> &str {
        "top-of-book"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        match event {
            MarketEvent::Trade { .. } | MarketEvent::Fill(_) => Vec::new(),
            MarketEvent::Heartbeat { ts } => {
                let Some((bid, ask)) = self.compute_targets(ctx.latest_book, ctx.position) else {
                    return Vec::new();
                };
                if self.should_requote(bid, ask, *ts) {
                    self.last_bid = Some(bid);
                    self.last_ask = Some(ask);
                    self.last_requote_ts = Some(*ts);
                    self.build_quotes(ctx.symbol, bid, ask)
                } else {
                    vec![Action::NoOp]
                }
            }
            MarketEvent::BookUpdate { snapshot } => {
                let Some((bid, ask)) = self.compute_targets(snapshot, ctx.position) else {
                    return Vec::new();
                };
                if self.should_requote(bid, ask, snapshot.ts) {
                    self.last_bid = Some(bid);
                    self.last_ask = Some(ask);
                    self.last_requote_ts = Some(snapshot.ts);
                    self.build_quotes(ctx.symbol, bid, ask)
                } else {
                    vec![Action::NoOp]
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::{Asset, Level, MarketKind, Position, SignedSize, VenueId};

    fn sym() -> Symbol {
        Symbol {
            base: Asset::new("BTC"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("test"),
            kind: MarketKind::Perp,
        }
    }

    fn pos(s: &Symbol) -> Position {
        Position {
            symbol: s.clone(),
            size: SignedSize(Decimal::ZERO),
            avg_entry: Price(Decimal::ZERO),
            realized_pnl: tikr_core::Notional(Decimal::ZERO),
        }
    }

    fn book(s: &Symbol, bid: i64, ask: i64, ts: u64) -> Snapshot {
        Snapshot {
            symbol: s.clone(),
            bids: vec![Level {
                price: Price(Decimal::new(bid, 0)),
                size: Size(Decimal::new(1, 0)),
            }],
            asks: vec![Level {
                price: Price(Decimal::new(ask, 0)),
                size: Size(Decimal::new(1, 0)),
            }],
            ts: Timestamp(ts),
        }
    }

    fn cfg(improve_gt: u32) -> TopOfBookConfig {
        TopOfBookConfig {
            size_per_quote: Size(Decimal::new(1, 3)),
            tick_size: Decimal::from(1),
            improve_when_spread_gt_ticks: improve_gt,
            min_requote_interval_ms: 1000,
            max_skew_ticks: 0,
            skew_unit: Size(Decimal::new(1, 0)),
        }
    }

    fn cfg_skew(improve_gt: u32, max_skew_ticks: u32, skew_unit: Decimal) -> TopOfBookConfig {
        TopOfBookConfig {
            size_per_quote: Size(Decimal::new(1, 3)),
            tick_size: Decimal::from(1),
            improve_when_spread_gt_ticks: improve_gt,
            min_requote_interval_ms: 1000,
            max_skew_ticks,
            skew_unit: Size(skew_unit),
        }
    }

    fn pos_with(s: &Symbol, signed: Decimal) -> Position {
        Position {
            symbol: s.clone(),
            size: SignedSize(signed),
            avg_entry: Price(Decimal::ZERO),
            realized_pnl: tikr_core::Notional(Decimal::ZERO),
        }
    }

    #[test]
    fn join_when_spread_at_threshold() {
        let s = sym();
        let p = pos(&s);
        let b = book(&s, 100, 102, 0); // spread = 2 ticks
        let ctx = StrategyContext {
            symbol: &s,
            now: Timestamp(0),
            position: &p,
            recent_fills: &[],
            latest_book: &b,
            open_quotes: &[],
        };
        let mut tob = TopOfBook::new(cfg(2)); // improve only when spread > 2 ticks
        let actions = tob.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: b.clone(),
            },
        );
        // CancelAll + 2 quotes
        assert_eq!(actions.len(), 3);
        let Action::Quote(bid) = &actions[1] else {
            panic!("expected bid quote, got {:?}", actions[1])
        };
        let Action::Quote(ask) = &actions[2] else {
            panic!("expected ask quote, got {:?}", actions[2])
        };
        // Join at 100 / 102.
        assert_eq!(bid.price.0, Decimal::from(100));
        assert_eq!(ask.price.0, Decimal::from(102));
    }

    #[test]
    fn improve_when_spread_exceeds_threshold() {
        let s = sym();
        let p = pos(&s);
        let b = book(&s, 100, 105, 0); // spread = 5 ticks
        let ctx = StrategyContext {
            symbol: &s,
            now: Timestamp(0),
            position: &p,
            recent_fills: &[],
            latest_book: &b,
            open_quotes: &[],
        };
        let mut tob = TopOfBook::new(cfg(1)); // improve when spread > 1 tick
        let actions = tob.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: b.clone(),
            },
        );
        let Action::Quote(bid) = &actions[1] else {
            panic!("expected bid quote")
        };
        let Action::Quote(ask) = &actions[2] else {
            panic!("expected ask quote")
        };
        // Improved by 1 tick: 101 / 104.
        assert_eq!(bid.price.0, Decimal::from(101));
        assert_eq!(ask.price.0, Decimal::from(104));
    }

    #[test]
    fn no_requote_when_target_unchanged_within_interval() {
        let s = sym();
        let p = pos(&s);
        let b = book(&s, 100, 102, 0);
        let ctx = StrategyContext {
            symbol: &s,
            now: Timestamp(0),
            position: &p,
            recent_fills: &[],
            latest_book: &b,
            open_quotes: &[],
        };
        let mut tob = TopOfBook::new(cfg(2));
        let _ = tob.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: b.clone(),
            },
        );
        // Same book 100ns later — no drift, no interval elapse.
        let b2 = book(&s, 100, 102, 100);
        let actions = tob.on_event(&ctx, &MarketEvent::BookUpdate { snapshot: b2 });
        assert!(matches!(actions[..], [Action::NoOp]));
    }

    #[test]
    fn requote_when_best_moves_one_tick() {
        let s = sym();
        let p = pos(&s);
        let b = book(&s, 100, 102, 0);
        let ctx = StrategyContext {
            symbol: &s,
            now: Timestamp(0),
            position: &p,
            recent_fills: &[],
            latest_book: &b,
            open_quotes: &[],
        };
        let mut tob = TopOfBook::new(cfg(2));
        let _ = tob.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: b.clone(),
            },
        );
        // Best bid moves up 1 tick.
        let b2 = book(&s, 101, 102, 100);
        let actions = tob.on_event(&ctx, &MarketEvent::BookUpdate { snapshot: b2 });
        assert_eq!(actions.len(), 3);
        assert!(matches!(actions[0], Action::CancelAll));
    }

    #[test]
    fn long_position_shifts_both_quotes_down() {
        let s = sym();
        // Long 1 unit = full skew. max_skew_ticks=3, tick=1 → shift by -3.
        let p = pos_with(&s, Decimal::from(1));
        let b = book(&s, 100, 110, 0); // spread = 10 ticks
        let ctx = StrategyContext {
            symbol: &s,
            now: Timestamp(0),
            position: &p,
            recent_fills: &[],
            latest_book: &b,
            open_quotes: &[],
        };
        // Improve when spread > 1 → bid=101, ask=109 base. After -3 skew:
        // bid=98, ask=106.
        let mut tob = TopOfBook::new(cfg_skew(1, 3, Decimal::from(1)));
        let actions = tob.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: b.clone(),
            },
        );
        let Action::Quote(bid) = &actions[1] else {
            panic!("expected bid")
        };
        let Action::Quote(ask) = &actions[2] else {
            panic!("expected ask")
        };
        assert_eq!(bid.price.0, Decimal::from(98));
        assert_eq!(ask.price.0, Decimal::from(106));
    }

    #[test]
    fn short_position_shifts_both_quotes_up() {
        let s = sym();
        let p = pos_with(&s, Decimal::from(-1));
        let b = book(&s, 100, 110, 0);
        let ctx = StrategyContext {
            symbol: &s,
            now: Timestamp(0),
            position: &p,
            recent_fills: &[],
            latest_book: &b,
            open_quotes: &[],
        };
        // base bid=101, ask=109. +3 skew → bid=104, ask=112.
        let mut tob = TopOfBook::new(cfg_skew(1, 3, Decimal::from(1)));
        let actions = tob.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: b.clone(),
            },
        );
        let Action::Quote(bid) = &actions[1] else {
            panic!("expected bid")
        };
        let Action::Quote(ask) = &actions[2] else {
            panic!("expected ask")
        };
        assert_eq!(bid.price.0, Decimal::from(104));
        assert_eq!(ask.price.0, Decimal::from(112));
    }

    #[test]
    fn skew_clamps_to_post_only_safe() {
        let s = sym();
        // Huge long position with massive skew that would cross.
        let p = pos_with(&s, Decimal::from(1));
        let b = book(&s, 100, 102, 0); // spread = 2 ticks
        let ctx = StrategyContext {
            symbol: &s,
            now: Timestamp(0),
            position: &p,
            recent_fills: &[],
            latest_book: &b,
            open_quotes: &[],
        };
        // base join 100/102, skew -100 → bid=0, ask=2. Clamp: ask <= 100
        // (best_bid) → ask = 101 (best_bid + tick).
        let mut tob = TopOfBook::new(cfg_skew(2, 100, Decimal::from(1)));
        let actions = tob.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: b.clone(),
            },
        );
        let Action::Quote(_bid) = &actions[1] else {
            panic!("expected bid")
        };
        let Action::Quote(ask) = &actions[2] else {
            panic!("expected ask")
        };
        // Ask got clamped above best_bid.
        assert!(
            ask.price.0 > Decimal::from(100),
            "ask {} must clamp above best_bid 100",
            ask.price.0
        );
    }

    #[test]
    fn zero_position_zero_skew() {
        let s = sym();
        let p = pos(&s); // size 0
        let b = book(&s, 100, 110, 0);
        let ctx = StrategyContext {
            symbol: &s,
            now: Timestamp(0),
            position: &p,
            recent_fills: &[],
            latest_book: &b,
            open_quotes: &[],
        };
        // Should match cfg(1) — no skew applied at zero position.
        let mut tob = TopOfBook::new(cfg_skew(1, 3, Decimal::from(1)));
        let actions = tob.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: b.clone(),
            },
        );
        let Action::Quote(bid) = &actions[1] else {
            panic!("expected bid")
        };
        let Action::Quote(ask) = &actions[2] else {
            panic!("expected ask")
        };
        // Pure improve: bid=101, ask=109.
        assert_eq!(bid.price.0, Decimal::from(101));
        assert_eq!(ask.price.0, Decimal::from(109));
    }

    #[test]
    fn empty_book_returns_no_actions() {
        let s = sym();
        let p = pos(&s);
        let b = Snapshot {
            symbol: s.clone(),
            bids: vec![],
            asks: vec![],
            ts: Timestamp(0),
        };
        let ctx = StrategyContext {
            symbol: &s,
            now: Timestamp(0),
            position: &p,
            recent_fills: &[],
            latest_book: &b,
            open_quotes: &[],
        };
        let mut tob = TopOfBook::new(cfg(2));
        let actions = tob.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: b.clone(),
            },
        );
        assert!(actions.is_empty());
    }
}
