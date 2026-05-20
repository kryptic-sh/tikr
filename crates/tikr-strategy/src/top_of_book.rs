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

use tikr_core::{Decimal, MarketEvent, Position, Price, Size, Snapshot, Timestamp};

use crate::{
    Action, Strategy, StrategyContext, inventory_skew_price, post_only_pair,
    should_requote_on_tick_drift,
};

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
    /// Maximum book-imbalance shift in ticks (each side). Top-of-book size
    /// imbalance `(bid_size - ask_size) / (bid_size + ask_size)` in `[-1,
    /// +1]` is scaled to `[-max, +max]` ticks and added to both quotes.
    /// Positive imbalance (bid-heavy) shifts both UP — expects price drift
    /// up, wants to capture the uptick on the ask. `0` disables the term.
    pub max_imbalance_ticks: u32,
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
        let best_bid_lvl = snapshot.bids.first()?;
        let best_ask_lvl = snapshot.asks.first()?;
        let best_bid = best_bid_lvl.price;
        let best_ask = best_ask_lvl.price;

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

        // Combine inventory skew + imbalance skew. Both are price shifts in
        // tick-aligned increments.
        let inv_skew = inventory_skew_price(
            position.size.0,
            self.config.max_skew_ticks,
            self.config.skew_unit.0,
            self.config.tick_size,
        );
        let imb_skew = self.compute_imbalance_skew(best_bid_lvl.size, best_ask_lvl.size);
        let skew = inv_skew + imb_skew;
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

    /// Imbalance shift in price units (signed). Positive imbalance (bid-
    /// heavy) returns positive (shift quotes UP). Computes top-of-book
    /// size imbalance `(B - A) / (B + A)` in `[-1, +1]`, scales by
    /// `max_imbalance_ticks * tick_size`, then floors to integer ticks.
    fn compute_imbalance_skew(&self, bid_size: Size, ask_size: Size) -> Decimal {
        if self.config.max_imbalance_ticks == 0 {
            return Decimal::ZERO;
        }
        let b = bid_size.0;
        let a = ask_size.0;
        let total = b + a;
        if total <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        // imbalance in [-1, +1]
        let imbalance = (b - a) / total;
        let max_ticks = Decimal::from(self.config.max_imbalance_ticks);
        // Sign-preserving floor toward zero so the integer-tick shift
        // stays under the configured cap on both sides.
        let raw_ticks = imbalance * max_ticks;
        let ticks_shifted = if raw_ticks >= Decimal::ZERO {
            raw_ticks.floor()
        } else {
            -((-raw_ticks).floor())
        };
        ticks_shifted * self.config.tick_size
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
                if should_requote_on_tick_drift(
                    self.last_bid,
                    self.last_ask,
                    self.last_requote_ts,
                    bid,
                    ask,
                    *ts,
                    self.config.min_requote_interval_ms,
                    self.config.tick_size,
                ) {
                    self.last_bid = Some(bid);
                    self.last_ask = Some(ask);
                    self.last_requote_ts = Some(*ts);
                    post_only_pair(ctx.symbol, bid, ask, self.config.size_per_quote)
                } else {
                    vec![Action::NoOp]
                }
            }
            MarketEvent::BookUpdate { snapshot } => {
                let Some((bid, ask)) = self.compute_targets(snapshot, ctx.position) else {
                    return Vec::new();
                };
                if should_requote_on_tick_drift(
                    self.last_bid,
                    self.last_ask,
                    self.last_requote_ts,
                    bid,
                    ask,
                    snapshot.ts,
                    self.config.min_requote_interval_ms,
                    self.config.tick_size,
                ) {
                    self.last_bid = Some(bid);
                    self.last_ask = Some(ask);
                    self.last_requote_ts = Some(snapshot.ts);
                    post_only_pair(ctx.symbol, bid, ask, self.config.size_per_quote)
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
    use tikr_core::{Asset, Level, MarketKind, Position, SignedSize, Symbol, VenueId};

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
            max_imbalance_ticks: 0,
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
            max_imbalance_ticks: 0,
        }
    }

    fn cfg_imbalance(improve_gt: u32, max_imbalance_ticks: u32) -> TopOfBookConfig {
        TopOfBookConfig {
            size_per_quote: Size(Decimal::new(1, 3)),
            tick_size: Decimal::from(1),
            improve_when_spread_gt_ticks: improve_gt,
            min_requote_interval_ms: 1000,
            max_skew_ticks: 0,
            skew_unit: Size(Decimal::new(1, 0)),
            max_imbalance_ticks,
        }
    }

    fn book_sized(
        s: &Symbol,
        bid: i64,
        bid_size: i64,
        ask: i64,
        ask_size: i64,
        ts: u64,
    ) -> Snapshot {
        Snapshot {
            symbol: s.clone(),
            bids: vec![Level {
                price: Price(Decimal::new(bid, 0)),
                size: Size(Decimal::new(bid_size, 0)),
            }],
            asks: vec![Level {
                price: Price(Decimal::new(ask, 0)),
                size: Size(Decimal::new(ask_size, 0)),
            }],
            ts: Timestamp(ts),
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

    /// Regression: fractional position/unit ratios must produce tick-aligned
    /// skewed prices, not e.g. 76789.4666... that Binance silently truncates.
    #[test]
    fn skew_prices_stay_tick_aligned() {
        let s = sym();
        // position = 2, unit = 3 → ratio = 0.666..., max_ticks = 20.
        // ratio * max_ticks = 13.333... → floor = 13 ticks shift.
        let p = pos_with(&s, Decimal::from(2));
        let b = book(&s, 100, 200, 0); // spread = 100 ticks (improve mode)
        let ctx = StrategyContext {
            symbol: &s,
            now: Timestamp(0),
            position: &p,
            recent_fills: &[],
            latest_book: &b,
            open_quotes: &[],
        };
        // tick = 1, base improve: bid=101, ask=199. Skew = -13.
        let mut tob = TopOfBook::new(cfg_skew(1, 20, Decimal::from(3)));
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
        assert_eq!(bid.price.0, Decimal::from(88), "bid must be tick-aligned");
        assert_eq!(ask.price.0, Decimal::from(186), "ask must be tick-aligned");
    }

    /// Imbalance: bid-heavy book shifts both quotes UP by integer ticks.
    /// imbalance = (9 - 1) / 10 = 0.8 × max_imbalance_ticks 10 = 8 ticks.
    #[test]
    fn bid_heavy_imbalance_shifts_both_quotes_up() {
        let s = sym();
        let p = pos(&s);
        // bid_size=9, ask_size=1, spread=10 ticks (improve mode kicks in).
        let b = book_sized(&s, 100, 9, 110, 1, 0);
        let ctx = StrategyContext {
            symbol: &s,
            now: Timestamp(0),
            position: &p,
            recent_fills: &[],
            latest_book: &b,
            open_quotes: &[],
        };
        let mut tob = TopOfBook::new(cfg_imbalance(1, 10));
        let actions = tob.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: b.clone(),
            },
        );
        // Base improve: bid=101, ask=109. Shift +8: bid=109, ask=117.
        // ask=117 above best_ask 110 is fine (post-only doesn't cross).
        // bid=109 below best_ask 110 is fine (post-only-safe).
        let Action::Quote(bid) = &actions[1] else {
            panic!("bid")
        };
        let Action::Quote(ask) = &actions[2] else {
            panic!("ask")
        };
        assert_eq!(bid.price.0, Decimal::from(109));
        assert_eq!(ask.price.0, Decimal::from(117));
    }

    /// Imbalance: ask-heavy book shifts both quotes DOWN.
    /// imbalance = (1 - 9) / 10 = -0.8 × 10 = -8 ticks.
    #[test]
    fn ask_heavy_imbalance_shifts_both_quotes_down() {
        let s = sym();
        let p = pos(&s);
        let b = book_sized(&s, 100, 1, 110, 9, 0);
        let ctx = StrategyContext {
            symbol: &s,
            now: Timestamp(0),
            position: &p,
            recent_fills: &[],
            latest_book: &b,
            open_quotes: &[],
        };
        let mut tob = TopOfBook::new(cfg_imbalance(1, 10));
        let actions = tob.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: b.clone(),
            },
        );
        // Base improve: bid=101, ask=109. Shift -8: bid=93, ask=101.
        // bid=93 below best_bid 100 is fine. ask=101 above best_bid 100 fine.
        let Action::Quote(bid) = &actions[1] else {
            panic!("bid")
        };
        let Action::Quote(ask) = &actions[2] else {
            panic!("ask")
        };
        assert_eq!(bid.price.0, Decimal::from(93));
        assert_eq!(ask.price.0, Decimal::from(101));
    }

    /// Balanced book → zero imbalance shift.
    #[test]
    fn balanced_book_zero_imbalance() {
        let s = sym();
        let p = pos(&s);
        let b = book_sized(&s, 100, 5, 110, 5, 0);
        let ctx = StrategyContext {
            symbol: &s,
            now: Timestamp(0),
            position: &p,
            recent_fills: &[],
            latest_book: &b,
            open_quotes: &[],
        };
        let mut tob = TopOfBook::new(cfg_imbalance(1, 10));
        let actions = tob.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: b.clone(),
            },
        );
        // No imbalance: pure improve. bid=101, ask=109.
        let Action::Quote(bid) = &actions[1] else {
            panic!("bid")
        };
        let Action::Quote(ask) = &actions[2] else {
            panic!("ask")
        };
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
