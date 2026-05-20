//! Micro-price market-making strategy.
//!
//! Quotes symmetrically around the **size-weighted mid** ("micro-price")
//! instead of the raw mid. When the book is bid-heavy the micro-price is
//! pulled toward the ask, so both our quotes lean up — anticipating the
//! short-term drift the imbalance predicts.
//!
//! Micro-price formula:
//!
//! ```text
//! microprice = (bid_size · ask_price + ask_size · bid_price)
//!              / (bid_size + ask_size)
//! ```
//!
//! Targets:
//!
//! ```text
//! target_bid = floor((microprice − half_spread_ticks · tick) / tick) · tick
//! target_ask = ceil((microprice + half_spread_ticks · tick) / tick) · tick
//! ```
//!
//! Post-only safety: targets are clamped so the bid never reaches the live
//! best ask and the ask never reaches the live best bid (a 1-tick gap is
//! preserved in both cases).
//!
//! Optional inventory skew mirrors [`crate::TopOfBook`] — long → both shift
//! DOWN, short → both shift UP — to mean-revert position.

use tikr_core::{
    Decimal, MarketEvent, Position, Price, QuoteKind, Side, Size, Snapshot, Symbol, TimeInForce,
    Timestamp,
};
use tikr_venue::QuoteIntent;

use crate::{Action, Strategy, StrategyContext};

/// Configuration for [`MicroPrice`].
#[derive(Debug, Clone)]
pub struct MicroPriceConfig {
    /// Order size placed on each side.
    pub size_per_quote: Size,
    /// Venue tick size (price increment).
    pub tick_size: Decimal,
    /// Half-spread in ticks. Bid quoted `half_spread_ticks` ticks below the
    /// micro-price, ask quoted `half_spread_ticks` ticks above.
    pub half_spread_ticks: u32,
    /// Minimum time between forced requotes (ms).
    pub min_requote_interval_ms: u64,
    /// Maximum inventory-skew shift in ticks (each side). `0` = no skew.
    pub max_skew_ticks: u32,
    /// Position size at which skew is fully applied. Must be `> 0` if
    /// `max_skew_ticks > 0`.
    pub skew_unit: Size,
}

/// Micro-price strategy state.
pub struct MicroPrice {
    config: MicroPriceConfig,
    last_bid: Option<Price>,
    last_ask: Option<Price>,
    last_requote_ts: Option<Timestamp>,
}

impl MicroPrice {
    fn compute_targets(&self, snapshot: &Snapshot, position: &Position) -> Option<(Price, Price)> {
        let best_bid_lvl = snapshot.bids.first()?;
        let best_ask_lvl = snapshot.asks.first()?;
        let best_bid = best_bid_lvl.price.0;
        let best_ask = best_ask_lvl.price.0;
        let bid_size = best_bid_lvl.size.0;
        let ask_size = best_ask_lvl.size.0;

        let tick = self.config.tick_size;
        if tick <= Decimal::ZERO {
            return None;
        }
        let total = bid_size + ask_size;
        if total <= Decimal::ZERO {
            return None;
        }

        // Weighted-mid: side with more depth pulls price toward the OPPOSITE
        // touch (large bid weight ⇒ micro_price closer to ask, signaling buy
        // pressure ⇒ both quotes lean up).
        let micro = (bid_size * best_ask + ask_size * best_bid) / total;
        let half = Decimal::from(self.config.half_spread_ticks) * tick;

        // Snap to tick grid: bid floors away from micro, ask ceils away.
        let raw_bid = micro - half;
        let raw_ask = micro + half;
        let mut bid = Price((raw_bid / tick).floor() * tick);
        let mut ask = Price((raw_ask / tick).ceil() * tick);

        // Inventory skew (mirrors TopOfBook). Long → shift both DOWN.
        let skew_ticks = self.compute_inventory_skew(position);
        if !skew_ticks.is_zero() {
            bid = Price(bid.0 + skew_ticks);
            ask = Price(ask.0 + skew_ticks);
        }

        // Post-only safety clamp: keep a 1-tick gap from the live touch.
        if bid.0 >= best_ask {
            bid = Price(best_ask - tick);
        }
        if ask.0 <= best_bid {
            ask = Price(best_bid + tick);
        }
        // Also ensure bid < ask after clamps (degenerate spreads).
        if bid.0 >= ask.0 {
            bid = Price(ask.0 - tick);
        }
        Some((bid, ask))
    }

    fn compute_inventory_skew(&self, position: &Position) -> Decimal {
        if self.config.max_skew_ticks == 0 {
            return Decimal::ZERO;
        }
        let unit = self.config.skew_unit.0;
        if unit <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        let pos = position.size.0;
        let max = Decimal::from(self.config.max_skew_ticks);
        let scaled = (pos.abs() / unit).min(Decimal::from(1));
        let ticks = scaled * max;
        let signed_ticks = if pos > Decimal::ZERO { -ticks } else { ticks };
        // Convert ticks to price units.
        signed_ticks * self.config.tick_size
    }

    fn should_requote(&self, new_bid: Price, new_ask: Price, ts: Timestamp) -> bool {
        let Some(last_bid) = self.last_bid else {
            return true;
        };
        let Some(last_ask) = self.last_ask else {
            return true;
        };

        // Time-forced requote.
        if let Some(last_ts) = self.last_requote_ts {
            let elapsed_ns = ts.0.saturating_sub(last_ts.0);
            let min_ns = self
                .config
                .min_requote_interval_ms
                .saturating_mul(1_000_000);
            if elapsed_ns >= min_ns {
                return true;
            }
        }

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

impl Strategy for MicroPrice {
    type Config = MicroPriceConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            last_bid: None,
            last_ask: None,
            last_requote_ts: None,
        }
    }

    fn name(&self) -> &str {
        "micro-price"
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
    use tikr_core::{Asset, Level, MarketKind, Notional, Position, SignedSize, VenueId};

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
            realized_pnl: Notional(Decimal::ZERO),
        }
    }

    fn book_sized(s: &Symbol, bid: i64, bid_sz: i64, ask: i64, ask_sz: i64, ts: u64) -> Snapshot {
        Snapshot {
            symbol: s.clone(),
            bids: vec![Level {
                price: Price(Decimal::new(bid, 0)),
                size: Size(Decimal::new(bid_sz, 0)),
            }],
            asks: vec![Level {
                price: Price(Decimal::new(ask, 0)),
                size: Size(Decimal::new(ask_sz, 0)),
            }],
            ts: Timestamp(ts),
        }
    }

    fn cfg(half: u32, max_skew: u32) -> MicroPriceConfig {
        MicroPriceConfig {
            size_per_quote: Size(Decimal::new(1, 0)),
            tick_size: Decimal::new(1, 1), // 0.1
            half_spread_ticks: half,
            min_requote_interval_ms: 1000,
            max_skew_ticks: max_skew,
            skew_unit: Size(Decimal::new(1, 0)),
        }
    }

    #[test]
    fn balanced_book_quotes_at_mid_plus_or_minus_half() {
        let s = sym();
        let snap = book_sized(&s, 100, 1, 101, 1, 1000);
        let strat = MicroPrice::new(cfg(2, 0));
        let (bid, ask) = strat.compute_targets(&snap, &pos(&s)).unwrap();
        // Balanced 1:1 → micro = 100.5. half = 2*0.1 = 0.2.
        // raw_bid = 100.3 → floor to tick = 100.3. raw_ask = 100.7 → ceil to tick = 100.7.
        assert_eq!(bid.0, Decimal::new(1003, 1));
        assert_eq!(ask.0, Decimal::new(1007, 1));
    }

    #[test]
    fn bid_heavy_book_skews_quotes_up() {
        let s = sym();
        // Bid size 9, ask size 1. micro = (9·101 + 1·100) / 10 = 909+100/10 = 100.9.
        let snap = book_sized(&s, 100, 9, 101, 1, 1000);
        let strat = MicroPrice::new(cfg(2, 0));
        let (bid, ask) = strat.compute_targets(&snap, &pos(&s)).unwrap();
        // raw_bid = 100.7 → floor at tick 0.1 → 100.7. ask = 101.1.
        assert_eq!(bid.0, Decimal::new(1007, 1));
        assert_eq!(ask.0, Decimal::new(1011, 1));
    }

    #[test]
    fn ask_heavy_book_skews_quotes_down() {
        let s = sym();
        // Bid size 1, ask size 9. micro = (1·101 + 9·100) / 10 = 100.1.
        let snap = book_sized(&s, 100, 1, 101, 9, 1000);
        let strat = MicroPrice::new(cfg(2, 0));
        let (bid, ask) = strat.compute_targets(&snap, &pos(&s)).unwrap();
        // raw_bid = 99.9, raw_ask = 100.3.
        assert_eq!(bid.0, Decimal::new(999, 1));
        assert_eq!(ask.0, Decimal::new(1003, 1));
    }

    #[test]
    fn post_only_clamp_prevents_cross() {
        let s = sym();
        // Tight 1-tick spread + heavy bid pressure could push our bid >= best_ask.
        let snap = book_sized(&s, 100, 100, 101, 1, 1000);
        // micro = (100·101 + 1·100)/101 ≈ 100.99. half = 0 ticks (aggressive).
        let strat = MicroPrice::new(cfg(0, 0));
        let (bid, ask) = strat.compute_targets(&snap, &pos(&s)).unwrap();
        assert!(bid.0 < ask.0, "bid {} must be < ask {}", bid.0, ask.0);
        // Bid must stay below best_ask, ask above best_bid.
        assert!(bid.0 < Decimal::from(101));
        assert!(ask.0 > Decimal::from(100));
    }

    #[test]
    fn long_inventory_skews_quotes_down() {
        let s = sym();
        let snap = book_sized(&s, 100, 1, 101, 1, 1000);
        let strat = MicroPrice::new(cfg(2, 5));
        let mut p = pos(&s);
        p.size = SignedSize(Decimal::from(1)); // long 1 unit = full skew
        let (bid_long, ask_long) = strat.compute_targets(&snap, &p).unwrap();
        let (bid_flat, ask_flat) = strat.compute_targets(&snap, &pos(&s)).unwrap();
        assert!(
            bid_long.0 < bid_flat.0,
            "long bid {} should be below flat bid {}",
            bid_long.0,
            bid_flat.0
        );
        assert!(
            ask_long.0 < ask_flat.0,
            "long ask {} should be below flat ask {}",
            ask_long.0,
            ask_flat.0
        );
    }

    #[test]
    fn empty_book_returns_none() {
        let s = sym();
        let snap = Snapshot {
            symbol: s.clone(),
            bids: vec![],
            asks: vec![],
            ts: Timestamp(0),
        };
        let strat = MicroPrice::new(cfg(2, 0));
        assert!(strat.compute_targets(&snap, &pos(&s)).is_none());
    }
}
