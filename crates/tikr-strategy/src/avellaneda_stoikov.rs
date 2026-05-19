//! Avellaneda-Stoikov (2008) inventory-aware market-making strategy.
//!
//! # Formula (finite-horizon)
//!
//! Reservation price: `r = mid - q·γ·σ²·(T-t)`
//! Half-spread: `δ = mid · base_spread_bps / 10_000`
//!
//! Bid quote = `r - δ`, ask quote = `r + δ`. `mid` = book mid, `q` = signed
//! inventory (long > 0), `γ` = risk aversion, `σ²` = log-return variance from
//! [`crate::volatility::EwmaVolatility`], `T-t` = pinned horizon in seconds.
//!
//! # Half-spread
//!
//! The original academic formula `δ* = γ·σ²·(T-t)/2 + (1/γ)·ln(1 + γ/k)` is
//! in PRICE units, not relative. At crypto price levels (BTC ~$76k, ETH ~$2.1k)
//! the same γ/k parameters produce radically different spread widths in bps. The
//! formula was designed for academic mid ≈ $1 settings.
//!
//! The `base_spread_bps` config replaces the price-unit formula with a portable
//! `mid · bps / 10_000`, giving consistent bps width across assets. γ is RETAINED
//! for inventory-skew (reservation price) — that is the actual inventory-aware
//! magic of A-S vs NaiveGrid.
//!
//! # Sign convention
//!
//! Long inventory pushes the reservation price DOWN (encourages selling, makes
//! the ask more attractive). Short inventory pushes it UP.
//!
//! # Parameter notes
//!
//! `γ` (gamma) controls inventory mean-reversion strength via the reservation
//! price formula. `base_spread_bps` controls the raw spread width independently.
//!
//! # References
//!
//! Avellaneda, M. & Stoikov, S. (2008). *High-frequency trading in a limit
//! order book.* Quantitative Finance. <https://www.math.nyu.edu/~avellane/HighFrequencyTrading.pdf>

use crate::volatility::{EwmaConfig, EwmaVolatility, WARMUP_COUNT};
use crate::{Action, Strategy, StrategyContext};
use tikr_core::{
    Decimal, MarketEvent, Price, QuoteKind, Side, Size, Symbol, TimeInForce, Timestamp,
};
use tikr_venue::QuoteIntent;

/// Configuration for [`AvellanedaStoikov`].
#[derive(Clone, Debug)]
pub struct AvellanedaStoikovConfig {
    /// Risk aversion γ. Default 0.1. Higher → stronger inventory mean-reversion.
    pub gamma: Decimal,
    /// Half-spread in basis points (e.g. 5 = 5 bps per side, 10 bps round-trip).
    /// Converted to price units via `mid * base_spread_bps / 10_000` at quote time.
    pub base_spread_bps: u32,
    /// Pinned horizon T-t in seconds. Default 3600 (1 hour). NOT a wall-clock
    /// countdown — controls how aggressively the strategy pushes inventory to zero.
    pub horizon_sec: u64,
    /// Size placed at each quote level (both bid and ask).
    pub size_per_quote: Size,
    /// Minimum time between full requotes, in milliseconds.
    pub min_requote_interval_ms: u64,
    /// Mid-drift threshold for early requote: `|new_mid - last_mid| / last_mid > (level_step_bps/2) / 10_000`.
    pub level_step_bps: u32,
    /// Volatility estimator configuration.
    pub volatility: EwmaConfig,
}

/// Avellaneda-Stoikov (2008) finite-horizon strategy.
pub struct AvellanedaStoikov {
    config: AvellanedaStoikovConfig,
    estimator: EwmaVolatility,
    last_requote_ts: Option<Timestamp>,
    last_quoted_mid: Option<Price>,
}

impl AvellanedaStoikov {
    /// Returns the current EWMA variance (for diagnostics/tests).
    pub fn current_var(&self) -> Decimal {
        self.estimator.current_var()
    }

    /// Returns the count of computed-return samples seen so far.
    pub fn samples_seen(&self) -> u32 {
        self.estimator.samples_seen()
    }
}

impl Strategy for AvellanedaStoikov {
    type Config = AvellanedaStoikovConfig;

    fn new(config: Self::Config) -> Self {
        let estimator = EwmaVolatility::new(config.volatility.clone());
        Self {
            config,
            estimator,
            last_requote_ts: None,
            last_quoted_mid: None,
        }
    }

    fn name(&self) -> &str {
        "avellaneda-stoikov"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        let MarketEvent::BookUpdate { snapshot } = event else {
            return Vec::new(); // A-S ignores trades/fills/heartbeats
        };
        let Some(mid) = compute_mid(snapshot) else {
            return Vec::new(); // empty book side — nothing actionable
        };
        self.estimator.on_book_update(mid, snapshot.ts);
        if self.estimator.samples_seen() < WARMUP_COUNT {
            return Vec::new();
        }
        if !should_requote(
            self.last_requote_ts,
            self.last_quoted_mid,
            mid,
            snapshot.ts,
            self.config.min_requote_interval_ms,
            self.config.level_step_bps,
        ) {
            return Vec::new();
        }
        let q = ctx.position.size.0; // signed
        let var = self.estimator.current_var();
        let gamma = self.config.gamma;
        let horizon = Decimal::from(self.config.horizon_sec);
        // Reservation price: r = mid - q · γ · σ² · (T-t)
        let r = mid.0 - q * gamma * var * horizon;
        // Half-spread: δ = mid · base_spread_bps / 10_000
        let delta = mid.0 * Decimal::from(self.config.base_spread_bps) / Decimal::from(10_000);
        self.last_requote_ts = Some(snapshot.ts);
        self.last_quoted_mid = Some(mid);
        vec![
            Action::CancelAll,
            Action::Quote(make_intent(
                ctx.symbol,
                Side::Bid,
                Price(r - delta),
                self.config.size_per_quote,
            )),
            Action::Quote(make_intent(
                ctx.symbol,
                Side::Ask,
                Price(r + delta),
                self.config.size_per_quote,
            )),
        ]
    }
}

fn compute_mid(snapshot: &tikr_core::Snapshot) -> Option<Price> {
    let best_bid = snapshot.bids.first()?.price;
    let best_ask = snapshot.asks.first()?.price;
    Some(Price((best_bid.0 + best_ask.0) / Decimal::from(2)))
}

fn should_requote(
    last_ts: Option<Timestamp>,
    last_mid: Option<Price>,
    new_mid: Price,
    now: Timestamp,
    min_interval_ms: u64,
    level_step_bps: u32,
) -> bool {
    let (Some(prev_ts), Some(prev_mid)) = (last_ts, last_mid) else {
        return true;
    };
    let elapsed_ns = now.0.saturating_sub(prev_ts.0);
    let interval_ns = min_interval_ms.saturating_mul(1_000_000);
    if elapsed_ns >= interval_ns {
        return true;
    }
    let drift = (new_mid.0 - prev_mid.0).abs();
    let threshold =
        prev_mid.0 * (Decimal::from(level_step_bps) / Decimal::from(2)) / Decimal::from(10_000);
    drift > threshold
}

fn make_intent(symbol: &Symbol, side: Side, price: Price, size: Size) -> QuoteIntent {
    QuoteIntent {
        symbol: symbol.clone(),
        side,
        price,
        size,
        tif: TimeInForce::PostOnly,
        kind: QuoteKind::Point,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::{
        Asset, Decimal, Level, MarketKind, Notional, Position, SignedSize, Snapshot, VenueId,
    };

    fn make_symbol() -> Symbol {
        Symbol {
            base: Asset::new("BTC"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("test"),
            kind: MarketKind::Spot,
        }
    }

    fn make_position(symbol: &Symbol, size: Decimal) -> Position {
        Position {
            symbol: symbol.clone(),
            size: SignedSize(size),
            avg_entry: Price(Decimal::ZERO),
            realized_pnl: Notional(Decimal::ZERO),
        }
    }

    fn make_book(symbol: &Symbol, bid: Decimal, ask: Decimal, ts: u64) -> Snapshot {
        Snapshot {
            symbol: symbol.clone(),
            bids: vec![Level {
                price: Price(bid),
                size: Size(Decimal::from(1)),
            }],
            asks: vec![Level {
                price: Price(ask),
                size: Size(Decimal::from(1)),
            }],
            ts: Timestamp(ts),
        }
    }

    fn make_ctx<'a>(
        symbol: &'a Symbol,
        position: &'a Position,
        book: &'a Snapshot,
    ) -> StrategyContext<'a> {
        StrategyContext {
            symbol,
            now: book.ts,
            position,
            recent_fills: &[],
            latest_book: book,
            open_quotes: &[],
        }
    }

    fn default_config() -> AvellanedaStoikovConfig {
        AvellanedaStoikovConfig {
            gamma: Decimal::try_from(0.1).unwrap(),
            base_spread_bps: 5,
            horizon_sec: 3600,
            size_per_quote: Size(Decimal::from(1)),
            min_requote_interval_ms: 1000,
            level_step_bps: 10,
            volatility: EwmaConfig {
                half_life_sec: 60.0,
                initial_var: Decimal::try_from(0.0001).unwrap(),
            },
        }
    }

    /// Run N BookUpdates against the strategy to walk it through warmup.
    /// Returns the actions emitted on the (N+1)-th call (post-warmup), with
    /// position + a fresh book at the given mid.
    fn warmup_and_emit(
        strategy: &mut AvellanedaStoikov,
        symbol: &Symbol,
        position: &Position,
        warmup_calls: u32,
        final_bid: Decimal,
        final_ask: Decimal,
        base_ts_ns: u64,
    ) -> Vec<Action> {
        for i in 0..warmup_calls {
            let bid = Decimal::from(100) + Decimal::from(i as i64) / Decimal::from(1000);
            let ask = bid + Decimal::ONE;
            let book = make_book(symbol, bid, ask, base_ts_ns + (i as u64) * 1_000_000_000);
            let ctx = make_ctx(symbol, position, &book);
            let _ = strategy.on_event(
                &ctx,
                &MarketEvent::BookUpdate {
                    snapshot: book.clone(),
                },
            );
        }
        let final_book = make_book(
            symbol,
            final_bid,
            final_ask,
            base_ts_ns + warmup_calls as u64 * 1_000_000_000,
        );
        let ctx = make_ctx(symbol, position, &final_book);
        strategy.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: final_book.clone(),
            },
        )
    }

    #[test]
    fn warmup_returns_empty() {
        let sym = make_symbol();
        let pos = make_position(&sym, Decimal::ZERO);
        let mut strat = AvellanedaStoikov::new(default_config());

        // First 30 calls: samples_seen ranges 0..=29 after the call. All empty.
        for i in 0..WARMUP_COUNT {
            let bid = Decimal::from(100) + Decimal::from(i as i64) / Decimal::from(1000);
            let ask = bid + Decimal::ONE;
            let book = make_book(&sym, bid, ask, (i as u64) * 1_000_000_000);
            let ctx = make_ctx(&sym, &pos, &book);
            let actions = strat.on_event(
                &ctx,
                &MarketEvent::BookUpdate {
                    snapshot: book.clone(),
                },
            );
            assert!(
                actions.is_empty(),
                "call {i} should return empty during warmup, got {actions:?}"
            );
        }
        // 31st call: samples_seen becomes WARMUP_COUNT → quotes fire.
        let book = make_book(
            &sym,
            Decimal::from(100),
            Decimal::from(101),
            (WARMUP_COUNT as u64) * 1_000_000_000,
        );
        let ctx = make_ctx(&sym, &pos, &book);
        let actions = strat.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: book.clone(),
            },
        );
        assert!(!actions.is_empty());
    }

    #[test]
    fn flat_inventory_symmetric_quotes() {
        let sym = make_symbol();
        let pos = make_position(&sym, Decimal::ZERO);
        let mut strat = AvellanedaStoikov::new(default_config());
        let actions = warmup_and_emit(
            &mut strat,
            &sym,
            &pos,
            WARMUP_COUNT,
            Decimal::from(100),
            Decimal::from(101),
            0,
        );
        assert_eq!(actions.len(), 3);
        assert!(matches!(actions[0], Action::CancelAll));
        let mid = (Decimal::from(100) + Decimal::from(101)) / Decimal::from(2);
        let bid_price = match &actions[1] {
            Action::Quote(q) => q.price.0,
            _ => panic!("expected Quote in actions[1]"),
        };
        let ask_price = match &actions[2] {
            Action::Quote(q) => q.price.0,
            _ => panic!("expected Quote in actions[2]"),
        };
        assert_eq!((mid - bid_price).abs(), (ask_price - mid).abs());
    }

    #[test]
    fn post_warmup_quotes_at_reservation_skew() {
        let sym = make_symbol();
        let pos = make_position(&sym, Decimal::from(5));
        let mut strat = AvellanedaStoikov::new(default_config());
        let actions = warmup_and_emit(
            &mut strat,
            &sym,
            &pos,
            WARMUP_COUNT,
            Decimal::from(100),
            Decimal::from(101),
            0,
        );
        assert_eq!(actions.len(), 3);
        let mid = (Decimal::from(100) + Decimal::from(101)) / Decimal::from(2);
        let bid_price = match &actions[1] {
            Action::Quote(q) => q.price.0,
            _ => panic!("expected Quote in actions[1]"),
        };
        let ask_price = match &actions[2] {
            Action::Quote(q) => q.price.0,
            _ => panic!("expected Quote in actions[2]"),
        };
        let bid_offset = mid - bid_price;
        let ask_offset = ask_price - mid;
        assert_ne!(bid_offset, ask_offset);
        // Long inventory → r < mid → bid offset is larger than ask offset.
        assert!(
            bid_offset > ask_offset,
            "expected bid_offset > ask_offset (long pos skews quotes), got bid={bid_offset} ask={ask_offset}"
        );
    }

    #[test]
    fn requote_gated_by_interval() {
        let sym = make_symbol();
        let pos = make_position(&sym, Decimal::ZERO);
        let mut cfg = default_config();
        cfg.min_requote_interval_ms = 10_000;
        let mut strat = AvellanedaStoikov::new(cfg);

        let warmup_end_ts = WARMUP_COUNT as u64 * 1_000_000_000;
        let actions = warmup_and_emit(
            &mut strat,
            &sym,
            &pos,
            WARMUP_COUNT,
            Decimal::from(100),
            Decimal::from(101),
            0,
        );
        assert!(!actions.is_empty(), "first post-warmup call should quote");

        // 1ns later — well within 10s interval.
        let book2 = make_book(
            &sym,
            Decimal::from(100),
            Decimal::from(101),
            warmup_end_ts + 1,
        );
        let ctx2 = make_ctx(&sym, &pos, &book2);
        let actions2 = strat.on_event(
            &ctx2,
            &MarketEvent::BookUpdate {
                snapshot: book2.clone(),
            },
        );
        assert!(
            actions2.is_empty(),
            "within-interval requote should be empty, got {actions2:?}"
        );

        // 11s later — past interval.
        let book3 = make_book(
            &sym,
            Decimal::from(100),
            Decimal::from(101),
            warmup_end_ts + 11_000_000_000,
        );
        let ctx3 = make_ctx(&sym, &pos, &book3);
        let actions3 = strat.on_event(
            &ctx3,
            &MarketEvent::BookUpdate {
                snapshot: book3.clone(),
            },
        );
        assert!(
            !actions3.is_empty(),
            "post-interval requote should fire, got empty"
        );
    }

    #[test]
    fn trade_event_returns_empty() {
        let sym = make_symbol();
        let pos = make_position(&sym, Decimal::ZERO);
        let mut strat = AvellanedaStoikov::new(default_config());
        let _ = warmup_and_emit(
            &mut strat,
            &sym,
            &pos,
            WARMUP_COUNT,
            Decimal::from(100),
            Decimal::from(101),
            0,
        );
        let book = make_book(
            &sym,
            Decimal::from(100),
            Decimal::from(101),
            100_000_000_000,
        );
        let ctx = make_ctx(&sym, &pos, &book);
        let trade = MarketEvent::Trade {
            symbol: sym.clone(),
            price: Price(Decimal::from(100)),
            size: Size(Decimal::from(1)),
            side: Side::Bid,
            ts: Timestamp(100_000_000_000),
        };
        let actions = strat.on_event(&ctx, &trade);
        assert!(actions.is_empty());
    }

    #[test]
    fn half_spread_is_base_spread_bps_of_mid() {
        let sym = make_symbol();
        let pos = make_position(&sym, Decimal::ZERO);
        let mut cfg = default_config();
        cfg.base_spread_bps = 5;
        let mut strat = AvellanedaStoikov::new(cfg);

        // Use mid = 100.5 (bid=100, ask=101).
        let final_bid = Decimal::from(100);
        let final_ask = Decimal::from(101);
        let mid = (final_bid + final_ask) / Decimal::from(2); // 100.5

        let actions = warmup_and_emit(
            &mut strat,
            &sym,
            &pos,
            WARMUP_COUNT,
            final_bid,
            final_ask,
            0,
        );
        assert_eq!(actions.len(), 3);
        let bid_price = match &actions[1] {
            Action::Quote(q) => q.price.0,
            _ => panic!("expected Quote"),
        };
        let ask_price = match &actions[2] {
            Action::Quote(q) => q.price.0,
            _ => panic!("expected Quote"),
        };
        // With flat inventory (q=0): r = mid, so bid = mid - delta, ask = mid + delta.
        // delta = mid * 5 / 10_000 = 100.5 * 0.0005 = 0.05025
        let expected_delta = mid * Decimal::try_from(0.0005).unwrap();
        let actual_delta = (ask_price - mid).abs();
        let diff = (actual_delta - expected_delta).abs();
        assert!(
            diff < Decimal::try_from(0.0001).unwrap(),
            "expected half-spread ≈ {expected_delta}, got {actual_delta} (diff={diff})"
        );
        // Bid and ask should be symmetric around mid (flat inventory).
        assert_eq!((mid - bid_price).abs(), (ask_price - mid).abs());
    }
}
