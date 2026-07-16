//! GLFT (Guéant-Lehalle-Fernandez-Tapia) inventory-aware market-making strategy.
//!
//! Companion to [`crate::avellaneda_stoikov`] — same Strategy trait, same
//! `[CancelAll, Quote(Bid), Quote(Ask)]` output shape, same warmup gate. The
//! structural difference from A-S is the horizon: GLFT is infinite-horizon (no
//! `T-t` countdown that operators must manage). See [issue #18] for rationale.
//!
//! # Formula
//!
//! Reservation price: `r = mid · (1 - q·γ·σ²)`
//! Half-spread: `δ = mid · base_spread_bps / 10_000`
//! Bid = `r - δ`, Ask = `r + δ`.
//!
//! `σ²` is log-return variance (dimensionless, price-scale-independent) —
//! the skew term is multiplied by `mid` so it scales with the asset's price
//! level. `q·γ·σ²` alone is a dimensionless fraction of price; without the
//! `mid` factor the skew would be in raw base-unit magnitude and vanish in
//! relative terms for a high-priced asset (BTC ~$76k).
//!
//! # Academic note — simplified reservation price
//!
//! The original Guéant (2017) closed form for the reservation price is
//! `r = mid - q·γ·σ²·η` where `η = 2/(γ·k·A·exp(-1))` is a bounded constant
//! derived from order-arrival intensity parameters (`k`, `A`). Practically, `η`
//! is a scaling constant that was only calibratable via historical fill data
//! (Phase 3). With the removal of `k` and `a` from the config (those parameters
//! were only used in the now-replaced price-unit spread formula), the full `η`
//! expression has no home. The reservation price is simplified to
//! `r = mid · (1 - q·γ·σ²)` (unit-scale η = 1), making GLFT structurally
//! identical to A-S's inventory-skew term minus the finite-horizon multiplier. This
//! sacrifices academic purity for practical portability: the strategy still
//! delivers the core GLFT value proposition (infinite-horizon, no expiry reset)
//! while avoiding a calibration dependency that was dead weight pre-Phase 3.
//!
//! # Half-spread
//!
//! The original `(1/γ)·ln(1 + γ/k)` formula is in PRICE units, not relative.
//! At crypto price levels (BTC ~$76k, ETH ~$2.1k) the same γ/k parameters
//! produce radically different spread widths in bps. The `base_spread_bps`
//! config replaces the price-unit formula with a portable
//! `mid · bps / 10_000`, giving consistent bps width across assets.
//!
//! # Sign convention
//!
//! Same as A-S: long inventory (`q > 0`) pushes the reservation price DOWN
//! to encourage selling.
//!
//! # References
//!
//! - Guéant, O. (2017). *The Financial Mathematics of Market Liquidity*.
//!   Chapman & Hall / CRC. Simplified closed-form chapter.
//! - Guéant, O., Lehalle, C.-A., Fernandez-Tapia, J. (2013). *Dealing with
//!   the Inventory Risk: A Solution to the Market Making Problem.* arxiv 1206.4810.
//!
//! [issue #18]: https://github.com/kryptic-sh/tikr/issues/18

use crate::volatility::{EwmaConfig, EwmaVolatility, WARMUP_COUNT};
use crate::{
    Action, Strategy, StrategyContext, compute_mid_strict, make_post_only_intent,
    should_requote_drift,
};
use tikr_core::{Decimal, MarketEvent, Price, Side, Size, Timestamp};

/// Configuration for [`Glft`]. NOTE: no `horizon_sec` (load-bearing structural
/// difference from [`crate::avellaneda_stoikov::AvellanedaStoikovConfig`]).
#[derive(Clone, Debug)]
pub struct GlftConfig {
    /// Risk aversion γ. Higher → stronger inventory mean-reversion.
    pub gamma: Decimal,
    /// Half-spread in basis points (e.g. 5 = 5 bps per side, 10 bps round-trip).
    /// Converted to price units via `mid * base_spread_bps / 10_000` at quote time.
    pub base_spread_bps: u32,
    /// Size placed at each quote level (both sides). Fixed fallback when
    /// `notional_per_quote` is `None`.
    pub size_per_quote: Size,
    /// When `Some`, each quote is sized to this USDT notional:
    /// `size = floor(notional / price, step_size)` — lets the account notional /
    /// live balance poller drive sizing (see `on_notional_updated`). `None` →
    /// fixed `size_per_quote`.
    pub notional_per_quote: Option<Decimal>,
    /// Lot step for notional-based sizing. Ignored when `notional_per_quote` is
    /// `None`.
    pub step_size: Decimal,
    /// Minimum time between full requotes, in milliseconds.
    pub min_requote_interval_ms: u64,
    /// Mid-drift threshold: `|new - prev| / prev > (level_step_bps/2) / 10_000`.
    pub level_step_bps: u32,
    /// Volatility estimator configuration.
    pub volatility: EwmaConfig,
}

/// GLFT (Guéant-Lehalle-Fernandez-Tapia, 2013; Guéant 2017 closed form) strategy.
pub struct Glft {
    config: GlftConfig,
    estimator: EwmaVolatility,
    last_requote_ts: Option<Timestamp>,
    last_quoted_mid: Option<Price>,
}

impl Glft {
    /// Returns the current EWMA variance (for diagnostics/tests).
    pub fn current_var(&self) -> Decimal {
        self.estimator.current_var()
    }

    /// Returns the count of computed-return samples seen so far.
    pub fn samples_seen(&self) -> u32 {
        self.estimator.samples_seen()
    }

    /// Per-quote size: `notional_per_quote / price` lot-floored to `step_size`
    /// when notional sizing is on, else the fixed `size_per_quote`.
    fn quote_size(&self, price: Decimal) -> Size {
        match self.config.notional_per_quote {
            Some(n)
                if n > Decimal::ZERO
                    && price > Decimal::ZERO
                    && self.config.step_size > Decimal::ZERO =>
            {
                let lots = (n / price / self.config.step_size).floor();
                Size((lots * self.config.step_size).max(self.config.step_size))
            }
            _ => self.config.size_per_quote,
        }
    }
}

impl Strategy for Glft {
    type Config = GlftConfig;

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
        "glft"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        let MarketEvent::BookUpdate { snapshot } = event else {
            return Vec::new();
        };
        let Some(mid) = compute_mid_strict(snapshot) else {
            return Vec::new();
        };
        self.estimator.on_book_update(mid, snapshot.ts);
        if self.estimator.samples_seen() < WARMUP_COUNT {
            return Vec::new();
        }
        if !should_requote_drift(
            self.last_requote_ts,
            self.last_quoted_mid,
            mid,
            snapshot.ts,
            self.config.min_requote_interval_ms,
            self.config.level_step_bps,
        ) {
            return Vec::new();
        }
        let q = ctx.position.size.0;
        let var = self.estimator.current_var();
        let gamma = self.config.gamma;

        // Reservation price: r = mid · (1 - q·γ·σ²)
        // σ² (var) is log-return variance — dimensionless — so the skew
        // term is scaled by mid to convert it into a price-space shift.
        // Without the mid factor, q·γ·σ² alone is a raw base-unit
        // magnitude that's effectively inert at crypto price levels
        // (BTC ~$76k): the skew must scale with the price level.
        // Infinite-horizon: no (T-t) multiplier; η simplified to 1 (see module doc).
        let r = mid.0 * (Decimal::ONE - q * gamma * var);

        // Half-spread: δ = mid · base_spread_bps / 10_000
        let delta = mid.0 * Decimal::from(self.config.base_spread_bps) / Decimal::from(10_000);

        self.last_requote_ts = Some(snapshot.ts);
        self.last_quoted_mid = Some(mid);
        let bid_px = r - delta;
        let ask_px = r + delta;
        vec![
            Action::CancelAll,
            Action::Quote(make_post_only_intent(
                ctx.symbol,
                Side::Bid,
                Price(bid_px),
                self.quote_size(bid_px),
            )),
            Action::Quote(make_post_only_intent(
                ctx.symbol,
                Side::Ask,
                Price(ask_px),
                self.quote_size(ask_px),
            )),
        ]
    }

    fn on_notional_updated(
        &mut self,
        _ctx: &StrategyContext<'_>,
        notional_per_order: Decimal,
    ) -> Vec<Action> {
        if notional_per_order > Decimal::ZERO {
            self.config.notional_per_quote = Some(notional_per_order);
        }
        Vec::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::avellaneda_stoikov::{AvellanedaStoikov, AvellanedaStoikovConfig};
    use tikr_core::{
        Asset, Decimal, Level, MarketKind, Notional, Position, SignedSize, Snapshot, Symbol,
        Timestamp, VenueId,
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
            recent_liqs: &[],
        }
    }

    fn default_config() -> GlftConfig {
        GlftConfig {
            gamma: Decimal::try_from(0.1).unwrap(),
            base_spread_bps: 5,
            size_per_quote: Size(Decimal::from(1)),
            notional_per_quote: None,
            step_size: Decimal::ZERO,
            min_requote_interval_ms: 1000,
            level_step_bps: 10,
            volatility: EwmaConfig {
                half_life_sec: 60.0,
                initial_var: Decimal::try_from(0.0001).unwrap(),
            },
        }
    }

    fn warmup_and_emit(
        strategy: &mut Glft,
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

    fn extract_quotes(actions: &[Action]) -> (Price, Price) {
        let mut bid = None;
        let mut ask = None;
        for a in actions {
            if let Action::Quote(intent) = a {
                match intent.side {
                    Side::Bid => bid = Some(intent.price),
                    Side::Ask => ask = Some(intent.price),
                }
            }
        }
        (bid.expect("bid"), ask.expect("ask"))
    }

    #[test]
    fn warmup_returns_empty() {
        let sym = make_symbol();
        let pos = make_position(&sym, Decimal::ZERO);
        let mut strat = Glft::new(default_config());

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
        let mut strat = Glft::new(default_config());
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
        let (bid, ask) = extract_quotes(&actions);
        assert_eq!((mid - bid.0).abs(), (ask.0 - mid).abs());
    }

    #[test]
    fn post_warmup_quotes_with_inventory_skew() {
        let sym = make_symbol();
        let pos = make_position(&sym, Decimal::from(5));
        let mut strat = Glft::new(default_config());
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
        let (bid, ask) = extract_quotes(&actions);
        let bid_offset = mid - bid.0;
        let ask_offset = ask.0 - mid;
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
        let mut strat = Glft::new(cfg);

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
    fn differs_from_avellaneda_stoikov_on_same_inputs() {
        let sym = make_symbol();
        let pos = make_position(&sym, Decimal::from(5));

        let gamma = Decimal::try_from(0.1).unwrap();
        let vol_cfg = EwmaConfig {
            half_life_sec: 60.0,
            initial_var: Decimal::try_from(0.0001).unwrap(),
        };
        // A-S uses horizon_sec = 3600 (finite-horizon); GLFT is infinite-horizon.
        // With the same base_spread_bps and gamma, the spread widths are identical
        // but reservation prices differ because A-S multiplies by horizon_sec.
        let as_cfg = AvellanedaStoikovConfig {
            gamma,
            base_spread_bps: 5,
            horizon_sec: 3600,
            size_per_quote: Size(Decimal::from(1)),
            notional_per_quote: None,
            step_size: Decimal::ZERO,
            min_requote_interval_ms: 1000,
            level_step_bps: 10,
            volatility: vol_cfg.clone(),
        };
        let glft_cfg = GlftConfig {
            gamma,
            base_spread_bps: 5,
            size_per_quote: Size(Decimal::from(1)),
            notional_per_quote: None,
            step_size: Decimal::ZERO,
            min_requote_interval_ms: 1000,
            level_step_bps: 10,
            volatility: vol_cfg,
        };

        let mut as_strat = AvellanedaStoikov::new(as_cfg);
        let mut glft_strat = Glft::new(glft_cfg);

        // Drive both with the SAME warmup sequence.
        for i in 0..WARMUP_COUNT {
            let bid = Decimal::from(100) + Decimal::from(i as i64) / Decimal::from(1000);
            let ask = bid + Decimal::ONE;
            let book = make_book(&sym, bid, ask, (i as u64) * 1_000_000_000);
            let ctx = make_ctx(&sym, &pos, &book);
            let evt = MarketEvent::BookUpdate {
                snapshot: book.clone(),
            };
            let _ = as_strat.on_event(&ctx, &evt);
            let _ = glft_strat.on_event(&ctx, &evt);
        }
        let final_book = make_book(
            &sym,
            Decimal::from(100),
            Decimal::from(101),
            (WARMUP_COUNT as u64) * 1_000_000_000,
        );
        let ctx = make_ctx(&sym, &pos, &final_book);
        let evt = MarketEvent::BookUpdate {
            snapshot: final_book.clone(),
        };
        let as_actions = as_strat.on_event(&ctx, &evt);
        let glft_actions = glft_strat.on_event(&ctx, &evt);

        let (as_bid, as_ask) = extract_quotes(&as_actions);
        let (glft_bid, glft_ask) = extract_quotes(&glft_actions);

        assert_ne!(
            as_bid, glft_bid,
            "A-S and GLFT should produce different bid prices on identical inputs (A-S has horizon_sec=3600, GLFT is infinite-horizon)"
        );
        assert_ne!(
            as_ask, glft_ask,
            "A-S and GLFT should produce different ask prices on identical inputs (A-S has horizon_sec=3600, GLFT is infinite-horizon)"
        );
    }

    #[test]
    fn half_spread_is_base_spread_bps_of_mid() {
        let sym = make_symbol();
        let pos = make_position(&sym, Decimal::ZERO);
        let mut cfg = default_config();
        cfg.base_spread_bps = 5;
        let mut strat = Glft::new(cfg);

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
