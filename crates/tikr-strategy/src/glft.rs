//! GLFT (Guéant-Lehalle-Fernandez-Tapia) inventory-aware market-making strategy.
//!
//! Companion to [`crate::avellaneda_stoikov`] — same Strategy trait, same
//! `[CancelAll, Quote(Bid), Quote(Ask)]` output shape, same warmup gate. The
//! load-bearing difference is the formula: GLFT removes the `(T-t)` horizon
//! that A-S requires (and that operators have to reset). See [issue #18] for
//! the full design rationale.
//!
//! # Formula (simplified Guéant 2017 closed form)
//!
//! Reservation price: `r = mid - q·γ·σ²·η` where `η = 2 / (γ·k·A·exp(-1))`.
//! Optimal half-spread: `δ* = (1/γ)·ln(1 + γ/k) + 0.5·γ·σ²·η`.
//! Bid = `r - δ*`, Ask = `r + δ*`.
//!
//! `η` is a bounded constant (no `T-t` dependence), so the formula stays
//! well-behaved over arbitrary-length runs.
//!
//! # Sign convention
//!
//! Same as A-S: long inventory (`q > 0`) pushes the reservation price DOWN
//! to encourage selling.
//!
//! # Decimal ↔ f64 island
//!
//! `Decimal` lacks `exp`/`ln`/`pow`, so `η` and `ln(1 + γ/k)` are computed
//! in f64 and converted back to `Decimal`. Same pattern as
//! [`crate::volatility::EwmaVolatility`] and [`crate::avellaneda_stoikov`].
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
use crate::{Action, Strategy, StrategyContext};
use tikr_core::{
    Decimal, MarketEvent, Price, QuoteKind, Side, Size, Symbol, TimeInForce, Timestamp,
};
use tikr_venue::QuoteIntent;

/// Configuration for [`Glft`]. NOTE: no `horizon_sec` (load-bearing structural
/// difference from [`crate::avellaneda_stoikov::AvellanedaStoikovConfig`]).
#[derive(Clone, Debug)]
pub struct GlftConfig {
    /// Risk aversion γ. Higher → narrower spread + stronger inventory mean-reversion.
    pub gamma: Decimal,
    /// Market depth intensity k.
    pub k: Decimal,
    /// Intensity pre-factor A. Default 1.0. Calibration deferred to Phase 3.
    pub a: Decimal,
    /// Size placed at each quote level (both sides).
    pub size_per_quote: Size,
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
        let Some(mid) = compute_mid(snapshot) else {
            return Vec::new();
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
        let q = ctx.position.size.0;
        let var = self.estimator.current_var();
        let gamma = self.config.gamma;
        let k = self.config.k;
        let a = self.config.a;

        // η = 2 / (γ·k·A·exp(-1))
        // f64 island: compute exp(-1) and the full denominator, then back to Decimal.
        let denom_f64 = {
            let g = gamma.to_string().parse::<f64>().unwrap_or(0.0);
            let k_f = k.to_string().parse::<f64>().unwrap_or(0.0);
            let a_f = a.to_string().parse::<f64>().unwrap_or(0.0);
            g * k_f * a_f * (-1.0_f64).exp()
        };
        let eta = if denom_f64.abs() > 0.0 {
            Decimal::try_from(2.0 / denom_f64).unwrap_or(Decimal::ZERO)
        } else {
            // Degenerate config (gamma/k/A = 0): η undefined, fall back to zero skew.
            // Strategy still quotes (with symmetric ln-spread only).
            Decimal::ZERO
        };

        // Reservation price: r = mid - q·γ·σ²·η
        let r = mid.0 - q * gamma * var * eta;

        // Half-spread: δ* = (1/γ)·ln(1 + γ/k) + 0.5·γ·σ²·η
        let inv_gamma = Decimal::ONE / gamma;
        let one_plus_ratio_f64 = (Decimal::ONE + gamma / k)
            .to_string()
            .parse::<f64>()
            .unwrap_or(1.0);
        let ln_term = Decimal::try_from(one_plus_ratio_f64.ln()).unwrap_or(Decimal::ZERO);
        let half = Decimal::ONE / Decimal::from(2);
        let delta = inv_gamma * ln_term + half * gamma * var * eta;

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

// Duplicated helpers (Phase 1 expediency precedent; refactor across 3 consumers
// is overdue and tracked as a separate cleanup pass — not part of this issue's scope).
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
    use crate::avellaneda_stoikov::{AvellanedaStoikov, AvellanedaStoikovConfig};
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

    fn default_config() -> GlftConfig {
        GlftConfig {
            gamma: Decimal::try_from(0.1).unwrap(),
            k: Decimal::try_from(1.5).unwrap(),
            a: Decimal::try_from(1.0).unwrap(),
            size_per_quote: Size(Decimal::from(1)),
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
        let k = Decimal::try_from(1.5).unwrap();
        let vol_cfg = EwmaConfig {
            half_life_sec: 60.0,
            initial_var: Decimal::try_from(0.0001).unwrap(),
        };
        let as_cfg = AvellanedaStoikovConfig {
            gamma,
            k,
            horizon_sec: 3600,
            size_per_quote: Size(Decimal::from(1)),
            min_requote_interval_ms: 1000,
            level_step_bps: 10,
            volatility: vol_cfg.clone(),
        };
        let glft_cfg = GlftConfig {
            gamma,
            k,
            a: Decimal::try_from(1.0).unwrap(),
            size_per_quote: Size(Decimal::from(1)),
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
            "A-S and GLFT should produce different bid prices on identical inputs"
        );
        assert_ne!(
            as_ask, glft_ask,
            "A-S and GLFT should produce different ask prices on identical inputs"
        );
    }
}
