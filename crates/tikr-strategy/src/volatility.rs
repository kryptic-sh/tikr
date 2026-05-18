//! EWMA volatility estimator — online, allocation-free, used by inventory-aware
//! strategies (Avellaneda-Stoikov #19, GLFT #20).
//!
//! # Decay math
//!
//! Per-update decay: `λ_step = 0.5^(Δt_sec / half_life_sec)`. Variance update:
//! `σ²_new = λ_step · σ²_old + (1 - λ_step) · r²` where `r = ln(s_t / s_{t-1})`.
//! Per-update λ handles non-uniform tick spacing without resampling.
//!
//! # Decimal ↔ f64 island
//!
//! `Decimal` has no `ln` or `pow`, so `on_book_update` converts mid + timestamps
//! to `f64` to compute `λ_step` and the log return, then converts the new
//! variance back to `Decimal`. This is the documented Phase 2 expediency.
//! Numeric drift at typical MM scales is well below the precision the operator
//! tunes against.
//!
//! # Warmup
//!
//! [`WARMUP_COUNT`] returns are required before downstream strategies should
//! trust [`EwmaVolatility::current_var`]. [`EwmaVolatility::samples_seen`]
//! counts computed returns (NOT raw `on_book_update` calls — the first call
//! only seeds the previous mid).

use tikr_core::{Decimal, Price, Timestamp};

/// Number of computed-return samples required before downstream strategies
/// should trust [`EwmaVolatility::current_var`]. First [`EwmaVolatility::on_book_update`]
/// call seeds the previous mid (no return), so the 1st sample is computed on the
/// 2nd call. Strategies should gate quoting on `samples_seen() >= WARMUP_COUNT`.
pub const WARMUP_COUNT: u32 = 30;

/// Configuration for [`EwmaVolatility`].
#[derive(Clone, Debug)]
pub struct EwmaConfig {
    /// Half-life of the decay, in seconds. Operator-friendly knob: half-life
    /// of 60s means each observation's weight halves after 60s of sim time.
    pub half_life_sec: f64,
    /// Initial variance value used until the second sample arrives.
    pub initial_var: Decimal,
}

/// Online EWMA variance estimator on log returns of book mid.
pub struct EwmaVolatility {
    cfg: EwmaConfig,
    var: Decimal,
    last_mid: Option<Price>,
    last_ts: Option<Timestamp>,
    /// Counts COMPUTED RETURNS, not raw on_book_update calls. First call seeds; samples_seen stays 0.
    samples_seen: u32,
}

impl EwmaVolatility {
    /// Construct a new estimator with `cfg`. `current_var()` returns `cfg.initial_var`
    /// until the second `on_book_update` call.
    pub fn new(cfg: EwmaConfig) -> Self {
        Self {
            var: cfg.initial_var,
            cfg,
            last_mid: None,
            last_ts: None,
            samples_seen: 0,
        }
    }

    /// Observe a new mid + timestamp.
    ///
    /// First call seeds the previous mid (no variance update, `samples_seen()` stays 0).
    /// Subsequent calls compute the log return, update variance via per-update decay,
    /// and increment `samples_seen`.
    ///
    /// Calls with `ts <= last_ts` (clock did not advance) are no-ops — guards
    /// against duplicate or out-of-order BookUpdates without panicking.
    pub fn on_book_update(&mut self, mid: Price, ts: Timestamp) {
        let (Some(prev_mid), Some(prev_ts)) = (self.last_mid, self.last_ts) else {
            // First call — seed only.
            self.last_mid = Some(mid);
            self.last_ts = Some(ts);
            return;
        };
        if ts.0 <= prev_ts.0 {
            // Non-monotonic timestamp — skip the update but DO advance the seed
            // to the latest mid (best-effort recovery if data is mildly disordered).
            self.last_mid = Some(mid);
            self.last_ts = Some(ts);
            return;
        }
        // f64 island: ln + pow for decay + return.
        let dt_sec = (ts.0 - prev_ts.0) as f64 / 1e9;
        let lambda_step = 0.5_f64.powf(dt_sec / self.cfg.half_life_sec);
        let s_t = mid.0.to_string().parse::<f64>().unwrap_or(0.0);
        let s_prev = prev_mid.0.to_string().parse::<f64>().unwrap_or(0.0);
        // Guard against zero/negative mid before ln.
        if s_t <= 0.0 || s_prev <= 0.0 {
            self.last_mid = Some(mid);
            self.last_ts = Some(ts);
            return;
        }
        let r = (s_t / s_prev).ln();
        let var_old = self.var.to_string().parse::<f64>().unwrap_or(0.0);
        let var_new = lambda_step * var_old + (1.0 - lambda_step) * r * r;
        self.var = Decimal::try_from(var_new).unwrap_or(self.var);
        self.last_mid = Some(mid);
        self.last_ts = Some(ts);
        self.samples_seen = self.samples_seen.saturating_add(1);
    }

    /// Current variance estimate (Decimal).
    pub fn current_var(&self) -> Decimal {
        self.var
    }

    /// Count of computed-return samples observed. Strategies should wait
    /// for `samples_seen() >= WARMUP_COUNT` before quoting.
    pub fn samples_seen(&self) -> u32 {
        self.samples_seen
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg_half_life_1s() -> EwmaConfig {
        EwmaConfig {
            half_life_sec: 1.0,
            initial_var: Decimal::ONE,
        }
    }

    #[test]
    fn first_sample_no_var() {
        let mut ewma = EwmaVolatility::new(EwmaConfig {
            half_life_sec: 60.0,
            initial_var: Decimal::from(1),
        });
        ewma.on_book_update(Price(Decimal::from(100)), Timestamp(0));
        assert_eq!(ewma.samples_seen(), 0);
        assert_eq!(ewma.current_var(), Decimal::from(1));
    }

    #[test]
    fn two_samples_compute_return() {
        let mut ewma = EwmaVolatility::new(EwmaConfig {
            half_life_sec: 60.0,
            initial_var: Decimal::from(1),
        });
        ewma.on_book_update(Price(Decimal::from(100)), Timestamp(0));
        ewma.on_book_update(Price(Decimal::from(110)), Timestamp(1_000_000_000));
        assert_eq!(ewma.samples_seen(), 1);
        assert_ne!(ewma.current_var(), Decimal::from(1));
    }

    #[test]
    fn half_life_decay() {
        let mut ewma = EwmaVolatility::new(cfg_half_life_1s());
        ewma.on_book_update(Price(Decimal::from(100)), Timestamp(0));
        ewma.on_book_update(Price(Decimal::from(100)), Timestamp(1_000_000_000));
        ewma.on_book_update(Price(Decimal::from(100)), Timestamp(2_000_000_000));
        assert_eq!(ewma.samples_seen(), 2);
        let actual: f64 = ewma.current_var().to_string().parse().unwrap();
        assert!((actual - 0.25).abs() < 1e-9, "expected ~0.25, got {actual}");
    }

    #[test]
    fn constant_mid_yields_zero_variance() {
        let mut ewma = EwmaVolatility::new(cfg_half_life_1s());
        for i in 0..21 {
            ewma.on_book_update(Price(Decimal::from(100)), Timestamp(i * 1_000_000_000));
        }
        let actual: f64 = ewma.current_var().to_string().parse().unwrap();
        assert!(actual < 1e-5, "expected variance < 1e-5, got {actual}");
    }
}
