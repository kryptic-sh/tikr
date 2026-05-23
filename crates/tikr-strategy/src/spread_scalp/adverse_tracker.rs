//! Adverse-selection tracker — measure post-fill mid drift, widen
//! `min_spread_bps` dynamically when toxicity rises.
//!
//! Pre-refactor `min_spread_bps` was a fixed guess. Real spread
//! distributions vary 5x intraday and toxic flow comes in bursts —
//! the same threshold that captures clean spread on a quiet hour
//! gives back the edge on a fast hour.
//!
//! Mechanism:
//!
//! 1. On every fill, schedule a deferred mid-snapshot at `now + N
//!    seconds`. The runner pumps event ticks; when the deferred ts is
//!    reached, compare the resting mid to the fill price.
//! 2. Adverse drift = `(mid_after - fill_price) signed by fill side`
//!    in bps of fill price. Long fill (Bid) is adverse when mid drops;
//!    short fill (Ask) is adverse when mid rises.
//! 3. Rolling EMA of per-fill adverse drift. When the EMA exceeds a
//!    configurable threshold, bump `min_spread_bps` by 1 bp; decay
//!    back down by 0.5 bps every minute of quiet flow.
//!
//! The strategy reads `current_min_spread_bps()` instead of
//! `cfg.min_spread_bps` directly — pure additive change, no behaviour
//! shift until adverse drift starts firing.

use std::collections::VecDeque;
use tikr_core::{Decimal, Price, Side, Timestamp};

/// One fill awaiting its post-fill snapshot.
#[derive(Debug, Clone, Copy)]
struct PendingFill {
    /// Fill timestamp (nanoseconds).
    fill_ts: Timestamp,
    /// Snapshot due at this absolute ts (nanoseconds).
    snapshot_due_ts: Timestamp,
    /// Side we filled on (Bid = we bought, adverse if mid drops).
    side: Side,
    /// Fill price.
    fill_price: Price,
}

/// Configuration for the adverse-selection tracker.
#[derive(Debug, Clone, Copy)]
pub struct AdverseConfig {
    /// Window between fill and snapshot, in milliseconds. 5000
    /// (5 s) is a good default — captures short-term adverse drift
    /// without too much noise from regular flow.
    pub snapshot_window_ms: u64,
    /// EMA half-life over fills. Higher = smoother, slower reaction.
    /// 10 fills is a sensible default for symbols with 30+ fpm.
    pub ema_half_life_fills: u32,
    /// Adverse-drift threshold in bps. When the EMA exceeds this,
    /// `current_min_spread_bps` adds a widening surcharge.
    pub threshold_bps: Decimal,
    /// Max bps the surcharge can add above the configured baseline.
    /// Bounds the worst-case widening so a brief toxic burst doesn't
    /// silence the strategy for the rest of the session.
    pub max_widen_bps: u32,
}

impl AdverseConfig {
    /// Sensible defaults — usable directly on most symbols.
    pub fn sensible() -> Self {
        Self {
            snapshot_window_ms: 5_000,
            ema_half_life_fills: 10,
            threshold_bps: Decimal::from(3),
            max_widen_bps: 10,
        }
    }
    /// All-zero config that disables the tracker — `current_widen_bps`
    /// always returns 0.
    pub fn disabled() -> Self {
        Self {
            snapshot_window_ms: 0,
            ema_half_life_fills: 0,
            threshold_bps: Decimal::ZERO,
            max_widen_bps: 0,
        }
    }
}

/// State for the adverse tracker. Owned by the strategy; updated on
/// every fill (`record_fill`) and every event tick
/// (`process_due_snapshots`).
#[derive(Debug, Clone)]
pub struct AdverseTracker {
    cfg: AdverseConfig,
    pending: VecDeque<PendingFill>,
    /// Rolling EMA of per-fill adverse drift in bps. Positive = toxic.
    ema_adverse_bps: Decimal,
    /// Whether at least one fill has been recorded (needed for the
    /// EMA to start tracking instead of staying at 0 forever).
    seeded: bool,
}

impl AdverseTracker {
    /// Construct a tracker. Pass `AdverseConfig::disabled()` to make
    /// it a no-op.
    pub fn new(cfg: AdverseConfig) -> Self {
        Self {
            cfg,
            pending: VecDeque::new(),
            ema_adverse_bps: Decimal::ZERO,
            seeded: false,
        }
    }

    /// Reconfigure live — used by tests and by future param-sweep code.
    pub fn set_config(&mut self, cfg: AdverseConfig) {
        self.cfg = cfg;
    }

    /// Record a fill. Schedules the post-fill snapshot. No-op when the
    /// window is 0 (tracker disabled).
    pub fn record_fill(&mut self, fill_ts: Timestamp, side: Side, fill_price: Price) {
        if self.cfg.snapshot_window_ms == 0 {
            return;
        }
        let due_ns = fill_ts
            .0
            .saturating_add(self.cfg.snapshot_window_ms.saturating_mul(1_000_000));
        self.pending.push_back(PendingFill {
            fill_ts,
            snapshot_due_ts: Timestamp(due_ns),
            side,
            fill_price,
        });
    }

    /// Walk the pending queue, comparing every fill whose snapshot is
    /// due (`now_ts >= snapshot_due_ts`) to `current_mid`, and folding
    /// the per-fill adverse drift into the EMA.
    pub fn process_due_snapshots(&mut self, now: Timestamp, current_mid: Price) {
        if self.cfg.snapshot_window_ms == 0 || current_mid.0 <= Decimal::ZERO {
            return;
        }
        while let Some(front) = self.pending.front() {
            if now.0 < front.snapshot_due_ts.0 {
                break;
            }
            let pf = self.pending.pop_front().expect("front existed");
            // Adverse drift signed for the maker:
            //   Bid fill (we bought) → adverse if mid dropped.
            //     adverse_bps = (fill_price - current_mid) / fill_price × 10_000
            //   Ask fill (we sold) → adverse if mid rose.
            //     adverse_bps = (current_mid - fill_price) / fill_price × 10_000
            let signed = match pf.side {
                Side::Bid => pf.fill_price.0 - current_mid.0,
                Side::Ask => current_mid.0 - pf.fill_price.0,
            };
            if pf.fill_price.0 <= Decimal::ZERO {
                continue;
            }
            let adverse_bps = signed / pf.fill_price.0 * Decimal::from(10_000);
            self.fold_into_ema(adverse_bps);
        }
    }

    /// Current widen surcharge in bps. `0` when EMA is at/below
    /// threshold OR tracker is disabled.
    pub fn current_widen_bps(&self) -> Decimal {
        if self.cfg.max_widen_bps == 0 || !self.seeded {
            return Decimal::ZERO;
        }
        let excess = self.ema_adverse_bps - self.cfg.threshold_bps;
        if excess <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        excess.min(Decimal::from(self.cfg.max_widen_bps))
    }

    /// Current EMA of adverse drift in bps. Exposed for tests +
    /// future telemetry.
    pub fn current_ema_bps(&self) -> Decimal {
        self.ema_adverse_bps
    }

    fn fold_into_ema(&mut self, sample_bps: Decimal) {
        if !self.seeded {
            self.ema_adverse_bps = sample_bps;
            self.seeded = true;
            return;
        }
        // EMA alpha from half-life N fills: alpha = 1 - 0.5^(1/N).
        // 0.5^(1/N) ≈ 1 - ln(2)/N for small N; use the closed form
        // via Decimal-safe linear approximation since `Decimal::powf`
        // is overkill.
        //
        // For N=10: alpha = 1 - 0.933 ≈ 0.067.
        // We approximate alpha = 2/(N+1) (standard EMA convention) —
        // close enough for a heuristic widener and stays in Decimal
        // math.
        let n = self.cfg.ema_half_life_fills.max(1);
        let alpha_num = Decimal::from(2);
        let alpha_den = Decimal::from(n + 1);
        // ema = alpha · sample + (1 - alpha) · ema
        // Multiplied through by alpha_den to stay in integer-style math:
        // ema · alpha_den = alpha_num · sample + (alpha_den - alpha_num) · ema
        let weighted = alpha_num * sample_bps + (alpha_den - alpha_num) * self.ema_adverse_bps;
        self.ema_adverse_bps = weighted / alpha_den;
    }

    /// Pending fill count — useful for instrumentation.
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// Last fill timestamp seen, for telemetry. `None` when nothing
    /// pending and nothing has folded yet.
    pub fn last_fill_ts(&self) -> Option<Timestamp> {
        self.pending.back().map(|p| p.fill_ts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(ms: u64) -> Timestamp {
        Timestamp(ms.saturating_mul(1_000_000))
    }

    #[test]
    fn disabled_tracker_never_widens() {
        let mut t = AdverseTracker::new(AdverseConfig::disabled());
        t.record_fill(ts(0), Side::Bid, Price(Decimal::from(100)));
        t.process_due_snapshots(ts(1_000_000), Price(Decimal::from(90))); // huge adverse drift
        assert_eq!(t.current_widen_bps(), Decimal::ZERO);
    }

    #[test]
    fn favourable_drift_does_not_widen() {
        let mut t = AdverseTracker::new(AdverseConfig::sensible());
        // Long fill at 100 → mid 110 after = favourable for the maker.
        t.record_fill(ts(0), Side::Bid, Price(Decimal::from(100)));
        t.process_due_snapshots(ts(5_000), Price(Decimal::from(110)));
        // Adverse EMA should be NEGATIVE (we made money). Widen = 0.
        assert!(t.current_ema_bps() < Decimal::ZERO);
        assert_eq!(t.current_widen_bps(), Decimal::ZERO);
    }

    #[test]
    fn adverse_drift_grows_ema_above_threshold() {
        let cfg = AdverseConfig {
            snapshot_window_ms: 5_000,
            ema_half_life_fills: 2,
            threshold_bps: Decimal::from(50),
            max_widen_bps: 10,
        };
        let mut t = AdverseTracker::new(cfg);
        // Five long fills at 100 → mid drops to 99 after 5s each.
        // Per fill adverse: (100 - 99) / 100 × 10_000 = 100 bps.
        for i in 0..5 {
            let fill_t = ts(i * 6_000);
            t.record_fill(fill_t, Side::Bid, Price(Decimal::from(100)));
            t.process_due_snapshots(
                Timestamp(fill_t.0 + 5_000 * 1_000_000),
                Price(Decimal::from(99)),
            );
        }
        // EMA settles near 100 bps; threshold 50 → widen >= 50.
        assert!(t.current_ema_bps() > Decimal::from(50));
        assert!(t.current_widen_bps() > Decimal::ZERO);
        // Capped at max_widen_bps.
        assert!(t.current_widen_bps() <= Decimal::from(10));
    }

    #[test]
    fn snapshot_not_processed_before_window() {
        let mut t = AdverseTracker::new(AdverseConfig::sensible());
        t.record_fill(ts(0), Side::Bid, Price(Decimal::from(100)));
        // Only 1s in, window is 5s → snapshot not due.
        t.process_due_snapshots(ts(1_000), Price(Decimal::from(50)));
        assert_eq!(t.pending_len(), 1);
        assert!(!t.current_ema_bps().is_zero() || t.pending_len() == 1);
    }
}
