//! Core risk-gate trait, decision types, limits, and the [`BasicRiskGate`] impl.

use serde::{Deserialize, Serialize};
use tikr_backtest::pnl::PnLReport;
use tikr_core::{Notional, Side, SignedSize, Timestamp};
use tikr_strategy::Action;
use tikr_venue::QuoteIntent;

// ---------------------------------------------------------------------------
// RiskContext
// ---------------------------------------------------------------------------

/// Read-only snapshot passed to [`RiskGate::check`] each event.
pub struct RiskContext<'a> {
    /// Current position state for the symbol under management.
    pub position: &'a tikr_core::Position,
    /// Current cumulative P&L report.
    pub pnl: PnLReport,
    /// Wall-clock time of the event being evaluated (nanoseconds since UNIX epoch).
    pub now: Timestamp,
}

// ---------------------------------------------------------------------------
// RiskDecision
// ---------------------------------------------------------------------------

/// Possible outcomes of a [`RiskGate::check`] call.
#[derive(Debug, Clone)]
pub enum RiskDecision {
    /// Action is allowed; runner forwards to FillSim.
    Allow,
    /// Action rejected; runner drops it but continues processing future actions.
    Reject(String),
    /// Sticky halt; all subsequent actions are rejected until [`RiskGate::clear_halt`].
    Halt(String),
}

// ---------------------------------------------------------------------------
// RiskLimits
// ---------------------------------------------------------------------------

/// Operator-tunable risk limits. Each is independently nullable.
#[derive(Debug, Clone, Default)]
pub struct RiskLimits {
    /// Maximum absolute position size. Halt if exceeded.
    pub max_position_size: Option<SignedSize>,
    /// Maximum open notional (sum of |position| × current price). Halt if exceeded.
    pub max_open_notional: Option<Notional>,
    /// Maximum drawdown (negative-only P&L threshold). Halt if `pnl.net <= max_drawdown`.
    pub max_drawdown: Option<Notional>,
    /// Maximum fills per rolling 60-second window. Halt if exceeded (cancel-storm guard).
    pub max_fills_per_minute: Option<u32>,
}

// ---------------------------------------------------------------------------
// RiskState
// ---------------------------------------------------------------------------

/// Persisted state for a [`RiskGate`]. Serializes alongside `PaperReport` for resume (#29 / #32).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RiskState {
    /// Whether the gate is currently halted.
    pub halted: bool,
    /// Human-readable reason for the current halt (None if not halted).
    pub halt_reason: Option<String>,
    /// Timestamps (ns) of fills observed in the last 60s rolling window.
    pub recent_fill_window: Vec<u64>,
}

// ---------------------------------------------------------------------------
// RiskGate trait
// ---------------------------------------------------------------------------

/// The risk-check trait. Sync (matches `Strategy` sync per #28).
pub trait RiskGate: Send + Sync {
    /// Evaluate `action` against `ctx`. May return `Allow`, `Reject(reason)`, or `Halt(reason)`.
    ///
    /// On `Halt`, the gate's internal state flips to halted (sticky); subsequent
    /// `check` calls return `Reject("halted: <original reason>")` until
    /// [`RiskGate::clear_halt`] is called.
    fn check(&mut self, action: &Action, ctx: &RiskContext<'_>) -> RiskDecision;

    /// Record a fill timestamp into the rolling 60s fills-per-minute window.
    ///
    /// The runner is responsible for calling this on every observed fill so
    /// the cancel-storm guard (`max_fills_per_minute`) can fire on the next
    /// [`RiskGate::check`].
    fn record_fill(&mut self, ts: Timestamp);

    /// Clear the halted state. Does NOT reset `recent_fill_window` — fills-per-minute
    /// breaches re-arm correctly on next event.
    fn clear_halt(&mut self);

    /// Read-only access to current persisted state.
    fn state(&self) -> &RiskState;
}

// ---------------------------------------------------------------------------
// BasicRiskGate
// ---------------------------------------------------------------------------

/// Default `RiskGate` implementation backed by [`RiskLimits`].
pub struct BasicRiskGate {
    limits: RiskLimits,
    state: RiskState,
}

impl BasicRiskGate {
    /// Construct a fresh gate with the given limits + clean state.
    pub fn new(limits: RiskLimits) -> Self {
        Self {
            limits,
            state: RiskState::default(),
        }
    }

    /// Construct a gate from persisted limits + state (resume path; #32).
    pub fn from_state(limits: RiskLimits, state: RiskState) -> Self {
        Self { limits, state }
    }

    /// Inspect the configured limits (read-only).
    pub fn limits(&self) -> &RiskLimits {
        &self.limits
    }
}

impl RiskGate for BasicRiskGate {
    fn check(&mut self, action: &Action, ctx: &RiskContext<'_>) -> RiskDecision {
        // 1. Halted? Reject immediately (sticky).
        if self.state.halted {
            return RiskDecision::Reject(format!(
                "halted: {}",
                self.state.halt_reason.as_deref().unwrap_or("unknown")
            ));
        }

        // 2. Prune recent_fill_window to last 60s.
        let cutoff_ns = ctx.now.0.saturating_sub(60_000_000_000);
        self.state.recent_fill_window.retain(|&ts| ts >= cutoff_ns);

        // 3. Drawdown check — irrespective of action. Halt if breached.
        if let Some(threshold) = self.limits.max_drawdown
            && ctx.pnl.net.0 <= threshold.0
        {
            let reason = format!(
                "max_drawdown exceeded: net={} <= threshold={}",
                ctx.pnl.net.0, threshold.0
            );
            self.state.halted = true;
            self.state.halt_reason = Some(reason.clone());
            return RiskDecision::Halt(reason);
        }

        // 4. Fills-per-minute check — irrespective of action. Halt if breached.
        if let Some(limit) = self.limits.max_fills_per_minute
            && self.state.recent_fill_window.len() > limit as usize
        {
            let reason = format!(
                "max_fills_per_minute exceeded: {} > {}",
                self.state.recent_fill_window.len(),
                limit
            );
            self.state.halted = true;
            self.state.halt_reason = Some(reason.clone());
            return RiskDecision::Halt(reason);
        }

        // 5. Position + notional checks — only for Quote / Requote actions.
        //    Cancel / CancelAll / NoOp can't grow position.
        if let Some(intent) = action_intent(action) {
            // 5a. Hypothetical post-action position size.
            let delta = match intent.side {
                Side::Bid => intent.size.0,
                Side::Ask => -intent.size.0,
            };
            let post_size = ctx.position.size.0 + delta;
            if let Some(max) = self.limits.max_position_size
                && post_size.abs() > max.0.abs()
            {
                let reason = format!(
                    "max_position_size exceeded: {} > {}",
                    post_size.abs(),
                    max.0.abs()
                );
                self.state.halted = true;
                self.state.halt_reason = Some(reason.clone());
                return RiskDecision::Halt(reason);
            }

            // 5b. Hypothetical post-action open notional.
            if let Some(max) = self.limits.max_open_notional {
                let post_notional = post_size.abs() * intent.price.0;
                if post_notional > max.0 {
                    let reason =
                        format!("max_open_notional exceeded: {} > {}", post_notional, max.0);
                    self.state.halted = true;
                    self.state.halt_reason = Some(reason.clone());
                    return RiskDecision::Halt(reason);
                }
            }
        }

        RiskDecision::Allow
    }

    fn record_fill(&mut self, ts: Timestamp) {
        self.state.recent_fill_window.push(ts.0);
    }

    fn clear_halt(&mut self) {
        self.state.halted = false;
        self.state.halt_reason = None;
        // recent_fill_window deliberately NOT cleared — fills-per-minute breaches
        // re-arm correctly on next event after natural window aging.
    }

    fn state(&self) -> &RiskState {
        &self.state
    }
}

fn action_intent(action: &Action) -> Option<&QuoteIntent> {
    match action {
        Action::Quote(intent) => Some(intent),
        Action::Requote { intent, .. } => Some(intent),
        Action::Cancel(_) | Action::CancelAll | Action::NoOp => None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::{
        Asset, Decimal, Notional, Position, Price, QuoteKind, Side, SignedSize, Size, Symbol,
        TimeInForce, Timestamp, VenueId,
    };
    use tikr_strategy::Action;
    use tikr_venue::QuoteIntent;

    fn make_symbol() -> Symbol {
        Symbol {
            base: Asset::new("BTC"),
            quote: Asset::new("USDC"),
            venue: VenueId::new("test"),
        }
    }

    fn make_position(size: i64) -> Position {
        Position {
            symbol: make_symbol(),
            size: SignedSize(Decimal::from(size)),
            avg_entry: Price(Decimal::from(100)),
            realized_pnl: Notional(Decimal::ZERO),
        }
    }

    fn make_pnl(net: i64) -> PnLReport {
        PnLReport {
            realized: Notional(Decimal::ZERO),
            unrealized: Notional(Decimal::ZERO),
            fees: Notional(Decimal::ZERO),
            funding: Notional(Decimal::ZERO),
            net: Notional(Decimal::from(net)),
        }
    }

    fn quote_action(side: Side, size: i64) -> Action {
        Action::Quote(QuoteIntent {
            symbol: make_symbol(),
            side,
            price: Price(Decimal::from(100)),
            size: Size(Decimal::from(size)),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        })
    }

    fn ctx<'a>(pos: &'a Position, pnl: PnLReport, now_ns: u64) -> RiskContext<'a> {
        RiskContext {
            position: pos,
            pnl,
            now: Timestamp(now_ns),
        }
    }

    #[test]
    fn allow_when_no_limits() {
        let mut gate = BasicRiskGate::new(RiskLimits::default());
        let pos = make_position(0);
        let decision = gate.check(&quote_action(Side::Bid, 1), &ctx(&pos, make_pnl(0), 0));
        assert!(matches!(decision, RiskDecision::Allow));
    }

    #[test]
    fn halt_on_max_position_breach() {
        let mut gate = BasicRiskGate::new(RiskLimits {
            max_position_size: Some(SignedSize(Decimal::from(10))),
            ..Default::default()
        });
        let pos = make_position(10);
        // Bid size 1 -> hypothetical post = 11 > 10 -> Halt
        let decision = gate.check(&quote_action(Side::Bid, 1), &ctx(&pos, make_pnl(0), 0));
        assert!(matches!(decision, RiskDecision::Halt(_)));
        assert!(gate.state().halted);
        // Subsequent check returns Reject (sticky)
        let second = gate.check(&quote_action(Side::Ask, 1), &ctx(&pos, make_pnl(0), 0));
        assert!(matches!(second, RiskDecision::Reject(_)));
    }

    #[test]
    fn halt_on_max_drawdown() {
        let mut gate = BasicRiskGate::new(RiskLimits {
            max_drawdown: Some(Notional(Decimal::from(-100))),
            ..Default::default()
        });
        let pos = make_position(0);
        // pnl.net = -150 <= -100 -> Halt regardless of action
        let decision = gate.check(&quote_action(Side::Bid, 1), &ctx(&pos, make_pnl(-150), 0));
        assert!(matches!(decision, RiskDecision::Halt(_)));
    }

    #[test]
    fn reject_on_fills_per_minute() {
        let mut gate = BasicRiskGate::new(RiskLimits {
            max_fills_per_minute: Some(3),
            ..Default::default()
        });
        // Pre-populate window with 4 fills within last 60s (limit=3 -> 4 > 3 -> Halt)
        let now_ns = 60_000_000_000_u64; // 60s
        for ts in [
            10_000_000_000,
            20_000_000_000,
            30_000_000_000,
            40_000_000_000,
        ] {
            gate.record_fill(Timestamp(ts));
        }
        let pos = make_position(0);
        let decision = gate.check(&quote_action(Side::Bid, 1), &ctx(&pos, make_pnl(0), now_ns));
        // Per the locked decision: halt-on-fills (cancel-storm = bug)
        assert!(matches!(decision, RiskDecision::Halt(_)));
    }

    #[test]
    fn halt_is_sticky() {
        let mut gate = BasicRiskGate::new(RiskLimits {
            max_position_size: Some(SignedSize(Decimal::from(10))),
            ..Default::default()
        });
        let pos = make_position(10);
        // Trip halt
        let _ = gate.check(&quote_action(Side::Bid, 1), &ctx(&pos, make_pnl(0), 0));
        assert!(gate.state().halted);
        // Clear halt
        gate.clear_halt();
        assert!(!gate.state().halted);
        // Non-breaching action allowed
        let pos_clean = make_position(0);
        let decision = gate.check(
            &quote_action(Side::Bid, 1),
            &ctx(&pos_clean, make_pnl(0), 0),
        );
        assert!(matches!(decision, RiskDecision::Allow));
        // Re-trip halt
        let decision2 = gate.check(&quote_action(Side::Bid, 1), &ctx(&pos, make_pnl(0), 0));
        assert!(matches!(decision2, RiskDecision::Halt(_)));
    }

    #[test]
    fn window_prunes_old_fills() {
        let mut gate = BasicRiskGate::new(RiskLimits {
            max_fills_per_minute: Some(3),
            ..Default::default()
        });
        // Old fills from earlier window
        for ts in [
            10_000_000_000,
            20_000_000_000,
            30_000_000_000,
            40_000_000_000,
        ] {
            gate.record_fill(Timestamp(ts));
        }
        let pos = make_position(0);
        // now = 120s; cutoff = 120s - 60s = 60s; all old fills < 60s -> pruned
        let decision = gate.check(
            &quote_action(Side::Bid, 1),
            &ctx(&pos, make_pnl(0), 120_000_000_000),
        );
        assert!(matches!(decision, RiskDecision::Allow));
        assert_eq!(gate.state().recent_fill_window.len(), 0);
    }
}
