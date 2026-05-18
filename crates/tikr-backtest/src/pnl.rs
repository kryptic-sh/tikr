//! Position + P&L accounting — WACC cost basis, separate fee tracking.
//! See [issue #12] for cost-basis rationale.
//!
//! [issue #12]: https://github.com/kryptic-sh/tikr/issues/12

use tikr_core::{Decimal, Fill, Notional, Position, Price, SignedSize, Symbol};

/// Running position + P&L state for one symbol. WACC cost basis.
pub struct PositionTracker {
    symbol: Symbol,
    size: SignedSize,
    avg_entry: Price,
    realized_pnl: Notional,
    #[allow(dead_code)] // wired in issue #12
    fees_paid: Notional,
    /// Phase 1: always zero. Phase 2 wires real funding.
    #[allow(dead_code)] // wired in Phase 2
    funding_accrued: Notional,
}

impl PositionTracker {
    /// Construct a new flat tracker for `symbol`.
    pub fn new(symbol: Symbol) -> Self {
        Self {
            symbol,
            size: SignedSize(Decimal::ZERO),
            avg_entry: Price(Decimal::ZERO),
            realized_pnl: Notional(Decimal::ZERO),
            fees_paid: Notional(Decimal::ZERO),
            funding_accrued: Notional(Decimal::ZERO),
        }
    }

    /// Apply `fill` to the running state. WACC math, fees accumulated separately.
    pub fn apply(&mut self, fill: &Fill) {
        let _ = fill;
        todo!("issue #12: WACC math, realize on direction-flip or size-reduce")
    }

    /// Current position snapshot (immutable view).
    pub fn snapshot(&self) -> Position {
        Position {
            symbol: self.symbol.clone(),
            size: self.size,
            avg_entry: self.avg_entry,
            realized_pnl: self.realized_pnl,
        }
    }

    /// Aggregate report at the given mark price.
    pub fn report(&self, last_mid: Price) -> PnLReport {
        let _ = last_mid;
        todo!("issue #12: realized + unrealized at last_mid + fees + funding = net")
    }
}

/// Aggregate P&L report at a point in time.
#[derive(Debug, Clone, Copy)]
pub struct PnLReport {
    /// Realized P&L (gross, before fees).
    pub realized: Notional,
    /// Unrealized P&L marked at the report price.
    pub unrealized: Notional,
    /// Total fees paid (positive) or rebated (negative).
    pub fees: Notional,
    /// `realized - fees + funding + unrealized`.
    pub net: Notional,
}
