//! [`PaperReport`] — extends [`tikr_backtest::pnl::PnLReport`] with runtime stats.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::str::FromStr;
use tikr_core::{Decimal, Notional};

/// Aggregate P&L + runtime stats for a paper trading session.
///
/// Serializes as JSON for state snapshots (see [`crate::state::write_snapshot`]).
/// `Notional` fields are emitted as decimal strings (via [`rust_decimal`]'s
/// `Display`/`FromStr`) since `tikr-core` does not opt into `rust_decimal`'s
/// `serde` feature.
#[derive(Debug, Clone, Copy)]
pub struct PaperReport {
    /// Realized P&L (gross of fees).
    pub realized: Notional,
    /// Unrealized P&L marked at last observed mid.
    pub unrealized: Notional,
    /// Total fees paid (positive) or rebated (negative).
    pub fees: Notional,
    /// Funding accrued (Phase 3: always zero; real funding is Phase 4).
    pub funding: Notional,
    /// Net P&L: `realized + unrealized - fees + funding`.
    pub net: Notional,
    /// Total runtime in seconds.
    pub runtime_secs: u64,
    /// Total `MarketEvent` count processed by the runner.
    pub events_processed: u64,
    /// Total `Fill` count emitted by [`FillSim`][tikr_backtest::fill_sim::FillSim].
    pub fills_emitted: u64,
}

// --- serde wire format ---------------------------------------------------

#[derive(Serialize, Deserialize)]
struct PaperReportWire {
    realized: String,
    unrealized: String,
    fees: String,
    funding: String,
    net: String,
    runtime_secs: u64,
    events_processed: u64,
    fills_emitted: u64,
}

impl Serialize for PaperReport {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        PaperReportWire {
            realized: self.realized.0.to_string(),
            unrealized: self.unrealized.0.to_string(),
            fees: self.fees.0.to_string(),
            funding: self.funding.0.to_string(),
            net: self.net.0.to_string(),
            runtime_secs: self.runtime_secs,
            events_processed: self.events_processed,
            fills_emitted: self.fills_emitted,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PaperReport {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = PaperReportWire::deserialize(deserializer)?;
        let parse = |s: &str| -> Result<Notional, D::Error> {
            Decimal::from_str(s)
                .map(Notional)
                .map_err(serde::de::Error::custom)
        };
        Ok(PaperReport {
            realized: parse(&wire.realized)?,
            unrealized: parse(&wire.unrealized)?,
            fees: parse(&wire.fees)?,
            funding: parse(&wire.funding)?,
            net: parse(&wire.net)?,
            runtime_secs: wire.runtime_secs,
            events_processed: wire.events_processed,
            fills_emitted: wire.fills_emitted,
        })
    }
}
