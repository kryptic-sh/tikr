//! [`PaperReport`] — extends [`tikr_backtest::pnl::PnLReport`] with runtime stats.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::str::FromStr;
use tikr_core::{Decimal, Notional};
use tikr_risk::RiskState;

/// Current schema version of [`PaperReport`]'s serialized form.
///
/// Bumped any time the wire layout grows or changes semantics. The
/// [`crate::runner::run_with_resume`] entry point hard-fails on a mismatch.
pub const SCHEMA_VERSION: u32 = 1;

/// Aggregate P&L + runtime stats for a paper trading session.
///
/// Serializes as JSON for state snapshots (see [`crate::state::write_snapshot`]).
/// `Notional` fields are emitted as decimal strings (via [`rust_decimal`]'s
/// `Display`/`FromStr`) since `tikr-core` does not opt into `rust_decimal`'s
/// `serde` feature.
#[derive(Debug, Clone)]
pub struct PaperReport {
    /// Wire-format schema version. Always [`SCHEMA_VERSION`] for fresh reports.
    pub schema_version: u32,
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
    /// Wall-clock runtime in seconds (engine execution time). On resume,
    /// accumulates across incarnations. For per-time metrics on backtests
    /// (e.g. fills/min), prefer [`Self::sim_duration_secs`].
    pub runtime_secs: u64,
    /// Simulated-time span in seconds, derived from the first → last event
    /// timestamps. In live mode this tracks wall-clock; in backtest it
    /// tracks the data span (1h of recorded data → ~3600 here regardless
    /// of how fast we replayed). Accumulates across resumed incarnations.
    pub sim_duration_secs: u64,
    /// Total `MarketEvent` count processed by the runner.
    pub events_processed: u64,
    /// Total `Fill` count emitted by [`FillSim`][tikr_backtest::fill_sim::FillSim].
    pub fills_emitted: u64,
    /// Persisted risk-gate state, present when a [`tikr_risk::RiskGate`] was
    /// supplied to [`crate::runner::run_with_resume`]; `None` otherwise.
    pub risk_state: Option<RiskState>,
    /// Number of profit-skims executed. `0` when skim mode disabled (no
    /// [`crate::runner::SkimConfig`] supplied) or when realized PnL never
    /// crossed the skim threshold.
    pub skim_count: u64,
    /// Cumulative USDT moved from perp account to spot via skim.
    pub skim_total_usdt: Notional,
    /// Base-asset quantity accumulated via skim spot buys.
    pub base_stacked: Notional,
    /// Perp account value at end, marked to market on any open position.
    /// `budget + realized − fees − skim_total + unrealized`. Meaningful
    /// only when skim mode enabled.
    pub final_perp_balance: Notional,
    /// `base_stacked × last_mid`. Spot leg of the final account value.
    pub final_base_value: Notional,
    /// Symbol of base asset accumulated via skim (e.g. "BTC", "ETH").
    /// Empty when skim disabled. For cross-base aggregates in
    /// [`crate::multi`], may be `"MIXED"`.
    pub base_asset: String,
    /// Total Bid-side USDT notional crossed (sum of `price × size` for
    /// every Bid fill the runner observed). Useful for backtests to
    /// quantify deployed capital independent of NET / fees.
    pub buy_volume_usdt: Notional,
    /// Total Ask-side USDT notional crossed. See `buy_volume_usdt`.
    pub sell_volume_usdt: Notional,
    /// Peak absolute position value seen during the run, in USDT
    /// (`|size| × last_mid` sampled on every event). Shows how close
    /// the strategy came to its `max_position_usdt` cap.
    pub peak_position_usdt: Notional,
    /// Mean absolute position value over the run, in USDT (`|size| × mark`
    /// averaged over every sample point, same cadence as `peak_position_usdt`).
    /// Unlike the peak, this shows the strategy's *typical* inventory load —
    /// a lower mean at similar net means the algo carried less risk on
    /// average (e.g. inventory-skew shrinks this; a flat market keeps it low).
    pub mean_position_usdt: Notional,
    /// Count of fully-filled resting orders (`Fill.is_full == true`).
    pub full_fills: u64,
    /// Count of partial fills (`Fill.is_full == false`). `fills_emitted` =
    /// `full_fills + partial_fills`; comparing strategies by raw `fills_emitted`
    /// is misleading because larger orders fragment into more partials.
    pub partial_fills: u64,
    /// Number of forced liquidations triggered during the run. `0` when no
    /// [`crate::runner::RunnerConfig::liquidation`] model was configured, or
    /// when the mark never breached the position's liquidation price. A
    /// non-zero value means the strategy blew through its margin — the
    /// realized loss is already folded into `realized` / `net`.
    pub liquidations: u64,
}

// --- serde wire format ---------------------------------------------------

#[derive(Serialize, Deserialize)]
struct PaperReportWire {
    schema_version: u32,
    realized: String,
    unrealized: String,
    fees: String,
    funding: String,
    net: String,
    runtime_secs: u64,
    #[serde(default)]
    sim_duration_secs: u64,
    events_processed: u64,
    fills_emitted: u64,
    #[serde(default)]
    risk_state: Option<RiskState>,
    #[serde(default)]
    skim_count: u64,
    #[serde(default)]
    skim_total_usdt: String,
    #[serde(default)]
    base_stacked: String,
    #[serde(default)]
    final_perp_balance: String,
    #[serde(default)]
    final_base_value: String,
    #[serde(default)]
    base_asset: String,
    #[serde(default)]
    buy_volume_usdt: String,
    #[serde(default)]
    sell_volume_usdt: String,
    #[serde(default)]
    peak_position_usdt: String,
    #[serde(default)]
    mean_position_usdt: String,
    #[serde(default)]
    full_fills: u64,
    #[serde(default)]
    partial_fills: u64,
    #[serde(default)]
    liquidations: u64,
}

impl Serialize for PaperReport {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        PaperReportWire {
            schema_version: self.schema_version,
            realized: self.realized.0.to_string(),
            unrealized: self.unrealized.0.to_string(),
            fees: self.fees.0.to_string(),
            funding: self.funding.0.to_string(),
            net: self.net.0.to_string(),
            runtime_secs: self.runtime_secs,
            sim_duration_secs: self.sim_duration_secs,
            events_processed: self.events_processed,
            fills_emitted: self.fills_emitted,
            risk_state: self.risk_state.clone(),
            skim_count: self.skim_count,
            skim_total_usdt: self.skim_total_usdt.0.to_string(),
            base_stacked: self.base_stacked.0.to_string(),
            final_perp_balance: self.final_perp_balance.0.to_string(),
            final_base_value: self.final_base_value.0.to_string(),
            base_asset: self.base_asset.clone(),
            buy_volume_usdt: self.buy_volume_usdt.0.to_string(),
            sell_volume_usdt: self.sell_volume_usdt.0.to_string(),
            peak_position_usdt: self.peak_position_usdt.0.to_string(),
            mean_position_usdt: self.mean_position_usdt.0.to_string(),
            full_fills: self.full_fills,
            partial_fills: self.partial_fills,
            liquidations: self.liquidations,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PaperReport {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = PaperReportWire::deserialize(deserializer)?;
        if wire.schema_version != SCHEMA_VERSION {
            return Err(serde::de::Error::custom(format!(
                "unsupported PaperReport schema_version {}; tikr-paper supports {}",
                wire.schema_version, SCHEMA_VERSION
            )));
        }
        let parse = |s: &str| -> Result<Notional, D::Error> {
            Decimal::from_str(s)
                .map(Notional)
                .map_err(serde::de::Error::custom)
        };
        Ok(PaperReport {
            schema_version: wire.schema_version,
            realized: parse(&wire.realized)?,
            unrealized: parse(&wire.unrealized)?,
            fees: parse(&wire.fees)?,
            funding: parse(&wire.funding)?,
            net: parse(&wire.net)?,
            runtime_secs: wire.runtime_secs,
            sim_duration_secs: wire.sim_duration_secs,
            events_processed: wire.events_processed,
            fills_emitted: wire.fills_emitted,
            risk_state: wire.risk_state,
            skim_count: wire.skim_count,
            skim_total_usdt: if wire.skim_total_usdt.is_empty() {
                Notional(Decimal::ZERO)
            } else {
                parse(&wire.skim_total_usdt)?
            },
            base_stacked: if wire.base_stacked.is_empty() {
                Notional(Decimal::ZERO)
            } else {
                parse(&wire.base_stacked)?
            },
            final_perp_balance: if wire.final_perp_balance.is_empty() {
                Notional(Decimal::ZERO)
            } else {
                parse(&wire.final_perp_balance)?
            },
            final_base_value: if wire.final_base_value.is_empty() {
                Notional(Decimal::ZERO)
            } else {
                parse(&wire.final_base_value)?
            },
            base_asset: wire.base_asset,
            buy_volume_usdt: if wire.buy_volume_usdt.is_empty() {
                Notional(Decimal::ZERO)
            } else {
                parse(&wire.buy_volume_usdt)?
            },
            sell_volume_usdt: if wire.sell_volume_usdt.is_empty() {
                Notional(Decimal::ZERO)
            } else {
                parse(&wire.sell_volume_usdt)?
            },
            peak_position_usdt: if wire.peak_position_usdt.is_empty() {
                Notional(Decimal::ZERO)
            } else {
                parse(&wire.peak_position_usdt)?
            },
            mean_position_usdt: if wire.mean_position_usdt.is_empty() {
                Notional(Decimal::ZERO)
            } else {
                parse(&wire.mean_position_usdt)?
            },
            full_fills: wire.full_fills,
            partial_fills: wire.partial_fills,
            liquidations: wire.liquidations,
        })
    }
}
