//! TOML configuration schema for the dashboard.
//!
//! See `examples/config.toml` for a full annotated example.

use std::path::{Path, PathBuf};

use rust_decimal::Decimal;
use serde::Deserialize;

/// Top-level dashboard config.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct DashboardConfig {
    /// Account-wide settings (env, keys, leverage, etc).
    pub account: AccountConfig,
    /// Per-symbol bot specifications. Renamed to `bot` in TOML for the
    /// `[[bot]]` array-of-tables syntax.
    #[serde(rename = "bot", default)]
    pub bots: Vec<BotConfig>,
    /// Optional rotating SpreadScalp manager.
    #[serde(default)]
    pub scalp_rotation: Option<ScalpRotationConfig>,
}

/// Rotating SpreadScalp manager configuration.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct ScalpRotationConfig {
    /// Enable rotating scalp mode.
    #[serde(default)]
    pub enabled: bool,
    /// Number of active bots to keep running.
    #[serde(default = "scalp_rotation_default_slots")]
    pub slots: usize,
    /// How often to rescan volatility and rotate symbols.
    #[serde(default = "scalp_rotation_default_refresh_secs")]
    pub refresh_secs: u64,
    /// Quote asset suffix to include.
    #[serde(default = "scalp_rotation_default_quote_asset")]
    pub quote_asset: String,
    /// Minimum quote volume filter.
    #[serde(default)]
    pub min_quote_volume: Decimal,
    /// Optional allow-list. Empty means all matching quote assets.
    #[serde(default)]
    pub candidates: Vec<String>,
}

fn scalp_rotation_default_slots() -> usize {
    4
}
fn scalp_rotation_default_refresh_secs() -> u64 {
    300
}
fn scalp_rotation_default_quote_asset() -> String {
    "USDT".to_string()
}

/// Account-wide settings.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct AccountConfig {
    /// Binance environment: `"futures-testnet"` or `"futures-mainnet"`.
    pub env: String,
    /// Optional path to a key file (HMAC `key:secret` single-line, or
    /// Ed25519 PEM if `key_type = "ed25519"`).
    #[serde(default)]
    pub key_file: Option<PathBuf>,
    /// State directory shared across bots — each bot writes its
    /// snapshots under a subdir keyed by symbol.
    #[serde(default = "default_state_dir")]
    pub state_dir: PathBuf,
    /// Percent of account margin balance allocated to orders across all bots.
    /// Split evenly by bot count when a bot's strategy does not set `notional`.
    #[serde(default = "default_order_balance_pct")]
    pub order_balance_pct: Decimal,
    /// Multiplier applied to wallet balance before order sizing, typically leverage.
    #[serde(default = "default_margin_multiplier")]
    pub margin_multiplier: Decimal,
}

fn default_state_dir() -> PathBuf {
    PathBuf::from("./state")
}

fn default_order_balance_pct() -> Decimal {
    Decimal::new(2, 1)
}

fn default_margin_multiplier() -> Decimal {
    Decimal::ONE
}

/// Per-bot configuration.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct BotConfig {
    /// Binance-style symbol, e.g. `"BTCUSDT"`.
    pub symbol: String,
    /// Strategy id: one of `"static-grid"`, `"layered-grid"`,
    /// `"ladder-reentry"`, `"simple-gap"`, `"micro-mean-reversion"`,
    /// `"spread-scalp"`, `"avellaneda-stoikov"`, `"glft"`, `"top-of-book"`.
    pub strategy: String,
    /// StaticGrid params (only honored when `strategy = "static-grid"`).
    #[serde(default)]
    pub sg: Option<SgParams>,
    /// LayeredGrid params.
    #[serde(default)]
    pub lg: Option<LgParams>,
    /// LadderReentry params.
    #[serde(default)]
    pub ladder_reentry: Option<LadderReentryParams>,
    /// SimpleGap params.
    #[serde(default)]
    pub simple_gap: Option<SimpleGapParams>,
    /// MicroMeanReversion params.
    #[serde(default)]
    pub micro_mean_reversion: Option<MicroMeanReversionParams>,
    /// SpreadScalp params.
    #[serde(default)]
    pub spread_scalp: Option<SpreadScalpParams>,
}

/// StaticGrid configuration.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct SgParams {
    /// Fiat notional per order. Defaults to account-level balance percent split.
    #[serde(default)]
    pub notional: Option<Decimal>,
    /// Levels per side (total orders = `2 × levels`).
    #[serde(default = "sg_default_levels")]
    pub levels: u32,
    /// Inner spread from mid in bps.
    #[serde(default = "sg_default_inner")]
    pub inner_bps: u32,
    /// Step between consecutive levels on the same side, in bps.
    #[serde(default = "sg_default_step")]
    pub step_bps: u32,
    /// Adaptive scaler target fills/min. `0` disables.
    #[serde(default)]
    pub target_fills_per_min: Decimal,
    /// Adaptive scaler rolling window in seconds.
    #[serde(default = "sg_default_fpm_window")]
    pub fillrate_window_secs: u32,
    /// Adaptive scaler lower bound.
    #[serde(default = "sg_default_scale_min")]
    pub scale_min: Decimal,
    /// Adaptive scaler upper bound.
    #[serde(default = "sg_default_scale_max")]
    pub scale_max: Decimal,
    /// Enable inventory-driven asymmetric skew (default `true`).
    /// `false` = symmetric ladder regardless of position.
    #[serde(default = "sg_default_auto_skew")]
    pub auto_skew: bool,
}

fn sg_default_levels() -> u32 {
    3
}
fn sg_default_inner() -> u32 {
    3
}
fn sg_default_step() -> u32 {
    3
}
fn sg_default_fpm_window() -> u32 {
    60
}
fn sg_default_scale_min() -> Decimal {
    Decimal::from(1)
}
fn sg_default_scale_max() -> Decimal {
    Decimal::from(4)
}
fn sg_default_auto_skew() -> bool {
    true
}

/// LayeredGrid configuration.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct LgParams {
    /// Notional per order.
    #[serde(default)]
    pub notional: Option<Decimal>,
    /// Orders per side.
    #[serde(default = "lg_default_levels")]
    pub levels: u32,
    /// Spacing in bps.
    #[serde(default = "lg_default_bps")]
    pub bps: u32,
}

fn lg_default_levels() -> u32 {
    1
}
fn lg_default_bps() -> u32 {
    6
}

/// LadderReentry configuration.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct LadderReentryParams {
    /// Notional per order.
    #[serde(default)]
    pub notional: Option<Decimal>,
    /// Initial orders per side.
    #[serde(default = "ladder_reentry_default_levels")]
    pub levels: u32,
    /// Initial inner distance from mid, in bps.
    #[serde(default = "ladder_reentry_default_inner_bps")]
    pub inner_bps: u32,
    /// Initial spacing between levels, in bps.
    #[serde(default = "ladder_reentry_default_step_bps")]
    pub step_bps: u32,
    /// Opposite-side reentry distance from filled price, in bps.
    #[serde(default = "ladder_reentry_default_reentry_bps")]
    pub reentry_bps: u32,
    /// Same-side continuation distance from filled price, in bps.
    #[serde(default = "ladder_reentry_default_continuation_bps")]
    pub continuation_bps: u32,
}

fn ladder_reentry_default_levels() -> u32 {
    10
}
fn ladder_reentry_default_inner_bps() -> u32 {
    5
}
fn ladder_reentry_default_step_bps() -> u32 {
    1
}
fn ladder_reentry_default_reentry_bps() -> u32 {
    5
}
fn ladder_reentry_default_continuation_bps() -> u32 {
    11
}

/// SimpleGap configuration.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct SimpleGapParams {
    /// Notional per order.
    #[serde(default)]
    pub notional: Option<Decimal>,
    /// Fixed distance from mid, in bps.
    #[serde(default = "simple_gap_default_gap_bps")]
    pub gap_bps: u32,
}

fn simple_gap_default_gap_bps() -> u32 {
    4
}

/// MicroMeanReversion configuration.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct MicroMeanReversionParams {
    /// Notional per order.
    #[serde(default)]
    pub notional: Option<Decimal>,
    /// Trade distance from mid required before entering, in bps.
    #[serde(default = "mmr_default_trigger_bps")]
    pub trigger_bps: u32,
    /// Passive entry distance from mid, in bps.
    #[serde(default = "mmr_default_entry_bps")]
    pub entry_bps: u32,
    /// Exit distance from fill price, in bps.
    #[serde(default = "mmr_default_exit_bps")]
    pub exit_bps: u32,
    /// Maximum same-side entry quotes to keep open.
    #[serde(default = "mmr_default_max_open_entries")]
    pub max_open_entries: u32,
}

fn mmr_default_trigger_bps() -> u32 {
    10
}
fn mmr_default_entry_bps() -> u32 {
    2
}
fn mmr_default_exit_bps() -> u32 {
    6
}
fn mmr_default_max_open_entries() -> u32 {
    1
}

/// SpreadScalp configuration.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct SpreadScalpParams {
    /// Notional per order.
    #[serde(default)]
    pub notional: Option<Decimal>,
    /// Venue tick size. Optional when exchangeInfo has symbol filters.
    #[serde(default)]
    pub tick_size: Decimal,
    /// Minimum spread in bps required to quote.
    #[serde(default = "spread_scalp_default_min_spread_bps")]
    pub min_spread_bps: Decimal,
    /// Requote interval in ms.
    #[serde(default = "spread_scalp_default_requote_interval_ms")]
    pub requote_interval_ms: u64,
}

fn spread_scalp_default_min_spread_bps() -> Decimal {
    Decimal::from(5)
}
fn spread_scalp_default_requote_interval_ms() -> u64 {
    1000
}

/// Parse a TOML config file.
pub fn load(path: &Path) -> anyhow::Result<DashboardConfig> {
    let s = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("read {}: {e}", path.display()))?;
    toml::from_str(&s).map_err(|e| anyhow::anyhow!("parse {}: {e}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal() {
        let s = r#"
            [account]
            env = "futures-testnet"
            state_dir = "./state"

            [[bot]]
            symbol = "BTCUSDT"
            strategy = "static-grid"
            sg = { notional = 25, levels = 2, inner_bps = 3, step_bps = 2 }

            [[bot]]
            symbol = "ETHUSDT"
            strategy = "layered-grid"
            lg = { notional = 25, levels = 1, bps = 6 }

            [[bot]]
            symbol = "BNBUSDT"
            strategy = "simple-gap"
            simple_gap = { notional = 25, gap_bps = 4 }

            [[bot]]
            symbol = "SOLUSDT"
            strategy = "ladder-reentry"
            ladder_reentry = { notional = 25, levels = 10, inner_bps = 5, step_bps = 1, reentry_bps = 5, continuation_bps = 11 }

            [[bot]]
            symbol = "DOGEUSDT"
            strategy = "micro-mean-reversion"
            micro_mean_reversion = { notional = 25, trigger_bps = 10, entry_bps = 2, exit_bps = 6, max_open_entries = 1 }

            [[bot]]
            symbol = "XRPUSDT"
            strategy = "spread-scalp"
            spread_scalp = { notional = 25, min_spread_bps = 5, requote_interval_ms = 1000 }
        "#;
        let cfg: DashboardConfig = toml::from_str(s).unwrap();
        assert_eq!(cfg.bots.len(), 6);
        assert_eq!(cfg.bots[0].symbol, "BTCUSDT");
        assert_eq!(cfg.bots[0].strategy, "static-grid");
        assert_eq!(cfg.bots[1].strategy, "layered-grid");
        assert_eq!(cfg.bots[2].strategy, "simple-gap");
        assert_eq!(cfg.bots[3].strategy, "ladder-reentry");
        assert_eq!(cfg.bots[4].strategy, "micro-mean-reversion");
        assert_eq!(cfg.bots[5].strategy, "spread-scalp");
        let sg = cfg.bots[0].sg.as_ref().unwrap();
        assert_eq!(sg.levels, 2);
    }

    #[test]
    fn defaults_kick_in() {
        let s = r#"
            [account]
            env = "futures-testnet"

            [[bot]]
            symbol = "BTCUSDT"
            strategy = "static-grid"
            sg = {}
        "#;
        let cfg: DashboardConfig = toml::from_str(s).unwrap();
        let sg = cfg.bots[0].sg.as_ref().unwrap();
        assert_eq!(cfg.account.order_balance_pct, Decimal::new(2, 1));
        assert_eq!(cfg.account.margin_multiplier, Decimal::ONE);
        assert_eq!(sg.notional, None);
        assert_eq!(sg.levels, 3);
        assert_eq!(sg.inner_bps, 3);
        assert_eq!(sg.scale_max, Decimal::from(4));
    }
}
