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
    /// Optional rotating StaticGrid manager.
    #[serde(default)]
    pub static_grid_rotation: Option<ScalpRotationConfig>,
}

/// Rotating SpreadScalp manager configuration.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct ScalpRotationConfig {
    /// Enable rotating scalp mode.
    #[serde(default)]
    pub enabled: bool,
    /// Bot strategy template name to match in the `[[bot]]` list, e.g.
    /// `"spread-scalp"` or `"static-grid"`.
    #[serde(default = "scalp_rotation_default_strategy")]
    pub strategy: String,
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
fn scalp_rotation_default_strategy() -> String {
    "spread-scalp".to_string()
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
    /// Percent of account margin balance used as the per-bot peak position
    /// cap, split evenly by bot count. Per-bot max position USDT =
    /// `wallet × margin_multiplier × max_position_pct / 100 / bot_count`.
    /// Default `80` preserves legacy behavior (effectively 80% of wallet
    /// divided across bots).
    #[serde(default = "default_max_position_pct")]
    pub max_position_pct: Decimal,
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

fn default_max_position_pct() -> Decimal {
    Decimal::from(80)
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
    /// LiqFade params (only honored when `strategy = "liq-fade"`).
    #[serde(default)]
    pub liq_fade: Option<LiqFadeParams>,
    /// Hydra params (only honored when `strategy = "hydra"`).
    #[serde(default)]
    pub hydra: Option<HydraParams>,
}

/// LiqFade configuration — knobs match `LiqFadeConfig` 1:1 plus
/// `arm_window_secs` which sets the runner-side rolling buffer length.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct LiqFadeParams {
    /// Fiat notional per fade entry.
    #[serde(default)]
    pub notional: Option<Decimal>,
    /// Per-side liquidation USDT threshold to arm. `5_000_000` for
    /// BTC, smaller for alts.
    #[serde(default = "liq_default_arm_threshold")]
    pub arm_threshold_usdt: Decimal,
    /// Dominance ratio of light side / heavy side at arm.
    #[serde(default = "liq_default_arm_dominance")]
    pub arm_dominance: Decimal,
    /// Capitulation overshoot in bps past pre-liq mid.
    #[serde(default = "liq_default_capit_bps")]
    pub capitulation_overshoot_bps: u32,
    /// Fade-quote offset in bps deeper than dislocated touch.
    #[serde(default = "liq_default_fade_offset_bps")]
    pub fade_offset_bps: u32,
    /// TP target in bps of revert toward pre-liq mid.
    #[serde(default = "liq_default_revert_target_bps")]
    pub revert_target_bps: u32,
    /// Entry quote rest timeout (seconds).
    #[serde(default = "liq_default_entry_timeout_secs")]
    pub entry_timeout_secs: u32,
    /// Position time-stop (seconds).
    #[serde(default = "liq_default_position_timeout_secs")]
    pub position_timeout_secs: u32,
    /// Stop-loss in bps of position notional. `0` disables.
    #[serde(default)]
    pub stop_loss_bps: u32,
    /// Hard inventory cap in USDT notional. `0` falls back to the
    /// account-level cap.
    #[serde(default)]
    pub max_position_usdt: Decimal,
    /// Rolling-window length (seconds) for the runner-side liq buffer.
    /// Must be ≥ `entry_timeout_secs + position_timeout_secs`. Sets
    /// `RunnerConfig.liq_window_secs` for this bot.
    #[serde(default = "liq_default_window_secs")]
    pub window_secs: u32,
}

fn liq_default_arm_threshold() -> Decimal {
    Decimal::from(5_000_000u64)
}
fn liq_default_arm_dominance() -> Decimal {
    Decimal::from_str_exact("0.5").unwrap()
}
fn liq_default_capit_bps() -> u32 {
    15
}
fn liq_default_fade_offset_bps() -> u32 {
    5
}
fn liq_default_revert_target_bps() -> u32 {
    10
}
fn liq_default_entry_timeout_secs() -> u32 {
    30
}
fn liq_default_position_timeout_secs() -> u32 {
    120
}
fn liq_default_window_secs() -> u32 {
    180
}

/// Hydra configuration — knobs match `HydraConfig` 1:1.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct HydraParams {
    /// Distance from mid each straddle leg posts, in bps.
    #[serde(default = "hydra_default_entry_offset_bps")]
    pub entry_offset_bps: u32,
    /// Pyramid step in bps — favorable-drift band that triggers an add.
    #[serde(default = "hydra_default_pyramid_step_bps")]
    pub pyramid_step_bps: u32,
    /// Max pyramid adds. `0` disables the pyramid arm.
    #[serde(default = "hydra_default_pyramid_max_adds")]
    pub pyramid_max_adds: u32,
    /// DCA step in bps — adverse-drift band that triggers an add.
    #[serde(default = "hydra_default_dca_step_bps")]
    pub dca_step_bps: u32,
    /// Max DCA adds. `0` disables the DCA arm.
    #[serde(default = "hydra_default_dca_max_adds")]
    pub dca_max_adds: u32,
    /// Take-profit in bps from rolling `avg_entry`.
    #[serde(default = "hydra_default_tp_bps_from_avg")]
    pub tp_bps_from_avg: u32,
    /// Stop-loss in bps from FIRST-fill price (anchored, not rolling avg).
    #[serde(default = "hydra_default_sl_bps_from_first")]
    pub sl_bps_from_first: u32,
    /// Hard inventory cap in USDT notional. `0` falls back to the
    /// account-level cap.
    #[serde(default)]
    pub max_position_usdt: Decimal,
    /// Min elapsed time between adds (ms).
    #[serde(default = "hydra_default_add_cooldown_ms")]
    pub add_cooldown_ms: u64,
    /// Refresh the resting straddle this many seconds after it was
    /// placed. `0` disables. Default `60`.
    #[serde(default = "hydra_default_straddle_refresh_secs")]
    pub straddle_refresh_secs: u32,
    /// Refresh the straddle when mid has drifted this many bps from
    /// the anchor. `0` disables. Default `40`.
    #[serde(default = "hydra_default_straddle_drift_bps")]
    pub straddle_drift_bps: u32,
    /// Per-add multiplier for the pyramid arm (× `notional`).
    #[serde(default = "hydra_default_pyramid_size_mult")]
    pub pyramid_size_mult: Decimal,
    /// Per-add multiplier for the DCA arm (× `notional`).
    #[serde(default = "hydra_default_dca_size_mult")]
    pub dca_size_mult: Decimal,
}

fn hydra_default_entry_offset_bps() -> u32 {
    100
}
fn hydra_default_pyramid_step_bps() -> u32 {
    50
}
fn hydra_default_pyramid_max_adds() -> u32 {
    2
}
fn hydra_default_dca_step_bps() -> u32 {
    60
}
fn hydra_default_dca_max_adds() -> u32 {
    1
}
fn hydra_default_tp_bps_from_avg() -> u32 {
    30
}
fn hydra_default_sl_bps_from_first() -> u32 {
    60
}
fn hydra_default_add_cooldown_ms() -> u64 {
    1_000
}
fn hydra_default_straddle_refresh_secs() -> u32 {
    0
}
fn hydra_default_straddle_drift_bps() -> u32 {
    0
}
fn hydra_default_pyramid_size_mult() -> Decimal {
    Decimal::ONE
}
fn hydra_default_dca_size_mult() -> Decimal {
    Decimal::ONE
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
    /// Regime-tracker window (seconds). `0` disables regime gating;
    /// `auto_skew` then applies unconditionally. Non-zero
    /// suppresses skew during chop regimes and engages it during
    /// trending ones — auto-tunes the "skew helps for trending
    /// pairs, hurts for choppy pairs" tradeoff. Sensible default:
    /// `300` (5 minutes).
    #[serde(default)]
    pub regime_window_secs: u64,
    /// Drift threshold (bps) above which the regime classifier
    /// flags "trending". Default `10` bps over the chosen window.
    /// Only meaningful when `regime_window_secs > 0`.
    #[serde(default = "sg_default_regime_trend_threshold")]
    pub regime_trend_threshold_bps: u32,
    /// Directional-efficiency threshold for regime classification
    /// (Kaufman's efficiency ratio). Range `[0, 1]` — `0` (default)
    /// falls back to `regime_trend_threshold_bps`. Sensible value:
    /// `"0.3"`. Self-scales per symbol — preferred over the bps path.
    #[serde(default)]
    pub regime_efficiency_threshold: Decimal,
    /// Hard inventory cap in USDT notional. `0` (default) falls back
    /// to the account-level `max_position_usdt`. Add-side quotes are
    /// suppressed when `|position × mid| >= cap` so existing
    /// rest-orders can drain inventory.
    #[serde(default)]
    pub max_position_usdt: Decimal,
    /// Take-profit threshold in bps of position notional. `0`
    /// (default) disables. Same shape as the SpreadScalp knob.
    #[serde(default)]
    pub take_profit_bps: u32,
    /// Stop-loss threshold in bps of position notional. `0` (default)
    /// disables.
    #[serde(default)]
    pub stop_loss_bps: u32,
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
fn sg_default_regime_trend_threshold() -> u32 {
    10
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
    /// Hard inventory cap in USDT notional. `0` (default) falls back
    /// to the account-level `max_position_usdt`.
    #[serde(default)]
    pub max_position_usdt: Decimal,
    /// Take-profit threshold in bps of position notional. `0`
    /// (default) disables.
    #[serde(default)]
    pub take_profit_bps: u32,
    /// Stop-loss threshold in bps of position notional. `0` (default)
    /// disables.
    #[serde(default)]
    pub stop_loss_bps: u32,
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
    /// Max position in quote currency before switching to one-sided quoting.
    /// 0 = disabled.
    #[serde(default)]
    pub max_position_usdt: Decimal,
    /// Unrealized PnL threshold to trigger take-profit. When exceeded, the
    /// strategy fires an IOC on the reducing side at the opposing touch
    /// to close as taker. 0 = disabled.
    #[serde(default)]
    pub take_profit_usdt: Decimal,
    /// Per-side cooldown (ms) after a venue rejection before another
    /// rebuild is allowed. Default 2000 mirrors SG. 0 disables.
    #[serde(default = "spread_scalp_default_reject_cooldown_ms")]
    pub reject_cooldown_ms: u64,
    /// Per-side requote price tolerance in ticks. 0 = exact-price
    /// match required for skip; 1-2 absorbs micro-mid jitter. Default 1.
    #[serde(default = "spread_scalp_default_price_tolerance_ticks")]
    pub price_tolerance_ticks: u32,
    /// Take-profit threshold in bps of position notional (entry × qty).
    /// When non-zero, wins over `take_profit_usdt`. 0 = disabled.
    #[serde(default)]
    pub take_profit_bps: u32,
    /// Stop-loss threshold in bps of position notional. 0 = disabled.
    /// Fires an IOC at the opposing touch on every event.
    #[serde(default)]
    pub stop_loss_bps: u32,
    /// Adverse-selection window for dynamic min_spread widening, in ms.
    /// 0 = adverse tracker disabled (legacy fixed-threshold behaviour).
    #[serde(default)]
    pub adverse_window_ms: u64,
    /// EMA half-life in fills for the adverse-drift average.
    /// Only used when `adverse_window_ms > 0`.
    #[serde(default = "spread_scalp_default_adverse_half_life")]
    pub adverse_half_life_fills: u32,
    /// Adverse-drift threshold in bps. EMA above this widens
    /// `min_spread_bps` by `(ema - threshold)` capped at
    /// `adverse_max_widen_bps`. Only used when `adverse_window_ms > 0`.
    #[serde(default = "spread_scalp_default_adverse_threshold_bps")]
    pub adverse_threshold_bps: Decimal,
    /// Cap on the dynamic `min_spread_bps` surcharge in bps.
    /// Only used when `adverse_window_ms > 0`.
    #[serde(default = "spread_scalp_default_adverse_max_widen_bps")]
    pub adverse_max_widen_bps: u32,
    /// Keep the close-side passive quote alive even when book spread
    /// falls below `min_spread_bps`, so a held position can drain at
    /// maker fee once the cascade event that triggered the entry cools
    /// off. Default `true`. Set `false` for the legacy behaviour where
    /// BOTH sides cancel when targets are unavailable.
    #[serde(default = "spread_scalp_default_close_side_always_quotes")]
    pub close_side_always_quotes: bool,
}

fn spread_scalp_default_min_spread_bps() -> Decimal {
    Decimal::from(5)
}
fn spread_scalp_default_requote_interval_ms() -> u64 {
    1000
}
fn spread_scalp_default_reject_cooldown_ms() -> u64 {
    2000
}
fn spread_scalp_default_price_tolerance_ticks() -> u32 {
    1
}
fn spread_scalp_default_adverse_half_life() -> u32 {
    10
}
fn spread_scalp_default_adverse_threshold_bps() -> Decimal {
    Decimal::from(3)
}
fn spread_scalp_default_close_side_always_quotes() -> bool {
    true
}
fn spread_scalp_default_adverse_max_widen_bps() -> u32 {
    10
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
        assert_eq!(cfg.account.max_position_pct, Decimal::from(80));
        assert_eq!(sg.notional, None);
        assert_eq!(sg.levels, 3);
        assert_eq!(sg.inner_bps, 3);
        assert_eq!(sg.scale_max, Decimal::from(4));
    }
}
