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
    /// Optional unified auto-rotation manager. Replaces the old `[wave_auto]`
    /// and `[tide_auto]` sections. Selects symbols by `score` mode and runs
    /// the configured `strategy` on the top N qualifiers.
    #[serde(default)]
    pub rampage: Option<RampageConfig>,
    /// Optional MEXC spot accumulator (bagboy). Places a single
    /// resting LIMIT BUY at best_bid for the configured symbol,
    /// refills on fill, refreshes when book moves. Pure accumulator —
    /// no sells, no closes. Independent of the Binance bots.
    #[serde(default)]
    pub bagboy: Option<BagboyConfig>,
}

/// Bagboy = MEXC spot accumulator. Maintains 1 limit BUY at best_bid
/// for the configured symbol; refills on fill or book move.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct BagboyConfig {
    #[serde(default)]
    pub enabled: bool,
    /// MEXC spot symbol, e.g. `"NAVUSDT"`.
    pub symbol: String,
    /// Quote-currency budget per order in USDT. Bot auto-bumps to the
    /// venue's min_notional if this is too small.
    #[serde(default = "bagboy_default_usdt_per_order")]
    pub usdt_per_order: Decimal,
    /// Hard cap on total USDT spent. `None` = no cap. Bot stops
    /// placing new orders when cumulative_spent_usdt ≥ cap.
    #[serde(default)]
    pub max_total_usdt: Option<Decimal>,
    /// Hard cap on total base asset accumulated (e.g. NAV count).
    /// `None` = no cap.
    #[serde(default)]
    pub max_total_base: Option<Decimal>,
    /// Book/order poll interval in ms. Default `500`. With MEXC's
    /// 20-req/s public limit, 200-500ms is safe.
    #[serde(default = "bagboy_default_poll_ms")]
    pub poll_interval_ms: u64,
    /// Number of laddered BUY orders. `1` = single resting order at
    /// best_bid (legacy). `N > 1` places N orders: at best_bid,
    /// best_bid − step, best_bid − 2×step, ... best_bid − (N−1)×step.
    /// Catches deeper bids without re-emitting on every book tick.
    /// Default `1`.
    #[serde(default = "bagboy_default_ladder_levels")]
    pub ladder_levels: u32,
    /// Spacing between ladder levels in bps of best_bid (snapped to
    /// tick). `0` = legacy 1-tick spacing. Default `5` bps.
    #[serde(default = "bagboy_default_ladder_step_bps")]
    pub ladder_step_bps: u32,
}

fn bagboy_default_usdt_per_order() -> Decimal {
    Decimal::new(1, 0) // $1
}
fn bagboy_default_poll_ms() -> u64 {
    500
}
fn bagboy_default_ladder_levels() -> u32 {
    1
}
fn bagboy_default_ladder_step_bps() -> u32 {
    5
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
    /// Minimum `tick_bps = tick_size / price × 10000` filter. Symbols
    /// below this threshold are excluded before volatility ranking.
    /// `0` (default) = filter disabled. `6` recommended to ensure each
    /// round-trip clears USDT-M maker fees (~3.6 bps BNB-discounted RT)
    /// with edge to spare.
    #[serde(default)]
    pub min_tick_bps: Decimal,
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
    /// Percent of wallet balance each bot allocates to orders. PER-BOT,
    /// NOT split across bots — `1` means every bot orders 1% of wallet
    /// (mirrors `max_position_pct`). Applied when a bot's strategy does
    /// not set `notional`. Strictly wallet-relative — leverage does NOT
    /// scale this; set `leverage` separately for Binance-side margin
    /// configuration.
    #[serde(default = "default_order_balance_pct")]
    pub order_balance_pct: Decimal,
    /// Percent of wallet balance used as the PER-BOT peak position
    /// cap. NOT split across bots — `100` means each bot can hold up
    /// to 100% of wallet notional. Total risk is capped by Binance
    /// margin engine + leverage, not by per-bot sum.
    /// Default `80`.
    #[serde(default = "default_max_position_pct")]
    pub max_position_pct: Decimal,
    /// BNB-refill trigger in USDT-equivalent. When BNB-pays-fees is
    /// enabled on the account AND `bnb_refill_enabled = true`, the
    /// refill task tops up BNB whenever
    /// `bnb_balance × bnb_price < bnb_min_balance_usdt`. Default `$1`.
    #[serde(default = "default_bnb_min_balance_usdt")]
    pub bnb_min_balance_usdt: Decimal,
    /// BNB-refill target in USDT-equivalent. Refill buys enough BNB
    /// to bring the USDT-value up to this level. Default `$50`.
    #[serde(default = "default_bnb_target_balance_usdt")]
    pub bnb_target_balance_usdt: Decimal,
    /// Master switch for BNB auto-refill. Defaults `true`. Has no
    /// effect unless BNB-pays-fees is also enabled on the Binance
    /// account (auto-detected via `GET /fapi/v1/feeBurn`).
    #[serde(default = "default_bnb_refill_enabled")]
    pub bnb_refill_enabled: bool,
    /// Per-symbol Binance Futures leverage. Sent via
    /// `POST /fapi/v1/leverage` at startup for each bot's symbol.
    /// Default `1` = no leverage. Independent from sizing —
    /// `order_balance_pct` + `max_position_pct` are wallet-relative,
    /// not margin-relative. Leverage controls liquidation distance
    /// and initial margin requirement on Binance's side; sizing is
    /// untouched.
    #[serde(default = "default_leverage")]
    pub leverage: u32,
    /// Margin asset for the wallet balance poller + TUI display.
    /// `"USDT"` (default) for USDT-M perps, `"USDC"` for USDC-M.
    /// When `tide_auto.quote_asset` is set, that takes
    /// precedence; this field covers the fixed-bot-list case.
    #[serde(default = "default_account_asset")]
    pub asset: String,
    /// Inventory-aware order-size boost: extra size at full inventory
    /// (|position| == per-bot cap), as a percent of the base order size.
    /// `0` (default) disables. `100` ≈ up to 2× base size when maxed. Scales
    /// only the *inventory-reducing* side (short → buys, long → sells), so the
    /// book leans harder toward flattening as inventory builds. Applies to
    /// every strategy (runner-side, like the position cap).
    #[serde(default = "default_inventory_boost_pct")]
    pub inventory_boost_pct: Decimal,
    /// Curve exponent on the inventory ratio (|pos|/cap, clamped `0..=1`) used
    /// by `inventory_boost_pct`. `1` (default) = linear; `>1` = slow start
    /// then steep (boost concentrated near the cap); `<1` = fast early ramp.
    #[serde(default = "default_inventory_boost_curve")]
    pub inventory_boost_curve: Decimal,
}

impl AccountConfig {
    /// Build the runner-side inventory boost config, or `None` when the boost
    /// percent is non-positive (feature off).
    pub fn inventory_boost(&self) -> Option<tikr_paper::InventoryBoostConfig> {
        if self.inventory_boost_pct > Decimal::ZERO {
            Some(tikr_paper::InventoryBoostConfig {
                max_boost_pct: self.inventory_boost_pct,
                curve_exponent: self.inventory_boost_curve,
            })
        } else {
            None
        }
    }
}

fn default_state_dir() -> PathBuf {
    PathBuf::from("./state")
}

fn default_bnb_min_balance_usdt() -> Decimal {
    Decimal::ONE
}
fn default_bnb_target_balance_usdt() -> Decimal {
    Decimal::from(50)
}
fn default_bnb_refill_enabled() -> bool {
    true
}

fn default_order_balance_pct() -> Decimal {
    Decimal::new(2, 1)
}
fn default_account_asset() -> String {
    "USDT".to_string()
}
fn default_inventory_boost_pct() -> Decimal {
    Decimal::ZERO
}
fn default_inventory_boost_curve() -> Decimal {
    Decimal::ONE
}

fn default_leverage() -> u32 {
    1
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
    /// Tide params (only honored when `strategy = "tide"`).
    #[serde(default)]
    pub tide: Option<TideParams>,
    /// Joker params (only honored when `strategy = "joker"`).
    #[serde(default)]
    pub joker: Option<JokerParams>,
    /// RSI-MR params (only honored when `strategy = "rsi-mr"`).
    #[serde(default)]
    pub rsi_mr: Option<RsiMrParams>,
    /// Wave params (only honored when `strategy = "wave"`).
    #[serde(default)]
    pub wave: Option<WaveParams>,
    /// Mantis params (only honored when `strategy = "mantis"`).
    #[serde(default)]
    pub mantis: Option<MantisParams>,
    /// Volley params (only honored when `strategy = "volley"`).
    #[serde(default)]
    pub volley: Option<VolleyParams>,
}

/// Wave — frozen fixed-step lattice with round-trip refill (pure form).
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaveParams {
    /// Per-order notional. Account-derived if unset.
    #[serde(default)]
    pub notional: Option<Decimal>,
    /// Orders per side. Default 12.
    #[serde(default = "wave_default_levels")]
    pub levels: u32,
    /// Level spacing in bps — gap between consecutive levels. `0` = 1-tick.
    #[serde(default)]
    pub steps_bps: u32,
    /// Inner dead-zone in STEPS (mid → first order = `steps_inner × step`).
    /// `0` (default) = origins at the touch.
    #[serde(default)]
    pub steps_inner: u32,
    /// Completed round-trips needed to trigger a refill (≥ N bids AND ≥ N asks
    /// drained). A whole side emptying refills regardless. Default 1.
    #[serde(default = "wave_default_round_trips")]
    pub round_trips: u32,
}

fn wave_default_round_trips() -> u32 {
    1
}

fn wave_default_levels() -> u32 {
    12
}

/// Mantis — symmetric touch scalper; rests a bid+ask at the touch.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct MantisParams {
    /// Per-order notional. Account-derived if unset.
    #[serde(default)]
    pub notional: Option<Decimal>,
    /// Minimum book spread (bps) required to quote. Default 1.
    #[serde(default = "mantis_default_min_spread_bps")]
    pub min_spread_bps: Decimal,
    /// Tick offset from touch. `0` = join (default), `-1` = inside/outbid,
    /// `+1` = one tick outside.
    #[serde(default)]
    pub tick_offset: i32,
    /// Ticks price must move from the last fill before reopening a pair.
    /// Default 1.
    #[serde(default = "mantis_default_reopen_distance_ticks")]
    pub reopen_distance_ticks: u32,
}

fn mantis_default_min_spread_bps() -> Decimal {
    Decimal::ONE
}

fn mantis_default_reopen_distance_ticks() -> u32 {
    1
}

/// RSI mean-reversion + KER regime gate, long-only.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct RsiMrParams {
    /// Per-order notional in quote currency. Account-derived if unset.
    #[serde(default)]
    pub notional: Option<Decimal>,
    /// Bar interval in seconds. Default 60 (1-minute).
    #[serde(default = "rsi_mr_default_bar_interval_secs")]
    pub bar_interval_secs: u64,
    /// Closed-bar buffer size. Default 200.
    #[serde(default = "rsi_mr_default_max_bars")]
    pub max_bars: usize,
    /// RSI period. Default 14.
    #[serde(default = "rsi_mr_default_rsi_period")]
    pub rsi_period: u32,
    /// Enter long when RSI < threshold. Default 25.
    #[serde(default = "rsi_mr_default_rsi_buy_threshold")]
    pub rsi_buy_threshold: u32,
    /// Exit long when RSI > threshold. Default 50.
    #[serde(default = "rsi_mr_default_rsi_exit_threshold")]
    pub rsi_exit_threshold: u32,
    /// Kaufman Efficiency Ratio period. Default 20.
    #[serde(default = "rsi_mr_default_ker_period")]
    pub ker_period: u32,
    /// Skip entry when KER > value (trending). Default `"0.4"`.
    #[serde(default = "rsi_mr_default_ker_max_trending")]
    pub ker_max_trending: Decimal,
    /// Volume z-score lookback. Default 20.
    #[serde(default = "rsi_mr_default_vol_zscore_period")]
    pub vol_zscore_period: u32,
    /// Skip entry when volume z-score < value. Default `"1.5"`.
    #[serde(default = "rsi_mr_default_vol_zscore_min")]
    pub vol_zscore_min: Decimal,
    /// ATR period. Default 14.
    #[serde(default = "rsi_mr_default_atr_period")]
    pub atr_period: u32,
    /// SL distance in ATR multiples. Default `"2"`.
    #[serde(default = "rsi_mr_default_atr_sl_mult")]
    pub atr_sl_mult: Decimal,
    /// TP distance in ATR multiples. Default `"3"`.
    #[serde(default = "rsi_mr_default_atr_tp_mult")]
    pub atr_tp_mult: Decimal,
    /// Max bars to hold before timeout IOC. Default 60 (= 1h on 1m bars).
    #[serde(default = "rsi_mr_default_max_hold_bars")]
    pub max_hold_bars: u32,
}

fn rsi_mr_default_bar_interval_secs() -> u64 {
    60
}
fn rsi_mr_default_max_bars() -> usize {
    200
}
fn rsi_mr_default_rsi_period() -> u32 {
    14
}
fn rsi_mr_default_rsi_buy_threshold() -> u32 {
    25
}
fn rsi_mr_default_rsi_exit_threshold() -> u32 {
    50
}
fn rsi_mr_default_ker_period() -> u32 {
    20
}
fn rsi_mr_default_ker_max_trending() -> Decimal {
    Decimal::new(4, 1)
}
fn rsi_mr_default_vol_zscore_period() -> u32 {
    20
}
fn rsi_mr_default_vol_zscore_min() -> Decimal {
    Decimal::new(15, 1)
}
fn rsi_mr_default_atr_period() -> u32 {
    14
}
fn rsi_mr_default_atr_sl_mult() -> Decimal {
    Decimal::from(2)
}
fn rsi_mr_default_atr_tp_mult() -> Decimal {
    Decimal::from(3)
}
fn rsi_mr_default_max_hold_bars() -> u32 {
    60
}

/// Joker — join touch, dedupe by exact price, never cancel.
/// `step_size` / `tick_size` / `min_notional` come from venue
/// exchangeInfo. Nothing else to tune.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct JokerParams {
    /// Per-order notional in quote currency. When omitted, the
    /// account-derived default applies
    /// (`account.order_balance_pct × wallet`, per-bot).
    #[serde(default)]
    pub notional: Option<Decimal>,
    /// Cancel any open order older than this many seconds since its
    /// emit. Forces the joker to refresh its book — stale orders that
    /// sat through book moves get reaped instead of pinning margin.
    /// `0` (default) disables the age sweep.
    #[serde(default)]
    pub max_order_age_secs: u64,
    /// Tick offset from best. `-1` improve (post in front), `0` join,
    /// `1+` lag behind by N ticks. Default `0`.
    #[serde(default)]
    pub order_tick_offset: i32,
    /// Skip emit if a same-side resting order sits within this many
    /// ticks of target. `0` = exact-price dedupe. Default `5`.
    #[serde(default = "joker_default_order_tick_tolerance")]
    pub order_tick_tolerance: u32,
}

fn joker_default_order_tick_tolerance() -> u32 {
    5
}

/// Volley — timed batched book-flooding. `step_size` / `tick_size` /
/// `min_notional` come from venue exchangeInfo.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct VolleyParams {
    /// Per-order notional in quote currency. Account-derived if unset.
    #[serde(default)]
    pub notional: Option<Decimal>,
    /// Orders per side per volley. Default 10.
    #[serde(default = "volley_default_levels")]
    pub levels: u32,
    /// Fire a fresh volley (cancel all + re-place) this often, in seconds.
    /// `0` = every event. Default 1.
    #[serde(default = "volley_default_interval_secs")]
    pub interval_secs: u32,
    /// Tick gap between consecutive orders on a side. Default 1.
    #[serde(default = "volley_default_step_ticks")]
    pub step_ticks: u32,
    /// Dead-zone in ticks: first order on each side sits this far off the touch.
    /// Default 5.
    #[serde(default = "volley_default_inner_ticks")]
    pub inner_ticks: u32,
}

fn volley_default_levels() -> u32 {
    10
}
fn volley_default_interval_secs() -> u32 {
    1
}
fn volley_default_step_ticks() -> u32 {
    1
}
fn volley_default_inner_ticks() -> u32 {
    5
}

/// Tide — grid-only at-touch MM with optional N-level depth.
/// `step_size` / `tick_size` / `min_notional` come from venue
/// exchangeInfo.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct TideParams {
    /// Per-order notional in USDT. When omitted, the account-derived
    /// default applies (`account.order_balance_pct × wallet`, per-bot).
    #[serde(default)]
    pub notional: Option<Decimal>,
    /// Grid depth per side. `1` = single order at touch (classic),
    /// `N > 1` places `N` orders per side spaced `grid_step_bps` apart
    /// starting at touch. Default `1`. With N=12 the bot defends
    /// against ~10-tick adverse price jumps. Inventory cap scales:
    /// max per-side position = N × notional_per_order.
    #[serde(default = "tide_default_grid_levels")]
    pub grid_levels: u32,
    /// Lattice geometry in bps of mid — drives BOTH the inner self-spread
    /// (min gap between top bid and top ask) AND the spacing between grid
    /// levels. Snapped to tick. `0` (default) = at-touch with 1-tick spacing.
    #[serde(default)]
    pub step_bps: u32,
    /// Cancel BID/ASK orders that drift outside the active lattice
    /// window. Default `true`. `false` = never cancel — orders rest
    /// forever, may pin margin.
    #[serde(default = "tide_prune_default")]
    pub prune_stragglers: bool,
    /// Recenter threshold in bps: when the mid drifts more than this from the
    /// frozen lattice center, re-anchor the grid around the current touch.
    /// `0` (default) = never recenter. Set wide (only on real range shifts).
    #[serde(default)]
    pub recenter_bps: u32,
    /// Time-based recenter interval in seconds: every N seconds, re-anchor the
    /// grid around the current touch. `0` (default) = off.
    #[serde(default)]
    pub recenter_secs: u32,
    /// Hold the top order `inner_steps × step` from mid (skip inner rungs).
    /// `0` (default) = legacy self-spread. `2` = first order 2× step from mid.
    #[serde(default)]
    pub inner_steps: u32,
    /// Chase price both ways (bids follow up, asks follow down). `false`
    /// (default) = one-sided/frozen.
    #[serde(default)]
    pub chase: bool,
    /// Chase the reducing side only to cost basis (asks→avg+gap when long,
    /// bids→avg−gap when short). Never sells below cost. `false` (default) = off.
    #[serde(default)]
    pub chase_to_avg: bool,
    /// Idle re-lattice timeout in seconds: when the lattice has gone this long
    /// without a fill, re-freeze the grid around the current touch. `300`
    /// (default).
    #[serde(default = "tide_default_relattice_timeout_secs")]
    pub relattice_timeout_secs: u32,
}

fn tide_prune_default() -> bool {
    true
}

fn tide_default_relattice_timeout_secs() -> u32 {
    300
}

fn tide_default_grid_levels() -> u32 {
    1
}

fn tide_auto_default_min_tick_bps() -> Decimal {
    Decimal::from(6)
}
fn tide_auto_default_min_volume_usdt() -> Decimal {
    Decimal::from(20_000_000)
}
fn tide_auto_default_recheck_interval_secs() -> u64 {
    60
}
fn tide_auto_default_step_bps() -> u32 {
    10
}
fn tide_auto_default_grid_levels() -> u32 {
    12
}

fn wave_auto_default_defer_underwater() -> bool {
    true
}

fn wave_auto_default_candle_count() -> u32 {
    5
}
fn wave_auto_default_top_n() -> usize {
    5
}
fn wave_auto_default_quote_asset() -> String {
    "USDC".to_string()
}
fn wave_auto_default_step_bps() -> u32 {
    10
}
fn wave_auto_default_inner_steps() -> u32 {
    2
}

/// Scoring mode for the rampage auto-rotation manager.
///
/// Uses an adjacently-tagged representation so the `toml` crate can parse it
/// correctly — internally-tagged enums are not supported by the `toml` crate
/// when the variant contains non-string fields.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "mode", content = "params", rename_all = "snake_case")]
pub enum ScoreMode {
    /// Score by average 1-minute candle height (wicks included) as a percent.
    CandleHeight {
        #[serde(default = "wave_auto_default_candle_count")]
        candle_count: u32,
        #[serde(default)]
        min_candle_pct: Decimal,
    },
    /// Score by tick_bps (`tick_size / price × 10000`) from the exchange info
    /// — no extra HTTP calls.
    TickBps {
        #[serde(default = "tide_auto_default_min_tick_bps")]
        min_tick_bps: Decimal,
    },
}

/// Strategy spawned by the rampage manager for each qualifying symbol.
///
/// Uses an adjacently-tagged representation for `toml` crate compatibility.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", content = "params", rename_all = "snake_case")]
pub enum RampageStrategy {
    /// Spawn a Wave (frozen-lattice) bot.
    Wave {
        #[serde(default = "wave_default_levels")]
        levels: u32,
        #[serde(default = "wave_auto_default_step_bps")]
        steps_bps: u32,
        #[serde(default = "wave_auto_default_inner_steps")]
        steps_inner: u32,
        #[serde(default = "wave_default_round_trips")]
        round_trips: u32,
    },
    /// Spawn a Tide (at-touch grid) bot.
    Tide {
        #[serde(default = "tide_auto_default_grid_levels")]
        grid_levels: u32,
        #[serde(default = "tide_auto_default_step_bps")]
        step_bps: u32,
        #[serde(default)]
        inner_steps: u32,
        #[serde(default)]
        chase: bool,
        #[serde(default)]
        chase_to_avg: bool,
        /// Trim straggler orders so the resting ladder stays `grid_levels` wide
        /// (two-sided window prune). Default `true`.
        #[serde(default = "tide_prune_default")]
        prune_stragglers: bool,
    },
}

/// `[rampage]` — unified auto-rotation manager. Replaces both `[wave_auto]`
/// and `[tide_auto]`. Discovers qualifying symbols via the configured `score`
/// mode, takes the top `top_n`, and runs the configured `strategy` (Wave or
/// Tide) on each. Preserves all wave_auto features: orphan adoption,
/// defer_underwater, retired-tab GC, graceful shutdown that leaves positions.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct RampageConfig {
    /// Master switch.
    #[serde(default)]
    pub enabled: bool,
    /// Minimum 24h quote volume in USDT for a symbol to qualify.
    #[serde(default = "tide_auto_default_min_volume_usdt")]
    pub min_volume_usdt: Decimal,
    /// How often to re-discover + re-rank (seconds). Clamped to ≥ 10s.
    #[serde(default = "tide_auto_default_recheck_interval_secs")]
    pub recheck_interval_secs: u64,
    /// Quote asset to filter discovery on. Default `"USDC"`.
    #[serde(default = "wave_auto_default_quote_asset")]
    pub quote_asset: String,
    /// How many top-scored symbols to run concurrently. Default `5`.
    #[serde(default = "wave_auto_default_top_n")]
    pub top_n: usize,
    /// When `true` (default), do NOT rotate a symbol out while its bot's NET PnL
    /// (`realized + unrealized − fees`) is a loss LARGER than the acceptable
    /// `rotate_loss_pct` of total wallet. The bot keeps running until its NET is
    /// green or the loss shrinks within tolerance. When `false`, rotate
    /// regardless of NET.
    #[serde(default = "wave_auto_default_defer_underwater")]
    pub defer_underwater: bool,
    /// Acceptable NET loss when rotating a symbol out, as a PERCENT of total
    /// wallet balance (futures wallet + BNB value). A bot rotates only when its
    /// NET (`realized + unrealized − fees`) is green, OR its NET loss is within
    /// this percent of the wallet; a larger NET loss defers rotation (the bot
    /// keeps working the bag off so rotation never crystallizes more than this).
    /// Only consulted when `defer_underwater` is on. Default `1` (= 1% of wallet).
    #[serde(default = "rampage_default_rotate_loss_pct")]
    pub rotate_loss_pct: Decimal,
    /// Big-bag hold: when a bot that would otherwise rotate out is sitting on a
    /// POSITIVE unrealized PnL and its gross position notional (`|position| ×
    /// mark`) is at least this PERCENT of total wallet, defer rotation and let
    /// the bot work the bag down instead of market-closing a large profitable
    /// position. Gates on unrealized only (independent of NET sign). Only
    /// consulted when `defer_underwater` is on. `0` = disabled. Default `25`.
    #[serde(default = "rampage_default_big_bag_pct")]
    pub big_bag_pct: Decimal,
    /// Optional explicit symbol allowlist. When non-empty, only these symbols
    /// are considered (volume + score filters still apply).
    #[serde(default)]
    pub symbols_allowlist: Vec<String>,
    /// Scoring mode used to rank candidate symbols.
    pub score: ScoreMode,
    /// Strategy to spawn on each qualifying symbol.
    pub strategy: RampageStrategy,
}

fn rampage_default_rotate_loss_pct() -> Decimal {
    Decimal::from(1)
}

fn rampage_default_big_bag_pct() -> Decimal {
    Decimal::from(25)
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
    /// Fiat notional per order. Defaults to account-level balance percent (per-bot).
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
    /// Dislocation-confirmation gate. When `true`, an Ask-side entry only
    /// fires when `trade.price >= best_ask` and a Bid-side entry only fires
    /// when `trade.price <= best_bid`. `false` = legacy behaviour.
    #[serde(default = "mmr_default_confirm_touch")]
    pub confirm_touch: bool,
    /// TP relaxation trigger in bps from avg_entry. When adverse move exceeds
    /// this, reprice the resting exit to avg_entry +/- floor_bps (maker-safe,
    /// never crossing). `0` disables.
    #[serde(default = "mmr_default_tp_relax_trigger_bps")]
    pub tp_relax_trigger_bps: u32,
    /// TP relaxation floor in bps above avg_entry.
    #[serde(default = "mmr_default_tp_relax_floor_bps")]
    pub tp_relax_floor_bps: u32,
    /// Adverse-side entry cooldown in bps from avg_entry.
    /// Suppresses same-side adds when adverse_bps >= this threshold. `0` disables.
    #[serde(default = "mmr_default_add_block_bps")]
    pub add_block_bps: u32,
    /// Entry velocity throttle: minimum milliseconds between same-side entry
    /// posts. Stops a live trade-print burst firing many entries in a few
    /// hundred ms. `0` disables.
    #[serde(default = "mmr_default_entry_cooldown_ms")]
    pub entry_cooldown_ms: u64,
    /// Entry-price anchor. `false` (default) prices entries `entry_bps` from
    /// mid; `true` prices off the live touch (improve the near best by
    /// `entry_bps`) — fills better and avoids the stale-mid `-5022` cross.
    #[serde(default = "mmr_default_entry_from_touch")]
    pub entry_from_touch: bool,
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
fn mmr_default_confirm_touch() -> bool {
    true
}
fn mmr_default_tp_relax_trigger_bps() -> u32 {
    20
}
fn mmr_default_tp_relax_floor_bps() -> u32 {
    3
}
fn mmr_default_add_block_bps() -> u32 {
    15
}
fn mmr_default_entry_cooldown_ms() -> u64 {
    0
}
fn mmr_default_entry_from_touch() -> bool {
    false
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
    /// Time-decay close-target step 1: after this many seconds holding
    /// a position, multiply the close target by `close_decay_factor_1`
    /// to ratchet TP closer. `0` (default) disables.
    #[serde(default)]
    pub close_decay_after_secs_1: u64,
    /// Multiplier applied after step 1. `1.0` (default) = no-op.
    #[serde(default = "spread_scalp_default_decay_factor")]
    pub close_decay_factor_1: Decimal,
    /// Time-decay step 2: stronger decay after a longer hold.
    /// Supersedes step 1 once reached. `0` (default) disables.
    #[serde(default)]
    pub close_decay_after_secs_2: u64,
    /// Multiplier applied after step 2. `1.0` (default) = no-op.
    #[serde(default = "spread_scalp_default_decay_factor")]
    pub close_decay_factor_2: Decimal,
    /// Adverse-drift hard stop: after N seconds holding, if mid drift
    /// is at least `adverse_stop_drift_bps` against position, IOC close
    /// at touch. Default `120s` from the 2026-05-25 sweep winner
    /// (+93% banked profit on DOGE/$700/33h). Set `0` to disable.
    #[serde(default = "spread_scalp_default_adverse_stop_after_secs")]
    pub adverse_stop_after_secs: u64,
    /// Bps drift threshold for the adverse stop. Default `30` from the
    /// 2026-05-25 sweep winner. Set `0` to disable.
    #[serde(default = "spread_scalp_default_adverse_stop_drift_bps")]
    pub adverse_stop_drift_bps: u32,
    /// Tick offset from touch for quote placement (signed).
    /// `-1` (default): 1 tick INSIDE — legacy SS, captures cascade
    /// widenings (requires book >= 2 ticks wide).
    /// `0`: AT touches, joins queue.
    /// `+1`, `+2`, …: N ticks OUTSIDE — tick-floor sitter (owns its
    /// own level, captures (2N+1) ticks per RT). Best on wide-tick
    /// symbols where book sits at 1-tick floor.
    #[serde(default = "spread_scalp_default_quote_offset_ticks")]
    pub quote_offset_ticks: i32,
    /// Tick-mode close target in ticks from `avg_entry`. Only used when
    /// `quote_offset_ticks >= 0`. `0` (default) = auto =
    /// `quote_offset_ticks + 1`. Close quote sits at avg±N×tick on the
    /// favorable side, taking max(target, touch) for long-close /
    /// min(target, touch) for short-close. Pure tick math, no bps.
    #[serde(default)]
    pub close_target_ticks: u32,
    /// Bypass the close-side avg-anchored pin so both sides quote at
    /// touch always. Trade: gives up the "never close at loss" floor
    /// in exchange for staying at front of book. Default `false`.
    #[serde(default)]
    pub strict_touch_quotes: bool,
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
fn spread_scalp_default_decay_factor() -> Decimal {
    Decimal::ONE
}
fn spread_scalp_default_adverse_stop_after_secs() -> u64 {
    120
}
fn spread_scalp_default_adverse_stop_drift_bps() -> u32 {
    30
}
fn spread_scalp_default_quote_offset_ticks() -> i32 {
    -1
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
        assert_eq!(cfg.account.leverage, 1);
        assert_eq!(cfg.account.max_position_pct, Decimal::from(80));
        assert_eq!(sg.notional, None);
        assert_eq!(sg.levels, 3);
        assert_eq!(sg.inner_bps, 3);
        assert_eq!(sg.scale_max, Decimal::from(4));
    }

    #[test]
    fn rampage_wave_variant_deserializes() {
        let s = r#"
            [account]
            env = "futures-testnet"

            [rampage]
            enabled = true
            min_volume_usdt = "2000000"
            recheck_interval_secs = 60
            quote_asset = "USDT"
            top_n = 10

            [rampage.score]
            mode = "candle_height"
            [rampage.score.params]
            candle_count = 60
            min_candle_pct = "0"

            [rampage.strategy]
            kind = "wave"
            [rampage.strategy.params]
            levels = 10
            steps_bps = 30
            steps_inner = 2
            round_trips = 5
        "#;
        let cfg: DashboardConfig = toml::from_str(s).unwrap();
        let r = cfg.rampage.as_ref().expect("rampage must be present");
        assert!(r.enabled);
        assert_eq!(r.top_n, 10);
        match &r.score {
            ScoreMode::CandleHeight {
                candle_count,
                min_candle_pct,
            } => {
                assert_eq!(*candle_count, 60);
                assert_eq!(*min_candle_pct, Decimal::ZERO);
            }
            other => panic!("expected CandleHeight, got {other:?}"),
        }
        match &r.strategy {
            RampageStrategy::Wave {
                levels,
                steps_bps,
                steps_inner,
                round_trips,
            } => {
                assert_eq!(*levels, 10);
                assert_eq!(*steps_bps, 30);
                assert_eq!(*steps_inner, 2);
                assert_eq!(*round_trips, 5);
            }
            other => panic!("expected Wave, got {other:?}"),
        }
    }

    #[test]
    fn rampage_tide_variant_deserializes() {
        let s = r#"
            [account]
            env = "futures-testnet"

            [rampage]
            enabled = true
            min_volume_usdt = "20000000"
            recheck_interval_secs = 60
            quote_asset = "USDT"
            top_n = 5

            [rampage.score]
            mode = "tick_bps"
            [rampage.score.params]
            min_tick_bps = "6"

            [rampage.strategy]
            kind = "tide"
            [rampage.strategy.params]
            grid_levels = 12
            step_bps = 10
        "#;
        let cfg: DashboardConfig = toml::from_str(s).unwrap();
        let r = cfg.rampage.as_ref().expect("rampage must be present");
        assert!(r.enabled);
        assert_eq!(r.top_n, 5);
        match &r.score {
            ScoreMode::TickBps { min_tick_bps } => {
                assert_eq!(*min_tick_bps, Decimal::from(6));
            }
            other => panic!("expected TickBps, got {other:?}"),
        }
        match &r.strategy {
            RampageStrategy::Tide {
                grid_levels,
                step_bps,
                ..
            } => {
                assert_eq!(*grid_levels, 12);
                assert_eq!(*step_bps, 10);
            }
            other => panic!("expected Tide, got {other:?}"),
        }
    }
}
