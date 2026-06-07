//! TOML configuration schema for the dashboard.
//!
//! See `examples/config.toml` for a full annotated example.

use std::path::{Path, PathBuf};

use rust_decimal::Decimal;
use serde::Deserialize;

/// Top-level dashboard config.
#[derive(Debug, Clone, Deserialize)]
pub struct DashboardConfig {
    /// Account-wide settings (env, keys, leverage, etc).
    pub account: AccountConfig,
    /// Per-symbol bot specifications. Renamed to `bot` in TOML for the
    /// `[[bot]]` array-of-tables syntax.
    #[serde(rename = "bot", default)]
    pub bots: Vec<BotConfig>,
    /// The single auto-rotation manager. Selects symbols by `score` mode and
    /// runs the configured `strategy` (Wave / Tide / any `[[bot]]` template via
    /// `kind = "template"`) on the top N qualifiers. Replaced the old
    /// `[scalp_rotation]` / `[static_grid_rotation]` / `[wave_auto]` /
    /// `[tide_auto]` managers.
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

/// Account-wide settings.
#[derive(Debug, Clone, Deserialize)]
pub struct AccountConfig {
    /// Binance environment: `"futures-testnet"` or `"futures-mainnet"`.
    pub env: String,
    /// Optional path to a key file (HMAC `key:secret` single-line, or
    /// Ed25519 PEM if `key_type = "ed25519"`).
    #[serde(default)]
    pub key_file: Option<PathBuf>,
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
    /// Wallet-aware auto-scaling. When `true`, rampage derives the active bot
    /// count and per-bot order size from the live wallet instead of always
    /// running `rampage.top_n` bots at `order_balance_pct` of the FULL wallet:
    /// `N = clamp(floor(wallet / min_bot_capital_usdc), 1, top_n)` and each bot
    /// orders `(wallet / N) × order_balance_pct` (still floored at the venue
    /// min-notional). As the wallet grows it fills bots up to `top_n` first,
    /// then grows order sizes — keeping total order flow ≈ `wallet × pct`
    /// regardless of N. `false` (default) preserves the legacy behavior
    /// (every bot orders `wallet × order_balance_pct`, count fixed at `top_n`).
    #[serde(default)]
    pub auto_scale: bool,
    /// Auto-scale margin gate: minimum wallet (in the margin asset) required to
    /// justify each additional bot. `N = min(top_n, floor(wallet / this))`.
    /// Ignored unless `auto_scale`. Default `$250`.
    #[serde(default = "default_min_bot_capital_usdc")]
    pub min_bot_capital_usdc: Decimal,
    /// Take-profit: when ANY bot's UNREALIZED P&L exceeds this percent of the
    /// account wallet balance, the runner rests a reduce-only maker limit at the
    /// touch to close HALF that bot's position, locking in profit. Account-level
    /// (all bots/strategies). `0` (default) = disabled.
    #[serde(default)]
    pub take_profit_pct: Decimal,
    /// BNB-refill trigger in USDT-equivalent. When BNB-pays-fees is
    /// enabled on the account AND `bnb_refill_enabled = true`, the
    /// refill task tops up BNB whenever
    /// `bnb_balance × bnb_price < bnb_min_balance_usdt`. Default `$1`.
    #[serde(default = "default_bnb_min_balance_usdt")]
    pub bnb_min_balance_usdt: Decimal,
    /// BNB-refill target in USDT-equivalent. Refill converts enough USDT→BNB
    /// to bring the USDT-value up to this level. Default `$10`.
    #[serde(default = "default_bnb_target_balance_usdt")]
    pub bnb_target_balance_usdt: Decimal,
    /// Master switch for BNB auto-refill. Defaults `false` — enable on
    /// exactly ONE process per account (uncoordinated monitors on a shared
    /// account would convert concurrently). Has no effect unless BNB-pays-fees
    /// is also enabled on the Binance account (auto-detected via
    /// `GET /fapi/v1/feeBurn`).
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
    /// by `inventory_boost_pct` AND `avg_chase_boost_pct`. `1` (default) =
    /// linear; `>1` = slow start then steep (boost concentrated near the cap);
    /// `<1` = fast early ramp.
    #[serde(default = "default_inventory_boost_curve")]
    pub inventory_boost_curve: Decimal,
    /// Average-chase order-size boost: the INVERSE of `inventory_boost_pct`.
    /// Scales the inventory-*adding* side up (short → sells, long → buys) as the
    /// bag fills, averaging the entry toward mid — paired with an avg-price
    /// take-profit, this keeps the bag near-empty in chop. `0` (default) off.
    /// Takes precedence over `inventory_boost_pct` (they're opposite directions,
    /// one runner slot). WARNING: this is averaging-down/martingale — it grows
    /// the bag faster into a sustained trend; pair with low leverage + a bag cap.
    #[serde(default = "default_inventory_boost_pct")]
    pub avg_chase_boost_pct: Decimal,
    /// Profit-gated reducing-side boost. When `true`, `inventory_boost_pct`
    /// scales each reducing-side quote by how far its price sits PAST the
    /// position's average entry in the favorable direction (long: sell above
    /// avg; short: buy below avg) instead of by `|pos|/cap` — so the bag is
    /// only shed harder when the fill banks a gain, and a quote that would lock
    /// a loss is left untouched. Needs no position cap (works with
    /// `max_position_pct = 0`). Only affects the reducing side (ignored when
    /// `avg_chase_boost_pct > 0`). `false` (default) = legacy `|pos|/cap` curve.
    #[serde(default)]
    pub inventory_boost_profit_gated: bool,
    /// Profit distance past avg-entry, in bps, at which the profit-gated boost
    /// saturates to `inventory_boost_pct`. Ignored unless
    /// `inventory_boost_profit_gated`. Default 50 bps.
    #[serde(default = "default_inventory_boost_profit_full_bps")]
    pub inventory_boost_profit_full_bps: Decimal,
    /// Account-level bagger (inventory-risk flatten). All mechanisms default
    /// off; populate the `[account.bagger]` table to enable one. Applied to
    /// every bot by the runner.
    #[serde(default)]
    pub bagger: BaggerSettings,
}

/// `[account.bagger]` — inventory-risk flatten mechanisms. Mirrors
/// `tikr_paper::bagger::BaggerConfig`; every mechanism is off by default and
/// composes (the runner evaluates them in priority order). The `compare`
/// `--bagger-*` flags exercise the same knobs in backtest.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BaggerSettings {
    /// Exit style: `true` = taker (cross spread), `false` = reduce-only maker.
    pub exit_taker: bool,
    /// Take-profit flatten size: `true` = full bag, `false` = half. (SL / equity
    /// / size-cap always flatten what they must.)
    pub flatten_full: bool,
    /// Size gate: gated mechanisms (all except equity giveback / inv-cap /
    /// wallet bracket / pnl-flat / periodic) arm only when `|pos notional| ≥
    /// this %` of the wallet. `0` = those never arm.
    pub size_gate_pct: Decimal,
    /// Size cap: trim the bag back to this % of wallet when exceeded. `0` off.
    pub cap_pct: Decimal,
    /// Stop-loss: flatten when unrealized ≤ `−this %` of wallet. `0` off.
    pub sl_pct: Decimal,
    /// Stop-loss gate: only cut an underwater bag that made a new adverse
    /// extreme within this many seconds (still trending). `0` = bare level stop.
    pub deteriorate_secs: u64,
    /// Equity giveback: flatten when MTM equity drops this **percent** from its
    /// session peak (e.g. `10` = 10% off the high). Global, no size gate.
    /// `0` off.
    pub equity_giveback_pct: Decimal,
    /// Trailing TP: flatten when unrealized retraces this **percent** from its
    /// peak (e.g. `30` = 30%). `0` off.
    pub trail_pct: Decimal,
    /// Fixed TP: flatten when unrealized ≥ this % of wallet. `0` off.
    pub fixed_tp_pct: Decimal,
    /// Inventory cap: dump the WHOLE bag when its notional reaches this % of
    /// the wallet (e.g. `100` = bag ≥ wallet). Hard circuit-breaker. `0` off.
    pub inv_flat_wallet_pct: Decimal,
    /// Gate the inventory cap on profit: `true` = only fire when the bag is in
    /// profit (big-winner TP); `false` = unconditional (liquidation breaker).
    pub inv_flat_require_profit: bool,
    /// Periodic flatten: every this-many seconds, reduce the bag by
    /// `periodic_flatten_frac`. `0` off.
    pub periodic_flatten_secs: u64,
    /// Fraction reduced on each periodic flatten. Default `0.5`.
    pub periodic_flatten_frac: Decimal,
    /// Wallet bracket TP: reduce by `wallet_flat_frac` when unrealized reaches
    /// `+this %` of wallet. `0` off.
    pub wallet_tp_pct: Decimal,
    /// Wallet bracket SL: reduce by `wallet_flat_frac` when unrealized falls to
    /// `−this %` of wallet. `0` off.
    pub wallet_sl_pct: Decimal,
    /// Fraction reduced on either wallet-bracket trigger. Default `0.5`.
    pub wallet_flat_frac: Decimal,
    /// P&L flat: flatten the whole bag (maker, ungated) when `|unrealized| ≥
    /// this %` of the per-order notional. High-churn. `0` off.
    pub pnl_flat_pct: Decimal,
    /// Profit-lock ratchet: bank the whole bag once MTM equity grows `+this %`
    /// past the snapshot, then re-snapshot at the new equity. `0` off.
    pub profit_lock_pct: Decimal,
    /// Loss-lock ratchet (downside counterpart): cut the whole bag once MTM
    /// equity falls `−this %` below the shared snapshot, then re-snapshot at the
    /// new equity. Enable both for a two-sided bracket. `0` off.
    pub loss_lock_pct: Decimal,
    /// Buying-power cut: reduce the bag by `bp_flat_frac` when its notional
    /// reaches `this %` of buying power (wallet × leverage), e.g. `30`. `0` off.
    pub bp_flat_pct: Decimal,
    /// Fraction reduced on each buying-power-cut trigger. Default `0.5`.
    pub bp_flat_frac: Decimal,
    /// Avg take-profit: rest a reduce-only post-only order `this many bps` beyond
    /// the average entry and sell `avg_tp_frac` of the bag when the mark reaches
    /// it. Companion to `avg_chase_boost_pct`. `0` off.
    pub avg_tp_bps: Decimal,
    /// Fraction of the bag sold on each avg-take-profit trigger. Default `0.5`.
    pub avg_tp_frac: Decimal,
}

impl Default for BaggerSettings {
    fn default() -> Self {
        // Mirror `BaggerConfig::default()` except `exit_taker` — live exits
        // prefer maker (reduce-only) to avoid paying the spread.
        Self {
            exit_taker: false,
            flatten_full: true,
            size_gate_pct: Decimal::ZERO,
            cap_pct: Decimal::ZERO,
            sl_pct: Decimal::ZERO,
            deteriorate_secs: 0,
            equity_giveback_pct: Decimal::ZERO,
            trail_pct: Decimal::ZERO,
            fixed_tp_pct: Decimal::ZERO,
            inv_flat_wallet_pct: Decimal::ZERO,
            inv_flat_require_profit: false,
            periodic_flatten_secs: 0,
            periodic_flatten_frac: Decimal::new(5, 1),
            wallet_tp_pct: Decimal::ZERO,
            wallet_sl_pct: Decimal::ZERO,
            wallet_flat_frac: Decimal::new(5, 1),
            pnl_flat_pct: Decimal::ZERO,
            profit_lock_pct: Decimal::ZERO,
            loss_lock_pct: Decimal::ZERO,
            bp_flat_pct: Decimal::ZERO,
            bp_flat_frac: Decimal::new(5, 1),
            avg_tp_bps: Decimal::ZERO,
            avg_tp_frac: Decimal::new(5, 1),
        }
    }
}

impl BaggerSettings {
    /// Convert to the runner's `BaggerConfig`. A 1:1 field map.
    pub fn to_config(&self) -> tikr_paper::bagger::BaggerConfig {
        tikr_paper::bagger::BaggerConfig {
            size_gate_pct: self.size_gate_pct,
            exit_taker: self.exit_taker,
            flatten_full: self.flatten_full,
            cap_pct: self.cap_pct,
            sl_pct: self.sl_pct,
            deteriorate_secs: self.deteriorate_secs,
            equity_giveback_pct: self.equity_giveback_pct,
            trail_pct: self.trail_pct,
            fixed_tp_pct: self.fixed_tp_pct,
            inv_flat_wallet_pct: self.inv_flat_wallet_pct,
            inv_flat_require_profit: self.inv_flat_require_profit,
            periodic_flatten_secs: self.periodic_flatten_secs,
            periodic_flatten_frac: self.periodic_flatten_frac,
            wallet_tp_pct: self.wallet_tp_pct,
            wallet_sl_pct: self.wallet_sl_pct,
            wallet_flat_frac: self.wallet_flat_frac,
            pnl_flat_pct: self.pnl_flat_pct,
            profit_lock_pct: self.profit_lock_pct,
            loss_lock_pct: self.loss_lock_pct,
            bp_flat_pct: self.bp_flat_pct,
            bp_flat_frac: self.bp_flat_frac,
            avg_tp_bps: self.avg_tp_bps,
            avg_tp_frac: self.avg_tp_frac,
        }
    }
}

impl AccountConfig {
    /// Build the runner-side inventory boost config, or `None` when off.
    ///
    /// Two mutually-exclusive directions share one runner slot:
    /// - `avg_chase_boost_pct > 0` → boost the inventory-**adding** side
    ///   (average the entry toward mid as the bag fills); takes precedence.
    /// - else `inventory_boost_pct > 0` → boost the inventory-**reducing** side
    ///   (flatten faster).
    ///
    /// Both use `inventory_boost_curve` for the ramp shape.
    pub fn inventory_boost(&self) -> Option<tikr_paper::InventoryBoostConfig> {
        if self.avg_chase_boost_pct > Decimal::ZERO {
            Some(tikr_paper::InventoryBoostConfig {
                max_boost_pct: self.avg_chase_boost_pct,
                curve_exponent: self.inventory_boost_curve,
                chase: true,
                // Profit-gating only applies to the reducing side.
                profit_gated: false,
                profit_full_bps: Decimal::ZERO,
            })
        } else if self.inventory_boost_pct > Decimal::ZERO {
            Some(tikr_paper::InventoryBoostConfig {
                max_boost_pct: self.inventory_boost_pct,
                curve_exponent: self.inventory_boost_curve,
                chase: false,
                profit_gated: self.inventory_boost_profit_gated,
                profit_full_bps: self.inventory_boost_profit_full_bps,
            })
        } else {
            None
        }
    }

    /// Build the runner-side bagger config from the `[account.bagger]` table.
    /// Always returns a config; `BaggerConfig::enabled()` is `false` when no
    /// mechanism is set, so the runner skips it.
    pub fn bagger(&self) -> tikr_paper::bagger::BaggerConfig {
        self.bagger.to_config()
    }
}

fn default_bnb_min_balance_usdt() -> Decimal {
    Decimal::ONE
}
fn default_bnb_target_balance_usdt() -> Decimal {
    Decimal::from(10)
}
fn default_bnb_refill_enabled() -> bool {
    false
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
fn default_inventory_boost_profit_full_bps() -> Decimal {
    Decimal::from(50)
}
fn default_min_bot_capital_usdc() -> Decimal {
    Decimal::from(250)
}

fn default_leverage() -> u32 {
    1
}

fn default_max_position_pct() -> Decimal {
    Decimal::from(80)
}

/// Per-bot configuration.
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
    /// Strangler params (only honored when `strategy = "strangler"`).
    #[serde(default)]
    pub strangler: Option<StranglerParams>,
}

/// Strangler — plain tick-spaced lattice window; keep it full.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StranglerParams {
    /// Per-order notional. Account-derived if unset.
    #[serde(default)]
    pub notional: Option<Decimal>,
    /// Orders per side. Default 10.
    #[serde(default = "strangler_default_levels")]
    pub levels: u32,
    /// Ticks between consecutive levels (min 1). Default 1.
    #[serde(default = "strangler_default_step_ticks")]
    pub step_ticks: u32,
    /// Ticks from mid to the first order on each side. `0` (default) = at the
    /// mid tick.
    #[serde(default)]
    pub inner_ticks: u32,
}

fn strangler_default_levels() -> u32 {
    10
}

fn strangler_default_step_ticks() -> u32 {
    1
}

/// Wave — frozen fixed-step lattice with round-trip refill (pure form).
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
    /// `0` (default) = origins at the touch. Used only when `auto_inner=false`.
    #[serde(default)]
    pub steps_inner: u32,
    /// Auto-size the inner dead-zone from recent volatility (~half the mean
    /// high→low gap of the last 60 one-second candles, in bps, ÷ `steps_bps`).
    /// `true` (default) ignores `steps_inner` and starts the inner at 0,
    /// adapting as candles roll. Set `false` to pin the fixed `steps_inner`.
    #[serde(default = "wave_default_auto_inner")]
    pub auto_inner: bool,
    /// Completed round-trips needed to trigger a refill (≥ N bids AND ≥ N asks
    /// drained). A whole side emptying refills regardless. Default 1.
    #[serde(default = "wave_default_round_trips")]
    pub round_trips: u32,
    /// Slow-market valve: after this many seconds with vacant slots, refill them
    /// regardless of `round_trips`. `0` = off. Default `300` (5 min).
    #[serde(default = "wave_default_force_refill_secs")]
    pub force_refill_secs: u64,
    /// Auto-size the lattice step from recent volatility (mirrors `auto_inner`):
    /// step tracks `auto_step_k × mean 1s candle gap (bps)`, floored at the
    /// round-trip break-even (`2 × maker fee`) and capped at `steps_bps`. `false`
    /// (default) uses the fixed `steps_bps`.
    #[serde(default = "wave_default_auto_step")]
    pub auto_step: bool,
    /// Fraction of the mean candle range one step targets when `auto_step` is on.
    /// Default `0.5`.
    #[serde(default = "wave_default_auto_step_k")]
    pub auto_step_k: Decimal,
    /// Trailing 1s candles averaged for the auto-step / auto-inner vol signal.
    /// Fewer = faster reaction (less lag). Default `15`.
    #[serde(default = "wave_default_auto_candle_window")]
    pub auto_candle_window: u32,
    /// Relattice/reposition deadband (fraction): re-place only when the computed
    /// step differs from the placed one by ≥ this. Default `0.02` (2%).
    #[serde(default = "wave_default_relattice_drift_pct")]
    pub relattice_drift_pct: Decimal,
    /// Geometric order-size multiplier per lattice step (deeper orders bigger).
    /// `1.0` (default) = uniform. e.g. `1.2` → each step out is 1.2× the prior.
    #[serde(default = "wave_default_size_mult")]
    pub size_mult: Decimal,
}

fn wave_default_auto_inner() -> bool {
    true
}

fn wave_default_auto_step() -> bool {
    false
}

fn wave_default_auto_step_k() -> Decimal {
    Decimal::new(5, 1) // 0.5
}

fn wave_default_auto_candle_window() -> u32 {
    15
}

fn wave_default_relattice_drift_pct() -> Decimal {
    Decimal::new(2, 2) // 0.02
}

fn wave_default_size_mult() -> Decimal {
    Decimal::ONE
}

fn wave_default_force_refill_secs() -> u64 {
    300
}

fn wave_default_round_trips() -> u32 {
    1
}

fn wave_default_levels() -> u32 {
    12
}

/// Mantis — symmetric touch scalper; rests a bid+ask at the touch.
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
    /// Score by realized volatility (1-minute close-to-close range, bps) minus
    /// taker fee (bps) — prefers high-volatility, low-fee symbols. Mirrors the
    /// old `scalp_rotation` ranking. Fetches 1m klines + the commission rate per
    /// candidate (extra HTTP), gated by `min_tick_bps`.
    RealizedVol {
        #[serde(default = "wave_auto_default_candle_count")]
        candle_count: u32,
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
        #[serde(default = "wave_default_auto_inner")]
        auto_inner: bool,
        #[serde(default = "wave_default_round_trips")]
        round_trips: u32,
        #[serde(default = "wave_default_force_refill_secs")]
        force_refill_secs: u64,
        #[serde(default = "wave_default_auto_step")]
        auto_step: bool,
        #[serde(default = "wave_default_auto_step_k")]
        auto_step_k: Decimal,
        #[serde(default = "wave_default_auto_candle_window")]
        auto_candle_window: u32,
        #[serde(default = "wave_default_relattice_drift_pct")]
        relattice_drift_pct: Decimal,
        #[serde(default = "wave_default_size_mult")]
        size_mult: Decimal,
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
    /// Spawn ANY strategy by cloning a `[[bot]]` template whose `strategy`
    /// matches `name` (e.g. `spread-scalp`, `static-grid`). Lets rampage rotate
    /// strategies that aren't Wave/Tide — the params live in the `[[bot]]` block,
    /// the rampage manager just picks the symbol. Replaces the old
    /// `scalp_rotation` / `static_grid_rotation` managers.
    Template {
        /// Strategy name to match against a `[[bot]]` template's `strategy`.
        name: String,
    },
}

/// `[rampage]` — unified auto-rotation manager. Replaces both `[wave_auto]`
/// and `[tide_auto]`. Discovers qualifying symbols via the configured `score`
/// mode, takes the top `top_n`, and runs the configured `strategy` (Wave or
/// Tide) on each. Preserves all wave_auto features: orphan adoption,
/// defer_underwater, retired-tab GC, graceful shutdown that leaves positions.
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
    /// On retire (rotate-out), convert this PERCENT of the retired bot's final
    /// NET PROFIT into BNB on the futures wallet (USDT→BNB Convert API), to
    /// accumulate BNB for VIP-tier fee discounts. A losing bot is a no-op.
    /// `50` (default) banks half the profit into BNB; `0` disables.
    #[serde(default = "rampage_default_retire_bnb_pct")]
    pub retire_bnb_pct: Decimal,
}

fn rampage_default_rotate_loss_pct() -> Decimal {
    Decimal::from(1)
}

fn rampage_default_retire_bnb_pct() -> Decimal {
    Decimal::from(50)
}

fn rampage_default_big_bag_pct() -> Decimal {
    Decimal::from(25)
}

/// LiqFade configuration — knobs match `LiqFadeConfig` 1:1 plus
/// `arm_window_secs` which sets the runner-side rolling buffer length.
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
                auto_inner,
                round_trips,
                force_refill_secs,
                auto_step,
                auto_step_k,
                auto_candle_window,
                relattice_drift_pct,
                size_mult,
            } => {
                assert_eq!(*levels, 10);
                assert_eq!(*steps_bps, 30);
                assert_eq!(*steps_inner, 2);
                assert!(*auto_inner, "auto_inner defaults true");
                assert_eq!(*round_trips, 5);
                assert_eq!(*force_refill_secs, 300, "default 5min");
                assert!(!*auto_step, "auto_step defaults false");
                assert_eq!(*auto_step_k, Decimal::new(5, 1), "auto_step_k defaults 0.5");
                assert_eq!(*auto_candle_window, 15, "auto_candle_window defaults 15");
                assert_eq!(
                    *relattice_drift_pct,
                    Decimal::new(2, 2),
                    "relattice_drift_pct defaults 0.02"
                );
                assert_eq!(*size_mult, Decimal::ONE, "size_mult defaults 1.0");
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
