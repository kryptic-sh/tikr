//! Run a fixed suite of strategy presets against the same recorded parquet
//! data and print a comparison table.
//!
//! Each preset gets a fresh `ParquetReplay` + `FillSim` + `run_with_resume`
//! pass, so results are apples-to-apples on identical historical events.

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use clap::Parser;
use futures::stream::{self, BoxStream};
use tikr_backtest::fill_sim::{FillSim, FillSimConfig, VenueFees};
use tikr_backtest::replay::{LoadedReplayData, ParquetReplay, ReplayConfig};
use tikr_core::{
    Asset, Decimal, Fill, MarketEvent, MarketKind, Position, SignedSize, Size, Snapshot, Symbol,
    VenueId,
};
use tikr_paper::{FundingConfig, PaperReport, RunnerConfig, SkimConfig, run_with_resume};
use tikr_strategy::{
    AvellanedaStoikov, AvellanedaStoikovConfig, EwmaConfig, Glft, GlftConfig, Hawk, HawkConfig,
    Hydra, HydraConfig, LadderReentry, LadderReentryConfig, LayeredGrid, LayeredGridConfig,
    LiqFade, LiqFadeConfig, MicroMeanReversion, MicroMeanReversionConfig, MicroPrice,
    MicroPriceConfig, SimpleGap, SimpleGapConfig, SpreadScalp, SpreadScalpConfig, StaticGrid,
    StaticGridConfig, Strategy, TopOfBook, TopOfBookConfig,
};
use tikr_venue::{QuoteId, QuoteIntent, Venue, VenueError};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::info;

/// Backtest balance-compounding config: `(initial_balance, order_balance_pct,
/// max_position_pct)`. Set once in `main` from CLI args; read by every spawn
/// helper. Default = all zeros → compounding disabled, static notional path.
static BALANCE_COMPOUNDING: OnceLock<(Decimal, Decimal, Decimal)> = OnceLock::new();

fn balance_compounding() -> (Decimal, Decimal, Decimal) {
    BALANCE_COMPOUNDING
        .get()
        .copied()
        .unwrap_or((Decimal::ZERO, Decimal::ZERO, Decimal::ZERO))
}

#[derive(Parser, Debug, Clone)]
#[command(
    name = "compare",
    about = "Run a strategy suite over recorded parquet data and print a comparison"
)]
struct Args {
    /// Directory containing `book_<BASE>_*.parquet` + `trades_<BASE>_*.parquet`.
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,

    /// Binance-style symbol (e.g. `BTCUSDT`).
    #[arg(long, default_value = "BTCUSDT")]
    symbol: String,

    /// Basket mode: directory containing one subdir per symbol (e.g.
    /// `./data/24h/{BTCUSDT,ETHUSDT,DOGEUSDT}/`). When set, overrides
    /// `--data-dir` + `--symbol` and runs the full sweep for every
    /// matching subdir, printing a per-symbol table followed by a
    /// summary row totalling each symbol's best-preset NET.
    /// Empty (default) = single-symbol mode.
    #[arg(long, default_value = "")]
    data_root: String,

    /// Basket mode allow-list — only run for symbols whose subdir name
    /// matches one of these (comma-separated). Empty (default) accepts
    /// every subdir whose name matches `*USDT`/`*USDC`. Use to limit a
    /// basket sweep to a subset without trimming the directory.
    #[arg(long, default_value = "")]
    symbols_filter: String,

    /// Output format: `table` (default, pretty-aligned columns), `csv`
    /// (one row per preset, header on first line), or `markdown`
    /// (pipe-separated table). CSV is suitable for piping into
    /// spreadsheets or downstream plotting; markdown for pasting into
    /// commit messages or Github discussions.
    #[arg(long, default_value = "table")]
    output: String,

    /// Sort rows by NET descending before printing (best-first). Set
    /// `false` to keep spawn-order (the original behaviour) — useful
    /// when comparing the same preset across runs by row index.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    sort_by_net: bool,

    /// Drop presets with fewer than this many fills from the output
    /// (and from cross-symbol totals). `0` (default) keeps every row.
    /// `1` is the common "trim 0-fill noise" setting on big sweeps.
    #[arg(long, default_value_t = 0u64)]
    min_fills: u64,

    /// Maximum number of presets to run concurrently. `0` (default) =
    /// unlimited (tokio runtime decides). Set to `cpus` or a smaller
    /// number to keep the machine usable while a big sweep runs —
    /// 135-preset sweeps can otherwise pin every core for 20+ minutes.
    #[arg(long, default_value_t = 0usize)]
    parallel: usize,

    /// Baseline preset name (or substring match). When set, the table
    /// gets an extra ΔNET column showing each preset's NET minus the
    /// baseline's NET. Lets you A/B knob changes against a reference
    /// without eyeballing the numeric diff. The baseline row itself
    /// shows ΔNET = `0.0000`. Empty (default) skips the column.
    /// Example: `--baseline "SG in=3 st=3 lv=3"` (partial match OK).
    #[arg(long, default_value = "")]
    baseline: String,

    /// Directory where per-preset equity-curve CSVs are written. One
    /// file per preset (`<dir>/<sanitized_preset_name>.csv`) with
    /// header `ts_ns,sim_secs,fills,pos_size,realized,unrealized,fees,funding,net`.
    /// Rows are appended at each snapshot tick (defaults to every event
    /// — see `RunnerConfig::snapshot_every_n_events`). Empty (default)
    /// disables the export. Basket mode (`--data-root`) suffixes the
    /// symbol so per-symbol files don't collide.
    #[arg(long, default_value = "")]
    equity_csv_dir: String,

    /// Order size per quote (applied to ALL presets).
    #[arg(long, default_value = "0.001")]
    size: String,

    /// Maker fee in bps (default = Binance Futures USD-M tier 0).
    #[arg(long, default_value_t = 2i32)]
    maker_bps: i32,

    /// Taker fee in bps.
    #[arg(long, default_value_t = 5u32)]
    taker_bps: u32,

    /// Heartbeat synthesis cadence (ms).
    #[arg(long, default_value_t = 1000u64)]
    heartbeat_ms: u64,

    /// Venue tick size (price increment). Defaults to `auto` — detected
    /// from a static map of known Binance USD-M perps, falling back to
    /// sniffing the first `book_*.parquet` for the smallest non-zero
    /// price gap. Pass an explicit decimal (`0.1`, `0.00001`, etc.) to
    /// override.
    #[arg(long, default_value = "auto")]
    tick_size: String,

    /// Venue lot step size (quantity rounding). `auto` (default) → same
    /// detection path as `--tick-size` (static map → parquet sniff on
    /// the trades file). `""` (empty) falls back to `tick_size` — kept
    /// for back-compat with old invocations.
    #[arg(long, default_value = "auto")]
    step_size: String,

    /// Skim mode: starting USDT budget per preset. `0` disables (default).
    /// When enabled, each preset's perp account starts at this budget and
    /// profit is moved to spot in `skim_pct` chunks.
    #[arg(long, default_value_t = 0.0_f64)]
    budget: f64,

    /// Skim threshold as percent of budget. `5` = skim every 5% gain.
    #[arg(long, default_value_t = 5.0_f64)]
    skim_pct: f64,

    /// Fraction of each skim chunk that moves to spot. `1.0` = classic
    /// (all → spot). `0.5` = half to spot, half compounds in perp.
    /// `0.0` = no spot buys, all profits stay in perp.
    #[arg(long, default_value_t = 1.0_f64)]
    skim_ratio: f64,

    /// LayeredGrid sweep: comma-separated `bps` values (single spacing
    /// param). Default `4` is the prior-sweep winner on 2026-05-24 72h
    /// frozen-snapshot data; expand the list for per-strategy retuning.
    #[arg(long, default_value = "4")]
    lg_bps_list: String,

    /// LayeredGrid sweep: comma-separated `levels` values. Default `2`
    /// (prior-sweep winner — see `lg_bps_list`).
    #[arg(long, default_value = "2")]
    lg_levels_list: String,

    /// StaticGrid sweep: comma-separated `inner_bps` values. Default `6`
    /// (prior-sweep winner — expand for retuning).
    #[arg(long, default_value = "6")]
    sg_inner_bps_list: String,

    /// StaticGrid sweep: comma-separated `step_bps` values. Default `3`
    /// (prior-sweep winner — expand for retuning).
    #[arg(long, default_value = "3")]
    sg_step_bps_list: String,

    /// StaticGrid sweep: comma-separated `levels_per_side` values.
    /// Default `3` (prior-sweep winner — expand for retuning).
    #[arg(long, default_value = "3")]
    sg_levels_list: String,

    /// StaticGrid sweep: comma-separated `target_fills_per_min` values
    /// (decimals). `0` disables the adaptive scaler. Default `0` keeps
    /// the scaler off so baseline sweeps are comparable.
    #[arg(long, default_value = "0")]
    sg_target_fpm_list: String,

    /// StaticGrid sweep: comma-separated `fillrate_window_secs` values.
    #[arg(long, default_value = "60")]
    sg_fpm_window_list: String,

    /// StaticGrid sweep: comma-separated `scale_min` values (decimals).
    #[arg(long, default_value = "1.0")]
    sg_scale_min_list: String,

    /// StaticGrid sweep: comma-separated `scale_max` values (decimals).
    #[arg(long, default_value = "4.0")]
    sg_scale_max_list: String,

    /// StaticGrid: enable inventory-driven auto-skew (weak side joins
    /// best touch; strong side widens by `(1 + |ratio|)`). Default
    /// true — pass `--sg-auto-skew=false` to A/B test symmetric
    /// quoting.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    sg_auto_skew: bool,

    /// StaticGrid: regime-tracker window in seconds. `0` disables
    /// regime gating — `auto_skew` then applies unconditionally
    /// (legacy behaviour). Non-zero suppresses skew during chop
    /// regimes (|drift_bps| <= threshold over window) and engages
    /// it during trending ones.
    #[arg(long, default_value_t = 0u64)]
    sg_regime_window_secs: u64,

    /// StaticGrid: drift threshold (bps) above which the regime
    /// classifier flags "trending". Default 10 bps over the chosen
    /// window. Only meaningful when `sg_regime_window_secs > 0`.
    #[arg(long, default_value_t = 10u32)]
    sg_regime_trend_threshold_bps: u32,

    /// StaticGrid: directional-efficiency threshold for regime
    /// classification (Kaufman's efficiency ratio). Range [0, 1] —
    /// `0` disables (falls back to `sg_regime_trend_threshold_bps`).
    /// Sensible: `0.3` (30% of total path was directional). Self-
    /// scales per symbol (no per-symbol bps tuning needed).
    #[arg(long, default_value = "0")]
    sg_regime_efficiency_threshold: String,

    /// StaticGrid: hard inventory cap in USDT notional. When
    /// `|position × mid| >= cap`, the add-side quote (Bid for longs,
    /// Ask for shorts) is suppressed so existing rest-orders can drain
    /// the position. `0` (default) disables.
    #[arg(long, default_value = "0")]
    sg_max_pos_usdt: String,

    /// StaticGrid: take-profit threshold in bps of position notional.
    /// `0` (default) disables. Same bps-of-notional shape as SS.
    #[arg(long, default_value_t = 0u32)]
    sg_take_profit_bps: u32,

    /// StaticGrid: stop-loss threshold in bps of position notional.
    /// `0` (default) disables. Pairs with `sg_take_profit_bps`.
    #[arg(long, default_value_t = 0u32)]
    sg_stop_loss_bps: u32,

    /// LayeredGrid: hard inventory cap in USDT notional. `0`
    /// (default) disables. Same shape as `sg_max_pos_usdt`.
    #[arg(long, default_value = "0")]
    lg_max_pos_usdt: String,

    /// LayeredGrid: take-profit threshold in bps of position notional.
    /// `0` (default) disables.
    #[arg(long, default_value_t = 0u32)]
    lg_take_profit_bps: u32,

    /// LayeredGrid: stop-loss threshold in bps of position notional.
    /// `0` (default) disables.
    #[arg(long, default_value_t = 0u32)]
    lg_stop_loss_bps: u32,

    /// SpreadScalp: keep the close-side passive quote alive even when
    /// book spread falls below `min_spread_bps`, so a held position
    /// can drain at maker fee once the cascade event that triggered
    /// the entry cools off. Default `true`. Pass
    /// `--spread-scalp-close-side-always-quotes=false` for the legacy
    /// behaviour (both sides cancel when targets unavailable).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    spread_scalp_close_side_always_quotes: bool,

    /// SpreadScalp time-decay step 1: after this many seconds holding
    /// a position, multiply the close-target distance by
    /// `--spread-scalp-close-decay-factor-1` to ratchet TP closer.
    /// `0` (default) disables — no decay, behaviour unchanged.
    #[arg(long, default_value_t = 0u64)]
    spread_scalp_close_decay_after_secs_1: u64,

    /// SpreadScalp time-decay multiplier applied after secs_1.
    /// `1.0` (default) = no-op. Sensible: 0.5-0.8.
    #[arg(long, default_value = "1.0")]
    spread_scalp_close_decay_factor_1: String,

    /// SpreadScalp time-decay step 2: stronger decay after a longer
    /// hold. Supersedes step 1 once reached. `0` (default) disables.
    #[arg(long, default_value_t = 0u64)]
    spread_scalp_close_decay_after_secs_2: u64,

    /// SpreadScalp time-decay multiplier applied after secs_2.
    /// `1.0` (default) = no-op. Typically tighter than factor_1.
    #[arg(long, default_value = "1.0")]
    spread_scalp_close_decay_factor_2: String,

    /// SpreadScalp adverse-drift stop: after this many seconds holding,
    /// if mid drifts >= `--spread-scalp-adverse-stop-drift-bps` against
    /// position direction, IOC close at touch. Default `120s` from the
    /// 2026-05-25 sweep winner (+93% banked profit on DOGE/$700/33h).
    /// Set `0` to disable.
    #[arg(long, default_value_t = 120u64)]
    spread_scalp_adverse_stop_after_secs: u64,

    /// SpreadScalp adverse-stop drift threshold (bps). Default `30` from
    /// the 2026-05-25 sweep winner. Set `0` to disable even when the
    /// time gate is set.
    #[arg(long, default_value_t = 30u32)]
    spread_scalp_adverse_stop_drift_bps: u32,

    /// SpreadScalp quote-placement offset in ticks (signed).
    /// `-1` (default, legacy SS): 1 tick INSIDE touches, requires
    /// book >= 2 ticks wide. `0`: AT touches, joins queue. `+1`, `+2`:
    /// N ticks OUTSIDE touches (tick-floor sitter — owns its own level,
    /// captures (2N+1) ticks per RT; best on wide-tick symbols).
    #[arg(long, default_value_t = -1i32, allow_hyphen_values = true)]
    spread_scalp_quote_offset_ticks: i32,

    /// SpreadScalp tick-mode close target in ticks (used only when
    /// `quote_offset_ticks >= 0`). Close-side quote sits at
    /// `avg_entry ± N×tick` on the favorable side, taking the better
    /// of (target, touch). `0` (default) = auto = `quote_offset_ticks+1`.
    #[arg(long, default_value_t = 0u32)]
    spread_scalp_close_target_ticks: u32,

    /// LiqFade: directory holding `record_liquidations`-style parquet
    /// shards (per-day `YYYY-MM-DD/all_symbols.parquet`). Empty (default)
    /// disables LiqFade — the preset is skipped even when included in
    /// `--strategies` because the strategy needs the @forceOrder feed.
    #[arg(long, default_value = "")]
    liq_data_dir: String,

    /// LiqFade: rolling window (seconds) for runner-side liq buffer.
    /// Must be ≥ the strategy's longest internal timeout
    /// (`entry_timeout_secs` + `position_timeout_secs`). Default `120`.
    #[arg(long, default_value_t = 120u32)]
    liq_window_secs: u32,

    /// LiqFade: fiat notional per fade entry.
    #[arg(long, default_value = "100")]
    liq_notional: String,

    /// LiqFade: per-side liquidation USDT threshold to arm.
    /// Default `5_000_000`. Lower for alts where 5M is too rare.
    #[arg(long, default_value = "5000000")]
    liq_arm_threshold_usdt: String,

    /// LiqFade: dominance ratio of light side / heavy side at arm.
    /// `0.5` = heavy ≥ 2× light. Range `(0, 1)`.
    #[arg(long, default_value = "0.5")]
    liq_arm_dominance: String,

    /// LiqFade: capitulation overshoot in bps past pre-liq mid before
    /// posting the fade quote. Default `15`.
    #[arg(long, default_value_t = 15u32)]
    liq_capit_bps: u32,

    /// LiqFade: fade-quote offset in bps deeper than the dislocated
    /// touch. Default `5`.
    #[arg(long, default_value_t = 5u32)]
    liq_fade_offset_bps: u32,

    /// LiqFade: TP target in bps of revert toward pre-liq mid.
    /// Must be < `liq_capit_bps`. Default `10`.
    #[arg(long, default_value_t = 10u32)]
    liq_revert_target_bps: u32,

    /// LiqFade: entry-quote rest timeout in seconds. Default `30`.
    #[arg(long, default_value_t = 30u32)]
    liq_entry_timeout_secs: u32,

    /// LiqFade: position time-stop (force IOC flatten). Default `120`.
    #[arg(long, default_value_t = 120u32)]
    liq_position_timeout_secs: u32,

    /// LiqFade: stop-loss in bps of position notional. `0` disables.
    #[arg(long, default_value_t = 0u32)]
    liq_stop_loss_bps: u32,

    /// LiqFade: hard inventory cap in USDT notional. `0` disables.
    #[arg(long, default_value = "0")]
    liq_max_pos_usdt: String,

    /// Hawk: fiat notional per quote level.
    #[arg(long, default_value = "100")]
    hawk_notional: String,

    /// Hawk: comma-separated `levels_per_side` sweep.
    #[arg(long, default_value = "2,3")]
    hawk_levels_list: String,

    /// Hawk: comma-separated `inner_bps` sweep.
    #[arg(long, default_value = "3,5")]
    hawk_inner_bps_list: String,

    /// Hawk: comma-separated `step_bps` sweep.
    #[arg(long, default_value = "2,3")]
    hawk_step_bps_list: String,

    /// Hawk: comma-separated `min_spread_bps` sweep — the spread gate
    /// that decides hot vs cold mode.
    #[arg(long, default_value = "3,5")]
    hawk_min_spread_bps_list: String,

    /// Hawk: hard inventory cap in USDT notional.
    #[arg(long, default_value = "200")]
    hawk_max_pos_usdt: String,

    /// Hawk: close-side target offset (bps from avg_entry) in cold
    /// mode. `0` (default) falls back to `min_spread_bps`.
    #[arg(long, default_value_t = 0u32)]
    hawk_close_target_bps: u32,

    /// Hawk: take-profit threshold in bps of position notional.
    #[arg(long, default_value_t = 0u32)]
    hawk_take_profit_bps: u32,

    /// Hawk: stop-loss threshold in bps of position notional.
    #[arg(long, default_value_t = 0u32)]
    hawk_stop_loss_bps: u32,

    /// Hydra: per-order notional as percent of `hydra_max_pos_usdt`.
    /// Default `15` is the 2026-05-25 retune sweet spot (notional=75 /
    /// cap=500). Strategies are now wallet-scaled in live mode; this
    /// flag exists for backtests so the ratio between order size and
    /// cap stays explicit instead of being a raw USDT number that
    /// doesn't generalise across wallet sizes.
    #[arg(long, default_value = "15")]
    hydra_notional_pct: String,

    /// Hydra: comma-separated `entry_offset_bps` sweep — straddle
    /// distance from mid. Wider = bigger-move filter; tighter = more
    /// chop fills.
    /// Default `50` is the 2026-05-25 uniform-eo sweep winner ($13.94
    /// basket NET on 13.6h frozen-snapshot). Bimodal — local peaks at
    /// 10 and 50; valleys at 30 and 75. Expand the list for per-symbol
    /// retuning (compare picks the best per symbol from a multi-value
    /// list).
    #[arg(long, default_value = "50")]
    hydra_entry_offset_bps_list: String,

    /// Hydra: comma-separated `pyramid_step_bps` sweep — favorable-
    /// drift band that triggers a pyramid add.
    #[arg(long, default_value = "50")]
    hydra_pyramid_step_bps_list: String,

    /// Hydra: max pyramid adds. `0` disables the pyramid arm.
    #[arg(long, default_value_t = 2u32)]
    hydra_pyramid_max_adds: u32,

    /// Hydra: comma-separated `dca_step_bps` sweep — adverse-drift
    /// band that triggers a DCA add.
    #[arg(long, default_value = "60")]
    hydra_dca_step_bps_list: String,

    /// Hydra: max DCA adds. `0` disables the DCA arm.
    #[arg(long, default_value_t = 2u32)]
    hydra_dca_max_adds: u32,

    /// Hydra: take-profit threshold in bps from rolling avg_entry.
    #[arg(long, default_value_t = 30u32)]
    hydra_tp_bps_from_avg: u32,

    /// Hydra: stop-loss threshold in bps from the ORIGINAL first-fill
    /// price. Anchored on first fill so DCA can't drag the trigger
    /// out indefinitely.
    #[arg(long, default_value_t = 100u32)]
    hydra_sl_bps_from_first: u32,

    /// Hydra: hard inventory cap in USDT notional.
    #[arg(long, default_value = "500")]
    hydra_max_pos_usdt: String,

    /// Hydra: minimum elapsed time between adds (ms).
    #[arg(long, default_value_t = 500u64)]
    hydra_add_cooldown_ms: u64,

    /// Hydra: refresh the resting straddle this many seconds after
    /// placement. `0` (default) disables — the naive refresh tends
    /// to cancel the closer-to-touch leg before it can fill, which
    /// HURT net on the 2026-05-24 sweep (basket dropped from +\$7.82
    /// to +\$1.95). Left in for experimentation but off by default.
    #[arg(long, default_value_t = 0u32)]
    hydra_straddle_refresh_secs: u32,

    /// Hydra: refresh the straddle when mid drifts this many bps from
    /// the anchor. `0` (default) disables — same observation as above.
    #[arg(long, default_value_t = 0u32)]
    hydra_straddle_drift_bps: u32,

    /// Hydra: pyramid arm notional multiplier (× notional).
    #[arg(long, default_value = "1.0")]
    hydra_pyramid_size_mult: String,

    /// Hydra: DCA arm notional multiplier (× notional).
    #[arg(long, default_value = "1.0")]
    hydra_dca_size_mult: String,

    /// Ratchet: per-order notional as percent of `ratchet_max_pos_usdt`
    /// (mirrors hydra_notional_pct — keeps the ratio explicit).
    #[arg(long, default_value = "15")]
    ratchet_notional_pct: String,

    /// Ratchet: comma-separated `tp_bps` sweep — bps offset from the
    /// last-fill price for the opposite-side ratchet order.
    #[arg(long, default_value = "30")]
    ratchet_tp_bps_list: String,

    /// Ratchet: bps offset from mid for the cold-start straddle (used
    /// until the first fill establishes a last-buy / last-sell anchor).
    #[arg(long, default_value_t = 50u32)]
    ratchet_initial_offset_bps: u32,

    /// Ratchet: comma-separated `sl_bps_from_first` sweep — stop-loss
    /// offset from the first entry of a `Holding` cycle. Only fires
    /// while inventory is non-zero.
    #[arg(long, default_value = "75")]
    ratchet_sl_bps_list: String,

    /// Ratchet: trend-filter window in seconds. `0` disables the
    /// filter (orders always placed on both sides).
    #[arg(long, default_value_t = 300u32)]
    ratchet_trend_window_secs: u32,

    /// Ratchet: trend-filter threshold in bps. Suppresses the BUY
    /// side when mid has risen this much over the window (don't catch
    /// falling knives on a rip-up); mirror for ASK on rip-down.
    #[arg(long, default_value_t = 30u32)]
    ratchet_trend_filter_bps: u32,

    /// Ratchet: min elapsed time between order placements (ms).
    #[arg(long, default_value_t = 500u64)]
    ratchet_refresh_cooldown_ms: u64,

    /// Ratchet: hard inventory cap in USDT notional (per bot).
    #[arg(long, default_value = "500")]
    ratchet_max_pos_usdt: String,

    /// Ratchet pyramid: bps step between adds beyond the first entry.
    /// Each add is placed `pyramid_step_bps` past the previous add
    /// price. `0` disables (only first entry is placed).
    #[arg(long, default_value_t = 0u32)]
    ratchet_pyramid_step_bps: u32,

    /// Ratchet pyramid: maximum adds beyond the first entry. `0`
    /// disables the pyramid path (Ratchet stays single-entry).
    #[arg(long, default_value_t = 0u32)]
    ratchet_pyramid_max_adds: u32,

    /// Ratchet pyramid: size multiplier per add. Add n uses
    /// `notional × pyramid_size_mult^n`. `1.0` flat, `<1.0` decay,
    /// `>1.0` martingale (risky).
    #[arg(long, default_value = "1.0")]
    ratchet_pyramid_size_mult: String,

    /// Ratchet: TP bps from `avg_entry` while `Phase::Holding`.
    /// `0` falls back to `tp_bps_list` value (from first entry).
    #[arg(long, default_value_t = 0u32)]
    ratchet_tp_bps_from_avg: u32,

    /// SimpleGap sweep: comma-separated fixed gaps from mid, in bps.
    #[arg(long, default_value = "4")]
    simple_gap_bps_list: String,

    /// SimpleGap notional per order.
    #[arg(long, default_value = "100")]
    simple_gap_notional: String,

    /// LadderReentry notional per order.
    #[arg(long, default_value = "100")]
    ladder_reentry_notional: String,

    /// MicroMeanReversion notional per order.
    #[arg(long, default_value = "100")]
    micro_mean_reversion_notional: String,

    /// MicroMeanReversion sweep: comma-separated trigger distances in bps.
    #[arg(long, default_value = "8,10,12")]
    mmr_trigger_bps_list: String,

    /// MicroMeanReversion sweep: comma-separated passive entry distances in bps.
    #[arg(long, default_value = "1,2,3")]
    mmr_entry_bps_list: String,

    /// MicroMeanReversion sweep: comma-separated exit distances from fill in bps.
    #[arg(long, default_value = "4,6,8")]
    mmr_exit_bps_list: String,

    /// SpreadScalp notional per order.
    #[arg(long, default_value = "100")]
    spread_scalp_notional: String,

    /// SpreadScalp sweep: comma-separated min spread in bps.
    #[arg(long, default_value = "7")]
    spread_scalp_min_spread_bps_list: String,

    /// SpreadScalp / SpreadScalpOld: position cap in USDT notional.
    /// `0` (default) keeps the cap disabled — matches the legacy
    /// preset behaviour. Set non-zero to reproduce the Stage 4 cap-
    /// driven divergence between NEW and OLD.
    #[arg(long, default_value = "0")]
    spread_scalp_max_pos_usdt: String,
    /// SpreadScalp: take-profit threshold in bps of position notional.
    /// `0` disables. NEW only (OLD does not honour bps-of-notional —
    /// it uses the legacy absolute USDT path via take_profit_usdt).
    #[arg(long, default_value_t = 0u32)]
    spread_scalp_take_profit_bps: u32,

    /// SpreadScalp / SpreadScalpOld: absolute-USDT take-profit
    /// threshold (mirrors OLD's `take_profit_usdt`). `0` disables.
    /// When set with `spread_scalp_take_profit_bps == 0`, NEW falls
    /// back to this same threshold via the legacy USDT path.
    #[arg(long, default_value = "0")]
    spread_scalp_take_profit_usdt: String,
    /// SpreadScalp: stop-loss threshold in bps of position notional.
    /// `0` disables. NEW only (OLD has no SL path at all).
    #[arg(long, default_value_t = 0u32)]
    spread_scalp_stop_loss_bps: u32,
    /// SpreadScalp / SpreadScalpOld: per-side reject cooldown in ms.
    #[arg(long, default_value_t = 2000u64)]
    spread_scalp_reject_cooldown_ms: u64,
    /// SpreadScalp: adverse-selection window in ms. `0` disables the
    /// dynamic min_spread widening (NEW only).
    #[arg(long, default_value_t = 0u64)]
    spread_scalp_adverse_window_ms: u64,
    /// SpreadScalp adverse tracker: EMA half-life in fills.
    #[arg(long, default_value_t = 10u32)]
    spread_scalp_adverse_half_life_fills: u32,
    /// SpreadScalp adverse tracker: drift threshold (bps).
    #[arg(long, default_value = "3")]
    spread_scalp_adverse_threshold_bps: String,
    /// SpreadScalp adverse tracker: max widen surcharge (bps).
    #[arg(long, default_value_t = 10u32)]
    spread_scalp_adverse_max_widen_bps: u32,

    /// Perp funding rate per 8h in bps (signed). Default 1 (~0.01%/8h,
    /// typical Binance mid-cap). Positive = longs pay shorts. Set to 0
    /// to disable funding accrual entirely.
    #[arg(long, default_value_t = 1i32)]
    funding_bps_per_8h: i32,

    /// FillSim: submit-ack latency in ms. Bumping this from 0 exposes
    /// post-only crosses on fast moves (book ticks through our intended
    /// price between decision and ack). Realistic NA → AWS-Tokyo ~50ms.
    #[arg(long, default_value_t = 50u64)]
    sim_submit_latency_ms: u64,

    /// FillSim: cancel-ack latency in ms.
    #[arg(long, default_value_t = 10u64)]
    sim_cancel_latency_ms: u64,

    /// FillSim: synthetic `-2019` margin cap in USDT notional (signed
    /// position abs). `0` = unlimited.
    #[arg(long, default_value_t = 0.0_f64)]
    sim_max_position_notional: f64,

    /// Backtest balance compounding: initial wallet balance in USDT.
    /// `0` (default) disables compounding — per-order notional and
    /// per-bot cap stay static at the strategy-spec defaults. When set
    /// `> 0` together with `--sim-order-balance-pct > 0`, the runner
    /// tracks `balance = initial + realized − fees` per fill and pushes
    /// updated notional/cap to the strategy via the existing
    /// `on_notional_updated` / `on_max_position_updated` hooks.
    #[arg(long, default_value = "0")]
    sim_initial_balance: String,

    /// Backtest compounding: percent of running balance allocated per
    /// order (0-100). Mirrors the live account poller in
    /// `apps/tikr/src/main.rs`. `0` = disabled even when initial
    /// balance is set.
    #[arg(long, default_value = "0")]
    sim_order_balance_pct: String,

    /// Backtest compounding: percent of running balance used as the
    /// per-bot position cap (0-100). `0` keeps the static cap
    /// (`--sim-max-position-notional`).
    #[arg(long, default_value = "0")]
    sim_max_position_pct: String,

    /// FillSim: silent-cancel rate per minute per live quote (simulates
    /// venue cancel/expire events the WS misses; runner reconciliation
    /// eventually purges them). `0.0` = disabled.
    #[arg(long, default_value_t = 0.0_f64)]
    sim_silent_cancel_rate_per_min: f64,

    /// FillSim: deterministic RNG seed for silent-cancel rolls.
    #[arg(long, default_value_t = 0u64)]
    sim_rng_seed: u64,

    /// Comma-separated list of strategy categories to run. Empty
    /// (default) runs the full suite. Categories: avellaneda-stoikov,
    /// glft, top-of-book, micro-price, layered-grid, simple-gap,
    /// micro-mean-reversion, spread-scalp, spread-scalp-old,
    /// static-grid. Short aliases also accepted (as, tob, mp, lg,
    /// mmr, ss, ss-old, sg).
    #[arg(long, default_value = "")]
    strategies: String,
}

/// Parse `--strategies` into a normalised set. Empty input ⇒ `None`
/// (run all). Otherwise returns a set of canonical category names —
/// aliases ("as", "ss-old", etc.) are mapped to their canonical form.
fn parse_strategies(s: &str) -> Option<std::collections::HashSet<String>> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return None;
    }
    let set = trimmed
        .split(',')
        .map(|t| t.trim().to_ascii_lowercase())
        .filter(|t| !t.is_empty())
        .map(|t| match t.as_str() {
            "as" => "avellaneda-stoikov".to_string(),
            "tob" => "top-of-book".to_string(),
            "mp" => "micro-price".to_string(),
            "lg" => "layered-grid".to_string(),
            "mmr" => "micro-mean-reversion".to_string(),
            "ss" => "spread-scalp".to_string(),
            "ss-old" | "ssold" | "old" => "spread-scalp-old".to_string(),
            "sg" => "static-grid".to_string(),
            "lf" => "liq-fade".to_string(),
            "hk" => "hawk".to_string(),
            "hd" | "hy" => "hydra".to_string(),
            "rt" => "ratchet".to_string(),
            _ => t,
        })
        .collect();
    Some(set)
}

/// Read a TOML file of `kebab-case-key = value` (or `snake_case_key`)
/// pairs and convert each pair into a pair of CLI args `--key value`.
/// Lets `compare --config sweep.toml` replace 30+ CLI flags with a
/// versionable text file.
///
/// Recognised TOML value types:
/// - `bool` → emitted as `"true"` / `"false"` (clap parses these)
/// - integer / float → stringified
/// - string → emitted as-is
/// - array of integers/strings → joined with `,` (matches the existing
///   CSV-list arg style for sweep ranges, e.g. `lg-bps-list = "2,4,6"`
///   or `lg-bps-list = [2, 4, 6]` both work)
///
/// Nested tables are flattened — `[sg]` + `inner-bps-list = "3,5,8"`
/// becomes `--sg-inner-bps-list 3,5,8`. Lets the operator group
/// related knobs visually.
///
/// The args are injected before any CLI flags so clap's last-wins
/// behaviour means a CLI flag still overrides the TOML default — the
/// TOML is for sweep templates, CLI for one-off tweaks.
fn toml_to_args(path: &std::path::Path) -> Result<Vec<String>, String> {
    let raw = std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let value: toml::Value =
        toml::from_str(&raw).map_err(|e| format!("parse {}: {e}", path.display()))?;
    let table = value
        .as_table()
        .ok_or_else(|| format!("{}: top-level must be a TOML table", path.display()))?;
    let mut out: Vec<String> = Vec::new();
    flatten_table(table, "", &mut out);
    Ok(out)
}

/// Recursive helper for `toml_to_args`. `prefix` is the section path
/// joined by `-`. Empty prefix = top-level.
fn flatten_table(table: &toml::Table, prefix: &str, out: &mut Vec<String>) {
    for (key, val) in table {
        // Both kebab + snake forms accepted in the TOML; emit as kebab
        // (matches clap's flag style).
        let key_kebab = key.replace('_', "-");
        let flag = if prefix.is_empty() {
            format!("--{key_kebab}")
        } else {
            format!("--{prefix}-{key_kebab}")
        };
        match val {
            toml::Value::Table(inner) => {
                let new_prefix = if prefix.is_empty() {
                    key_kebab
                } else {
                    format!("{prefix}-{key_kebab}")
                };
                flatten_table(inner, &new_prefix, out);
            }
            toml::Value::Array(arr) => {
                // Join scalars with `,` to match the existing CSV-list
                // arg style. Tables-in-arrays are silently skipped —
                // not used by any current sweep schema.
                let joined: Vec<String> = arr
                    .iter()
                    .filter_map(|v| match v {
                        toml::Value::Integer(i) => Some(i.to_string()),
                        toml::Value::Float(f) => Some(f.to_string()),
                        toml::Value::String(s) => Some(s.clone()),
                        toml::Value::Boolean(b) => Some(b.to_string()),
                        _ => None,
                    })
                    .collect();
                out.push(flag);
                out.push(joined.join(","));
            }
            toml::Value::Boolean(b) => {
                out.push(flag);
                out.push(b.to_string());
            }
            toml::Value::Integer(i) => {
                out.push(flag);
                out.push(i.to_string());
            }
            toml::Value::Float(f) => {
                out.push(flag);
                out.push(f.to_string());
            }
            toml::Value::String(s) => {
                out.push(flag);
                out.push(s.clone());
            }
            toml::Value::Datetime(d) => {
                out.push(flag);
                out.push(d.to_string());
            }
        }
    }
}

/// Inclusion predicate: `None` allowlist ⇒ everything runs.
fn included(category: &str, allow: &Option<std::collections::HashSet<String>>) -> bool {
    match allow {
        None => true,
        Some(set) => set.contains(category),
    }
}

fn parse_u32_list(s: &str) -> Result<Vec<u32>, String> {
    s.split(',')
        .map(|t| t.trim())
        .filter(|t| !t.is_empty())
        .map(|t| t.parse::<u32>().map_err(|e| format!("bad u32 '{t}': {e}")))
        .collect()
}

fn parse_decimal_list(s: &str) -> Result<Vec<Decimal>, String> {
    s.split(',')
        .map(|t| t.trim())
        .filter(|t| !t.is_empty())
        .map(|t| Decimal::from_str(t).map_err(|e| format!("bad decimal '{t}': {e}")))
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    // `--config <path>` extends the CLI args from a TOML file BEFORE
    // clap parses, so the TOML's pairs land in argv and clap's
    // last-wins behaviour means an explicit CLI flag still overrides
    // the TOML default. Hand-scanned because `--config` itself
    // wouldn't be visible inside `Args` until after parsing.
    let mut argv: Vec<String> = std::env::args().collect();
    if let Some(idx) = argv.iter().position(|a| a == "--config") {
        if idx + 1 >= argv.len() {
            return Err("--config requires a path argument".into());
        }
        let path = argv.remove(idx + 1);
        argv.remove(idx);
        let toml_args = toml_to_args(std::path::Path::new(&path))?;
        if !toml_args.is_empty() {
            info!(
                path,
                count = toml_args.len(),
                "loaded sweep config from TOML"
            );
        }
        // Insert TOML-derived args after argv[0] (the binary name) so
        // they're earlier in clap's sequence than any explicit CLI
        // flag — clap's last-wins gives CLI the final say.
        argv.splice(1..1, toml_args);
    }
    let args = Args::parse_from(argv);

    // Lock the balance-compounding config before any spawn fires.
    let initial = Decimal::from_str(&args.sim_initial_balance)?;
    let order_pct = Decimal::from_str(&args.sim_order_balance_pct)?;
    let max_pct = Decimal::from_str(&args.sim_max_position_pct)?;
    let _ = BALANCE_COMPOUNDING.set((initial, order_pct, max_pct));

    if !args.data_root.is_empty() {
        return run_basket(args).await;
    }
    run_sweep(args).await
}

/// Discover symbol subdirs under `data_root`, filter via `symbols_filter`
/// when provided, and run a full sweep per symbol with the per-symbol
/// `data_dir` + `symbol` overrides. Prints each symbol's table inline
/// (delegated to `run_sweep` per call). Cross-symbol summary aggregation
/// lives here — currently a placeholder banner; deeper aggregation
/// lands when results are surfaced back to the basket layer.
async fn run_basket(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let root = std::path::Path::new(&args.data_root);
    if !root.exists() {
        return Err(format!("--data-root path not found: {}", root.display()).into());
    }
    let allow: Option<std::collections::HashSet<String>> = {
        let filter = args.symbols_filter.trim();
        if filter.is_empty() {
            None
        } else {
            Some(
                filter
                    .split(',')
                    .map(|s| s.trim().to_uppercase())
                    .filter(|s| !s.is_empty())
                    .collect(),
            )
        }
    };
    // Discover subdirs whose name ends in a known quote-asset suffix.
    let mut symbols: Vec<(String, PathBuf)> = Vec::new();
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        let upper = name.to_uppercase();
        let known_quote = ["USDT", "USDC", "BUSD", "TUSD"]
            .iter()
            .any(|q| upper.ends_with(q));
        if !known_quote {
            continue;
        }
        if let Some(ref a) = allow
            && !a.contains(&upper)
        {
            continue;
        }
        symbols.push((upper, path));
    }
    symbols.sort_by(|a, b| a.0.cmp(&b.0));
    if symbols.is_empty() {
        return Err(format!(
            "no symbol subdirs found under {} (filter={:?})",
            root.display(),
            allow
        )
        .into());
    }
    info!(
        symbol_count = symbols.len(),
        root = %root.display(),
        "basket sweep starting"
    );
    let mut per_symbol: Vec<(String, Vec<(String, PaperReport)>)> = Vec::new();
    for (i, (sym, dir)) in symbols.iter().enumerate() {
        let body = format!("[{}/{}] {sym}  ({})", i + 1, symbols.len(), dir.display());
        // Banner sized to the content so the right edge always aligns.
        let bar_len = body.chars().count() + 4;
        let bar: String = "═".repeat(bar_len);
        println!("\n╔{bar}╗");
        println!("║  {body}  ║");
        println!("╚{bar}╝");
        let mut sub = args.clone();
        sub.symbol = sym.clone();
        sub.data_dir = dir.clone();
        // Per-symbol liq subdir if the user pointed `--liq-data-dir` at
        // a basket root too. Same `<liq_root>/<SYMBOL>/` convention.
        if !sub.liq_data_dir.is_empty() {
            let liq_per_sym = std::path::Path::new(&sub.liq_data_dir).join(sym);
            if liq_per_sym.exists() {
                sub.liq_data_dir = liq_per_sym.to_string_lossy().to_string();
            }
        }
        // Clear data_root so the inner call doesn't recurse.
        sub.data_root = String::new();
        sub.symbols_filter = String::new();
        // Per-symbol equity CSV subdir so basket runs don't collide.
        if !sub.equity_csv_dir.is_empty() {
            sub.equity_csv_dir = std::path::Path::new(&sub.equity_csv_dir)
                .join(sym)
                .to_string_lossy()
                .to_string();
        }
        match run_sweep_collect(sub).await {
            Ok(results) => per_symbol.push((sym.clone(), results)),
            Err(e) => {
                eprintln!("WARN: symbol {sym} sweep failed: {e} — continuing");
                per_symbol.push((sym.clone(), Vec::new()));
            }
        }
    }

    // Cross-symbol summary: best (highest-NET) preset per symbol +
    // total basket NET. Skipped when only one symbol contributed
    // results (the per-symbol table already shows the same info).
    print_basket_summary(&per_symbol, args.sim_max_position_notional);
    Ok(())
}

/// Per-symbol best preset + basket NET sum. Empty result vectors
/// (symbol whose sweep failed) contribute zero to the basket but show
/// `—` in the per-symbol cells so the operator sees the gap.
fn print_basket_summary(per_symbol: &[(String, Vec<(String, PaperReport)>)], per_bot_cap: f64) {
    let non_empty = per_symbol.iter().filter(|(_, r)| !r.is_empty()).count();
    if non_empty < 2 {
        return;
    }
    println!();
    let body = "BASKET SUMMARY — best preset per symbol + total NET";
    let bar_len = body.chars().count() + 4;
    let bar: String = "═".repeat(bar_len);
    println!("╔{bar}╗");
    println!("║  {body}  ║");
    println!("╚{bar}╝");
    // Build rows for render_mysql_table — first column left-aligned
    // (symbol), rest right-aligned numeric. TOTAL row goes through the
    // same renderer so column widths stay consistent.
    let mut rows: Vec<Vec<String>> = Vec::with_capacity(per_symbol.len() + 1);
    let mut total_net = 0.0;
    let mut total_volume = 0.0;
    let mut sum_peak = 0.0;
    // ROI uses the operator's allocated capital per bot, NOT the peak
    // observed in the run. Allocation is what the wallet reserves; the
    // peak is just the high-water mark of how much of that allocation
    // got used. A bot that earns $5 on a $500 allocation has 1% ROI
    // regardless of whether it actually deployed all $500 or only $300
    // — the operator still tied up $500 of margin to run it. When
    // `per_bot_cap == 0` (operator passed no `--sim-max-position-notional`)
    // we fall back to peak so the cell still renders something useful.
    let cap_denom = if per_bot_cap > 0.0 { per_bot_cap } else { 0.0 };
    for (sym, results) in per_symbol {
        if let Some((name, report)) = results.iter().max_by(|(_, a), (_, b)| {
            decimal_to_f64(&a.net.0)
                .partial_cmp(&decimal_to_f64(&b.net.0))
                .unwrap_or(std::cmp::Ordering::Equal)
        }) {
            let net = decimal_to_f64(&report.net.0);
            let volume = decimal_to_f64(&report.buy_volume_usdt.0)
                + decimal_to_f64(&report.sell_volume_usdt.0);
            let peak = decimal_to_f64(&report.peak_position_usdt.0);
            let denom = if cap_denom > 0.0 { cap_denom } else { peak };
            let roi = if denom > 0.0 {
                format!("{:.3}", net / denom * 100.0)
            } else {
                "—".to_string()
            };
            total_net += net;
            total_volume += volume;
            sum_peak += peak;
            rows.push(vec![
                sym.clone(),
                format!("{:.4}", net),
                report.fills_emitted.to_string(),
                format!("{:.0}", volume),
                format!("{:.0}", peak),
                roi,
                name.clone(),
            ]);
        } else {
            rows.push(vec![
                sym.clone(),
                "—".to_string(),
                "—".to_string(),
                "—".to_string(),
                "—".to_string(),
                "—".to_string(),
                "(no results)".to_string(),
            ]);
        }
    }
    // Total ROI = total_net / (per_bot_cap × num_bots). Denominator is
    // the operator's wallet allocation — what they tied up to run the
    // basket, regardless of how much each bot actually deployed at peak.
    // Falls back to sum_peak when no explicit cap was set (matches
    // per-symbol fallback above).
    let total_capital = if cap_denom > 0.0 {
        cap_denom * per_symbol.len() as f64
    } else {
        sum_peak
    };
    let total_roi = if total_capital > 0.0 {
        format!("{:.3}", total_net / total_capital * 100.0)
    } else {
        "—".to_string()
    };
    rows.push(vec![
        "TOTAL".to_string(),
        format!("{:.4}", total_net),
        String::new(),
        format!("{:.0}", total_volume),
        format!("{:.0}", sum_peak),
        total_roi,
        String::new(),
    ]);
    let headers = [
        "symbol", "NET", "fills", "volume", "peak_pos", "ROI%", "preset",
    ];
    render_mysql_table(&headers, &rows);
    let roi_note = if cap_denom > 0.0 {
        format!(
            "ROI% = NET / allocated_capital  (per_bot_cap=${:.0}, basket={}×${:.0}=${:.0})",
            cap_denom,
            per_symbol.len(),
            cap_denom,
            total_capital,
        )
    } else {
        "ROI% = NET / peak_pos (no --sim-max-position-notional set; falling back to peak)"
            .to_string()
    };
    println!("(TOTAL row: volume + peak_pos summed; {roi_note})");
}

async fn run_sweep(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let _ = run_sweep_collect(args).await?;
    Ok(())
}

/// Body of `run_sweep`, but returns the sorted/filtered results so
/// basket mode can aggregate across symbols. Single-symbol path stays
/// at `run_sweep` for the void return + side-effect printing.
async fn run_sweep_collect(
    args: Args,
) -> Result<Vec<(String, PaperReport)>, Box<dyn std::error::Error>> {
    let (base_str, quote_str) = split_symbol(&args.symbol);
    let symbol = Symbol {
        base: Asset::new(base_str),
        quote: Asset::new(quote_str),
        venue: VenueId::new("binance"),
        kind: MarketKind::Perp,
    };

    let size_per_quote = Size(Decimal::from_str(&args.size)?);
    // Auto-detect tick + step when either is set to "auto". Static map
    // covers the major Binance USD-M perps; falls back to parquet sniff
    // for unknown symbols.
    let (auto_tick, auto_step) = if args.tick_size == "auto" || args.step_size == "auto" {
        match tikr_backtest::grid_detect::detect_grid(&args.data_dir, &args.symbol) {
            Ok((t, s)) => {
                info!(tick = %t, step = %s, symbol = %args.symbol, "auto-detected grid");
                (Some(t), Some(s))
            }
            Err(e) => {
                return Err(format!(
                    "auto-detect failed for {}: {e}; pass --tick-size + --step-size explicitly",
                    args.symbol
                )
                .into());
            }
        }
    } else {
        (None, None)
    };
    let tick = if args.tick_size == "auto" {
        auto_tick.unwrap()
    } else {
        Decimal::from_str(&args.tick_size)?
    };
    let lot_step = if args.step_size == "auto" {
        auto_step.unwrap()
    } else if args.step_size.trim().is_empty() {
        tick
    } else {
        Decimal::from_str(args.step_size.trim())?
    };
    let fees = VenueFees {
        maker_bps: args.maker_bps,
        taker_bps: args.taker_bps,
    };
    // Binance Futures USD-M with BNB-pay enabled = 10% discount on both
    // sides → 1.8 bps maker, 4.5 bps taker (rounded). See [[binance-fees]].
    let bnb_fees = VenueFees {
        maker_bps: ((args.maker_bps * 9) as f64 / 10.0).round() as i32,
        taker_bps: ((args.taker_bps as f64 * 0.9).round()) as u32,
    };
    let ewma = EwmaConfig {
        half_life_sec: 60.0,
        initial_var: Decimal::from_str("0.000001")?,
    };

    // Skim mode (per-preset, shared config). Disabled when budget==0.
    let skim_cfg: Option<SkimConfig> = if args.budget > 0.0 {
        Some(SkimConfig {
            budget: Decimal::try_from(args.budget)?,
            skim_pct: Decimal::try_from(args.skim_pct / 100.0)?,
            skim_ratio: Decimal::try_from(args.skim_ratio)?,
        })
    } else {
        None
    };
    // Perp funding model. Disabled when rate == 0. CLI takes integer bps per
    // 8h; convert to the per-interval fraction the runner expects.
    let funding_cfg: Option<FundingConfig> = if args.funding_bps_per_8h != 0 {
        Some(FundingConfig {
            interval_secs: 28_800,
            rate_per_interval: Decimal::from(args.funding_bps_per_8h) / Decimal::from(10_000),
        })
    } else {
        None
    };

    let sim_cfg_template = FillSimConfig {
        submit_latency_ms: args.sim_submit_latency_ms,
        cancel_latency_ms: args.sim_cancel_latency_ms,
        fees,
        max_position_notional_usdt: if args.sim_max_position_notional > 0.0 {
            Some(Decimal::try_from(args.sim_max_position_notional)?)
        } else {
            None
        },
        silent_cancel_rate_per_min: args.sim_silent_cancel_rate_per_min,
        rng_seed: args.sim_rng_seed,
    };
    let simple_gap_notional = Decimal::from_str(&args.simple_gap_notional)?;
    let ladder_reentry_notional = Decimal::from_str(&args.ladder_reentry_notional)?;
    let micro_mean_reversion_notional = Decimal::from_str(&args.micro_mean_reversion_notional)?;
    let spread_scalp_notional = Decimal::from_str(&args.spread_scalp_notional)?;
    let spread_scalp_max_pos = Decimal::from_str(&args.spread_scalp_max_pos_usdt)?;
    let spread_scalp_tp_usdt = Decimal::from_str(&args.spread_scalp_take_profit_usdt)?;
    let sg_regime_eff_threshold = Decimal::from_str(&args.sg_regime_efficiency_threshold)?;
    let sg_max_pos = Decimal::from_str(&args.sg_max_pos_usdt)?;
    let lg_max_pos = Decimal::from_str(&args.lg_max_pos_usdt)?;
    let liq_notional = Decimal::from_str(&args.liq_notional)?;
    let liq_arm_threshold = Decimal::from_str(&args.liq_arm_threshold_usdt)?;
    let liq_arm_dominance = Decimal::from_str(&args.liq_arm_dominance)?;
    let liq_max_pos = Decimal::from_str(&args.liq_max_pos_usdt)?;
    let spread_scalp_adverse_threshold =
        Decimal::from_str(&args.spread_scalp_adverse_threshold_bps)?;

    // Load + sort + validate parquet once; share across all presets via Arc.
    let load_start = std::time::Instant::now();
    let shared_data = LoadedReplayData::load(ReplayConfig {
        heartbeat_ms: args.heartbeat_ms,
        symbols: vec![symbol.clone()],
        data_dir: args.data_dir.clone(),
        tick_size: tick,
        allow_seq_gaps: true,
    })?;
    info!(
        events = shared_data.len(),
        elapsed_ms = load_start.elapsed().as_millis() as u64,
        "parquet load done"
    );

    // Spread-gate pre-check: scan the loaded book to derive `(median, max)`
    // observed top-of-book spread in bps. Any spread-gated preset whose
    // `min_spread_bps` exceeds the max-observed spread provably never
    // satisfies its gate on this dataset, so the runner pre-skips it +
    // prints a one-line summary instead of spawning a guaranteed 0-fill
    // task. `max_observed_spread_bps` is `Decimal::ZERO` when the dataset
    // has no completed snapshots (heartbeat-only fixtures).
    let (median_spread_bps, max_observed_spread_bps) = shared_data
        .book_spread_stats_bps()
        .unwrap_or((Decimal::ZERO, Decimal::ZERO));
    if max_observed_spread_bps > Decimal::ZERO {
        info!(
            median_bps = %median_spread_bps,
            max_bps = %max_observed_spread_bps,
            "book spread profile (presets with min_spread_bps > max will be skipped)"
        );
    }
    let mut skipped_presets: Vec<String> = Vec::new();

    // Equity-curve CSV root resolution. Create the dir up front so the
    // per-preset opens don't all race the mkdir. Empty arg → disabled.
    let equity_csv_dir: Option<PathBuf> = if args.equity_csv_dir.is_empty() {
        None
    } else {
        let p = PathBuf::from(&args.equity_csv_dir);
        if let Err(e) = std::fs::create_dir_all(&p) {
            eprintln!(
                "WARN: equity_csv_dir create failed: {} ({}); curve export disabled",
                p.display(),
                e
            );
            None
        } else {
            info!(dir = %p.display(), "equity curve CSV export enabled");
            Some(p)
        }
    };

    // Build all preset handles up front; each runs as a tokio task. The
    // multi-thread runtime fans them across cores. State dirs are unique
    // per preset (derived from the preset name) so concurrent snapshot /
    // resume writes don't collide.
    // JoinSet (vs Vec<JoinHandle>) so the await loop drains in
    // COMPLETION order — the progress line for a fast preset isn't
    // queued behind a slow earlier one. Per-preset wall-clock is
    // measured inside the spawned task + returned with the report.
    let mut handles: JoinSet<(String, PaperReport, std::time::Duration)> = JoinSet::new();
    let allow = parse_strategies(&args.strategies);
    if let Some(set) = &allow {
        info!(allowed = ?set, "strategy filter active — only listed categories will run");
    }
    // Install the global concurrency limiter on first call. Subsequent
    // sweeps (basket mode iterates per symbol) reuse the existing
    // semaphore — `set` only succeeds once. When `--parallel 0`,
    // SWEEP_LIMITER stays empty and every preset runs immediately.
    if args.parallel > 0 {
        let _ = SWEEP_LIMITER.set(Arc::new(tokio::sync::Semaphore::new(args.parallel)));
    }

    // Load liq parquet once if a dir is provided + LiqFade is requested.
    // The Vec is cloned into a fresh mpsc channel per preset so each LiqFade
    // sweep gets its own pre-loaded receiver.
    let liq_events: Vec<tikr_core::LiqEvent> =
        if !args.liq_data_dir.is_empty() && included("liq-fade", &allow) {
            let dir = std::path::Path::new(&args.liq_data_dir);
            match tikr_backtest::liq_replay::LiqEventStream::load(dir, &args.symbol) {
                Ok(s) => {
                    info!(
                        liq_events = s.len(),
                        dir = %dir.display(),
                        "loaded liq parquet for LiqFade"
                    );
                    s.into_events()
                }
                Err(e) => {
                    eprintln!(
                        "WARN: failed to load liq dir {}: {e}; LiqFade preset will see no liqs",
                        dir.display()
                    );
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        };

    if included("avellaneda-stoikov", &allow) {
        spawn_preset(
            &mut handles,
            &shared_data,
            &symbol,
            "A-S γ=0.1 5bps",
            AvellanedaStoikov::new(AvellanedaStoikovConfig {
                gamma: Decimal::from_str("0.1")?,
                base_spread_bps: 5,
                horizon_sec: 3600,
                size_per_quote,
                min_requote_interval_ms: 1000,
                level_step_bps: 1,
                volatility: ewma.clone(),
            }),
            fees,
            skim_cfg,
            funding_cfg,
            sim_cfg_template.clone(),
            equity_csv_dir.clone(),
        );
    }
    if included("glft", &allow) {
        spawn_preset(
            &mut handles,
            &shared_data,
            &symbol,
            "GLFT γ=0.1 5bps",
            Glft::new(GlftConfig {
                gamma: Decimal::from_str("0.1")?,
                base_spread_bps: 5,
                size_per_quote,
                min_requote_interval_ms: 1000,
                level_step_bps: 1,
                volatility: ewma.clone(),
            }),
            fees,
            skim_cfg,
            funding_cfg,
            sim_cfg_template.clone(),
            equity_csv_dir.clone(),
        );
    }
    if included("top-of-book", &allow) {
        spawn_preset(
            &mut handles,
            &shared_data,
            &symbol,
            "TOB improve=1 noskew",
            TopOfBook::new(TopOfBookConfig {
                size_per_quote,
                tick_size: tick,
                improve_when_spread_gt_ticks: 1,
                min_requote_interval_ms: 1000,
                max_skew_ticks: 0,
                skew_unit: Size(Decimal::from(1)),
                max_imbalance_ticks: 0,
            }),
            fees,
            skim_cfg,
            funding_cfg,
            sim_cfg_template.clone(),
            equity_csv_dir.clone(),
        );

        spawn_preset(
            &mut handles,
            &shared_data,
            &symbol,
            "TOB pure-join",
            TopOfBook::new(TopOfBookConfig {
                size_per_quote,
                tick_size: tick,
                improve_when_spread_gt_ticks: 1_000_000,
                min_requote_interval_ms: 1000,
                max_skew_ticks: 0,
                skew_unit: Size(Decimal::from(1)),
                max_imbalance_ticks: 0,
            }),
            fees,
            skim_cfg,
            funding_cfg,
            sim_cfg_template.clone(),
            equity_csv_dir.clone(),
        );

        spawn_preset(
            &mut handles,
            &shared_data,
            &symbol,
            "TOB improve=1 skew(10,0.005)",
            TopOfBook::new(TopOfBookConfig {
                size_per_quote,
                tick_size: tick,
                improve_when_spread_gt_ticks: 1,
                min_requote_interval_ms: 1000,
                max_skew_ticks: 10,
                skew_unit: Size(Decimal::from_str("0.005")?),
                max_imbalance_ticks: 0,
            }),
            fees,
            skim_cfg,
            funding_cfg,
            sim_cfg_template.clone(),
            equity_csv_dir.clone(),
        );

        spawn_preset(
            &mut handles,
            &shared_data,
            &symbol,
            "TOB improve=1 skew(20,0.005)",
            TopOfBook::new(TopOfBookConfig {
                size_per_quote,
                tick_size: tick,
                improve_when_spread_gt_ticks: 1,
                min_requote_interval_ms: 1000,
                max_skew_ticks: 20,
                skew_unit: Size(Decimal::from_str("0.005")?),
                max_imbalance_ticks: 0,
            }),
            fees,
            skim_cfg,
            funding_cfg,
            sim_cfg_template.clone(),
            equity_csv_dir.clone(),
        );

        for max_imb in [3u32, 5, 7, 10, 20] {
            let name = format!("TOB improve=1 imb({max_imb})");
            spawn_preset(
                &mut handles,
                &shared_data,
                &symbol,
                &name,
                TopOfBook::new(TopOfBookConfig {
                    size_per_quote,
                    tick_size: tick,
                    improve_when_spread_gt_ticks: 1,
                    min_requote_interval_ms: 1000,
                    max_skew_ticks: 0,
                    skew_unit: Size(Decimal::from(1)),
                    max_imbalance_ticks: max_imb,
                }),
                fees,
                skim_cfg,
                funding_cfg,
                sim_cfg_template.clone(),
                equity_csv_dir.clone(),
            );
        }

        spawn_preset(
            &mut handles,
            &shared_data,
            &symbol,
            "TOB improve=1 skew(10) + imb(5)",
            TopOfBook::new(TopOfBookConfig {
                size_per_quote,
                tick_size: tick,
                improve_when_spread_gt_ticks: 1,
                min_requote_interval_ms: 1000,
                max_skew_ticks: 10,
                skew_unit: Size(Decimal::from_str("0.005")?),
                max_imbalance_ticks: 5,
            }),
            fees,
            skim_cfg,
            funding_cfg,
            sim_cfg_template.clone(),
            equity_csv_dir.clone(),
        );

        spawn_preset(
            &mut handles,
            &shared_data,
            &symbol,
            "TOB improve=1 noskew (BNB)",
            TopOfBook::new(TopOfBookConfig {
                size_per_quote,
                tick_size: tick,
                improve_when_spread_gt_ticks: 1,
                min_requote_interval_ms: 1000,
                max_skew_ticks: 0,
                skew_unit: Size(Decimal::from(1)),
                max_imbalance_ticks: 0,
            }),
            bnb_fees,
            skim_cfg,
            funding_cfg,
            sim_cfg_template.clone(),
            equity_csv_dir.clone(),
        );

        spawn_preset(
            &mut handles,
            &shared_data,
            &symbol,
            "TOB improve=1 skew(10,0.005) (BNB)",
            TopOfBook::new(TopOfBookConfig {
                size_per_quote,
                tick_size: tick,
                improve_when_spread_gt_ticks: 1,
                min_requote_interval_ms: 1000,
                max_skew_ticks: 10,
                skew_unit: Size(Decimal::from_str("0.005")?),
                max_imbalance_ticks: 0,
            }),
            bnb_fees,
            skim_cfg,
            funding_cfg,
            sim_cfg_template.clone(),
            equity_csv_dir.clone(),
        );
    }

    // Micro-price sweep: half-spread 1/2/3/5 ticks. Direct comparable against
    // the TOB imbalance sweep — both react to top-of-book size imbalance, but
    // micro-price uses a continuous weighted mid instead of discrete tick shifts.
    if included("micro-price", &allow) {
        for half in [1u32, 2, 3, 5] {
            let name = format!("micro-price half={half}t");
            spawn_preset(
                &mut handles,
                &shared_data,
                &symbol,
                &name,
                MicroPrice::new(MicroPriceConfig {
                    size_per_quote,
                    tick_size: tick,
                    half_spread_ticks: half,
                    min_requote_interval_ms: 1000,
                    max_skew_ticks: 0,
                    skew_unit: Size(Decimal::from(1)),
                }),
                fees,
                skim_cfg,
                funding_cfg,
                sim_cfg_template.clone(),
                equity_csv_dir.clone(),
            );
        }

        // Micro-price + inventory skew combined.
        spawn_preset(
            &mut handles,
            &shared_data,
            &symbol,
            "micro-price half=2t skew(10,0.005)",
            MicroPrice::new(MicroPriceConfig {
                size_per_quote,
                tick_size: tick,
                half_spread_ticks: 2,
                min_requote_interval_ms: 1000,
                max_skew_ticks: 10,
                skew_unit: Size(Decimal::from_str("0.005")?),
            }),
            fees,
            skim_cfg,
            funding_cfg,
            sim_cfg_template.clone(),
            equity_csv_dir.clone(),
        );
    }

    // Layered grid sweep — re-entry-scalping ladder, fill-driven. Re-entry
    // bps dominates per-cycle PnL (must clear 2× maker fee or it's a loser).
    // notional_per_order is dollars per limit; coin qty = notional / price.
    // Levels-per-side sweep at the best re-entry from the prior sweep
    // (re=20 peaked on the 2h BTC sample). More levels = more capital
    // committed (each level adds $100 of resting orders on both sides),
    // but also more chances to catch the spread.
    if included("layered-grid", &allow) {
        let lg_bps_sweep = parse_u32_list(&args.lg_bps_list)?;
        let lg_levels_sweep = parse_u32_list(&args.lg_levels_list)?;
        for &bps in &lg_bps_sweep {
            for &levels in &lg_levels_sweep {
                let label = format!("LG bps={bps} lv={levels}");
                spawn_preset(
                    &mut handles,
                    &shared_data,
                    &symbol,
                    &label,
                    LayeredGrid::new(LayeredGridConfig {
                        notional_per_order: Decimal::from(100),
                        levels_per_side: levels,
                        inner_bps: bps,
                        max_position_usdt: lg_max_pos,
                        take_profit_bps: args.lg_take_profit_bps,
                        stop_loss_bps: args.lg_stop_loss_bps,
                    }),
                    fees,
                    skim_cfg,
                    funding_cfg,
                    sim_cfg_template.clone(),
                    equity_csv_dir.clone(),
                );
            }
        }
    }

    if included("ladder-reentry", &allow) {
        spawn_preset(
            &mut handles,
            &shared_data,
            &symbol,
            "LadderReentry in=5 st=1 lv=10 re=5 cont=11",
            LadderReentry::new(LadderReentryConfig {
                notional_per_order: ladder_reentry_notional,
                levels_per_side: 10,
                inner_bps: 5,
                step_bps: 1,
                reentry_bps: 5,
                continuation_bps: 11,
            }),
            fees,
            skim_cfg,
            funding_cfg,
            sim_cfg_template.clone(),
            equity_csv_dir.clone(),
        );
    }

    // SimpleGap — one fixed-distance bid/ask pair, then another pair after
    // every fill. No cancels, skew, requotes, or inventory logic.
    if included("simple-gap", &allow) {
        let simple_gap_sweep = parse_u32_list(&args.simple_gap_bps_list)?;
        for &gap in &simple_gap_sweep {
            let label = format!("SimpleGap gap={gap}bps");
            spawn_preset(
                &mut handles,
                &shared_data,
                &symbol,
                &label,
                SimpleGap::new(SimpleGapConfig {
                    notional_per_order: simple_gap_notional,
                    gap_bps: gap,
                }),
                fees,
                skim_cfg,
                funding_cfg,
                sim_cfg_template.clone(),
                equity_csv_dir.clone(),
            );
        }
    }

    if included("micro-mean-reversion", &allow) {
        let mmr_trigger_sweep = parse_u32_list(&args.mmr_trigger_bps_list)?;
        let mmr_entry_sweep = parse_u32_list(&args.mmr_entry_bps_list)?;
        let mmr_exit_sweep = parse_u32_list(&args.mmr_exit_bps_list)?;
        for &trigger in &mmr_trigger_sweep {
            for &entry in &mmr_entry_sweep {
                for &exit in &mmr_exit_sweep {
                    let label = format!("MMR trig={trigger} entry={entry} exit={exit}");
                    spawn_preset(
                        &mut handles,
                        &shared_data,
                        &symbol,
                        &label,
                        MicroMeanReversion::new(MicroMeanReversionConfig {
                            notional_per_order: micro_mean_reversion_notional,
                            trigger_bps: trigger,
                            entry_bps: entry,
                            exit_bps: exit,
                            max_open_entries: 1,
                        }),
                        fees,
                        skim_cfg,
                        funding_cfg,
                        sim_cfg_template.clone(),
                        equity_csv_dir.clone(),
                    );
                }
            }
        }
    }

    if included("spread-scalp", &allow) || included("spread-scalp-old", &allow) {
        let spread_scalp_spread_sweep = parse_decimal_list(&args.spread_scalp_min_spread_bps_list)?;
        for &min_spread_bps in &spread_scalp_spread_sweep {
            // Pre-skip: gate above max observed spread → 0-fill guaranteed.
            if max_observed_spread_bps > Decimal::ZERO && min_spread_bps > max_observed_spread_bps {
                skipped_presets.push(format!("SpreadScalp(*) spread>={min_spread_bps}bps"));
                continue;
            }
            if included("spread-scalp", &allow) {
                let label = format!("SpreadScalp spread>={min_spread_bps}bps");
                spawn_preset(
                    &mut handles,
                    &shared_data,
                    &symbol,
                    &label,
                    SpreadScalp::new(SpreadScalpConfig {
                        notional_per_order: spread_scalp_notional,
                        tick_size: tick,
                        step_size: lot_step,
                        min_notional: Decimal::ZERO,
                        min_spread_bps,
                        requote_interval_ms: 1000,
                        max_position_usdt: spread_scalp_max_pos,
                        take_profit_usdt: spread_scalp_tp_usdt,
                        reject_cooldown_ms: args.spread_scalp_reject_cooldown_ms,
                        price_tolerance_ticks: 1,
                        take_profit_bps: args.spread_scalp_take_profit_bps,
                        stop_loss_bps: args.spread_scalp_stop_loss_bps,
                        adverse: if args.spread_scalp_adverse_window_ms > 0 {
                            tikr_strategy::spread_scalp::adverse_tracker::AdverseConfig {
                                snapshot_window_ms: args.spread_scalp_adverse_window_ms,
                                ema_half_life_fills: args.spread_scalp_adverse_half_life_fills,
                                threshold_bps: spread_scalp_adverse_threshold,
                                max_widen_bps: args.spread_scalp_adverse_max_widen_bps,
                            }
                        } else {
                            tikr_strategy::spread_scalp::adverse_tracker::AdverseConfig::disabled()
                        },
                        close_side_always_quotes: args.spread_scalp_close_side_always_quotes,
                        close_decay_after_secs_1: args.spread_scalp_close_decay_after_secs_1,
                        close_decay_factor_1: Decimal::from_str(
                            &args.spread_scalp_close_decay_factor_1,
                        )?,
                        close_decay_after_secs_2: args.spread_scalp_close_decay_after_secs_2,
                        close_decay_factor_2: Decimal::from_str(
                            &args.spread_scalp_close_decay_factor_2,
                        )?,
                        adverse_stop_after_secs: args.spread_scalp_adverse_stop_after_secs,
                        adverse_stop_drift_bps: args.spread_scalp_adverse_stop_drift_bps,
                        quote_offset_ticks: args.spread_scalp_quote_offset_ticks,
                        close_target_ticks: args.spread_scalp_close_target_ticks,
                        strict_touch_quotes: false,
                    }),
                    fees,
                    skim_cfg,
                    funding_cfg,
                    sim_cfg_template.clone(),
                    equity_csv_dir.clone(),
                );
            }
            if included("spread-scalp-old", &allow) {
                // Pre-refactor SpreadScalpOld — A/B baseline against the new impl.
                let label_old = format!("SpreadScalpOLD spread>={min_spread_bps}bps");
                spawn_preset(
                    &mut handles,
                    &shared_data,
                    &symbol,
                    &label_old,
                    tikr_strategy::SpreadScalpOld::new(tikr_strategy::SpreadScalpOldConfig {
                        notional_per_order: spread_scalp_notional,
                        tick_size: tick,
                        // NOTE: OLD only supports take_profit_usdt (abs USDT)
                        // and reject_cooldown_ms. It has no take_profit_bps,
                        // stop_loss_bps, or adverse tracker. We translate
                        // the user-supplied bps TP into a rough abs USDT
                        // value (tp_bps × cap × 1e-4) so OLD has SOMETHING
                        // closing positions on the cap-hit scenario.
                        step_size: lot_step,
                        min_notional: Decimal::ZERO,
                        min_spread_bps,
                        requote_interval_ms: 1000,
                        max_position_usdt: spread_scalp_max_pos,
                        take_profit_usdt: if spread_scalp_tp_usdt > Decimal::ZERO {
                            spread_scalp_tp_usdt
                        } else if args.spread_scalp_take_profit_bps > 0
                            && spread_scalp_max_pos > Decimal::ZERO
                        {
                            // bps × cap / 10_000 ≈ absolute USDT threshold
                            // that bites at the same point as NEW's bps path.
                            Decimal::from(args.spread_scalp_take_profit_bps) * spread_scalp_max_pos
                                / Decimal::from(10_000)
                        } else {
                            Decimal::ZERO
                        },
                        reject_cooldown_ms: args.spread_scalp_reject_cooldown_ms,
                    }),
                    fees,
                    skim_cfg,
                    funding_cfg,
                    sim_cfg_template.clone(),
                    equity_csv_dir.clone(),
                );
            }
        }
    }

    // StaticGrid sweep — place-once-then-sit grid. Triggers a fresh batch
    // when remaining open quotes are <= 2 OR one side is empty. Pure passive
    // accumulation vs the rolling re-anchor of LG.
    if included("static-grid", &allow) {
        let sg_inner_sweep = parse_u32_list(&args.sg_inner_bps_list)?;
        let sg_step_sweep = parse_u32_list(&args.sg_step_bps_list)?;
        let sg_levels_sweep = parse_u32_list(&args.sg_levels_list)?;
        let sg_fpm_sweep = parse_decimal_list(&args.sg_target_fpm_list)?;
        let sg_fpm_window_sweep = parse_u32_list(&args.sg_fpm_window_list)?;
        let sg_scale_min_sweep = parse_decimal_list(&args.sg_scale_min_list)?;
        let sg_scale_max_sweep = parse_decimal_list(&args.sg_scale_max_list)?;
        for &inner in &sg_inner_sweep {
            for &step in &sg_step_sweep {
                for &levels in &sg_levels_sweep {
                    for &fpm_target in &sg_fpm_sweep {
                        for &fpm_window in &sg_fpm_window_sweep {
                            for &sc_min in &sg_scale_min_sweep {
                                for &sc_max in &sg_scale_max_sweep {
                                    if sc_min > sc_max {
                                        continue;
                                    }
                                    // Hide scaler knobs from the label when
                                    // they're at their defaults (fpm=0 means
                                    // adaptive off, window/scale_min/scale_max
                                    // are inert). Keeps the column narrow on
                                    // the common case where only inner/step/
                                    // levels vary.
                                    let mut label = format!("SG in={inner} st={step} lv={levels}");
                                    if fpm_target > Decimal::ZERO {
                                        use std::fmt::Write;
                                        let _ = write!(
                                            label,
                                            " fpm={fpm_target} w={fpm_window} sm={sc_min} sM={sc_max}"
                                        );
                                    }
                                    spawn_preset(
                                        &mut handles,
                                        &shared_data,
                                        &symbol,
                                        &label,
                                        StaticGrid::new(StaticGridConfig {
                                            notional_per_order: Decimal::from(100),
                                            levels_per_side: levels,
                                            inner_bps: inner,
                                            step_bps: step,
                                            step_size: lot_step,
                                            min_notional: Decimal::ZERO,
                                            target_fills_per_min: fpm_target,
                                            fillrate_window_secs: fpm_window,
                                            scale_min: sc_min,
                                            scale_max: sc_max,
                                            auto_skew: args.sg_auto_skew,
                                            regime_window_secs: args.sg_regime_window_secs,
                                            regime_trend_threshold_bps: args
                                                .sg_regime_trend_threshold_bps,
                                            regime_efficiency_threshold: sg_regime_eff_threshold,
                                            max_position_usdt: sg_max_pos,
                                            take_profit_bps: args.sg_take_profit_bps,
                                            stop_loss_bps: args.sg_stop_loss_bps,
                                        }),
                                        fees,
                                        skim_cfg,
                                        funding_cfg,
                                        sim_cfg_template.clone(),
                                        equity_csv_dir.clone(),
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Hawk — SS spread-gate + SG ladder + always-alive close-side.
    if included("hawk", &allow) {
        let hawk_notional = Decimal::from_str(&args.hawk_notional)?;
        let hawk_max_pos = Decimal::from_str(&args.hawk_max_pos_usdt)?;
        let hawk_levels = parse_u32_list(&args.hawk_levels_list)?;
        let hawk_inner = parse_u32_list(&args.hawk_inner_bps_list)?;
        let hawk_step = parse_u32_list(&args.hawk_step_bps_list)?;
        let hawk_min_spread = parse_decimal_list(&args.hawk_min_spread_bps_list)?;
        for &levels in &hawk_levels {
            for &inner in &hawk_inner {
                for &step in &hawk_step {
                    for &min_spread in &hawk_min_spread {
                        let label =
                            format!("Hawk lv={levels} in={inner} st={step} ms={min_spread}");
                        if max_observed_spread_bps > Decimal::ZERO
                            && min_spread > max_observed_spread_bps
                        {
                            skipped_presets.push(label);
                            continue;
                        }
                        spawn_preset(
                            &mut handles,
                            &shared_data,
                            &symbol,
                            &label,
                            Hawk::new(HawkConfig {
                                notional_per_order: hawk_notional,
                                tick_size: tick,
                                step_size: lot_step,
                                min_notional: Decimal::ZERO,
                                levels_per_side: levels,
                                inner_bps: inner,
                                step_bps: step,
                                min_spread_bps: min_spread,
                                max_position_usdt: hawk_max_pos,
                                close_target_bps: args.hawk_close_target_bps,
                                take_profit_bps: args.hawk_take_profit_bps,
                                stop_loss_bps: args.hawk_stop_loss_bps,
                            }),
                            fees,
                            skim_cfg,
                            funding_cfg,
                            sim_cfg_template.clone(),
                            equity_csv_dir.clone(),
                        );
                    }
                }
            }
        }
    }

    // Hydra — straddle-bracket entry + pyramid/DCA adds + bracketed exit.
    if included("hydra", &allow) {
        let hydra_max_pos = Decimal::from_str(&args.hydra_max_pos_usdt)?;
        // Notional derives from cap × pct — keeps the ratio explicit
        // so retuning the sweep stays scale-invariant (matches live
        // mode where both notional and cap auto-rescale with wallet).
        let hydra_notional_pct = Decimal::from_str(&args.hydra_notional_pct)?;
        let hydra_notional = hydra_max_pos * hydra_notional_pct / Decimal::from(100);
        let hydra_entry = parse_u32_list(&args.hydra_entry_offset_bps_list)?;
        let hydra_pyr = parse_u32_list(&args.hydra_pyramid_step_bps_list)?;
        let hydra_dca = parse_u32_list(&args.hydra_dca_step_bps_list)?;
        let hydra_pyr_mult = Decimal::from_str(&args.hydra_pyramid_size_mult)?;
        let hydra_dca_mult = Decimal::from_str(&args.hydra_dca_size_mult)?;
        for &entry_off in &hydra_entry {
            for &pyr_step in &hydra_pyr {
                for &dca_step in &hydra_dca {
                    let label = format!(
                        "Hydra eo={entry_off} pyr={pyr_step}x{} dca={dca_step}x{} tp={} sl={}",
                        args.hydra_pyramid_max_adds,
                        args.hydra_dca_max_adds,
                        args.hydra_tp_bps_from_avg,
                        args.hydra_sl_bps_from_first,
                    );
                    spawn_preset(
                        &mut handles,
                        &shared_data,
                        &symbol,
                        &label,
                        Hydra::new(HydraConfig {
                            notional_per_order: hydra_notional,
                            tick_size: tick,
                            step_size: lot_step,
                            min_notional: Decimal::ZERO,
                            entry_offset_bps: entry_off,
                            pyramid_step_bps: pyr_step,
                            pyramid_max_adds: args.hydra_pyramid_max_adds,
                            dca_step_bps: dca_step,
                            dca_max_adds: args.hydra_dca_max_adds,
                            tp_bps_from_avg: args.hydra_tp_bps_from_avg,
                            sl_bps_from_first: args.hydra_sl_bps_from_first,
                            max_position_usdt: hydra_max_pos,
                            add_cooldown_ms: args.hydra_add_cooldown_ms,
                            straddle_refresh_secs: args.hydra_straddle_refresh_secs,
                            straddle_drift_bps: args.hydra_straddle_drift_bps,
                            pyramid_size_mult: hydra_pyr_mult,
                            dca_size_mult: hydra_dca_mult,
                        }),
                        fees,
                        skim_cfg,
                        funding_cfg,
                        sim_cfg_template.clone(),
                        equity_csv_dir.clone(),
                    );
                }
            }
        }
    }

    // Ratchet — price-ratchet mean reversion. Places opposite-side
    // limit at last_fill ± tp_bps after each fill. Sweeps tp_bps and
    // sl_bps; other knobs single-valued via flags.
    if included("ratchet", &allow) {
        let r_max_pos = Decimal::from_str(&args.ratchet_max_pos_usdt)?;
        let r_notional_pct = Decimal::from_str(&args.ratchet_notional_pct)?;
        let r_notional = r_max_pos * r_notional_pct / Decimal::from(100);
        let r_tp_list = parse_u32_list(&args.ratchet_tp_bps_list)?;
        let r_sl_list = parse_u32_list(&args.ratchet_sl_bps_list)?;
        let r_pyr_mult = Decimal::from_str(&args.ratchet_pyramid_size_mult)?;
        for &tp_bps in &r_tp_list {
            for &sl_bps in &r_sl_list {
                let label = format!(
                    "Ratchet tp={tp_bps} sl={sl_bps} init={} trend={}s/{}bps cool={}ms pyr={}/{}@{}bps tpAvg={}",
                    args.ratchet_initial_offset_bps,
                    args.ratchet_trend_window_secs,
                    args.ratchet_trend_filter_bps,
                    args.ratchet_refresh_cooldown_ms,
                    args.ratchet_pyramid_max_adds,
                    r_pyr_mult,
                    args.ratchet_pyramid_step_bps,
                    args.ratchet_tp_bps_from_avg,
                );
                spawn_preset(
                    &mut handles,
                    &shared_data,
                    &symbol,
                    &label,
                    tikr_strategy::Ratchet::new(tikr_strategy::RatchetConfig {
                        tick_size: tick,
                        step_size: lot_step,
                        min_notional: Decimal::ZERO,
                        notional_per_order: r_notional,
                        tp_bps,
                        initial_offset_bps: args.ratchet_initial_offset_bps,
                        max_position_usdt: r_max_pos,
                        sl_bps_from_first: sl_bps,
                        trend_window_secs: args.ratchet_trend_window_secs,
                        trend_filter_bps: args.ratchet_trend_filter_bps,
                        refresh_cooldown_ms: args.ratchet_refresh_cooldown_ms,
                        pyramid_step_bps: args.ratchet_pyramid_step_bps,
                        pyramid_max_adds: args.ratchet_pyramid_max_adds,
                        pyramid_size_mult: r_pyr_mult,
                        tp_bps_from_avg: args.ratchet_tp_bps_from_avg,
                    }),
                    fees,
                    skim_cfg,
                    funding_cfg,
                    sim_cfg_template.clone(),
                    equity_csv_dir.clone(),
                );
            }
        }
    }

    // LiqFade — gated on `@forceOrder` cluster + price overshoot.
    // Skipped silently when no liq_data_dir was provided OR loading
    // returned zero events (warn already emitted above).
    if included("liq-fade", &allow) && !liq_events.is_empty() {
        let label = format!(
            "LiqFade arm={}/dom={}/capit={}bps/fade={}bps/tp={}bps",
            liq_arm_threshold,
            liq_arm_dominance,
            args.liq_capit_bps,
            args.liq_fade_offset_bps,
            args.liq_revert_target_bps
        );
        spawn_preset_with_liqs(
            &mut handles,
            &shared_data,
            &symbol,
            &label,
            LiqFade::new(LiqFadeConfig {
                notional_per_entry: liq_notional,
                tick_size: tick,
                step_size: lot_step,
                min_notional: Decimal::ZERO,
                max_position_usdt: liq_max_pos,
                arm_threshold_usdt: liq_arm_threshold,
                arm_dominance: liq_arm_dominance,
                capitulation_overshoot_bps: args.liq_capit_bps,
                fade_offset_bps: args.liq_fade_offset_bps,
                revert_target_bps: args.liq_revert_target_bps,
                entry_timeout_secs: args.liq_entry_timeout_secs,
                position_timeout_secs: args.liq_position_timeout_secs,
                stop_loss_bps: args.liq_stop_loss_bps,
            }),
            fees,
            skim_cfg,
            funding_cfg,
            sim_cfg_template.clone(),
            liq_events.clone(),
            args.liq_window_secs,
            equity_csv_dir.clone(),
        );
    }

    let sweep_start = std::time::Instant::now();
    let total = handles.len();
    if !skipped_presets.is_empty() {
        eprintln!(
            "pre-skip: {} preset(s) gated above max-observed spread (median={}bps, max={}bps) — skipping:",
            skipped_presets.len(),
            median_spread_bps.round_dp(3),
            max_observed_spread_bps.round_dp(3)
        );
        for name in &skipped_presets {
            eprintln!("  - {name}");
        }
    }
    info!(presets = total, "awaiting parallel preset completion");
    let mut results: Vec<(String, PaperReport)> = Vec::with_capacity(total);
    let mut crashed: Vec<String> = Vec::new();
    let mut done = 0usize;
    // JoinSet drains in completion order — fast presets get their
    // progress line as soon as they finish, even when an earlier-
    // spawned slow one is still running.
    while let Some(joined) = handles.join_next().await {
        match joined {
            Ok((name, report, preset_elapsed)) => {
                done += 1;
                let total_elapsed = sweep_start.elapsed().as_secs_f64();
                let eta_secs = if done > 0 {
                    (total_elapsed / done as f64) * (total - done) as f64
                } else {
                    0.0
                };
                eprintln!(
                    "[{}/{}] {} — {:.0}s — fills={} NET={:.2} — eta {}",
                    done,
                    total,
                    name,
                    preset_elapsed.as_secs_f64(),
                    report.fills_emitted,
                    decimal_to_f64(&report.net.0),
                    format_eta(eta_secs)
                );
                results.push((name, report));
            }
            Err(e) if e.is_panic() => {
                // Tokio captures panic payload; extract a short message
                // so the user sees what blew up without dragging the
                // whole sweep down with it.
                let msg = match e.try_into_panic() {
                    Ok(p) => {
                        if let Some(s) = p.downcast_ref::<String>() {
                            s.clone()
                        } else if let Some(s) = p.downcast_ref::<&str>() {
                            (*s).to_string()
                        } else {
                            "<unknown panic payload>".to_string()
                        }
                    }
                    Err(_) => "<panic, payload uncapturable>".to_string(),
                };
                eprintln!("WARN: preset CRASHED: {msg}");
                crashed.push(msg);
                done += 1;
            }
            Err(e) => {
                eprintln!("WARN: preset join error: {e}");
                crashed.push(e.to_string());
                done += 1;
            }
        }
    }
    info!(
        elapsed_ms = sweep_start.elapsed().as_millis() as u64,
        completed = results.len(),
        crashed = crashed.len(),
        "all presets done"
    );

    // Drop low-fill noise and (optionally) sort best-first so the
    // operator can scan the top of the table without grep.
    if args.min_fills > 0 {
        let before = results.len();
        results.retain(|(_, r)| r.fills_emitted >= args.min_fills);
        let dropped = before - results.len();
        if dropped > 0 {
            info!(
                dropped,
                min_fills = args.min_fills,
                "filtered low-fill presets"
            );
        }
    }
    if args.sort_by_net {
        results.sort_by(|(_, a), (_, b)| {
            decimal_to_f64(&b.net.0)
                .partial_cmp(&decimal_to_f64(&a.net.0))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    // Resolve baseline NET (if any) once — match is a substring search
    // on the preset name. First match wins. None when --baseline empty
    // or no preset matched (warn so the user notices a typo).
    let baseline_net: Option<f64> = if args.baseline.is_empty() {
        None
    } else {
        match results.iter().find(|(n, _)| n.contains(&args.baseline)) {
            Some((n, r)) => {
                let net = decimal_to_f64(&r.net.0);
                info!(baseline = %n, baseline_net = net, "baseline preset resolved");
                Some(net)
            }
            None => {
                eprintln!(
                    "WARN: --baseline {:?} matched no preset; ΔNET column omitted",
                    args.baseline
                );
                None
            }
        }
    };

    match args.output.as_str() {
        "csv" => print_csv(&args.symbol, &results),
        "markdown" | "md" => print_markdown(&args.symbol, &results),
        _ => print_table(&results, baseline_net),
    }
    if !crashed.is_empty() {
        eprintln!("\n{} preset(s) CRASHED during sweep:", crashed.len());
        for (i, msg) in crashed.iter().enumerate() {
            eprintln!("  [{}] {msg}", i + 1);
        }
    }
    Ok(results)
}

#[allow(clippy::too_many_arguments)]
async fn run_one<S: Strategy>(
    shared_data: Arc<LoadedReplayData>,
    symbol: Symbol,
    state_id: String,
    strategy: S,
    fees: VenueFees,
    skim: Option<SkimConfig>,
    funding: Option<FundingConfig>,
    sim_cfg: FillSimConfig,
    equity_csv_path: Option<PathBuf>,
) -> PaperReport {
    let replay = ParquetReplay::from_shared(shared_data);
    let venue = BacktestVenue::new(replay);
    let fill_sim = FillSim::new(FillSimConfig { fees, ..sim_cfg });
    let runner_config = RunnerConfig {
        state_dir: PathBuf::from(format!("./state/backtest_compare/{}", state_id)),
        // Equity-curve export needs snapshot ticks; default `0` (no
        // ticks) → no rows. Force a modest cadence when CSV requested
        // so the curve has resolution without spamming dashboards.
        snapshot_every_n_events: if equity_csv_path.is_some() { 1000 } else { 0 },
        skim,
        funding,
        snapshot_tap: None,
        live_tap: None,
        notional_rx: None,
        max_position_rx: None,
        liq_window_secs: 0,
        seed_position: None,
        equity_csv_path,
        initial_balance: balance_compounding().0,
        order_balance_pct: balance_compounding().1,
        max_position_pct: balance_compounding().2,
        min_notional: Decimal::ZERO,
        max_expected_open_orders: 2,
        liquidation: None,
    };
    let (_tx, rx) = watch::channel(false);
    let external_fills: Option<tokio::sync::mpsc::UnboundedReceiver<Fill>> = None;
    info!(strategy = strategy.name(), preset = %state_id, "preset start");
    let report = run_with_resume(
        venue,
        strategy,
        fill_sim,
        symbol,
        rx,
        runner_config,
        None,
        None,
        None,
        external_fills,
        None,
    )
    .await;
    info!(
        preset = %state_id,
        events = report.events_processed,
        fills = report.fills_emitted,
        "preset done"
    );
    report
}

/// LiqFade preset spawn — pre-loads the liq channel with all events
/// from `liq_events` before invoking `run_with_resume`. Distinct fn so
/// the (now bigger) run wrapper doesn't touch the existing
/// `spawn_preset` callers.
#[allow(clippy::too_many_arguments)]
fn spawn_preset_with_liqs<S: Strategy + Send + 'static>(
    handles: &mut JoinSet<(String, PaperReport, std::time::Duration)>,
    shared_data: &Arc<LoadedReplayData>,
    symbol: &Symbol,
    name: &str,
    strategy: S,
    fees: VenueFees,
    skim: Option<SkimConfig>,
    funding: Option<FundingConfig>,
    sim_cfg: FillSimConfig,
    liq_events: Vec<tikr_core::LiqEvent>,
    liq_window_secs: u32,
    equity_csv_dir: Option<PathBuf>,
) {
    let sd = Arc::clone(shared_data);
    let sym = symbol.clone();
    let display = name.to_string();
    let state_id = name
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect::<String>();
    let equity_csv_path = equity_csv_dir.map(|d| d.join(format!("{state_id}.csv")));
    handles.spawn(async move {
        let _permit = acquire_sweep_permit().await;
        let preset_start = std::time::Instant::now();
        // Pre-load the liq channel — events are sorted; the runner
        // timestamp-gates them on observe so the strategy only sees
        // those whose ts <= current event ts.
        let (liq_tx, liq_rx) = tokio::sync::mpsc::unbounded_channel::<tikr_core::LiqEvent>();
        for ev in liq_events {
            // Unbounded send; recv side reads when the runner ticks.
            let _ = liq_tx.send(ev);
        }
        drop(liq_tx);
        let replay = ParquetReplay::from_shared(sd);
        let venue = BacktestVenue::new(replay);
        let fill_sim = FillSim::new(FillSimConfig { fees, ..sim_cfg });
        let runner_config = RunnerConfig {
            state_dir: PathBuf::from(format!("./state/backtest_compare/{}", state_id)),
            snapshot_every_n_events: if equity_csv_path.is_some() { 1000 } else { 0 },
            skim,
            funding,
            snapshot_tap: None,
            live_tap: None,
            notional_rx: None,
            max_position_rx: None,
            liq_window_secs,
            seed_position: None,
            equity_csv_path,
            initial_balance: balance_compounding().0,
            order_balance_pct: balance_compounding().1,
            max_position_pct: balance_compounding().2,
            min_notional: Decimal::ZERO,
            max_expected_open_orders: 2,
            liquidation: None,
        };
        let (_tx, rx) = watch::channel(false);
        info!(strategy = strategy.name(), preset = %state_id, "preset start (liq-gated)");
        let report = run_with_resume(
            venue,
            strategy,
            fill_sim,
            sym,
            rx,
            runner_config,
            None,
            None,
            None,
            None,
            Some(liq_rx),
        )
        .await;
        info!(
            preset = %state_id,
            events = report.events_processed,
            fills = report.fills_emitted,
            "preset done (liq-gated)"
        );
        (display, report, preset_start.elapsed())
    });
}

/// Global semaphore set once by `run_sweep_collect` when `--parallel N`
/// is non-zero. Each spawned preset awaits an owned permit before
/// starting the heavy work, then drops it on completion — caps active
/// presets at N regardless of how many were spawned.
///
/// `None` when `--parallel 0` (default): no limit, every spawn runs
/// immediately on the tokio worker pool.
static SWEEP_LIMITER: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();

/// Acquire a permit from `SWEEP_LIMITER` if the limiter is set; returns
/// `None` when unlimited. Held permit is dropped when the returned
/// `Option<OwnedSemaphorePermit>` falls out of scope.
async fn acquire_sweep_permit() -> Option<tokio::sync::OwnedSemaphorePermit> {
    match SWEEP_LIMITER.get() {
        Some(sem) => Some(
            sem.clone()
                .acquire_owned()
                .await
                .expect("sweep limiter closed"),
        ),
        None => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_preset<S: Strategy + Send + 'static>(
    handles: &mut JoinSet<(String, PaperReport, std::time::Duration)>,
    shared_data: &Arc<LoadedReplayData>,
    symbol: &Symbol,
    name: &str,
    strategy: S,
    fees: VenueFees,
    skim: Option<SkimConfig>,
    funding: Option<FundingConfig>,
    sim_cfg: FillSimConfig,
    equity_csv_dir: Option<PathBuf>,
) {
    let sd = Arc::clone(shared_data);
    let sym = symbol.clone();
    let display = name.to_string();
    let state_id = name
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect::<String>();
    let equity_csv_path = equity_csv_dir.map(|d| d.join(format!("{state_id}.csv")));
    handles.spawn(async move {
        let _permit = acquire_sweep_permit().await;
        let preset_start = std::time::Instant::now();
        let r = run_one(
            sd,
            sym,
            state_id,
            strategy,
            fees,
            skim,
            funding,
            sim_cfg,
            equity_csv_path,
        )
        .await;
        (display, r, preset_start.elapsed())
    });
}

/// Compact ETA string for the per-preset progress line:
/// `<5s` / `42s` / `5m12s` / `1h22m`. Anything < 1s rounds to `<1s`.
fn format_eta(secs: f64) -> String {
    if !secs.is_finite() || secs <= 0.0 {
        return "<1s".to_string();
    }
    let s = secs as u64;
    if s < 5 {
        "<5s".to_string()
    } else if s < 60 {
        format!("{s}s")
    } else if s < 3600 {
        format!("{}m{:02}s", s / 60, s % 60)
    } else {
        format!("{}h{:02}m", s / 3600, (s % 3600) / 60)
    }
}

/// Comma-separated row per preset, with a header line. Numeric columns
/// emit decimals (no formatting) so downstream tooling can parse without
/// stripping currency symbols. `symbol` is repeated on every row so
/// basket-mode CSV streams stay row-addressable when concatenated.
fn print_csv(symbol: &str, results: &[(String, PaperReport)]) {
    println!(
        "symbol,preset,fills,fills_per_min,volume_usdt,peak_pos_usdt,realized,unrealized,fees,net,dollars_per_fill,roi_pct"
    );
    for (name, r) in results {
        let sim_min = (r.sim_duration_secs as f64) / 60.0;
        let fpm = if sim_min > 0.0 {
            r.fills_emitted as f64 / sim_min
        } else {
            0.0
        };
        let realized = decimal_to_f64(&r.realized.0);
        let unrealized = decimal_to_f64(&r.unrealized.0);
        let fees = decimal_to_f64(&r.fees.0);
        let net = decimal_to_f64(&r.net.0);
        let per_fill = if r.fills_emitted > 0 {
            net / r.fills_emitted as f64
        } else {
            0.0
        };
        let volume = decimal_to_f64(&r.buy_volume_usdt.0) + decimal_to_f64(&r.sell_volume_usdt.0);
        let peak = decimal_to_f64(&r.peak_position_usdt.0);
        let roi = if peak > 0.0 { net / peak * 100.0 } else { 0.0 };
        // CSV escape: wrap preset name in quotes if it contains a comma.
        let safe_name = if name.contains(',') {
            format!("\"{}\"", name.replace('"', "\"\""))
        } else {
            name.clone()
        };
        println!(
            "{symbol},{safe_name},{},{:.4},{:.4},{:.4},{:.6},{:.6},{:.6},{:.6},{:.6},{:.4}",
            r.fills_emitted, fpm, volume, peak, realized, unrealized, fees, net, per_fill, roi,
        );
    }
}

/// Github-flavoured Markdown table — paste-friendly for commit messages
/// or PR descriptions. Numeric formatting matches the table printer's
/// columns but with `|` separators + a header underline row.
fn print_markdown(symbol: &str, results: &[(String, PaperReport)]) {
    println!("### {symbol}");
    println!();
    println!(
        "| preset | fills | fills/min | volume | peak_pos | realized | unrealized | fees | NET | $/fill | ROI% |"
    );
    println!(
        "|--------|------:|----------:|-------:|---------:|---------:|-----------:|-----:|----:|-------:|-----:|"
    );
    for (name, r) in results {
        let sim_min = (r.sim_duration_secs as f64) / 60.0;
        let fpm = if sim_min > 0.0 {
            r.fills_emitted as f64 / sim_min
        } else {
            0.0
        };
        let realized = decimal_to_f64(&r.realized.0);
        let unrealized = decimal_to_f64(&r.unrealized.0);
        let fees = decimal_to_f64(&r.fees.0);
        let net = decimal_to_f64(&r.net.0);
        let per_fill = if r.fills_emitted > 0 {
            net / r.fills_emitted as f64
        } else {
            0.0
        };
        let volume = decimal_to_f64(&r.buy_volume_usdt.0) + decimal_to_f64(&r.sell_volume_usdt.0);
        let peak = decimal_to_f64(&r.peak_position_usdt.0);
        let roi = if peak > 0.0 {
            format!("{:.3}", net / peak * 100.0)
        } else {
            "—".to_string()
        };
        // Markdown escape: pipes inside cell text break the row.
        let safe_name = name.replace('|', "\\|");
        println!(
            "| {safe_name} | {} | {:.2} | {volume:.0} | {peak:.0} | {:.4} | {:.4} | {:.4} | {:.4} | {:.5} | {roi} |",
            r.fills_emitted, fpm, realized, unrealized, fees, net, per_fill,
        );
    }
    println!();
}

/// MySQL-CLI-style table renderer:
///
/// ```text
/// +-----------+-------+
/// | preset    | fills |
/// +-----------+-------+
/// | LG b=6 v=2|    29 |
/// +-----------+-------+
/// ```
///
/// First column is left-aligned (the preset name); remaining columns
/// are right-aligned (numerics). Column widths grow to fit the widest
/// cell in each. Headers + body must have the same column count.
fn render_mysql_table(headers: &[&str], rows: &[Vec<String>]) {
    let cols = headers.len();
    if cols == 0 {
        return;
    }
    let widths: Vec<usize> = (0..cols)
        .map(|i| {
            let h = headers[i].len();
            rows.iter()
                .map(|r| r.get(i).map(String::len).unwrap_or(0))
                .max()
                .unwrap_or(0)
                .max(h)
        })
        .collect();
    // Border: `+-(w+2)-+...+`
    let mut border = String::from("+");
    for &w in &widths {
        border.push_str(&"-".repeat(w + 2));
        border.push('+');
    }
    // Format one row given alignment per col (true = right, false = left).
    let row_line = |cells: &[String], rights: &[bool]| -> String {
        let mut s = String::from("|");
        for ((cell, w), right) in cells.iter().zip(widths.iter()).zip(rights.iter()) {
            if *right {
                s.push_str(&format!(" {:>w$} |", cell, w = w));
            } else {
                s.push_str(&format!(" {:<w$} |", cell, w = w));
            }
        }
        s
    };
    // Header: all left-aligned (matches MySQL).
    let header_align = vec![false; cols];
    // Body: column 0 left, rest right.
    let mut body_align = vec![true; cols];
    body_align[0] = false;
    let header_cells: Vec<String> = headers.iter().map(|s| s.to_string()).collect();
    println!("{border}");
    println!("{}", row_line(&header_cells, &header_align));
    println!("{border}");
    for row in rows {
        println!("{}", row_line(row, &body_align));
    }
    println!("{border}");
}

fn print_table(results: &[(String, PaperReport)], baseline_net: Option<f64>) {
    let skim_active = results
        .iter()
        .any(|(_, r)| r.skim_count > 0 || decimal_to_f64(&r.final_perp_balance.0) != 0.0);

    // Detect base asset label from any preset with skim active. Empty
    // (no skim) → header reads generic "base stack".
    let base_label = results
        .iter()
        .find_map(|(_, r)| {
            if r.base_asset.is_empty() {
                None
            } else {
                Some(format!("{} stack", r.base_asset))
            }
        })
        .unwrap_or_else(|| "base stack".to_string());

    // Build header + rows as Vec<Vec<String>> so a generic MySQL-style
    // renderer can compute column widths and draw the table once.
    let mut headers: Vec<&str> = Vec::new();
    let mut rows: Vec<Vec<String>> = Vec::with_capacity(results.len());
    if skim_active {
        headers.extend([
            "preset",
            "fills",
            "fills/min",
            "realized",
            "fees",
            "skims",
            base_label.as_str(),
            "perp+unreal",
            "TOTAL ACCT",
        ]);
        for (name, r) in results {
            let sim_min = (r.sim_duration_secs as f64) / 60.0;
            let fpm = if sim_min > 0.0 {
                r.fills_emitted as f64 / sim_min
            } else {
                0.0
            };
            let perp = decimal_to_f64(&r.final_perp_balance.0);
            let btc_v = decimal_to_f64(&r.final_base_value.0);
            rows.push(vec![
                name.clone(),
                r.fills_emitted.to_string(),
                format!("{fpm:.2}"),
                format!("{:.4}", decimal_to_f64(&r.realized.0)),
                format!("{:.4}", decimal_to_f64(&r.fees.0)),
                r.skim_count.to_string(),
                format!("{:.6}", decimal_to_f64(&r.base_stacked.0)),
                format!("{perp:.4}"),
                format!("{:.4}", perp + btc_v),
            ]);
        }
    } else {
        headers.extend([
            "preset",
            "fills",
            "fills/min",
            "volume",
            "peak_pos",
            "realized",
            "unrealized",
            "fees",
            "NET",
            "$/fill",
            "ROI%",
        ]);
        if baseline_net.is_some() {
            headers.push("ΔNET");
        }
        for (name, r) in results {
            let sim_min = (r.sim_duration_secs as f64) / 60.0;
            let fpm = if sim_min > 0.0 {
                r.fills_emitted as f64 / sim_min
            } else {
                0.0
            };
            let net = decimal_to_f64(&r.net.0);
            let dpf = if r.fills_emitted > 0 {
                net / r.fills_emitted as f64
            } else {
                0.0
            };
            let volume =
                decimal_to_f64(&r.buy_volume_usdt.0) + decimal_to_f64(&r.sell_volume_usdt.0);
            let peak = decimal_to_f64(&r.peak_position_usdt.0);
            // ROI% = NET / peak_position × 100 — return on the largest
            // capital deployed at any moment. `—` when peak is zero
            // (preset never opened a position).
            let roi = if peak > 0.0 {
                format!("{:.3}", net / peak * 100.0)
            } else {
                "—".to_string()
            };
            let mut row = vec![
                name.clone(),
                r.fills_emitted.to_string(),
                format!("{fpm:.2}"),
                format!("{volume:.0}"),
                format!("{peak:.0}"),
                format!("{:.4}", decimal_to_f64(&r.realized.0)),
                format!("{:.4}", decimal_to_f64(&r.unrealized.0)),
                format!("{:.4}", decimal_to_f64(&r.fees.0)),
                format!("{net:.4}"),
                format!("{dpf:.5}"),
                roi,
            ];
            if let Some(bn) = baseline_net {
                let delta = net - bn;
                // Prefix `+` on positive deltas so the sign is immediately
                // visible at a glance.
                let s = if delta >= 0.0 {
                    format!("+{delta:.4}")
                } else {
                    format!("{delta:.4}")
                };
                row.push(s);
            }
            rows.push(row);
        }
    }
    println!();
    render_mysql_table(&headers, &rows);
    // Stash name_width for the best/worst footer alignment below.
    let name_width = headers[0]
        .len()
        .max(rows.iter().map(|r| r[0].len()).max().unwrap_or(0));
    // Footer: best/worst NET.
    if let (Some(best), Some(worst)) = (
        results.iter().max_by(|a, b| {
            decimal_to_f64(&a.1.net.0)
                .partial_cmp(&decimal_to_f64(&b.1.net.0))
                .unwrap()
        }),
        results.iter().min_by(|a, b| {
            decimal_to_f64(&a.1.net.0)
                .partial_cmp(&decimal_to_f64(&b.1.net.0))
                .unwrap()
        }),
    ) {
        println!(
            "best:  {:<nw$} NET = {:>11.4}",
            best.0,
            decimal_to_f64(&best.1.net.0),
            nw = name_width,
        );
        println!(
            "worst: {:<nw$} NET = {:>11.4}",
            worst.0,
            decimal_to_f64(&worst.1.net.0),
            nw = name_width,
        );
    }
    println!();
}

fn decimal_to_f64(d: &Decimal) -> f64 {
    d.to_string().parse::<f64>().unwrap_or(0.0)
}

fn split_symbol(sym: &str) -> (&str, &str) {
    for suffix in &["USDT", "BUSD", "USDC", "TUSD"] {
        if let Some(base) = sym.strip_suffix(suffix)
            && !base.is_empty()
        {
            return (base, suffix);
        }
    }
    let n = sym.len();
    if n > 4 {
        (&sym[..n - 4], &sym[n - 4..])
    } else {
        (sym, "USDT")
    }
}

// ---------------------------------------------------------------------------
// BacktestVenue (mirrors run_backtest.rs)
// ---------------------------------------------------------------------------

struct BacktestVenue {
    replay: Mutex<Option<ParquetReplay>>,
}

impl BacktestVenue {
    fn new(replay: ParquetReplay) -> Self {
        Self {
            replay: Mutex::new(Some(replay)),
        }
    }
}

#[async_trait]
impl Venue for BacktestVenue {
    fn id(&self) -> &str {
        "backtest"
    }

    async fn snapshot(&self, _symbol: &Symbol) -> Result<Snapshot, VenueError> {
        Err(VenueError::Internal(Box::new(std::io::Error::other(
            "BacktestVenue::snapshot not supported",
        ))))
    }

    async fn subscribe(&self, _symbol: &Symbol) -> Result<BoxStream<'_, MarketEvent>, VenueError> {
        let replay = self.replay.lock().unwrap().take().ok_or_else(|| {
            VenueError::Internal(Box::new(std::io::Error::other(
                "BacktestVenue::subscribe called twice",
            )))
        })?;
        let s = stream::unfold(replay, |mut r| async move {
            use tikr_backtest::replay::Replay;
            r.next().await.map(|ev| (ev, r))
        });
        Ok(Box::pin(s))
    }

    async fn quote(&self, _intent: QuoteIntent) -> Result<QuoteId, VenueError> {
        Ok(QuoteId::new())
    }
    async fn requote(&self, _id: QuoteId, _intent: QuoteIntent) -> Result<(), VenueError> {
        Ok(())
    }
    async fn cancel(&self, _id: QuoteId) -> Result<(), VenueError> {
        Ok(())
    }
    async fn cancel_all(&self, _symbol: &Symbol) -> Result<(), VenueError> {
        Ok(())
    }
    async fn position(&self, symbol: &Symbol) -> Result<Position, VenueError> {
        Ok(Position {
            symbol: symbol.clone(),
            size: SignedSize(Decimal::ZERO),
            avg_entry: tikr_core::Price(Decimal::ZERO),
            realized_pnl: tikr_core::Notional(Decimal::ZERO),
        })
    }
    async fn fills_since(&self, _since_ts: u64) -> Result<Vec<Fill>, VenueError> {
        Ok(Vec::new())
    }
}
