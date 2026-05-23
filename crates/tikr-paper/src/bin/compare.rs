//! Run a fixed suite of strategy presets against the same recorded parquet
//! data and print a comparison table.
//!
//! Each preset gets a fresh `ParquetReplay` + `FillSim` + `run_with_resume`
//! pass, so results are apples-to-apples on identical historical events.

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

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
    AvellanedaStoikov, AvellanedaStoikovConfig, EwmaConfig, Glft, GlftConfig, LadderReentry,
    LadderReentryConfig, LayeredGrid, LayeredGridConfig, LiqFade, LiqFadeConfig,
    MicroMeanReversion, MicroMeanReversionConfig, MicroPrice, MicroPriceConfig, SimpleGap,
    SimpleGapConfig, SpreadScalp, SpreadScalpConfig, StaticGrid, StaticGridConfig, Strategy,
    TopOfBook, TopOfBookConfig,
};
use tikr_venue::{QuoteId, QuoteIntent, Venue, VenueError};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::info;

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

    /// LayeredGrid sweep: comma-separated `bps` values (single spacing param).
    #[arg(long, default_value = "2,4,6,8,10")]
    lg_bps_list: String,

    /// LayeredGrid sweep: comma-separated `levels` values.
    #[arg(long, default_value = "1,2,3,4,5")]
    lg_levels_list: String,

    /// StaticGrid sweep: comma-separated `inner_bps` values.
    #[arg(long, default_value = "3,6,10")]
    sg_inner_bps_list: String,

    /// StaticGrid sweep: comma-separated `step_bps` values.
    #[arg(long, default_value = "3,6")]
    sg_step_bps_list: String,

    /// StaticGrid sweep: comma-separated `levels_per_side` values.
    #[arg(long, default_value = "3,5")]
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
    #[arg(long, default_value = "5,7,10")]
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
            _ => t,
        })
        .collect();
    Some(set)
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
    let args = Args::parse();

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
        if let Some(ref a) = allow {
            if !a.contains(&upper) {
                continue;
            }
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
    for (i, (sym, dir)) in symbols.iter().enumerate() {
        println!(
            "\n╔══════════════════════════════════════════════════════════════╗"
        );
        println!(
            "║ [{}/{}] {sym}  ({})", i + 1, symbols.len(), dir.display()
        );
        println!(
            "╚══════════════════════════════════════════════════════════════╝"
        );
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
        if let Err(e) = run_sweep(sub).await {
            eprintln!("WARN: symbol {sym} sweep failed: {e} — continuing");
        }
    }
    Ok(())
}

async fn run_sweep(args: Args) -> Result<(), Box<dyn std::error::Error>> {
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
    // Perp funding model. Disabled when rate == 0.
    let funding_cfg: Option<FundingConfig> = if args.funding_bps_per_8h != 0 {
        Some(FundingConfig {
            rate_bps_per_8h: args.funding_bps_per_8h,
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

    // Build all preset handles up front; each runs as a tokio task. The
    // multi-thread runtime fans them across cores. State dirs are unique
    // per preset (derived from the preset name) so concurrent snapshot /
    // resume writes don't collide.
    let mut handles: Vec<JoinHandle<(String, PaperReport)>> = Vec::new();
    let allow = parse_strategies(&args.strategies);
    if let Some(set) = &allow {
        info!(allowed = ?set, "strategy filter active — only listed categories will run");
    }

    // Load liq parquet once if a dir is provided + LiqFade is requested.
    // The Vec is cloned into a fresh mpsc channel per preset so each LiqFade
    // sweep gets its own pre-loaded receiver.
    let liq_events: Vec<tikr_core::LiqEvent> = if !args.liq_data_dir.is_empty()
        && included("liq-fade", &allow)
    {
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
                    );
                }
            }
        }
    }

    if included("spread-scalp", &allow) || included("spread-scalp-old", &allow) {
        let spread_scalp_spread_sweep = parse_decimal_list(&args.spread_scalp_min_spread_bps_list)?;
        for &min_spread_bps in &spread_scalp_spread_sweep {
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
                    }),
                    fees,
                    skim_cfg,
                    funding_cfg,
                    sim_cfg_template.clone(),
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
                                    let label = format!(
                                        "SG in={inner} st={step} lv={levels} fpm={fpm_target} w={fpm_window} sm={sc_min} sM={sc_max}",
                                    );
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
                                    );
                                }
                            }
                        }
                    }
                }
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
        );
    }

    let sweep_start = std::time::Instant::now();
    info!(
        presets = handles.len(),
        "awaiting parallel preset completion"
    );
    let mut results: Vec<(String, PaperReport)> = Vec::with_capacity(handles.len());
    let mut crashed: Vec<String> = Vec::new();
    for h in handles {
        match h.await {
            Ok(pair) => results.push(pair),
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
            }
            Err(e) => {
                eprintln!("WARN: preset join error: {e}");
                crashed.push(e.to_string());
            }
        }
    }
    info!(
        elapsed_ms = sweep_start.elapsed().as_millis() as u64,
        completed = results.len(),
        crashed = crashed.len(),
        "all presets done"
    );

    match args.output.as_str() {
        "csv" => print_csv(&args.symbol, &results),
        "markdown" | "md" => print_markdown(&args.symbol, &results),
        _ => print_table(&results),
    }
    if !crashed.is_empty() {
        eprintln!("\n{} preset(s) CRASHED during sweep:", crashed.len());
        for (i, msg) in crashed.iter().enumerate() {
            eprintln!("  [{}] {msg}", i + 1);
        }
    }
    Ok(())
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
) -> PaperReport {
    let replay = ParquetReplay::from_shared(shared_data);
    let venue = BacktestVenue::new(replay);
    let fill_sim = FillSim::new(FillSimConfig { fees, ..sim_cfg });
    let runner_config = RunnerConfig {
        state_dir: PathBuf::from(format!("./state/backtest_compare/{}", state_id)),
        snapshot_every_n_events: 0,
        skim,
        funding,
        snapshot_tap: None,
        live_tap: None,
        notional_rx: None,
        liq_window_secs: 0,
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

#[allow(clippy::too_many_arguments)]
/// LiqFade preset spawn — pre-loads the liq channel with all events
/// from `liq_events` before invoking `run_with_resume`. Distinct fn so
/// the (now bigger) run wrapper doesn't touch the existing
/// `spawn_preset` callers.
#[allow(clippy::too_many_arguments)]
fn spawn_preset_with_liqs<S: Strategy + Send + 'static>(
    handles: &mut Vec<JoinHandle<(String, PaperReport)>>,
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
) {
    let sd = Arc::clone(shared_data);
    let sym = symbol.clone();
    let display = name.to_string();
    let state_id = name
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect::<String>();
    handles.push(tokio::spawn(async move {
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
            snapshot_every_n_events: 0,
            skim,
            funding,
            snapshot_tap: None,
            live_tap: None,
            notional_rx: None,
            liq_window_secs,
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
        (display, report)
    }));
}

fn spawn_preset<S: Strategy + Send + 'static>(
    handles: &mut Vec<JoinHandle<(String, PaperReport)>>,
    shared_data: &Arc<LoadedReplayData>,
    symbol: &Symbol,
    name: &str,
    strategy: S,
    fees: VenueFees,
    skim: Option<SkimConfig>,
    funding: Option<FundingConfig>,
    sim_cfg: FillSimConfig,
) {
    let sd = Arc::clone(shared_data);
    let sym = symbol.clone();
    let display = name.to_string();
    let state_id = name
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect::<String>();
    handles.push(tokio::spawn(async move {
        let r = run_one(sd, sym, state_id, strategy, fees, skim, funding, sim_cfg).await;
        (display, r)
    }));
}

/// Comma-separated row per preset, with a header line. Numeric columns
/// emit decimals (no formatting) so downstream tooling can parse without
/// stripping currency symbols. `symbol` is repeated on every row so
/// basket-mode CSV streams stay row-addressable when concatenated.
fn print_csv(symbol: &str, results: &[(String, PaperReport)]) {
    println!(
        "symbol,preset,fills,fills_per_min,realized,unrealized,fees,net,dollars_per_fill"
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
        // CSV escape: wrap preset name in quotes if it contains a comma.
        let safe_name = if name.contains(',') {
            format!("\"{}\"", name.replace('"', "\"\""))
        } else {
            name.clone()
        };
        println!(
            "{symbol},{safe_name},{},{:.4},{:.6},{:.6},{:.6},{:.6},{:.6}",
            r.fills_emitted, fpm, realized, unrealized, fees, net, per_fill,
        );
    }
}

/// Github-flavoured Markdown table — paste-friendly for commit messages
/// or PR descriptions. Numeric formatting matches the table printer's
/// columns but with `|` separators + a header underline row.
fn print_markdown(symbol: &str, results: &[(String, PaperReport)]) {
    println!("### {symbol}");
    println!();
    println!("| preset | fills | fills/min | realized | unrealized | fees | NET | $/fill |");
    println!("|--------|------:|----------:|---------:|-----------:|-----:|----:|-------:|");
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
        // Markdown escape: pipes inside cell text break the row.
        let safe_name = name.replace('|', "\\|");
        println!(
            "| {safe_name} | {} | {:.2} | {:.4} | {:.4} | {:.4} | {:.4} | {:.5} |",
            r.fills_emitted, fpm, realized, unrealized, fees, net, per_fill,
        );
    }
    println!();
}

fn print_table(results: &[(String, PaperReport)]) {
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

    println!();
    if skim_active {
        println!(
            "{:<36} {:>7} {:>9} {:>11} {:>11} {:>6} {:>11} {:>12} {:>12}",
            "preset",
            "fills",
            "fills/min",
            "realized",
            "fees",
            "skims",
            base_label,
            "perp+unreal",
            "TOTAL ACCT"
        );
        println!("{}", "-".repeat(120));
    } else {
        println!(
            "{:<36} {:>7} {:>9} {:>11} {:>10} {:>11} {:>11} {:>11}",
            "preset", "fills", "fills/min", "realized", "unrealized", "fees", "NET", "$/fill"
        );
        println!("{}", "-".repeat(110));
    }
    for (name, r) in results {
        // Use sim_duration (data-time span) not runtime_secs (wall-clock
        // replay speed) so fills/min reflects market-time throughput.
        let sim_min = (r.sim_duration_secs as f64) / 60.0;
        let fills_per_min = if sim_min > 0.0 {
            r.fills_emitted as f64 / sim_min
        } else {
            0.0
        };
        let net = decimal_to_f64(&r.net.0);
        if skim_active {
            let perp = decimal_to_f64(&r.final_perp_balance.0);
            let btc_v = decimal_to_f64(&r.final_base_value.0);
            let total = perp + btc_v;
            println!(
                "{:<36} {:>7} {:>9.2} {:>11.4} {:>11.4} {:>6} {:>10.6} {:>12.4} {:>12.4}",
                name,
                r.fills_emitted,
                fills_per_min,
                decimal_to_f64(&r.realized.0),
                decimal_to_f64(&r.fees.0),
                r.skim_count,
                decimal_to_f64(&r.base_stacked.0),
                perp,
                total,
            );
        } else {
            let dollars_per_fill = if r.fills_emitted > 0 {
                net / r.fills_emitted as f64
            } else {
                0.0
            };
            println!(
                "{:<36} {:>7} {:>9.2} {:>11.4} {:>10.4} {:>11.4} {:>11.4} {:>11.5}",
                name,
                r.fills_emitted,
                fills_per_min,
                decimal_to_f64(&r.realized.0),
                decimal_to_f64(&r.unrealized.0),
                decimal_to_f64(&r.fees.0),
                net,
                dollars_per_fill,
            );
        }
    }
    println!();
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
            "best:  {:<36} NET = {:>11.4}",
            best.0,
            decimal_to_f64(&best.1.net.0)
        );
        println!(
            "worst: {:<36} NET = {:>11.4}",
            worst.0,
            decimal_to_f64(&worst.1.net.0)
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
