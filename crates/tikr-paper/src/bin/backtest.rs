//! Run a tikr strategy against recorded parquet market data.
//!
//! Loads `book_<SYM>_*.parquet` + `trades_<SYM>_*.parquet` from the data
//! directory via [`tikr_backtest::replay::ParquetReplay`], wraps the stream
//! in a no-op [`BacktestVenue`], and drives it through `tikr_paper::run_with_resume`
//! against the chosen strategy. Emits a `PaperReport` JSON on completion.
//!
//! Backtest is paper-mode only: write-side venue calls are recorded but not
//! dispatched, and fills come from `FillSim` (not external_fills).

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Mutex;

use async_trait::async_trait;
use clap::{Parser, ValueEnum};
use futures::stream::{self, BoxStream};
use tikr_backtest::fill_sim::{FillSim, FillSimConfig, VenueFees};
use tikr_backtest::liquidation::LiquidationConfig;
use tikr_backtest::replay::{ParquetReplay, Replay, ReplayConfig};
use tikr_core::{
    Asset, Decimal, Fill, MarketEvent, MarketKind, Position, SignedSize, Size, Snapshot, Symbol,
    VenueId,
};
use tikr_paper::{FundingConfig, RunnerConfig, run_with_resume};
use tikr_strategy::{
    AvellanedaStoikov, AvellanedaStoikovConfig, EwmaConfig, Glft, GlftConfig, Mantis, MantisConfig,
    MicroPrice, MicroPriceConfig, Strategy, Tide, TideConfig, TopOfBook, TopOfBookConfig, Wave,
    WaveConfig,
};
use tikr_venue::{QuoteId, QuoteIntent, Venue, VenueError};
use tokio::sync::watch;
use tracing::info;

/// Venue environment for `--autodetect-filters` exchangeInfo lookups. Local
/// (not `tikr_binance::BinanceEnv`) because tikr-binance depends on tikr-paper,
/// so this crate can't depend back on it — the fetch is done inline here.
#[derive(Debug, Clone, Copy, ValueEnum)]
enum EnvArg {
    #[value(name = "spot-mainnet")]
    SpotMainnet,
    #[value(name = "futures-mainnet")]
    FuturesMainnet,
    #[value(name = "spot-testnet")]
    SpotTestnet,
    #[value(name = "futures-testnet")]
    FuturesTestnet,
}

impl EnvArg {
    fn base_url(self) -> &'static str {
        match self {
            EnvArg::SpotMainnet => "https://api.binance.com",
            EnvArg::FuturesMainnet => "https://fapi.binance.com",
            EnvArg::SpotTestnet => "https://testnet.binance.vision",
            EnvArg::FuturesTestnet => "https://testnet.binancefuture.com",
        }
    }
    fn is_futures(self) -> bool {
        matches!(self, EnvArg::FuturesMainnet | EnvArg::FuturesTestnet)
    }
}

/// Fetch `exchangeInfo` for `env` and return `(tick_size, step_size,
/// min_notional)` strings for `symbol`, or `None` if the symbol isn't listed
/// or the fetch/parse fails. Inline (no tikr-binance dep — see [`EnvArg`]).
async fn autodetect_filters(env: EnvArg, symbol: &str) -> Option<(String, String, String)> {
    let path = if env.is_futures() {
        "/fapi/v1/exchangeInfo"
    } else {
        "/api/v3/exchangeInfo"
    };
    let url = format!("{}{}", env.base_url(), path);
    let resp = reqwest::Client::new().get(&url).send().await.ok()?;
    let json: serde_json::Value = resp.json().await.ok()?;
    let want = symbol.to_uppercase();
    let syms = json.get("symbols")?.as_array()?;
    let s = syms
        .iter()
        .find(|s| s.get("symbol").and_then(|v| v.as_str()) == Some(want.as_str()))?;
    let filters = s.get("filters")?.as_array()?;
    let mut tick = None;
    let mut step = None;
    let mut min_notional = None;
    for f in filters {
        match f.get("filterType").and_then(|v| v.as_str()) {
            Some("PRICE_FILTER") => {
                tick = f.get("tickSize").and_then(|v| v.as_str()).map(String::from)
            }
            Some("LOT_SIZE") => step = f.get("stepSize").and_then(|v| v.as_str()).map(String::from),
            Some("MIN_NOTIONAL") | Some("NOTIONAL") => {
                min_notional = f.get("notional").and_then(|v| v.as_str()).map(String::from)
            }
            _ => {}
        }
    }
    Some((tick?, step?, min_notional?))
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum StrategyArg {
    #[value(name = "avellaneda-stoikov", alias = "as")]
    AvellanedaStoikov,
    #[value(name = "glft")]
    Glft,
    #[value(name = "top-of-book", alias = "tob")]
    TopOfBook,
    #[value(name = "micro-price", alias = "mp")]
    MicroPrice,
    #[value(name = "tide", alias = "td")]
    Tide,
    #[value(name = "wave", alias = "wv")]
    Wave,
    #[value(name = "mantis", alias = "mn")]
    Mantis,
}

#[derive(Parser, Debug)]
#[command(
    name = "backtest",
    about = "Replay recorded parquet data through a strategy and emit a P&L report"
)]
struct Args {
    /// Directory containing `book_<SYM>_*.parquet` + `trades_<SYM>_*.parquet`.
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,

    /// Binance-style symbol (e.g. `BTCUSDT`). Base+quote split via 4-char
    /// suffix heuristic (USDT/USDC/BUSD/TUSD).
    #[arg(long, default_value = "BTCUSDT")]
    symbol: String,

    /// Strategy to run.
    #[arg(long, value_enum, default_value = "avellaneda-stoikov")]
    strategy: StrategyArg,

    /// Auto-detect price tick / lot step / min-notional from Binance
    /// `exchangeInfo` for `--symbol` (overrides --tick-size and the per-
    /// strategy step/min-notional flags). On by default; needs network. Use
    /// `--no-autodetect-filters` to rely purely on the CLI values (offline).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    autodetect_filters: bool,

    /// Venue env used only for `--autodetect-filters` exchangeInfo lookup.
    /// Default `futures-mainnet` (USDC/USDT perps).
    #[arg(long, value_enum, default_value = "futures-mainnet")]
    venue_env: EnvArg,

    /// Order size per quote.
    #[arg(long, default_value = "0.001")]
    size: String,

    /// Maker fee in basis points (default = Binance Futures USD-M tier 0).
    /// Negative values mean rebate.
    #[arg(long, default_value_t = 2i32)]
    maker_bps: i32,

    /// Taker fee in basis points.
    #[arg(long, default_value_t = 5u32)]
    taker_bps: u32,

    /// Order submit (and cancel) latency in ms. Models the book moving
    /// while an order is in flight: a post-only that crosses the touch on
    /// arrival is rejected (Binance -5022), exercising on_quote_rejected, and
    /// a cancel that lands late can still get filled. Default `100` — a
    /// realistic retail WS→decision→REST round-trip to Binance; passing `0`
    /// gives the optimistic instant-fill model (not recommended).
    #[arg(long, default_value_t = 100u64)]
    submit_latency_ms: u64,

    /// Mean exponential latency jitter (ms) added per op on top of
    /// `--submit-latency-ms`. Models network jitter + occasional spikes
    /// (the exponential tail). Default `50`; `0` = fixed latency, deterministic.
    #[arg(long, default_value_t = 50u64)]
    submit_latency_jitter_ms: u64,

    /// Measure real round-trip latency to the venue at startup (10 pings) and
    /// use the mean as submit/cancel latency and the stddev as jitter,
    /// OVERRIDING --submit-latency-ms / --submit-latency-jitter-ms. On by
    /// default so a run always reflects this machine's actual link to Binance.
    /// `--no-measure-latency` keeps the static CLI values (offline runs).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    measure_latency: bool,

    /// Funding rate per interval as a fraction (e.g. 0.0001 = 1bp/8h).
    /// Positive = longs pay shorts. `0` (default) = funding off. Charged on
    /// open inventory every `--funding-interval-secs`, mirroring perp funding.
    #[arg(long, default_value = "0")]
    funding_bps: String,
    /// Funding interval in seconds (Binance USD-M = 8h = 28800).
    #[arg(long, default_value_t = 28800u64)]
    funding_interval_secs: u64,

    /// Venue silent-cancel/expiry rate per minute (post-only orders the
    /// venue randomly drops). `0.0` (default) = off.
    #[arg(long, default_value_t = 0.0f64)]
    silent_cancel_rate_per_min: f64,

    /// Max simultaneously-resting orders per symbol (Binance `MAX_NUM_ORDERS`
    /// filter). Default matches the live venue; a Place past the cap is
    /// rejected like `-1015`. `0` = unlimited.
    #[arg(long, default_value_t = tikr_backtest::fill_sim::BINANCE_MAX_OPEN_ORDERS_PER_SYMBOL)]
    max_open_orders: u32,

    /// Isolated-margin leverage for forced liquidation. When `> 0`, the
    /// backtest force-closes the position if the mark (book mid here, no mark
    /// series) breaches the liquidation price. `0` (default) = liquidation
    /// disabled (position rides any drawdown — optimistic).
    #[arg(long, default_value = "0")]
    leverage: String,
    /// Maintenance-margin rate (fraction) for the liquidation trigger, e.g.
    /// `0.005` = 0.5%. Only used when `--leverage > 0`.
    #[arg(long, default_value = "0.005")]
    maint_margin_rate: String,

    /// Balance-sim: initial wallet balance (USDT). When > 0 with
    /// order-balance-pct > 0, order notional + position cap compound off the
    /// running balance (initial + realized − fees), mirroring live sizing.
    /// `0` (default) = fixed notional from the per-strategy notional arg.
    #[arg(long, default_value = "0")]
    initial_balance: String,
    /// Balance-sim: percent of running balance per order (0-100).
    #[arg(long, default_value = "0")]
    order_balance_pct: String,
    /// Balance-sim: percent of running balance as the per-bot position cap
    /// (0-100). `0` = uncapped.
    #[arg(long, default_value = "0")]
    max_position_pct: String,
    /// Inventory-aware order-size boost: extra size on the reducing side at
    /// full inventory, as a percent of base size. `0` (default) disables.
    #[arg(long, default_value = "0")]
    inventory_boost_pct: String,
    /// Curve exponent for the inventory boost (|pos|/cap ratio). `1` = linear,
    /// `>1` = slow start then steep, `<1` = fast early ramp.
    #[arg(long, default_value = "1")]
    inventory_boost_curve: String,

    // --- A-S / GLFT spread ---
    /// Half-spread in bps (used by A-S + GLFT).
    #[arg(long, default_value_t = 5u32)]
    spread_bps: u32,

    // --- A-S / GLFT inventory aversion ---
    /// γ risk aversion for A-S / GLFT.
    #[arg(long, default_value = "0.1")]
    gamma: String,

    // --- TopOfBook ---
    /// TopOfBook: venue tick size.
    #[arg(long, default_value = "0.1")]
    tick_size: String,

    /// TopOfBook: improve when spread > N ticks.
    #[arg(long, default_value_t = 1u32)]
    improve_when_spread_gt_ticks: u32,

    /// TopOfBook: max inventory-skew shift in ticks (0 = no skew).
    #[arg(long, default_value_t = 0u32)]
    max_skew_ticks: u32,

    /// TopOfBook: position at which skew is fully applied.
    #[arg(long, default_value = "0.005")]
    skew_unit: String,

    /// TopOfBook: max book-imbalance shift in ticks (0 = disable).
    #[arg(long, default_value_t = 0u32)]
    max_imbalance_ticks: u32,

    /// MicroPrice: half-spread in ticks.
    #[arg(long, default_value_t = 1u32)]
    micro_half_spread_ticks: u32,

    /// Tide: grid depth per side (1 = single-touch).
    #[arg(long, default_value_t = 12u32)]
    tr_grid_levels: u32,

    /// Tide: lattice geometry in bps (inner gap AND level spacing). 0 = at-touch/1-tick.
    #[arg(long, default_value_t = 0u32)]
    tr_step_bps: u32,

    /// Tide: per-order notional in USDT.
    #[arg(long, default_value = "10")]
    tr_notional: String,

    /// Tide: venue step size (lot floor).
    #[arg(long, default_value = "0.001")]
    tr_step_size: String,

    /// Tide: venue min order notional in USDT.
    #[arg(long, default_value = "5")]
    tr_min_notional: String,

    /// Wave: lattice slots per side.
    #[arg(long, default_value_t = 12u32)]
    wv_grid_levels: u32,
    /// Wave: refill batching threshold (slots empty before refill).
    #[arg(long, default_value_t = 1u32)]
    wv_refill_threshold: u32,

    /// Wave inventory skew in lattice slots: shift the overloaded side's band
    /// deeper as |position| approaches the cap (long → bids lower, short →
    /// asks higher). `0` = symmetric (off). Requires a non-zero position cap.
    #[arg(long, default_value_t = 0u32)]
    wv_inventory_skew_slots: u32,

    /// Wave inner self-spread (bps from mid to the first order each side),
    /// independent of `--wv-step-bps` spacing. `0` = legacy (step_bps/2).
    #[arg(long, default_value_t = 0u32)]
    wv_inner_bps: u32,
    /// Wave: hard position cap in quote notional (suppress add side). 0 = off.
    #[arg(long, default_value = "0")]
    wv_max_position_usdt: String,
    /// Wave: lattice geometry in bps (inner gap AND level spacing). 0 = 1-tick.
    #[arg(long, default_value_t = 0u32)]
    wv_step_bps: u32,
    /// Wave: per-order notional.
    #[arg(long, default_value = "10")]
    wv_notional: String,
    /// Wave: step size.
    #[arg(long, default_value = "0.001")]
    wv_step_size: String,
    /// Wave: min notional.
    #[arg(long, default_value = "5")]
    wv_min_notional: String,

    /// Mantis: min book spread (bps) required to quote.
    #[arg(long, default_value = "1")]
    mn_min_spread_bps: String,
    /// Mantis: tick offset from touch. 0 = join, -1 = inside/outbid, +1 = outside.
    #[arg(long, default_value_t = 0i32)]
    mn_tick_offset: i32,
    /// Mantis: ticks price must move from the last fill before reopening a pair.
    #[arg(long, default_value_t = 1u32)]
    mn_reopen_distance_ticks: u32,
    /// Mantis: per-order notional.
    #[arg(long, default_value = "10")]
    mn_notional: String,
    /// Mantis: step size.
    #[arg(long, default_value = "0.001")]
    mn_step_size: String,
    /// Mantis: min notional.
    #[arg(long, default_value = "5")]
    mn_min_notional: String,
    /// Mantis: position cap in quote notional (suppress deepening side). 0 = off.
    #[arg(long, default_value = "0")]
    mn_max_position_usdt: String,

    /// Heartbeat synthesis cadence (ms) injected during quiet stretches.
    #[arg(long, default_value_t = 1000u64)]
    heartbeat_ms: u64,

    /// State directory for snapshots (unused for one-shot backtest but
    /// required by RunnerConfig).
    #[arg(long, default_value = "./state/backtest")]
    state_dir: PathBuf,

    /// Write a running equity-curve CSV: one row per snapshot tick with
    /// `ts_ns,sim_secs,fills,pos_size,realized,unrealized,fees,funding,net`.
    /// Lets you plot how PnL / position / fees evolve across the run. Cadence
    /// is `--snapshot-every-n-events`.
    #[arg(long)]
    equity_csv: Option<PathBuf>,

    /// Snapshot + equity-curve cadence, in events. `0` (default) emits only
    /// the first-event row. When `--equity-csv` is set and this is `0`, it
    /// defaults to 10000 so the curve actually has points.
    #[arg(long, default_value_t = 0u32)]
    snapshot_every_n_events: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let mut args = Args::parse();

    // Auto-detect tick/step/min-notional from the venue's exchangeInfo so the
    // backtest geometry matches the live market without hand-passing filters.
    // Overrides --tick-size + the per-strategy step/min-notional flags. Falls
    // back to the CLI values (with a warning) if the lookup fails.
    if args.autodetect_filters {
        match autodetect_filters(args.venue_env, &args.symbol).await {
            Some((tick, step, min_notional)) => {
                info!(
                    symbol = %args.symbol,
                    tick = %tick,
                    step = %step,
                    min_notional = %min_notional,
                    "autodetected filters from exchangeInfo"
                );
                args.tick_size = tick.clone();
                args.wv_step_size = step.clone();
                args.tr_step_size = step.clone();
                args.mn_step_size = step.clone();
                args.wv_min_notional = min_notional.clone();
                args.tr_min_notional = min_notional.clone();
                args.mn_min_notional = min_notional;
            }
            None => {
                eprintln!(
                    "warning: could not autodetect filters for {} on {:?}; using CLI values \
                     (--tick-size {}, step/min per strategy). Pass --no-autodetect-filters to silence.",
                    args.symbol, args.venue_env, args.tick_size
                );
            }
        }
    }

    // Measure this machine's real round-trip latency to the venue (10 pings)
    // and use mean → submit/cancel latency, stddev → jitter. Keeps the fill
    // sim honest about how long orders are in flight (and how stale a cancel
    // is) instead of a guessed constant. Falls back to the CLI values on
    // failure.
    if args.measure_latency {
        match tikr_paper::probe::measure_api_latency(
            args.venue_env.base_url(),
            args.venue_env.is_futures(),
            10,
        )
        .await
        {
            Some((mean_ms, jitter_ms)) => {
                info!(
                    mean_ms,
                    jitter_ms,
                    samples = 10,
                    "measured venue latency (mean → submit/cancel, stddev → jitter)"
                );
                args.submit_latency_ms = mean_ms;
                args.submit_latency_jitter_ms = jitter_ms;
            }
            None => {
                eprintln!(
                    "warning: latency probe to {:?} failed; using static --submit-latency-ms {} \
                     / jitter {}. Pass --no-measure-latency to silence.",
                    args.venue_env, args.submit_latency_ms, args.submit_latency_jitter_ms
                );
            }
        }
    }

    let (base_str, quote_str) = split_symbol(&args.symbol);
    let symbol = Symbol {
        base: Asset::new(base_str),
        quote: Asset::new(quote_str),
        venue: VenueId::new("binance"),
        kind: MarketKind::Perp,
    };

    let replay = ParquetReplay::new(ReplayConfig {
        heartbeat_ms: args.heartbeat_ms,
        symbols: vec![symbol.clone()],
        data_dir: args.data_dir.clone(),
        tick_size: Decimal::from_str(&args.tick_size)?,
        allow_seq_gaps: true,
    })?;

    let venue = BacktestVenue::new(replay);
    let size_per_quote = Size(Decimal::from_str(&args.size)?);

    let leverage = Decimal::from_str(&args.leverage)?;
    let initial_balance = Decimal::from_str(&args.initial_balance)?;
    // Buying-power margin cap: with isolated margin, max position notional =
    // balance × leverage. Wired into FillSim's synthetic Binance -2019 reject
    // so the bot can't grow inventory past what the account can fund. Needs
    // both a balance AND a leverage; otherwise unbounded (prior behaviour).
    let buying_power = if initial_balance > Decimal::ZERO && leverage > Decimal::ZERO {
        Some((initial_balance * leverage).round_dp(8))
    } else {
        None
    };

    // Balance-derived sizing (mirrors the live config's `order_balance_pct` /
    // `max_position_pct`). When a balance + percent are set, the per-order
    // notional and the strategy's position cap are computed from the balance
    // up front (so they apply from event 1, not just after the runner's first
    // post-fill compounding update). The runner's compounding watch then keeps
    // them in sync as the balance grows.
    let order_balance_pct = Decimal::from_str(&args.order_balance_pct)?;
    let max_position_pct = Decimal::from_str(&args.max_position_pct)?;
    let inventory_boost_pct = Decimal::from_str(&args.inventory_boost_pct)?;
    let inventory_boost = if inventory_boost_pct > Decimal::ZERO {
        Some(tikr_paper::InventoryBoostConfig {
            max_boost_pct: inventory_boost_pct,
            curve_exponent: Decimal::from_str(&args.inventory_boost_curve)?,
        })
    } else {
        None
    };
    let hundred = Decimal::from(100);
    let balance_notional = if initial_balance > Decimal::ZERO && order_balance_pct > Decimal::ZERO {
        Some((initial_balance * order_balance_pct / hundred).round_dp(8))
    } else {
        None
    };
    let balance_max_position =
        if initial_balance > Decimal::ZERO && max_position_pct > Decimal::ZERO {
            Some((initial_balance * max_position_pct / hundred).round_dp(8))
        } else {
            None
        };

    let fill_sim = FillSim::new(FillSimConfig {
        submit_latency_ms: args.submit_latency_ms,
        cancel_latency_ms: args.submit_latency_ms,
        fees: VenueFees {
            maker_bps: args.maker_bps,
            taker_bps: args.taker_bps,
        },
        max_position_notional_usdt: buying_power,
        silent_cancel_rate_per_min: args.silent_cancel_rate_per_min,
        rng_seed: 0,
        latency_jitter_ms: args.submit_latency_jitter_ms,
        max_open_orders: if args.max_open_orders > 0 {
            Some(args.max_open_orders)
        } else {
            None
        },
    });

    let funding_rate = Decimal::from_str(&args.funding_bps)?;
    let funding = if funding_rate != Decimal::ZERO {
        Some(FundingConfig {
            interval_secs: args.funding_interval_secs,
            rate_per_interval: funding_rate,
        })
    } else {
        None
    };
    // Isolated-margin liquidation: enabled when --leverage > 0. The forced
    // close is a taker, so charge the taker fee on it.
    let liquidation = if leverage > Decimal::ZERO {
        Some(LiquidationConfig {
            leverage,
            maint_margin_rate: Decimal::from_str(&args.maint_margin_rate)?,
            close_fee_bps: args.taker_bps,
        })
    } else {
        None
    };
    // Equity-curve cadence: when a CSV is requested but no cadence given,
    // default to 10000 events so the curve has points.
    let snapshot_every_n_events = if args.equity_csv.is_some() && args.snapshot_every_n_events == 0
    {
        10_000
    } else {
        args.snapshot_every_n_events
    };
    let runner_config = RunnerConfig {
        state_dir: args.state_dir.clone(),
        snapshot_every_n_events,
        skim: None,
        funding,
        snapshot_tap: None,
        live_tap: None,
        notional_rx: None,
        max_position_rx: None,
        liq_window_secs: 0,
        seed_position: None,
        equity_csv_path: args.equity_csv.clone(),
        initial_balance,
        order_balance_pct,
        max_position_pct,
        min_notional: Decimal::ZERO,
        max_expected_open_orders: 2,
        liquidation,
        mark_series: None,
        inventory_boost,
    };

    // No shutdown trigger — replay ends naturally when events exhaust.
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    // No external fills — paper mode uses FillSim.
    let external_fills: Option<tokio::sync::mpsc::UnboundedReceiver<Fill>> = None;

    info!(
        symbol = %args.symbol,
        strategy = ?args.strategy,
        data_dir = %args.data_dir.display(),
        "starting backtest"
    );

    let report = match args.strategy {
        StrategyArg::AvellanedaStoikov => {
            let strategy = AvellanedaStoikov::new(AvellanedaStoikovConfig {
                gamma: Decimal::from_str(&args.gamma)?,
                base_spread_bps: args.spread_bps,
                horizon_sec: 3600,
                size_per_quote,
                min_requote_interval_ms: 1000,
                level_step_bps: 1,
                volatility: EwmaConfig {
                    half_life_sec: 60.0,
                    initial_var: Decimal::from_str("0.000001")?,
                },
            });
            run_with_resume(
                venue,
                strategy,
                fill_sim,
                symbol,
                shutdown_rx,
                runner_config,
                None,
                None,
                None,
                external_fills,
                None,
            )
            .await
        }
        StrategyArg::Glft => {
            let strategy = Glft::new(GlftConfig {
                gamma: Decimal::from_str(&args.gamma)?,
                base_spread_bps: args.spread_bps,
                size_per_quote,
                min_requote_interval_ms: 1000,
                level_step_bps: 1,
                volatility: EwmaConfig {
                    half_life_sec: 60.0,
                    initial_var: Decimal::from_str("0.000001")?,
                },
            });
            run_with_resume(
                venue,
                strategy,
                fill_sim,
                symbol,
                shutdown_rx,
                runner_config,
                None,
                None,
                None,
                external_fills,
                None,
            )
            .await
        }
        StrategyArg::TopOfBook => {
            let strategy = TopOfBook::new(TopOfBookConfig {
                size_per_quote,
                tick_size: Decimal::from_str(&args.tick_size)?,
                improve_when_spread_gt_ticks: args.improve_when_spread_gt_ticks,
                min_requote_interval_ms: 1000,
                max_skew_ticks: args.max_skew_ticks,
                skew_unit: Size(Decimal::from_str(&args.skew_unit)?),
                max_imbalance_ticks: args.max_imbalance_ticks,
            });
            run_with_resume(
                venue,
                strategy,
                fill_sim,
                symbol,
                shutdown_rx,
                runner_config,
                None,
                None,
                None,
                external_fills,
                None,
            )
            .await
        }
        StrategyArg::MicroPrice => {
            let strategy = MicroPrice::new(MicroPriceConfig {
                size_per_quote,
                tick_size: Decimal::from_str(&args.tick_size)?,
                half_spread_ticks: args.micro_half_spread_ticks,
                min_requote_interval_ms: 1000,
                max_skew_ticks: args.max_skew_ticks,
                skew_unit: Size(Decimal::from_str(&args.skew_unit)?),
            });
            run_with_resume(
                venue,
                strategy,
                fill_sim,
                symbol,
                shutdown_rx,
                runner_config,
                None,
                None,
                None,
                external_fills,
                None,
            )
            .await
        }
        StrategyArg::Tide => {
            let strategy = Tide::new(TideConfig {
                notional_per_order: balance_notional
                    .map(Ok)
                    .unwrap_or_else(|| Decimal::from_str(&args.tr_notional))?,
                tick_size: Decimal::from_str(&args.tick_size)?,
                step_size: Decimal::from_str(&args.tr_step_size)?,
                min_notional: Decimal::from_str(&args.tr_min_notional)?,
                grid_levels: args.tr_grid_levels,
                step_bps: args.tr_step_bps,
                max_position_usdt: balance_max_position.unwrap_or(Decimal::ZERO),
                prune_stragglers: true,
            });
            run_with_resume(
                venue,
                strategy,
                fill_sim,
                symbol,
                shutdown_rx,
                runner_config,
                None,
                None,
                None,
                external_fills,
                None,
            )
            .await
        }
        StrategyArg::Wave => {
            let strategy = Wave::new(WaveConfig {
                notional_per_order: balance_notional
                    .map(Ok)
                    .unwrap_or_else(|| Decimal::from_str(&args.wv_notional))?,
                tick_size: Decimal::from_str(&args.tick_size)?,
                step_size: Decimal::from_str(&args.wv_step_size)?,
                min_notional: Decimal::from_str(&args.wv_min_notional)?,
                grid_levels: args.wv_grid_levels,
                step_bps: args.wv_step_bps,
                inner_bps: args.wv_inner_bps,
                refill_threshold: args.wv_refill_threshold,
                max_position_usdt: match balance_max_position {
                    Some(cap) => cap,
                    None => Decimal::from_str(&args.wv_max_position_usdt)?,
                },
                inventory_skew_slots: args.wv_inventory_skew_slots,
            });
            run_with_resume(
                venue,
                strategy,
                fill_sim,
                symbol,
                shutdown_rx,
                runner_config,
                None,
                None,
                None,
                external_fills,
                None,
            )
            .await
        }
        StrategyArg::Mantis => {
            let strategy = Mantis::new(MantisConfig {
                notional_per_order: balance_notional
                    .map(Ok)
                    .unwrap_or_else(|| Decimal::from_str(&args.mn_notional))?,
                tick_size: Decimal::from_str(&args.tick_size)?,
                step_size: Decimal::from_str(&args.mn_step_size)?,
                min_notional: Decimal::from_str(&args.mn_min_notional)?,
                min_spread_bps: Decimal::from_str(&args.mn_min_spread_bps)?,
                tick_offset: args.mn_tick_offset,
                reopen_distance_ticks: args.mn_reopen_distance_ticks,
                max_position_usdt: match balance_max_position {
                    Some(cap) => cap,
                    None => Decimal::from_str(&args.mn_max_position_usdt)?,
                },
            });
            run_with_resume(
                venue,
                strategy,
                fill_sim,
                symbol,
                shutdown_rx,
                runner_config,
                None,
                None,
                None,
                external_fills,
                None,
            )
            .await
        }
    };

    let json = serde_json::to_string_pretty(&report)?;
    println!("{json}");
    info!(
        events = report.events_processed,
        fills = report.fills_emitted,
        "backtest done"
    );
    Ok(())
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
// BacktestVenue — wraps a Replay as a Venue. Write methods are no-ops; the
// runner is in paper mode, so FillSim handles fill simulation.
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
            "BacktestVenue::snapshot not supported — strategies read book via MarketEvent",
        ))))
    }

    async fn subscribe(&self, _symbol: &Symbol) -> Result<BoxStream<'_, MarketEvent>, VenueError> {
        let replay = self.replay.lock().unwrap().take().ok_or_else(|| {
            VenueError::Internal(Box::new(std::io::Error::other(
                "BacktestVenue::subscribe called twice — Replay can only be consumed once",
            )))
        })?;

        // Bridge Replay::next() (async) into a Stream.
        let s = stream::unfold(
            replay,
            |mut r| async move { r.next().await.map(|ev| (ev, r)) },
        );
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
