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
    AvellanedaStoikov, AvellanedaStoikovConfig, EwmaConfig, Glft, GlftConfig, MicroPrice,
    MicroPriceConfig, Strategy, Tide, TideConfig, TopOfBook, TopOfBookConfig, Wave, WaveConfig,
};
use tikr_venue::{QuoteId, QuoteIntent, Venue, VenueError};
use tokio::sync::watch;
use tracing::info;

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
}

#[derive(Parser, Debug)]
#[command(
    name = "run_backtest",
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
    /// arrival is rejected (Binance -5022), exercising on_quote_rejected.
    /// `0` (default) = instant placement (optimistic).
    #[arg(long, default_value_t = 0u64)]
    submit_latency_ms: u64,

    /// Mean exponential latency jitter (ms) added per op on top of
    /// `--submit-latency-ms`. Models network jitter + occasional spikes
    /// (the exponential tail). `0` (default) = fixed latency, deterministic.
    #[arg(long, default_value_t = 0u64)]
    submit_latency_jitter_ms: u64,

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
    let args = Args::parse();

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
                refill_threshold: args.wv_refill_threshold,
                max_position_usdt: match balance_max_position {
                    Some(cap) => cap,
                    None => Decimal::from_str(&args.wv_max_position_usdt)?,
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
