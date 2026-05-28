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
use tikr_backtest::replay::{ParquetReplay, Replay, ReplayConfig};
use tikr_core::{
    Asset, Decimal, Fill, MarketEvent, MarketKind, Position, SignedSize, Size, Snapshot, Symbol,
    VenueId,
};
use tikr_paper::{RunnerConfig, run_with_resume};
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

    /// Tide: minimum self-spread in bps of mid.
    #[arg(long, default_value_t = 10u32)]
    tr_min_self_spread_bps: u32,

    /// Tide: per-order notional in USDT.
    #[arg(long, default_value = "10")]
    tr_notional: String,

    /// Tide: venue step size (lot floor).
    #[arg(long, default_value = "0.001")]
    tr_step_size: String,

    /// Tide: venue min order notional in USDT.
    #[arg(long, default_value = "5")]
    tr_min_notional: String,

    /// Tide: profit target for close-on-fill in bps of fill
    /// price. `0` (default) = falls back to min_self_spread_bps.
    #[arg(long, default_value_t = 0u32)]
    tr_close_profit_bps: u32,

    /// Tide: grid spacing in bps (snapped to tick, min 1 tick).
    /// `0` = legacy 1-tick spacing.
    #[arg(long, default_value_t = 0u32)]
    tr_grid_step_bps: u32,

    /// Wave: lattice slots per side.
    #[arg(long, default_value_t = 12u32)]
    wv_grid_levels: u32,
    /// Wave: drain trigger (slots, either side).
    #[arg(long, default_value_t = 4u32)]
    wv_recenter_drain_slots: u32,
    /// Wave: max skew on recenter (fraction).
    #[arg(long, default_value = "0.25")]
    wv_skew_max_pct: String,
    /// Wave: ATR step multiplier. 0 = use ticks/bps.
    #[arg(long, default_value = "1.0")]
    wv_step_atr_mult: String,
    /// Wave: ATR period (bars).
    #[arg(long, default_value_t = 14u32)]
    wv_atr_period: u32,
    /// Wave: bar interval (s).
    #[arg(long, default_value_t = 60u64)]
    wv_bar_interval_secs: u64,
    /// Wave: bar warmup count.
    #[arg(long, default_value_t = 14u32)]
    wv_bar_warmup_bars: u32,
    /// Wave: relattice every Nth recenter.
    #[arg(long, default_value_t = 10u32)]
    wv_relattice_every_n: u32,
    /// Wave: min ms between recenters (cooldown).
    #[arg(long, default_value_t = 1000u64)]
    wv_recenter_cooldown_ms: u64,
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

    let fill_sim = FillSim::new(FillSimConfig {
        submit_latency_ms: 0,
        cancel_latency_ms: 0,
        fees: VenueFees {
            maker_bps: args.maker_bps,
            taker_bps: args.taker_bps,
        },
        max_position_notional_usdt: None,
        silent_cancel_rate_per_min: 0.0,
        rng_seed: 0,
    });

    let runner_config = RunnerConfig {
        state_dir: args.state_dir.clone(),
        snapshot_every_n_events: 0, // backtest = no snapshots
        skim: None,
        funding: None,
        snapshot_tap: None,
        live_tap: None,
        notional_rx: None,
        max_position_rx: None,
        liq_window_secs: 0,
        seed_position: None,
        equity_csv_path: None,
        initial_balance: Decimal::ZERO,
        order_balance_pct: Decimal::ZERO,
        max_position_pct: Decimal::ZERO,
        min_notional: Decimal::ZERO,
        max_expected_open_orders: 2,
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
                notional_per_order: Decimal::from_str(&args.tr_notional)?,
                tick_size: Decimal::from_str(&args.tick_size)?,
                step_size: Decimal::from_str(&args.tr_step_size)?,
                min_notional: Decimal::from_str(&args.tr_min_notional)?,
                grid_levels: args.tr_grid_levels,
                min_self_spread_bps: args.tr_min_self_spread_bps,
                close_profit_bps: args.tr_close_profit_bps,
                grid_step_bps: args.tr_grid_step_bps,
                min_self_spread_ticks: 0,
                close_profit_ticks: 0,
                grid_step_ticks: 0,
                max_position_usdt: Decimal::ZERO,
                adaptive_bps_enabled: false,
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
                notional_per_order: Decimal::from_str(&args.wv_notional)?,
                tick_size: Decimal::from_str(&args.tick_size)?,
                step_size: Decimal::from_str(&args.wv_step_size)?,
                min_notional: Decimal::from_str(&args.wv_min_notional)?,
                grid_levels: args.wv_grid_levels,
                min_self_spread_bps: 0,
                min_self_spread_ticks: 0,
                grid_step_bps: 0,
                grid_step_ticks: 0,
                recenter_drain_slots: args.wv_recenter_drain_slots,
                skew_max_pct: Decimal::from_str(&args.wv_skew_max_pct)?,
                max_position_usdt: Decimal::ZERO,
                bar_interval_secs: args.wv_bar_interval_secs,
                max_bars: 200,
                atr_period: args.wv_atr_period,
                step_atr_mult: Decimal::from_str(&args.wv_step_atr_mult)?,
                bar_warmup_bars: args.wv_bar_warmup_bars,
                relattice_every_n_recenters: args.wv_relattice_every_n,
                recenter_cooldown_ms: args.wv_recenter_cooldown_ms,
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
