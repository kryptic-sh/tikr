//! Run a fixed suite of strategy presets against the same recorded parquet
//! data and print a comparison table.
//!
//! Each preset gets a fresh `ParquetReplay` + `FillSim` + `run_with_resume`
//! pass, so results are apples-to-apples on identical historical events.

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Mutex;

use async_trait::async_trait;
use clap::Parser;
use futures::stream::{self, BoxStream};
use tikr_backtest::fill_sim::{FillSim, FillSimConfig, VenueFees};
use tikr_backtest::replay::{ParquetReplay, ReplayConfig};
use tikr_core::{
    Asset, Decimal, Fill, MarketEvent, MarketKind, Position, SignedSize, Size, Snapshot, Symbol,
    VenueId,
};
use tikr_paper::{PaperReport, RunnerConfig, run_with_resume};
use tikr_strategy::{
    AvellanedaStoikov, AvellanedaStoikovConfig, EwmaConfig, Glft, GlftConfig, NaiveGrid,
    NaiveGridConfig, Strategy, TopOfBook, TopOfBookConfig,
};
use tikr_venue::{QuoteId, QuoteIntent, Venue, VenueError};
use tokio::sync::watch;
use tracing::info;

#[derive(Parser, Debug)]
#[command(
    name = "compare_strategies",
    about = "Run a strategy suite over recorded parquet data and print a comparison"
)]
struct Args {
    /// Directory containing `book_<BASE>_*.parquet` + `trades_<BASE>_*.parquet`.
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,

    /// Binance-style symbol (e.g. `BTCUSDT`).
    #[arg(long, default_value = "BTCUSDT")]
    symbol: String,

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

    /// Tick size for TopOfBook presets.
    #[arg(long, default_value = "0.1")]
    tick_size: String,
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

    let size_per_quote = Size(Decimal::from_str(&args.size)?);
    let tick = Decimal::from_str(&args.tick_size)?;
    let fees = VenueFees {
        maker_bps: args.maker_bps,
        taker_bps: args.taker_bps,
    };
    let ewma = EwmaConfig {
        half_life_sec: 60.0,
        initial_var: Decimal::from_str("0.000001")?,
    };

    let mut results: Vec<(String, PaperReport)> = Vec::new();

    // Preset: NaiveGrid at 5 bps half-spread.
    let r = run_one(
        &args,
        &symbol,
        NaiveGrid::new(NaiveGridConfig {
            levels_per_side: 1,
            base_spread_bps: 5,
            level_step_bps: 1,
            size_per_quote,
            min_requote_interval_ms: 1000,
        }),
        fees,
    )
    .await?;
    results.push(("naive-grid 5bps".into(), r));

    // Preset: NaiveGrid at 2 bps half-spread.
    let r = run_one(
        &args,
        &symbol,
        NaiveGrid::new(NaiveGridConfig {
            levels_per_side: 1,
            base_spread_bps: 2,
            level_step_bps: 1,
            size_per_quote,
            min_requote_interval_ms: 1000,
        }),
        fees,
    )
    .await?;
    results.push(("naive-grid 2bps".into(), r));

    // Preset: Avellaneda-Stoikov γ=0.1, 5 bps.
    let r = run_one(
        &args,
        &symbol,
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
    )
    .await?;
    results.push(("A-S γ=0.1 5bps".into(), r));

    // Preset: GLFT γ=0.1, 5 bps.
    let r = run_one(
        &args,
        &symbol,
        Glft::new(GlftConfig {
            gamma: Decimal::from_str("0.1")?,
            base_spread_bps: 5,
            size_per_quote,
            min_requote_interval_ms: 1000,
            level_step_bps: 1,
            volatility: ewma.clone(),
        }),
        fees,
    )
    .await?;
    results.push(("GLFT γ=0.1 5bps".into(), r));

    // Preset: TOB 1-tick improve, no skew.
    let r = run_one(
        &args,
        &symbol,
        TopOfBook::new(TopOfBookConfig {
            size_per_quote,
            tick_size: tick,
            improve_when_spread_gt_ticks: 1,
            min_requote_interval_ms: 1000,
            max_skew_ticks: 0,
            skew_unit: Size(Decimal::from(1)),
        }),
        fees,
    )
    .await?;
    results.push(("TOB improve=1 noskew".into(), r));

    // Preset: TOB pure-join (never improves), no skew.
    let r = run_one(
        &args,
        &symbol,
        TopOfBook::new(TopOfBookConfig {
            size_per_quote,
            tick_size: tick,
            improve_when_spread_gt_ticks: 1_000_000,
            min_requote_interval_ms: 1000,
            max_skew_ticks: 0,
            skew_unit: Size(Decimal::from(1)),
        }),
        fees,
    )
    .await?;
    results.push(("TOB pure-join".into(), r));

    // Preset: TOB 1-tick improve + skew(10, 0.005).
    let r = run_one(
        &args,
        &symbol,
        TopOfBook::new(TopOfBookConfig {
            size_per_quote,
            tick_size: tick,
            improve_when_spread_gt_ticks: 1,
            min_requote_interval_ms: 1000,
            max_skew_ticks: 10,
            skew_unit: Size(Decimal::from_str("0.005")?),
        }),
        fees,
    )
    .await?;
    results.push(("TOB improve=1 skew(10,0.005)".into(), r));

    // Preset: TOB 1-tick improve + skew(20, 0.005).
    let r = run_one(
        &args,
        &symbol,
        TopOfBook::new(TopOfBookConfig {
            size_per_quote,
            tick_size: tick,
            improve_when_spread_gt_ticks: 1,
            min_requote_interval_ms: 1000,
            max_skew_ticks: 20,
            skew_unit: Size(Decimal::from_str("0.005")?),
        }),
        fees,
    )
    .await?;
    results.push(("TOB improve=1 skew(20,0.005)".into(), r));

    print_table(&results);
    Ok(())
}

async fn run_one<S: Strategy>(
    args: &Args,
    symbol: &Symbol,
    strategy: S,
    fees: VenueFees,
) -> Result<PaperReport, Box<dyn std::error::Error>> {
    let replay = ParquetReplay::new(ReplayConfig {
        heartbeat_ms: args.heartbeat_ms,
        symbols: vec![symbol.clone()],
        data_dir: args.data_dir.clone(),
    })?;
    let venue = BacktestVenue::new(replay);
    let fill_sim = FillSim::new(FillSimConfig {
        submit_latency_ms: 0,
        cancel_latency_ms: 0,
        fees,
    });
    let runner_config = RunnerConfig {
        state_dir: PathBuf::from("./state/backtest_compare"),
        snapshot_every_n_events: 0,
    };
    let (_tx, rx) = watch::channel(false);
    let external_fills: Option<tokio::sync::mpsc::UnboundedReceiver<Fill>> = None;
    info!(strategy = strategy.name(), "preset start");
    let report = run_with_resume(
        venue,
        strategy,
        fill_sim,
        symbol.clone(),
        rx,
        runner_config,
        None,
        None,
        None,
        external_fills,
    )
    .await;
    info!(
        events = report.events_processed,
        fills = report.fills_emitted,
        "preset done"
    );
    Ok(report)
}

fn print_table(results: &[(String, PaperReport)]) {
    println!();
    println!(
        "{:<32} {:>8} {:>14} {:>14} {:>10} {:>14}",
        "preset", "fills", "realized", "unrealized", "fees", "NET"
    );
    println!("{}", "-".repeat(96));
    for (name, r) in results {
        println!(
            "{:<32} {:>8} {:>14.4} {:>14.4} {:>10.4} {:>14.4}",
            name,
            r.fills_emitted,
            decimal_to_f64(&r.realized.0),
            decimal_to_f64(&r.unrealized.0),
            decimal_to_f64(&r.fees.0),
            decimal_to_f64(&r.net.0),
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
