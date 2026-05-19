//! Runnable paper-trading example for the tikr engine.
//!
//! Drives a live Hyperliquid market feed through one of the reference
//! strategies plus `FillSim`, prints a final `PaperReport` to stdout.
//!
//! No real orders are sent — fills are simulated. See `tikr-paper/README.md`.
//!
//! Usage:
//!
//! ```text
//! cargo run -p tikr-paper --example run_paper -- --symbol BTC --minutes 60
//! ```

use clap::{Parser, ValueEnum};
use std::time::Duration;
use tikr_backtest::fill_sim::{FillSim, FillSimConfig, VenueFees};
use tikr_core::{Asset, Decimal, MarketKind, Size, Symbol, VenueId};
use tikr_hyperliquid::{Hyperliquid, HyperliquidConfig, HyperliquidEnv};
use tikr_paper::{RunnerConfig, run};
use tikr_strategy::{
    AvellanedaStoikov, AvellanedaStoikovConfig, EwmaConfig, Glft, GlftConfig, NaiveGrid,
    NaiveGridConfig, Strategy,
};
use tokio::sync::watch;

#[derive(Copy, Clone, Debug, ValueEnum)]
enum StrategyKind {
    NaiveGrid,
    AvellanedaStoikov,
    Glft,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum EnvKind {
    Mainnet,
    Testnet,
}

impl From<EnvKind> for HyperliquidEnv {
    fn from(e: EnvKind) -> Self {
        match e {
            EnvKind::Mainnet => HyperliquidEnv::Mainnet,
            EnvKind::Testnet => HyperliquidEnv::Testnet,
        }
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "run_paper",
    about = "Run a paper trading session against Hyperliquid"
)]
struct Args {
    /// Symbol base asset (e.g. BTC).
    #[arg(long, default_value = "BTC")]
    symbol: String,
    /// Strategy to run.
    #[arg(long, value_enum, default_value_t = StrategyKind::NaiveGrid)]
    strategy: StrategyKind,
    /// How many minutes to run. 0 = until SIGINT.
    #[arg(long, default_value_t = 60u32)]
    minutes: u32,
    /// Environment.
    #[arg(long, value_enum, default_value_t = EnvKind::Mainnet)]
    env: EnvKind,
    /// User wallet address (required only for Venue::position / fills_since).
    #[arg(long)]
    user_address: Option<String>,
}

fn naive_grid_config() -> NaiveGridConfig {
    NaiveGridConfig {
        levels_per_side: 3,
        base_spread_bps: 10,
        level_step_bps: 5,
        size_per_quote: Size(Decimal::try_from(0.01).unwrap()),
        min_requote_interval_ms: 1000,
    }
}

fn avellaneda_stoikov_config() -> AvellanedaStoikovConfig {
    AvellanedaStoikovConfig {
        gamma: Decimal::try_from(0.1).unwrap(),
        k: Decimal::try_from(1.5).unwrap(),
        horizon_sec: 3600,
        size_per_quote: Size(Decimal::try_from(0.01).unwrap()),
        min_requote_interval_ms: 1000,
        level_step_bps: 10,
        volatility: EwmaConfig {
            half_life_sec: 60.0,
            initial_var: Decimal::try_from(0.0001).unwrap(),
        },
    }
}

fn glft_config() -> GlftConfig {
    GlftConfig {
        gamma: Decimal::try_from(0.1).unwrap(),
        k: Decimal::try_from(1.5).unwrap(),
        a: Decimal::try_from(1.0).unwrap(),
        size_per_quote: Size(Decimal::try_from(0.01).unwrap()),
        min_requote_interval_ms: 1000,
        level_step_bps: 10,
        volatility: EwmaConfig {
            half_life_sec: 60.0,
            initial_var: Decimal::try_from(0.0001).unwrap(),
        },
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    if args.symbol.trim().is_empty() {
        eprintln!("--symbol must not be empty");
        std::process::exit(2);
    }

    let symbol = Symbol {
        base: Asset::new(&args.symbol),
        quote: Asset::new("USDC"),
        venue: VenueId::new("hyperliquid"),
        kind: MarketKind::Perp,
    };

    let venue = Hyperliquid::with_config(HyperliquidConfig {
        env: args.env.into(),
        user_address: args.user_address.clone(),
        ..Default::default()
    });

    let fill_sim = FillSim::new(FillSimConfig {
        submit_latency_ms: 50,
        cancel_latency_ms: 0,
        fees: VenueFees {
            maker_bps: 0,
            taker_bps: 0,
        },
    });

    let (tx, rx) = watch::channel(false);
    let tx_sig = tx.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("SIGINT received");
        let _ = tx_sig.send(true);
    });

    if args.minutes > 0 {
        let tx_dur = tx.clone();
        let secs = (args.minutes as u64).saturating_mul(60);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(secs)).await;
            tracing::info!(minutes = args.minutes, "duration cap reached");
            let _ = tx_dur.send(true);
        });
    }

    let report = match args.strategy {
        StrategyKind::NaiveGrid => {
            let s = NaiveGrid::new(naive_grid_config());
            run(venue, s, fill_sim, symbol, rx, RunnerConfig::default()).await
        }
        StrategyKind::AvellanedaStoikov => {
            let s = AvellanedaStoikov::new(avellaneda_stoikov_config());
            run(venue, s, fill_sim, symbol, rx, RunnerConfig::default()).await
        }
        StrategyKind::Glft => {
            let s = Glft::new(glft_config());
            run(venue, s, fill_sim, symbol, rx, RunnerConfig::default()).await
        }
    };

    println!("{report:#?}");
}
