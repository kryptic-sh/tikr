//! Live testnet/mainnet single-symbol trading runner.
//!
//! # Usage
//!
//! ```
//! # Testnet (safe default):
//! cargo run -p tikr-hyperliquid --example run_live -- \
//!   --env testnet \
//!   --symbol BTC \
//!   --minutes 5
//!
//! # Mainnet (requires TIKR_HL_ENABLE_MAINNET=1):
//! TIKR_HL_ENABLE_MAINNET=1 cargo run -p tikr-hyperliquid --example run_live -- \
//!   --env mainnet \
//!   --symbol BTC
//! ```
//!
//! # Key material
//!
//! The private key is read from (in priority order):
//! 1. `TIKR_HL_PRIVATE_KEY` env var (0x-prefixed hex, env wins if both set).
//! 2. `--key-file <path>` flag (single-line hex, optionally 0x-prefixed).
//!
//! **NEVER log the raw key.** The signer hides it internally.
//!
//! # Architecture
//!
//! 1. Build `Hyperliquid::with_wallet` — fetches asset metadata, performs
//!    defensive `cancel_all` for the symbol, sets 1x cross leverage.
//! 2. Start `subscribe_user_events` — WS task emitting real fills into
//!    an `mpsc::UnboundedReceiver<Fill>`.
//! 3. Call `run_with_resume` with `external_fills = Some(rx)` — live mode:
//!    the runner drives the strategy on market events, sends real orders via
//!    the Venue trait, and applies venue fills (not FillSim-synthesized fills)
//!    to the position tracker.
//! 4. Ctrl-C → shutdown watch fires → runner exits → final `PaperReport`
//!    printed as JSON.
//!
//! # Deferred
//!
//! `crates/tikr-paper/src/bin/live.rs` (multi-symbol) is deferred; this
//! single-symbol example covers v0 testnet trading. See issue #38.

use alloy_signer_local::PrivateKeySigner;
use clap::{Parser, ValueEnum};
use std::path::PathBuf;
use tikr_backtest::fill_sim::{FillSim, FillSimConfig, VenueFees};
use tikr_core::{Asset, Decimal, MarketKind, Symbol, VenueId};
use tikr_hyperliquid::{Hyperliquid, HyperliquidConfig, HyperliquidEnv, subscribe_user_events};
use tikr_paper::{RunnerConfig, run_with_resume};
use tikr_strategy::{LayeredGrid, LayeredGridConfig, Strategy};
use tokio::signal;
use tokio::sync::watch;
use tracing::{info, warn};

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, ValueEnum)]
enum EnvArg {
    Testnet,
    Mainnet,
}

#[derive(Parser, Debug)]
#[command(
    name = "run_live",
    about = "Single-symbol live Hyperliquid market-maker (testnet or mainnet)"
)]
struct Args {
    /// Hyperliquid environment to target.
    #[arg(long, value_enum, default_value = "testnet")]
    env: EnvArg,

    /// Symbol to trade (base asset name, e.g. `BTC`).
    #[arg(long, default_value = "BTC")]
    symbol: String,

    /// Path to a file containing the private key (single-line hex, 0x-prefix optional).
    /// `TIKR_HL_PRIVATE_KEY` env var takes precedence if set.
    #[arg(long)]
    key_file: Option<PathBuf>,

    /// State directory for snapshots.
    #[arg(long, default_value = "./state")]
    state_dir: PathBuf,

    /// Run duration in minutes. 0 = run until Ctrl-C.
    #[arg(long, default_value_t = 0u32)]
    minutes: u32,
}

// ---------------------------------------------------------------------------
// Key loading
// ---------------------------------------------------------------------------

fn load_key(args: &Args) -> Result<PrivateKeySigner, Box<dyn std::error::Error>> {
    // Env var wins.
    if let Ok(raw) = std::env::var("TIKR_HL_PRIVATE_KEY") {
        return parse_key(&raw).map_err(|e| format!("TIKR_HL_PRIVATE_KEY: {}", e).into());
    }
    // Fall back to key file.
    if let Some(ref path) = args.key_file {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| format!("key-file {}: {}", path.display(), e))?;
        return parse_key(raw.trim())
            .map_err(|e| format!("key-file {}: {}", path.display(), e).into());
    }
    Err("No private key: set TIKR_HL_PRIVATE_KEY or pass --key-file".into())
}

fn parse_key(raw: &str) -> Result<PrivateKeySigner, String> {
    let hex = raw.strip_prefix("0x").unwrap_or(raw);
    let bytes = hex::decode(hex).map_err(|e| format!("hex decode: {}", e))?;
    PrivateKeySigner::from_slice(&bytes).map_err(|e| format!("key parse: {}", e))
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load .env from cwd if present (ignore missing — env-only setups still work).
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt::init();

    let args = Args::parse();

    // Warn loudly when running mainnet.
    if matches!(args.env, EnvArg::Mainnet) {
        warn!("MAINNET mode — real funds at risk. Set TIKR_HL_ENABLE_MAINNET=1 to proceed.");
    }

    let env = match args.env {
        EnvArg::Testnet => HyperliquidEnv::Testnet,
        EnvArg::Mainnet => HyperliquidEnv::Mainnet,
    };

    let signer = load_key(&args)?;
    info!(address = %signer.address(), "signer loaded");

    let symbol = Symbol {
        base: Asset::new(&args.symbol),
        quote: Asset::new("USDC"),
        venue: VenueId::new("hyperliquid"),
        kind: MarketKind::Perp,
    };

    let config = HyperliquidConfig {
        env,
        user_address: Some(signer.address().to_checksum(None)),
        heartbeat_ms: 1000,
        reconnect_min_backoff_ms: 1000,
        reconnect_max_backoff_ms: 30_000,
        defensive_cancel_all: true,
    };

    // Build the live venue adapter (fetches metadata, defensive cancel_all,
    // sets 1x leverage).
    let venue = Hyperliquid::with_wallet(config.clone(), signer, Some(&symbol)).await?;
    let user_address = venue.address().expect("wallet is set");

    info!(
        env = ?args.env,
        symbol = %args.symbol,
        address = %user_address,
        state_dir = %args.state_dir.display(),
        "starting live runner"
    );

    // Subscribe to userEvents fills.
    let fill_rx = subscribe_user_events(config, user_address).await?;

    // Shutdown channel: Ctrl-C or optional time cap.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Wire Ctrl-C.
    let tx_ctrlc = shutdown_tx.clone();
    tokio::spawn(async move {
        signal::ctrl_c().await.ok();
        info!("Ctrl-C received; shutting down");
        let _ = tx_ctrlc.send(true);
    });

    // Optional time cap.
    if args.minutes > 0 {
        let tx_timer = shutdown_tx;
        let secs = args.minutes as u64 * 60;
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
            info!(minutes = args.minutes, "time cap reached; shutting down");
            let _ = tx_timer.send(true);
        });
    }

    // LayeredGrid strategy with sensible defaults for live trading.
    // notional_per_order controls exposure; 25 USDC clears HL minNotional.
    let strategy = LayeredGrid::new(LayeredGridConfig {
        notional_per_order: Decimal::from(25),
        levels_per_side: 1,
        inner_bps: 6, // 0.06% half-spread
    });

    // FillSim is required by the runner trait but synthesized fills are
    // discarded in live mode (external_fills = Some(fill_rx)).
    let fill_sim = FillSim::new(FillSimConfig {
        submit_latency_ms: 0,
        cancel_latency_ms: 0,
        fees: VenueFees {
            maker_bps: 0,
            taker_bps: 0,
        },
    });

    let runner_config = RunnerConfig {
        state_dir: args.state_dir,
        snapshot_every_n_events: 100,
        skim: None,
        funding: None,
        snapshot_tap: None,
        live_tap: None,
    };

    let report = run_with_resume(
        venue,
        strategy,
        fill_sim,
        symbol,
        shutdown_rx,
        runner_config,
        None,          // no resume (crash recovery via cancel_all at startup)
        None,          // no risk gate (add tikr-risk::BasicRiskGate for production)
        None,          // no alert sink (add tikr-paper::alerts for production)
        Some(fill_rx), // live mode: fills from userEvents WS
    )
    .await;

    let report_json = serde_json::to_string_pretty(&report)
        .unwrap_or_else(|e| format!("{{\"error\": \"{}\"}}", e));
    println!("{}", report_json);
    info!(
        events = report.events_processed,
        fills = report.fills_emitted,
        net = %report.net.0,
        "live runner done"
    );

    Ok(())
}
