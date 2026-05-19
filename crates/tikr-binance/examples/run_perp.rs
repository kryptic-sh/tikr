//! Live testnet/mainnet single-symbol Binance **USD-M Futures (Perp)** market-maker.
//!
//! # Operator pre-flight
//!
//! ```bash
//! # Option A: HMAC (existing, unchanged)
//! export TIKR_BINANCE_KEY_TYPE=hmac    # or omit; default
//! export TIKR_BINANCE_API_KEY=your-testnet-key
//! export TIKR_BINANCE_API_SECRET=your-testnet-secret
//!
//! # Option B: Ed25519 (recommended for mainnet — private key never leaves machine)
//! # 1. Generate locally:
//! #    openssl genpkey -algorithm ed25519 -out keys/binance-ed25519.pem
//! #    openssl pkey -pubout -in keys/binance-ed25519.pem
//! # 2. Paste the printed public key into Binance API Management UI
//! # 3. Copy the API key string Binance shows back to you
//! # 4. Env:
//! export TIKR_BINANCE_KEY_TYPE=ed25519
//! export TIKR_BINANCE_API_KEY=your-api-key-from-binance-ui
//! export TIKR_BINANCE_PRIVATE_KEY_PATH=./keys/binance-ed25519.pem
//! ```
//!
//! # Key loading (priority order)
//!
//! HMAC:
//!   1. `TIKR_BINANCE_API_KEY` + `TIKR_BINANCE_API_SECRET` env vars.
//!   2. `--key-file <path>` flag (single line: `key:secret`).
//!
//! Ed25519:
//!   1. `TIKR_BINANCE_KEY_TYPE=ed25519` + `TIKR_BINANCE_PRIVATE_KEY_PATH`.
//!   2. `--ed25519-key-file <path>` flag overrides `TIKR_BINANCE_PRIVATE_KEY_PATH`.
//!
//! # Position mode
//!
//! One-way mode is assumed (no `positionSide` param). If your account is in
//! Hedge Mode, orders will be rejected; switch to One-way mode in the Binance
//! UI before using this runner.
//!
//! # Leverage
//!
//! Set to 1x cross-margin at startup via `POST /fapi/v1/leverage`.
//!
//! # IP whitelisting
//!
//! Binance API keys can be restricted to a list of IPs. For testnet testing,
//! disable the IP whitelist OR add your runner's public IP.

use clap::{Parser, ValueEnum};
use reqwest::Client as HttpClient;
use std::path::PathBuf;
use tikr_backtest::fill_sim::{FillSim, FillSimConfig, VenueFees};
use tikr_binance::{
    BinanceClient, BinanceEnv, BinanceKeyMaterial, load_credentials_from_file,
    load_key_material_from_env, user_stream::subscribe_user_data_stream,
};
use tikr_core::{Asset, Decimal, MarketKind, Size, Symbol, VenueId};
use tikr_paper::{RunnerConfig, run_with_resume};
use tikr_strategy::{NaiveGrid, NaiveGridConfig, Strategy};
use tokio::signal;
use tokio::sync::watch;
use tracing::{info, warn};

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, ValueEnum)]
enum EnvArg {
    #[value(name = "futures-testnet")]
    FuturesTestnet,
    #[value(name = "futures-mainnet")]
    FuturesMainnet,
}

#[derive(Parser, Debug)]
#[command(
    name = "run_perp",
    about = "Single-symbol live Binance USD-M Perp market-maker (testnet-first)"
)]
struct Args {
    /// Binance Futures environment.
    #[arg(long, value_enum, default_value = "futures-testnet")]
    env: EnvArg,

    /// Symbol to trade (e.g. `BTCUSDT`).
    #[arg(long, default_value = "BTCUSDT")]
    symbol: String,

    /// Path to a key file (`key:secret` single line) for HMAC auth.
    /// `TIKR_BINANCE_API_KEY` / `TIKR_BINANCE_API_SECRET` take precedence.
    #[arg(long)]
    key_file: Option<PathBuf>,

    /// Path to an Ed25519 private key PEM file.
    /// Overrides `TIKR_BINANCE_PRIVATE_KEY_PATH`. Requires `TIKR_BINANCE_KEY_TYPE=ed25519`.
    #[arg(long)]
    ed25519_key_file: Option<PathBuf>,

    /// State directory for snapshots.
    #[arg(long, default_value = "./state")]
    state_dir: PathBuf,

    /// Run duration in minutes. 0 = run until Ctrl-C.
    #[arg(long, default_value_t = 0u32)]
    minutes: u32,
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load .env (ignore missing — env-only setups still work).
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let env = match args.env {
        EnvArg::FuturesTestnet => BinanceEnv::FuturesTestnet,
        EnvArg::FuturesMainnet => BinanceEnv::FuturesMainnet,
    };

    if env.is_mainnet() {
        warn!(
            "MAINNET Futures mode — real funds at risk. \
             Requires TIKR_BINANCE_ENABLE_MAINNET=1."
        );
    }

    // Load API key.
    let api_key =
        std::env::var("TIKR_BINANCE_API_KEY").map_err(|_| "TIKR_BINANCE_API_KEY not set")?;

    // Load key material based on TIKR_BINANCE_KEY_TYPE.
    let key_material: BinanceKeyMaterial = if let Some(ref kf) = args.key_file
        && std::env::var("TIKR_BINANCE_KEY_TYPE")
            .unwrap_or_default()
            .to_lowercase()
            != "ed25519"
    {
        // HMAC via --key-file: parse key:secret, use only the secret part.
        let (_k, secret) =
            load_credentials_from_file(kf).map_err(|e| format!("key-file error: {e}"))?;
        BinanceKeyMaterial::Hmac { secret }
    } else {
        load_key_material_from_env(args.ed25519_key_file.as_deref())
            .map_err(|e| format!("credential error: {e}"))?
    };

    // Derive base/quote from symbol.
    let (base, quote) = split_symbol(&args.symbol);
    let symbol = Symbol {
        base: Asset::new(base),
        quote: Asset::new(quote),
        venue: VenueId::new("binance"),
        kind: MarketKind::Perp,
    };

    info!(
        env = ?env,
        symbol = %args.symbol,
        state_dir = %args.state_dir.display(),
        key_type = ?key_material,
        "starting Perp runner"
    );

    // Build the venue. with_credentials moves api_key, so clone first for the
    // userDataStream task (it needs the same key independently).
    let api_key_for_user_stream = api_key.clone();
    let venue = BinanceClient::with_credentials(env, api_key, key_material, Some(&symbol)).await?;

    info!(venue = ?venue, "BinanceClient ready");

    // Subscribe to userDataStream for fills (separate HttpClient — cheap).
    let http_for_user_stream = HttpClient::new();
    let fill_rx = subscribe_user_data_stream(
        http_for_user_stream,
        env,
        api_key_for_user_stream,
        MarketKind::Perp,
    )
    .await?;
    info!("userDataStream listenKey minted; subscribed to fills");

    // Shutdown channel.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Ctrl-C handler.
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

    // NaiveGrid: 1 level per side, 20bps spread, 0.001 BTC per quote.
    let strategy = NaiveGrid::new(NaiveGridConfig {
        levels_per_side: 1,
        base_spread_bps: 20,
        level_step_bps: 5,
        size_per_quote: Size(Decimal::from_str_exact("0.001").unwrap()),
        min_requote_interval_ms: 5000,
    });

    // FillSim required by the runner but discarded in live mode (external_fills set).
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
    };

    let report = run_with_resume(
        venue,
        strategy,
        fill_sim,
        symbol,
        shutdown_rx,
        runner_config,
        None,          // no resume
        None,          // no risk gate (add tikr-risk for production)
        None,          // no alert sink (add tikr-paper::alerts for production)
        Some(fill_rx), // live mode: fills via userDataStream
    )
    .await;

    let report_json = serde_json::to_string_pretty(&report)
        .unwrap_or_else(|e| format!("{{\"error\": \"{}\"}}", e));
    println!("{}", report_json);
    info!(
        events = report.events_processed,
        fills = report.fills_emitted,
        "Perp runner stopped"
    );
    Ok(())
}

/// Simple symbol split: BTCUSDT → ("BTC", "USDT").
fn split_symbol(sym: &str) -> (&str, &str) {
    let upper = sym;
    for suffix in &["USDT", "BUSD", "USDC", "TUSD"] {
        if let Some(base) = upper.strip_suffix(suffix)
            && !base.is_empty()
        {
            return (base, suffix);
        }
    }
    for suffix in &["BTC", "ETH", "BNB"] {
        if let Some(base) = upper.strip_suffix(suffix)
            && !base.is_empty()
        {
            return (base, suffix);
        }
    }
    let n = upper.len();
    if n > 4 {
        (&upper[..n - 4], &upper[n - 4..])
    } else {
        (upper, "USDT")
    }
}
