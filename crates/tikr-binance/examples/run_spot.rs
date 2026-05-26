//! Live testnet/mainnet single-symbol Binance **Spot** market-maker.
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
//! **NEVER log the raw credentials.** The client hides them internally.
//!
//! # IP whitelisting
//!
//! Binance API keys can be restricted to a list of IPs. For testnet testing,
//! disable the IP whitelist OR add your runner's public IP.

use clap::{Parser, ValueEnum};
use reqwest::Client as HttpClient;
use std::path::PathBuf;
use std::sync::Arc;
use tikr_backtest::fill_sim::{FillSim, FillSimConfig, VenueFees};
use tikr_binance::{
    BinanceClient, BinanceEnv, BinanceKeyMaterial, env_with_product_fallback,
    load_credentials_from_file, load_key_material_from_env, product_var,
    user_stream::subscribe_user_data_stream,
};
use tikr_core::{Asset, Decimal, MarketKind, Symbol, VenueId};
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
    #[value(name = "spot-testnet")]
    SpotTestnet,
    #[value(name = "spot-mainnet")]
    SpotMainnet,
}

#[derive(Parser, Debug)]
#[command(
    name = "run_spot",
    about = "Single-symbol live Binance Spot market-maker (testnet-first)"
)]
struct Args {
    /// Binance Spot environment.
    #[arg(long, value_enum, default_value = "spot-testnet")]
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
        EnvArg::SpotTestnet => BinanceEnv::SpotTestnet,
        EnvArg::SpotMainnet => BinanceEnv::SpotMainnet,
    };

    if env.is_mainnet() {
        warn!(
            "MAINNET Spot mode — real funds at risk. \
             Requires TIKR_BINANCE_ENABLE_MAINNET=1."
        );
    }

    // Load API key (product-aware: tries TIKR_BINANCE_SPOT_API_KEY then
    // falls back to TIKR_BINANCE_API_KEY).
    let api_key = env_with_product_fallback(env, "API_KEY")
        .ok_or_else(|| format!("{} (or fallback) not set", product_var(env, "API_KEY")))?;

    // Load key material (product-aware key type + secret/PEM lookup).
    let key_material: BinanceKeyMaterial = if let Some(ref kf) = args.key_file
        && env_with_product_fallback(env, "KEY_TYPE")
            .unwrap_or_default()
            .to_lowercase()
            != "ed25519"
    {
        // HMAC via --key-file: parse key:secret, use only the secret part.
        let (_k, secret) =
            load_credentials_from_file(kf).map_err(|e| format!("key-file error: {e}"))?;
        BinanceKeyMaterial::Hmac { secret }
    } else {
        load_key_material_from_env(env, args.ed25519_key_file.as_deref())
            .map_err(|e| format!("credential error: {e}"))?
    };

    // Derive quote asset from symbol (last 4 chars heuristic, e.g. USDT/BUSD).
    let (base, quote) = split_symbol(&args.symbol);
    let symbol = Symbol {
        base: Asset::new(base),
        quote: Asset::new(quote),
        venue: VenueId::new("binance"),
        kind: MarketKind::Spot,
    };

    info!(
        env = ?env,
        symbol = %args.symbol,
        state_dir = %args.state_dir.display(),
        key_type = ?key_material,
        "starting Spot runner"
    );

    let api_key_for_user_stream = api_key.clone();
    // Wrap key_material in Arc so it can be shared with the user-stream pump.
    let key_material = Arc::new(key_material);
    let venue = BinanceClient::with_credentials(
        env,
        api_key,
        // BinanceClient takes ownership; clone the inner value from Arc.
        // Safety: Arc::try_unwrap would work here but we need a ref for subscribe too,
        // so we clone the key material for the venue client.
        match key_material.as_ref() {
            BinanceKeyMaterial::Hmac { secret } => BinanceKeyMaterial::Hmac {
                secret: secret.clone(),
            },
            BinanceKeyMaterial::Ed25519 { signing_key } => BinanceKeyMaterial::Ed25519 {
                signing_key: signing_key.clone(),
            },
        },
        Some(&symbol),
        1,
    )
    .await?;

    info!(venue = ?venue, "BinanceClient ready");

    let http_for_user_stream = HttpClient::new();
    let symbol_for_filter =
        format!("{}{}", symbol.base.0.as_ref(), symbol.quote.0.as_ref()).to_uppercase();
    let fill_rx = subscribe_user_data_stream(
        http_for_user_stream,
        env,
        api_key_for_user_stream,
        key_material,
        MarketKind::Spot,
        symbol_for_filter,
    )
    .await?;
    info!("spot userDataStream subscribed (WS-API session.logon path)");

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let tx_ctrlc = shutdown_tx.clone();
    tokio::spawn(async move {
        signal::ctrl_c().await.ok();
        info!("Ctrl-C received; shutting down");
        let _ = tx_ctrlc.send(true);
    });

    if args.minutes > 0 {
        let tx_timer = shutdown_tx;
        let secs = args.minutes as u64 * 60;
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
            info!(minutes = args.minutes, "time cap reached; shutting down");
            let _ = tx_timer.send(true);
        });
    }

    // LayeredGrid: 1 level per side, 6bps inner spread, 20bps re-entry.
    // $25 notional clears Spot minNotional on majors; size arg unused here
    // (notional_per_order drives qty = notional / price).
    let strategy = LayeredGrid::new(LayeredGridConfig {
        notional_per_order: Decimal::from(25),
        levels_per_side: 1,
        inner_bps: 6,
        max_position_usdt: Decimal::ZERO,
        take_profit_bps: 0,
        stop_loss_bps: 0,
    });

    let fill_sim = FillSim::new(FillSimConfig {
        submit_latency_ms: 0,
        cancel_latency_ms: 0,
        fees: VenueFees {
            maker_bps: 0,
            taker_bps: 0,
        },
        max_position_notional_usdt: None,
        silent_cancel_rate_per_min: 0.0,
        rng_seed: 0,
    });

    let runner_config = RunnerConfig {
        state_dir: args.state_dir,
        snapshot_every_n_events: 100,
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

    let report = run_with_resume(
        venue,
        strategy,
        fill_sim,
        symbol,
        shutdown_rx,
        runner_config,
        None,
        None,
        None,
        Some(fill_rx),
        None,
    )
    .await;

    let report_json = serde_json::to_string_pretty(&report)
        .unwrap_or_else(|e| format!("{{\"error\": \"{}\"}}", e));
    println!("{}", report_json);
    info!(
        events = report.events_processed,
        fills = report.fills_emitted,
        "Spot runner stopped"
    );
    Ok(())
}

/// Very simple split: BTCUSDT → ("BTC", "USDT"), ETHBTC → ("ETH", "BTC").
/// Checks for known 4-char quote assets (USDT, BUSD, USDC), then 3-char (BTC, ETH, BNB).
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
    // Fallback: assume last 4 chars are quote.
    let n = upper.len();
    if n > 4 {
        (&upper[..n - 4], &upper[n - 4..])
    } else {
        (upper, "USDT")
    }
}
