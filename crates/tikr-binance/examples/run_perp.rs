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
use std::str::FromStr;
use std::sync::Arc;
use tikr_backtest::fill_sim::{FillSim, FillSimConfig, VenueFees};
use tikr_binance::{
    BinanceClient, BinanceEnv, BinanceKeyMaterial, env_with_product_fallback,
    load_credentials_from_file, load_key_material_from_env, product_var,
    user_stream::subscribe_user_data_stream,
};
use tikr_core::{Asset, Decimal, MarketKind, Size, Symbol, VenueId};
use tikr_paper::{RunnerConfig, run_with_resume};
use tikr_strategy::{
    AvellanedaStoikov, AvellanedaStoikovConfig, EwmaConfig, Glft, GlftConfig, NaiveGrid,
    NaiveGridConfig, Strategy,
};
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

#[derive(Debug, Clone, Copy, ValueEnum)]
enum StrategyArg {
    /// NaiveGrid — symmetric grid around mid, no inventory awareness.
    #[value(name = "naive-grid")]
    NaiveGrid,
    /// Avellaneda-Stoikov — inventory-aware finite-horizon optimal MM.
    #[value(name = "avellaneda-stoikov", alias = "as")]
    AvellanedaStoikov,
    /// GLFT (Guéant-Lehalle-Fernandez-Tapia, 2013) — infinite-horizon variant.
    #[value(name = "glft")]
    Glft,
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

    /// Strategy to run.
    #[arg(long, value_enum, default_value = "naive-grid")]
    strategy: StrategyArg,

    /// Order size per quote. Default 0.001 (works for BTCUSDT @ $76k).
    /// ETHUSDT needs ≥ 0.01 (minNotional $20 / ~$2100 ETH); BNBUSDT
    /// needs ≥ 0.01 (minQty). Other symbols vary — check
    /// `/fapi/v1/exchangeInfo`.
    #[arg(long, default_value = "0.001")]
    size: String,

    /// A-S / GLFT risk aversion γ. Controls inventory mean-reversion strength
    /// via the reservation price formula `r = mid - q·γ·σ²·(T-t)` (A-S) or
    /// `r = mid - q·γ·σ²` (GLFT). Higher γ → stronger push toward flat inventory.
    /// Does NOT affect spread width (use `--spread-bps` for that).
    #[arg(long, default_value = "0.1")]
    gamma: String,

    /// A-S / GLFT half-spread in basis points per side (e.g. 5 = 5 bps/side,
    /// 10 bps round-trip). Portable across assets: 5 bps on BTC ($76k) ≈ $38,
    /// on ETH ($2.1k) ≈ $1.05. Replaces the old price-unit `(1/γ)·ln(1+γ/k)`
    /// formula that required per-asset γ/k retuning.
    #[arg(long, default_value = "5")]
    spread_bps: u32,
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

    // Load API key (product-aware: tries TIKR_BINANCE_FUTURES_API_KEY then
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

    // Build the venue. Wrap key_material in Arc so it can be shared with the
    // user-stream pump (futures path doesn't use it, but the API requires it).
    let api_key_for_user_stream = api_key.clone();
    let key_material = Arc::new(key_material);
    let venue = BinanceClient::with_credentials(
        env,
        api_key,
        match key_material.as_ref() {
            BinanceKeyMaterial::Hmac { secret } => BinanceKeyMaterial::Hmac {
                secret: secret.clone(),
            },
            BinanceKeyMaterial::Ed25519 { signing_key } => BinanceKeyMaterial::Ed25519 {
                signing_key: signing_key.clone(),
            },
        },
        Some(&symbol),
    )
    .await?;

    info!(venue = ?venue, "BinanceClient ready");

    // Subscribe to userDataStream for fills (separate HttpClient — cheap).
    // Pass the Binance symbol string so fills for OTHER symbols on the same
    // account are filtered out (matters when multiple processes share one
    // account; Binance issues ONE listenKey per account).
    let http_for_user_stream = HttpClient::new();
    let symbol_for_filter =
        format!("{}{}", symbol.base.0.as_ref(), symbol.quote.0.as_ref()).to_uppercase();
    let fill_rx = subscribe_user_data_stream(
        http_for_user_stream,
        env,
        api_key_for_user_stream,
        key_material,
        MarketKind::Perp,
        symbol_for_filter,
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

    let size_per_quote = Size(
        Decimal::from_str(&args.size)
            .map_err(|e| format!("--size '{}' is not a valid decimal: {}", args.size, e))?,
    );
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

    info!(strategy = ?args.strategy, "strategy selected");

    // Dispatch on strategy enum. Each branch builds its concrete Strategy
    // impl and hands it to run_with_resume.
    let report = match args.strategy {
        StrategyArg::NaiveGrid => {
            // 5bps/side = 10bps round trip; ~6bps margin over 2bps maker fee.
            // Verified profitable on Binance Futures testnet 2026-05-19:
            // 3-min run, 14 fills, net +$0.36.
            let strategy = NaiveGrid::new(NaiveGridConfig {
                levels_per_side: 1,
                base_spread_bps: 5,
                level_step_bps: 1,
                size_per_quote,
                min_requote_interval_ms: 5000,
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
                Some(fill_rx),
            )
            .await
        }
        StrategyArg::AvellanedaStoikov => {
            // γ controls inventory-skew (reservation price); --spread-bps controls width.
            let strategy = AvellanedaStoikov::new(AvellanedaStoikovConfig {
                gamma: Decimal::from_str(&args.gamma)
                    .map_err(|e| format!("--gamma '{}' invalid: {}", args.gamma, e))?,
                base_spread_bps: args.spread_bps,
                horizon_sec: 3600,
                size_per_quote,
                min_requote_interval_ms: 5000,
                level_step_bps: 1,
                volatility: EwmaConfig {
                    half_life_sec: 60.0,
                    initial_var: Decimal::from_str("0.000001").unwrap(),
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
                Some(fill_rx),
            )
            .await
        }
        StrategyArg::Glft => {
            // γ controls inventory-skew (reservation price); --spread-bps controls width.
            let strategy = Glft::new(GlftConfig {
                gamma: Decimal::from_str(&args.gamma)
                    .map_err(|e| format!("--gamma '{}' invalid: {}", args.gamma, e))?,
                base_spread_bps: args.spread_bps,
                size_per_quote,
                min_requote_interval_ms: 5000,
                level_step_bps: 1,
                volatility: EwmaConfig {
                    half_life_sec: 60.0,
                    initial_var: Decimal::from_str("0.000001").unwrap(),
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
                Some(fill_rx),
            )
            .await
        }
    };

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
