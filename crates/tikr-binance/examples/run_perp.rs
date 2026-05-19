//! Live testnet/mainnet single-symbol Binance **USD-M Futures (Perp)** market-maker.
//!
//! # Operator pre-flight
//!
//! ```bash
//! # 1. Get a futures testnet API key + secret:
//! #    https://testnet.binancefuture.com (log in, create API key)
//! #
//! # 2. Fund with test USDT via the testnet faucet (free, no KYC).
//! #
//! # 3. Put credentials in .env (or export to shell):
//! echo 'TIKR_BINANCE_API_KEY=your-testnet-key' >> .env
//! echo 'TIKR_BINANCE_API_SECRET=your-testnet-secret' >> .env
//!
//! # 4. Run (testnet, safe default):
//! cargo run -p tikr-binance --example run_perp -- \
//!   --env futures-testnet --symbol BTCUSDT --minutes 30
//!
//! # 5. Mainnet (requires real Binance account + futures trading permissions):
//! TIKR_BINANCE_ENABLE_MAINNET=1 cargo run -p tikr-binance --example run_perp -- \
//!   --env futures-mainnet --symbol BTCUSDT --minutes 30
//! ```
//!
//! # Key loading (priority order)
//!
//! 1. `TIKR_BINANCE_API_KEY` + `TIKR_BINANCE_API_SECRET` env vars.
//! 2. `--key-file <path>` flag (single line: `key:secret`).
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
use std::path::PathBuf;
use tikr_binance::{
    BinanceClient, BinanceEnv, load_credentials_from_env, load_credentials_from_file,
};
use tikr_core::{Asset, MarketKind, Symbol, VenueId};
use tikr_venue::Venue;
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

    /// Path to a key file (`key:secret` single line).
    /// `TIKR_BINANCE_API_KEY` / `TIKR_BINANCE_API_SECRET` take precedence.
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

    // Load credentials.
    let (api_key, api_secret) = if let Ok(creds) = load_credentials_from_env() {
        creds
    } else if let Some(ref path) = args.key_file {
        load_credentials_from_file(path).map_err(|e| format!("key-file error: {e}"))?
    } else {
        return Err("No credentials: set TIKR_BINANCE_API_KEY/SECRET or pass --key-file".into());
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
        "starting Perp runner"
    );

    let client = BinanceClient::with_credentials(env, api_key, api_secret, Some(&symbol)).await?;

    info!(client = ?client, "BinanceClient ready");

    // Subscribe to depth stream.
    let mut stream = client.subscribe(&symbol).await?;

    // Shutdown channel.
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

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

    // Event loop: print depth updates until shutdown.
    use futures::StreamExt;
    loop {
        tokio::select! {
            event = stream.next() => {
                match event {
                    Some(e) => {
                        info!(event = ?e, "market event");
                    }
                    None => {
                        warn!("depth stream ended");
                        break;
                    }
                }
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    info!("shutdown received");
                    break;
                }
            }
        }
    }

    info!("Perp runner stopped");
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
