//! Live BSC mainnet single-pair DODO LimitOrder market-maker (v1 — Chainlink-driven).
//!
//! # Architecture
//!
//! DODO LimitOrder provides the full `Venue` trait surface:
//! - `quote()` → sign EIP-712 → POST to DODO API → order live on BSC
//! - `requote()` → warn (old order self-expires) → place new order
//! - `cancel()` / `cancel_all()` → no-op + warn (self-expiry is the mechanism)
//! - `snapshot()` → Chainlink `latestRoundData()` → 1-level BBO around mid
//! - `subscribe()` → poll Chainlink every `price_poll_interval_secs` seconds →
//!   emit `MarketEvent::BookUpdate`
//!
//! The price feed is driven by the Chainlink BNB/USD aggregator on BSC mainnet:
//! `0x0567F2323251f0Aab15c8dFb1967E4e8A7D42aeE`. No synthetic mid-price needed.
//!
//! # Operator pre-flight
//!
//! ## 1. Get DODO API key
//!
//! Visit https://developer.dodoex.io, connect your wallet, copy the API key
//! into `.env`:
//! ```text
//! TIKR_DODO_API_KEY=your-api-key-here
//! ```
//!
//! ## 2. Set private key
//!
//! ```text
//! TIKR_BSC_PRIVATE_KEY=0xac0974...   # 0x-prefixed hex
//! ```
//!
//! Or pass `--key-file /path/to/keyfile` (single-line hex, 0x-prefix optional).
//!
//! ## 3. Set BSC RPC URL (WebSocket, used for fills + Chainlink reads)
//!
//! ```text
//! TIKR_BSC_RPC_URL=wss://your-bsc-wss-endpoint
//! ```
//!
//! ## 4. Set Chainlink feed address
//!
//! ```text
//! TIKR_CHAINLINK_FEED=0x0567F2323251f0Aab15c8dFb1967E4e8A7D42aeE
//! ```
//!
//! Or pass `--chainlink-feed <address>` on the command line.
//! Default (if neither set): BNB/USD on BSC mainnet.
//!
//! ## 5. Wrap BNB → WBNB (one-time per wallet)
//!
//! ```bash
//! cast send 0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c \
//!     "deposit()" \
//!     --value 0.1ether --rpc-url $TIKR_BSC_RPC_URL \
//!     --private-key $TIKR_BSC_PRIVATE_KEY
//! ```
//!
//! ## 6. Approve DODO to spend WBNB and USDT (one-time per token)
//!
//! DODOApprove contract on BSC: `0xa128Ba44B2738A558A1fdC06d6303d52D3Cef8c1`
//!
//! ```bash
//! # Approve WBNB (0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c)
//! cast send 0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c \
//!     "approve(address,uint256)" \
//!     0xa128Ba44B2738A558A1fdC06d6303d52D3Cef8c1 \
//!     115792089237316195423570985008687907853269984665640564039457584007913129639935 \
//!     --rpc-url $TIKR_BSC_RPC_URL --private-key $TIKR_BSC_PRIVATE_KEY
//!
//! # Approve USDT (0x55d398326f99059fF775485246999027B3197955)
//! cast send 0x55d398326f99059fF775485246999027B3197955 \
//!     "approve(address,uint256)" \
//!     0xa128Ba44B2738A558A1fdC06d6303d52D3Cef8c1 \
//!     115792089237316195423570985008687907853269984665640564039457584007913129639935 \
//!     --rpc-url $TIKR_BSC_RPC_URL --private-key $TIKR_BSC_PRIVATE_KEY
//! ```
//!
//! ## 7. Run
//!
//! ```bash
//! TIKR_DODO_ENABLE_MAINNET=1 cargo run -p tikr-dodo --example run_live -- \
//!     --pair WBNB/USDT \
//!     --minutes 30
//! ```
//!
//! ## Notes
//!
//! - Orders self-cancel after `--expiry-secs` seconds (default 60). No cancel API in v0.
//! - Market data is driven by Chainlink BNB/USD (configurable via `--chainlink-feed`).
//! - The subscribe loop polls every `--poll-interval` seconds (default 5s).
//! - Spread is `--spread-bps` per side (default 20 = 0.20%).
//! - WBNB and USDT both use 18 decimals on BSC.
//! - Maker rebates: DODO LimitOrder charges no maker fee. Spread is your profit.
//! - See issue #42 for real cancel, #43 for approval helper, #44 for WBNB wrap helper.

use alloy_primitives::Address;
use alloy_signer_local::PrivateKeySigner;
use clap::Parser;
use std::path::PathBuf;
use std::str::FromStr;
use tikr_core::QuoteKind;
use tikr_core::{Asset, Decimal, MarketEvent, Price, Side, Size, Symbol, TimeInForce, VenueId};
use tikr_dodo::{DodoClient, DodoConfig};
use tikr_venue::Venue;
use tracing::{info, warn};

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser, Debug)]
#[command(
    name = "run_live",
    about = "Single-pair live DODO LimitOrder market-maker on BSC mainnet (Chainlink-driven)"
)]
struct Args {
    /// Token pair to trade in BASE/QUOTE format (e.g. `WBNB/USDT`).
    /// Operator must configure token addresses via environment variables.
    #[arg(long, default_value = "WBNB/USDT")]
    pair: String,

    /// Path to a file containing the BSC private key (single-line hex, 0x-prefix optional).
    /// `TIKR_BSC_PRIVATE_KEY` env var takes precedence if set.
    #[arg(long)]
    key_file: Option<PathBuf>,

    /// State directory for snapshots.
    #[arg(long, default_value = "./state")]
    state_dir: PathBuf,

    /// Run duration in minutes. 0 = run until Ctrl-C.
    #[arg(long, default_value_t = 0u32)]
    minutes: u32,

    /// Order expiry in seconds. Orders self-cancel after this time (default 60s).
    /// Keep ≤ your requote interval to bound max open-order age.
    #[arg(long, default_value_t = 60u64)]
    expiry_secs: u64,

    /// Chainlink AggregatorV3Interface feed address.
    /// Defaults to `TIKR_CHAINLINK_FEED` env var, then BNB/USD on BSC mainnet
    /// (`0x0567F2323251f0Aab15c8dFb1967E4e8A7D42aeE`).
    #[arg(long)]
    chainlink_feed: Option<String>,

    /// Chainlink poll interval in seconds (default 5s).
    /// Polls faster than the 30s heartbeat so updates are caught promptly.
    #[arg(long, default_value_t = 5u64)]
    poll_interval: u64,

    /// Spread per side in basis points (default 20 = 0.20% per side).
    #[arg(long, default_value_t = 20u16)]
    spread_bps: u16,
}

// ---------------------------------------------------------------------------
// Key loading
// ---------------------------------------------------------------------------

fn load_key(args: &Args) -> Result<PrivateKeySigner, Box<dyn std::error::Error>> {
    if let Ok(raw) = std::env::var("TIKR_BSC_PRIVATE_KEY") {
        return parse_key(&raw).map_err(|e| format!("TIKR_BSC_PRIVATE_KEY: {}", e).into());
    }
    if let Some(ref path) = args.key_file {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| format!("key-file {}: {}", path.display(), e))?;
        return parse_key(raw.trim())
            .map_err(|e| format!("key-file {}: {}", path.display(), e).into());
    }
    Err("No private key: set TIKR_BSC_PRIVATE_KEY or pass --key-file".into())
}

fn parse_key(raw: &str) -> Result<PrivateKeySigner, String> {
    let hex = raw.strip_prefix("0x").unwrap_or(raw);
    let bytes = hex::decode(hex).map_err(|e| format!("hex decode: {}", e))?;
    PrivateKeySigner::from_slice(&bytes).map_err(|e| format!("key parse: {}", e))
}

// ---------------------------------------------------------------------------
// Symbol + token address parsing
// ---------------------------------------------------------------------------

/// Parse "BASE/QUOTE" pair string into base and quote asset names.
fn parse_pair(pair: &str) -> Result<(String, String), Box<dyn std::error::Error>> {
    let parts: Vec<&str> = pair.splitn(2, '/').collect();
    if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
        return Err(format!(
            "invalid pair format '{}'; expected BASE/QUOTE (e.g. WBNB/USDT)",
            pair
        )
        .into());
    }
    Ok((parts[0].to_uppercase(), parts[1].to_uppercase()))
}

/// Load token addresses from env vars.
///
/// Env vars:
/// - `TIKR_DODO_MAKER_TOKEN` — maker token address (ERC-20 hex)
/// - `TIKR_DODO_TAKER_TOKEN` — taker token address (ERC-20 hex)
///
/// Defaults (for WBNB/USDT on BSC mainnet):
/// - WBNB: 0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c
/// - USDT: 0x55d398326f99059fF775485246999027B3197955
fn load_token_addresses() -> Result<(Address, Address), Box<dyn std::error::Error>> {
    let maker_raw = std::env::var("TIKR_DODO_MAKER_TOKEN")
        .unwrap_or_else(|_| "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c".to_string());
    let taker_raw = std::env::var("TIKR_DODO_TAKER_TOKEN")
        .unwrap_or_else(|_| "0x55d398326f99059fF775485246999027B3197955".to_string());

    let maker = Address::from_str(&maker_raw)
        .map_err(|e| format!("TIKR_DODO_MAKER_TOKEN '{}': {}", maker_raw, e))?;
    let taker = Address::from_str(&taker_raw)
        .map_err(|e| format!("TIKR_DODO_TAKER_TOKEN '{}': {}", taker_raw, e))?;

    Ok((maker, taker))
}

/// Resolve the Chainlink feed address.
///
/// Priority:
/// 1. `--chainlink-feed <addr>` CLI flag
/// 2. `TIKR_CHAINLINK_FEED` env var
/// 3. Default: BNB/USD on BSC mainnet (`0x0567F2323251f0Aab15c8dFb1967E4e8A7D42aeE`)
fn resolve_chainlink_feed(args: &Args) -> Result<Address, Box<dyn std::error::Error>> {
    const BNB_USD_FEED: &str = "0x0567F2323251f0Aab15c8dFb1967E4e8A7D42aeE";

    let raw = args
        .chainlink_feed
        .clone()
        .or_else(|| std::env::var("TIKR_CHAINLINK_FEED").ok())
        .unwrap_or_else(|| BNB_USD_FEED.to_string());

    Address::from_str(&raw).map_err(|e| format!("Chainlink feed address '{}': {}", raw, e).into())
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

    // Mainnet gate: refuse to run without explicit opt-in.
    if std::env::var("TIKR_DODO_ENABLE_MAINNET").as_deref() != Ok("1") {
        eprintln!(
            "ERROR: TIKR_DODO_ENABLE_MAINNET=1 is required.\n\
             DODO LimitOrder operates on BSC mainnet — all orders are real funds.\n\
             Set TIKR_DODO_ENABLE_MAINNET=1 to confirm you understand this."
        );
        std::process::exit(1);
    }

    let (base_name, quote_name) = parse_pair(&args.pair)?;

    let symbol = Symbol {
        base: Asset::new(&base_name),
        quote: Asset::new(&quote_name),
        venue: VenueId::new("dodo"),
    };

    let signer = load_key(&args)?;
    let address = signer.address().to_checksum(None);
    info!(address = %address, pair = %args.pair, "signer loaded");

    let (base_token, quote_token) = load_token_addresses()?;
    info!(
        base_token = %base_token,
        quote_token = %quote_token,
        "token addresses loaded"
    );

    let chainlink_feed = resolve_chainlink_feed(&args)?;
    info!(feed = %chainlink_feed, "Chainlink feed resolved");

    let rpc_ws_url = std::env::var("TIKR_BSC_RPC_URL")
        .unwrap_or_else(|_| "wss://bsc-ws-node.nariox.org".to_string());

    let config = DodoConfig {
        order_expiry_secs: args.expiry_secs,
        base_token,
        quote_token,
        rpc_ws_url: rpc_ws_url.clone(),
        chainlink_feed_addr: Some(chainlink_feed),
        price_poll_interval_secs: args.poll_interval,
        spread_bps: args.spread_bps,
    };

    warn!(
        "MAINNET mode — DODO LimitOrder on BSC mainnet. Real funds at risk.\n\
         Order expiry: {}s. cancel()/cancel_all() are no-ops in v0 (self-expiry only).\n\
         Chainlink feed: {}. Poll interval: {}s. Spread: {} bps per side.",
        args.expiry_secs, chainlink_feed, args.poll_interval, args.spread_bps
    );

    let venue = DodoClient::with_wallet(config, signer)?;
    let order_map = venue.order_map();

    info!(
        pair = %args.pair,
        maker = %address,
        base_token = %base_token,
        quote_token = %quote_token,
        expiry_secs = args.expiry_secs,
        state_dir = %args.state_dir.display(),
        chainlink_feed = %chainlink_feed,
        poll_interval_secs = args.poll_interval,
        spread_bps = args.spread_bps,
        "starting DODO live runner (Chainlink-driven)"
    );

    // Subscribe to fill events via BSC log subscription.
    let fill_rx = tikr_dodo::subscribe_fills(
        rpc_ws_url,
        alloy_primitives::Address::from_str(&address)?,
        order_map,
        symbol.clone(),
    )
    .await;

    match &fill_rx {
        Ok(_) => info!("DODO fill subscription active"),
        Err(e) => warn!(
            error = %e,
            "DODO fill subscription failed (BSC WS may be unavailable); \
             continuing without live fill events. Use a private RPC for production."
        ),
    }

    // Subscribe to Chainlink-driven market data.
    let mut market_stream = venue.subscribe(&symbol).await?;

    // Shutdown channel.
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

    // Wire Ctrl-C.
    let tx_ctrlc = shutdown_tx.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
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

    let size = Size(Decimal::from_str_exact("0.001")?); // 0.001 WBNB per quote

    // Main loop: consume Chainlink BookUpdate events and place quotes on each update.
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    info!("shutdown signal received; exiting");
                    break;
                }
            }
            event = futures::StreamExt::next(&mut market_stream) => {
                match event {
                    None => {
                        info!("market stream ended; exiting");
                        break;
                    }
                    Some(MarketEvent::BookUpdate { snapshot }) => {
                        let best_bid = snapshot.bids.first();
                        let best_ask = snapshot.asks.first();

                        if let (Some(bid), Some(ask)) = (best_bid, best_ask) {
                            info!(
                                bid = %bid.price.0,
                                ask = %ask.price.0,
                                "Chainlink market update"
                            );

                            // Place a bid quote.
                            let bid_intent = tikr_venue::QuoteIntent {
                                symbol: symbol.clone(),
                                side: Side::Bid,
                                price: bid.price,
                                size,
                                tif: TimeInForce::GTC,
                                kind: QuoteKind::Point,
                            };

                            match venue.quote(bid_intent).await {
                                Ok(quote_id) => info!(quote_id = ?quote_id, "bid quote placed"),
                                Err(e) => warn!(error = %e, "bid quote failed"),
                            }

                            // Place an ask quote.
                            let ask_intent = tikr_venue::QuoteIntent {
                                symbol: symbol.clone(),
                                side: Side::Ask,
                                price: Price(ask.price.0),
                                size,
                                tif: TimeInForce::GTC,
                                kind: QuoteKind::Point,
                            };

                            match venue.quote(ask_intent).await {
                                Ok(quote_id) => info!(quote_id = ?quote_id, "ask quote placed"),
                                Err(e) => warn!(error = %e, "ask quote failed"),
                            }
                        }
                    }
                    Some(other) => {
                        // Heartbeats, fills, etc. — log and continue.
                        info!(event = ?other, "market event (non-book)");
                    }
                }
            }
        }
    }

    Ok(())
}
