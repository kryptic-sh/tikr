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
    AvellanedaStoikov, AvellanedaStoikovConfig, EwmaConfig, Glft, GlftConfig, LadderReentry,
    LadderReentryConfig, LayeredGrid, LayeredGridConfig, SimpleGap, SimpleGapConfig, Strategy,
    TopOfBook, TopOfBookConfig,
};
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

#[derive(Debug, Clone, Copy, ValueEnum)]
enum StrategyArg {
    /// Avellaneda-Stoikov — inventory-aware finite-horizon optimal MM.
    #[value(name = "avellaneda-stoikov", alias = "as")]
    AvellanedaStoikov,
    /// GLFT (Guéant-Lehalle-Fernandez-Tapia, 2013) — infinite-horizon variant.
    #[value(name = "glft")]
    Glft,
    /// TopOfBook — join or improve at best bid/ask. Post-only safe.
    #[value(name = "top-of-book", alias = "tob")]
    TopOfBook,
    /// LayeredGrid — fixed-fiat rolling ladder with re-entry scalping.
    #[value(name = "layered-grid", alias = "lg")]
    LayeredGrid,
    /// LadderReentry — seeded ladder, then opposite-side reentry on fills.
    #[value(name = "ladder-reentry", alias = "lr")]
    LadderReentry,
    /// StaticGrid — passive grid that rebuilds only when batch is consumed.
    #[value(name = "static-grid", alias = "sg")]
    StaticGrid,
    /// SimpleGap — fixed-gap pair; adds one pair after each fill.
    #[value(name = "simple-gap", alias = "sgap")]
    SimpleGap,
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
    #[arg(long, value_enum, default_value = "layered-grid")]
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

    /// TopOfBook: venue tick size (price increment). 0.1 for Binance Futures
    /// BTCUSDT/ETHUSDT, 0.01 for spot BTCUSDT.
    #[arg(long, default_value = "0.1")]
    tick_size: String,

    /// TopOfBook: improve (post inside the book by 1 tick) when the current
    /// book spread is strictly greater than this many ticks. Set high
    /// (e.g. 1000000) to always join, set 0 to always improve.
    #[arg(long, default_value_t = 1u32)]
    improve_when_spread_gt_ticks: u32,

    /// TopOfBook: maximum inventory-skew shift in ticks. `0` = no skew
    /// (symmetric). Higher values mean-revert harder toward flat. Example:
    /// `--max-skew-ticks 20` on perp BTC ($0.10 tick) shifts up to $2 per
    /// side when inventory hits `--skew-unit`.
    #[arg(long, default_value_t = 0u32)]
    max_skew_ticks: u32,

    /// TopOfBook: inventory at which skew reaches `max_skew_ticks`. Default
    /// matches `--size` for 1-unit linear scale.
    #[arg(long, default_value = "0.01")]
    skew_unit: String,

    /// TopOfBook: maximum book-imbalance shift in ticks. Positive top-of-
    /// book imbalance (bid-heavy) shifts both quotes UP. 0 disables.
    #[arg(long, default_value_t = 0u32)]
    max_imbalance_ticks: u32,
    /// LayeredGrid: fiat notional per order. Default $25 to clear Binance
    /// USD-M Futures testnet's $20 minNotional on majors.
    #[arg(long, default_value = "25")]
    lg_notional: String,
    /// LayeredGrid: orders per side (1 = 1 buy + 1 sell).
    #[arg(long, default_value_t = 1u32)]
    lg_levels: u32,
    /// LayeredGrid: spacing in bps. Controls cold-start level spacing,
    /// TP distance on fill, and same-side extension step (all the same).
    #[arg(long, default_value_t = 6u32)]
    lg_bps: u32,
    /// LadderReentry: fiat notional per order. Auto-bumped if below symbol minNotional × 1.2.
    #[arg(long, default_value = "25")]
    lr_notional: String,
    /// LadderReentry: initial orders per side.
    #[arg(long, default_value_t = 10u32)]
    lr_levels: u32,
    /// LadderReentry: initial inner spread from mid in bps.
    #[arg(long, default_value_t = 5u32)]
    lr_inner_bps: u32,
    /// LadderReentry: initial step between same-side levels in bps.
    #[arg(long, default_value_t = 1u32)]
    lr_step_bps: u32,
    /// LadderReentry: opposite-side reentry distance from filled price in bps.
    #[arg(long, default_value_t = 5u32)]
    lr_reentry_bps: u32,
    /// LadderReentry: same-side continuation distance from filled price in bps.
    #[arg(long, default_value_t = 11u32)]
    lr_continuation_bps: u32,
    /// StaticGrid: fiat notional per order. Auto-bumped if below symbol minNotional × 1.2.
    #[arg(long, default_value = "25")]
    sg_notional: String,
    /// StaticGrid: orders per side. Default 3 = 6 total open at start.
    #[arg(long, default_value_t = 3u32)]
    sg_levels: u32,
    /// StaticGrid: inner spread from mid in bps (nearest level on each side).
    #[arg(long, default_value_t = 3u32)]
    sg_inner_bps: u32,
    /// StaticGrid: step between consecutive levels on the same side in bps.
    #[arg(long, default_value_t = 3u32)]
    sg_step_bps: u32,
    /// StaticGrid: target fills-per-minute (adaptive bps scaler). `0` disables.
    /// When realised fpm exceeds target, inner/step bps widen by
    /// `(actual/target)` clamped to `[scale_min, scale_max]`.
    #[arg(long, default_value = "5")]
    sg_target_fills_per_min: String,
    /// StaticGrid: rolling window (seconds) for fill-rate measurement.
    #[arg(long, default_value_t = 60u32)]
    sg_fillrate_window_secs: u32,
    /// StaticGrid: lower bound on adaptive scale multiplier (default 1.0 = never tighten).
    #[arg(long, default_value = "1.0")]
    sg_scale_min: String,
    /// StaticGrid: upper bound on adaptive scale multiplier (default 4.0).
    #[arg(long, default_value = "4.0")]
    sg_scale_max: String,
    /// StaticGrid: disable inventory-driven asymmetric skew. By default
    /// the weak side joins best bid/ask and the strong side widens by
    /// `(1 + |ratio|)`; this flag forces a symmetric ladder.
    #[arg(long, default_value_t = false)]
    sg_no_auto_skew: bool,
    /// SimpleGap: fiat notional per order. Auto-bumped if below symbol minNotional × 1.2.
    #[arg(long, default_value = "25")]
    simple_gap_notional: String,
    /// SimpleGap: fixed distance from mid in bps.
    #[arg(long, default_value_t = 4u32)]
    simple_gap_bps: u32,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Cancel every open order for `symbol`, THEN flatten any open position
/// at touch via IOC. Used at both startup and shutdown.
///
/// Order is load-bearing: cancelling first prevents resting orders from
/// fighting the flatten IOC across the spread.
async fn reset_symbol_state(venue: &BinanceClient, symbol: &Symbol, phase: &str) {
    if let Err(e) = venue.cancel_all(symbol).await {
        warn!(phase, error = ?e, "cancel_all failed (continuing)");
    } else {
        info!(
            phase,
            "cancelled all open orders for {}{}",
            symbol.base.0.as_ref(),
            symbol.quote.0.as_ref()
        );
    }
    match venue.position(symbol).await {
        Ok(pos) if pos.size.0 != Decimal::ZERO => {
            let qty = pos.size.0.abs();
            let close_side = if pos.size.0 > Decimal::ZERO {
                tikr_core::Side::Ask
            } else {
                tikr_core::Side::Bid
            };
            let snap = venue.snapshot(symbol).await.ok();
            let close_price = snap.as_ref().and_then(|s| match close_side {
                tikr_core::Side::Bid => s.asks.first().map(|l| l.price),
                tikr_core::Side::Ask => s.bids.first().map(|l| l.price),
            });
            if let Some(price) = close_price {
                let close_intent = tikr_venue::QuoteIntent {
                    symbol: symbol.clone(),
                    side: close_side,
                    price,
                    size: tikr_core::Size(qty),
                    tif: tikr_core::TimeInForce::IOC,
                    kind: tikr_core::QuoteKind::Point,
                };
                match venue.quote(close_intent).await {
                    Ok(qid) => info!(
                        phase, side = ?close_side, qty = %qty, quote_id = ?qid,
                        "position flatten IOC submitted"
                    ),
                    Err(e) => warn!(phase, error = ?e, "position flatten failed (continuing)"),
                }
            } else {
                warn!(phase, "no book snapshot — cannot flatten position");
            }
        }
        Ok(_) => info!(phase, "no open position to flatten"),
        Err(e) => warn!(phase, error = ?e, "venue.position failed (continuing)"),
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Install the rustls crypto provider before any TLS use. With multiple
    // crates pulling rustls (reqwest, tokio-tungstenite), the provider must
    // be selected explicitly. Ignore the result — re-installing is harmless.
    let _ = rustls::crypto::ring::default_provider().install_default();

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
    let api_key_for_shutdown = api_key.clone();
    let key_material = Arc::new(key_material);
    let key_material_for_shutdown = key_material.clone();
    let symbol_for_shutdown = symbol.clone();
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
        1,
    )
    .await?;

    info!(venue = ?venue, "BinanceClient ready");

    // Startup cleanup: orders first, then position. Without this, restarts
    // inherit stale resting orders and unknown inventory which the
    // strategy didn't place and won't track correctly.
    reset_symbol_state(&venue, &symbol, "startup").await;

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
    };

    info!(strategy = ?args.strategy, "strategy selected");

    // Dispatch on strategy enum. Each branch builds its concrete Strategy
    // impl and hands it to run_with_resume.
    let report = match args.strategy {
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
                None,
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
                None,
            )
            .await
        }
        StrategyArg::TopOfBook => {
            let strategy = TopOfBook::new(TopOfBookConfig {
                size_per_quote,
                tick_size: Decimal::from_str(&args.tick_size)
                    .map_err(|e| format!("--tick-size '{}' invalid: {}", args.tick_size, e))?,
                improve_when_spread_gt_ticks: args.improve_when_spread_gt_ticks,
                min_requote_interval_ms: 1000,
                max_skew_ticks: args.max_skew_ticks,
                skew_unit: Size(
                    Decimal::from_str(&args.skew_unit)
                        .map_err(|e| format!("--skew-unit '{}' invalid: {}", args.skew_unit, e))?,
                ),
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
                Some(fill_rx),
                None,
            )
            .await
        }
        StrategyArg::LayeredGrid => {
            // Auto-bump --lg-notional to clear per-symbol minNotional from
            // exchangeInfo cache (BTC=$50, ETH=$20, alts=$5 on testnet).
            // 1.2× buffer covers the post-only safety margin where the
            // strategy posts at fill_price ± inner_bps and price ticks
            // could put us slightly under the floor.
            let requested = Decimal::from_str(&args.lg_notional)
                .map_err(|e| format!("--lg-notional '{}' invalid: {}", args.lg_notional, e))?;
            let mut notional = requested;
            if let Some(min_n) = venue.min_notional(&symbol) {
                let floor = min_n * Decimal::from_str("1.2").unwrap();
                if notional < floor {
                    warn!(
                        requested = %requested, min_notional = %min_n, bumped_to = %floor,
                        "lg-notional below symbol minNotional × 1.2 — auto-bumping"
                    );
                    notional = floor;
                }
            }
            let strategy = LayeredGrid::new(LayeredGridConfig {
                notional_per_order: notional,
                levels_per_side: args.lg_levels,
                inner_bps: args.lg_bps,
                max_position_usdt: Decimal::ZERO,
                take_profit_bps: 0,
                stop_loss_bps: 0,
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
                None,
            )
            .await
        }
        StrategyArg::LadderReentry => {
            let requested = Decimal::from_str(&args.lr_notional)
                .map_err(|e| format!("--lr-notional '{}' invalid: {}", args.lr_notional, e))?;
            let mut notional = requested;
            if let Some(min_n) = venue.min_notional(&symbol) {
                let floor = min_n * Decimal::from_str("1.2").unwrap();
                if notional < floor {
                    warn!(
                        requested = %requested, min_notional = %min_n, bumped_to = %floor,
                        "lr-notional below symbol minNotional × 1.2 — auto-bumping"
                    );
                    notional = floor;
                }
            }
            let strategy = LadderReentry::new(LadderReentryConfig {
                notional_per_order: notional,
                levels_per_side: args.lr_levels,
                inner_bps: args.lr_inner_bps,
                step_bps: args.lr_step_bps,
                reentry_bps: args.lr_reentry_bps,
                continuation_bps: args.lr_continuation_bps,
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
                None,
            )
            .await
        }
        StrategyArg::StaticGrid => {
            // Same auto-bump dance as LG — symbol-aware minNotional × 1.2 buffer.
            let requested = Decimal::from_str(&args.sg_notional)
                .map_err(|e| format!("--sg-notional '{}' invalid: {}", args.sg_notional, e))?;
            let mut notional = requested;
            if let Some(min_n) = venue.min_notional(&symbol) {
                let floor = min_n * Decimal::from_str("1.2").unwrap();
                if notional < floor {
                    warn!(
                        requested = %requested, min_notional = %min_n, bumped_to = %floor,
                        "sg-notional below symbol minNotional × 1.2 — auto-bumping"
                    );
                    notional = floor;
                }
            }
            let strategy = tikr_strategy::StaticGrid::new(tikr_strategy::StaticGridConfig {
                notional_per_order: notional,
                levels_per_side: args.sg_levels,
                inner_bps: args.sg_inner_bps,
                step_bps: args.sg_step_bps,
                target_fills_per_min: Decimal::from_str(&args.sg_target_fills_per_min).map_err(
                    |e| {
                        format!(
                            "--sg-target-fills-per-min '{}' invalid: {}",
                            args.sg_target_fills_per_min, e
                        )
                    },
                )?,
                fillrate_window_secs: args.sg_fillrate_window_secs,
                scale_min: Decimal::from_str(&args.sg_scale_min).map_err(|e| {
                    format!("--sg-scale-min '{}' invalid: {}", args.sg_scale_min, e)
                })?,
                scale_max: Decimal::from_str(&args.sg_scale_max).map_err(|e| {
                    format!("--sg-scale-max '{}' invalid: {}", args.sg_scale_max, e)
                })?,
                auto_skew: !args.sg_no_auto_skew,
                step_size: venue.step_size(&symbol).unwrap_or(Decimal::ONE),
                min_notional: venue.min_notional(&symbol).unwrap_or(Decimal::ZERO),
                regime_window_secs: 0,
                regime_trend_threshold_bps: 10,
                regime_efficiency_threshold: Decimal::ZERO,
                max_position_usdt: Decimal::ZERO,
                take_profit_bps: 0,
                stop_loss_bps: 0,
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
                None,
            )
            .await
        }
        StrategyArg::SimpleGap => {
            let requested = Decimal::from_str(&args.simple_gap_notional).map_err(|e| {
                format!(
                    "--simple-gap-notional '{}' invalid: {}",
                    args.simple_gap_notional, e
                )
            })?;
            let mut notional = requested;
            if let Some(min_n) = venue.min_notional(&symbol) {
                let floor = min_n * Decimal::from_str("1.2").unwrap();
                if notional < floor {
                    warn!(
                        requested = %requested, min_notional = %min_n, bumped_to = %floor,
                        "simple-gap-notional below symbol minNotional × 1.2 — auto-bumping"
                    );
                    notional = floor;
                }
            }
            let strategy = SimpleGap::new(SimpleGapConfig {
                notional_per_order: notional,
                gap_bps: args.simple_gap_bps,
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
                None,
            )
            .await
        }
    };

    // Shutdown cleanup: mirror the startup cleanup so we never leave
    // orphan orders or open inventory on the venue when this instance
    // exits (--minutes cap, SIGINT, stream EOF, …). Symbol-scoped, so
    // other symbols on the same account are not touched. The original
    // venue handle was consumed by run_with_resume, so we rebuild a
    // fresh client from saved credentials.
    let shutdown_venue = BinanceClient::with_credentials(
        env,
        api_key_for_shutdown,
        match key_material_for_shutdown.as_ref() {
            BinanceKeyMaterial::Hmac { secret } => BinanceKeyMaterial::Hmac {
                secret: secret.clone(),
            },
            BinanceKeyMaterial::Ed25519 { signing_key } => BinanceKeyMaterial::Ed25519 {
                signing_key: signing_key.clone(),
            },
        },
        Some(&symbol_for_shutdown),
        1,
    )
    .await?;
    reset_symbol_state(&shutdown_venue, &symbol_for_shutdown, "shutdown").await;

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
