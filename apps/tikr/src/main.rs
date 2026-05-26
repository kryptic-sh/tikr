//! tikr — multi-bot live trading orchestrator.
//!
//! ```bash
//! tikr                  # auto-discover config, launch TUI
//! tikr --headless       # same but no TUI (for SSH / CI / smoke tests)
//! tikr --config <path>  # explicit override
//! tikr --check          # validate + exit
//! ```
//!
//! Config discovery (when `--config` is not passed):
//!   1. `./config.toml`                       — cwd, wins if present
//!   2. `$XDG_CONFIG_HOME/tikr/config.toml`   — defaults to `~/.config/tikr/config.toml`

mod bnb_refill;
mod build;
mod config;
mod logs;
mod scalp_rotation;
mod selection;
mod state;
mod supervisor;
mod tui;
mod venue;

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use rust_decimal::Decimal;
use tokio::sync::watch;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::logs::LogStore;
use crate::state::{ApiAccountSnapshot, ApiPositionSnapshot, BotStatus, BotView, SharedBotState};
use crate::supervisor::{SupervisorCtx, spawn_supervisor};

#[derive(Parser, Debug)]
#[command(name = "tikr", about = "Multi-bot live trading orchestrator")]
struct Args {
    /// Path to the dashboard config TOML. If omitted, the loader looks
    /// at `./config.toml` first, then `$XDG_CONFIG_HOME/tikr/config.toml`.
    #[arg(long)]
    config: Option<PathBuf>,

    /// Validate the config and exit without spawning bots.
    #[arg(long)]
    check: bool,

    /// Run without a TUI — spawn bots, log to stdout, exit on Ctrl-C.
    /// Useful for SSH sessions, CI/smoke tests, or any place where the
    /// interactive TUI is in the way.
    #[arg(long)]
    headless: bool,

    /// Headless-only: stop after `--minutes` (0 = run until Ctrl-C).
    /// Ignored in TUI mode.
    #[arg(long, default_value_t = 0u32)]
    minutes: u32,

    /// Override [account].order_balance_pct for computed per-order notional.
    /// Split evenly across configured bots. Example: 10 with 2 bots = 5% each.
    #[arg(long)]
    order_balance_pct: Option<Decimal>,

    /// Override [account].leverage for the POST /fapi/v1/leverage call
    /// applied to each bot's symbol at startup.
    #[arg(long)]
    leverage: Option<u32>,

    /// Reset open positions + cancel all resting orders at startup
    /// before spawning bots. Default `false` — the bot resumes against
    /// the existing live state so a quick code-change / restart cycle
    /// doesn't churn positions. Pass `--clear` for a clean-slate boot
    /// (mirrors the pre-2026-05-24 default behaviour).
    #[arg(long, default_value_t = false)]
    clear: bool,
}

/// Resolve the config path using cwd-first → XDG fallback discovery.
///
/// Returns the resolved path (display-able) AND the path that was
/// actually opened, so the TUI can surface the source.
fn resolve_config_path(cli: Option<&std::path::Path>) -> anyhow::Result<PathBuf> {
    if let Some(p) = cli {
        if !p.exists() {
            anyhow::bail!("--config '{}' does not exist", p.display());
        }
        return Ok(p.to_path_buf());
    }
    let cwd = std::path::Path::new("./config.toml");
    if cwd.exists() {
        return Ok(cwd.to_path_buf());
    }
    let xdg = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|h| {
                let mut p = PathBuf::from(h);
                p.push(".config");
                p
            })
        });
    if let Some(mut base) = xdg {
        base.push("tikr");
        base.push("config.toml");
        if base.exists() {
            return Ok(base);
        }
    }
    anyhow::bail!(
        "no config found. searched: ./config.toml, $XDG_CONFIG_HOME/tikr/config.toml \
         (default ~/.config/tikr/config.toml). Pass --config <path> to override."
    )
}

struct AccountPollerConfig {
    shared_state: SharedBotState,
    notional_tx: watch::Sender<Decimal>,
    max_position_tx: watch::Sender<Decimal>,
    max_position_pct: Decimal,
    env: tikr_binance::BinanceEnv,
    api_key: String,
    key_material: Arc<tikr_binance::BinanceKeyMaterial>,
    symbols: Vec<String>,
    bot_count: usize,
    order_balance_pct: Decimal,
    shutdown: watch::Receiver<bool>,
    /// Published price (USDT per BNB) for the user-stream parser to
    /// convert BNB commissions → USDT-equivalent. Set to ZERO when
    /// BNB-pays-fees is disabled on the account.
    bnb_price_tx: watch::Sender<Decimal>,
}

fn spawn_account_balance_poller(cfg: AccountPollerConfig) {
    tokio::spawn(async move {
        let http = reqwest::Client::new();
        let mut shutdown = cfg.shutdown;
        let key_material = match cfg.key_material.as_ref() {
            tikr_binance::BinanceKeyMaterial::Hmac { secret } => {
                tikr_binance::BinanceKeyMaterial::Hmac {
                    secret: secret.clone(),
                }
            }
            tikr_binance::BinanceKeyMaterial::Ed25519 { signing_key } => {
                tikr_binance::BinanceKeyMaterial::Ed25519 {
                    signing_key: signing_key.clone(),
                }
            }
        };

        // BNB-fee autodetect: one-time at startup, cached for the
        // remainder of the process. Cheap (1 REST call) but logs
        // loudly so operators can see which mode they're in.
        let bnb_fee_enabled = match tikr_binance::futs::get_fee_burn_status(
            &http,
            cfg.env.rest_base_url(),
            &cfg.api_key,
            &key_material,
        )
        .await
        {
            Ok(on) => {
                tracing::info!(
                    enabled = on,
                    "feeBurn status: BNB-pays-fees {}",
                    if on { "ENABLED" } else { "disabled" }
                );
                on
            }
            Err(e) => {
                tracing::warn!(error = ?e, "feeBurn check failed; assuming disabled");
                false
            }
        };

        loop {
            // BNB-aware accounting block — only fires when feeBurn is on.
            // Fetches BNB futures-wallet balance + BNBUSDT mark, publishes
            // to SharedState + price watch channel. The user-stream parser
            // reads `bnb_price_tx` to convert commissions; the refill task
            // reads SharedState to decide when to top up.
            if bnb_fee_enabled {
                let mut bnb_balance = Decimal::ZERO;
                let mut bnb_price = Decimal::ZERO;
                if let Ok(b) = tikr_binance::futs::get_balance(
                    &http,
                    cfg.env.rest_base_url(),
                    &cfg.api_key,
                    &key_material,
                    "BNB",
                )
                .await
                {
                    bnb_balance = b.wallet_balance;
                }
                if let Ok(t) =
                    tikr_binance::futs::get_book_ticker(&http, cfg.env.rest_base_url(), "BNBUSDT")
                        .await
                {
                    bnb_price = (t.bid_price + t.ask_price) / Decimal::from(2);
                }
                cfg.shared_state.set_bnb(crate::state::BnbState {
                    enabled: true,
                    balance: bnb_balance,
                    price_usdt: bnb_price,
                    fetched_at_ms: current_time_ms(),
                });
                if bnb_price > Decimal::ZERO && bnb_price != *cfg.bnb_price_tx.borrow() {
                    let _ = cfg.bnb_price_tx.send(bnb_price);
                }
                tracing::info!(
                    bnb_balance = %bnb_balance,
                    bnb_price = %bnb_price,
                    bnb_usdt_value = %(bnb_balance * bnb_price),
                    "bnb poll"
                );
            }
            match tikr_binance::futs::get_balance(
                &http,
                cfg.env.rest_base_url(),
                &cfg.api_key,
                &key_material,
                "USDT",
            )
            .await
            {
                Ok(balance) => {
                    cfg.shared_state.set_api_account(ApiAccountSnapshot {
                        asset: "USDT".to_string(),
                        wallet_balance: balance.wallet_balance,
                        available_balance: balance.available_balance,
                        cross_unrealized_pnl: balance.cross_unrealized_pnl,
                        fetched_at_ms: current_time_ms(),
                    });
                    let symbols = cfg.shared_state.symbols();
                    let symbols = if symbols.is_empty() {
                        cfg.symbols.clone()
                    } else {
                        symbols
                    };
                    let bot_count = Decimal::from(cfg.bot_count.max(1) as u64);
                    // Sizing is purely wallet-relative — leverage only
                    // affects the Binance POST /fapi/v1/leverage call.
                    // With max_position_pct=100, the per-bot cap is the
                    // full wallet split by bot_count, regardless of how
                    // much margin headroom leverage exposes.
                    let notional = balance.wallet_balance * cfg.order_balance_pct
                        / Decimal::from(100)
                        / bot_count;
                    if notional != *cfg.notional_tx.borrow() {
                        let _ = cfg.notional_tx.send(notional);
                    }
                    let max_position = balance.wallet_balance * cfg.max_position_pct
                        / Decimal::from(100)
                        / bot_count;
                    if max_position != *cfg.max_position_tx.borrow() {
                        let _ = cfg.max_position_tx.send(max_position);
                    }
                    for symbol in &symbols {
                        tracing::info!(
                            symbol,
                            wallet = %balance.wallet_balance,
                            available = %balance.available_balance,
                            api_unrealized = %balance.cross_unrealized_pnl,
                            "account balance poll"
                        );
                    }
                }
                Err(e) => tracing::warn!(error = ?e, "account balance poll failed"),
            }

            let symbols = cfg.shared_state.symbols();
            let symbols = if symbols.is_empty() {
                cfg.symbols.clone()
            } else {
                symbols
            };
            for symbol in &symbols {
                match tikr_binance::futs::get_position_risk(
                    &http,
                    cfg.env.rest_base_url(),
                    &cfg.api_key,
                    &key_material,
                    symbol,
                )
                .await
                {
                    Ok(pos) => {
                        tracing::info!(
                            symbol,
                            amount = %pos.position_amount,
                            entry = %pos.entry_price,
                            breakeven = %pos.break_even_price,
                            mark = %pos.mark_price,
                            api_unrealized = %pos.unrealized_profit,
                            "position risk poll"
                        );
                        cfg.shared_state.set_api_position(
                            symbol,
                            ApiPositionSnapshot {
                                position_amount: pos.position_amount,
                                entry_price: pos.entry_price,
                                break_even_price: pos.break_even_price,
                                mark_price: pos.mark_price,
                                unrealized_profit: pos.unrealized_profit,
                                fetched_at_ms: current_time_ms(),
                            },
                        );
                    }
                    Err(e) => tracing::warn!(symbol, error = ?e, "position risk poll failed"),
                }
            }

            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    });
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let _ = dotenvy::dotenv();

    let args = Args::parse();
    let config_path = resolve_config_path(args.config.as_deref())?;
    let mut cfg = config::load(&config_path)?;
    if let Some(pct) = args.order_balance_pct {
        cfg.account.order_balance_pct = pct;
    }
    if let Some(lev) = args.leverage {
        cfg.account.leverage = lev;
    }
    if cfg.account.order_balance_pct <= Decimal::ZERO {
        anyhow::bail!("order_balance_pct must be positive");
    }
    if cfg.account.leverage == 0 {
        anyhow::bail!("leverage must be >= 1");
    }

    if args.check {
        println!(
            "config OK ({}): {} bots configured",
            config_path.display(),
            cfg.bots.len()
        );
        for b in &cfg.bots {
            println!("  - {} ({})", b.symbol, b.strategy);
        }
        return Ok(());
    }

    // Tracing setup differs by mode:
    // - TUI mode: per-bot LogStore + custom Layer routes events to the
    //   active tab's log pane (no stdout writes, the TUI owns the screen).
    // - Headless mode: standard fmt::layer to stdout for SSH / CI runs.
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(
            "info,\
             tikr=info,\
             tikr_paper=info,\
             tikr_binance=info,\
             tikr_strategy=info,\
             tikr_backtest=info,\
             tikr_venue=info,\
             tikr_risk=info",
        )
    });
    let log_store = LogStore::new();
    if args.headless {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer())
            .init();
    } else {
        let log_layer = crate::logs::LogLayer::new(log_store.clone());
        tracing_subscriber::registry()
            .with(env_filter)
            .with(log_layer)
            .init();
    }

    // Account-wide credentials.
    let env = venue::parse_env(&cfg.account.env)?;
    let (api_key, key_material) = venue::load_credentials(env, cfg.account.key_file.as_deref())?;
    let key_material: Arc<tikr_binance::BinanceKeyMaterial> = key_material;

    let shared_state = SharedBotState::new();

    let rotation_enabled = cfg.scalp_rotation.as_ref().is_some_and(|r| r.enabled)
        || cfg.static_grid_rotation.as_ref().is_some_and(|r| r.enabled);

    // Pre-seed static BotViews so the TUI has tabs from frame 1. Rotating
    // modes insert real active symbols after the volatility scan; do not
    // insert template bots or they appear stuck in `starting` forever.
    if !rotation_enabled {
        for b in &cfg.bots {
            let view = BotView {
                label: format!("{}/{}", b.symbol, b.strategy),
                symbol: b.symbol.clone(),
                strategy: b.strategy.clone(),
                status: BotStatus::Starting,
                snapshot: Arc::new(std::sync::RwLock::new(None)),
                live: Arc::new(std::sync::RwLock::new(None)),
                shutdown_tx: None,
                api_position: Arc::new(std::sync::RwLock::new(None)),
            };
            shared_state.insert(&b.symbol, view);
        }
    }

    // Global shutdown channel — TUI flips it on `q`; supervisors observe.
    let (global_shutdown_tx, global_shutdown_rx) = watch::channel(false);
    let (notional_tx, notional_rx) = watch::channel(Decimal::ZERO);
    let (max_position_tx, max_position_rx) = watch::channel(Decimal::ZERO);
    // Live BNBUSDT mid; account poller refreshes when feeBurn is on.
    // Subscribers (user_stream parser, refill task) read latest via `borrow()`.
    let (bnb_price_tx, bnb_price_rx) = watch::channel(Decimal::ZERO);

    let total_slots = cfg
        .scalp_rotation
        .as_ref()
        .filter(|r| r.enabled)
        .map(|r| r.slots)
        .unwrap_or(0)
        + cfg
            .static_grid_rotation
            .as_ref()
            .filter(|r| r.enabled)
            .map(|r| r.slots)
            .unwrap_or(0);

    spawn_account_balance_poller(AccountPollerConfig {
        shared_state: shared_state.clone(),
        notional_tx,
        max_position_tx,
        max_position_pct: cfg.account.max_position_pct,
        env,
        api_key: api_key.clone(),
        key_material: key_material.clone(),
        symbols: cfg.bots.iter().map(|b| b.symbol.clone()).collect(),
        bot_count: total_slots.max(1),
        order_balance_pct: cfg.account.order_balance_pct,
        shutdown: global_shutdown_rx.clone(),
        bnb_price_tx,
    });
    // Silence unused-rx warning until user_stream parser wires it up.
    let _bnb_price_rx_for_parser = bnb_price_rx.clone();

    // BNB-balance monitor — warns when balance drops below threshold.
    // No-ops when bnb_refill_enabled=false in TOML OR when the account
    // doesn't have BNB-pays-fees enabled. Refill is monitor-only for
    // now; spot buy + transfer wiring will land in a follow-up commit
    // once tikr-binance has a SAPI module.
    bnb_refill::spawn_bnb_monitor(bnb_refill::BnbMonitorConfig {
        shared_state: shared_state.clone(),
        min_balance_usdt: cfg.account.bnb_min_balance_usdt,
        target_balance_usdt: cfg.account.bnb_target_balance_usdt,
        refill_enabled: cfg.account.bnb_refill_enabled,
        shutdown: global_shutdown_rx.clone(),
    });

    // Spawn supervisors. Each enabled rotation type gets its own manager.
    let mut supervisors = Vec::new();
    if let Some(rotation) = cfg.scalp_rotation.clone().filter(|r| r.enabled) {
        supervisors.push(scalp_rotation::spawn_rotation_manager(
            rotation,
            cfg.bots.clone(),
            scalp_rotation::RotationAccountCtx {
                env,
                api_key: api_key.clone(),
                key_material: key_material.clone(),
                base_state_dir: cfg.account.state_dir.clone(),
                order_balance_pct: cfg.account.order_balance_pct,
                leverage: cfg.account.leverage,
                max_position_pct: cfg.account.max_position_pct,
                notional_rx: notional_rx.clone(),
                max_position_rx: max_position_rx.clone(),
                bnb_price_rx: bnb_price_rx.clone(),
            },
            shared_state.clone(),
            global_shutdown_rx.clone(),
        ));
    }
    if let Some(rotation) = cfg.static_grid_rotation.clone().filter(|r| r.enabled) {
        supervisors.push(scalp_rotation::spawn_rotation_manager(
            rotation,
            cfg.bots.clone(),
            scalp_rotation::RotationAccountCtx {
                env,
                api_key: api_key.clone(),
                key_material: key_material.clone(),
                base_state_dir: cfg.account.state_dir.clone(),
                order_balance_pct: cfg.account.order_balance_pct,
                leverage: cfg.account.leverage,
                max_position_pct: cfg.account.max_position_pct,
                notional_rx: notional_rx.clone(),
                max_position_rx: max_position_rx.clone(),
                bnb_price_rx: bnb_price_rx.clone(),
            },
            shared_state.clone(),
            global_shutdown_rx.clone(),
        ));
    }
    if !rotation_enabled {
        supervisors.reserve(cfg.bots.len());
        for b in &cfg.bots {
            let ctx = SupervisorCtx {
                cfg: b.clone(),
                env,
                api_key: api_key.clone(),
                key_material: key_material.clone(),
                base_state_dir: cfg.account.state_dir.clone(),
                order_balance_pct: cfg.account.order_balance_pct,
                leverage: cfg.account.leverage,
                max_position_pct: cfg.account.max_position_pct,
                bot_count: cfg.bots.len(),
                notional_rx: notional_rx.clone(),
                max_position_rx: max_position_rx.clone(),
                bnb_price_rx: bnb_price_rx.clone(),
                clear_on_start: args.clear,
            };
            let h = spawn_supervisor(ctx, shared_state.clone(), global_shutdown_rx.clone());
            supervisors.push(h);
        }
    }

    if args.headless {
        // No TUI — wait for Ctrl-C (or --minutes timer if set).
        let ctrl_c = tokio::signal::ctrl_c();
        if args.minutes > 0 {
            let dur = std::time::Duration::from_secs(args.minutes as u64 * 60);
            tracing::info!(
                bots = cfg.bots.len(),
                minutes = args.minutes,
                "headless mode — running until time cap or Ctrl-C"
            );
            tokio::select! {
                _ = ctrl_c => tracing::info!("Ctrl-C received"),
                _ = tokio::time::sleep(dur) => tracing::info!("time cap reached"),
            }
        } else {
            tracing::info!(
                bots = cfg.bots.len(),
                "headless mode — running until Ctrl-C"
            );
            let _ = ctrl_c.await;
            tracing::info!("Ctrl-C received");
        }
    } else {
        // Run the TUI on a dedicated OS thread, OFF the tokio runtime.
        // crossterm event-poll and ratatui draws are sync I/O — running
        // them inside a tokio task would block a worker that should be
        // servicing bot futures. The dedicated thread also gets its own
        // OS-level scheduling so render frames aren't gated on tokio
        // wakeups.
        let tui_state = shared_state.clone();
        let tui_logs = log_store.clone();
        let tui_shutdown = global_shutdown_tx.clone();
        let tui_config_path = config_path.clone();
        let tui_thread = std::thread::Builder::new()
            .name("tikr-tui".into())
            .spawn(move || tui::run(tui_state, tui_logs, tui_shutdown, tui_config_path))?;
        let _ = tokio::task::spawn_blocking(move || tui_thread.join()).await;
    }

    // Tell supervisors to wind down (the TUI thread already did this
    // on exit, but redundant signaling is harmless).
    let _ = global_shutdown_tx.send(true);

    // Give supervisors up to 6s to finish.
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(6),
        futures::future::join_all(supervisors),
    )
    .await;

    Ok(())
}
