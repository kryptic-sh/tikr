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

mod build;
mod config;
mod logs;
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

    /// Override [account].margin_multiplier for computed order notional.
    #[arg(long)]
    margin_multiplier: Option<Decimal>,
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
    env: tikr_binance::BinanceEnv,
    api_key: String,
    key_material: Arc<tikr_binance::BinanceKeyMaterial>,
    symbols: Vec<String>,
    order_balance_pct: Decimal,
    margin_multiplier: Decimal,
    shutdown: watch::Receiver<bool>,
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
        loop {
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
                    tracing::info!(
                        wallet = %balance.wallet_balance,
                        available = %balance.available_balance,
                        api_unrealized = %balance.cross_unrealized_pnl,
                        "account balance poll"
                    );
                    cfg.shared_state.set_api_account(ApiAccountSnapshot {
                        asset: "USDT".to_string(),
                        wallet_balance: balance.wallet_balance,
                        available_balance: balance.available_balance,
                        cross_unrealized_pnl: balance.cross_unrealized_pnl,
                        fetched_at_ms: current_time_ms(),
                    });
                    let bot_count = Decimal::from(cfg.symbols.len().max(1) as u64);
                    let notional =
                        balance.wallet_balance * cfg.margin_multiplier * cfg.order_balance_pct
                            / Decimal::from(100)
                            / bot_count;
                    if notional != *cfg.notional_tx.borrow() {
                        let _ = cfg.notional_tx.send(notional);
                    }
                }
                Err(e) => tracing::warn!(error = ?e, "account balance poll failed"),
            }

            for symbol in &cfg.symbols {
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
    if let Some(multiplier) = args.margin_multiplier {
        cfg.account.margin_multiplier = multiplier;
    }
    if cfg.account.order_balance_pct <= Decimal::ZERO {
        anyhow::bail!("order_balance_pct must be positive");
    }
    if cfg.account.margin_multiplier <= Decimal::ZERO {
        anyhow::bail!("margin_multiplier must be positive");
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

    // Pre-seed BotViews so the TUI has tabs to show from frame 1.
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

    // Global shutdown channel — TUI flips it on `q`; supervisors observe.
    let (global_shutdown_tx, global_shutdown_rx) = watch::channel(false);
    let (notional_tx, notional_rx) = watch::channel(Decimal::ZERO);

    spawn_account_balance_poller(AccountPollerConfig {
        shared_state: shared_state.clone(),
        notional_tx,
        env,
        api_key: api_key.clone(),
        key_material: key_material.clone(),
        symbols: cfg.bots.iter().map(|b| b.symbol.clone()).collect(),
        order_balance_pct: cfg.account.order_balance_pct,
        margin_multiplier: cfg.account.margin_multiplier,
        shutdown: global_shutdown_rx.clone(),
    });

    // Spawn one supervisor per bot.
    let mut supervisors = Vec::with_capacity(cfg.bots.len());
    for b in &cfg.bots {
        let ctx = SupervisorCtx {
            cfg: b.clone(),
            env,
            api_key: api_key.clone(),
            key_material: key_material.clone(),
            base_state_dir: cfg.account.state_dir.clone(),
            order_balance_pct: cfg.account.order_balance_pct,
            margin_multiplier: cfg.account.margin_multiplier,
            bot_count: cfg.bots.len(),
            notional_rx: notional_rx.clone(),
        };
        let h = spawn_supervisor(ctx, shared_state.clone(), global_shutdown_rx.clone());
        supervisors.push(h);
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
