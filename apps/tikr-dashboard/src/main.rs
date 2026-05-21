//! tikr-dashboard — multi-bot live trading TUI.
//!
//! ```bash
//! tikr-dashboard --config ./tikr.toml          # launch dashboard
//! tikr-dashboard --config ./tikr.toml --check  # validate config only
//! ```

mod build;
mod config;
mod logs;
mod state;
mod supervisor;
mod tui;
mod venue;

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tokio::sync::watch;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::logs::LogStore;
use crate::state::{BotStatus, BotView, SharedBotState};
use crate::supervisor::{SupervisorCtx, spawn_supervisor};

#[derive(Parser, Debug)]
#[command(
    name = "tikr-dashboard",
    about = "Multi-bot live trading dashboard for tikr"
)]
struct Args {
    /// Path to the dashboard config TOML.
    #[arg(long, default_value = "tikr.toml")]
    config: PathBuf,

    /// Validate the config and exit without spawning bots.
    #[arg(long)]
    check: bool,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let _ = dotenvy::dotenv();

    let args = Args::parse();
    let cfg = config::load(&args.config)?;

    if args.check {
        println!("config OK: {} bots configured", cfg.bots.len());
        for b in &cfg.bots {
            println!("  - {} ({})", b.symbol, b.strategy);
        }
        return Ok(());
    }

    // Set up the per-bot log capture BEFORE any tracing macros fire.
    let log_store = LogStore::new();
    let log_layer = crate::logs::LogLayer::new(log_store.clone());
    // Default: capture INFO+ from every tikr-* crate so the bot's log
    // pane shows the same stream a manual `run_perp` invocation would.
    // Operators can override via `RUST_LOG=...` for noisier debugging.
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(
            "info,\
             tikr_dashboard=info,\
             tikr_paper=info,\
             tikr_binance=info,\
             tikr_strategy=info,\
             tikr_backtest=info,\
             tikr_venue=info,\
             tikr_risk=info",
        )
    });
    tracing_subscriber::registry()
        .with(env_filter)
        .with(log_layer)
        .init();

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
        };
        shared_state.insert(&b.symbol, view);
    }

    // Global shutdown channel — TUI flips it on `q`; supervisors observe.
    let (global_shutdown_tx, global_shutdown_rx) = watch::channel(false);

    // Spawn one supervisor per bot.
    let mut supervisors = Vec::with_capacity(cfg.bots.len());
    for b in &cfg.bots {
        let ctx = SupervisorCtx {
            cfg: b.clone(),
            env,
            api_key: api_key.clone(),
            key_material: key_material.clone(),
            base_state_dir: cfg.account.state_dir.clone(),
        };
        let h = spawn_supervisor(ctx, shared_state.clone(), global_shutdown_rx.clone());
        supervisors.push(h);
    }

    // Run the TUI on a dedicated OS thread, OFF the tokio runtime.
    // crossterm event-poll and ratatui draws are sync I/O — running
    // them inside a tokio task would block a worker that should be
    // servicing bot futures. The dedicated thread also gets its own
    // OS-level scheduling so render frames aren't gated on tokio
    // wakeups.
    let tui_state = shared_state.clone();
    let tui_logs = log_store.clone();
    let tui_shutdown = global_shutdown_tx.clone();
    let tui_thread = std::thread::Builder::new()
        .name("tikr-dashboard-tui".into())
        .spawn(move || tui::run(tui_state, tui_logs, tui_shutdown))?;

    // Wait for the TUI thread to exit. Joining off a blocking task so
    // the tokio runtime stays free for the supervisors.
    let _ = tokio::task::spawn_blocking(move || tui_thread.join()).await;

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
