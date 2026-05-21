//! tikr-dashboard — multi-bot live trading TUI.
//!
//! Phase: config + spawn scaffolding (TUI lands in M3).
//!
//! ```bash
//! tikr-dashboard --config ./tikr.toml
//! ```

mod config;

use std::path::PathBuf;

use clap::Parser;

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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let cfg = config::load(&args.config)?;

    if args.check {
        println!("config OK: {} bots configured", cfg.bots.len());
        for b in &cfg.bots {
            println!("  - {} ({})", b.symbol, b.strategy);
        }
        return Ok(());
    }

    eprintln!("tikr-dashboard scaffold ready — TUI lands in M3.");
    eprintln!(
        "Loaded {} bots from {}",
        cfg.bots.len(),
        args.config.display()
    );
    Ok(())
}
