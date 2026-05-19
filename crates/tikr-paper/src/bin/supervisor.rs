//! Process-level supervisor for paper trading runs.
//!
//! Spawns `cargo run -p tikr-paper --example run_paper` as a child process,
//! watches it to completion, respawns on non-zero exit, and gives up once
//! `--max-restarts-per-hour` is exceeded inside a rolling 1-hour window.
//!
//! # v0 limitation: restarts without state continuity
//!
//! The example bin (`run_paper`) does not yet accept a `--resume-from
//! <snapshot>` flag, so each respawned child starts cold. State snapshots
//! continue to land in `./paper_state/` for post-mortem analysis but are not
//! re-read by the new child. Wiring `--resume-from` into the example +
//! supervisor is a follow-up issue; do not rely on continuity across
//! supervisor restarts in v0.
//!
//! # SIGINT handling
//!
//! `Command::status()` blocks waiting for the child. The supervisor itself
//! does not install a signal handler; the child's SIGINT handler ends the
//! child cleanly, the supervisor observes `success()` and exits 0. Acceptable
//! for v0.

use clap::Parser;
use std::process::Command;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

#[derive(Parser, Debug)]
#[command(
    name = "supervisor",
    about = "Auto-restart wrapper for the paper trading runner"
)]
struct Args {
    /// Symbol to trade.
    #[arg(long, default_value = "BTC")]
    symbol: String,
    /// Strategy to run.
    #[arg(long, default_value = "naive-grid")]
    strategy: String,
    /// Environment.
    #[arg(long, default_value = "mainnet")]
    env: String,
    /// State directory (also where snapshots land).
    #[arg(long, default_value = "./paper_state")]
    state_dir: std::path::PathBuf,
    /// Maximum restarts allowed within a rolling 1-hour window.
    #[arg(long, default_value_t = 5u32)]
    max_restarts_per_hour: u32,
}

fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let mut restarts: Vec<Instant> = Vec::new();

    loop {
        // Prune restarts older than 1 hour.
        let cutoff = Instant::now() - Duration::from_secs(3600);
        restarts.retain(|t| *t >= cutoff);

        if restarts.len() >= args.max_restarts_per_hour as usize {
            error!(
                count = restarts.len(),
                limit = args.max_restarts_per_hour,
                "max_restarts_per_hour exceeded; supervisor giving up"
            );
            std::process::exit(1);
        }

        info!(
            attempt = restarts.len() + 1,
            symbol = %args.symbol,
            strategy = %args.strategy,
            env = %args.env,
            state_dir = %args.state_dir.display(),
            "spawning runner"
        );

        // Build the cargo run command for the example bin.
        let mut cmd = Command::new("cargo");
        cmd.args([
            "run",
            "-p",
            "tikr-paper",
            "--example",
            "run_paper",
            "--",
            "--symbol",
            &args.symbol,
            "--strategy",
            &args.strategy,
            "--env",
            &args.env,
            "--minutes",
            "0",
        ]);

        let status = match cmd.status() {
            Ok(s) => s,
            Err(e) => {
                error!("spawn failed: {}", e);
                restarts.push(Instant::now());
                std::thread::sleep(Duration::from_secs(5));
                continue;
            }
        };

        if status.success() {
            info!("runner exited cleanly; supervisor done");
            std::process::exit(0);
        }

        warn!(?status, "runner exited non-zero; respawning");
        restarts.push(Instant::now());
        std::thread::sleep(Duration::from_secs(2));
    }
}
