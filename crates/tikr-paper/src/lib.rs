//! Paper-trading runner for the tikr market-making engine.
//!
//! Drives a live [`tikr_venue::Venue`] stream through [`tikr_strategy::Strategy`],
//! [`tikr_backtest::fill_sim::FillSim`], and [`tikr_backtest::pnl::PositionTracker`]
//! to simulate fills against real market activity. No real orders are sent —
//! that's Phase 5.
//!
//! # Live vs backtest
//!
//! Mirrors the shape of [`tikr_backtest::runner::run`] but swaps [`Replay`]
//! for [`Venue`] (real-time push stream) and adds cooperative shutdown via a
//! [`tokio::sync::watch`] channel. State snapshots written periodically as
//! JSON for post-mortem analysis.
//!
//! # v0 limitations (Phase 4 risk engine fills these)
//!
//! - No resume from snapshot on restart
//! - No crash recovery / auto-restart on panic
//! - Single-symbol per run (multi-symbol = spawn N runners)
//! - No alerting (Slack/Discord webhooks)
//! - No real-time TUI
//!
//! [`Replay`]: tikr_backtest::replay::Replay
//! [`Venue`]: tikr_venue::Venue

#![deny(missing_docs)]

pub mod report;
pub mod runner;
pub mod state;

pub use report::PaperReport;
pub use runner::{RunnerConfig, run};
pub use state::write_snapshot;
