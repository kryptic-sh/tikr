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
//! # Resume + multi-symbol + supervisor (Phase 4)
//!
//! - [`runner::run_with_resume`] re-seeds aggregate P&L / counters from a
//!   prior [`PaperReport`] and can layer a [`tikr_risk::RiskGate`] between
//!   strategy and fill simulator.
//! - [`multi::run_multi`] joins N per-symbol runner futures concurrently.
//! - `supervisor` (binary) spawns the example runner and respawns on
//!   non-zero exit, bounded by `--max-restarts-per-hour`.
//!
//! # v0 limitations
//!
//! - Resume: position size is reset to zero (only aggregate P&L carries over).
//!   Operators must close all positions before restart.
//! - Supervisor: no `--resume-from` flag on the example bin yet, so each
//!   restart is cold; state snapshots are written but not re-read.
//! - No real-time TUI; no alerting wiring (that's #33).
//!
//! [`Replay`]: tikr_backtest::replay::Replay
//! [`Venue`]: tikr_venue::Venue

#![deny(missing_docs)]

pub mod alerts;
pub mod bagger;
pub mod live;
pub mod metrics;
pub mod multi;
pub mod probe;
pub mod report;
pub mod runner;
pub mod spawn;
pub mod state;

pub use alerts::{
    Alert, AlertError, AlertSink, MultiSink, Severity, StdoutSink, WebhookFormat, WebhookSink,
};
pub use live::LiveSnapshot;
pub use metrics::MetricRegistry;
pub use multi::{MultiPaperReport, MultiSymbolRun, run_multi};
pub use report::{PaperReport, SCHEMA_VERSION};
pub use runner::{
    FundingConfig, InventoryBoostConfig, RunnerConfig, SkimConfig, run, run_with_resume,
};
pub use spawn::{BotHandle, BotSpec, StrategyChoice, live_runner_config, spawn_bot};
pub use state::write_snapshot;
