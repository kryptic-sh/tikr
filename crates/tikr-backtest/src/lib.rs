//! Backtest engine for the tikr market-making engine.
//!
//! **Status: Phase 1 scaffold.** All public functions are `todo!()`. This crate
//! exists to anchor module structure for the Phase 1 design issues
//! (#9 data format, #10 Replay trait, #11 fill sim, #12 P&L accounting).
//! Real logic lands per-module via follow-up issues.
//!
//! See `README.md` for status and the optimistic fill-bias note.

#![deny(missing_docs)]

pub mod fill_sim;
pub mod grid_detect;
pub mod liq_replay;
pub mod liquidation;
pub mod mark;
pub mod parquet_util;
pub mod pnl;
pub mod replay;
pub mod runner;
