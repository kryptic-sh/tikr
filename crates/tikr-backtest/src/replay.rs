//! Replay engine — produces a deterministic [`MarketEvent`] stream from
//! parquet-backed historical data. See [issue #10] for the full design.
//!
//! [issue #10]: https://github.com/kryptic-sh/tikr/issues/10

use async_trait::async_trait;
use thiserror::Error;
use tikr_core::MarketEvent;

// Re-exports to anchor crate intent for the Phase 1 scaffold; real impls
// in the modules below will consume these directly (see #9/#10/#15).
#[doc(hidden)]
pub use futures as _futures;
#[doc(hidden)]
pub use polars as _polars;

/// Forward iterator over historical market events. Sim time advances per event.
#[async_trait]
pub trait Replay: Send {
    /// Pull the next event from the replay stream. `None` signals end-of-data.
    async fn next(&mut self) -> Option<MarketEvent>;
}

/// Configuration for [`ParquetReplay`].
#[derive(Debug, Clone)]
pub struct ReplayConfig {
    /// Heartbeat synthesis cadence, in milliseconds of sim time.
    /// Injected during quiet stretches to let time-driven strategies tick.
    pub heartbeat_ms: u64,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self { heartbeat_ms: 1000 }
    }
}

/// Parquet-backed [`Replay`] implementation. Phase 1 stub.
pub struct ParquetReplay {
    _cfg: ReplayConfig,
}

impl ParquetReplay {
    /// Construct a new parquet replay from `cfg`. Real impl in #10.
    pub fn new(cfg: ReplayConfig) -> Result<Self, ReplayError> {
        let _ = cfg;
        todo!("issue #10: open parquet, build iterator, validate seq monotonicity")
    }
}

#[async_trait]
impl Replay for ParquetReplay {
    async fn next(&mut self) -> Option<MarketEvent> {
        todo!("issue #10: merge book + trades streams, inject heartbeats")
    }
}

/// Errors returned by replay construction or iteration.
#[derive(Error, Debug)]
pub enum ReplayError {
    /// I/O failure reading the parquet file.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// Gap detected in the `seq` column (book stream).
    #[error("seq gap at ts {ts_ns}: expected {expected}, got {got}")]
    SeqGap {
        /// Timestamp of the gap.
        ts_ns: u64,
        /// Expected next seq.
        expected: u64,
        /// Actual seq observed.
        got: u64,
    },
    /// Schema mismatch (missing required column, wrong type).
    #[error("schema: {0}")]
    Schema(String),
}
