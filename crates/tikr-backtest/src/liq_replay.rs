//! Liquidation-event replay — loads `record_liquidations` parquet shards
//! and exposes them as a time-ordered cursor for the backtest runner.
//!
//! Schema (matches `record_liquidations.rs`):
//!   `ts_ns` u64, `symbol` str, `side` str ("BUY"|"SELL"),
//!   `qty` f64, `price` f64, `notional` f64
//!
//! The recorder shards by UTC date under `out_dir/YYYY-MM-DD/all_symbols.parquet`.
//! This loader walks `data_dir` recursively, accepts any `*.parquet` whose
//! schema matches, and concatenates into one sorted vector. Per-symbol
//! filtering happens at load time so the runner-side cursor only sees the
//! events its bot cares about.
//!
//! Runtime API mirrors the `Replay` trait shape so the runner pumps it the
//! same way as the book/trade timeline:
//!
//! ```ignore
//! let mut s = LiqEventStream::load(dir, "BTCUSDT")?;
//! while let Some(ev) = s.advance_to(now_ns) {
//!     runner.push_liq(ev);
//! }
//! ```

use std::path::{Path, PathBuf};

use polars::prelude::*;
use tikr_core::Decimal;
use tikr_core::{LiqEvent, Notional, Price, Side, Size, Timestamp};

/// Errors returned by liquidation-replay construction or iteration.
#[derive(Debug, thiserror::Error)]
pub enum LiqReplayError {
    /// IO error walking the data directory or opening a parquet file.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// Polars error decoding a parquet shard.
    #[error("parquet: {0}")]
    Parquet(#[from] PolarsError),
    /// Schema mismatch — a column was missing or had the wrong dtype.
    #[error("schema: {0}")]
    Schema(String),
    /// Numeric conversion (f64 → Decimal) failed for a row's qty/price.
    #[error("decimal conversion: {0}")]
    Decimal(String),
}

/// Sorted in-memory queue of [`LiqEvent`]s for one symbol.
///
/// Constructed via [`LiqEventStream::load`]; consumed via
/// [`LiqEventStream::advance_to`] which drains all events whose timestamp
/// is `<= now_ns`.
pub struct LiqEventStream {
    /// Events sorted ascending by `ts_ns`. `cursor` advances monotonically.
    events: Vec<LiqEvent>,
    cursor: usize,
}

impl LiqEventStream {
    /// Load every `*.parquet` under `data_dir` (recursively), keeping only
    /// rows whose `symbol` matches `symbol_filter` (case-sensitive,
    /// exact). Sort by `ts_ns` ascending. Empty directory or zero matching
    /// rows returns an empty stream — not an error.
    ///
    /// `symbol_filter = ""` accepts all symbols (multi-symbol stream).
    pub fn load(data_dir: &Path, symbol_filter: &str) -> Result<Self, LiqReplayError> {
        let mut events: Vec<LiqEvent> = Vec::new();
        let paths = collect_parquets(data_dir)?;
        for path in paths {
            load_one(&path, symbol_filter, &mut events)?;
        }
        events.sort_by_key(|e| e.ts.0);
        Ok(Self { events, cursor: 0 })
    }

    /// Construct a stream from an in-memory event vector. Useful for tests
    /// and for the live path where events arrive via a channel instead of
    /// parquet.
    pub fn from_events(mut events: Vec<LiqEvent>) -> Self {
        events.sort_by_key(|e| e.ts.0);
        Self { events, cursor: 0 }
    }

    /// Total event count (post-filter, post-sort).
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// True iff no events were loaded.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Drain and return every event whose `ts <= now_ns`. Caller pushes
    /// the returned slice into the runner's rolling buffer.
    ///
    /// Cursor advances monotonically — once an event is returned it
    /// won't be returned again. Returns an empty slice when no fresh
    /// events are ready.
    pub fn advance_to(&mut self, now_ns: u64) -> &[LiqEvent] {
        let start = self.cursor;
        while self.cursor < self.events.len() && self.events[self.cursor].ts.0 <= now_ns {
            self.cursor += 1;
        }
        &self.events[start..self.cursor]
    }

    /// Borrow the full sorted event vector. Used by the backtest path
    /// to pre-load the runner's liq channel with every known event in
    /// one go — the runner timestamp-filters them on observe.
    pub fn events(&self) -> &[LiqEvent] {
        &self.events
    }

    /// Consume the stream and return the inner sorted Vec — cheaper
    /// than cloning when the caller owns the stream and just needs the
    /// data to push into an mpsc channel.
    pub fn into_events(self) -> Vec<LiqEvent> {
        self.events
    }
}

/// Walk `dir` recursively and return every `*.parquet` path.
fn collect_parquets(dir: &Path) -> Result<Vec<PathBuf>, LiqReplayError> {
    let mut out = Vec::new();
    if !dir.exists() {
        return Ok(out);
    }
    let mut stack: Vec<PathBuf> = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|s| s.to_str()) == Some("parquet") {
                out.push(path);
            }
        }
    }
    out.sort();
    Ok(out)
}

/// Load one parquet shard, append matching rows to `out`.
fn load_one(
    path: &Path,
    symbol_filter: &str,
    out: &mut Vec<LiqEvent>,
) -> Result<(), LiqReplayError> {
    let file = std::fs::File::open(path)?;
    let df = ParquetReader::new(file).finish()?;
    let ts_ns = df
        .column("ts_ns")
        .map_err(|e| LiqReplayError::Schema(format!("missing ts_ns in {}: {e}", path.display())))?
        .u64()
        .map_err(|e| LiqReplayError::Schema(format!("ts_ns not u64 in {}: {e}", path.display())))?;
    let symbol = df
        .column("symbol")
        .map_err(|e| LiqReplayError::Schema(format!("missing symbol in {}: {e}", path.display())))?
        .str()
        .map_err(|e| {
            LiqReplayError::Schema(format!("symbol not str in {}: {e}", path.display()))
        })?;
    let side = df
        .column("side")
        .map_err(|e| LiqReplayError::Schema(format!("missing side in {}: {e}", path.display())))?
        .str()
        .map_err(|e| LiqReplayError::Schema(format!("side not str in {}: {e}", path.display())))?;
    let qty = df
        .column("qty")
        .map_err(|e| LiqReplayError::Schema(format!("missing qty in {}: {e}", path.display())))?
        .f64()
        .map_err(|e| LiqReplayError::Schema(format!("qty not f64 in {}: {e}", path.display())))?;
    let price = df
        .column("price")
        .map_err(|e| LiqReplayError::Schema(format!("missing price in {}: {e}", path.display())))?
        .f64()
        .map_err(|e| LiqReplayError::Schema(format!("price not f64 in {}: {e}", path.display())))?;
    let notional = df
        .column("notional")
        .map_err(|e| {
            LiqReplayError::Schema(format!("missing notional in {}: {e}", path.display()))
        })?
        .f64()
        .map_err(|e| {
            LiqReplayError::Schema(format!("notional not f64 in {}: {e}", path.display()))
        })?;

    let n = df.height();
    for i in 0..n {
        let sym = symbol
            .get(i)
            .ok_or_else(|| LiqReplayError::Schema(format!("null symbol at row {i}")))?;
        if !symbol_filter.is_empty() && sym != symbol_filter {
            continue;
        }
        let ts = ts_ns
            .get(i)
            .ok_or_else(|| LiqReplayError::Schema(format!("null ts_ns at row {i}")))?;
        let side_str = side
            .get(i)
            .ok_or_else(|| LiqReplayError::Schema(format!("null side at row {i}")))?;
        let side = match side_str {
            // Binance reports the side of the FORCED ORDER (i.e. the
            // hedge the liquidation engine submitted, NOT the position
            // side). A liquidated long → forced sell. A liquidated
            // short → forced buy. Strategies treat `side` as the side
            // that crossed on the book.
            "BUY" => Side::Bid,
            "SELL" => Side::Ask,
            other => {
                return Err(LiqReplayError::Schema(format!(
                    "unknown side '{other}' at row {i}"
                )));
            }
        };
        let q = qty
            .get(i)
            .ok_or_else(|| LiqReplayError::Schema(format!("null qty at row {i}")))?;
        let p = price
            .get(i)
            .ok_or_else(|| LiqReplayError::Schema(format!("null price at row {i}")))?;
        let n_ = notional
            .get(i)
            .ok_or_else(|| LiqReplayError::Schema(format!("null notional at row {i}")))?;

        let qty_d = Decimal::try_from(q)
            .map_err(|e| LiqReplayError::Decimal(format!("qty {q} at row {i}: {e}")))?;
        let price_d = Decimal::try_from(p)
            .map_err(|e| LiqReplayError::Decimal(format!("price {p} at row {i}: {e}")))?;
        let notional_d = Decimal::try_from(n_)
            .map_err(|e| LiqReplayError::Decimal(format!("notional {n_} at row {i}: {e}")))?;

        out.push(LiqEvent {
            ts: Timestamp(ts),
            side,
            qty: Size(qty_d),
            price: Price(price_d),
            notional: Notional(notional_d),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::{Notional, Price, Side, Size, Timestamp};

    fn ev(ts: u64, side: Side, n: u64) -> LiqEvent {
        LiqEvent {
            ts: Timestamp(ts),
            side,
            qty: Size(Decimal::ONE),
            price: Price(Decimal::from(100_000)),
            notional: Notional(Decimal::from(n)),
        }
    }

    #[test]
    fn from_events_sorts_and_advances_monotonically() {
        let mut s = LiqEventStream::from_events(vec![
            ev(3_000_000_000, Side::Ask, 500_000),
            ev(1_000_000_000, Side::Bid, 200_000),
            ev(2_000_000_000, Side::Ask, 300_000),
        ]);
        assert_eq!(s.len(), 3);
        // advance_to picks up the two events <= 2s.
        let batch1 = s.advance_to(2_000_000_000).len();
        assert_eq!(batch1, 2);
        // advance_to 1s later — no new events (cursor doesn't rewind).
        let batch2 = s.advance_to(2_500_000_000).len();
        assert_eq!(batch2, 0);
        // advance_to past last event — drains the rest.
        let batch3 = s.advance_to(4_000_000_000).len();
        assert_eq!(batch3, 1);
    }

    #[test]
    fn empty_dir_returns_empty_stream() {
        let dir = tempfile::tempdir().unwrap();
        let s = LiqEventStream::load(dir.path(), "BTCUSDT").unwrap();
        assert!(s.is_empty());
    }
}
