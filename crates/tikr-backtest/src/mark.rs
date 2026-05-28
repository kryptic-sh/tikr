//! Mark-price replay — loads `mark_<BASE>_*.parquet` shards and exposes a
//! time-ordered cursor the backtest runner queries each event.
//!
//! Perp venues mark unrealized PnL, funding, and liquidation triggers against
//! a **mark / index price**, not the order-book mid. In stress the two diverge
//! (mid gaps around thin liquidity while mark tracks the index), so marking
//! against mid mis-states PnL and mis-times liquidations. When a mark series
//! is present the runner uses it; otherwise it falls back to book mid.
//!
//! Schema (matches the `record_binance` mark writer):
//!   `ts_ns` u64, `mark_price` f64
//!
//! Discovery mirrors [`crate::replay::ParquetReplay`]: files in `data_dir`
//! whose name starts with `mark_<BASE>_` and that are finished writing
//! (trailing `PAR1` magic). Cursor advances monotonically as the runner's
//! sim clock moves forward.

use std::path::Path;

use polars::prelude::*;
use tikr_core::{Decimal, Price};

/// Errors returned by mark-series construction.
#[derive(Debug, thiserror::Error)]
pub enum MarkSeriesError {
    /// IO error reading the data directory or a parquet file.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// Polars error decoding a parquet shard.
    #[error("parquet: {0}")]
    Parquet(#[from] PolarsError),
    /// Schema mismatch — a column was missing or had the wrong dtype.
    #[error("schema: {0}")]
    Schema(String),
    /// Numeric conversion (f64 → Decimal) failed for a row's mark price.
    #[error("decimal conversion: {0}")]
    Decimal(String),
}

/// One sampled mark observation: timestamp, mark price, and (optionally) the
/// funding rate recorded at that instant.
#[derive(Debug, Clone, Copy)]
struct MarkPoint {
    ts: u64,
    mark: Price,
    /// Recorded funding rate (fraction per funding interval) at this ts.
    /// `None` when the shard predates the funding-rate column — the runner
    /// then falls back to its flat configured rate.
    funding: Option<Decimal>,
}

/// Sorted in-memory mark-price (+ funding-rate) series for one symbol.
///
/// Constructed via [`MarkSeries::load`] (or [`MarkSeries::from_points`] for
/// tests); queried via [`MarkSeries::mark_at`] / [`MarkSeries::funding_at`],
/// which return the latest value whose timestamp is `<= now_ns`. Cloneable so
/// a sweep can hand a fresh (cursor-0) copy to each preset run.
#[derive(Debug, Clone)]
pub struct MarkSeries {
    /// Points sorted ascending by `ts`.
    points: Vec<MarkPoint>,
    /// Index of the next not-yet-consumed point.
    cursor: usize,
    /// Latest point whose ts was `<=` the last queried `now_ns`.
    current: Option<MarkPoint>,
}

impl MarkSeries {
    /// Load every `mark_<BASE>_*.parquet` shard in `data_dir` for `base`
    /// (e.g. `"BTC"`), sorted ascending by `ts_ns`. An empty / missing dir or
    /// zero matching shards yields an empty series — not an error.
    pub fn load(data_dir: &Path, base: &str) -> Result<Self, MarkSeriesError> {
        let mut points: Vec<MarkPoint> = Vec::new();
        if data_dir.exists() {
            let prefix = format!("mark_{base}_");
            for entry in std::fs::read_dir(data_dir)? {
                let entry = entry?;
                let fname = entry.file_name();
                let name = fname.to_string_lossy();
                if !name.starts_with(&prefix) || !name.ends_with(".parquet") {
                    continue;
                }
                let path = entry.path();
                if !crate::parquet_util::is_complete_parquet(&path) {
                    continue;
                }
                load_one(&path, &mut points)?;
            }
        }
        points.sort_by_key(|p| p.ts);
        Ok(Self {
            points,
            cursor: 0,
            current: None,
        })
    }

    /// Construct a mark-only series from `(ts_ns, mark_price)` (tests / live);
    /// funding is `None` for every point.
    pub fn from_points(points: Vec<(u64, Price)>) -> Self {
        Self::from_points_with_funding(points.into_iter().map(|(ts, m)| (ts, m, None)).collect())
    }

    /// Construct from `(ts_ns, mark_price, funding_rate)` triples (tests).
    pub fn from_points_with_funding(points: Vec<(u64, Price, Option<Decimal>)>) -> Self {
        let mut points: Vec<MarkPoint> = points
            .into_iter()
            .map(|(ts, mark, funding)| MarkPoint { ts, mark, funding })
            .collect();
        points.sort_by_key(|p| p.ts);
        Self {
            points,
            cursor: 0,
            current: None,
        }
    }

    /// Number of loaded points.
    pub fn len(&self) -> usize {
        self.points.len()
    }

    /// True iff no points were loaded.
    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }

    /// Advance the cursor to the latest point whose ts is `<= now_ns`.
    /// `now_ns` must be non-decreasing across calls (the runner's sim clock
    /// guarantees this); both [`Self::mark_at`] and [`Self::funding_at`] drive
    /// it, so calling them with the same ts is idempotent.
    fn advance(&mut self, now_ns: u64) {
        while self.cursor < self.points.len() && self.points[self.cursor].ts <= now_ns {
            self.current = Some(self.points[self.cursor]);
            self.cursor += 1;
        }
    }

    /// Latest mark price whose timestamp is `<= now_ns`, or `None` if the
    /// series hasn't reached its first point yet.
    pub fn mark_at(&mut self, now_ns: u64) -> Option<Price> {
        self.advance(now_ns);
        self.current.map(|p| p.mark)
    }

    /// Latest recorded funding rate (fraction per funding interval) whose
    /// timestamp is `<= now_ns`. `None` when no point has been reached yet or
    /// the shard carried no funding-rate column — the runner then falls back
    /// to its flat configured rate.
    pub fn funding_at(&mut self, now_ns: u64) -> Option<Decimal> {
        self.advance(now_ns);
        self.current.and_then(|p| p.funding)
    }
}

fn load_one(path: &Path, out: &mut Vec<MarkPoint>) -> Result<(), MarkSeriesError> {
    let file = std::fs::File::open(path)?;
    let df = ParquetReader::new(file).finish()?;
    let ts_ns = df
        .column("ts_ns")
        .map_err(|e| MarkSeriesError::Schema(format!("missing ts_ns in {}: {e}", path.display())))?
        .u64()
        .map_err(|e| {
            MarkSeriesError::Schema(format!("ts_ns not u64 in {}: {e}", path.display()))
        })?;
    let mark = df
        .column("mark_price")
        .map_err(|e| {
            MarkSeriesError::Schema(format!("missing mark_price in {}: {e}", path.display()))
        })?
        .f64()
        .map_err(|e| {
            MarkSeriesError::Schema(format!("mark_price not f64 in {}: {e}", path.display()))
        })?;
    // Funding rate is optional — older shards (or hand-made fixtures) may omit
    // it. When present it must be f64.
    let funding = match df.column("funding_rate") {
        Ok(col) => Some(col.f64().map_err(|e| {
            MarkSeriesError::Schema(format!("funding_rate not f64 in {}: {e}", path.display()))
        })?),
        Err(_) => None,
    };
    let n = df.height();
    for i in 0..n {
        let ts = ts_ns
            .get(i)
            .ok_or_else(|| MarkSeriesError::Schema(format!("null ts_ns at row {i}")))?;
        let m = mark
            .get(i)
            .ok_or_else(|| MarkSeriesError::Schema(format!("null mark_price at row {i}")))?;
        if m <= 0.0 {
            continue;
        }
        let m_d = Decimal::try_from(m)
            .map_err(|e| MarkSeriesError::Decimal(format!("mark {m} at row {i}: {e}")))?;
        // A null funding cell on an otherwise-present column → None for that
        // row (fall back to flat rate); a malformed value errors.
        let f_d =
            match funding.as_ref().and_then(|c| c.get(i)) {
                Some(f) => Some(Decimal::try_from(f).map_err(|e| {
                    MarkSeriesError::Decimal(format!("funding {f} at row {i}: {e}"))
                })?),
                None => None,
            };
        out.push(MarkPoint {
            ts,
            mark: Price(m_d),
            funding: f_d,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pt(ts: u64, px: i64) -> (u64, Price) {
        (ts, Price(Decimal::from(px)))
    }

    #[test]
    fn from_points_sorts_and_holds_latest() {
        let mut s = MarkSeries::from_points(vec![pt(3_000, 103), pt(1_000, 101), pt(2_000, 102)]);
        assert_eq!(s.len(), 3);
        // Before the first point — nothing yet.
        assert_eq!(s.mark_at(500), None);
        // At 1_500 the latest <= now is the 1_000 point.
        assert_eq!(s.mark_at(1_500), Some(Price(Decimal::from(101))));
        // At 2_000 it advances to the 2_000 point.
        assert_eq!(s.mark_at(2_000), Some(Price(Decimal::from(102))));
        // Past the last point — holds the final mark.
        assert_eq!(s.mark_at(9_999), Some(Price(Decimal::from(103))));
    }

    #[test]
    fn mark_at_holds_value_between_points() {
        let mut s = MarkSeries::from_points(vec![pt(1_000, 100), pt(5_000, 200)]);
        assert_eq!(s.mark_at(1_000), Some(Price(Decimal::from(100))));
        // Between points the prior mark holds.
        assert_eq!(s.mark_at(4_999), Some(Price(Decimal::from(100))));
        assert_eq!(s.mark_at(5_000), Some(Price(Decimal::from(200))));
    }

    #[test]
    fn funding_at_tracks_recorded_rate_and_mark_only_is_none() {
        let bp = |n: i64, d: u32| Decimal::new(n, d);
        let mut s = MarkSeries::from_points_with_funding(vec![
            (1_000, Price(Decimal::from(100)), Some(bp(1, 4))), // 0.0001
            (5_000, Price(Decimal::from(100)), Some(bp(2, 4))), // 0.0002
        ]);
        assert_eq!(s.funding_at(500), None, "before first point");
        assert_eq!(s.funding_at(1_000), Some(bp(1, 4)));
        assert_eq!(s.funding_at(4_999), Some(bp(1, 4)), "holds between points");
        assert_eq!(s.funding_at(5_000), Some(bp(2, 4)));

        // A mark-only series exposes no funding → runner falls back to flat.
        let mut m = MarkSeries::from_points(vec![pt(1_000, 100)]);
        assert_eq!(m.mark_at(1_000), Some(Price(Decimal::from(100))));
        assert_eq!(m.funding_at(1_000), None);
    }

    #[test]
    fn empty_dir_returns_empty_series() {
        let dir = tempfile::tempdir().unwrap();
        let s = MarkSeries::load(dir.path(), "BTC").unwrap();
        assert!(s.is_empty());
        // Loading a non-existent dir is also fine.
        let s2 = MarkSeries::load(&dir.path().join("nope"), "BTC").unwrap();
        assert!(s2.is_empty());
    }
}
