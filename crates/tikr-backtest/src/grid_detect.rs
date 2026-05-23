//! Auto-detect venue tick + step size from recorded parquet data.
//!
//! Two paths, in priority:
//!
//! 1. **Static lookup** — known Binance USD-M Futures perps shipped with
//!    the binary. Saves a parquet read for the common case.
//! 2. **Parquet sniff** — read the first `book_<BASE>_*.parquet` in the
//!    data directory, take the smallest non-zero gap between consecutive
//!    sorted distinct prices, that's the tick. Step detected the same way
//!    from a `trades_*.parquet` size column when available, else falls
//!    back to tick.
//!
//! Used by `compare_strategies --tick-size auto --step-size auto` so the
//! operator doesn't have to remember per-symbol filters.

use std::path::{Path, PathBuf};

use polars::prelude::*;
use tikr_core::Decimal;

/// Errors returned by grid auto-detection.
#[derive(Debug, thiserror::Error)]
pub enum GridDetectError {
    /// IO error walking the data directory.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// Polars error decoding a parquet shard.
    #[error("parquet: {0}")]
    Parquet(#[from] PolarsError),
    /// No usable parquet rows found to sniff.
    #[error("no usable parquet rows in {0}")]
    NoData(String),
}

/// Best-guess `(tick, step)` for a Binance-style symbol given recorded
/// parquet data. Symbol matching is case-insensitive on the raw ticker
/// (e.g. `"BTCUSDT"`). When the symbol is in the hardcoded table, no
/// parquet is read.
pub fn detect_grid(
    data_dir: &Path,
    symbol: &str,
) -> Result<(Decimal, Decimal), GridDetectError> {
    if let Some((tick, step)) = static_lookup(symbol) {
        return Ok((tick, step));
    }
    sniff_from_parquet(data_dir)
}

/// Hardcoded tick/step for Binance USD-M Futures perps we've been
/// trading this session. Returns `None` for unknown symbols so the
/// caller falls through to parquet sniffing.
fn static_lookup(symbol: &str) -> Option<(Decimal, Decimal)> {
    use std::str::FromStr;
    let s = symbol.to_uppercase();
    // (tick, step) pairs. Verified via `fapi.binance.com/fapi/v1/exchangeInfo`
    // 2026-05-23.
    let pair = match s.as_str() {
        "BTCUSDT" => ("0.1", "0.001"),
        "ETHUSDT" => ("0.01", "0.001"),
        "BNBUSDT" => ("0.01", "0.01"),
        "SOLUSDT" => ("0.01", "0.01"),
        "DOGEUSDT" => ("0.00001", "1"),
        "HYPERUSDT" => ("0.00001", "1"),
        _ => return None,
    };
    Some((
        Decimal::from_str(pair.0).unwrap(),
        Decimal::from_str(pair.1).unwrap(),
    ))
}

/// Walk `data_dir` for the first `book_*.parquet`, read prices, derive
/// tick as the smallest non-zero gap. Step derived from the first
/// `trades_*.parquet` size column the same way; falls back to tick when
/// no trades parquet is present.
fn sniff_from_parquet(data_dir: &Path) -> Result<(Decimal, Decimal), GridDetectError> {
    if !data_dir.exists() {
        return Err(GridDetectError::NoData(format!(
            "data dir not found: {}",
            data_dir.display()
        )));
    }
    let mut book_path: Option<PathBuf> = None;
    let mut trade_path: Option<PathBuf> = None;
    for entry in std::fs::read_dir(data_dir)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if name.ends_with(".parquet") {
            if book_path.is_none() && name.starts_with("book_") {
                book_path = Some(path.clone());
            } else if trade_path.is_none() && name.starts_with("trades_") {
                trade_path = Some(path);
            }
        }
        if book_path.is_some() && trade_path.is_some() {
            break;
        }
    }
    let book = book_path.ok_or_else(|| {
        GridDetectError::NoData(format!(
            "no book_*.parquet in {}",
            data_dir.display()
        ))
    })?;
    let tick = sniff_min_gap(&book, "price")?;
    let step = match trade_path {
        Some(t) => sniff_min_gap(&t, "size").unwrap_or(tick),
        None => tick,
    };
    Ok((tick, step))
}

/// Read `column` from a parquet, return the smallest non-zero gap
/// between consecutive sorted distinct values. Handles both `Float64`
/// and `Decimal` dtype columns by routing through string conversion
/// when needed.
fn sniff_min_gap(path: &Path, column: &str) -> Result<Decimal, GridDetectError> {
    use std::str::FromStr;
    let file = std::fs::File::open(path)?;
    let df = ParquetReader::new(file).finish()?;
    let s = df.column(column)?;
    let values: Vec<Decimal> = match s.dtype() {
        DataType::Float64 => s
            .f64()?
            .into_iter()
            .filter_map(|opt| opt.and_then(|v| Decimal::try_from(v).ok()))
            .collect(),
        DataType::String => s
            .str()?
            .into_iter()
            .filter_map(|opt| opt.and_then(|v| Decimal::from_str(v).ok()))
            .collect(),
        _ => {
            return Err(GridDetectError::NoData(format!(
                "{column} dtype {:?} unsupported for sniff",
                s.dtype()
            )));
        }
    };
    if values.len() < 2 {
        return Err(GridDetectError::NoData(format!(
            "too few rows ({}) in {} to sniff {column}",
            values.len(),
            path.display()
        )));
    }
    let mut sorted: Vec<Decimal> = values;
    sorted.sort();
    sorted.dedup();
    let mut min_gap: Option<Decimal> = None;
    for w in sorted.windows(2) {
        let gap = w[1] - w[0];
        if gap > Decimal::ZERO && min_gap.is_none_or(|m| gap < m) {
            min_gap = Some(gap);
        }
    }
    min_gap.ok_or_else(|| GridDetectError::NoData(format!("no positive gaps in {}", path.display())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_lookup_known_symbols() {
        assert_eq!(
            static_lookup("BTCUSDT"),
            Some((
                Decimal::from_str_exact("0.1").unwrap(),
                Decimal::from_str_exact("0.001").unwrap(),
            ))
        );
        assert_eq!(
            static_lookup("dogeusdt"),
            Some((
                Decimal::from_str_exact("0.00001").unwrap(),
                Decimal::from_str_exact("1").unwrap(),
            ))
        );
        assert_eq!(static_lookup("UNKNOWNUSDT"), None);
    }
}
