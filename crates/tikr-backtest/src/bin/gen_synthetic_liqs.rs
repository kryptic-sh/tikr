//! One-shot synthetic liquidation-parquet generator for LiqFade smoke tests.
//!
//! Reads the ts_ns range from a symbol's book parquet files, picks a moment
//! `--offset_pct` into that window, and writes a burst of synthetic forced
//! liquidations all on the same side at 100ms intervals. Output schema +
//! path layout match `record_liquidations.rs` so `LiqEventStream::load`
//! consumes it as-is.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use clap::{Parser, ValueEnum};
use polars::prelude::*;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum SideArg {
    Buy,
    Sell,
}

impl SideArg {
    fn as_str(self) -> &'static str {
        match self {
            SideArg::Buy => "BUY",
            SideArg::Sell => "SELL",
        }
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "gen_synthetic_liqs",
    about = "Write a synthetic liquidation parquet burst inside an existing book-data time window for LiqFade smoke tests"
)]
struct Args {
    /// Book-data dir for the symbol — used to derive the ts_ns range.
    #[arg(long)]
    book_dir: PathBuf,
    /// Symbol to encode in the parquet rows (e.g. BTCUSDT).
    #[arg(long)]
    symbol: String,
    /// Output dir — synthetic parquet lands at `{out_dir}/{YYYY-MM-DD}/all_symbols.parquet`.
    #[arg(long)]
    out_dir: PathBuf,
    /// Side of the burst — SELL → forces price down → LiqFade buys.
    #[arg(long, value_enum, default_value = "sell")]
    side: SideArg,
    /// How far into the book-data window to plant the FIRST burst (0.0..1.0).
    /// Subsequent bursts (when `--burst-count > 1`) are spaced evenly out
    /// to `--offset-end-pct`.
    #[arg(long, default_value_t = 0.25)]
    offset_pct: f64,
    /// How far into the window the LAST burst lands (0.0..1.0). Only used
    /// when `--burst-count > 1`. Defaults to `0.95` so the final burst
    /// stays inside the data window.
    #[arg(long, default_value_t = 0.95)]
    offset_end_pct: f64,
    /// Number of bursts to scatter across the window. `1` (default)
    /// plants a single cluster at `--offset-pct`.
    #[arg(long, default_value_t = 1)]
    burst_count: u32,
    /// Per-event notional in USDT.
    #[arg(long, default_value_t = 200_000.0)]
    event_notional_usdt: f64,
    /// Events per burst.
    #[arg(long, default_value_t = 10)]
    event_count: u32,
    /// Spacing between events in ms within a burst.
    #[arg(long, default_value_t = 100)]
    spacing_ms: u64,
    /// Reference price (USDT). If 0, defaults to 100_000 (BTC-ish).
    #[arg(long, default_value_t = 0.0)]
    price: f64,
    /// Alternate side per burst. Default plants every burst on the same
    /// side as `--side`. With this on, burst 0 = `--side`, burst 1 =
    /// opposite, etc.
    #[arg(long, default_value_t = false)]
    alternate_sides: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let (start_ns, end_ns) = scan_ts_range(&args.book_dir)?;
    let span = end_ns.saturating_sub(start_ns);
    let price = if args.price > 0.0 {
        args.price
    } else {
        100_000.0
    };
    let qty = args.event_notional_usdt / price;
    let burst_count = args.burst_count.max(1);

    // Compute per-burst start ts. Single burst → exactly at offset_pct.
    // Multiple bursts → evenly spaced from offset_pct to offset_end_pct.
    let total_events = (burst_count * args.event_count) as usize;
    let mut ts_ns: Vec<u64> = Vec::with_capacity(total_events);
    let mut symbol: Vec<String> = Vec::with_capacity(total_events);
    let mut side: Vec<String> = Vec::with_capacity(total_events);
    let mut qtys: Vec<f64> = Vec::with_capacity(total_events);
    let mut prices: Vec<f64> = Vec::with_capacity(total_events);
    let mut notionals: Vec<f64> = Vec::with_capacity(total_events);
    for b in 0..burst_count {
        let pct = if burst_count == 1 {
            args.offset_pct
        } else {
            args.offset_pct
                + (args.offset_end_pct - args.offset_pct) * (b as f64) / ((burst_count - 1) as f64)
        };
        let burst_start = start_ns + ((span as f64 * pct) as u64);
        let burst_side = if args.alternate_sides && b % 2 == 1 {
            match args.side {
                SideArg::Buy => "SELL",
                SideArg::Sell => "BUY",
            }
        } else {
            args.side.as_str()
        };
        for i in 0..args.event_count {
            let t = burst_start + (i as u64) * args.spacing_ms * 1_000_000;
            ts_ns.push(t);
            symbol.push(args.symbol.clone());
            side.push(burst_side.to_string());
            qtys.push(qty);
            prices.push(price);
            notionals.push(args.event_notional_usdt);
        }
    }

    let mut df = df!(
        "ts_ns"    => ts_ns.clone(),
        "symbol"   => symbol,
        "side"     => side,
        "qty"      => qtys,
        "price"    => prices,
        "notional" => notionals,
    )?;

    let label_ts = ts_ns.first().copied().unwrap_or(start_ns);
    let label = DateTime::<Utc>::from_timestamp(
        (label_ts / 1_000_000_000) as i64,
        (label_ts % 1_000_000_000) as u32,
    )
    .map(|d| d.format("%Y-%m-%d").to_string())
    .unwrap_or_else(|| "1970-01-01".to_string());
    let dir = args.out_dir.join(label);
    std::fs::create_dir_all(&dir)?;
    let path = dir.join("all_symbols.parquet");
    let file = std::fs::File::create(&path)?;
    ParquetWriter::new(file).finish(&mut df)?;

    println!(
        "wrote {} synthetic liqs across {} burst(s) ({} USDT/event, total {:.0} USDT) for {} → {} (ts {}..{})",
        total_events,
        burst_count,
        args.event_notional_usdt,
        args.event_notional_usdt * total_events as f64,
        args.symbol,
        path.display(),
        ts_ns.first().copied().unwrap_or(0),
        ts_ns.last().copied().unwrap_or(0),
    );
    Ok(())
}

fn scan_ts_range(dir: &Path) -> Result<(u64, u64), Box<dyn std::error::Error>> {
    let mut min_ts: Option<u64> = None;
    let mut max_ts: Option<u64> = None;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let name = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or_default();
        if !name.ends_with(".parquet") || !name.starts_with("book_") {
            continue;
        }
        let df = ParquetReader::new(std::fs::File::open(&path)?).finish()?;
        if let Ok(col) = df.column("ts_ns") {
            if let Ok(arr) = col.u64() {
                if let Some(lo) = arr.min() {
                    min_ts = Some(min_ts.map(|c| c.min(lo)).unwrap_or(lo));
                }
                if let Some(hi) = arr.max() {
                    max_ts = Some(max_ts.map(|c| c.max(hi)).unwrap_or(hi));
                }
            }
        }
    }
    match (min_ts, max_ts) {
        (Some(lo), Some(hi)) if lo < hi => Ok((lo, hi)),
        _ => Err(format!("no book_*.parquet with ts_ns range under {}", dir.display()).into()),
    }
}
