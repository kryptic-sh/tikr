//! Binance USD-M Futures liquidation recorder — writes all-symbol parquet.
//!
//! Connects to `wss://fstream.binance.com/ws/!forceOrder@arr` (mainnet only)
//! and records every [`LiquidationEvent`] into parquet files partitioned by
//! UTC date under `{out_dir}/{YYYY-MM-DD}/all_symbols.parquet`.
//!
//! Because liquidations arrive in bursts, the recorder writes in batches rather
//! than per-event. Two thresholds control flushing:
//! - `FLUSH_EVENT_THRESHOLD` — flush when the buffer reaches this many events.
//! - `FLUSH_INTERVAL` — flush every N seconds regardless of buffer size.
//!
//! ## CLI
//!
//! ```text
//! record_liquidations \
//!   --out-dir ./data/liquidations \
//!   --hours 24 \
//!   --env futures-mainnet
//! ```
//!
//! `--hours 0` runs until SIGINT.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use clap::{Parser, ValueEnum};
use futures::StreamExt;
use polars::prelude::*;
use tikr_binance::{BinanceEnv, liquidation_stream};
use tikr_core::Side;
use tokio::signal;
use tracing::{info, warn};

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, ValueEnum)]
enum EnvArg {
    #[value(name = "futures-mainnet")]
    FuturesMainnet,
}

/// Record Binance USD-M Futures forced liquidations to per-day parquet.
#[derive(Parser, Debug)]
#[command(
    name = "record_liquidations",
    about = "Record Binance USD-M Futures forced liquidations to per-day parquet (mainnet only)"
)]
struct Args {
    /// Binance environment. Only `futures-mainnet` is valid for liquidation data.
    #[arg(long, value_enum, default_value = "futures-mainnet")]
    env: EnvArg,

    /// Root directory for output parquet files.
    /// Files land at `{out_dir}/{YYYY-MM-DD}/all_symbols.parquet`.
    #[arg(long, default_value = "./data/liquidations")]
    out_dir: PathBuf,

    /// How many hours to record. `0` runs until SIGINT.
    #[arg(long, default_value_t = 24u64)]
    hours: u64,
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Flush the buffer when it reaches this many events.
const FLUSH_EVENT_THRESHOLD: usize = 1000;

/// Flush the buffer at least every this many seconds even if the threshold
/// has not been reached.
const FLUSH_INTERVAL: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Row types
// ---------------------------------------------------------------------------

struct LiquidationRow {
    ts_ns: u64,
    symbol: String,
    side: String,
    qty: f64,
    price: f64,
    notional: f64,
}

// ---------------------------------------------------------------------------
// Per-day recorder
// ---------------------------------------------------------------------------

/// Buffers rows and flushes to parquet when triggered.
///
/// Output path: `{out_dir}/{YYYY-MM-DD}/all_symbols.parquet`.
/// The file is **overwritten** on each flush so the consumer always sees the
/// complete day's data; a full rewrite is safe at liquidation rates.
struct DayRecorder {
    out_dir: PathBuf,
    /// Current UTC date label (YYYY-MM-DD). When the date rolls over we
    /// start a new file.
    current_date: String,
    buf: Vec<LiquidationRow>,
    last_flush: Instant,
    events_written: u64,
    flush_count: u64,
}

impl DayRecorder {
    fn new(out_dir: PathBuf) -> Self {
        let current_date = utc_date_label(Utc::now());
        Self {
            out_dir,
            current_date,
            buf: Vec::new(),
            last_flush: Instant::now(),
            events_written: 0,
            flush_count: 0,
        }
    }

    fn push(&mut self, row: LiquidationRow) {
        // Detect date rollover — flush what we have for the old date before
        // switching so the previous file is complete.
        let now_label = utc_date_label(Utc::now());
        if now_label != self.current_date {
            if let Err(e) = self.flush_inner(&self.current_date.clone()) {
                warn!(error = %e, "day-rollover flush failed");
            }
            self.current_date = now_label;
        }
        self.buf.push(row);
    }

    fn row_count(&self) -> usize {
        self.buf.len()
    }

    fn flush_if_stale(&mut self) -> Result<(), String> {
        if self.last_flush.elapsed() < FLUSH_INTERVAL {
            return Ok(());
        }
        self.flush()
    }

    fn flush(&mut self) -> Result<(), String> {
        let date = self.current_date.clone();
        self.flush_inner(&date)
    }

    fn flush_inner(&mut self, date: &str) -> Result<(), String> {
        if self.buf.is_empty() {
            self.last_flush = Instant::now();
            return Ok(());
        }
        let dir = self.out_dir.join(date);
        std::fs::create_dir_all(&dir)
            .map_err(|e| format!("create_dir_all {}: {e}", dir.display()))?;
        let path = dir.join("all_symbols.parquet");
        write_liquidation_parquet(&path, &self.buf)
            .map_err(|e| format!("parquet write {}: {e}", path.display()))?;
        self.events_written += self.buf.len() as u64;
        self.flush_count += 1;
        self.buf.clear();
        self.last_flush = Instant::now();
        info!(
            date,
            path = %path.display(),
            events_written = self.events_written,
            flushes = self.flush_count,
            "liquidation flush OK",
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    // Install ring crypto provider so rustls doesn't panic on TLS handshake.
    let _ = rustls::crypto::ring::default_provider().install_default();

    // Default log level: INFO in release (so per-flush + lifecycle lines are
    // visible without forcing RUST_LOG), DEBUG in debug. RUST_LOG overrides
    // when set (e.g. `RUST_LOG=warn` to quiet it).
    let default_level = if cfg!(debug_assertions) {
        "debug"
    } else {
        "info"
    };
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_level));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let args = Args::parse();

    // The only supported env is futures-mainnet; the EnvArg enum enforces this.
    let env = BinanceEnv::FuturesMainnet;

    info!(
        env = ?env,
        out_dir = %args.out_dir.display(),
        hours = args.hours,
        "liquidation recorder starting",
    );

    let mut stream = match liquidation_stream::subscribe_liquidations(env).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("failed to subscribe to liquidations: {e}");
            std::process::exit(1);
        }
    };

    let mut recorder = DayRecorder::new(args.out_dir.clone());
    let started = Instant::now();
    let duration_cap = if args.hours == 0 {
        None
    } else {
        Some(Duration::from_secs(args.hours * 3600))
    };

    // Throttle flush checks to every 5 seconds.
    let mut flush_tick = tokio::time::interval(FLUSH_INTERVAL);
    flush_tick.tick().await; // burn the immediate first tick

    // Per-symbol event counter for progress logs.
    let mut symbol_counts: HashMap<String, u64> = HashMap::new();

    loop {
        if let Some(cap) = duration_cap
            && started.elapsed() >= cap
        {
            info!("duration limit reached; shutting down");
            break;
        }

        tokio::select! {
            ev = stream.next() => match ev {
                Some(liq) => {
                    use rust_decimal::prelude::ToPrimitive;
                    let side_str = match liq.side {
                        Side::Ask => "SELL",
                        Side::Bid => "BUY",
                    };
                    *symbol_counts.entry(liq.symbol.clone()).or_insert(0) += 1;
                    recorder.push(LiquidationRow {
                        ts_ns: liq.ts.0,
                        symbol: liq.symbol,
                        side: side_str.to_string(),
                        qty: liq.qty.to_f64().unwrap_or(0.0),
                        price: liq.price.0.to_f64().unwrap_or(0.0),
                        notional: liq.notional.to_f64().unwrap_or(0.0),
                    });
                    if recorder.row_count() >= FLUSH_EVENT_THRESHOLD
                        && let Err(e) = recorder.flush()
                    {
                        warn!(error = %e, "threshold flush failed");
                    }
                }
                None => {
                    warn!("liquidation stream ended unexpectedly");
                    break;
                }
            },
            _ = flush_tick.tick() => {
                if let Err(e) = recorder.flush_if_stale() {
                    warn!(error = %e, "periodic flush failed");
                }
                // Progress log.
                let total: u64 = symbol_counts.values().sum();
                info!(
                    total_events = total,
                    unique_symbols = symbol_counts.len(),
                    pending = recorder.row_count(),
                    "liquidation recorder progress",
                );
            }
            _ = signal::ctrl_c() => {
                info!("SIGINT received — flushing and exiting");
                break;
            }
        }
    }

    // Final flush.
    if let Err(e) = recorder.flush() {
        warn!(error = %e, "final flush failed");
    }

    let total: u64 = symbol_counts.values().sum();
    info!(
        total_events = total,
        unique_symbols = symbol_counts.len(),
        flushes = recorder.flush_count,
        "liquidation recorder done",
    );
}

// ---------------------------------------------------------------------------
// Parquet writer
// ---------------------------------------------------------------------------

fn write_liquidation_parquet(path: &Path, rows: &[LiquidationRow]) -> PolarsResult<()> {
    let ts_ns: Vec<u64> = rows.iter().map(|r| r.ts_ns).collect();
    let symbol: Vec<&str> = rows.iter().map(|r| r.symbol.as_str()).collect();
    let side: Vec<&str> = rows.iter().map(|r| r.side.as_str()).collect();
    let qty: Vec<f64> = rows.iter().map(|r| r.qty).collect();
    let price: Vec<f64> = rows.iter().map(|r| r.price).collect();
    let notional: Vec<f64> = rows.iter().map(|r| r.notional).collect();

    let mut df = df!(
        "ts_ns"    => ts_ns,
        "symbol"   => symbol,
        "side"     => side,
        "qty"      => qty,
        "price"    => price,
        "notional" => notional,
    )?;
    let file = std::fs::File::create(path).map_err(PolarsError::from)?;
    ParquetWriter::new(file).finish(&mut df)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn utc_date_label(dt: DateTime<Utc>) -> String {
    dt.format("%Y-%m-%d").to_string()
}
