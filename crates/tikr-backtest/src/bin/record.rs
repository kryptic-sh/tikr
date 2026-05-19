//! Hyperliquid WS recorder — captures L2 + trades into per-flush parquet
//! files matching `SCHEMA.md`.
//!
//! Spec lock: per-flush files
//! (`book_<SYM>_<DATE>_<FLUSH-COUNTER>.parquet` and
//! `trades_<SYM>_<DATE>_<FLUSH-COUNTER>.parquet`); flush every 1000 rows or
//! every 60s; `--hours 0` runs until SIGINT; `--env mainnet|testnet`.
//!
//! v0 caveats are documented in `SCHEMA.md` (full snapshot dumped as deltas;
//! f64 price/size for the existing replay path; seq monotonic within a single
//! recorder process).

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use chrono::Utc;
use clap::Parser;
use futures::StreamExt;
use polars::prelude::*;
use tikr_core::{Asset, MarketEvent, Price, Side, Size, Snapshot, Symbol, Timestamp, VenueId};
use tikr_hyperliquid::{Hyperliquid, HyperliquidConfig, HyperliquidEnv};
use tikr_venue::Venue;
use tokio::signal;
use tracing::{info, warn};

/// Record a Hyperliquid market-data stream into parquet.
#[derive(Parser, Debug)]
#[command(name = "record", about = "Record Hyperliquid market data to parquet")]
struct Args {
    /// Symbol base asset (e.g. `BTC`).
    #[arg(long)]
    symbol: String,

    /// How many hours to record. `0` runs until SIGINT.
    #[arg(long, default_value_t = 1)]
    hours: u32,

    /// Output directory (created if missing).
    #[arg(long, default_value = "./data")]
    out: PathBuf,

    /// Environment: `mainnet` or `testnet`.
    #[arg(long, default_value = "mainnet")]
    env: String,
}

const FLUSH_ROW_THRESHOLD: usize = 1000;
const FLUSH_INTERVAL: Duration = Duration::from_secs(60);

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let env = match args.env.as_str() {
        "testnet" => HyperliquidEnv::Testnet,
        "mainnet" => HyperliquidEnv::Mainnet,
        other => {
            eprintln!("invalid --env: {other} (use 'mainnet' or 'testnet')");
            std::process::exit(2);
        }
    };

    if let Err(e) = std::fs::create_dir_all(&args.out) {
        eprintln!("failed to create output dir {}: {}", args.out.display(), e);
        std::process::exit(1);
    }

    let symbol = Symbol {
        base: Asset::new(&args.symbol),
        quote: Asset::new("USDC"),
        venue: VenueId::new("hyperliquid"),
    };

    let venue = Hyperliquid::with_config(HyperliquidConfig {
        env,
        heartbeat_ms: 0, // suppress heartbeats in recorder output
        ..Default::default()
    });

    let mut stream = match venue.subscribe(&symbol).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("subscribe failed: {e}");
            std::process::exit(1);
        }
    };

    let mut recorder = Recorder::new(args.symbol.clone(), args.out.clone());

    let started = Instant::now();
    let duration_cap = if args.hours == 0 {
        None
    } else {
        Some(Duration::from_secs(args.hours as u64 * 3600))
    };
    let mut progress_tick = tokio::time::interval(FLUSH_INTERVAL);
    // skip the immediate first tick that `interval` fires synchronously
    progress_tick.tick().await;

    info!(
        symbol = %args.symbol,
        hours = args.hours,
        env = %args.env,
        out = %args.out.display(),
        "recorder starting",
    );

    loop {
        if let Some(cap) = duration_cap
            && started.elapsed() >= cap
        {
            info!("duration limit reached");
            break;
        }

        tokio::select! {
            ev = stream.next() => {
                match ev {
                    Some(MarketEvent::BookUpdate { snapshot }) => recorder.record_book(&snapshot),
                    Some(MarketEvent::Trade { price, size, side, ts, .. }) => {
                        recorder.record_trade(ts, price, size, side);
                    }
                    Some(MarketEvent::Fill(_)) | Some(MarketEvent::Heartbeat { .. }) => {}
                    None => {
                        warn!("market stream ended unexpectedly");
                        break;
                    }
                }
                if recorder.row_count() >= FLUSH_ROW_THRESHOLD
                    && let Err(e) = recorder.flush()
                {
                    warn!(error = %e, "row-threshold flush failed");
                }
            }
            _ = progress_tick.tick() => {
                recorder.log_progress();
                if let Err(e) = recorder.flush_if_stale(FLUSH_INTERVAL) {
                    warn!(error = %e, "periodic flush failed");
                }
            }
            _ = signal::ctrl_c() => {
                info!("SIGINT received");
                break;
            }
        }
    }

    if let Err(e) = recorder.flush() {
        eprintln!("final flush failed: {e}");
        std::process::exit(1);
    }
    info!(
        books_total = recorder.books_written,
        trades_total = recorder.trades_written,
        flushes = recorder.flush_count,
        "recorder done",
    );
}

// ---------------------------------------------------------------------------
// Recorder
// ---------------------------------------------------------------------------

struct BookRow {
    ts_ns: u64,
    side: i64,
    price: f64,
    size: f64,
    seq: u64,
}

struct TradeRow {
    ts_ns: u64,
    price: f64,
    size: f64,
    taker_side: i64,
    trade_id: u64,
}

struct Recorder {
    symbol: String,
    out_dir: PathBuf,
    book_buf: Vec<BookRow>,
    trade_buf: Vec<TradeRow>,
    seq: u64,
    trade_id: u64,
    flush_count: u64,
    last_flush: Instant,
    books_written: u64,
    trades_written: u64,
}

impl Recorder {
    fn new(symbol: String, out_dir: PathBuf) -> Self {
        Self {
            symbol,
            out_dir,
            book_buf: Vec::new(),
            trade_buf: Vec::new(),
            seq: 0,
            trade_id: 0,
            flush_count: 0,
            last_flush: Instant::now(),
            books_written: 0,
            trades_written: 0,
        }
    }

    fn record_book(&mut self, snap: &Snapshot) {
        let ts_ns = snap.ts.0;
        for level in &snap.bids {
            self.seq += 1;
            self.book_buf.push(BookRow {
                ts_ns,
                side: 0,
                price: decimal_to_f64(level.price.0),
                size: decimal_to_f64(level.size.0),
                seq: self.seq,
            });
        }
        for level in &snap.asks {
            self.seq += 1;
            self.book_buf.push(BookRow {
                ts_ns,
                side: 1,
                price: decimal_to_f64(level.price.0),
                size: decimal_to_f64(level.size.0),
                seq: self.seq,
            });
        }
    }

    fn record_trade(&mut self, ts: Timestamp, price: Price, size: Size, taker: Side) {
        self.trade_id += 1;
        self.trade_buf.push(TradeRow {
            ts_ns: ts.0,
            price: decimal_to_f64(price.0),
            size: decimal_to_f64(size.0),
            taker_side: if taker == Side::Bid { 0 } else { 1 },
            trade_id: self.trade_id,
        });
    }

    fn row_count(&self) -> usize {
        self.book_buf.len() + self.trade_buf.len()
    }

    fn flush(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.book_buf.is_empty() && self.trade_buf.is_empty() {
            return Ok(());
        }
        let date = Utc::now().format("%Y-%m-%d").to_string();
        let counter = self.flush_count;

        if !self.book_buf.is_empty() {
            let path = self.out_dir.join(format!(
                "book_{}_{}_{:06}.parquet",
                self.symbol, date, counter
            ));
            write_book_df(&self.book_buf, &path)?;
            info!(rows = self.book_buf.len(), path = %path.display(), "wrote book parquet");
            self.books_written += self.book_buf.len() as u64;
            self.book_buf.clear();
        }
        if !self.trade_buf.is_empty() {
            let path = self.out_dir.join(format!(
                "trades_{}_{}_{:06}.parquet",
                self.symbol, date, counter
            ));
            write_trades_df(&self.trade_buf, &path)?;
            info!(rows = self.trade_buf.len(), path = %path.display(), "wrote trades parquet");
            self.trades_written += self.trade_buf.len() as u64;
            self.trade_buf.clear();
        }
        self.flush_count += 1;
        self.last_flush = Instant::now();
        Ok(())
    }

    fn flush_if_stale(&mut self, stale_after: Duration) -> Result<(), Box<dyn std::error::Error>> {
        if self.last_flush.elapsed() >= stale_after && self.row_count() > 0 {
            return self.flush();
        }
        Ok(())
    }

    fn log_progress(&self) {
        info!(
            books_buffered = self.book_buf.len(),
            trades_buffered = self.trade_buf.len(),
            books_written = self.books_written,
            trades_written = self.trades_written,
            flushes = self.flush_count,
            "progress",
        );
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert a [`tikr_core::Decimal`] to `f64`, matching the existing test
/// fixture path that [`tikr_backtest::replay::ParquetReplay`] consumes.
///
/// We round-trip through the string form so the recorded value matches the
/// human-readable decimal exactly (within `f64` precision); this is the same
/// shape the existing fixture helpers in `tests/golden.rs` produce.
fn decimal_to_f64(d: tikr_core::Decimal) -> f64 {
    d.to_string().parse::<f64>().unwrap_or(0.0)
}

fn write_book_df(rows: &[BookRow], path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let ts: Vec<u64> = rows.iter().map(|r| r.ts_ns).collect();
    let side: Vec<i64> = rows.iter().map(|r| r.side).collect();
    let price: Vec<f64> = rows.iter().map(|r| r.price).collect();
    let size: Vec<f64> = rows.iter().map(|r| r.size).collect();
    let seq: Vec<u64> = rows.iter().map(|r| r.seq).collect();
    let mut df = df!(
        "ts_ns" => ts,
        "side" => side,
        "price" => price,
        "size" => size,
        "seq" => seq,
    )?;
    let file = std::fs::File::create(path)?;
    ParquetWriter::new(file).finish(&mut df)?;
    Ok(())
}

fn write_trades_df(rows: &[TradeRow], path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let ts: Vec<u64> = rows.iter().map(|r| r.ts_ns).collect();
    let price: Vec<f64> = rows.iter().map(|r| r.price).collect();
    let size: Vec<f64> = rows.iter().map(|r| r.size).collect();
    let taker: Vec<i64> = rows.iter().map(|r| r.taker_side).collect();
    let trade_id: Vec<u64> = rows.iter().map(|r| r.trade_id).collect();
    let mut df = df!(
        "ts_ns" => ts,
        "price" => price,
        "size" => size,
        "taker_side" => taker,
        "trade_id" => trade_id,
    )?;
    let file = std::fs::File::create(path)?;
    ParquetWriter::new(file).finish(&mut df)?;
    Ok(())
}
