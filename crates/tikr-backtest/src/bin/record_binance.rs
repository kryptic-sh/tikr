//! Binance WS recorder — captures depth + aggTrade into per-flush parquet
//! files matching SCHEMA.md (same shape as the Hyperliquid recorder).
//!
//! Multi-symbol mode: pass `--symbols BTCUSDT,ETHUSDT,BNBUSDT`. Each symbol
//! gets its own recorder task writing to `{base_dir}/{label}/{symbol}/`. A
//! single SIGINT cleanly stops all tasks (each flushes its remaining buffer
//! before exiting).

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use chrono::Utc;
use clap::{Parser, ValueEnum};
use futures::StreamExt;
use polars::prelude::*;
use tikr_binance::{BinanceEnv, depth_stream, trade_stream};
use tikr_core::{
    Asset, MarketEvent, MarketKind, Price, Side, Size, Snapshot, Symbol, Timestamp, VenueId,
};
use tokio::signal;
use tokio::sync::broadcast;
use tracing::{info, warn};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum EnvArg {
    #[value(name = "spot-testnet")]
    SpotTestnet,
    #[value(name = "spot-mainnet")]
    SpotMainnet,
    #[value(name = "futures-testnet")]
    FuturesTestnet,
    #[value(name = "futures-mainnet")]
    FuturesMainnet,
}

/// Record one or more Binance market-data streams into parquet.
#[derive(Parser, Debug)]
#[command(
    name = "record_binance",
    about = "Record Binance market data (depth + aggTrade) to parquet — multi-symbol capable"
)]
struct Args {
    /// Binance environment.
    #[arg(long, value_enum, default_value = "futures-mainnet")]
    env: EnvArg,

    /// Comma-separated Binance symbols (e.g. `BTCUSDT,ETHUSDT,BNBUSDT`).
    /// Each symbol gets its own recorder task + output subdirectory. Base
    /// + quote inferred via 4-char suffix heuristic (USDT/USDC/BUSD/TUSD).
    #[arg(long, default_value = "BTCUSDT", value_delimiter = ',')]
    symbols: Vec<String>,

    /// How many hours to record. `0` runs until SIGINT.
    #[arg(long, default_value_t = 1u32)]
    hours: u32,

    /// Base output directory. Per-symbol parquets land at
    /// `{base_dir}/{label}/{symbol}/`. The `{label}` segment defaults to
    /// `{hours}h` (or `unlimited` when `--hours 0`) and is overridable via
    /// `--label`.
    #[arg(long, default_value = "./data")]
    base_dir: PathBuf,

    /// Override the second path segment under `--base-dir`. Default
    /// derives from `--hours` (e.g. "24h", "168h", "unlimited").
    #[arg(long)]
    label: Option<String>,
}

const FLUSH_ROW_THRESHOLD: usize = 1000;
const FLUSH_INTERVAL: Duration = Duration::from_secs(60);

#[tokio::main]
async fn main() {
    // reqwest + tokio-tungstenite both pull rustls 0.23 with different
    // provider defaults; install ring explicitly so TLS handshakes don't
    // panic at first use.
    let _ = rustls::crypto::ring::default_provider().install_default();
    // Default log level: INFO in release (so per-flush + lifecycle lines are
    // visible without forcing RUST_LOG), DEBUG in debug. RUST_LOG overrides
    // when set (e.g. `RUST_LOG=warn` to quiet it, or
    // `RUST_LOG=info,tikr_binance=debug` for surgical traces).
    let default_level = if cfg!(debug_assertions) {
        "debug"
    } else {
        "info"
    };
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_level));
    tracing_subscriber::fmt().with_env_filter(filter).init();
    let args = Args::parse();

    let env = match args.env {
        EnvArg::SpotTestnet => BinanceEnv::SpotTestnet,
        EnvArg::SpotMainnet => BinanceEnv::SpotMainnet,
        EnvArg::FuturesTestnet => BinanceEnv::FuturesTestnet,
        EnvArg::FuturesMainnet => BinanceEnv::FuturesMainnet,
    };
    let market_kind = if matches!(env, BinanceEnv::FuturesTestnet | BinanceEnv::FuturesMainnet) {
        MarketKind::Perp
    } else {
        MarketKind::Spot
    };

    let label = args.label.clone().unwrap_or_else(|| {
        if args.hours == 0 {
            "unlimited".to_string()
        } else {
            format!("{}h", args.hours)
        }
    });

    // Single broadcast channel: one ctrl-c fans out to every recorder task.
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    // Spawn signal-handler task once. Broadcasts shutdown then exits.
    {
        let tx = shutdown_tx.clone();
        tokio::spawn(async move {
            let _ = signal::ctrl_c().await;
            info!("SIGINT received — broadcasting shutdown");
            let _ = tx.send(());
        });
    }

    let mut handles = Vec::with_capacity(args.symbols.len());
    for sym_str in &args.symbols {
        let out_dir = args.base_dir.join(&label).join(sym_str);
        if let Err(e) = std::fs::create_dir_all(&out_dir) {
            eprintln!("failed to create output dir {}: {}", out_dir.display(), e);
            std::process::exit(1);
        }

        let (base_str, quote_str) = split_symbol(sym_str);
        let symbol = Symbol {
            base: Asset::new(base_str),
            quote: Asset::new(quote_str),
            venue: VenueId::new("binance"),
            kind: market_kind,
        };
        let base_name = base_str.to_string();
        let sym_label = sym_str.clone();
        let hours = args.hours;
        let rx = shutdown_tx.subscribe();

        handles.push(tokio::spawn(async move {
            let result = run_recorder(
                env,
                symbol,
                base_name,
                sym_label.clone(),
                hours,
                out_dir,
                rx,
            )
            .await;
            (sym_label, result)
        }));
    }

    // Wait for all recorder tasks. We don't bail on first failure — let
    // siblings finish their flush cycle so partial captures still land.
    for h in handles {
        match h.await {
            Ok((sym, Ok(stats))) => {
                info!(
                    symbol = %sym,
                    books_total = stats.books,
                    trades_total = stats.trades,
                    flushes = stats.flushes,
                    "recorder done",
                );
            }
            Ok((sym, Err(e))) => {
                warn!(symbol = %sym, error = %e, "recorder task failed");
            }
            Err(e) => {
                warn!(error = %e, "recorder join error");
            }
        }
    }
}

struct RecorderStats {
    books: u64,
    trades: u64,
    flushes: u64,
}

async fn run_recorder(
    env: BinanceEnv,
    symbol: Symbol,
    base_name: String,
    sym_label: String,
    hours: u32,
    out_dir: PathBuf,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<RecorderStats, String> {
    let mut depth = depth_stream::subscribe_depth(env, symbol.clone())
        .await
        .map_err(|e| format!("depth subscribe failed: {e}"))?;
    let mut trades = trade_stream::subscribe_trades(env, symbol.clone())
        .await
        .map_err(|e| format!("trade subscribe failed: {e}"))?;

    // ParquetReplay discovers `book_<BASE>_*.parquet` / `trades_<BASE>_*.parquet`
    // — keep just the base asset in filenames so the discovery matches.
    let mut recorder = Recorder::new(base_name, out_dir.clone());

    let started = Instant::now();
    let duration_cap = if hours == 0 {
        None
    } else {
        Some(Duration::from_secs(hours as u64 * 3600))
    };
    let mut progress_tick = tokio::time::interval(FLUSH_INTERVAL);
    progress_tick.tick().await;

    info!(
        symbol = %sym_label,
        hours = hours,
        env = ?env,
        out = %out_dir.display(),
        "recorder starting",
    );

    loop {
        if let Some(cap) = duration_cap
            && started.elapsed() >= cap
        {
            info!(symbol = %sym_label, "duration limit reached");
            break;
        }

        tokio::select! {
            ev = depth.next() => match ev {
                Some(MarketEvent::BookUpdate { snapshot }) => recorder.record_book(&snapshot),
                Some(_) => {}
                None => { warn!(symbol = %sym_label, "depth stream ended"); break; }
            },
            ev = trades.next() => match ev {
                Some(MarketEvent::Trade { price, size, side, ts, .. }) => {
                    recorder.record_trade(ts, price, size, side);
                }
                Some(_) => {}
                None => { warn!(symbol = %sym_label, "trade stream ended"); break; }
            },
            _ = progress_tick.tick() => {
                recorder.log_progress(&sym_label);
                if let Err(e) = recorder.flush_if_stale(FLUSH_INTERVAL) {
                    warn!(symbol = %sym_label, error = %e, "periodic flush failed");
                }
            }
            _ = shutdown.recv() => {
                info!(symbol = %sym_label, "shutdown signal received");
                break;
            }
        }

        if recorder.row_count() >= FLUSH_ROW_THRESHOLD
            && let Err(e) = recorder.flush()
        {
            warn!(symbol = %sym_label, error = %e, "row-threshold flush failed");
        }
    }

    recorder
        .flush()
        .map_err(|e| format!("final flush failed: {e}"))?;
    Ok(RecorderStats {
        books: recorder.books_written,
        trades: recorder.trades_written,
        flushes: recorder.flush_count,
    })
}

fn split_symbol(sym: &str) -> (&str, &str) {
    for suffix in &["USDT", "BUSD", "USDC", "TUSD"] {
        if let Some(base) = sym.strip_suffix(suffix)
            && !base.is_empty()
        {
            return (base, suffix);
        }
    }
    let n = sym.len();
    if n > 4 {
        (&sym[..n - 4], &sym[n - 4..])
    } else {
        (sym, "USDT")
    }
}

// ---------------------------------------------------------------------------
// Recorder (mirrors src/bin/record.rs Hyperliquid recorder)
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

    fn log_progress(&self, sym_label: &str) {
        info!(
            symbol = %sym_label,
            books_pending = self.book_buf.len(),
            trades_pending = self.trade_buf.len(),
            books_total = self.books_written,
            trades_total = self.trades_written,
            flushes = self.flush_count,
            "progress",
        );
    }

    fn flush_if_stale(&mut self, interval: Duration) -> Result<(), String> {
        if self.last_flush.elapsed() < interval {
            return Ok(());
        }
        self.flush()
    }

    fn flush(&mut self) -> Result<(), String> {
        if self.book_buf.is_empty() && self.trade_buf.is_empty() {
            return Ok(());
        }
        let stamp = Utc::now().format("%Y%m%d_%H%M%S").to_string();

        if !self.book_buf.is_empty() {
            let path = self
                .out_dir
                .join(format!("book_{}_{}.parquet", self.symbol, stamp));
            write_book_parquet(&path, &self.book_buf)
                .map_err(|e| format!("book parquet write failed: {e}"))?;
            self.books_written += self.book_buf.len() as u64;
            self.book_buf.clear();
        }
        if !self.trade_buf.is_empty() {
            let path = self
                .out_dir
                .join(format!("trades_{}_{}.parquet", self.symbol, stamp));
            write_trades_parquet(&path, &self.trade_buf)
                .map_err(|e| format!("trades parquet write failed: {e}"))?;
            self.trades_written += self.trade_buf.len() as u64;
            self.trade_buf.clear();
        }
        self.flush_count += 1;
        self.last_flush = Instant::now();
        Ok(())
    }
}

fn decimal_to_f64(d: tikr_core::Decimal) -> f64 {
    use rust_decimal::prelude::ToPrimitive;
    d.to_f64().unwrap_or(0.0)
}

fn write_book_parquet(path: &Path, rows: &[BookRow]) -> PolarsResult<()> {
    let ts_ns: Vec<u64> = rows.iter().map(|r| r.ts_ns).collect();
    let side: Vec<i64> = rows.iter().map(|r| r.side).collect();
    let price: Vec<f64> = rows.iter().map(|r| r.price).collect();
    let size: Vec<f64> = rows.iter().map(|r| r.size).collect();
    let seq: Vec<u64> = rows.iter().map(|r| r.seq).collect();
    let mut df = df!(
        "ts_ns" => ts_ns,
        "side" => side,
        "price" => price,
        "size" => size,
        "seq" => seq,
    )?;
    let file = std::fs::File::create(path).map_err(PolarsError::from)?;
    ParquetWriter::new(file).finish(&mut df)?;
    Ok(())
}

fn write_trades_parquet(path: &Path, rows: &[TradeRow]) -> PolarsResult<()> {
    let ts_ns: Vec<u64> = rows.iter().map(|r| r.ts_ns).collect();
    let price: Vec<f64> = rows.iter().map(|r| r.price).collect();
    let size: Vec<f64> = rows.iter().map(|r| r.size).collect();
    let taker_side: Vec<i64> = rows.iter().map(|r| r.taker_side).collect();
    let trade_id: Vec<u64> = rows.iter().map(|r| r.trade_id).collect();
    let mut df = df!(
        "ts_ns" => ts_ns,
        "price" => price,
        "size" => size,
        "taker_side" => taker_side,
        "trade_id" => trade_id,
    )?;
    let file = std::fs::File::create(path).map_err(PolarsError::from)?;
    ParquetWriter::new(file).finish(&mut df)?;
    Ok(())
}
