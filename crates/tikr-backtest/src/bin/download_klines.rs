//! Download historical klines (OHLCV candles) from Binance and save as
//! parquet. Used as the data source for `backtest_klines`.
//!
//! Notes:
//!
//! - Binance supports intervals `1m / 3m / 5m / 15m / 30m / 1h / 2h / 4h /
//!   6h / 8h / 12h / 1d / 3d / 1w / 1M`. There is NO native 10m interval —
//!   pick `5m` (and resample externally if needed) or `15m`. The strategy
//!   tester treats whatever interval you give it as the unit candle.
//! - Spot endpoint: `https://api.binance.com/api/v3/klines`.
//! - Futures (USD-M) endpoint: `https://fapi.binance.com/fapi/v1/klines`.

use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use polars::prelude::*;
use serde::Deserialize;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Market {
    Spot,
    Futures,
}

#[derive(Parser, Debug)]
#[command(
    name = "download_klines",
    about = "Download Binance klines (OHLCV candles) to a parquet file"
)]
struct Args {
    /// Binance symbol (e.g. ETHUSDT).
    #[arg(long, default_value = "ETHUSDT")]
    symbol: String,
    /// Kline interval (1m,3m,5m,15m,30m,1h,2h,4h,6h,8h,12h,1d).
    #[arg(long, default_value = "15m")]
    interval: String,
    /// How many days of history to fetch, ending at `end` (default: now).
    #[arg(long, default_value_t = 90u32)]
    days: u32,
    /// End-of-window timestamp (ISO 8601 like `2022-05-15T00:00:00Z`).
    /// Default empty = "now". Use to backtest specific historical periods
    /// (e.g. LUNA collapse: `--end 2022-05-15T00:00:00Z --days 30`).
    #[arg(long, default_value = "")]
    end: String,
    /// Spot or USD-M futures.
    #[arg(long, value_enum, default_value = "futures")]
    market: Market,
    /// Output parquet path.
    #[arg(long, default_value = "./data/klines/eth_15m.parquet")]
    out: PathBuf,
}

/// One kline row, post-decode.
#[derive(Debug, Clone)]
struct Kline {
    open_ts_ms: u64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    close_ts_ms: u64,
    trades: u64,
}

/// Raw Binance kline shape — array of 12 mixed scalars per row. Parse with
/// permissive types because numeric fields are wire-serialized as strings.
#[derive(Debug, Deserialize)]
struct RawKline(
    u64,                        // open time (ms)
    String,                     // open
    String,                     // high
    String,                     // low
    String,                     // close
    String,                     // volume (base asset)
    u64,                        // close time (ms)
    String,                     // quote volume
    u64,                        // number of trades
    #[allow(dead_code)] String, // taker buy base
    #[allow(dead_code)] String, // taker buy quote
    #[allow(dead_code)] String, // ignore
);

impl TryFrom<RawKline> for Kline {
    type Error = String;
    fn try_from(r: RawKline) -> Result<Self, Self::Error> {
        let parse = |s: &str| -> Result<f64, String> {
            s.parse::<f64>().map_err(|e| format!("parse {s}: {e}"))
        };
        Ok(Kline {
            open_ts_ms: r.0,
            open: parse(&r.1)?,
            high: parse(&r.2)?,
            low: parse(&r.3)?,
            close: parse(&r.4)?,
            volume: parse(&r.5)?,
            close_ts_ms: r.6,
            trades: r.8,
        })
    }
}

fn base_url(m: Market) -> &'static str {
    match m {
        Market::Spot => "https://api.binance.com/api/v3/klines",
        Market::Futures => "https://fapi.binance.com/fapi/v1/klines",
    }
}

async fn fetch_page(
    client: &reqwest::Client,
    url: &str,
    symbol: &str,
    interval: &str,
    start_ms: u64,
    end_ms: u64,
) -> Result<Vec<Kline>, Box<dyn std::error::Error>> {
    // limit=1500 is the max for klines on both spot and futures endpoints.
    // Retry on 429 (rate limit) and 418 (IP ban warn-up) with exponential
    // backoff. Respect `Retry-After` header when present.
    let mut delay = Duration::from_secs(2);
    loop {
        let resp = client
            .get(url)
            .query(&[
                ("symbol", symbol.to_string()),
                ("interval", interval.to_string()),
                ("startTime", start_ms.to_string()),
                ("endTime", end_ms.to_string()),
                ("limit", "1500".to_string()),
            ])
            .send()
            .await?;
        let status = resp.status();
        if status == 429 || status == 418 {
            let wait = resp
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .map(Duration::from_secs)
                .unwrap_or(delay);
            eprintln!("rate-limited ({status}); backing off {}s", wait.as_secs());
            tokio::time::sleep(wait).await;
            delay = (delay * 2).min(Duration::from_secs(60));
            continue;
        }
        let resp = resp.error_for_status()?;
        let raws: Vec<RawKline> = resp.json().await?;
        let mut out = Vec::with_capacity(raws.len());
        for r in raws {
            out.push(Kline::try_from(r)?);
        }
        return Ok(out);
    }
}

fn interval_ms(interval: &str) -> Result<u64, String> {
    let (num, unit) = interval.split_at(interval.len() - 1);
    let n: u64 = num.parse().map_err(|e| format!("bad interval num: {e}"))?;
    let mult: u64 = match unit {
        "m" => 60_000,
        "h" => 60 * 60_000,
        "d" => 24 * 60 * 60_000,
        "w" => 7 * 24 * 60 * 60_000,
        "M" => 30 * 24 * 60 * 60_000, // rough
        other => return Err(format!("unknown interval unit '{other}'")),
    };
    Ok(n * mult)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // reqwest + rustls 0.23 needs an explicit CryptoProvider when feature
    // selection is ambiguous (see record_binance.rs for the same install).
    let _ = rustls::crypto::ring::default_provider().install_default();
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let url = base_url(args.market);
    let end_ms: u64 = if args.end.is_empty() {
        chrono::Utc::now().timestamp_millis() as u64
    } else {
        chrono::DateTime::parse_from_rfc3339(&args.end)
            .map_err(|e| format!("bad --end timestamp '{}': {e}", args.end))?
            .timestamp_millis() as u64
    };
    let total_window_ms = (args.days as u64) * 24 * 60 * 60_000;
    let start_ms = end_ms.saturating_sub(total_window_ms);
    eprintln!(
        "fetch window: {} ms .. {} ms ({} days)",
        start_ms, end_ms, args.days
    );
    let step_ms = interval_ms(&args.interval)? * 1500;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    let mut all: Vec<Kline> = Vec::new();
    let mut cursor = start_ms;
    while cursor < end_ms {
        let end = (cursor + step_ms).min(end_ms);
        let page = fetch_page(&client, url, &args.symbol, &args.interval, cursor, end).await?;
        let page_len = page.len();
        if page_len == 0 {
            cursor = end;
            continue;
        }
        let last = page.last().unwrap().open_ts_ms;
        all.extend(page);
        // Advance past the last open time so we don't refetch it.
        cursor = last + 1;
        eprintln!(
            "fetched {page_len} candles, total {} so far, cursor={cursor}",
            all.len()
        );
        // 250ms inter-request pacing keeps us well under the 2400 weight/min
        // budget for klines (cost=10 at limit=1500 → 240 req/min ceiling).
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    eprintln!("total candles: {}", all.len());
    if all.is_empty() {
        return Err("no candles fetched".into());
    }

    // Build parquet columns.
    let open_ts_ms: Vec<u64> = all.iter().map(|k| k.open_ts_ms).collect();
    let open: Vec<f64> = all.iter().map(|k| k.open).collect();
    let high: Vec<f64> = all.iter().map(|k| k.high).collect();
    let low: Vec<f64> = all.iter().map(|k| k.low).collect();
    let close: Vec<f64> = all.iter().map(|k| k.close).collect();
    let volume: Vec<f64> = all.iter().map(|k| k.volume).collect();
    let close_ts_ms: Vec<u64> = all.iter().map(|k| k.close_ts_ms).collect();
    let trades: Vec<u64> = all.iter().map(|k| k.trades).collect();
    let mut df = df!(
        "open_ts_ms" => open_ts_ms,
        "open" => open,
        "high" => high,
        "low" => low,
        "close" => close,
        "volume" => volume,
        "close_ts_ms" => close_ts_ms,
        "trades" => trades,
    )?;

    if let Some(parent) = args.out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::File::create(&args.out)?;
    ParquetWriter::new(file).finish(&mut df)?;
    eprintln!("wrote {} candles to {}", all.len(), args.out.display());
    Ok(())
}
