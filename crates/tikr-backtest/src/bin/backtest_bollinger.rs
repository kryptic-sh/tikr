//! Bollinger-band z-score scalper against OHLCV klines.
//!
//! Strategy:
//!
//! 1. Rolling window of `N` closes: compute `mean` and `stdev`.
//! 2. **Bollinger bands**: upper = mean + Z × stdev, lower = mean − Z × stdev.
//! 3. When flat:
//!    - Enter LONG if `close < lower` (price hit lower band).
//!    - Enter SHORT if `close > upper` (price hit upper band).
//! 4. When long: exit when `close ≥ mean`. When short: exit when `close ≤ mean`.
//! 5. One position at a time — no new entry until prior exit fires.
//! 6. Fixed fiat notional per entry. Qty = `notional / close`.
//! 7. Entry and exit both pay maker fee (we assume we can rest limit orders at
//!    the band / mean — optimistic model consistent with the other kline bins).
//!
//! Self-tuning to volatility: band width tracks σ, so entries fire on
//! statistically significant deviations regardless of absolute price level.

use std::path::{Path, PathBuf};

use clap::Parser;
use polars::prelude::*;

#[derive(Parser, Debug)]
#[command(
    name = "backtest_bollinger",
    about = "Bollinger-band z-score mean-reversion scalper against OHLCV klines"
)]
struct Args {
    /// Single parquet path. Mutually exclusive with `--symbols`.
    #[arg(long, default_value = "./data/klines/eth_1m_90d.parquet")]
    data: PathBuf,
    /// Comma-separated symbols (e.g. `BTCUSDT,ETHUSDT`).
    /// Resolves each to `{data-dir}/{lower-strip-USDT}{file-suffix}`.
    /// Overrides `--data` when set.
    #[arg(long, default_value = "")]
    symbols: String,
    /// Base directory for `--symbols` lookup.
    #[arg(long, default_value = "./data/klines")]
    data_dir: PathBuf,
    /// Filename suffix after the lowercase base symbol.
    #[arg(long, default_value = "_1m_1d.parquet")]
    file_suffix: String,
    /// Rolling window size for mean / stdev computation.
    #[arg(long, default_value_t = 20usize)]
    window: usize,
    /// Number of standard deviations for the band threshold.
    #[arg(long, default_value_t = 2.0_f64)]
    z_score: f64,
    /// Fixed fiat notional per entry (USDT). Qty = notional / close.
    #[arg(long, default_value_t = 100.0_f64)]
    notional: f64,
    /// Maker fee in bps. Entry + exit each pay this.
    #[arg(long, default_value_t = 2u32)]
    maker_bps: u32,
    /// Starting USDT budget for account-level reporting. `0` disables.
    #[arg(long, default_value_t = 0.0_f64)]
    budget: f64,
}

#[derive(Debug, Clone, Copy)]
struct Candle {
    high: f64,
    low: f64,
    close: f64,
    open_ts_ms: u64,
}

fn load_candles(path: &Path) -> Result<Vec<Candle>, Box<dyn std::error::Error>> {
    let file = std::fs::File::open(path)?;
    let df = ParquetReader::new(file).finish()?;
    let ts = df.column("open_ts_ms")?.u64()?;
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let close = df.column("close")?.f64()?;
    let n = df.height();
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        out.push(Candle {
            high: high.get(i).ok_or("null high")?,
            low: low.get(i).ok_or("null low")?,
            close: close.get(i).ok_or("null close")?,
            open_ts_ms: ts.get(i).ok_or("null ts")?,
        });
    }
    Ok(out)
}

/// Rolling mean and population stdev over the last `window` closes.
fn rolling_stats(closes: &[f64]) -> (f64, f64) {
    let n = closes.len() as f64;
    let mean = closes.iter().sum::<f64>() / n;
    let var = closes.iter().map(|c| (c - mean).powi(2)).sum::<f64>() / n;
    (mean, var.sqrt())
}

#[derive(Debug, Clone, Copy)]
enum Position {
    Flat,
    Long { entry: f64, qty: f64 },
    Short { entry: f64, qty: f64 },
}

#[derive(Default, Debug, Clone)]
struct Stats {
    fills: u64,
    wins: u64,
    losses: u64,
    realized_usdt: f64,
    fees: f64,
    max_drawdown: f64,
    base_position: f64,
}

impl Stats {
    fn net(&self) -> f64 {
        self.realized_usdt - self.fees
    }
}

fn simulate(candles: &[Candle], args: &Args) -> (Stats, f64) {
    if candles.len() < args.window + 1 {
        return (Stats::default(), 0.0);
    }
    let maker_rate = args.maker_bps as f64 / 10_000.0;
    let notional = args.notional;

    let mut pos = Position::Flat;
    let mut stats = Stats::default();
    let mut peak_net = 0.0_f64;

    for i in args.window..candles.len() {
        let window_closes: Vec<f64> = candles[i - args.window..i]
            .iter()
            .map(|c| c.close)
            .collect();
        let (mean, stdev) = rolling_stats(&window_closes);
        let c = &candles[i];

        // Degenerate case: flat price → stdev is ~0, bands are meaningless.
        if stdev < 1e-12 {
            continue;
        }

        let upper = mean + args.z_score * stdev;
        let lower = mean - args.z_score * stdev;

        match pos {
            Position::Flat => {
                // Check for entry: use candle range so we catch intra-bar touches.
                if c.low < lower {
                    // Long entry at the lower band (optimistic maker fill).
                    let entry_price = lower;
                    let qty = notional / entry_price;
                    stats.fees += notional * maker_rate;
                    stats.base_position += qty;
                    pos = Position::Long {
                        entry: entry_price,
                        qty,
                    };
                } else if c.high > upper {
                    // Short entry at upper band.
                    let entry_price = upper;
                    let qty = notional / entry_price;
                    stats.fees += notional * maker_rate;
                    stats.base_position -= qty;
                    pos = Position::Short {
                        entry: entry_price,
                        qty,
                    };
                }
            }
            Position::Long { entry, qty } => {
                // Exit when price reverts to mean.
                if c.high >= mean {
                    let exit_price = mean;
                    let gross = (exit_price - entry) * qty;
                    stats.realized_usdt += gross;
                    let exit_notional = exit_price * qty;
                    stats.fees += exit_notional * maker_rate;
                    stats.base_position -= qty;
                    stats.fills += 1;
                    if gross > 0.0 {
                        stats.wins += 1;
                    } else {
                        stats.losses += 1;
                    }
                    pos = Position::Flat;
                }
            }
            Position::Short { entry, qty } => {
                // Exit when price reverts to mean.
                if c.low <= mean {
                    let exit_price = mean;
                    let gross = (entry - exit_price) * qty;
                    stats.realized_usdt += gross;
                    let exit_notional = exit_price * qty;
                    stats.fees += exit_notional * maker_rate;
                    stats.base_position += qty;
                    stats.fills += 1;
                    if gross > 0.0 {
                        stats.wins += 1;
                    } else {
                        stats.losses += 1;
                    }
                    pos = Position::Flat;
                }
            }
        }

        // Drawdown: mark-to-market = net realized + open position value.
        let mtm_pos = match pos {
            Position::Long { qty, .. } => qty * c.close,
            Position::Short { qty, .. } => -(qty * c.close),
            Position::Flat => 0.0,
        };
        let net = stats.net() + mtm_pos;
        if net > peak_net {
            peak_net = net;
        }
        let dd = peak_net - net;
        if dd > stats.max_drawdown {
            stats.max_drawdown = dd;
        }
    }

    let last_close = candles.last().map(|c| c.close).unwrap_or(0.0);
    (stats, last_close)
}

fn parse_symbols(s: &str) -> Vec<String> {
    s.split(',')
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
        .collect()
}

fn symbol_to_path(data_dir: &Path, sym: &str, suffix: &str) -> PathBuf {
    let base = sym.trim_end_matches("USDT").to_lowercase();
    data_dir.join(format!("{base}{suffix}"))
}

fn print_single_summary(args: &Args, stats: &Stats, last_close: f64) {
    println!(
        "\nBollinger-band z-score  |  window={}  z={:.2}  notional=${}  maker={}bps",
        args.window, args.z_score, args.notional, args.maker_bps
    );
    println!("{}", "-".repeat(96));
    let win_rate = if stats.fills == 0 {
        0.0
    } else {
        stats.wins as f64 / stats.fills as f64 * 100.0
    };
    println!(
        "fills             : {}  ({} wins / {} losses  {:.1}% win rate)",
        stats.fills, stats.wins, stats.losses, win_rate
    );
    println!("realized          : {:>14.4}", stats.realized_usdt);
    println!("fees              : {:>14.4}", stats.fees);
    println!("realized − fees   : {:>14.4}", stats.net());
    let base_value = stats.base_position * last_close;
    println!(
        "base position USDT: {:>14.4}  (at last close {:.4})",
        base_value, last_close
    );
    let mtm = stats.net() + base_value;
    println!("TOTAL MTM PnL     : {:>14.4}", mtm);
    println!("max drawdown      : {:>14.4}", stats.max_drawdown);
    if args.budget > 0.0 {
        let pct = mtm / args.budget * 100.0;
        println!(
            "TOTAL ACCT (budget ${:.2}): {:>10.4}  ({:+.2}%)",
            args.budget,
            args.budget + mtm,
            pct
        );
    }
}

fn print_table_header(args: &Args) {
    println!(
        "\nBollinger sweep  |  window={}  z={:.2}  notional=${}  maker={}bps",
        args.window, args.z_score, args.notional, args.maker_bps
    );
    println!("{}", "-".repeat(96));
    println!(
        "{:<14} {:>7} {:>8} {:>12} {:>10} {:>10} {:>10} {:>10}",
        "SYMBOL", "FILLS", "WIN%", "REAL-FEE", "BASE_USDT", "MTM", "DD", "ACCT%"
    );
}

fn print_table_row(sym: &str, stats: &Stats, last_close: f64, budget: f64) {
    let win_pct = if stats.fills == 0 {
        0.0
    } else {
        stats.wins as f64 / stats.fills as f64 * 100.0
    };
    let base_value = stats.base_position * last_close;
    let mtm = stats.net() + base_value;
    let acct = if budget > 0.0 {
        format!("{:+.2}%", mtm / budget * 100.0)
    } else {
        "-".to_string()
    };
    println!(
        "{:<14} {:>7} {:>7.1}% {:>12.4} {:>10.4} {:>10.4} {:>10.4} {:>10}",
        sym,
        stats.fills,
        win_pct,
        stats.net(),
        base_value,
        mtm,
        stats.max_drawdown,
        acct
    );
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let symbols = parse_symbols(&args.symbols);

    if symbols.is_empty() {
        let candles = load_candles(&args.data)?;
        eprintln!(
            "loaded {} candles from {}",
            candles.len(),
            args.data.display()
        );
        if !candles.is_empty() {
            let span_ms = candles.last().unwrap().open_ts_ms - candles[0].open_ts_ms;
            let span_d = span_ms as f64 / (24.0 * 60.0 * 60_000.0);
            eprintln!("span: {:.1} days", span_d);
        }
        let (stats, last_close) = simulate(&candles, &args);
        print_single_summary(&args, &stats, last_close);
        return Ok(());
    }

    print_table_header(&args);
    let mut totals_mtm = 0.0;
    let mut totals_real_fee = 0.0;
    let mut totals_dd = 0.0;
    let mut wins = 0usize;
    for sym in &symbols {
        let path = symbol_to_path(&args.data_dir, sym, &args.file_suffix);
        let candles = match load_candles(&path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("{sym}: load failed ({} — {e})", path.display());
                continue;
            }
        };
        let (stats, last_close) = simulate(&candles, &args);
        let base_value = stats.base_position * last_close;
        let mtm = stats.net() + base_value;
        if mtm > 0.0 {
            wins += 1;
        }
        totals_mtm += mtm;
        totals_real_fee += stats.net();
        totals_dd += stats.max_drawdown;
        print_table_row(sym, &stats, last_close, args.budget);
    }
    let n = symbols.len() as f64;
    println!("{}", "-".repeat(96));
    println!(
        "MEAN ({} sym, {} wins): real-fee={:.4}  mtm={:.4}  dd={:.4}",
        symbols.len(),
        wins,
        totals_real_fee / n,
        totals_mtm / n,
        totals_dd / n
    );
    if args.budget > 0.0 {
        println!(
            "Aggregate acct% (sum mtm / (n × budget)): {:+.2}%",
            totals_mtm / (n * args.budget) * 100.0
        );
    }
    Ok(())
}
