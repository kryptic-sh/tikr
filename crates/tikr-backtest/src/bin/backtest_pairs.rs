//! Statistical-arbitrage pairs trader against OHLCV klines.
//!
//! Takes exactly two symbols (A and B), aligns their candles on `open_ts_ms`
//! (inner join — candles missing from either side are dropped), then trades the
//! log-spread:
//!
//!   s = ln(close_A) − beta × ln(close_B)
//!
//! Rolling window of N values of `s`: compute `mean_s`, `std_s`.
//! Z-score = (s − mean_s) / std_s.
//!
//! Entry / exit rules:
//! - LONG-SPREAD (long A, short B): enter when z < −Z_in, exit when z > −Z_out.
//! - SHORT-SPREAD (short A, long B): enter when z > Z_in, exit when z < Z_out.
//! - One spread position at a time.
//!
//! Leg sizing: each leg uses `--notional` USDT. Coin qty = notional / leg_close.
//! Both legs pay maker fee on entry AND exit.
//!
//! PnL is tracked as combined USDT across both legs.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use clap::Parser;
use polars::prelude::*;

#[derive(Parser, Debug)]
#[command(
    name = "backtest_pairs",
    about = "Statistical-arbitrage pairs trader: log-spread z-score mean reversion"
)]
struct Args {
    /// Symbol A (the numerator leg, e.g. BTCUSDT).
    #[arg(long, default_value = "BTCUSDT")]
    symbol_a: String,
    /// Symbol B (the denominator leg, e.g. ETHUSDT).
    #[arg(long, default_value = "ETHUSDT")]
    symbol_b: String,
    /// Base directory containing the parquet files.
    #[arg(long, default_value = "./data/klines")]
    data_dir: PathBuf,
    /// Filename suffix after the lowercase base symbol.
    /// Path = {data-dir}/{lower-strip-USDT}{file-suffix}.
    #[arg(long, default_value = "_1m_1y.parquet")]
    file_suffix: String,
    /// Rolling window (number of candles) for spread mean/stdev.
    #[arg(long, default_value_t = 60usize)]
    window: usize,
    /// Z-score threshold to enter a spread position.
    #[arg(long, default_value_t = 2.0_f64)]
    z_in: f64,
    /// Z-score threshold to exit a spread position.
    #[arg(long, default_value_t = 0.5_f64)]
    z_out: f64,
    /// Hedge ratio: s = ln(A) − beta × ln(B). `1.0` = log-ratio.
    #[arg(long, default_value_t = 1.0_f64)]
    beta: f64,
    /// Fiat notional per leg (USDT). Both legs use the same notional.
    #[arg(long, default_value_t = 100.0_f64)]
    notional: f64,
    /// Maker fee in bps. Each leg pays this on entry and on exit.
    #[arg(long, default_value_t = 2u32)]
    maker_bps: u32,
    /// Starting USDT budget for account-level reporting. `0` disables.
    #[arg(long, default_value_t = 0.0_f64)]
    budget: f64,
}

/// One aligned bar for a single symbol.
#[derive(Debug, Clone, Copy)]
struct Candle {
    close: f64,
    open_ts_ms: u64,
}

fn load_candles(path: &Path) -> Result<Vec<Candle>, Box<dyn std::error::Error>> {
    let file = std::fs::File::open(path)?;
    let df = ParquetReader::new(file).finish()?;
    let ts = df.column("open_ts_ms")?.u64()?;
    let close = df.column("close")?.f64()?;
    let n = df.height();
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        out.push(Candle {
            close: close.get(i).ok_or("null close")?,
            open_ts_ms: ts.get(i).ok_or("null ts")?,
        });
    }
    Ok(out)
}

/// Inner-join two candle series on `open_ts_ms`.
/// Returns `(aligned_a, aligned_b)` with equal length.
fn align(a: &[Candle], b: &[Candle]) -> (Vec<Candle>, Vec<Candle>) {
    // Build a hashmap of ts_ms → index for the smaller side.
    let map_b: HashMap<u64, usize> = b
        .iter()
        .enumerate()
        .map(|(i, c)| (c.open_ts_ms, i))
        .collect();

    let mut out_a = Vec::new();
    let mut out_b = Vec::new();
    for ca in a {
        if let Some(&ib) = map_b.get(&ca.open_ts_ms) {
            out_a.push(*ca);
            out_b.push(b[ib]);
        }
    }
    (out_a, out_b)
}

/// Rolling mean and population stdev of the last `window` values ending at `idx`.
fn rolling_stats(spreads: &[f64], idx: usize, window: usize) -> Option<(f64, f64)> {
    if idx + 1 < window {
        return None;
    }
    let slice = &spreads[idx + 1 - window..=idx];
    let n = slice.len() as f64;
    let mean = slice.iter().sum::<f64>() / n;
    let var = slice.iter().map(|s| (s - mean).powi(2)).sum::<f64>() / n;
    Some((mean, var.sqrt()))
}

#[derive(Debug, Clone, Copy)]
enum SpreadPosition {
    Flat,
    /// Long A, short B. Recorded entry closes (A, B) and coin quantities.
    LongSpread {
        entry_a: f64,
        qty_a: f64,
        entry_b: f64,
        qty_b: f64,
    },
    /// Short A, long B.
    ShortSpread {
        entry_a: f64,
        qty_a: f64,
        entry_b: f64,
        qty_b: f64,
    },
}

#[derive(Default, Debug)]
struct Stats {
    trades: u64,
    wins: u64,
    losses: u64,
    realized_usdt: f64,
    fees: f64,
    max_drawdown: f64,
}

impl Stats {
    fn net(&self) -> f64 {
        self.realized_usdt - self.fees
    }
}

fn simulate(a: &[Candle], b: &[Candle], args: &Args) -> Stats {
    // Pre-compute the full spread series.
    let spreads: Vec<f64> = a
        .iter()
        .zip(b.iter())
        .map(|(ca, cb)| ca.close.ln() - args.beta * cb.close.ln())
        .collect();

    let maker_rate = args.maker_bps as f64 / 10_000.0;
    let notional = args.notional;

    let mut pos = SpreadPosition::Flat;
    let mut stats = Stats::default();
    let mut peak_net = 0.0_f64;

    for i in 0..spreads.len() {
        let Some((mean_s, std_s)) = rolling_stats(&spreads, i, args.window) else {
            continue;
        };
        if std_s < 1e-12 {
            continue;
        }
        let z = (spreads[i] - mean_s) / std_s;
        let ca = &a[i];
        let cb = &b[i];

        match pos {
            SpreadPosition::Flat => {
                if z < -args.z_in {
                    // Long spread: long A, short B.
                    let qty_a = notional / ca.close;
                    let qty_b = notional / cb.close;
                    // Entry fees: both legs, maker.
                    stats.fees += notional * maker_rate * 2.0;
                    pos = SpreadPosition::LongSpread {
                        entry_a: ca.close,
                        qty_a,
                        entry_b: cb.close,
                        qty_b,
                    };
                } else if z > args.z_in {
                    // Short spread: short A, long B.
                    let qty_a = notional / ca.close;
                    let qty_b = notional / cb.close;
                    stats.fees += notional * maker_rate * 2.0;
                    pos = SpreadPosition::ShortSpread {
                        entry_a: ca.close,
                        qty_a,
                        entry_b: cb.close,
                        qty_b,
                    };
                }
            }
            SpreadPosition::LongSpread {
                entry_a,
                qty_a,
                entry_b,
                qty_b,
            } => {
                if z > -args.z_out {
                    // Exit: close long A at current A close, close short B at current B close.
                    let pnl_a = (ca.close - entry_a) * qty_a;
                    let pnl_b = (entry_b - cb.close) * qty_b; // short B profit when B falls
                    let gross = pnl_a + pnl_b;
                    stats.realized_usdt += gross;
                    // Exit fees: both legs, maker.
                    stats.fees += (ca.close * qty_a + cb.close * qty_b) * maker_rate;
                    stats.trades += 1;
                    if gross > 0.0 {
                        stats.wins += 1;
                    } else {
                        stats.losses += 1;
                    }
                    pos = SpreadPosition::Flat;
                }
            }
            SpreadPosition::ShortSpread {
                entry_a,
                qty_a,
                entry_b,
                qty_b,
            } => {
                if z < args.z_out {
                    // Exit: close short A, close long B.
                    let pnl_a = (entry_a - ca.close) * qty_a; // short A profit when A falls
                    let pnl_b = (cb.close - entry_b) * qty_b;
                    let gross = pnl_a + pnl_b;
                    stats.realized_usdt += gross;
                    stats.fees += (ca.close * qty_a + cb.close * qty_b) * maker_rate;
                    stats.trades += 1;
                    if gross > 0.0 {
                        stats.wins += 1;
                    } else {
                        stats.losses += 1;
                    }
                    pos = SpreadPosition::Flat;
                }
            }
        }

        // Mark-to-market for drawdown tracking.
        let mtm_pos = match pos {
            SpreadPosition::LongSpread {
                entry_a,
                qty_a,
                entry_b,
                qty_b,
            } => {
                // Unrealised PnL: long A + short B at current prices.
                (ca.close - entry_a) * qty_a + (entry_b - cb.close) * qty_b
            }
            SpreadPosition::ShortSpread {
                entry_a,
                qty_a,
                entry_b,
                qty_b,
            } => (entry_a - ca.close) * qty_a + (cb.close - entry_b) * qty_b,
            SpreadPosition::Flat => 0.0,
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

    stats
}

fn symbol_to_path(data_dir: &Path, sym: &str, suffix: &str) -> PathBuf {
    let base = sym.trim_end_matches("USDT").to_lowercase();
    data_dir.join(format!("{base}{suffix}"))
}

fn print_summary(args: &Args, stats: &Stats, n_aligned: usize) {
    println!(
        "\nPairs spread  |  {}/{} beta={:.2}  window={}  z_in={:.2}  z_out={:.2}  notional=${}  maker={}bps",
        args.symbol_a,
        args.symbol_b,
        args.beta,
        args.window,
        args.z_in,
        args.z_out,
        args.notional,
        args.maker_bps
    );
    println!("{}", "-".repeat(96));
    println!("aligned candles   : {}", n_aligned);
    let win_rate = if stats.trades == 0 {
        0.0
    } else {
        stats.wins as f64 / stats.trades as f64 * 100.0
    };
    println!(
        "trades            : {}  ({} wins / {} losses  {:.1}% win rate)",
        stats.trades, stats.wins, stats.losses, win_rate
    );
    println!("realized (gross)  : {:>14.4}", stats.realized_usdt);
    println!("fees              : {:>14.4}", stats.fees);
    println!("NET               : {:>14.4}", stats.net());
    println!("max drawdown      : {:>14.4}", stats.max_drawdown);
    if args.budget > 0.0 {
        let pct = stats.net() / args.budget * 100.0;
        println!(
            "TOTAL ACCT (budget ${:.2}): {:>10.4}  ({:+.2}%)",
            args.budget,
            args.budget + stats.net(),
            pct
        );
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let path_a = symbol_to_path(&args.data_dir, &args.symbol_a, &args.file_suffix);
    let path_b = symbol_to_path(&args.data_dir, &args.symbol_b, &args.file_suffix);

    let raw_a = load_candles(&path_a)?;
    let raw_b = load_candles(&path_b)?;
    eprintln!(
        "loaded {} candles for {} from {}",
        raw_a.len(),
        args.symbol_a,
        path_a.display()
    );
    eprintln!(
        "loaded {} candles for {} from {}",
        raw_b.len(),
        args.symbol_b,
        path_b.display()
    );

    let (a, b) = align(&raw_a, &raw_b);
    eprintln!("aligned: {} shared candles", a.len());
    if a.is_empty() {
        return Err(
            "no aligned candles — check that both parquets cover the same time window".into(),
        );
    }
    if !a.is_empty() {
        let span_ms = a.last().unwrap().open_ts_ms - a[0].open_ts_ms;
        let span_d = span_ms as f64 / (24.0 * 60.0 * 60_000.0);
        eprintln!("span: {:.1} days", span_d);
    }

    let stats = simulate(&a, &b, &args);
    print_summary(&args, &stats, a.len());
    Ok(())
}
