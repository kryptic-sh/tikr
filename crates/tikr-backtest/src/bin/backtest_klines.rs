//! Candle-based backtester for the random-bracket strategy.
//!
//! Loads a parquet file produced by `download_klines` and simulates random-
//! direction entries with bracketed exits (`tp_bps` take-profit / `sl_bps`
//! stop-loss). All fills are modeled as taker — entry at candle open, exit
//! at the bracket price the candle's high/low touched first.
//!
//! Conservative tie-break: if a single candle's range covers BOTH brackets
//! (e.g. high ≥ entry·(1+tp) AND low ≤ entry·(1-sl)), assume SL hit first.
//! Real OHLC doesn't carry tick order; the pessimistic call is the honest
//! assumption.

use std::path::{Path, PathBuf};

use clap::Parser;
use polars::prelude::*;
use rand::{Rng, SeedableRng, rngs::StdRng};

#[derive(Parser, Debug)]
#[command(
    name = "backtest_klines",
    about = "Run random-bracket strategy against historical OHLCV klines"
)]
struct Args {
    /// Parquet file produced by `download_klines`.
    #[arg(long, default_value = "./data/klines/eth_15m.parquet")]
    data: PathBuf,
    /// Order size (base asset) per cycle.
    #[arg(long, default_value_t = 0.01_f64)]
    size: f64,
    /// Take-profit threshold in bps. 2000 = 20%.
    #[arg(long, default_value_t = 2000u32)]
    tp_bps: u32,
    /// Stop-loss threshold in bps. 500 = 5%.
    #[arg(long, default_value_t = 500u32)]
    sl_bps: u32,
    /// Taker fee in bps (each side).
    #[arg(long, default_value_t = 5u32)]
    taker_bps: u32,
    /// PRNG seeds (comma-separated). Multiple seeds → averaged + variance.
    #[arg(long, default_value = "1,42,1337,2024,9999")]
    seeds: String,
}

#[derive(Debug, Clone, Copy)]
struct Candle {
    open: f64,
    high: f64,
    low: f64,
    open_ts_ms: u64,
}

fn load_candles(path: &Path) -> Result<Vec<Candle>, Box<dyn std::error::Error>> {
    let file = std::fs::File::open(path)?;
    let df = ParquetReader::new(file).finish()?;
    let ts = df.column("open_ts_ms")?.u64()?;
    let open = df.column("open")?.f64()?;
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let n = df.height();
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        out.push(Candle {
            open: open.get(i).ok_or("null open")?,
            high: high.get(i).ok_or("null high")?,
            low: low.get(i).ok_or("null low")?,
            open_ts_ms: ts.get(i).ok_or("null ts")?,
        });
    }
    Ok(out)
}

#[derive(Default, Debug, Clone)]
struct SimResult {
    seed: u64,
    cycles: u64,
    wins: u64,
    losses: u64,
    open_at_end: bool,
    realized: f64,
    fees: f64,
    max_drawdown: f64, // peak-to-trough cum_pnl drawdown
}

impl SimResult {
    fn net(&self) -> f64 {
        self.realized - self.fees
    }
    fn win_rate(&self) -> f64 {
        let n = self.cycles as f64;
        if n == 0.0 { 0.0 } else { self.wins as f64 / n }
    }
    fn avg_per_cycle(&self) -> f64 {
        let n = self.cycles as f64;
        if n == 0.0 { 0.0 } else { self.net() / n }
    }
}

fn simulate(candles: &[Candle], args: &Args, seed: u64) -> SimResult {
    let mut rng = StdRng::seed_from_u64(seed);
    let tp = args.tp_bps as f64 / 10_000.0;
    let sl = args.sl_bps as f64 / 10_000.0;
    let fee_rate = args.taker_bps as f64 / 10_000.0;
    let size = args.size;

    let mut res = SimResult {
        seed,
        ..Default::default()
    };

    // Cumulative PnL tracking for drawdown.
    let mut peak = 0.0f64;

    // Position state. side: +1 long, -1 short, 0 flat. entry: fill price.
    let mut side: i32 = 0;
    let mut entry: f64 = 0.0;

    for c in candles {
        if side == 0 {
            // Open at candle.open in a random direction.
            side = if rng.random::<bool>() { 1 } else { -1 };
            entry = c.open;
            res.fees += entry * size * fee_rate; // entry taker fee
        }

        // Compute bracket levels relative to entry.
        let (tp_price, sl_price) = if side > 0 {
            (entry * (1.0 + tp), entry * (1.0 - sl))
        } else {
            (entry * (1.0 - tp), entry * (1.0 + sl))
        };

        // Did either bracket fall inside this candle's range?
        let high = c.high;
        let low = c.low;
        let hit_tp = if side > 0 {
            high >= tp_price
        } else {
            low <= tp_price
        };
        let hit_sl = if side > 0 {
            low <= sl_price
        } else {
            high >= sl_price
        };

        if hit_tp || hit_sl {
            // Conservative tie-break: SL first.
            let (exit_price, is_win) = if hit_sl {
                (sl_price, false)
            } else {
                (tp_price, true)
            };
            let pnl_per_unit = if side > 0 {
                exit_price - entry
            } else {
                entry - exit_price
            };
            let realized = pnl_per_unit * size;
            res.realized += realized;
            res.fees += exit_price * size * fee_rate; // exit taker fee

            let cum_pnl = res.realized - res.fees;
            if cum_pnl > peak {
                peak = cum_pnl;
            }
            let dd = peak - cum_pnl;
            if dd > res.max_drawdown {
                res.max_drawdown = dd;
            }

            res.cycles += 1;
            if is_win {
                res.wins += 1
            } else {
                res.losses += 1
            };
            side = 0;
            entry = 0.0;
        }
    }

    res.open_at_end = side != 0;
    res
}

fn print_summary(args: &Args, results: &[SimResult]) {
    println!(
        "\nRandom-bracket on klines  |  tp={}bps  sl={}bps  taker={}bps  size={}",
        args.tp_bps, args.sl_bps, args.taker_bps, args.size
    );
    println!("{}", "-".repeat(96));
    println!(
        "{:>6} {:>8} {:>6} {:>6} {:>7} {:>12} {:>12} {:>12} {:>10} {:>7}",
        "seed", "cycles", "wins", "loss", "win%", "realized", "fees", "NET", "max_DD", "open"
    );
    for r in results {
        println!(
            "{:>6} {:>8} {:>6} {:>6} {:>6.1}% {:>12.4} {:>12.4} {:>12.4} {:>10.4} {:>7}",
            r.seed,
            r.cycles,
            r.wins,
            r.losses,
            r.win_rate() * 100.0,
            r.realized,
            r.fees,
            r.net(),
            r.max_drawdown,
            if r.open_at_end { "yes" } else { "no" }
        );
    }
    let n = results.len() as f64;
    let avg_net = results.iter().map(|r| r.net()).sum::<f64>() / n;
    let avg_per_cycle = results.iter().map(|r| r.avg_per_cycle()).sum::<f64>() / n;
    let avg_cycles = results.iter().map(|r| r.cycles as f64).sum::<f64>() / n;
    let avg_win = results.iter().map(|r| r.win_rate()).sum::<f64>() / n;
    println!("{}", "-".repeat(96));
    println!(
        "{:>6} {:>8.1} {:>6} {:>6} {:>6.1}% {:>12} {:>12} {:>12.4} {:>10} {:>7}",
        "AVG",
        avg_cycles,
        "-",
        "-",
        avg_win * 100.0,
        "-",
        "-",
        avg_net,
        "-",
        "-"
    );
    println!(
        "Per-cycle avg NET (across seeds): {:.6} ({:.2} bps on size {})",
        avg_per_cycle,
        avg_per_cycle / args.size * 10_000.0 / results[0].cycles.max(1) as f64,
        args.size
    );
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
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

    let seeds: Vec<u64> = args
        .seeds
        .split(',')
        .map(|s| s.trim().parse::<u64>())
        .collect::<Result<Vec<_>, _>>()?;

    let mut results = Vec::with_capacity(seeds.len());
    for seed in seeds {
        results.push(simulate(&candles, &args, seed));
    }
    print_summary(&args, &results);
    Ok(())
}
