//! Band-reversion candle backtester.
//!
//! Strategy:
//!
//! 1. Each bar, compute a band from the trailing `lookback` candles:
//!    `avg_top = mean(high)`, `avg_bot = mean(low)`. Center = midpoint.
//!    Width = `(avg_top − avg_bot) × compress`. Default compress = 0.5 →
//!    the band is INSIDE the avg range, so both edges are more likely to
//!    be touched within a single new bar.
//! 2. When **flat**, post post-only limits at both edges:
//!    - BUY at `lower = center − width/2`
//!    - SELL at `upper = center + width/2`
//!    If the next candle's `low ≤ lower` the BUY fills; `high ≥ upper`
//!    fills the SELL. Both filling = clean round trip (1 cycle, gross =
//!    `upper − lower` per unit).
//! 3. When **holding** (only one side filled, position open):
//!    - Recompute band on the new bar.
//!    - **Re-band exit** (loss cap): if long and `new_upper < entry`, the
//!      band has shifted entirely below us — close at the new bar's open
//!      as a taker market exit. Symmetric for short on `new_lower > entry`.
//!    - **TP exit**: otherwise place a post-only limit at the new opposite
//!      edge (long → SELL at new upper). If the bar's range crosses it,
//!      filled and flat.
//!    - Else hold to the next bar.
//!
//! Fees: limit fills pay maker; re-band exits pay taker. No spread/slippage
//! modeling beyond the OHLC bar (touches assumed at the edge price).

use std::path::PathBuf;

use clap::Parser;
use polars::prelude::*;

#[derive(Parser, Debug)]
#[command(
    name = "backtest_bands",
    about = "Band-reversion strategy: post limits at compressed avg-HL band, re-band exit"
)]
struct Args {
    /// Parquet file produced by `download_klines`.
    #[arg(long, default_value = "./data/klines/eth_15m_90d.parquet")]
    data: PathBuf,
    /// Order size (base asset) per cycle.
    #[arg(long, default_value_t = 0.01_f64)]
    size: f64,
    /// Number of trailing candles used for band computation.
    #[arg(long, default_value_t = 10u32)]
    lookback: u32,
    /// Band width as a fraction of avg(high) − avg(low). Default 0.5 →
    /// quotes sit inside the historical range, biased toward fills.
    #[arg(long, default_value_t = 0.5_f64)]
    compress: f64,
    /// Fixed-spread mode (NaiveGrid). When > 0, ignores `--lookback` /
    /// `--compress` and uses `close × (1 ± spread_bps/2/10000)` for the
    /// edges instead of the rolling avg-HL band. `0` keeps band mode.
    #[arg(long, default_value_t = 0u32)]
    spread_bps: u32,
    /// Maker fee in bps (each limit fill).
    #[arg(long, default_value_t = 2u32)]
    maker_bps: u32,
    /// Taker fee in bps (each re-band market exit).
    #[arg(long, default_value_t = 5u32)]
    taker_bps: u32,
}

#[derive(Debug, Clone, Copy)]
struct Candle {
    open: f64,
    high: f64,
    low: f64,
    #[allow(dead_code)]
    close: f64,
    open_ts_ms: u64,
}

fn load_candles(path: &PathBuf) -> Result<Vec<Candle>, Box<dyn std::error::Error>> {
    let file = std::fs::File::open(path)?;
    let df = ParquetReader::new(file).finish()?;
    let ts = df.column("open_ts_ms")?.u64()?;
    let open = df.column("open")?.f64()?;
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let close = df.column("close")?.f64()?;
    let n = df.height();
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        out.push(Candle {
            open: open.get(i).ok_or("null open")?,
            high: high.get(i).ok_or("null high")?,
            low: low.get(i).ok_or("null low")?,
            close: close.get(i).ok_or("null close")?,
            open_ts_ms: ts.get(i).ok_or("null ts")?,
        });
    }
    Ok(out)
}

#[derive(Debug, Clone, Copy)]
struct Band {
    upper: f64,
    lower: f64,
}

fn compute_band(window: &[Candle], compress: f64) -> Band {
    let n = window.len() as f64;
    let avg_top: f64 = window.iter().map(|c| c.high).sum::<f64>() / n;
    let avg_bot: f64 = window.iter().map(|c| c.low).sum::<f64>() / n;
    let center = (avg_top + avg_bot) * 0.5;
    let half_width = (avg_top - avg_bot) * 0.5 * compress;
    Band {
        upper: center + half_width,
        lower: center - half_width,
    }
}

#[derive(Debug, Clone, Copy)]
enum Position {
    Flat,
    Long { entry: f64 },
    Short { entry: f64 },
}

#[derive(Default, Debug, Clone)]
struct Result_ {
    cycles: u64,
    round_trips: u64, // both sides filled in one candle
    wins: u64,
    losses: u64,
    open_at_end: bool,
    final_position: Option<(&'static str, f64)>,
    realized: f64,
    fees: f64,
    max_drawdown: f64,
}

impl Result_ {
    fn net(&self) -> f64 {
        self.realized - self.fees
    }
    fn win_rate(&self) -> f64 {
        let n = self.cycles as f64;
        if n == 0.0 { 0.0 } else { self.wins as f64 / n }
    }
}

fn simulate(candles: &[Candle], args: &Args) -> Result_ {
    let n = args.lookback as usize;
    let maker_rate = args.maker_bps as f64 / 10_000.0;
    let taker_rate = args.taker_bps as f64 / 10_000.0;
    let size = args.size;
    let fixed_spread_mode = args.spread_bps > 0;
    let half_spread = args.spread_bps as f64 / 2.0 / 10_000.0;

    let mut pos = Position::Flat;
    let mut res = Result_::default();

    let mut cum_pnl = 0.0f64;
    let mut peak = 0.0f64;

    // In fixed-spread mode, lookback can be 0 (we don't need history). Start
    // from i=1 so we always have a prior-bar close as the centerline.
    let start = if fixed_spread_mode { 1 } else { n };
    for i in start..candles.len() {
        let band = if fixed_spread_mode {
            // NaiveGrid-style: centered on PRIOR bar's close.
            let ref_price = candles[i - 1].close;
            Band {
                upper: ref_price * (1.0 + half_spread),
                lower: ref_price * (1.0 - half_spread),
            }
        } else {
            compute_band(&candles[i - n..i], args.compress)
        };
        let c = candles[i];

        // Helper to track drawdown after each PnL change.
        let mut update_dd = |realized: f64, fees: f64, peak: &mut f64, max_dd: &mut f64| {
            let cur = realized - fees;
            if cur > *peak {
                *peak = cur;
            }
            let dd = *peak - cur;
            if dd > *max_dd {
                *max_dd = dd;
            }
        };

        match pos {
            Position::Flat => {
                let hit_buy = c.low <= band.lower;
                let hit_sell = c.high >= band.upper;
                match (hit_buy, hit_sell) {
                    (true, true) => {
                        // Round trip in one bar — both edges hit. Realized =
                        // upper − lower per unit (always positive here).
                        let pnl = (band.upper - band.lower) * size;
                        let fees = (band.upper + band.lower) * size * maker_rate;
                        res.realized += pnl;
                        res.fees += fees;
                        res.cycles += 1;
                        res.round_trips += 1;
                        res.wins += 1;
                        cum_pnl = res.realized - res.fees;
                        if cum_pnl > peak {
                            peak = cum_pnl;
                        }
                        let dd = peak - cum_pnl;
                        if dd > res.max_drawdown {
                            res.max_drawdown = dd;
                        }
                    }
                    (true, false) => {
                        pos = Position::Long { entry: band.lower };
                        res.fees += band.lower * size * maker_rate;
                        update_dd(res.realized, res.fees, &mut peak, &mut res.max_drawdown);
                    }
                    (false, true) => {
                        pos = Position::Short { entry: band.upper };
                        res.fees += band.upper * size * maker_rate;
                        update_dd(res.realized, res.fees, &mut peak, &mut res.max_drawdown);
                    }
                    (false, false) => {}
                }
            }
            Position::Long { entry } => {
                // Re-band exit: band moved entirely below our entry → bail.
                if band.upper < entry {
                    let pnl = (c.open - entry) * size;
                    res.realized += pnl;
                    res.fees += c.open * size * taker_rate;
                    res.cycles += 1;
                    if pnl > 0.0 {
                        res.wins += 1
                    } else {
                        res.losses += 1
                    };
                    update_dd(res.realized, res.fees, &mut peak, &mut res.max_drawdown);
                    pos = Position::Flat;
                } else if c.high >= band.upper {
                    // TP at new upper.
                    let pnl = (band.upper - entry) * size;
                    res.realized += pnl;
                    res.fees += band.upper * size * maker_rate;
                    res.cycles += 1;
                    if pnl > 0.0 {
                        res.wins += 1
                    } else {
                        res.losses += 1
                    };
                    update_dd(res.realized, res.fees, &mut peak, &mut res.max_drawdown);
                    pos = Position::Flat;
                }
                // else hold
            }
            Position::Short { entry } => {
                if band.lower > entry {
                    let pnl = (entry - c.open) * size;
                    res.realized += pnl;
                    res.fees += c.open * size * taker_rate;
                    res.cycles += 1;
                    if pnl > 0.0 {
                        res.wins += 1
                    } else {
                        res.losses += 1
                    };
                    update_dd(res.realized, res.fees, &mut peak, &mut res.max_drawdown);
                    pos = Position::Flat;
                } else if c.low <= band.lower {
                    let pnl = (entry - band.lower) * size;
                    res.realized += pnl;
                    res.fees += band.lower * size * maker_rate;
                    res.cycles += 1;
                    if pnl > 0.0 {
                        res.wins += 1
                    } else {
                        res.losses += 1
                    };
                    update_dd(res.realized, res.fees, &mut peak, &mut res.max_drawdown);
                    pos = Position::Flat;
                }
            }
        }
    }

    match pos {
        Position::Flat => {}
        Position::Long { entry } => {
            res.open_at_end = true;
            res.final_position = Some(("long", entry));
        }
        Position::Short { entry } => {
            res.open_at_end = true;
            res.final_position = Some(("short", entry));
        }
    }
    res
}

fn print_summary(args: &Args, res: &Result_) {
    let mode = if args.spread_bps > 0 {
        format!("fixed-spread (naive-grid) {}bps", args.spread_bps)
    } else {
        format!(
            "avg-HL band lookback={} compress={:.2}",
            args.lookback, args.compress
        )
    };
    println!(
        "\n{} on klines  |  maker={}bps  taker={}bps  size={}",
        mode, args.maker_bps, args.taker_bps, args.size
    );
    println!("{}", "-".repeat(96));
    println!(
        "cycles    : {}  (round-trips in one bar: {})",
        res.cycles, res.round_trips
    );
    println!(
        "wins/loss : {} / {}  ({:.1}% win rate)",
        res.wins,
        res.losses,
        res.win_rate() * 100.0
    );
    println!("realized  : {:>12.4}", res.realized);
    println!("fees      : {:>12.4}", res.fees);
    println!("NET       : {:>12.4}", res.net());
    println!("max DD    : {:>12.4}", res.max_drawdown);
    if let Some((side, entry)) = res.final_position {
        println!("open posn : {} @ {:.4}", side, entry);
    } else {
        println!("open posn : (flat)");
    }
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
    let res = simulate(&candles, &args);
    print_summary(&args, &res);
    Ok(())
}
