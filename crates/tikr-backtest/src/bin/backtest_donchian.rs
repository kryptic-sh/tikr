//! Turtle-style Donchian-channel breakout swing trader against OHLCV klines.
//!
//! Strategy (classic turtle rules, "next bar open" fill approximation):
//!
//! - **Entry** (checked at close of bar `t`, filled at open of bar `t+1`):
//!   - Go LONG  when `close[t] > max(high[t-1..t-N])`  (entry window N, default 20).
//!   - Go SHORT when `close[t] < min(low[t-1..t-N])`.
//! - **Exit** (same timing):
//!   - Long  exits when `close[t] < min(low[t-1..t-M])`  (exit window M, default 10).
//!   - Short exits when `close[t] > max(high[t-1..t-M])`.
//! - **Trailing stop** (optional, `--trailing-stop-atr-mult > 0`):
//!   - Long  stop: `low <= peak_since_entry - K × ATR(14)`.
//!   - Short stop: `high >= trough_since_entry + K × ATR(14)`.
//!   - Fills at the stop level (conservative; real-world might gap through).
//! - **Resampling**: 1-min input rows are grouped into `--resample-period`-minute
//!   bars before the strategy runs (default 1440 = daily).
//! - **Fees**: taker-only (market orders on breakout); entry + exit each pay
//!   `--taker-bps`.
//! - One position at a time. Long-only mode (`--long-only`) skips short signals.

use std::path::{Path, PathBuf};

use clap::Parser;
use polars::prelude::*;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser, Debug)]
#[command(
    name = "backtest_donchian",
    about = "Turtle-style Donchian-channel breakout swing trader against OHLCV klines"
)]
struct Args {
    /// Single parquet path. Mutually exclusive with `--symbols`.
    #[arg(long, default_value = "./data/klines/btc_1m_1y.parquet")]
    data: PathBuf,
    /// Comma-separated symbols (e.g. `BTCUSDT,ETHUSDT`).
    /// Resolves each to `{data-dir}/{lower-strip-USDT}{file-suffix}`.
    /// Overrides `--data` when set.
    #[arg(long, default_value = "")]
    symbols: String,
    /// Base directory for `--symbols` lookup.
    #[arg(long, default_value = "./data/klines/1y")]
    data_dir: PathBuf,
    /// Filename suffix after the lowercase base symbol.
    #[arg(long, default_value = "_1m_1y.parquet")]
    file_suffix: String,
    /// Donchian entry window: bars to look back for breakout high/low (N).
    #[arg(long, default_value_t = 20u32)]
    entry_window: u32,
    /// Donchian exit window: bars to look back for channel exit (M).
    #[arg(long, default_value_t = 10u32)]
    exit_window: u32,
    /// Resample period in minutes (1 = no resample, 1440 = daily).
    /// Input 1m candles are aggregated into this many-minute bars.
    #[arg(long, default_value_t = 1440u32)]
    resample_period: u32,
    /// ATR look-back (bars, post-resample). Used for trailing stop.
    #[arg(long, default_value_t = 14u32)]
    atr_period: u32,
    /// Trailing stop multiplier applied to ATR. 0 = disabled.
    #[arg(long, default_value_t = 0.0_f64)]
    trailing_stop_atr_mult: f64,
    /// Fixed fiat notional per entry (USDT). Qty = notional / fill_price.
    #[arg(long, default_value_t = 100.0_f64)]
    notional: f64,
    /// Taker fee in bps. Entry + exit each pay this.
    #[arg(long, default_value_t = 5u32)]
    taker_bps: u32,
    /// Starting USDT budget for account-level reporting. `0` disables.
    #[arg(long, default_value_t = 0.0_f64)]
    budget: f64,
    /// When true, skip short signals (long-only mode).
    #[arg(long, default_value_t = false)]
    long_only: bool,
}

// ---------------------------------------------------------------------------
// Raw candle (from parquet)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct RawCandle {
    open_ts_ms: u64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
}

fn load_raw_candles(path: &Path) -> Result<Vec<RawCandle>, Box<dyn std::error::Error>> {
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
        out.push(RawCandle {
            open_ts_ms: ts.get(i).ok_or("null open_ts_ms")?,
            open: open.get(i).ok_or("null open")?,
            high: high.get(i).ok_or("null high")?,
            low: low.get(i).ok_or("null low")?,
            close: close.get(i).ok_or("null close")?,
        });
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Resampled candle (OHLC rollup)
// ---------------------------------------------------------------------------

/// OHLC-rolled bar for the strategy to operate on.
#[derive(Debug, Clone, Copy)]
struct Bar {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
}

/// Aggregate `raw` 1-min candles into `period`-minute bars using OHLC rollup.
/// Groups are formed by sequential chunks of `period` rows (oldest first).
/// The last partial group is dropped if it has fewer than `period` rows,
/// keeping bars uniform — this avoids a thin bar biasing the channel.
fn resample(raw: &[RawCandle], period: u32) -> Vec<Bar> {
    if period <= 1 {
        return raw
            .iter()
            .map(|r| Bar {
                open: r.open,
                high: r.high,
                low: r.low,
                close: r.close,
            })
            .collect();
    }
    let p = period as usize;
    let full_chunks = raw.len() / p;
    let mut bars = Vec::with_capacity(full_chunks);
    for chunk_idx in 0..full_chunks {
        let start = chunk_idx * p;
        let end = start + p;
        let slice = &raw[start..end];
        let open = slice[0].open;
        let high = slice
            .iter()
            .map(|r| r.high)
            .fold(f64::NEG_INFINITY, f64::max);
        let low = slice.iter().map(|r| r.low).fold(f64::INFINITY, f64::min);
        let close = slice[p - 1].close;
        bars.push(Bar {
            open,
            high,
            low,
            close,
        });
    }
    bars
}

// ---------------------------------------------------------------------------
// ATR helper (simple MA over true range, post-resample bars)
// ---------------------------------------------------------------------------

/// Simple-MA ATR(period) at bar index `idx` (requires idx >= period).
/// Returns None if insufficient history.
fn compute_atr(bars: &[Bar], idx: usize, period: usize) -> Option<f64> {
    if idx < period {
        return None;
    }
    let mut sum = 0.0;
    for k in (idx - period + 1)..=idx {
        let b = &bars[k];
        let prev_close = bars[k - 1].close;
        let tr = (b.high - b.low)
            .max((b.high - prev_close).abs())
            .max((b.low - prev_close).abs());
        sum += tr;
    }
    Some(sum / period as f64)
}

// ---------------------------------------------------------------------------
// Position state machine
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum Position {
    Flat,
    Long {
        entry: f64,
        qty: f64,
        /// Highest close seen since entry (for trailing stop tracking).
        peak: f64,
    },
    Short {
        entry: f64,
        qty: f64,
        /// Lowest close seen since entry (for trailing stop tracking).
        trough: f64,
    },
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

#[derive(Default, Debug, Clone)]
struct Stats {
    trades: u64,
    wins: u64,
    losses: u64,
    realized_usdt: f64,
    fees: f64,
    max_drawdown: f64,
    /// Signed net base position (positive = net long, negative = net short).
    base_position: f64,
    // Win/loss sums for averages.
    sum_wins: f64,
    sum_losses: f64,
    largest_win: f64,
    largest_loss: f64,
}

impl Stats {
    fn net(&self) -> f64 {
        self.realized_usdt - self.fees
    }
}

// ---------------------------------------------------------------------------
// Core simulation
// ---------------------------------------------------------------------------

fn simulate(bars: &[Bar], args: &Args) -> (Stats, f64) {
    let n_entry = args.entry_window as usize;
    let n_exit = args.exit_window as usize;
    let atr_period = args.atr_period as usize;
    let taker_rate = args.taker_bps as f64 / 10_000.0;
    let notional = args.notional;
    let ts_mult = args.trailing_stop_atr_mult;
    let ts_enabled = ts_mult > 0.0;

    // Need enough bars for both entry/exit windows and (if trailing stop) ATR.
    let warmup = n_entry
        .max(n_exit)
        .max(if ts_enabled { atr_period } else { 0 });
    if bars.len() <= warmup {
        return (Stats::default(), 0.0);
    }

    let mut pos = Position::Flat;
    let mut stats = Stats::default();
    let mut peak_net = 0.0_f64;

    // Strategy: signal fired at close of bar `i`, entry/exit filled at open of bar `i+1`.
    // We iterate bar `i` from `warmup` to `len-2`, then use bar `i+1` for the fill price.
    for i in warmup..bars.len().saturating_sub(1) {
        let bar = &bars[i];
        let fill_bar = &bars[i + 1]; // next bar; fill at its open

        // --- Donchian channel values (exclude current bar from lookback per spec) ---
        // Entry: max(high[t-1..t-N]), min(low[t-1..t-N])
        // We look back from i-1 inclusive, N bars, so indices [i-N .. i-1].
        let entry_highs = &bars[i.saturating_sub(n_entry)..i];
        let entry_lows = entry_highs;
        let dc_entry_high = entry_highs
            .iter()
            .map(|b| b.high)
            .fold(f64::NEG_INFINITY, f64::max);
        let dc_entry_low = entry_lows
            .iter()
            .map(|b| b.low)
            .fold(f64::INFINITY, f64::min);

        // Exit: max(high[t-1..t-M]), min(low[t-1..t-M])
        let exit_highs = &bars[i.saturating_sub(n_exit)..i];
        let exit_lows = exit_highs;
        let dc_exit_high = exit_highs
            .iter()
            .map(|b| b.high)
            .fold(f64::NEG_INFINITY, f64::max);
        let dc_exit_low = exit_lows
            .iter()
            .map(|b| b.low)
            .fold(f64::INFINITY, f64::min);

        // Optional ATR for trailing stop (None during warm-up).
        let atr = if ts_enabled {
            compute_atr(bars, i, atr_period)
        } else {
            None
        };

        // Fill price for any entry/exit triggered this bar.
        let fill_price = fill_bar.open;

        match pos {
            Position::Flat => {
                // Entry breakout signal.
                if bar.close > dc_entry_high {
                    // Long breakout.
                    let qty = notional / fill_price;
                    let fee = fill_price * qty * taker_rate;
                    stats.fees += fee;
                    stats.base_position += qty;
                    pos = Position::Long {
                        entry: fill_price,
                        qty,
                        peak: bar.close,
                    };
                } else if !args.long_only && bar.close < dc_entry_low {
                    // Short breakout.
                    let qty = notional / fill_price;
                    let fee = fill_price * qty * taker_rate;
                    stats.fees += fee;
                    stats.base_position -= qty;
                    pos = Position::Short {
                        entry: fill_price,
                        qty,
                        trough: bar.close,
                    };
                }
            }

            Position::Long { entry, qty, peak } => {
                // Update peak on current bar close.
                let new_peak = peak.max(bar.close);

                // Check trailing stop first (intra-bar low).
                let trailing_exit = ts_enabled
                    && atr.is_some()
                    && fill_bar.low <= new_peak - ts_mult * atr.unwrap();

                // Check Donchian exit signal (close < exit channel low).
                let channel_exit = bar.close < dc_exit_low;

                if trailing_exit || channel_exit {
                    // Trailing stop fills at stop level; channel exit fills at next open.
                    let exit_price = if trailing_exit && !channel_exit {
                        // Stop level (may be worse than open if gap-down, but use
                        // the stop level as an optimistic fill — consistent with
                        // other kline bins that don't model slippage).
                        (new_peak - ts_mult * atr.unwrap()).max(fill_bar.low)
                    } else {
                        fill_price
                    };
                    let gross = (exit_price - entry) * qty;
                    stats.realized_usdt += gross;
                    let exit_fee = exit_price * qty * taker_rate;
                    stats.fees += exit_fee;
                    stats.base_position -= qty;
                    stats.trades += 1;
                    let net_trade = gross - exit_fee;
                    if net_trade > 0.0 {
                        stats.wins += 1;
                        stats.sum_wins += net_trade;
                        if net_trade > stats.largest_win {
                            stats.largest_win = net_trade;
                        }
                    } else {
                        stats.losses += 1;
                        stats.sum_losses += net_trade;
                        if net_trade < stats.largest_loss {
                            stats.largest_loss = net_trade;
                        }
                    }
                    pos = Position::Flat;
                } else {
                    // Update peak in state.
                    pos = Position::Long {
                        entry,
                        qty,
                        peak: new_peak,
                    };
                }
            }

            Position::Short { entry, qty, trough } => {
                // Update trough on current bar close.
                let new_trough = trough.min(bar.close);

                // Trailing stop: short exits if high >= trough + K×ATR.
                let trailing_exit = ts_enabled
                    && atr.is_some()
                    && fill_bar.high >= new_trough + ts_mult * atr.unwrap();

                // Donchian exit: close > exit channel high.
                let channel_exit = bar.close > dc_exit_high;

                if trailing_exit || channel_exit {
                    let exit_price = if trailing_exit && !channel_exit {
                        (new_trough + ts_mult * atr.unwrap()).min(fill_bar.high)
                    } else {
                        fill_price
                    };
                    let gross = (entry - exit_price) * qty;
                    stats.realized_usdt += gross;
                    let exit_fee = exit_price * qty * taker_rate;
                    stats.fees += exit_fee;
                    stats.base_position += qty;
                    stats.trades += 1;
                    let net_trade = gross - exit_fee;
                    if net_trade > 0.0 {
                        stats.wins += 1;
                        stats.sum_wins += net_trade;
                        if net_trade > stats.largest_win {
                            stats.largest_win = net_trade;
                        }
                    } else {
                        stats.losses += 1;
                        stats.sum_losses += net_trade;
                        if net_trade < stats.largest_loss {
                            stats.largest_loss = net_trade;
                        }
                    }
                    pos = Position::Flat;
                } else {
                    pos = Position::Short {
                        entry,
                        qty,
                        trough: new_trough,
                    };
                }
            }
        }

        // Mark-to-market equity curve for drawdown.
        let mtm_pos = match pos {
            Position::Long { qty, .. } => qty * fill_bar.open,
            Position::Short { qty, .. } => -(qty * fill_bar.open),
            Position::Flat => 0.0,
        };
        let equity = stats.net() + mtm_pos;
        if equity > peak_net {
            peak_net = equity;
        }
        let dd = peak_net - equity;
        if dd > stats.max_drawdown {
            stats.max_drawdown = dd;
        }
    }

    let last_close = bars.last().map(|b| b.close).unwrap_or(0.0);
    (stats, last_close)
}

// ---------------------------------------------------------------------------
// Output helpers
// ---------------------------------------------------------------------------

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
    let long_only_str = if args.long_only { " LONG-ONLY" } else { "" };
    println!(
        "\nDonchian breakout{long_only_str}  |  entry={}  exit={}  resample={}m  \
         taker={}bps  ts_mult={:.1}  notional=${}",
        args.entry_window,
        args.exit_window,
        args.resample_period,
        args.taker_bps,
        args.trailing_stop_atr_mult,
        args.notional
    );
    println!("{}", "-".repeat(96));
    let win_rate = if stats.trades == 0 {
        0.0
    } else {
        stats.wins as f64 / stats.trades as f64 * 100.0
    };
    println!(
        "trades            : {}  ({} wins / {} losses  {:.1}% win rate)",
        stats.trades, stats.wins, stats.losses, win_rate
    );
    let avg_win = if stats.wins == 0 {
        0.0
    } else {
        stats.sum_wins / stats.wins as f64
    };
    let avg_loss = if stats.losses == 0 {
        0.0
    } else {
        stats.sum_losses / stats.losses as f64
    };
    println!(
        "avg win           : {:>14.4}  avg loss: {:>14.4}",
        avg_win, avg_loss
    );
    println!(
        "largest win       : {:>14.4}  largest loss: {:>10.4}",
        stats.largest_win, stats.largest_loss
    );
    println!("gross realized    : {:>14.4}", stats.realized_usdt);
    println!("fees              : {:>14.4}", stats.fees);
    println!("realized − fees   : {:>14.4}", stats.net());
    let base_value = stats.base_position * last_close;
    println!(
        "open position USDT: {:>14.4}  (at last close {:.4})",
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
    let long_only_str = if args.long_only { " LONG-ONLY" } else { "" };
    println!(
        "\nDonchian sweep{long_only_str}  |  entry={}  exit={}  resample={}m  \
         taker={}bps  ts_mult={:.1}  notional=${}",
        args.entry_window,
        args.exit_window,
        args.resample_period,
        args.taker_bps,
        args.trailing_stop_atr_mult,
        args.notional
    );
    println!("{}", "-".repeat(96));
    println!(
        "{:<14} {:>7} {:>8} {:>12} {:>10} {:>10} {:>10} {:>10}",
        "SYMBOL", "TRADES", "WIN%", "REAL-FEE", "BASE_USDT", "MTM", "DD", "ACCT%"
    );
}

fn print_table_row(sym: &str, stats: &Stats, last_close: f64, budget: f64) {
    let win_pct = if stats.trades == 0 {
        0.0
    } else {
        stats.wins as f64 / stats.trades as f64 * 100.0
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
        stats.trades,
        win_pct,
        stats.net(),
        base_value,
        mtm,
        stats.max_drawdown,
        acct
    );
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    if args.resample_period == 0 {
        return Err("--resample-period must be >= 1".into());
    }

    let symbols = parse_symbols(&args.symbols);

    if symbols.is_empty() {
        // Single-file mode.
        let raw = load_raw_candles(&args.data)?;
        eprintln!(
            "loaded {} raw candles from {}",
            raw.len(),
            args.data.display()
        );
        if !raw.is_empty() {
            let span_ms = raw.last().unwrap().open_ts_ms - raw[0].open_ts_ms;
            let span_d = span_ms as f64 / (24.0 * 60.0 * 60_000.0);
            eprintln!("span: {:.1} days", span_d);
        }
        let bars = resample(&raw, args.resample_period);
        eprintln!(
            "resampled to {} bars (period={}m)",
            bars.len(),
            args.resample_period
        );
        let (stats, last_close) = simulate(&bars, &args);
        print_single_summary(&args, &stats, last_close);
        return Ok(());
    }

    // Multi-symbol table mode.
    print_table_header(&args);
    let mut totals_mtm = 0.0;
    let mut totals_real_fee = 0.0;
    let mut totals_dd = 0.0;
    let mut wins = 0usize;
    for sym in &symbols {
        let path = symbol_to_path(&args.data_dir, sym, &args.file_suffix);
        let raw = match load_raw_candles(&path) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("{sym}: load failed ({} — {e})", path.display());
                continue;
            }
        };
        let bars = resample(&raw, args.resample_period);
        let (stats, last_close) = simulate(&bars, &args);
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
