//! ATR-adaptive layered grid scalper against OHLCV klines.
//!
//! Like `backtest_layered_grid` but grid spacing is derived from rolling ATR
//! instead of fixed bps. This lets the grid automatically widen in volatile
//! regimes and tighten in choppy ones.
//!
//! **ATR computation** (Wilder's definition):
//! - True Range(i) = max(high − low, |high − prev_close|, |low − prev_close|)
//! - ATR(N) = simple moving average of TR over N periods.
//!
//! **Grid placement**:
//! - spacing = K × ATR  (absolute price units, NOT bps)
//! - Levels (k = 0..levels): buy at mid − (k+1)×spacing, sell at mid + (k+1)×spacing.
//!
//! **Re-entry rule** (mirrors layered_grid rolling-ladder):
//! - BUY fills at P → place SELL at P + spacing (current ATR-scaled).
//! - SELL fills at P → place BUY at P − spacing (current ATR-scaled).
//!
//! Fees: every limit fill pays maker fee on the fiat notional.

use std::path::{Path, PathBuf};

use clap::Parser;
use polars::prelude::*;

#[derive(Parser, Debug)]
#[command(
    name = "backtest_atr_grid",
    about = "ATR-adaptive layered grid scalper: spacing = K × ATR(N)"
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
    /// ATR look-back period (number of candles).
    #[arg(long, default_value_t = 14usize)]
    atr_period: usize,
    /// Grid spacing multiplier: spacing = atr_mult × ATR.
    #[arg(long, default_value_t = 0.5_f64)]
    atr_mult: f64,
    /// Number of levels per side (total orders = 2 × levels).
    #[arg(long, default_value_t = 1usize)]
    levels: usize,
    /// Fixed fiat notional per order. Coin qty = notional / price.
    #[arg(long, default_value_t = 100.0_f64)]
    notional: f64,
    /// Maker fee in bps. Every limit fill pays this.
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

/// Compute a simple-average ATR over the last `period` candles (i > 0).
/// Returns None when there is insufficient history.
fn compute_atr(candles: &[Candle], idx: usize, period: usize) -> Option<f64> {
    if idx < period {
        return None;
    }
    let mut sum = 0.0;
    for k in (idx - period + 1)..=idx {
        let c = &candles[k];
        let prev_close = candles[k - 1].close;
        let tr = (c.high - c.low)
            .max((c.high - prev_close).abs())
            .max((c.low - prev_close).abs());
        sum += tr;
    }
    Some(sum / period as f64)
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy)]
struct Order {
    side: Side,
    price: f64,
    qty: f64,
    fiat: f64,
}

#[derive(Default, Debug)]
struct Stats {
    fills: u64,
    buy_fills: u64,
    sell_fills: u64,
    realized_usdt: f64,
    fees: f64,
    base_position: f64,
    fiat_spent: f64,
    fiat_received: f64,
    max_drawdown: f64,
    max_base_position: f64,
}

fn place_initial_orders(mid: f64, spacing: f64, levels: usize, notional: f64) -> Vec<Order> {
    let mut orders = Vec::with_capacity(levels * 2);
    for k in 0..levels {
        let offset = spacing * (k + 1) as f64;
        let buy_price = (mid - offset).max(1e-12);
        let sell_price = mid + offset;
        orders.push(Order {
            side: Side::Buy,
            price: buy_price,
            qty: notional / buy_price,
            fiat: notional,
        });
        orders.push(Order {
            side: Side::Sell,
            price: sell_price,
            qty: notional / sell_price,
            fiat: notional,
        });
    }
    orders
}

fn fill_order(o: &Order, stats: &mut Stats, maker_rate: f64) {
    let fee = o.fiat * maker_rate;
    stats.fees += fee;
    stats.fills += 1;
    match o.side {
        Side::Buy => {
            stats.buy_fills += 1;
            stats.base_position += o.qty;
            stats.fiat_spent += o.fiat;
        }
        Side::Sell => {
            stats.sell_fills += 1;
            stats.base_position -= o.qty;
            stats.fiat_received += o.fiat;
        }
    }
    stats.realized_usdt = stats.fiat_received - stats.fiat_spent;
    if stats.base_position > stats.max_base_position {
        stats.max_base_position = stats.base_position;
    }
}

fn simulate(candles: &[Candle], args: &Args) -> (Stats, Vec<Order>, f64) {
    // Need at least atr_period + 1 candles (prev_close available from index 1).
    let warmup = args.atr_period + 1;
    if candles.len() <= warmup {
        return (Stats::default(), Vec::new(), 0.0);
    }

    let maker_rate = args.maker_bps as f64 / 10_000.0;
    let notional = args.notional;

    // Cold start: use first valid ATR after warm-up for initial spacing.
    let first_atr = compute_atr(candles, warmup, args.atr_period).unwrap_or(1.0);
    let init_spacing = args.atr_mult * first_atr;
    let mid = candles[warmup].close;
    let mut orders = place_initial_orders(mid, init_spacing, args.levels, notional);

    let mut stats = Stats::default();
    let mut peak_mtm = 0.0_f64;

    for i in (warmup + 1)..candles.len() {
        let c = &candles[i];
        // Current ATR — used for any re-entry orders placed this step.
        let cur_atr = compute_atr(candles, i, args.atr_period).unwrap_or(first_atr);
        let spacing = args.atr_mult * cur_atr;

        // Collect fills (descending index for safe removal).
        let mut filled: Vec<(usize, Order)> = Vec::new();
        for (idx, o) in orders.iter().enumerate() {
            let hit = match o.side {
                Side::Buy => c.low <= o.price,
                Side::Sell => c.high >= o.price,
            };
            if hit {
                filled.push((idx, *o));
            }
        }
        for (idx, _) in filled.iter().rev() {
            orders.remove(*idx);
        }
        for (_, fo) in &filled {
            fill_order(fo, &mut stats, maker_rate);

            // Re-entry: opposite side at fill_price ± current spacing.
            let (new_side, new_price) = match fo.side {
                Side::Buy => (Side::Sell, fo.price + spacing),
                Side::Sell => (Side::Buy, (fo.price - spacing).max(1e-12)),
            };
            orders.push(Order {
                side: new_side,
                price: new_price,
                qty: notional / new_price,
                fiat: notional,
            });
        }

        // Mark-to-market drawdown.
        let mtm = stats.realized_usdt - stats.fees + stats.base_position * c.close;
        if mtm > peak_mtm {
            peak_mtm = mtm;
        }
        let dd = peak_mtm - mtm;
        if dd > stats.max_drawdown {
            stats.max_drawdown = dd;
        }
    }

    let last_close = candles.last().map(|c| c.close).unwrap_or(0.0);
    (stats, orders, last_close)
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

fn print_single_summary(args: &Args, stats: &Stats, orders: &[Order], last_close: f64) {
    println!(
        "\nATR-grid  |  atr_period={}  atr_mult={:.2}  levels={}  notional=${}  maker={}bps",
        args.atr_period, args.atr_mult, args.levels, args.notional, args.maker_bps
    );
    println!("{}", "-".repeat(96));
    println!(
        "fills total       : {}  ({} buy, {} sell)",
        stats.fills, stats.buy_fills, stats.sell_fills
    );
    println!(
        "realized cash-flow: {:>14.4}  (fiat_received − fiat_spent)",
        stats.realized_usdt
    );
    println!("fees              : {:>14.4}", stats.fees);
    let net = stats.realized_usdt - stats.fees;
    println!("realized − fees   : {:>14.4}", net);
    println!(
        "base position end : {:>14.6}  (max during run: {:.6})",
        stats.base_position, stats.max_base_position
    );
    let base_value = stats.base_position * last_close;
    println!(
        "base position USDT: {:>14.4}  (at last close {:.4})",
        base_value, last_close
    );
    let mtm = net + base_value;
    println!("TOTAL MTM PnL     : {:>14.4}", mtm);
    println!("max drawdown      : {:>14.4}", stats.max_drawdown);
    println!(
        "open orders end   : {}  ({} buys, {} sells)",
        orders.len(),
        orders.iter().filter(|o| o.side == Side::Buy).count(),
        orders.iter().filter(|o| o.side == Side::Sell).count(),
    );
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
        "\nATR-grid sweep  |  atr_period={}  atr_mult={:.2}  levels={}  notional=${}  maker={}bps",
        args.atr_period, args.atr_mult, args.levels, args.notional, args.maker_bps
    );
    println!("{}", "-".repeat(96));
    println!(
        "{:<14} {:>7} {:>12} {:>10} {:>10} {:>10} {:>10}",
        "SYMBOL", "FILLS", "REAL-FEE", "BASE_USDT", "MTM", "DD", "ACCT%"
    );
}

fn print_table_row(sym: &str, stats: &Stats, last_close: f64, budget: f64) {
    let net = stats.realized_usdt - stats.fees;
    let base_value = stats.base_position * last_close;
    let mtm = net + base_value;
    let acct = if budget > 0.0 {
        format!("{:+.2}%", mtm / budget * 100.0)
    } else {
        "-".to_string()
    };
    println!(
        "{:<14} {:>7} {:>12.4} {:>10.4} {:>10.4} {:>10.4} {:>10}",
        sym, stats.fills, net, base_value, mtm, stats.max_drawdown, acct
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
        let (stats, orders, last_close) = simulate(&candles, &args);
        print_single_summary(&args, &stats, &orders, last_close);
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
        let (stats, _, last_close) = simulate(&candles, &args);
        let net = stats.realized_usdt - stats.fees;
        let mtm = net + stats.base_position * last_close;
        if mtm > 0.0 {
            wins += 1;
        }
        totals_mtm += mtm;
        totals_real_fee += net;
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
