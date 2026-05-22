//! Kline-scalp backtester.
//!
//! Simulates the KlineScalp strategy on historical OHLC data. At each
//! candle the strategy:
//!
//! 1. Computes momentum from the last N close prices.
//! 2. Quotes bid/ask at `close × (1 ∓ spread_bps/10000)` with momentum
//!    skew (aggressive side tightens, defensive side widens).
//! 3. Checks the candle range [low, high] for fills against open orders.
//! 4. Replaces filled orders with fresh quotes at the new close.
//!
//! Only one bid and one ask are kept open at any time (simple scalp, no
//! grid levels).

use std::path::{Path, PathBuf};

use clap::Parser;
use polars::prelude::*;

#[derive(Parser, Debug)]
#[command(name = "backtest_kline_scalp")]
struct Args {
    /// Parquet path.
    #[arg(long, default_value = "./data/klines/hype_1m_1d.parquet")]
    data: PathBuf,
    /// Comma-separated symbols. Resolves to `{data-dir}/{lower-base}{suffix}`.
    #[arg(long, default_value = "")]
    symbols: String,
    /// Base directory for --symbols lookup.
    #[arg(long, default_value = "./data/klines")]
    data_dir: PathBuf,
    /// Filename suffix after lowercase base.
    #[arg(long, default_value = "_1m_1d.parquet")]
    file_suffix: String,

    // ── Strategy params ──────────────────────────────────────────────
    #[arg(long, default_value_t = 50.0_f64)]
    notional: f64,
    #[arg(long, default_value_t = 12u32)]
    spread_bps: u32,
    #[arg(long, default_value_t = 3usize)]
    momentum_lookback: usize,
    #[arg(long, default_value_t = 10u32)]
    momentum_bps_threshold: u32,
    #[arg(long, default_value_t = 3.0_f64)]
    momentum_skew_mult: f64,
    #[arg(long, default_value_t = 2u32)]
    maker_bps: u32,
    /// Starting budget. 0 = stack-only mode.
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

/// Momentum signal from trailing closes.
fn momentum(candles: &[Candle], i: usize, lookback: usize, bps_threshold: f64) -> i8 {
    if i < lookback {
        return 0;
    }
    let past = candles[i - lookback].close;
    let current = candles[i].close;
    if past <= 0.0 || current <= 0.0 {
        return 0;
    }
    let change_bps = (current - past) / past * 10_000.0;
    if change_bps > bps_threshold {
        1
    } else if change_bps < -bps_threshold {
        -1
    } else {
        0
    }
}

/// Apply momentum skew to base bps distance on a side.
fn skewed_bps(side: Side, momentum: i8, base_bps: f64, skew_mult: f64) -> f64 {
    if momentum == 0 || skew_mult <= 1.0 {
        return base_bps;
    }
    match (side, momentum) {
        (Side::Sell, 1) => (base_bps / skew_mult).max(1.0), // tight ask
        (Side::Buy, 1) => base_bps * skew_mult,             // wide bid
        (Side::Buy, -1) => (base_bps / skew_mult).max(1.0), // tight bid
        (Side::Sell, -1) => base_bps * skew_mult,           // wide ask
        _ => base_bps,
    }
}

fn place_quote(price: f64, side: Side, notional: f64) -> Order {
    let qty = notional / price;
    Order {
        side,
        price,
        qty,
        fiat: notional,
    }
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

fn simulate(candles: &[Candle], args: &Args) -> Stats {
    if candles.is_empty() {
        return Stats::default();
    }
    let maker_rate = args.maker_bps as f64 / 10_000.0;
    let sp = args.spread_bps as f64;
    let lookback = args.momentum_lookback;
    let bps_threshold = args.momentum_bps_threshold as f64;
    let skew_mult = args.momentum_skew_mult;

    // Start cold — no orders until we have enough candles for momentum.
    let mut orders: Vec<Order> = Vec::new();
    let mut stats = Stats::default();
    let mut peak_mtm = 0.0_f64;

    for i in 0..candles.len() {
        let c = candles[i];

        // 1. Check fills on existing orders.
        // Process buys first (fill on low), then sells (fill on high).
        let mut filled_bid: Vec<Order> = Vec::new();
        let mut filled_ask: Vec<Order> = Vec::new();
        for o in &orders {
            match o.side {
                Side::Buy if c.low <= o.price => filled_bid.push(*o),
                Side::Sell if c.high >= o.price => filled_ask.push(*o),
                _ => {}
            }
        }
        for o in &filled_bid {
            fill_order(o, &mut stats, maker_rate);
        }
        for o in &filled_ask {
            fill_order(o, &mut stats, maker_rate);
        }
        // Remove filled orders.
        orders.retain(|o| match o.side {
            Side::Buy => c.low > o.price,
            Side::Sell => c.high < o.price,
        });

        // 2. Compute momentum.
        let m = momentum(candles, i, lookback, bps_threshold);

        // 3. Compute bid/ask prices with skew.
        let bid_bps = skewed_bps(Side::Buy, m, sp, skew_mult);
        let ask_bps = skewed_bps(Side::Sell, m, sp, skew_mult);

        // 4. Place or replace orders at the current close.
        let bid_price = c.close * (1.0 - bid_bps / 10_000.0);
        let ask_price = c.close * (1.0 + ask_bps / 10_000.0);
        // Replace all orders with fresh ones (cancel-and-place).
        orders.clear();
        orders.push(place_quote(bid_price, Side::Buy, args.notional));
        orders.push(place_quote(ask_price, Side::Sell, args.notional));

        // 5. Track drawdown.
        let mtm = stats.realized_usdt - stats.fees + stats.base_position * c.close;
        if mtm > peak_mtm {
            peak_mtm = mtm;
        }
        let dd = peak_mtm - mtm;
        if dd > stats.max_drawdown {
            stats.max_drawdown = dd;
        }
    }
    stats
}

fn print_summary(args: &Args, stats: &Stats, close: f64) {
    println!(
        "\nKlineScalp  |  notional=${}  spread={}bps  lookback={}  threshold={}bps  skew={}×  maker={}bps",
        args.notional,
        args.spread_bps,
        args.momentum_lookback,
        args.momentum_bps_threshold,
        args.momentum_skew_mult,
        args.maker_bps
    );
    println!("{}", "-".repeat(96));
    println!(
        "fills total       : {}  ({} buy, {} sell)",
        stats.fills, stats.buy_fills, stats.sell_fills
    );
    println!("realized cash-flow: {:>14.4}", stats.realized_usdt);
    println!("fees              : {:>14.4}", stats.fees);
    let realized_net = stats.realized_usdt - stats.fees;
    println!("realized − fees   : {:>14.4}", realized_net);
    println!("base position end : {:>14.6}", stats.base_position);
    let base_value = stats.base_position * close;
    println!(
        "base position USDT: {:>14.4}  (at last close {:.4})",
        base_value, close
    );
    let mtm = realized_net + base_value;
    println!("TOTAL MTM PnL     : {:>14.4}", mtm);
    println!("max drawdown      : {:>14.4}", stats.max_drawdown);
    if args.budget > 0.0 {
        let total = args.budget + mtm;
        let pct = mtm / args.budget * 100.0;
        println!(
            "TOTAL ACCT (budget ${:.2}): {:>10.4}  ({:+.2}%)",
            args.budget, total, pct
        );
    }
    let total_fills = stats.buy_fills + stats.sell_fills;
    let avg_scalp = if total_fills > 0 {
        stats.realized_usdt / total_fills as f64
    } else {
        0.0
    };
    println!("avg scalp PnL/fill: {:>14.4}", avg_scalp);
}

fn print_table_header(args: &Args) {
    println!(
        "\nKlineScalp sweep  |  notional=${}  spread={}bps  lookback={}  threshold={}bps  skew={}×",
        args.notional,
        args.spread_bps,
        args.momentum_lookback,
        args.momentum_bps_threshold,
        args.momentum_skew_mult
    );
    println!("{}", "-".repeat(120));
    println!(
        "{:<14} {:>7} {:>12} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "SYMBOL", "FILLS", "REAL-FEE", "BASE_USDT", "MTM", "DD", "ACCT%", "AVG$"
    );
}

fn print_table_row(sym: &str, stats: &Stats, close: f64, budget: f64) {
    let realized_net = stats.realized_usdt - stats.fees;
    let base_value = stats.base_position * close;
    let mtm = realized_net + base_value;
    let acct_pct = if budget > 0.0 {
        format!("{:+.2}%", mtm / budget * 100.0)
    } else {
        "-".to_string()
    };
    let total_fills = stats.buy_fills + stats.sell_fills;
    let avg_usd = if total_fills > 0 {
        stats.realized_usdt / total_fills as f64
    } else {
        0.0
    };
    println!(
        "{:<14} {:>7} {:>12.4} {:>10.4} {:>10.4} {:>10.4} {:>10} {:>10.4}",
        sym, stats.fills, realized_net, base_value, mtm, stats.max_drawdown, acct_pct, avg_usd
    );
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

fn run_one(path: &Path, args: &Args) -> Result<(Stats, f64), Box<dyn std::error::Error>> {
    let candles = load_candles(path)?;
    let close = candles.last().map(|c| c.close).unwrap_or(0.0);
    let stats = simulate(&candles, args);
    Ok((stats, close))
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
        let close = candles.last().map(|c| c.close).unwrap_or(0.0);
        let stats = simulate(&candles, &args);
        print_summary(&args, &stats, close);
        return Ok(());
    }

    print_table_header(&args);
    let mut totals_mtm = 0.0;
    let mut totals_real_fee = 0.0;
    let mut totals_dd = 0.0;
    let mut wins = 0usize;
    for sym in &symbols {
        let path = symbol_to_path(&args.data_dir, sym, &args.file_suffix);
        match run_one(&path, &args) {
            Ok((stats, close)) => {
                let realized_net = stats.realized_usdt - stats.fees;
                let mtm = realized_net + stats.base_position * close;
                if mtm > 0.0 {
                    wins += 1;
                }
                totals_mtm += mtm;
                totals_real_fee += realized_net;
                totals_dd += stats.max_drawdown;
                print_table_row(sym, &stats, close, args.budget);
            }
            Err(e) => {
                eprintln!("{sym}: load failed ({} — {e})", path.display());
            }
        }
    }
    let n = symbols.len() as f64;
    println!("{}", "-".repeat(120));
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
