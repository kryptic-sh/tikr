//! Rolling-mid naive grid against OHLCV klines.
//!
//! Mirrors the new [`NaiveGrid`] strategy live-mode behavior:
//!
//! - Cold start: place 1 buy at `mid × (1 − spread_bps/10000)` and 1 sell at
//!   `mid × (1 + spread_bps/10000)`.
//! - On fill: shift the anchor halfway toward the fill price
//!   (`new_mid = (prev_mid + fill_price) / 2`), cancel any remaining open
//!   order, place a fresh pair around `new_mid ± spread_bps`.
//!
//! Each scalp captures `2 × spread_bps` price action minus `2 × maker_bps`
//! fees. With the midpoint-shift rule, the anchor drifts in the direction
//! of fills so consecutive same-side fills extend deeper while the opposite
//! side stays close for a TP.

use std::path::PathBuf;

use clap::Parser;
use polars::prelude::*;

#[derive(Parser, Debug)]
#[command(
    name = "backtest_naive_rolling",
    about = "Rolling-mid naive grid (1 order per side) backtest"
)]
struct Args {
    /// Single parquet path. Mutually exclusive with `--symbols`.
    #[arg(long, default_value = "./data/klines/eth_1m_90d.parquet")]
    data: PathBuf,
    /// Comma-separated symbols (e.g. `BTCUSDT,ETHUSDT,SAHARAUSDT`).
    /// Resolves each to `{data-dir}/{lower-strip-USDT}{file-suffix}`.
    #[arg(long, default_value = "")]
    symbols: String,
    /// Base directory for `--symbols` lookup.
    #[arg(long, default_value = "./data/klines")]
    data_dir: PathBuf,
    /// Filename suffix after the lowercase base.
    #[arg(long, default_value = "_1m_1d.parquet")]
    file_suffix: String,
    /// Fixed fiat notional per order.
    #[arg(long, default_value_t = 100.0_f64)]
    notional_per_order: f64,
    /// Half-spread from mid in bps. Buy at `mid × (1 − spread/10000)`,
    /// sell at `mid × (1 + spread/10000)`.
    #[arg(long, default_value_t = 5u32)]
    spread_bps: u32,
    /// Maker fee in bps.
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
    open: f64,
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
}

#[derive(Debug, Clone, Copy)]
struct OpenPair {
    buy_price: f64,
    sell_price: f64,
}

fn place_pair(mid: f64, spread_bps: u32) -> OpenPair {
    let offset = spread_bps as f64 / 10_000.0;
    OpenPair {
        buy_price: mid * (1.0 - offset),
        sell_price: mid * (1.0 + offset),
    }
}

fn record_buy_fill(stats: &mut Stats, price: f64, notional: f64, maker_rate: f64) {
    let qty = notional / price;
    let fee = notional * maker_rate;
    stats.fills += 1;
    stats.buy_fills += 1;
    stats.fees += fee;
    stats.base_position += qty;
    stats.fiat_spent += notional;
    stats.realized_usdt = stats.fiat_received - stats.fiat_spent;
}

fn record_sell_fill(stats: &mut Stats, price: f64, notional: f64, maker_rate: f64) {
    let qty = notional / price;
    let fee = notional * maker_rate;
    stats.fills += 1;
    stats.sell_fills += 1;
    stats.fees += fee;
    stats.base_position -= qty;
    stats.fiat_received += notional;
    stats.realized_usdt = stats.fiat_received - stats.fiat_spent;
}

fn simulate(candles: &[Candle], args: &Args) -> (Stats, f64) {
    if candles.is_empty() {
        return (Stats::default(), 0.0);
    }
    let maker_rate = args.maker_bps as f64 / 10_000.0;
    let notional = args.notional_per_order;

    let mut mid = candles[0].open;
    let mut pair = place_pair(mid, args.spread_bps);
    let mut stats = Stats::default();
    let mut peak_pnl = 0.0_f64;

    for c in candles.iter().skip(1) {
        // Process buys first (price descended into our buy) then sells —
        // a single candle range can touch both. We model that as two
        // consecutive scalps within the same bar.
        loop {
            let buy_hit = c.low <= pair.buy_price;
            let sell_hit = c.high >= pair.sell_price;
            if !buy_hit && !sell_hit {
                break;
            }
            // Pick the leg closer to the candle's open as the first hit —
            // a fair approximation of intra-bar path order.
            let buy_first = if buy_hit && sell_hit {
                (c.open - pair.buy_price).abs() < (pair.sell_price - c.open).abs()
            } else {
                buy_hit
            };

            if buy_first {
                let fill_price = pair.buy_price;
                record_buy_fill(&mut stats, fill_price, notional, maker_rate);
                mid = fill_price;
                pair = place_pair(mid, args.spread_bps);
            } else {
                let fill_price = pair.sell_price;
                record_sell_fill(&mut stats, fill_price, notional, maker_rate);
                mid = fill_price;
                pair = place_pair(mid, args.spread_bps);
            }
        }

        let mtm = stats.realized_usdt - stats.fees + stats.base_position * c.close;
        if mtm > peak_pnl {
            peak_pnl = mtm;
        }
        let dd = peak_pnl - mtm;
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

fn symbol_to_path(data_dir: &PathBuf, sym: &str, suffix: &str) -> PathBuf {
    let base = sym.trim_end_matches("USDT").to_lowercase();
    data_dir.join(format!("{base}{suffix}"))
}

fn print_single_summary(args: &Args, stats: &Stats, last_close: f64) {
    println!(
        "\nNaive rolling-mid  |  notional/order=${}  spread={}bps  maker={}bps",
        args.notional_per_order, args.spread_bps, args.maker_bps
    );
    println!("{}", "-".repeat(96));
    println!(
        "fills total       : {}  ({} buy, {} sell)",
        stats.fills, stats.buy_fills, stats.sell_fills
    );
    println!("realized cash-flow: {:>14.4}", stats.realized_usdt);
    println!("fees              : {:>14.4}", stats.fees);
    let real_net = stats.realized_usdt - stats.fees;
    println!("realized − fees   : {:>14.4}", real_net);
    let base_value = stats.base_position * last_close;
    println!("base position end : {:>14.6}", stats.base_position);
    println!("base position USDT: {:>14.4}", base_value);
    let mtm = real_net + base_value;
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
        "\nNaive rolling-mid sweep  |  notional/order=${}  spread={}bps  maker={}bps",
        args.notional_per_order, args.spread_bps, args.maker_bps
    );
    println!("{}", "-".repeat(96));
    println!(
        "{:<14} {:>7} {:>12} {:>10} {:>10} {:>10} {:>10}",
        "SYMBOL", "FILLS", "REAL-FEE", "BASE_USDT", "MTM", "DD", "ACCT%"
    );
}

fn print_table_row(sym: &str, stats: &Stats, last_close: f64, budget: f64) {
    let real_net = stats.realized_usdt - stats.fees;
    let base_value = stats.base_position * last_close;
    let mtm = real_net + base_value;
    let acct = if budget > 0.0 {
        format!("{:+.2}%", mtm / budget * 100.0)
    } else {
        "-".to_string()
    };
    println!(
        "{:<14} {:>7} {:>12.4} {:>10.4} {:>10.4} {:>10.4} {:>10}",
        sym, stats.fills, real_net, base_value, mtm, stats.max_drawdown, acct
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
        let real_net = stats.realized_usdt - stats.fees;
        let mtm = real_net + stats.base_position * last_close;
        if mtm > 0.0 {
            wins += 1;
        }
        totals_mtm += mtm;
        totals_real_fee += real_net;
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
