//! Layered-grid re-entry scalper.
//!
//! Maintains a fixed count of open orders (3 per side by default) at
//! geometrically-spaced prices around mid:
//!
//! ```text
//! sell @ mid +12 bps  ←  outer sell (smaller coin-qty per fixed $ notional)
//! sell @ mid + 9 bps  ←  middle sell
//! sell @ mid + 6 bps  ←  inner sell
//!                    MID
//! buy  @ mid − 6 bps  ←  inner buy
//! buy  @ mid − 9 bps  ←  middle buy
//! buy  @ mid −12 bps  ←  outer buy (larger coin-qty per fixed $ notional)
//! ```
//!
//! **Each order has fixed FIAT notional** (e.g. `$100`). Since coin
//! quantity = `notional / price`, cheaper buys accumulate more coin and
//! higher sells release less coin — the structure has a built-in long
//! bias even before any price movement.
//!
//! **Re-entry rule**: when a BUY fills at price `P`, place a SELL at
//! `P × (1 + reentry_bps/10000)` for the same fiat notional. When a SELL
//! fills, mirror with a BUY at `P × (1 − reentry_bps/10000)`. The 6-order
//! count is preserved.
//!
//! Each completed buy→sell cycle captures the re-entry spread minus 2×
//! maker fee. Cycles complete in any order — buys can pyramid down,
//! then unwind from any level back up. Inventory drift on fixed-fiat
//! sizing means we accumulate base asset over time even on flat markets.

use std::path::PathBuf;

use clap::Parser;
use polars::prelude::*;

#[derive(Parser, Debug)]
#[command(
    name = "backtest_layered_grid",
    about = "Layered fixed-fiat grid with re-entry scalping (always N orders/side)"
)]
struct Args {
    #[arg(long, default_value = "./data/klines/eth_1m_90d.parquet")]
    data: PathBuf,
    /// Fixed fiat notional per order. Coin quantity = notional / price.
    #[arg(long, default_value_t = 100.0_f64)]
    notional_per_order: f64,
    /// Number of orders per side (3 → 3 buys + 3 sells = 6 total).
    #[arg(long, default_value_t = 3usize)]
    levels_per_side: usize,
    /// Inner spread from mid in bps. First buy at `mid × (1 − inner/10000)`,
    /// first sell at `mid × (1 + inner/10000)`.
    #[arg(long, default_value_t = 6u32)]
    inner_bps: u32,
    /// Step between levels in bps. Buy_2 = inner+step, buy_3 = inner+2×step, etc.
    #[arg(long, default_value_t = 3u32)]
    step_bps: u32,
    /// Re-entry spread in bps. When a buy fills at P, new sell at
    /// `P × (1 + reentry/10000)`; mirror for sell fills.
    #[arg(long, default_value_t = 3u32)]
    reentry_bps: u32,
    #[arg(long, default_value_t = 2u32)]
    maker_bps: u32,
    /// Starting USDT budget (for total account accounting). `0` runs in
    /// "stack only" mode without absolute account value.
    #[arg(long, default_value_t = 0.0_f64)]
    budget: f64,
    /// Scale `notional_per_order` by current account value when placing
    /// NEW orders. Requires `--budget > 0`. As perp balance grows, new
    /// orders use proportionally larger notional. Existing resting orders
    /// keep their original size (real exchanges can't resize a live limit).
    #[arg(long, default_value_t = false)]
    scale_with_balance: bool,
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

#[derive(Debug, Clone, Copy, PartialEq)]
enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy)]
struct Order {
    side: Side,
    price: f64,
    /// Coin quantity = fiat_notional / price (computed at placement).
    qty: f64,
    /// Source fiat notional for this order.
    fiat: f64,
}

#[derive(Default, Debug)]
struct Stats {
    fills: u64,
    buy_fills: u64,
    sell_fills: u64,
    realized_usdt: f64, // fiat PnL on buy→sell scalp pairs (FIFO matched)
    fees: f64,
    base_position: f64, // running coin position
    fiat_spent: f64,    // total fiat sent on buys
    fiat_received: f64, // total fiat received on sells
    max_drawdown: f64,
    max_base_position: f64,
}

fn place_initial_orders(mid: f64, args: &Args, scaled_notional: f64) -> Vec<Order> {
    let mut orders = Vec::with_capacity(args.levels_per_side * 2);
    for k in 0..args.levels_per_side {
        let bps = args.inner_bps as f64 + args.step_bps as f64 * k as f64;
        let buy_price = mid * (1.0 - bps / 10_000.0);
        let sell_price = mid * (1.0 + bps / 10_000.0);
        orders.push(Order {
            side: Side::Buy,
            price: buy_price,
            qty: scaled_notional / buy_price,
            fiat: scaled_notional,
        });
        orders.push(Order {
            side: Side::Sell,
            price: sell_price,
            qty: scaled_notional / sell_price,
            fiat: scaled_notional,
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
    // FIFO scalp realization: realized = fiat received − fiat spent so far.
    // Tracks the cumulative cash-flow PnL across all fills regardless of
    // pair attribution.
    stats.realized_usdt = stats.fiat_received - stats.fiat_spent;
    if stats.base_position > stats.max_base_position {
        stats.max_base_position = stats.base_position;
    }
}

fn simulate(candles: &[Candle], args: &Args) -> (Stats, Vec<Order>, f64, f64) {
    if candles.is_empty() {
        return (Stats::default(), Vec::new(), 0.0, args.notional_per_order);
    }
    let maker_rate = args.maker_bps as f64 / 10_000.0;
    let reentry = args.reentry_bps as f64 / 10_000.0;
    let scale_enabled = args.scale_with_balance && args.budget > 0.0;

    // `current_notional` is what a NEW order placed RIGHT NOW would use.
    // It scales with the running balance when `scale_with_balance` is set.
    let mut orders = place_initial_orders(candles[0].open, args, args.notional_per_order);
    let mut stats = Stats::default();
    let mut peak_cumulative_pnl = 0.0f64;
    let mut max_notional_per_order = args.notional_per_order;

    for c in candles.iter().skip(1) {
        // Find all orders that the candle's range would have crossed.
        // Process in price-conservative order: lows first (buys fill on the
        // way down), then highs (sells fill on the way back up). Real OHLC
        // can't tell us the true intra-bar path; we model both legs each
        // bar to keep the grid populated.
        let mut to_replace: Vec<(usize, Order)> = Vec::new();
        for (i, o) in orders.iter().enumerate() {
            let hit = match o.side {
                Side::Buy => c.low <= o.price,
                Side::Sell => c.high >= o.price,
            };
            if hit {
                to_replace.push((i, *o));
            }
        }
        // Apply in descending index order so removal is safe.
        for (i, _) in to_replace.iter().rev() {
            orders.remove(*i);
        }
        for (_, filled) in &to_replace {
            fill_order(filled, &mut stats, maker_rate);

            // Compute current account value and scaled notional for any
            // NEW orders placed THIS step. Account value reflects the
            // collateral cap if scaling is enabled.
            let cur_notional = if scale_enabled {
                let mtm = stats.realized_usdt - stats.fees + stats.base_position * c.close;
                let balance = (args.budget + mtm).max(0.0);
                let scale = balance / args.budget;
                let scaled = args.notional_per_order * scale;
                if scaled > max_notional_per_order {
                    max_notional_per_order = scaled;
                }
                scaled
            } else {
                args.notional_per_order
            };

            // Re-entry: opposite side at filled.price ± reentry_bps.
            let (new_side, new_price) = match filled.side {
                Side::Buy => (Side::Sell, filled.price * (1.0 + reentry)),
                Side::Sell => (Side::Buy, filled.price * (1.0 - reentry)),
            };
            orders.push(Order {
                side: new_side,
                price: new_price,
                qty: cur_notional / new_price,
                fiat: cur_notional,
            });
        }

        // Drawdown tracking: mark to market = realized + (base × close − fiat_held_short_of_base)
        // Simpler: account value = budget + fiat_received − fiat_spent − fees + base_position × close.
        let mtm = stats.realized_usdt - stats.fees + stats.base_position * c.close;
        if mtm > peak_cumulative_pnl {
            peak_cumulative_pnl = mtm;
        }
        let dd = peak_cumulative_pnl - mtm;
        if dd > stats.max_drawdown {
            stats.max_drawdown = dd;
        }
    }

    let last_close = candles.last().map(|c| c.close).unwrap_or(0.0);
    (stats, orders, last_close, max_notional_per_order)
}

fn print_summary(args: &Args, stats: &Stats, orders: &[Order], last_close: f64, max_notional: f64) {
    println!(
        "\nLayered-grid re-entry  |  notional/order=${}  levels={}  inner={}bps  step={}bps  reentry={}bps  maker={}bps",
        args.notional_per_order,
        args.levels_per_side,
        args.inner_bps,
        args.step_bps,
        args.reentry_bps,
        args.maker_bps
    );
    println!("{}", "-".repeat(96));
    println!("fills total       : {}  ({} buy, {} sell)", stats.fills, stats.buy_fills, stats.sell_fills);
    println!("realized cash-flow: {:>14.4}  (fiat_received − fiat_spent)", stats.realized_usdt);
    println!("fees              : {:>14.4}", stats.fees);
    let realized_net = stats.realized_usdt - stats.fees;
    println!("realized − fees   : {:>14.4}", realized_net);
    println!(
        "base position end : {:>14.6}  (max during run: {:.6})",
        stats.base_position, stats.max_base_position
    );
    let base_value = stats.base_position * last_close;
    println!(
        "base position USDT: {:>14.4}  (at last close {:.4})",
        base_value, last_close
    );
    let mtm = realized_net + base_value;
    println!("TOTAL MTM PnL     : {:>14.4}", mtm);
    println!("max drawdown      : {:>14.4}", stats.max_drawdown);
    println!("open orders end   : {}  ({} buys, {} sells)",
        orders.len(),
        orders.iter().filter(|o| o.side == Side::Buy).count(),
        orders.iter().filter(|o| o.side == Side::Sell).count(),
    );
    if args.budget > 0.0 {
        let total = args.budget + mtm;
        let pct = mtm / args.budget * 100.0;
        println!(
            "TOTAL ACCT (budget ${:.2}): {:>10.4}  ({:+.2}%)",
            args.budget, total, pct
        );
    }
    if args.scale_with_balance {
        println!(
            "scaled notional   : start ${:.2} → max ${:.4}  ({:.2}× growth)",
            args.notional_per_order,
            max_notional,
            max_notional / args.notional_per_order,
        );
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let candles = load_candles(&args.data)?;
    eprintln!("loaded {} candles from {}", candles.len(), args.data.display());
    if !candles.is_empty() {
        let span_ms = candles.last().unwrap().open_ts_ms - candles[0].open_ts_ms;
        let span_d = span_ms as f64 / (24.0 * 60.0 * 60_000.0);
        eprintln!("span: {:.1} days", span_d);
    }
    let (stats, orders, last_close, max_notional) = simulate(&candles, &args);
    print_summary(&args, &stats, &orders, last_close, max_notional);
    Ok(())
}
