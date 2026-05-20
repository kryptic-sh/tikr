//! DCA / Martingale band backtester.
//!
//! Extends `backtest_bands` with the "never close at a loss + scale-in
//! against the trend" pattern (a.k.a. **DCA grid bot**, **Martingale grid**):
//!
//! - When **flat**, post limits at both band edges (same as base strategy).
//! - When **holding** (one side filled), each new bar:
//!   - If the bar's `high` ≥ `avg_entry × (1 + tp_margin)` for a long (or
//!     symmetric for short), close the entire position at the TP price.
//!     Take-profit is the ONLY way out.
//!   - Otherwise, if the bar's `low` ≤ new lower band AND DCA count is
//!     below `max_dca`, **add** to the losing side at the band edge with
//!     size = `base_size × dca_factor^dca_count`. Position grows.
//!   - Position is never stopped out. If TP never hits, the cycle stays
//!     open forever (or until `max_dca` reached → just hold without adding).
//!
//! This strategy wins on **most periods** (market mean-reverts → eventually
//! hits avg_entry + margin). Losing periods are catastrophic — sustained
//! trend with no reversion → position grows to unbounded notional → if you
//! had hard capital limits in real life, you'd be liquidated.
//!
//! Backtest reports max position size + max DCA depth so you can see the
//! tail-risk shape.

use std::path::PathBuf;

use clap::Parser;
use polars::prelude::*;

#[derive(Parser, Debug)]
#[command(
    name = "backtest_dca_bands",
    about = "Band-reversion DCA grid: never close at a loss, scale-in against trend"
)]
struct Args {
    #[arg(long, default_value = "./data/klines/eth_15m_90d.parquet")]
    data: PathBuf,
    /// Base order size (first entry per cycle).
    #[arg(long, default_value_t = 0.01_f64)]
    size: f64,
    #[arg(long, default_value_t = 10u32)]
    lookback: u32,
    #[arg(long, default_value_t = 0.5_f64)]
    compress: f64,
    /// Size multiplier per DCA add. 1.5 = 50% bigger each step. 2.0 doubles.
    #[arg(long, default_value_t = 1.5_f64)]
    dca_factor: f64,
    /// Hard cap on DCA adds per cycle. After this many, position holds
    /// without further adds (still waiting for TP).
    #[arg(long, default_value_t = 10u32)]
    max_dca: u32,
    /// TP margin in bps above weighted-avg entry (or below for shorts).
    /// 20 = 0.20%. Close ENTIRE position when price reaches this.
    #[arg(long, default_value_t = 20u32)]
    tp_margin_bps: u32,
    #[arg(long, default_value_t = 2u32)]
    maker_bps: u32,
    #[arg(long, default_value_t = 5u32)]
    taker_bps: u32,
    /// Stop-and-reverse threshold in bps. When unrealized loss vs avg_entry
    /// exceeds this, close the position at market (taker) and open a
    /// FRESH opposite-side position at base_size. `0` disables reversal —
    /// strategy holds losers indefinitely (classic Martingale failure).
    /// Trades convert the negative-tail risk into trend-following behavior.
    #[arg(long, default_value_t = 0u32)]
    reverse_dd_bps: u32,
    /// Profit-skim mode. Perp account starts at this USDT budget. Trading
    /// PnL accrues to it. Every time accumulated profit reaches
    /// `budget × skim_pct`, that chunk is "withdrawn" and used to buy the
    /// base asset at the current candle close price (no fees on the spot
    /// buy — approximation). `0` disables skimming, results are pure perp PnL.
    #[arg(long, default_value_t = 0.0_f64)]
    budget: f64,
    /// Skim threshold as percent of budget. `5` = skim every 5% gain.
    #[arg(long, default_value_t = 5.0_f64)]
    skim_pct: f64,
}

#[derive(Debug, Clone, Copy)]
struct Candle {
    open: f64,
    high: f64,
    low: f64,
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
    Long { qty: f64, avg_entry: f64, dca_count: u32 },
    Short { qty: f64, avg_entry: f64, dca_count: u32 },
}

#[derive(Default, Debug, Clone)]
struct Result_ {
    cycles: u64,
    round_trips: u64,
    wins: u64,
    losses: u64,
    reversals: u64,
    realized: f64,
    fees: f64,
    max_position_qty: f64,
    max_position_notional: f64,
    max_dca_reached: u32,
    max_unrealized_dd: f64,
    total_notional_traded: f64,
    open_at_end: Option<(&'static str, f64, f64, u32)>, // side, qty, avg_entry, dca_count
    // Skim-mode accounting (zero when budget=0):
    skim_count: u64,
    skim_total_usdt: f64,
    btc_stacked: f64, // base asset accumulated on spot
    final_perp_balance: f64,
    final_btc_value: f64,
    final_close_price: f64,
    blown: bool, // perp account went to 0 or below
    blown_at_candle: u64,
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
    let base_size = args.size;
    let tp_margin = args.tp_margin_bps as f64 / 10_000.0;
    let max_dca = args.max_dca;
    let dca_factor = args.dca_factor;
    let reverse_threshold = args.reverse_dd_bps as f64 / 10_000.0;
    let reverse_enabled = args.reverse_dd_bps > 0;
    let skim_enabled = args.budget > 0.0;
    let skim_threshold = args.budget * args.skim_pct / 100.0;
    let mut profit_since_skim = 0.0f64;
    // Snapshot of net perp PnL used to compute deltas after each cycle/skim
    // (so we can credit profit_since_skim with each new gain).
    let mut last_net_seen = 0.0f64;

    let mut pos = Position::Flat;
    let mut res = Result_::default();

    for i in n..candles.len() {
        let band = compute_band(&candles[i - n..i], args.compress);
        let c = candles[i];

        match pos {
            Position::Flat => {
                let hit_buy = c.low <= band.lower;
                let hit_sell = c.high >= band.upper;
                match (hit_buy, hit_sell) {
                    (true, true) => {
                        let pnl = (band.upper - band.lower) * base_size;
                        let fees =
                            (band.upper + band.lower) * base_size * maker_rate;
                        res.realized += pnl;
                        res.fees += fees;
                        res.cycles += 1;
                        res.round_trips += 1;
                        res.wins += 1;
                        res.total_notional_traded +=
                            (band.upper + band.lower) * base_size;
                    }
                    (true, false) => {
                        pos = Position::Long {
                            qty: base_size,
                            avg_entry: band.lower,
                            dca_count: 1,
                        };
                        res.fees += band.lower * base_size * maker_rate;
                        res.total_notional_traded += band.lower * base_size;
                        let notional = band.lower * base_size;
                        if base_size > res.max_position_qty {
                            res.max_position_qty = base_size;
                        }
                        if notional > res.max_position_notional {
                            res.max_position_notional = notional;
                        }
                    }
                    (false, true) => {
                        pos = Position::Short {
                            qty: base_size,
                            avg_entry: band.upper,
                            dca_count: 1,
                        };
                        res.fees += band.upper * base_size * maker_rate;
                        res.total_notional_traded += band.upper * base_size;
                        let notional = band.upper * base_size;
                        if base_size > res.max_position_qty {
                            res.max_position_qty = base_size;
                        }
                        if notional > res.max_position_notional {
                            res.max_position_notional = notional;
                        }
                    }
                    (false, false) => {}
                }
            }
            Position::Long {
                qty,
                avg_entry,
                dca_count,
            } => {
                // Stop-and-reverse: bar's low crossed the SAR threshold below
                // entry → close at market and flip to short with base size.
                // Resets DCA counter; the new short side gets its own ladder.
                let sar_trigger = avg_entry * (1.0 - reverse_threshold);
                if reverse_enabled && c.low <= sar_trigger {
                    // Close long at SAR price (taker exit).
                    let pnl = (sar_trigger - avg_entry) * qty;
                    res.realized += pnl;
                    res.fees += sar_trigger * qty * taker_rate;
                    res.cycles += 1;
                    res.losses += 1;
                    res.reversals += 1;
                    res.total_notional_traded += sar_trigger * qty;
                    // Open fresh short at base size (taker entry).
                    pos = Position::Short {
                        qty: base_size,
                        avg_entry: sar_trigger,
                        dca_count: 1,
                    };
                    res.fees += sar_trigger * base_size * taker_rate;
                    res.total_notional_traded += sar_trigger * base_size;
                    let notional = sar_trigger * base_size;
                    if base_size > res.max_position_qty {
                        res.max_position_qty = base_size;
                    }
                    if notional > res.max_position_notional {
                        res.max_position_notional = notional;
                    }
                    continue;
                }

                let tp_price = avg_entry * (1.0 + tp_margin);
                let hit_tp = c.high >= tp_price;
                let hit_dca = c.low <= band.lower && dca_count < max_dca;

                if hit_tp {
                    // Close full position at TP price.
                    let pnl = (tp_price - avg_entry) * qty;
                    res.realized += pnl;
                    res.fees += tp_price * qty * maker_rate;
                    res.cycles += 1;
                    if pnl > 0.0 { res.wins += 1 } else { res.losses += 1 };
                    res.total_notional_traded += tp_price * qty;
                    pos = Position::Flat;
                } else if hit_dca {
                    let add_size = base_size * dca_factor.powi(dca_count as i32);
                    let new_qty = qty + add_size;
                    let new_avg =
                        (avg_entry * qty + band.lower * add_size) / new_qty;
                    let new_dca = dca_count + 1;
                    pos = Position::Long {
                        qty: new_qty,
                        avg_entry: new_avg,
                        dca_count: new_dca,
                    };
                    res.fees += band.lower * add_size * maker_rate;
                    res.total_notional_traded += band.lower * add_size;
                    if new_qty > res.max_position_qty {
                        res.max_position_qty = new_qty;
                    }
                    let notional = band.lower * new_qty;
                    if notional > res.max_position_notional {
                        res.max_position_notional = notional;
                    }
                    if new_dca > res.max_dca_reached {
                        res.max_dca_reached = new_dca;
                    }
                    // Track unrealized DD with the new (deeper) position.
                    let unrealized = (c.close - new_avg) * new_qty;
                    if -unrealized > res.max_unrealized_dd {
                        res.max_unrealized_dd = -unrealized;
                    }
                } else {
                    // Hold — track unrealized DD against close.
                    let unrealized = (c.close - avg_entry) * qty;
                    if -unrealized > res.max_unrealized_dd {
                        res.max_unrealized_dd = -unrealized;
                    }
                }
            }
            Position::Short {
                qty,
                avg_entry,
                dca_count,
            } => {
                let sar_trigger = avg_entry * (1.0 + reverse_threshold);
                if reverse_enabled && c.high >= sar_trigger {
                    let pnl = (avg_entry - sar_trigger) * qty;
                    res.realized += pnl;
                    res.fees += sar_trigger * qty * taker_rate;
                    res.cycles += 1;
                    res.losses += 1;
                    res.reversals += 1;
                    res.total_notional_traded += sar_trigger * qty;
                    pos = Position::Long {
                        qty: base_size,
                        avg_entry: sar_trigger,
                        dca_count: 1,
                    };
                    res.fees += sar_trigger * base_size * taker_rate;
                    res.total_notional_traded += sar_trigger * base_size;
                    let notional = sar_trigger * base_size;
                    if base_size > res.max_position_qty {
                        res.max_position_qty = base_size;
                    }
                    if notional > res.max_position_notional {
                        res.max_position_notional = notional;
                    }
                    continue;
                }

                let tp_price = avg_entry * (1.0 - tp_margin);
                let hit_tp = c.low <= tp_price;
                let hit_dca = c.high >= band.upper && dca_count < max_dca;

                if hit_tp {
                    let pnl = (avg_entry - tp_price) * qty;
                    res.realized += pnl;
                    res.fees += tp_price * qty * maker_rate;
                    res.cycles += 1;
                    if pnl > 0.0 { res.wins += 1 } else { res.losses += 1 };
                    res.total_notional_traded += tp_price * qty;
                    pos = Position::Flat;
                } else if hit_dca {
                    let add_size = base_size * dca_factor.powi(dca_count as i32);
                    let new_qty = qty + add_size;
                    let new_avg =
                        (avg_entry * qty + band.upper * add_size) / new_qty;
                    let new_dca = dca_count + 1;
                    pos = Position::Short {
                        qty: new_qty,
                        avg_entry: new_avg,
                        dca_count: new_dca,
                    };
                    res.fees += band.upper * add_size * maker_rate;
                    res.total_notional_traded += band.upper * add_size;
                    if new_qty > res.max_position_qty {
                        res.max_position_qty = new_qty;
                    }
                    let notional = band.upper * new_qty;
                    if notional > res.max_position_notional {
                        res.max_position_notional = notional;
                    }
                    if new_dca > res.max_dca_reached {
                        res.max_dca_reached = new_dca;
                    }
                    let unrealized = (new_avg - c.close) * new_qty;
                    if -unrealized > res.max_unrealized_dd {
                        res.max_unrealized_dd = -unrealized;
                    }
                } else {
                    let unrealized = (avg_entry - c.close) * qty;
                    if -unrealized > res.max_unrealized_dd {
                        res.max_unrealized_dd = -unrealized;
                    }
                }
            }
        }

        // --- end-of-candle skim + halt checks (skim_enabled only) ---
        if skim_enabled {
            let net_now = res.realized - res.fees - res.skim_total_usdt;
            let gain_delta = net_now - last_net_seen;
            if gain_delta > 0.0 {
                profit_since_skim += gain_delta;
            }
            // Skim in fixed `skim_threshold` chunks. Multiple skims per
            // candle possible if a single close generated a huge gain.
            while profit_since_skim >= skim_threshold {
                let btc_bought = skim_threshold / c.close;
                res.btc_stacked += btc_bought;
                res.skim_total_usdt += skim_threshold;
                res.skim_count += 1;
                profit_since_skim -= skim_threshold;
            }
            last_net_seen = res.realized - res.fees - res.skim_total_usdt;

            // Halt if perp account dropped to or below zero (margin call).
            let perp_balance = args.budget + last_net_seen;
            if perp_balance <= 0.0 && !res.blown {
                res.blown = true;
                res.blown_at_candle = i as u64;
                break;
            }
        }
    }

    match pos {
        Position::Flat => {}
        Position::Long { qty, avg_entry, dca_count } => {
            res.open_at_end = Some(("long", qty, avg_entry, dca_count));
        }
        Position::Short { qty, avg_entry, dca_count } => {
            res.open_at_end = Some(("short", qty, avg_entry, dca_count));
        }
    }
    // Final mark-to-market: BTC stack valued at last candle close. Perp
    // balance = budget + realized − fees − skimmed + unrealized on any
    // open perp position at the last close.
    let last_close = candles.last().map(|c| c.close).unwrap_or(0.0);
    res.final_close_price = last_close;
    res.final_btc_value = res.btc_stacked * last_close;
    let unrealized = match pos {
        Position::Flat => 0.0,
        Position::Long { qty, avg_entry, .. } => (last_close - avg_entry) * qty,
        Position::Short { qty, avg_entry, .. } => (avg_entry - last_close) * qty,
    };
    res.final_perp_balance = if skim_enabled {
        args.budget + res.realized - res.fees - res.skim_total_usdt + unrealized
    } else {
        res.realized - res.fees + unrealized
    };
    res
}

fn print_summary(args: &Args, res: &Result_) {
    println!(
        "\nBand DCA on klines  |  lookback={}  compress={:.2}  size={}  dca_factor={:.2}  max_dca={}  tp_margin={}bps",
        args.lookback, args.compress, args.size, args.dca_factor, args.max_dca, args.tp_margin_bps
    );
    println!("{}", "-".repeat(96));
    println!(
        "cycles closed     : {}  (round-trips in one bar: {})",
        res.cycles, res.round_trips
    );
    println!(
        "wins / loss closed: {} / {}  ({:.1}% win rate, {} SAR reversals)",
        res.wins,
        res.losses,
        res.win_rate() * 100.0,
        res.reversals,
    );
    println!("realized          : {:>14.4}", res.realized);
    println!("fees              : {:>14.4}", res.fees);
    println!("NET realized      : {:>14.4}", res.net());
    println!(
        "max DCA depth     : {}  (max position qty: {:.6}, max notional: {:.2})",
        res.max_dca_reached, res.max_position_qty, res.max_position_notional
    );
    println!("max unrealized DD : {:>14.4}", res.max_unrealized_dd);
    println!(
        "total notional    : {:>14.2}  (gross volume traded)",
        res.total_notional_traded
    );
    match res.open_at_end {
        None => println!("open posn         : (flat)"),
        Some((side, qty, avg, dca)) => println!(
            "open posn         : {} qty={:.6} avg={:.4} dca_depth={}",
            side, qty, avg, dca
        ),
    }
    if args.budget > 0.0 {
        println!();
        println!(
            "--- skim mode: budget=${:.2}, threshold per skim=${:.2} ({}%) ---",
            args.budget,
            args.budget * args.skim_pct / 100.0,
            args.skim_pct
        );
        println!(
            "skims executed    : {}  (total USDT moved to spot: {:.4})",
            res.skim_count, res.skim_total_usdt
        );
        println!(
            "BTC stacked       : {:.8} @ avg cost {:.4} USDT  (last close: {:.4})",
            res.btc_stacked,
            if res.btc_stacked > 0.0 {
                res.skim_total_usdt / res.btc_stacked
            } else {
                0.0
            },
            res.final_close_price
        );
        println!("perp balance end  : {:>14.4}", res.final_perp_balance);
        println!("BTC value end     : {:>14.4}", res.final_btc_value);
        let total = res.final_perp_balance + res.final_btc_value;
        let pnl = total - args.budget;
        let pnl_pct = if args.budget > 0.0 { pnl / args.budget * 100.0 } else { 0.0 };
        println!(
            "TOTAL ACCT VALUE  : {:>14.4}  (start ${:.2} → end ${:.2}, {:+.2}% [{:+.4}])",
            total, args.budget, total, pnl_pct, pnl
        );
        if res.blown {
            println!(
                "⚠ ACCOUNT BLOWN at candle {} — perp balance went ≤ 0",
                res.blown_at_candle
            );
        }
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
    let res = simulate(&candles, &args);
    print_summary(&args, &res);
    Ok(())
}
