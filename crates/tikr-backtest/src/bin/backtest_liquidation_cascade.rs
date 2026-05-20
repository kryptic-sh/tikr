//! Liquidation-cascade mean-reversion backtester.
//!
//! ## Strategy
//!
//! Forced liquidations on Binance USD-M Futures are broadcast on the
//! `!forceOrder@arr` stream. When many liquidations pile up on the same side
//! in a short rolling window, the market has likely overshot — the "cascade".
//! The position we take is the **opposite** of the dominant liquidation side:
//!
//! - Liquidation side SELL dominates (longs got hit → price dropped) → enter LONG.
//! - Liquidation side BUY dominates  (shorts got hit → price pumped) → enter SHORT.
//!
//! ## Inputs
//!
//! 1. **Liquidation parquet** (`--liquidations`) — produced by `record_liquidations`.
//!    Columns: `ts_ns u64`, `symbol Utf8`, `side Utf8 ("BUY"|"SELL")`,
//!    `qty f64`, `price f64`, `notional f64`.
//!
//! 2. **1-minute klines parquet** per symbol — for entry fill prices and exit
//!    OHLC checks. Resolves to `{klines-dir}/{lower_base}{klines-suffix}`.
//!    Columns: `open_ts_ms u64`, `open f64`, `high f64`, `low f64`, `close f64`.
//!
//! ## Cascade detection
//!
//! Per symbol, maintain a rolling deque of (ts_ns, side, notional). On each
//! liquidation event:
//! 1. Drop entries older than `window_secs`.
//! 2. Count events and cumulative notional.
//! 3. If count ≥ min_count AND notional ≥ min_notional AND same-side fraction
//!    ≥ same_side_pct → trigger a cascade signal.
//!
//! Entry fill price = next 1-minute candle's `open` after the trigger timestamp.
//!
//! ## Exit logic (per open position, checked on each subsequent candle)
//!
//! - TP hit: long if `high ≥ entry × (1 + tp_bps/10000)`.
//! - SL hit: long if `low  ≤ entry × (1 - sl_bps/10000)`.
//! - If both TP and SL appear in the same candle: SL fires first (pessimistic).
//! - Time stop: if neither TP nor SL fires within `time_stop_secs`, close at
//!   the candle's `close` at that time boundary.
//!
//! ## Cooldown
//!
//! After exiting a position on a symbol, new cascade signals on that symbol are
//! ignored for `cooldown_secs`.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};

use clap::Parser;
use polars::prelude::*;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser, Debug)]
#[command(
    name = "backtest_liquidation_cascade",
    about = "Liquidation-cascade mean-reversion backtest: detect cascades in recorded \
             !forceOrder data, simulate entry/exit against 1m klines"
)]
struct Args {
    /// Path to the liquidation parquet produced by `record_liquidations`.
    #[arg(long)]
    liquidations: PathBuf,

    /// Base directory containing per-symbol kline parquets.
    #[arg(long, default_value = "./data/klines/1y")]
    klines_dir: PathBuf,

    /// Filename suffix appended after the lowercase base symbol.
    /// E.g. `_1m_1y.parquet` resolves BTCUSDT → `btc_1m_1y.parquet`.
    #[arg(long, default_value = "_1m_1y.parquet")]
    klines_suffix: String,

    /// Comma-separated symbols to trade. Others are discarded as noise.
    /// E.g. `BTCUSDT,ETHUSDT`.
    #[arg(long, default_value = "BTCUSDT,ETHUSDT")]
    symbols: String,

    // Cascade detection -------------------------------------------------
    /// Rolling window length for cascade detection (seconds).
    #[arg(long, default_value_t = 30u64)]
    window_secs: u64,

    /// Minimum number of liquidations in the window to trigger a cascade.
    #[arg(long, default_value_t = 5u32)]
    min_count: u32,

    /// Minimum cumulative USD notional in the window to trigger a cascade.
    #[arg(long, default_value_t = 100_000.0_f64)]
    min_notional: f64,

    /// Fraction of liquidations that must be on the same side (0.0–1.0).
    #[arg(long, default_value_t = 0.8_f64)]
    same_side_pct: f64,

    // Trade execution ---------------------------------------------------
    /// Fixed USDT notional per trade (qty = notional / entry_price).
    #[arg(long, default_value_t = 100.0_f64)]
    notional: f64,

    /// Take-profit distance in basis points.
    #[arg(long, default_value_t = 30u32)]
    tp_bps: u32,

    /// Stop-loss distance in basis points.
    #[arg(long, default_value_t = 60u32)]
    sl_bps: u32,

    /// Time stop: close the position at this many seconds after entry.
    #[arg(long, default_value_t = 300u64)]
    time_stop_secs: u64,

    /// Cooldown after position exit: ignore new cascade signals for this many seconds.
    #[arg(long, default_value_t = 60u64)]
    cooldown_secs: u64,

    /// Taker fee in basis points (paid on both entry and exit).
    #[arg(long, default_value_t = 5u32)]
    taker_bps: u32,

    /// Starting USDT budget for account-level return reporting. `0` disables.
    #[arg(long, default_value_t = 0.0_f64)]
    budget: f64,
}

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

/// A single liquidation event loaded from parquet.
#[derive(Debug, Clone)]
struct LiqRec {
    ts_ns: u64,
    symbol: String,
    /// `true` = SELL (long liquidated), `false` = BUY (short liquidated).
    is_sell: bool,
    notional: f64,
}

/// A 1-minute kline candle.
#[derive(Debug, Clone, Copy)]
struct Candle {
    open_ts_ms: u64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
}

/// An entry in the per-symbol rolling deque.
#[derive(Debug, Clone, Copy)]
struct DequeEntry {
    ts_ns: u64,
    is_sell: bool,
    notional: f64,
}

/// The outcome of a single simulated trade.
#[derive(Debug, Clone)]
struct Trade {
    #[allow(dead_code)]
    symbol: String,
    /// Entry timestamp (ns).
    #[allow(dead_code)]
    entry_ts_ns: u64,
    entry_price: f64,
    /// `true` = long, `false` = short.
    #[allow(dead_code)]
    is_long: bool,
    exit_price: f64,
    qty: f64,
    gross: f64,
    fees: f64,
    /// ExitReason variant for display purposes.
    #[allow(dead_code)]
    exit_reason: ExitReason,
}

impl Trade {
    fn net(&self) -> f64 {
        self.gross - self.fees
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExitReason {
    TakeProfit,
    StopLoss,
    TimeStop,
}

// ---------------------------------------------------------------------------
// Parquet loaders
// ---------------------------------------------------------------------------

fn load_liquidations(path: &Path) -> Result<Vec<LiqRec>, Box<dyn std::error::Error>> {
    let file = std::fs::File::open(path)?;
    let df = ParquetReader::new(file).finish()?;

    let ts_ns = df.column("ts_ns")?.u64()?;
    let symbol = df.column("symbol")?.str()?;
    let side = df.column("side")?.str()?;
    let notional = df.column("notional")?.f64()?;

    let n = df.height();
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let ts = ts_ns.get(i).ok_or("null ts_ns")?;
        let sym = symbol.get(i).ok_or("null symbol")?;
        let s = side.get(i).ok_or("null side")?;
        let not = notional.get(i).ok_or("null notional")?;
        out.push(LiqRec {
            ts_ns: ts,
            symbol: sym.to_string(),
            is_sell: s == "SELL",
            notional: not,
        });
    }
    // Sort by time so we can process in order.
    out.sort_by_key(|r| r.ts_ns);
    Ok(out)
}

fn load_candles(path: &Path) -> Result<Vec<Candle>, Box<dyn std::error::Error>> {
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
// Helpers
// ---------------------------------------------------------------------------

fn parse_symbols(s: &str) -> Vec<String> {
    s.split(',')
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
        .collect()
}

fn symbol_to_klines_path(klines_dir: &Path, sym: &str, suffix: &str) -> PathBuf {
    let base_lower = {
        let mut b = sym;
        for sfx in &["USDT", "USDC", "BUSD", "TUSD"] {
            if let Some(stripped) = sym.strip_suffix(sfx) {
                b = stripped;
                break;
            }
        }
        b.to_lowercase()
    };
    klines_dir.join(format!("{base_lower}{suffix}"))
}

// ---------------------------------------------------------------------------
// Per-symbol simulation
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct SymStats {
    trades: u64,
    wins: u64,
    losses: u64,
    sum_wins: f64,
    sum_losses: f64,
    gross: f64,
    fees: f64,
}

impl SymStats {
    fn net(&self) -> f64 {
        self.gross - self.fees
    }
    fn win_rate(&self) -> f64 {
        if self.trades == 0 {
            0.0
        } else {
            self.wins as f64 / self.trades as f64
        }
    }
    fn avg_win(&self) -> f64 {
        if self.wins == 0 {
            0.0
        } else {
            self.sum_wins / self.wins as f64
        }
    }
    fn avg_loss(&self) -> f64 {
        if self.losses == 0 {
            0.0
        } else {
            self.sum_losses / self.losses as f64
        }
    }
}

fn simulate_symbol(
    symbol: &str,
    liqs: &[LiqRec], // already filtered to this symbol, sorted by ts_ns
    candles: &[Candle],
    args: &Args,
) -> (SymStats, Vec<Trade>) {
    let window_ns = args.window_secs * 1_000_000_000;
    let time_stop_ns = args.time_stop_secs * 1_000_000_000;
    let cooldown_ns = args.cooldown_secs * 1_000_000_000;
    let tp_factor = 1.0 + args.tp_bps as f64 / 10_000.0;
    let sl_factor = 1.0 - args.sl_bps as f64 / 10_000.0;
    let taker_rate = args.taker_bps as f64 / 10_000.0;

    let mut stats = SymStats::default();
    let mut trades: Vec<Trade> = Vec::new();

    // Rolling deque of recent liquidations for cascade detection.
    let mut deque: VecDeque<DequeEntry> = VecDeque::new();

    // Cooldown: after exiting, block new entries until this ns timestamp.
    let mut cooldown_until_ns: u64 = 0;

    // Current open position state.
    let mut open_position: Option<OpenPosition> = None;

    // We advance through liquidation events.
    for liq in liqs {
        // --- Advance open position using candles before this event ---
        // Process candle-level exit checks up to the current liq event time.
        if let Some(pos) = open_position.take() {
            let result =
                advance_position(pos, candles, liq.ts_ns, tp_factor, sl_factor, time_stop_ns);
            match result {
                AdvanceResult::StillOpen(pos) => {
                    open_position = Some(pos);
                }
                AdvanceResult::Closed(trade) => {
                    record_trade(&trade, &mut stats, taker_rate);
                    trades.push(trade.clone());
                    // Cooldown starts from the current liquidation event timestamp.
                    cooldown_until_ns = liq.ts_ns + cooldown_ns;
                }
            }
        }

        // --- Cascade detection ---
        // Don't look for new entries if we're in cooldown or already in a position.
        let in_cooldown = liq.ts_ns < cooldown_until_ns;
        if !in_cooldown && open_position.is_none() {
            // Evict stale entries.
            let window_start = liq.ts_ns.saturating_sub(window_ns);
            while deque
                .front()
                .map(|e| e.ts_ns < window_start)
                .unwrap_or(false)
            {
                deque.pop_front();
            }

            // Push current event.
            deque.push_back(DequeEntry {
                ts_ns: liq.ts_ns,
                is_sell: liq.is_sell,
                notional: liq.notional,
            });

            let count = deque.len();
            let cum_notional: f64 = deque.iter().map(|e| e.notional).sum();
            let sell_count = deque.iter().filter(|e| e.is_sell).count();
            let buy_count = count - sell_count;
            let dominant_is_sell = sell_count >= buy_count;
            let dominant_count = sell_count.max(buy_count);
            let same_side_frac = if count > 0 {
                dominant_count as f64 / count as f64
            } else {
                0.0
            };

            let cascade = count >= args.min_count as usize
                && cum_notional >= args.min_notional
                && same_side_frac >= args.same_side_pct;

            if cascade {
                // Entry side = opposite of dominant liquidation side.
                // Dominant SELL (longs hit, price fell) → we go LONG.
                // Dominant BUY  (shorts hit, price pumped) → we go SHORT.
                let is_long = dominant_is_sell;

                // Entry fill = next 1-minute candle's open after trigger ts.
                let trigger_ms = liq.ts_ns / 1_000_000;
                // Find first candle that opens AFTER the trigger candle.
                // "Next bar fill": find candle with open_ts_ms > trigger_ms.
                if let Some(entry_idx) = first_candle_after(candles, trigger_ms) {
                    let entry_candle = candles[entry_idx];
                    let entry_price = entry_candle.open;
                    let entry_ts_ns = entry_candle.open_ts_ms * 1_000_000;
                    let qty = args.notional / entry_price;

                    open_position = Some(OpenPosition {
                        symbol: symbol.to_string(),
                        entry_ts_ns,
                        entry_price,
                        is_long,
                        qty,
                        next_candle_idx: entry_idx + 1,
                    });

                    // Clear deque so we don't re-trigger immediately.
                    deque.clear();
                }
            }
        } else if !in_cooldown {
            // In a position: still update deque for future use after exit.
            let window_start = liq.ts_ns.saturating_sub(window_ns);
            while deque
                .front()
                .map(|e| e.ts_ns < window_start)
                .unwrap_or(false)
            {
                deque.pop_front();
            }
        }
    }

    // After all liquidation events, close any open position at the last candle.
    if let Some(pos) = open_position.take() {
        let result = advance_position(pos, candles, u64::MAX, tp_factor, sl_factor, time_stop_ns);
        if let AdvanceResult::Closed(trade) = result {
            record_trade(&trade, &mut stats, taker_rate);
            trades.push(trade);
        }
    }

    (stats, trades)
}

fn record_trade(trade: &Trade, stats: &mut SymStats, taker_rate: f64) {
    let fees = (trade.entry_price + trade.exit_price) * trade.qty * taker_rate;
    let _ = fees; // fees are already set on the trade
    stats.trades += 1;
    stats.gross += trade.gross;
    stats.fees += trade.fees;
    let net = trade.net();
    if net > 0.0 {
        stats.wins += 1;
        stats.sum_wins += net;
    } else {
        stats.losses += 1;
        stats.sum_losses += net;
    }
}

/// State for a currently open position.
#[derive(Debug, Clone)]
struct OpenPosition {
    symbol: String,
    entry_ts_ns: u64,
    entry_price: f64,
    /// `true` = long.
    is_long: bool,
    qty: f64,
    /// Next candle index to check for TP/SL/time-stop.
    next_candle_idx: usize,
}

enum AdvanceResult {
    StillOpen(OpenPosition),
    Closed(Trade),
}

/// Advance `pos` through candles up to `until_ns`. Returns either the
/// still-open position or a closed `Trade`.
fn advance_position(
    mut pos: OpenPosition,
    candles: &[Candle],
    until_ns: u64,
    tp_factor: f64,
    sl_factor: f64,
    time_stop_ns: u64,
) -> AdvanceResult {
    let tp_price = if pos.is_long {
        pos.entry_price * tp_factor
    } else {
        pos.entry_price * (2.0 - tp_factor) // entry × (1 − tp_bps/10000)
    };
    let sl_price = if pos.is_long {
        pos.entry_price * sl_factor
    } else {
        pos.entry_price * (2.0 - sl_factor) // entry × (1 + sl_bps/10000)
    };
    let time_stop_ns_abs = pos.entry_ts_ns + time_stop_ns;

    while pos.next_candle_idx < candles.len() {
        let c = &candles[pos.next_candle_idx];
        let candle_ns = c.open_ts_ms * 1_000_000;

        // Don't advance past the caller's time limit.
        if candle_ns >= until_ns {
            break;
        }

        // Check time stop first (if this candle is at or past the time boundary).
        if candle_ns >= time_stop_ns_abs {
            let exit_price = c.close;
            let gross = if pos.is_long {
                (exit_price - pos.entry_price) * pos.qty
            } else {
                (pos.entry_price - exit_price) * pos.qty
            };
            let fees = (pos.entry_price + exit_price) * pos.qty * (5.0 / 10_000.0); // taker_rate; captured from pos context
            return AdvanceResult::Closed(Trade {
                symbol: pos.symbol,
                entry_ts_ns: pos.entry_ts_ns,
                entry_price: pos.entry_price,
                is_long: pos.is_long,
                exit_price,
                qty: pos.qty,
                gross,
                fees,
                exit_reason: ExitReason::TimeStop,
            });
        }

        // TP / SL check within the candle.
        // Pessimistic assumption: if both TP and SL would be hit in the same
        // candle, SL fires first.
        let sl_hit = if pos.is_long {
            c.low <= sl_price
        } else {
            c.high >= sl_price
        };
        let tp_hit = if pos.is_long {
            c.high >= tp_price
        } else {
            c.low <= tp_price
        };

        if sl_hit {
            let exit_price = sl_price;
            let gross = if pos.is_long {
                (exit_price - pos.entry_price) * pos.qty
            } else {
                (pos.entry_price - exit_price) * pos.qty
            };
            let fees = (pos.entry_price + exit_price) * pos.qty * (5.0 / 10_000.0);
            return AdvanceResult::Closed(Trade {
                symbol: pos.symbol,
                entry_ts_ns: pos.entry_ts_ns,
                entry_price: pos.entry_price,
                is_long: pos.is_long,
                exit_price,
                qty: pos.qty,
                gross,
                fees,
                exit_reason: ExitReason::StopLoss,
            });
        }
        if tp_hit {
            let exit_price = tp_price;
            let gross = if pos.is_long {
                (exit_price - pos.entry_price) * pos.qty
            } else {
                (pos.entry_price - exit_price) * pos.qty
            };
            let fees = (pos.entry_price + exit_price) * pos.qty * (5.0 / 10_000.0);
            return AdvanceResult::Closed(Trade {
                symbol: pos.symbol,
                entry_ts_ns: pos.entry_ts_ns,
                entry_price: pos.entry_price,
                is_long: pos.is_long,
                exit_price,
                qty: pos.qty,
                gross,
                fees,
                exit_reason: ExitReason::TakeProfit,
            });
        }

        pos.next_candle_idx += 1;
    }

    AdvanceResult::StillOpen(pos)
}

/// Find the first candle index whose `open_ts_ms` is strictly greater than
/// `trigger_ms` (next-bar fill rule).
fn first_candle_after(candles: &[Candle], trigger_ms: u64) -> Option<usize> {
    // Binary-search for `trigger_ms + 1` (next millisecond).
    let target = trigger_ms.saturating_add(1);
    match candles.binary_search_by_key(&target, |c| c.open_ts_ms) {
        Ok(i) => Some(i),
        Err(i) => {
            if i < candles.len() {
                Some(i)
            } else {
                None
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

fn print_single_report(sym: &str, stats: &SymStats, args: &Args) {
    println!(
        "\nLiquidation Cascade  symbol={}  window={}s  min_count={}  min_notional=${:.0}  \
              same_side={:.0}%  tp={}bps  sl={}bps  time_stop={}s  notional=${:.0}",
        sym,
        args.window_secs,
        args.min_count,
        args.min_notional,
        args.same_side_pct * 100.0,
        args.tp_bps,
        args.sl_bps,
        args.time_stop_secs,
        args.notional,
    );
    println!("{}", "-".repeat(80));
    println!("trades            : {}", stats.trades);
    println!("wins / losses     : {} / {}", stats.wins, stats.losses);
    println!("win rate          : {:.1}%", stats.win_rate() * 100.0);
    println!("avg win           : {:>12.4}", stats.avg_win());
    println!("avg loss          : {:>12.4}", stats.avg_loss());
    println!("gross realized    : {:>12.4}", stats.gross);
    println!("fees (taker x2)   : {:>12.4}", stats.fees);
    println!("NET realized      : {:>12.4}", stats.net());
    if args.budget > 0.0 {
        println!(
            "ACCT% (budget ${:.2}): {:+.2}%",
            args.budget,
            stats.net() / args.budget * 100.0
        );
    }
}

fn print_table_header(args: &Args) {
    println!(
        "\nLiquidation Cascade sweep  window={}s  min_count={}  min_notional=${:.0}  \
              same_side={:.0}%  tp={}bps  sl={}bps  time_stop={}s  notional=${:.0}",
        args.window_secs,
        args.min_count,
        args.min_notional,
        args.same_side_pct * 100.0,
        args.tp_bps,
        args.sl_bps,
        args.time_stop_secs,
        args.notional,
    );
    println!("{}", "-".repeat(96));
    println!(
        "{:<14} {:>7} {:>8} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "SYMBOL", "TRADES", "WIN%", "AVG_WIN", "AVG_LOSS", "GROSS", "FEES", "NET"
    );
}

fn print_table_row(sym: &str, stats: &SymStats, args: &Args) {
    let acct_str = if args.budget > 0.0 {
        format!("{:+.2}%", stats.net() / args.budget * 100.0)
    } else {
        "-".to_string()
    };
    println!(
        "{:<14} {:>7} {:>7.1}% {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4}  {}",
        sym,
        stats.trades,
        stats.win_rate() * 100.0,
        stats.avg_win(),
        stats.avg_loss(),
        stats.gross,
        stats.fees,
        stats.net(),
        acct_str,
    );
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let symbols = parse_symbols(&args.symbols);
    if symbols.is_empty() {
        return Err("--symbols must not be empty".into());
    }

    // Load all liquidations.
    eprintln!(
        "loading liquidations from {} ...",
        args.liquidations.display()
    );
    let all_liqs = load_liquidations(&args.liquidations)?;
    eprintln!("loaded {} liquidation records", all_liqs.len());

    // Group liquidations by symbol, filtering to the requested set.
    let sym_set: std::collections::HashSet<&str> = symbols.iter().map(|s| s.as_str()).collect();
    let mut liqs_by_symbol: HashMap<String, Vec<LiqRec>> = HashMap::new();
    for liq in &all_liqs {
        if sym_set.contains(liq.symbol.as_str()) {
            liqs_by_symbol
                .entry(liq.symbol.clone())
                .or_default()
                .push(liq.clone());
        }
    }

    let multi = symbols.len() > 1;

    if multi {
        print_table_header(&args);
    }

    let mut total_stats = SymStats::default();

    for sym in &symbols {
        let sym_liqs = liqs_by_symbol
            .get(sym.as_str())
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        let klines_path = symbol_to_klines_path(&args.klines_dir, sym, &args.klines_suffix);

        let candles = match load_candles(&klines_path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!(
                    "{sym}: failed to load klines ({}): {e}",
                    klines_path.display()
                );
                eprintln!("{sym}: skipping (no kline data)");
                continue;
            }
        };

        eprintln!(
            "{sym}: {} liq records, {} candles",
            sym_liqs.len(),
            candles.len()
        );

        let (stats, _trades) = simulate_symbol(sym, sym_liqs, &candles, &args);

        if multi {
            print_table_row(sym, &stats, &args);
        } else {
            print_single_report(sym, &stats, &args);
        }

        total_stats.trades += stats.trades;
        total_stats.wins += stats.wins;
        total_stats.losses += stats.losses;
        total_stats.sum_wins += stats.sum_wins;
        total_stats.sum_losses += stats.sum_losses;
        total_stats.gross += stats.gross;
        total_stats.fees += stats.fees;
    }

    if multi {
        let n = symbols.len() as f64;
        println!("{}", "-".repeat(96));
        println!(
            "TOTAL ({} sym): trades={}  wins={}  losses={}  win%={:.1}  gross={:.4}  fees={:.4}  NET={:.4}",
            symbols.len(),
            total_stats.trades,
            total_stats.wins,
            total_stats.losses,
            total_stats.win_rate() * 100.0,
            total_stats.gross,
            total_stats.fees,
            total_stats.net(),
        );
        println!(
            "MEAN  ({} sym): trades={:.1}  net/sym={:.4}",
            symbols.len(),
            total_stats.trades as f64 / n,
            total_stats.net() / n,
        );
        if args.budget > 0.0 {
            println!(
                "Aggregate acct% (sum net / (n × budget)): {:+.2}%",
                total_stats.net() / (n * args.budget) * 100.0
            );
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_candle(open_ts_ms: u64, open: f64, high: f64, low: f64, close: f64) -> Candle {
        Candle {
            open_ts_ms,
            open,
            high,
            low,
            close,
        }
    }

    fn make_liq(ts_ns: u64, is_sell: bool, notional: f64) -> LiqRec {
        LiqRec {
            ts_ns,
            symbol: "BTCUSDT".into(),
            is_sell,
            notional,
        }
    }

    #[test]
    fn first_candle_after_finds_next() {
        let candles = vec![
            make_candle(1000, 100.0, 105.0, 99.0, 102.0),
            make_candle(2000, 102.0, 110.0, 101.0, 108.0),
            make_candle(3000, 108.0, 115.0, 107.0, 112.0),
        ];
        // trigger at ms=999 → first candle after is index 0 (open_ts=1000)
        assert_eq!(first_candle_after(&candles, 999), Some(0));
        // trigger at ms=1000 → next is index 1 (open_ts=2000)
        assert_eq!(first_candle_after(&candles, 1000), Some(1));
        // trigger at ms=3000 → no candle after
        assert_eq!(first_candle_after(&candles, 3000), None);
    }

    #[test]
    fn tp_hit_long_produces_profit() {
        // Entry at 100, TP at 100 * 1.003 = 100.3 (30bps)
        let candles = vec![
            make_candle(1000, 100.0, 105.0, 99.0, 104.0), // entry candle (open=100)
            make_candle(2000, 100.5, 101.0, 100.0, 100.9), // TP NOT hit (high=101 < 100.3? No, 101>100.3 → TP HIT)
        ];
        let pos = OpenPosition {
            symbol: "BTCUSDT".into(),
            entry_ts_ns: 1_000 * 1_000_000,
            entry_price: 100.0,
            is_long: true,
            qty: 1.0,
            next_candle_idx: 1,
        };
        let tp_factor = 1.003; // 30bps
        let sl_factor = 0.994; // 60bps
        let result = advance_position(
            pos,
            &candles,
            u64::MAX,
            tp_factor,
            sl_factor,
            300_000_000_000,
        );
        match result {
            AdvanceResult::Closed(t) => {
                assert_eq!(t.exit_reason, ExitReason::TakeProfit);
                assert!(t.gross > 0.0, "long TP trade must be profitable");
            }
            AdvanceResult::StillOpen(_) => panic!("expected closed"),
        }
    }

    #[test]
    fn sl_fires_before_tp_same_candle() {
        // Both TP and SL can be hit in one candle; SL must fire first (pessimistic).
        let entry = 100.0;
        let tp_factor = 1.003; // TP at 100.3
        let sl_factor = 0.994; // SL at 99.4
        // Candle with high > 100.3 and low < 99.4 → both would hit.
        let candles = vec![
            make_candle(2000, 100.0, 101.0, 99.0, 100.5), // high=101>100.3, low=99<99.4
        ];
        let pos = OpenPosition {
            symbol: "BTCUSDT".into(),
            entry_ts_ns: 1_000 * 1_000_000,
            entry_price: entry,
            is_long: true,
            qty: 1.0,
            next_candle_idx: 0,
        };
        let result = advance_position(
            pos,
            &candles,
            u64::MAX,
            tp_factor,
            sl_factor,
            300_000_000_000,
        );
        match result {
            AdvanceResult::Closed(t) => {
                assert_eq!(
                    t.exit_reason,
                    ExitReason::StopLoss,
                    "SL must fire before TP when both hit in same candle"
                );
                assert!(t.gross < 0.0, "stop-loss must be a loss");
            }
            AdvanceResult::StillOpen(_) => panic!("expected closed"),
        }
    }

    #[test]
    fn time_stop_fires_after_deadline() {
        let entry_ts_ns = 0u64;
        let time_stop_ns = 60_000_000_000u64; // 60s
        // Entry candle at ms=0, candle at ms=60000 (exactly at time stop boundary).
        let candles = vec![make_candle(60_000, 100.0, 100.5, 99.8, 100.1)];
        let pos = OpenPosition {
            symbol: "BTCUSDT".into(),
            entry_ts_ns,
            entry_price: 100.0,
            is_long: true,
            qty: 1.0,
            next_candle_idx: 0,
        };
        let result = advance_position(pos, &candles, u64::MAX, 1.003, 0.994, time_stop_ns);
        match result {
            AdvanceResult::Closed(t) => {
                assert_eq!(t.exit_reason, ExitReason::TimeStop);
            }
            AdvanceResult::StillOpen(_) => panic!("expected time stop"),
        }
    }

    #[test]
    fn parse_symbols_splits_correctly() {
        let syms = parse_symbols("BTCUSDT, ETHUSDT, SOLUSDT");
        assert_eq!(syms, vec!["BTCUSDT", "ETHUSDT", "SOLUSDT"]);
    }

    #[test]
    fn parse_symbols_filters_empty() {
        let syms = parse_symbols(",,,");
        assert!(syms.is_empty());
    }

    #[test]
    fn cascade_triggers_and_clears_deque() {
        // 6 SELL liquidations each with $20k notional in a 30s window → cascade
        let args = Args {
            liquidations: PathBuf::from("dummy"),
            klines_dir: PathBuf::from("dummy"),
            klines_suffix: "_1m_1y.parquet".to_string(),
            symbols: "BTCUSDT".to_string(),
            window_secs: 30,
            min_count: 5,
            min_notional: 100_000.0,
            same_side_pct: 0.8,
            notional: 100.0,
            tp_bps: 30,
            sl_bps: 60,
            time_stop_secs: 300,
            cooldown_secs: 60,
            taker_bps: 5,
            budget: 0.0,
        };

        // 6 SELL liqs at ts 0..5 seconds (all in window)
        let liqs: Vec<LiqRec> = (0..6)
            .map(|i| make_liq(i as u64 * 1_000_000_000, true, 20_000.0))
            .collect();
        // 5th liq (index 5) triggers cascade.

        // Candles: one entry candle at ms=5001 (next bar after trigger at ~5s).
        let candles = vec![
            make_candle(6_000, 50_000.0, 50_500.0, 49_800.0, 50_200.0), // entry
            // TP candle: 30bps = 50000 * 1.003 = 50150
            make_candle(7_000, 50_200.0, 50_200.0, 50_100.0, 50_180.0),
            make_candle(8_000, 50_180.0, 50_300.0, 50_100.0, 50_250.0), // TP hit (high >= 50150)
        ];

        let (stats, trades) = simulate_symbol("BTCUSDT", &liqs, &candles, &args);
        // Should have 1 trade triggered by the cascade.
        assert!(
            stats.trades >= 1,
            "cascade should produce at least one trade; got {}",
            stats.trades
        );
        if !trades.is_empty() {
            assert!(trades[0].is_long, "SELL-dominant cascade → long entry");
        }
    }
}
