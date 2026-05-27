//! Candle-driven backtester for the RSI-MR strategy.
//!
//! Loads a parquet from `download_klines` and drives `RsiMr` through it,
//! synthesizing one `MarketEvent::BookUpdate` + one `MarketEvent::Trade`
//! per closed candle. Fills are simulated against the NEXT candle's OHLC:
//!
//! - **Post-only BID at price P:** fills if next.low ≤ P (kline crossed).
//! - **Post-only ASK at price P:** fills if next.high ≥ P.
//! - **IOC:** immediate fill at the requested price (taker semantics).
//!
//! Tie-break (both barriers in same candle): pessimistic — SL wins.
//!
//! Fees: parameterized `--maker-bps` / `--taker-bps`. PostOnly = maker,
//! IOC = taker.

use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;

use clap::Parser;
use polars::prelude::*;
use rust_decimal::Decimal;
use tikr_core::{
    Asset, Fill, Level, MarketEvent, MarketKind, Notional, Position, Price, SignedSize, Size,
    Snapshot, Symbol, TimeInForce, Timestamp, VenueId,
};
use tikr_strategy::{Action, RsiMr, RsiMrConfig, Strategy, StrategyContext};
use tikr_venue::{QuoteId, QuoteIntent};

#[derive(Parser, Debug)]
#[command(
    name = "backtest_rsi_mr",
    about = "RSI-MR backtest over 1m kline parquet (candle-OHLC fill simulation)"
)]
struct Args {
    /// Path to a parquet produced by `download_klines`.
    #[arg(long, default_value = "./data/klines/ETHUSDT_1m_30d.parquet")]
    parquet: PathBuf,
    #[arg(long, default_value = "ETHUSDT")]
    symbol: String,
    /// Per-order notional in quote currency.
    #[arg(long, default_value = "100")]
    notional: String,
    /// Venue tick size.
    #[arg(long, default_value = "0.01")]
    tick_size: String,
    /// Venue lot step.
    #[arg(long, default_value = "0.001")]
    step_size: String,
    /// Venue min order notional.
    #[arg(long, default_value = "5")]
    min_notional: String,
    /// Bar interval (seconds). 60 for 1m klines.
    #[arg(long, default_value_t = 60u64)]
    bar_interval_secs: u64,
    #[arg(long, default_value_t = 200usize)]
    max_bars: usize,
    #[arg(long, default_value_t = 14u32)]
    rsi_period: u32,
    #[arg(long, default_value_t = 25u32)]
    rsi_buy_threshold: u32,
    #[arg(long, default_value_t = 50u32)]
    rsi_exit_threshold: u32,
    #[arg(long, default_value_t = 20u32)]
    ker_period: u32,
    #[arg(long, default_value = "0.4")]
    ker_max_trending: String,
    #[arg(long, default_value_t = 20u32)]
    vol_zscore_period: u32,
    #[arg(long, default_value = "1.5")]
    vol_zscore_min: String,
    #[arg(long, default_value_t = 14u32)]
    atr_period: u32,
    #[arg(long, default_value = "2")]
    atr_sl_mult: String,
    #[arg(long, default_value = "3")]
    atr_tp_mult: String,
    #[arg(long, default_value_t = 60u32)]
    max_hold_bars: u32,
    /// Maker fee in basis points (USDC promo = 0).
    #[arg(long, default_value_t = 0i32)]
    maker_bps: i32,
    /// Taker fee in basis points.
    #[arg(long, default_value_t = 5u32)]
    taker_bps: u32,
}

#[derive(Debug, Clone, Copy)]
struct Candle {
    open_ts_ms: u64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

fn load_candles(path: &PathBuf) -> Result<Vec<Candle>, Box<dyn std::error::Error>> {
    let df = LazyFrame::scan_parquet(path, ScanArgsParquet::default())?.collect()?;
    let ts = df.column("open_ts_ms")?.u64()?;
    let o = df.column("open")?.f64()?;
    let h = df.column("high")?.f64()?;
    let l = df.column("low")?.f64()?;
    let c = df.column("close")?.f64()?;
    let v = df.column("volume")?.f64()?;
    let mut out = Vec::with_capacity(df.height());
    for i in 0..df.height() {
        out.push(Candle {
            open_ts_ms: ts.get(i).ok_or("ts")?,
            open: o.get(i).ok_or("o")?,
            high: h.get(i).ok_or("h")?,
            low: l.get(i).ok_or("l")?,
            close: c.get(i).ok_or("c")?,
            volume: v.get(i).ok_or("v")?,
        });
    }
    Ok(out)
}

fn dec(f: f64) -> Decimal {
    Decimal::from_str(&format!("{f:.10}")).unwrap_or(Decimal::ZERO)
}

fn split_symbol(s: &str) -> (&str, &str) {
    for suffix in ["USDT", "USDC", "BUSD", "TUSD"] {
        if let Some(base) = s.strip_suffix(suffix)
            && !base.is_empty()
        {
            return (base, suffix);
        }
    }
    let n = s.len();
    (&s[..n - 4], &s[n - 4..])
}

#[derive(Debug, Clone)]
struct Resting {
    side: tikr_core::Side,
    price: Decimal,
    size: Decimal,
    tif: TimeInForce,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let candles = load_candles(&args.parquet)?;
    if candles.len() < 100 {
        return Err(format!("only {} candles loaded, need ≥100", candles.len()).into());
    }
    let (base, quote) = split_symbol(&args.symbol);
    let symbol = Symbol {
        base: Asset::new(base),
        quote: Asset::new(quote),
        venue: VenueId::new("binance"),
        kind: MarketKind::Perp,
    };
    let tick = Decimal::from_str(&args.tick_size)?;
    let cfg = RsiMrConfig {
        notional_per_order: Decimal::from_str(&args.notional)?,
        tick_size: tick,
        step_size: Decimal::from_str(&args.step_size)?,
        min_notional: Decimal::from_str(&args.min_notional)?,
        bar_interval_secs: args.bar_interval_secs,
        max_bars: args.max_bars,
        rsi_period: args.rsi_period,
        rsi_buy_threshold: args.rsi_buy_threshold,
        rsi_exit_threshold: args.rsi_exit_threshold,
        ker_period: args.ker_period,
        ker_max_trending: Decimal::from_str(&args.ker_max_trending)?,
        vol_zscore_period: args.vol_zscore_period,
        vol_zscore_min: Decimal::from_str(&args.vol_zscore_min)?,
        atr_period: args.atr_period,
        atr_sl_mult: Decimal::from_str(&args.atr_sl_mult)?,
        atr_tp_mult: Decimal::from_str(&args.atr_tp_mult)?,
        max_hold_bars: args.max_hold_bars,
    };
    let mut strat = RsiMr::new(cfg);

    let maker_bps = Decimal::from(args.maker_bps);
    let taker_bps = Decimal::from(args.taker_bps);
    let bps_denom = Decimal::from(10_000);

    // Position state (long-only).
    let mut pos_size = Decimal::ZERO;
    let mut pos_avg = Decimal::ZERO;
    let mut realized = Decimal::ZERO;
    let mut fees = Decimal::ZERO;
    let mut entries = 0u32;
    let mut tp_exits = 0u32;
    let mut sl_exits = 0u32;
    let mut rsi_exits = 0u32;
    let mut timeout_exits = 0u32;

    let mut resting: HashMap<QuoteId, Resting> = HashMap::new();
    let half_tick = tick / Decimal::from(2);

    for (i, c) in candles.iter().enumerate() {
        let close = dec(c.close);
        let bid_p = close - half_tick;
        let ask_p = close + half_tick;
        let ts_ns = c.open_ts_ms.saturating_mul(1_000_000) as u64;
        let snap = Snapshot {
            symbol: symbol.clone(),
            bids: vec![Level {
                price: Price(bid_p),
                size: Size(Decimal::from(100)),
            }],
            asks: vec![Level {
                price: Price(ask_p),
                size: Size(Decimal::from(100)),
            }],
            ts: Timestamp(ts_ns),
        };
        let position = Position {
            symbol: symbol.clone(),
            size: SignedSize(pos_size),
            avg_entry: Price(pos_avg),
            realized_pnl: Notional(realized),
        };
        let open_quotes: Vec<(QuoteId, QuoteIntent)> = resting
            .iter()
            .map(|(id, r)| {
                (
                    *id,
                    QuoteIntent {
                        symbol: symbol.clone(),
                        side: r.side,
                        price: Price(r.price),
                        size: Size(r.size),
                        tif: r.tif,
                        kind: tikr_core::QuoteKind::Point,
                    },
                )
            })
            .collect();
        let ctx = StrategyContext {
            symbol: &symbol,
            now: Timestamp(ts_ns),
            position: &position,
            recent_fills: &[],
            latest_book: &snap,
            open_quotes: &open_quotes,
            recent_liqs: &[],
        };
        // Emit BookUpdate + Trade to drive bar aggregation + indicators.
        let actions_book = strat.on_event(&ctx, &MarketEvent::BookUpdate { snapshot: snap.clone() });
        let trade = MarketEvent::Trade {
            symbol: symbol.clone(),
            price: Price(close),
            size: Size(dec(c.volume)),
            side: tikr_core::Side::Bid,
            ts: Timestamp(ts_ns),
        };
        let actions_trade = strat.on_event(&ctx, &trade);

        // Apply new actions: book updates resting set; cancels remove; quotes added.
        for action in actions_book.into_iter().chain(actions_trade.into_iter()) {
            match action {
                Action::Quote(intent) => {
                    let id = QuoteId::new();
                    // IOC fills immediately at the requested price (taker).
                    if intent.tif == TimeInForce::IOC {
                        let fill_price = intent.price.0;
                        let fill_size = intent.size.0;
                        let fee_bps = taker_bps;
                        let notional = fill_price * fill_size;
                        let fee = notional * fee_bps / bps_denom;
                        fees += fee;
                        match intent.side {
                            tikr_core::Side::Bid => {
                                // Long entry via taker (rare for this strat).
                                let new_size = pos_size + fill_size;
                                if new_size > Decimal::ZERO {
                                    pos_avg =
                                        (pos_avg * pos_size + fill_price * fill_size) / new_size;
                                }
                                pos_size = new_size;
                                entries += 1;
                            }
                            tikr_core::Side::Ask => {
                                // Long exit via taker (IOC SL or timeout).
                                if pos_size <= Decimal::ZERO {
                                    continue;
                                }
                                let exit_size = fill_size.min(pos_size);
                                let pnl = (fill_price - pos_avg) * exit_size;
                                let was_loss = fill_price < pos_avg;
                                realized += pnl;
                                pos_size -= exit_size;
                                if pos_size <= Decimal::ZERO {
                                    pos_size = Decimal::ZERO;
                                    pos_avg = Decimal::ZERO;
                                }
                                if was_loss {
                                    sl_exits += 1;
                                } else {
                                    timeout_exits += 1;
                                }
                            }
                        }
                    } else {
                        // PostOnly — add to resting, fill checked against NEXT candle.
                        // First remove any earlier maker resting at the same price+side
                        // (strategy occasionally re-emits at the same level).
                        let key_side = intent.side;
                        let key_price = intent.price.0;
                        resting.retain(|_, r| !(r.side == key_side && r.price == key_price));
                        resting.insert(
                            id,
                            Resting {
                                side: intent.side,
                                price: intent.price.0,
                                size: intent.size.0,
                                tif: intent.tif,
                            },
                        );
                    }
                }
                Action::Cancel(id) => {
                    resting.remove(&id);
                }
                _ => {}
            }
        }

        // Fill simulation: check resting orders against NEXT candle's OHLC range.
        if let Some(next) = candles.get(i + 1) {
            let next_high = dec(next.high);
            let next_low = dec(next.low);
            let next_ts_ns = next.open_ts_ms.saturating_mul(1_000_000) as u64;
            let mut filled: Vec<(QuoteId, Resting)> = Vec::new();
            // Strategy is long-only — BID = entry, ASK = exit. Process BIDs
            // before ASKs so an entry+exit in the same candle is ordered correctly.
            for (id, r) in resting.iter() {
                let crossed = match r.side {
                    tikr_core::Side::Bid => next_low <= r.price,
                    tikr_core::Side::Ask => next_high >= r.price,
                };
                if crossed {
                    filled.push((*id, r.clone()));
                }
            }
            filled.sort_by_key(|(_, r)| matches!(r.side, tikr_core::Side::Ask));
            for (id, r) in filled {
                resting.remove(&id);
                let fill_price = r.price;
                // ASK fills are exits — clamp to current long. If we're
                // already flat, ignore (resting stale ASK from earlier
                // emits that the strategy hasn't cancelled yet).
                let effective_size = match r.side {
                    tikr_core::Side::Bid => r.size,
                    tikr_core::Side::Ask => {
                        if pos_size <= Decimal::ZERO {
                            continue;
                        }
                        r.size.min(pos_size)
                    }
                };
                let notional = fill_price * effective_size;
                let fee = (notional * maker_bps) / bps_denom;
                fees += fee;
                let pre_avg = pos_avg;
                match r.side {
                    tikr_core::Side::Bid => {
                        let new_size = pos_size + effective_size;
                        if new_size > Decimal::ZERO {
                            pos_avg =
                                (pos_avg * pos_size + fill_price * effective_size) / new_size;
                        }
                        pos_size = new_size;
                        entries += 1;
                    }
                    tikr_core::Side::Ask => {
                        let pnl = (fill_price - pre_avg) * effective_size;
                        realized += pnl;
                        pos_size -= effective_size;
                        if pos_size <= Decimal::ZERO {
                            pos_size = Decimal::ZERO;
                            pos_avg = Decimal::ZERO;
                        }
                        if pnl >= Decimal::ZERO {
                            tp_exits += 1;
                        } else {
                            rsi_exits += 1;
                        }
                    }
                }
                // Feed the fill back into the strategy so its position
                // state stays consistent with the harness — otherwise the
                // strat thinks it's perpetually pending and floods exits.
                let fill_ev = Fill {
                    quote_id: id,
                    price: Price(fill_price),
                    size: Size(effective_size),
                    fee_asset: Asset::new(quote),
                    fee_amount: Decimal::ZERO,
                    fee_quote: Notional(fee),
                    side: r.side,
                    ts: Timestamp(next_ts_ns),
                    is_full: true,
                };
                let position_after = Position {
                    symbol: symbol.clone(),
                    size: SignedSize(pos_size),
                    avg_entry: Price(pos_avg),
                    realized_pnl: Notional(realized),
                };
                let open_quotes_after: Vec<(QuoteId, QuoteIntent)> = resting
                    .iter()
                    .map(|(id, r)| {
                        (
                            *id,
                            QuoteIntent {
                                symbol: symbol.clone(),
                                side: r.side,
                                price: Price(r.price),
                                size: Size(r.size),
                                tif: r.tif,
                                kind: tikr_core::QuoteKind::Point,
                            },
                        )
                    })
                    .collect();
                let ctx_fill = StrategyContext {
                    symbol: &symbol,
                    now: Timestamp(next_ts_ns),
                    position: &position_after,
                    recent_fills: &[fill_ev.clone()],
                    latest_book: &snap,
                    open_quotes: &open_quotes_after,
                    recent_liqs: &[],
                };
                // Strategy's Fill handling may emit follow-up TP/Exit ASKs.
                let post_actions = strat.on_event(&ctx_fill, &MarketEvent::Fill(fill_ev));
                for a in post_actions {
                    match a {
                        Action::Quote(intent) => {
                            if intent.tif == TimeInForce::IOC {
                                // Follow-up IOC — treat as immediate taker fill at requested price.
                                let fp = intent.price.0;
                                let fs = intent.size.0;
                                let n = fp * fs;
                                let f = n * taker_bps / bps_denom;
                                fees += f;
                                match intent.side {
                                    tikr_core::Side::Bid => {
                                        let new_size = pos_size + fs;
                                        if new_size > Decimal::ZERO {
                                            pos_avg = (pos_avg * pos_size + fp * fs) / new_size;
                                        }
                                        pos_size = new_size;
                                        entries += 1;
                                    }
                                    tikr_core::Side::Ask => {
                                        if pos_size <= Decimal::ZERO {
                                            continue;
                                        }
                                        let exit_size = fs.min(pos_size);
                                        let pnl = (fp - pos_avg) * exit_size;
                                        realized += pnl;
                                        pos_size -= exit_size;
                                        if pos_size <= Decimal::ZERO {
                                            pos_size = Decimal::ZERO;
                                            pos_avg = Decimal::ZERO;
                                        }
                                        if pnl < Decimal::ZERO {
                                            sl_exits += 1;
                                        } else {
                                            timeout_exits += 1;
                                        }
                                    }
                                }
                            } else {
                                let new_id = QuoteId::new();
                                let key_side = intent.side;
                                let key_price = intent.price.0;
                                resting.retain(|_, r| {
                                    !(r.side == key_side && r.price == key_price)
                                });
                                resting.insert(
                                    new_id,
                                    Resting {
                                        side: intent.side,
                                        price: intent.price.0,
                                        size: intent.size.0,
                                        tif: intent.tif,
                                    },
                                );
                            }
                        }
                        Action::Cancel(id) => {
                            resting.remove(&id);
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    // Mark final unrealized (rare — should be flat at end if strategy works).
    let last_close = dec(candles.last().unwrap().close);
    let unrealized = (last_close - pos_avg) * pos_size;
    let net = realized + unrealized - fees;
    let span_h = (candles.last().unwrap().open_ts_ms - candles.first().unwrap().open_ts_ms) as f64
        / 3_600_000.0;

    println!("--- RSI-MR backtest on {} ---", args.symbol);
    println!("candles      : {}", candles.len());
    println!("span_hours   : {span_h:.1}");
    println!("entries      : {entries}");
    println!("tp_exits     : {tp_exits}");
    println!("sl_exits     : {sl_exits}");
    println!("rsi_exits    : {rsi_exits}");
    println!("timeout_exits: {timeout_exits}  (heuristic; counted as taker)");
    println!("realized     : {realized}");
    println!("unrealized   : {unrealized}  (residual position={pos_size} avg={pos_avg})");
    println!("fees         : {fees}");
    println!("NET          : {net}");
    if entries > 0 {
        println!("$/entry      : {}", net / Decimal::from(entries));
    }
    Ok(())
}
