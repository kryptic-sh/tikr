//! Golden regression test for the full Phase 1 backtest stack.
//!
//! Phase 1 deviation from issue #15 spec: this test uses a synthesized
//! tiny deterministic dataset (~5 events) rather than the spec's "1 hour
//! of real Hyperliquid BTC data". Reasons:
//! - We have no Hyperliquid recorder yet (bin is `todo!()`).
//! - Deterministic synthetic data → hand-computable expected P&L.
//! - 10MB binary blob in git is harder to review than ~30 lines of code.
//!
//! Real Hyperliquid data swaps in when the recorder bin ships.

use std::fs::File;
use std::path::Path;

use polars::prelude::*;
use tempfile::TempDir;

use tikr_backtest::fill_sim::{FillSim, FillSimConfig, VenueFees};
use tikr_backtest::replay::{ParquetReplay, ReplayConfig};
use tikr_backtest::runner::run;
use tikr_core::{Asset, Decimal, MarketKind, Notional, Symbol, VenueId};
use tikr_strategy::{LayeredGrid, LayeredGridConfig, Strategy};

#[tokio::test]
async fn golden_layered_grid_btc_single_fill() {
    let temp = TempDir::new().unwrap();
    write_book_parquet(
        temp.path(),
        "BTC",
        "2026-05-18",
        &[
            // (ts_ns, side, price, size, seq)
            (1_000_000_000, 0, 99.5, 1.0, 1),   // bid level appears
            (1_000_000_000, 1, 100.5, 1.0, 2),  // ask level appears (book complete: mid=100)
            (30_000_000_000, 0, 99.5, 1.0, 3),  // bid unchanged
            (30_000_000_000, 1, 100.5, 1.0, 4), // ask unchanged
        ],
    )
    .unwrap();
    write_trades_parquet(
        temp.path(),
        "BTC",
        "2026-05-18",
        &[
            // (ts_ns, price, size, taker_side, trade_id)
            (20_000_000_000, 99.5, 2.0, 1, 1), // Ask-taker (someone sold) hits our bid
        ],
    )
    .unwrap();

    let symbol = Symbol {
        base: Asset::new("BTC"),
        quote: Asset::new("USDT"),
        venue: VenueId::new("test"),
        kind: MarketKind::Spot,
    };

    let replay = ParquetReplay::new(ReplayConfig {
        heartbeat_ms: 0, // suppress for golden — fewer events to reason about
        symbols: vec![symbol.clone()],
        data_dir: temp.path().to_path_buf(),
        tick_size: tikr_core::Decimal::ONE,
        allow_seq_gaps: false,
    })
    .unwrap();

    // LayeredGrid: $100 notional, 50bps inner → bid = mid*(1-0.005).
    // mid = (99.5+100.5)/2 = 100 → bid = 99.5 exactly.
    // qty = 100 / 99.5 ≈ 1.00503 BTC.
    let strategy = LayeredGrid::new(LayeredGridConfig {
        notional_per_order: Decimal::from(100), // $100 notional
        levels_per_side: 1,
        inner_bps: 50, // 0.5% half-spread → bid@99.5 at mid=100
        max_position_usdt: Decimal::ZERO,
        take_profit_bps: 0,
        stop_loss_bps: 0,
    });

    let fill_sim = FillSim::new(FillSimConfig {
        submit_latency_ms: 10,
        cancel_latency_ms: 0,
        fees: VenueFees {
            maker_bps: 0,
            taker_bps: 0,
        },

        max_position_notional_usdt: None,
        silent_cancel_rate_per_min: 0.0,
        rng_seed: 0,
        latency_jitter_ms: 0,
        max_open_orders: None,
    });

    let report = run(replay, strategy, fill_sim, symbol).await;

    // Timeline (heartbeats off, cold-start placement):
    //   ts=1s   BookUpdate (bid only)            → LG: no full book, no quote
    //   ts=1s   BookUpdate (full book mid=100)   → LG: cold-start emit Quote(Bid@99.5)
    //                                               FillSim: pending @ts=1.01s
    //   ts=20s  Trade(99.5, taker=Ask, size=2.0) → FillSim: quotes go live; match_trade
    //                                               Bid@99.5 eligible → Fill qty=100/99.5
    //                                               Tracker: long (100/99.5) @ 99.5, realized=0
    //   ts=30s  BookUpdate × 2 → LG no-op (fill-driven after cold start)
    //                                               last_mid = 100
    //
    // Final report (last_mid = 100):
    //   realized   = 0 (position not yet closed)
    //   fees       = 0
    //   unrealized = (100 - 99.5) × qty = 0.5 × (100/99.5) > 0
    //   net > 0
    assert_eq!(
        report.realized,
        Notional(Decimal::ZERO),
        "no realized P&L expected — position not closed"
    );
    assert_eq!(report.fees, Notional(Decimal::ZERO));
    assert_eq!(report.funding, Notional(Decimal::ZERO));
    assert!(
        report.net.0 > Decimal::ZERO,
        "net P&L should be positive (long position with mid above entry), got {:?}",
        report.net
    );
}

// ---------------------------------------------------------------------------
// Fixture write helpers (duplicated from replay.rs unit tests; see header).
// Schema follows SCHEMA.md with the Phase 1 f64/i64 fallbacks the loader
// accepts (column_as_decimal / column_as_u8).
// ---------------------------------------------------------------------------

fn write_book_parquet(
    dir: &Path,
    symbol: &str,
    date: &str,
    rows: &[(u64, i64, f64, f64, u64)],
) -> Result<(), Box<dyn std::error::Error>> {
    let path = dir.join(format!("book_{symbol}_{date}.parquet"));
    let ts: Vec<u64> = rows.iter().map(|r| r.0).collect();
    let side: Vec<i64> = rows.iter().map(|r| r.1).collect();
    let price: Vec<f64> = rows.iter().map(|r| r.2).collect();
    let size: Vec<f64> = rows.iter().map(|r| r.3).collect();
    let seq: Vec<u64> = rows.iter().map(|r| r.4).collect();
    let mut df = df!(
        "ts_ns" => ts,
        "side" => side,
        "price" => price,
        "size" => size,
        "seq" => seq,
    )?;
    let file = File::create(path)?;
    ParquetWriter::new(file).finish(&mut df)?;
    Ok(())
}

fn write_trades_parquet(
    dir: &Path,
    symbol: &str,
    date: &str,
    rows: &[(u64, f64, f64, i64, u64)],
) -> Result<(), Box<dyn std::error::Error>> {
    let path = dir.join(format!("trades_{symbol}_{date}.parquet"));
    let ts: Vec<u64> = rows.iter().map(|r| r.0).collect();
    let price: Vec<f64> = rows.iter().map(|r| r.1).collect();
    let size: Vec<f64> = rows.iter().map(|r| r.2).collect();
    let taker: Vec<i64> = rows.iter().map(|r| r.3).collect();
    let trade_id: Vec<u64> = rows.iter().map(|r| r.4).collect();
    let mut df = df!(
        "ts_ns" => ts,
        "price" => price,
        "size" => size,
        "taker_side" => taker,
        "trade_id" => trade_id,
    )?;
    let file = File::create(path)?;
    ParquetWriter::new(file).finish(&mut df)?;
    Ok(())
}
