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
use tikr_core::{Asset, Decimal, MarketKind, Notional, Size, Symbol, VenueId};
use tikr_strategy::{NaiveGrid, NaiveGridConfig, Strategy};

#[tokio::test]
async fn golden_naive_grid_btc_single_fill() {
    let temp = TempDir::new().unwrap();
    write_book_parquet(
        temp.path(),
        "BTC",
        "2026-05-18",
        &[
            // (ts_ns, side, price, size, seq)
            (1_000_000_000, 0, 99.5, 1.0, 1),   // bid level appears
            (1_000_000_000, 1, 100.5, 1.0, 2),  // ask level appears (book complete: mid=100)
            (30_000_000_000, 0, 99.5, 1.0, 3),  // bid unchanged (still 99.5)
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
            (20_000_000_000, 99.5, 1.0, 1, 1), // Ask-taker (someone sold) hits our bid
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
    })
    .unwrap();

    let strategy = NaiveGrid::new(NaiveGridConfig {
        levels_per_side: 1,
        base_spread_bps: 50, // 0.5% half-spread → bid=99.5, ask=100.5 at mid=100
        level_step_bps: 10,  // > 0 to avoid div-by-zero threshold; irrelevant at 1 level
        size_per_quote: Size(Decimal::from(1)),
        min_requote_interval_ms: 100_000, // 100s > test duration → quote ONCE at first valid book
    });

    let fill_sim = FillSim::new(FillSimConfig {
        submit_latency_ms: 10,
        cancel_latency_ms: 0, // instant cancel for golden — race semantics covered in #11 tests
        fees: VenueFees {
            maker_bps: 0,
            taker_bps: 0,
        },
    });

    let report = run(replay, strategy, fill_sim, symbol).await;

    // Hand-computed expected P&L:
    //
    // Timeline (heartbeats off, single-level grid, quote-once strategy):
    //   ts=1s   BookUpdate (bid only)         → strategy: no mid, no quote
    //   ts=1s   BookUpdate (full book mid=100)→ strategy: cold-start emit
    //                                           [CancelAll, Quote(Bid@99.5), Quote(Ask@100.5)]
    //                                           FillSim: CancelAll instant (no-op), Place×2 pending @ts=1.01s
    //   ts=20s  Trade(99.5, taker=Ask, size=1)→ FillSim: apply_pending → quotes go live;
    //                                           match_trade: Bid@99.5 eligible (Ask-taker, 99.5<=99.5)
    //                                           → Fill: Bid@99.5 size=1 fee=0
    //                                           Tracker: long 1 @ 99.5, realized=0, fees=0
    //   ts=30s  BookUpdate × 2 (no change)    → strategy: no requote (interval not elapsed, no drift)
    //                                           last_mid = (99.5 + 100.5)/2 = 100
    //
    // Final report (last_mid = 100):
    //   realized   = 0
    //   unrealized = (100 - 99.5) * 1 = 0.5
    //   fees       = 0
    //   funding    = 0
    //   net        = 0 + 0.5 - 0 + 0 = 0.5
    //
    // last updated: 2026-05-18
    let expected_net = Decimal::from(1) / Decimal::from(2);
    assert_eq!(
        report.net,
        Notional(expected_net),
        "PnL drift — update expected only if change is intentional. See README for regen protocol."
    );
    assert_eq!(report.realized, Notional(Decimal::ZERO));
    assert_eq!(report.unrealized, Notional(expected_net));
    assert_eq!(report.fees, Notional(Decimal::ZERO));
    assert_eq!(report.funding, Notional(Decimal::ZERO));
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
