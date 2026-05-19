//! Phase 2 capstone: end-to-end regression of all three reference strategies
//! against the same synthetic dataset.
//!
//! Does NOT assert "strategy X better than Y" — a synthetic 1-trade dataset
//! cannot validate that. Real comparison waits for Phase 3 paper-trading.
//!
//! The dataset is sized to walk the EWMA volatility estimator (used by A-S
//! and GLFT) past `WARMUP_COUNT (= 30)` computed-return samples so all three
//! strategies actually quote and fill once.
//!
//! Fixture-write helpers (`write_book_parquet`, `write_trades_parquet`) are
//! intentionally duplicated from `tests/golden.rs` per the Phase 2 spec —
//! refactor to a shared helpers module is deferred until a 4th caller exists.

use std::fs::File;
use std::path::Path;

use polars::prelude::*;
use tempfile::TempDir;

use tikr_backtest::fill_sim::{FillSim, FillSimConfig, VenueFees};
use tikr_backtest::pnl::PnLReport;
use tikr_backtest::replay::{ParquetReplay, ReplayConfig};
use tikr_backtest::runner::run;
use tikr_core::{Asset, Decimal, MarketKind, Notional, Size, Symbol, VenueId};
use tikr_strategy::{
    AvellanedaStoikov, AvellanedaStoikovConfig, EwmaConfig, Glft, GlftConfig, NaiveGrid,
    NaiveGridConfig, Strategy,
};

// ---------------------------------------------------------------------------
// Shared setup
// ---------------------------------------------------------------------------

fn make_symbol() -> Symbol {
    Symbol {
        base: Asset::new("BTC"),
        quote: Asset::new("USDT"),
        venue: VenueId::new("test"),
        kind: MarketKind::Spot,
    }
}

/// Synthetic book stream:
/// - rows 1+2 (ts=1.0s): bid then ask appear → first full mid=100 seeds the EWMA estimator
/// - rows 3..32 (ts=1.1s..4.0s @ 100ms): 30 perturbations, prices unchanged → 30 computed returns
///   (after row 32 `samples_seen == 30`, A-S / GLFT exit warmup and quote)
/// - rows 33+34 (ts=30.0s): final book updates, no change
fn build_book_rows() -> Vec<(u64, i64, f64, f64, u64)> {
    let mut rows = vec![
        (1_000_000_000, 0, 99.5, 1.0, 1),
        (1_000_000_000, 1, 100.5, 1.0, 2),
    ];
    for i in 0..30u64 {
        let ts = 1_100_000_000 + i * 100_000_000;
        let side = (i % 2) as i64; // 0 = bid, 1 = ask
        let price = if side == 0 { 99.5 } else { 100.5 };
        let seq = 3 + i;
        rows.push((ts, side, price, 1.0, seq));
    }
    rows.push((30_000_000_000, 0, 99.5, 1.0, 33));
    rows.push((30_000_000_000, 1, 100.5, 1.0, 34));
    rows
}

fn naive_grid_config() -> NaiveGridConfig {
    NaiveGridConfig {
        levels_per_side: 1,
        base_spread_bps: 50, // 0.5% → bid=99.5, ask=100.5 at mid=100
        level_step_bps: 10,
        size_per_quote: Size(Decimal::from(1)),
        min_requote_interval_ms: 100_000, // 100s > test horizon → quote once
    }
}

fn avellaneda_stoikov_config() -> AvellanedaStoikovConfig {
    AvellanedaStoikovConfig {
        gamma: Decimal::try_from(0.1).unwrap(),
        // 30 bps → bid = 100.5*(1-0.003) ≈ 99.8 at mid=100.5; different from naive-grid's 99.5
        base_spread_bps: 30,
        horizon_sec: 3600,
        size_per_quote: Size(Decimal::from(1)),
        min_requote_interval_ms: 100_000,
        level_step_bps: 10,
        volatility: EwmaConfig {
            half_life_sec: 60.0,
            initial_var: Decimal::try_from(0.0001).unwrap(),
        },
    }
}

fn glft_config() -> GlftConfig {
    GlftConfig {
        gamma: Decimal::try_from(0.1).unwrap(),
        // 20 bps → bid = 100.5*(1-0.002) ≈ 99.9 at mid=100.5; different from A-S (30 bps)
        base_spread_bps: 20,
        size_per_quote: Size(Decimal::from(1)),
        min_requote_interval_ms: 100_000,
        level_step_bps: 10,
        volatility: EwmaConfig {
            half_life_sec: 60.0,
            initial_var: Decimal::try_from(0.0001).unwrap(),
        },
    }
}

fn fill_sim_config() -> FillSimConfig {
    FillSimConfig {
        submit_latency_ms: 10,
        cancel_latency_ms: 0, // instant cancel — avoids race per #15 precedent
        fees: VenueFees {
            maker_bps: 0,
            taker_bps: 0,
        },
    }
}

async fn run_strategy<S: Strategy>(strategy: S) -> PnLReport {
    let temp = TempDir::new().unwrap();
    write_book_parquet(temp.path(), "BTC", "2026-05-18", &build_book_rows()).unwrap();
    write_trades_parquet(
        temp.path(),
        "BTC",
        "2026-05-18",
        // (ts_ns, price, size, taker_side, trade_id)
        // taker_side=1 (Ask): someone sold into the book → eligible to fill a resting Bid
        &[(20_000_000_000, 99.0, 1.0, 1, 1)],
    )
    .unwrap();
    let symbol = make_symbol();
    let replay = ParquetReplay::new(ReplayConfig {
        heartbeat_ms: 0,
        symbols: vec![symbol.clone()],
        data_dir: temp.path().to_path_buf(),
    })
    .unwrap();
    let fill_sim = FillSim::new(fill_sim_config());
    run(replay, strategy, fill_sim, symbol).await
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn naive_grid_runs() {
    let report = run_strategy(NaiveGrid::new(naive_grid_config())).await;
    assert_eq!(
        report.fees,
        Notional(Decimal::ZERO),
        "zero-fee config should produce zero fees"
    );
    assert!(
        report.net.0.abs() < Decimal::from(100),
        "net P&L should be bounded (no runaway inventory), got {:?}",
        report.net
    );
}

#[tokio::test]
async fn avellaneda_stoikov_runs() {
    let report = run_strategy(AvellanedaStoikov::new(avellaneda_stoikov_config())).await;
    assert_eq!(
        report.fees,
        Notional(Decimal::ZERO),
        "zero-fee config should produce zero fees"
    );
    assert!(
        report.net.0.abs() < Decimal::from(100),
        "net P&L should be bounded (no runaway inventory), got {:?}",
        report.net
    );
}

#[tokio::test]
async fn glft_runs() {
    let report = run_strategy(Glft::new(glft_config())).await;
    assert_eq!(
        report.fees,
        Notional(Decimal::ZERO),
        "zero-fee config should produce zero fees"
    );
    assert!(
        report.net.0.abs() < Decimal::from(100),
        "net P&L should be bounded (no runaway inventory), got {:?}",
        report.net
    );
}

#[tokio::test]
async fn all_three_strategies_produce_distinct_pnl() {
    let net_ng = run_strategy(NaiveGrid::new(naive_grid_config())).await.net;
    let net_as = run_strategy(AvellanedaStoikov::new(avellaneda_stoikov_config()))
        .await
        .net;
    let net_glft = run_strategy(Glft::new(glft_config())).await.net;
    // Degeneration guard: if two strategies collapsed to the same quote math,
    // their net P&L would coincide. We only require ONE pair to differ —
    // synthetic 1-trade data can't justify a stronger claim.
    assert!(
        net_ng != net_as || net_as != net_glft,
        "at least one pair of strategy net P&L values should differ \
         (net_ng={net_ng:?}, net_as={net_as:?}, net_glft={net_glft:?}) — \
         possible degeneration bug"
    );
}

// ---------------------------------------------------------------------------
// Fixture write helpers (duplicated verbatim from tests/golden.rs; see header).
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
