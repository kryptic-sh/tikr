//! Phase 3 capstone smoke test — runs the paper runner against Hyperliquid
//! testnet for 5 minutes. Gated with `#[ignore]` so CI doesn't burn on every PR.
//!
//! Run manually: `cargo test -p tikr-paper --test smoke -- --ignored`.

use std::time::Duration;
use tempfile::TempDir;
use tikr_backtest::fill_sim::{FillSim, FillSimConfig, VenueFees};
use tikr_core::{Asset, Decimal, MarketKind, Size, Symbol, VenueId};
use tikr_hyperliquid::{Hyperliquid, HyperliquidConfig, HyperliquidEnv};
use tikr_paper::{RunnerConfig, run};
use tikr_strategy::{NaiveGrid, NaiveGridConfig, Strategy};
use tokio::sync::watch;

#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn paper_runner_against_testnet_5min() {
    let temp = TempDir::new().unwrap();
    let symbol = Symbol {
        base: Asset::new("BTC"),
        quote: Asset::new("USDC"),
        venue: VenueId::new("hyperliquid"),
        kind: MarketKind::Perp,
    };

    let venue = Hyperliquid::with_config(HyperliquidConfig {
        env: HyperliquidEnv::Testnet,
        ..Default::default()
    });
    let strategy = NaiveGrid::new(NaiveGridConfig {
        levels_per_side: 1,
        base_spread_bps: 50,
        level_step_bps: 10,
        size_per_quote: Size(Decimal::try_from(0.01).unwrap()),
        min_requote_interval_ms: 5000,
    });
    let fill_sim = FillSim::new(FillSimConfig {
        submit_latency_ms: 50,
        cancel_latency_ms: 0,
        fees: VenueFees {
            maker_bps: 0,
            taker_bps: 0,
        },
    });
    let config = RunnerConfig {
        state_dir: temp.path().to_path_buf(),
        snapshot_every_n_events: 100,
    };

    let (tx, rx) = watch::channel(false);
    let tx_timer = tx.clone();
    let timer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(5 * 60)).await;
        let _ = tx_timer.send(true);
    });

    let report = tokio::time::timeout(
        Duration::from_secs(6 * 60), // 1-minute grace period over the 5-minute cap
        run(venue, strategy, fill_sim, symbol, rx, config),
    )
    .await
    .expect("runner did not exit within timeout");

    timer.await.unwrap();
    assert!(
        report.events_processed > 0,
        "expected at least one event from testnet"
    );
    assert!(
        report.runtime_secs >= 280,
        "runtime should be ~5min (got {})",
        report.runtime_secs
    );
}
