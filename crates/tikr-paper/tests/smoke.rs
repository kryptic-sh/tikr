//! Phase 3 capstone smoke test — runs the paper runner against Hyperliquid
//! testnet for 5 minutes. Gated with `#[ignore]` so CI doesn't burn on every PR.
//!
//! Run manually: `cargo test -p tikr-paper --test smoke -- --ignored`.

use std::time::Duration;
use tempfile::TempDir;
use tikr_backtest::fill_sim::{FillSim, FillSimConfig, VenueFees};
use tikr_core::{Asset, Decimal, MarketKind, Symbol, VenueId};
use tikr_hyperliquid::{Hyperliquid, HyperliquidConfig, HyperliquidEnv};
use tikr_paper::{RunnerConfig, run};
use tikr_strategy::{LayeredGrid, LayeredGridConfig, Strategy};
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
    let strategy = LayeredGrid::new(LayeredGridConfig {
        notional_per_order: Decimal::from(25),
        levels_per_side: 1,
        inner_bps: 20,
        max_position_usdt: Decimal::ZERO,
        take_profit_bps: 0,
        stop_loss_bps: 0,
    });
    let fill_sim = FillSim::new(FillSimConfig {
        submit_latency_ms: 50,
        cancel_latency_ms: 0,
        fees: VenueFees {
            maker_bps: 0,
            taker_bps: 0,
        },
        max_position_notional_usdt: None,
        leverage: rust_decimal::Decimal::ZERO,
        max_position_frac: rust_decimal::Decimal::ZERO,
        silent_cancel_rate_per_min: 0.0,
        rng_seed: 0,
        latency_jitter_ms: 0,
        max_open_orders: None,
        queue_cancel_decay_per_sec: 0.0,
    });
    let config = RunnerConfig {
        state_dir: temp.path().to_path_buf(),
        snapshot_every_n_events: 100,
        skim: None,
        funding: None,
        snapshot_tap: None,
        live_tap: None,
        notional_rx: None,
        max_position_rx: None,
        wallet_rx: None,
        take_profit_pct: tikr_core::Decimal::ZERO,
        liq_window_secs: 0,
        seed_position: None,
        equity_csv_path: None,
        initial_balance: Decimal::ZERO,
        order_balance_pct: Decimal::ZERO,
        max_position_pct: Decimal::ZERO,
        min_notional: Decimal::ZERO,
        max_expected_open_orders: 2,
        liquidation: None,
        mark_series: None,
        retrace_boundary_ts: None,
        inventory_boost: None,
        bagger: tikr_paper::bagger::BaggerConfig::default(),
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
