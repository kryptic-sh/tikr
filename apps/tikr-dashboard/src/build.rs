//! `BotConfig` → `BotSpec` conversion + per-symbol min-notional auto-bump.

use std::path::PathBuf;

use anyhow::Result;
use rust_decimal::Decimal;
use std::str::FromStr;
use tikr_backtest::fill_sim::{FillSim, FillSimConfig, VenueFees};
use tikr_binance::BinanceClient;
use tikr_core::Symbol;
use tikr_paper::{BotSpec, RunnerConfig, StrategyChoice};
use tikr_strategy::{LayeredGridConfig, StaticGridConfig};

use crate::config::{BotConfig, LgParams};

/// Convert a parsed `BotConfig` into a runnable `BotSpec`.
///
/// `venue` is consulted for symbol min-notional so per-bot notional is
/// auto-bumped to `min_notional × 1.2` if the operator set it too low.
pub fn to_spec(
    cfg: &BotConfig,
    symbol: Symbol,
    venue: &BinanceClient,
    base_state_dir: &std::path::Path,
) -> Result<BotSpec> {
    let strategy = match cfg.strategy.as_str() {
        "static-grid" | "sg" => build_sg(cfg, &symbol, venue)?,
        "layered-grid" | "lg" => build_lg(cfg, &symbol, venue)?,
        other => {
            return Err(anyhow::anyhow!(
                "unknown strategy '{other}' (supported: static-grid, layered-grid)"
            ));
        }
    };

    let label = format!("{}/{}", cfg.symbol, cfg.strategy);
    let state_dir = per_bot_state_dir(base_state_dir, &cfg.symbol);
    let runner_config = RunnerConfig {
        state_dir,
        snapshot_every_n_events: 100,
        skim: None,
        funding: None,
        snapshot_tap: None, // spawn_bot installs its own
    };

    // Live mode → FillSim is discarded but the runner takes it unconditionally.
    let fill_sim = FillSim::new(FillSimConfig {
        submit_latency_ms: 0,
        cancel_latency_ms: 0,
        fees: VenueFees {
            maker_bps: 0,
            taker_bps: 0,
        },
    });

    Ok(BotSpec {
        label,
        symbol,
        strategy,
        runner_config,
        fill_sim,
    })
}

fn per_bot_state_dir(base: &std::path::Path, symbol: &str) -> PathBuf {
    base.join(symbol.to_lowercase())
}

fn build_sg(cfg: &BotConfig, symbol: &Symbol, venue: &BinanceClient) -> Result<StrategyChoice> {
    let sg = cfg.sg.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "bot {} strategy=static-grid but [bot.sg] missing",
            cfg.symbol
        )
    })?;
    let notional = autobump_notional(sg.notional, symbol, venue)?;
    Ok(StrategyChoice::StaticGrid(StaticGridConfig {
        notional_per_order: notional,
        levels_per_side: sg.levels,
        inner_bps: sg.inner_bps,
        step_bps: sg.step_bps,
        skew_strength: sg.skew_strength,
        target_inventory_usdt: sg.target_inventory_usdt,
        rebuild_pos_ratio_delta: sg.rebuild_pos_ratio_delta,
        target_fills_per_min: sg.target_fills_per_min,
        fillrate_window_secs: sg.fillrate_window_secs,
        scale_min: sg.scale_min,
        scale_max: sg.scale_max,
    }))
}

fn build_lg(cfg: &BotConfig, symbol: &Symbol, venue: &BinanceClient) -> Result<StrategyChoice> {
    let lg: &LgParams = cfg.lg.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "bot {} strategy=layered-grid but [bot.lg] missing",
            cfg.symbol
        )
    })?;
    let notional = autobump_notional(lg.notional, symbol, venue)?;
    Ok(StrategyChoice::LayeredGrid(LayeredGridConfig {
        notional_per_order: notional,
        levels_per_side: lg.levels,
        inner_bps: lg.bps,
    }))
}

fn autobump_notional(
    requested: Decimal,
    symbol: &Symbol,
    venue: &BinanceClient,
) -> Result<Decimal> {
    if let Some(min_n) = venue.min_notional(symbol) {
        let floor = min_n * Decimal::from_str("1.2").unwrap();
        if requested < floor {
            return Ok(floor);
        }
    }
    Ok(requested)
}

