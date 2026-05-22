//! `BotConfig` → `BotSpec` conversion + per-symbol min-notional auto-bump.

use std::path::PathBuf;

use anyhow::Result;
use rust_decimal::Decimal;
use std::str::FromStr;
use tikr_backtest::fill_sim::{FillSim, FillSimConfig, VenueFees};
use tikr_binance::BinanceClient;
use tikr_core::Symbol;
use tikr_paper::{BotSpec, RunnerConfig, StrategyChoice};
use tikr_strategy::{
    LadderReentryConfig, LayeredGridConfig, MicroMeanReversionConfig, SimpleGapConfig,
    SpreadScalpConfig, StaticGridConfig,
};
use tokio::sync::watch;

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
    default_notional: Decimal,
    notional_rx: Option<watch::Receiver<Decimal>>,
) -> Result<BotSpec> {
    let uses_default_notional = strategy_notional(cfg)?.is_none();
    let strategy = match cfg.strategy.as_str() {
        "static-grid" | "sg" => build_sg(cfg, &symbol, venue, default_notional)?,
        "layered-grid" | "lg" => build_lg(cfg, &symbol, venue, default_notional)?,
        "ladder-reentry" | "lr" => build_ladder_reentry(cfg, &symbol, venue, default_notional)?,
        "simple-gap" | "sgap" => build_simple_gap(cfg, &symbol, venue, default_notional)?,
        "micro-mean-reversion" | "mmr" => {
            build_micro_mean_reversion(cfg, &symbol, venue, default_notional)?
        }
        "spread-scalp" | "ss" => build_spread_scalp(cfg, &symbol, venue, default_notional)?,
        other => {
            return Err(anyhow::anyhow!(
                "unknown strategy '{other}' (supported: static-grid, layered-grid, ladder-reentry, simple-gap, micro-mean-reversion, spread-scalp)"
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
        live_tap: None,
        notional_rx: if uses_default_notional {
            notional_rx
        } else {
            None
        },
    };

    // Live mode → FillSim is discarded but the runner takes it unconditionally.
    let fill_sim = FillSim::new(FillSimConfig {
        submit_latency_ms: 0,
        cancel_latency_ms: 0,
        fees: VenueFees {
            maker_bps: 0,
            taker_bps: 0,
        },
        max_position_notional_usdt: None,
        silent_cancel_rate_per_min: 0.0,
        rng_seed: 0,
    });

    Ok(BotSpec {
        label,
        symbol,
        strategy,
        runner_config,
        fill_sim,
    })
}

fn strategy_notional(cfg: &BotConfig) -> Result<Option<Decimal>> {
    match cfg.strategy.as_str() {
        "static-grid" | "sg" => Ok(cfg.sg.as_ref().map(|p| p.notional).unwrap_or(None)),
        "layered-grid" | "lg" => Ok(cfg.lg.as_ref().map(|p| p.notional).unwrap_or(None)),
        "ladder-reentry" | "lr" => Ok(cfg
            .ladder_reentry
            .as_ref()
            .map(|p| p.notional)
            .unwrap_or(None)),
        "simple-gap" | "sgap" => Ok(cfg.simple_gap.as_ref().map(|p| p.notional).unwrap_or(None)),
        "micro-mean-reversion" | "mmr" => Ok(cfg
            .micro_mean_reversion
            .as_ref()
            .map(|p| p.notional)
            .unwrap_or(None)),
        "spread-scalp" | "ss" => Ok(cfg
            .spread_scalp
            .as_ref()
            .map(|p| p.notional)
            .unwrap_or(None)),
        other => Err(anyhow::anyhow!("unknown strategy '{other}'")),
    }
}

fn per_bot_state_dir(base: &std::path::Path, symbol: &str) -> PathBuf {
    base.join(symbol.to_lowercase())
}

fn build_sg(
    cfg: &BotConfig,
    symbol: &Symbol,
    venue: &BinanceClient,
    default_notional: Decimal,
) -> Result<StrategyChoice> {
    let sg = cfg.sg.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "bot {} strategy=static-grid but [bot.sg] missing",
            cfg.symbol
        )
    })?;
    let notional = autobump_notional(sg.notional.unwrap_or(default_notional), symbol, venue)?;
    Ok(StrategyChoice::StaticGrid(StaticGridConfig {
        notional_per_order: notional,
        levels_per_side: sg.levels,
        inner_bps: sg.inner_bps,
        step_bps: sg.step_bps,
        target_fills_per_min: sg.target_fills_per_min,
        fillrate_window_secs: sg.fillrate_window_secs,
        scale_min: sg.scale_min,
        scale_max: sg.scale_max,
        auto_skew: sg.auto_skew,
    }))
}

fn build_lg(
    cfg: &BotConfig,
    symbol: &Symbol,
    venue: &BinanceClient,
    default_notional: Decimal,
) -> Result<StrategyChoice> {
    let lg: &LgParams = cfg.lg.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "bot {} strategy=layered-grid but [bot.lg] missing",
            cfg.symbol
        )
    })?;
    let notional = autobump_notional(lg.notional.unwrap_or(default_notional), symbol, venue)?;
    Ok(StrategyChoice::LayeredGrid(LayeredGridConfig {
        notional_per_order: notional,
        levels_per_side: lg.levels,
        inner_bps: lg.bps,
    }))
}

fn build_ladder_reentry(
    cfg: &BotConfig,
    symbol: &Symbol,
    venue: &BinanceClient,
    default_notional: Decimal,
) -> Result<StrategyChoice> {
    let lr = cfg.ladder_reentry.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "bot {} strategy=ladder-reentry but [bot.ladder_reentry] missing",
            cfg.symbol
        )
    })?;
    let notional = autobump_notional(lr.notional.unwrap_or(default_notional), symbol, venue)?;
    Ok(StrategyChoice::LadderReentry(LadderReentryConfig {
        notional_per_order: notional,
        levels_per_side: lr.levels,
        inner_bps: lr.inner_bps,
        step_bps: lr.step_bps,
        reentry_bps: lr.reentry_bps,
        continuation_bps: lr.continuation_bps,
    }))
}

fn build_simple_gap(
    cfg: &BotConfig,
    symbol: &Symbol,
    venue: &BinanceClient,
    default_notional: Decimal,
) -> Result<StrategyChoice> {
    let simple_gap = cfg.simple_gap.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "bot {} strategy=simple-gap but [bot.simple_gap] missing",
            cfg.symbol
        )
    })?;
    let notional = autobump_notional(
        simple_gap.notional.unwrap_or(default_notional),
        symbol,
        venue,
    )?;
    Ok(StrategyChoice::SimpleGap(SimpleGapConfig {
        notional_per_order: notional,
        gap_bps: simple_gap.gap_bps,
    }))
}

fn build_micro_mean_reversion(
    cfg: &BotConfig,
    symbol: &Symbol,
    venue: &BinanceClient,
    default_notional: Decimal,
) -> Result<StrategyChoice> {
    let mmr = cfg.micro_mean_reversion.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "bot {} strategy=micro-mean-reversion but [bot.micro_mean_reversion] missing",
            cfg.symbol
        )
    })?;
    let notional = autobump_notional(mmr.notional.unwrap_or(default_notional), symbol, venue)?;
    Ok(StrategyChoice::MicroMeanReversion(
        MicroMeanReversionConfig {
            notional_per_order: notional,
            trigger_bps: mmr.trigger_bps,
            entry_bps: mmr.entry_bps,
            exit_bps: mmr.exit_bps,
            max_open_entries: mmr.max_open_entries,
        },
    ))
}

fn build_spread_scalp(
    cfg: &BotConfig,
    symbol: &Symbol,
    venue: &BinanceClient,
    default_notional: Decimal,
) -> Result<StrategyChoice> {
    let spread_scalp = cfg.spread_scalp.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "bot {} strategy=spread-scalp but [bot.spread_scalp] missing",
            cfg.symbol
        )
    })?;
    let notional = autobump_notional(
        spread_scalp.notional.unwrap_or(default_notional),
        symbol,
        venue,
    )?;
    let tick_size = venue.tick_size(symbol).unwrap_or(spread_scalp.tick_size);
    if tick_size <= Decimal::ZERO {
        anyhow::bail!(
            "bot {} strategy=spread-scalp needs tick_size because exchangeInfo has no symbol filters",
            cfg.symbol
        );
    }
    Ok(StrategyChoice::SpreadScalp(SpreadScalpConfig {
        notional_per_order: notional,
        tick_size,
        improve_ticks: spread_scalp.improve_ticks,
        min_requote_interval_ms: spread_scalp.min_requote_interval_ms,
        requote_tick_threshold: spread_scalp.requote_tick_threshold,
        force_requote_interval_ms: spread_scalp.force_requote_interval_ms,
        min_quote_edge_bps: spread_scalp.min_quote_edge_bps,
        flatten_threshold_notional: spread_scalp.flatten_threshold_notional,
        skew_unit_notional: spread_scalp.skew_unit_notional,
        max_skew_ticks: spread_scalp.max_skew_ticks,
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
