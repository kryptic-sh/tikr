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
    HydraConfig, LadderReentryConfig, LayeredGridConfig, LiqFadeConfig, MicroMeanReversionConfig,
    SimpleGapConfig, SpreadScalpConfig, StaticGridConfig, TouchRefillConfig,
};
use tokio::sync::watch;

use crate::config::{BotConfig, LgParams};

/// Convert a parsed `BotConfig` into a runnable `BotSpec`.
///
/// `venue` is consulted for symbol min-notional so per-bot notional is
/// auto-bumped to `min_notional × 1.2` if the operator set it too low.
#[allow(clippy::too_many_arguments)]
pub fn to_spec(
    cfg: &BotConfig,
    symbol: Symbol,
    venue: &BinanceClient,
    base_state_dir: &std::path::Path,
    default_notional: Decimal,
    notional_rx: Option<watch::Receiver<Decimal>>,
    max_position_rx: Option<watch::Receiver<Decimal>>,
    max_position_usdt_default: Decimal,
) -> Result<BotSpec> {
    let uses_default_notional = strategy_notional(cfg)?.is_none();
    let strategy = match cfg.strategy.as_str() {
        "static-grid" | "sg" => build_sg(
            cfg,
            &symbol,
            venue,
            default_notional,
            max_position_usdt_default,
        )?,
        "layered-grid" | "lg" => build_lg(
            cfg,
            &symbol,
            venue,
            default_notional,
            max_position_usdt_default,
        )?,
        "ladder-reentry" | "lr" => build_ladder_reentry(cfg, &symbol, venue, default_notional)?,
        "simple-gap" | "sgap" => build_simple_gap(cfg, &symbol, venue, default_notional)?,
        "micro-mean-reversion" | "mmr" => {
            build_micro_mean_reversion(cfg, &symbol, venue, default_notional)?
        }
        "spread-scalp" | "ss" => build_spread_scalp(
            cfg,
            &symbol,
            venue,
            default_notional,
            max_position_usdt_default,
        )?,
        "liq-fade" | "lf" => build_liq_fade(
            cfg,
            &symbol,
            venue,
            default_notional,
            max_position_usdt_default,
        )?,
        "hydra" | "hd" | "hy" => build_hydra(
            cfg,
            &symbol,
            venue,
            default_notional,
            max_position_usdt_default,
        )?,
        "touch-refill" | "tr" => build_touch_refill(cfg, &symbol, venue, default_notional)?,
        other => {
            return Err(anyhow::anyhow!(
                "unknown strategy '{other}' (supported: static-grid, layered-grid, ladder-reentry, simple-gap, micro-mean-reversion, spread-scalp, liq-fade, hydra, touch-refill)"
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
        // Cap rescaling tracks the same default-vs-explicit semantics
        // as notional: when the bot defines its own `max_position_usdt`
        // in TOML the live channel is suppressed (operator intent
        // wins). When the bot inherits the account-level default, the
        // strategy receives live updates as wallet grows.
        max_position_rx: if strategy_max_position(cfg)?.is_none() {
            max_position_rx
        } else {
            None
        },
        // LiqFade is the only consumer; other strategies leave the
        // buffer empty regardless of this value.
        liq_window_secs: cfg.liq_fade.as_ref().map(|p| p.window_secs).unwrap_or(0),
        // Supervisor fills this in from venue.position_risk on
        // `--clear`-off startup. build.rs leaves it None — the spec is
        // constructed before supervisor knows whether to seed.
        seed_position: None,
        equity_csv_path: None,
        initial_balance: Decimal::ZERO,
        order_balance_pct: Decimal::ZERO,
        max_position_pct: Decimal::ZERO,
        // Same lookup as the strategy-side min_notional plumbing
        // above. Runner uses it as a defense-in-depth guard against
        // dust emits (close-side pinned to residual qty, etc.).
        min_notional: venue.min_notional(&symbol).unwrap_or(Decimal::ZERO),
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
        "liq-fade" | "lf" => Ok(cfg.liq_fade.as_ref().map(|p| p.notional).unwrap_or(None)),
        // Hydra: notional is wallet-derived only — no per-bot override.
        "hydra" | "hd" | "hy" => Ok(None),
        "touch-refill" | "tr" => Ok(cfg.touch_refill.as_ref().and_then(|p| p.notional)),
        other => Err(anyhow::anyhow!("unknown strategy '{other}'")),
    }
}

/// Returns `Some(cap)` when the bot's TOML explicitly sets a per-bot
/// `max_position_usdt > 0`, else `None`. Used by `to_spec` to decide
/// whether to subscribe the runner to live-cap updates from the
/// account poller.
fn strategy_max_position(cfg: &BotConfig) -> Result<Option<Decimal>> {
    let cap = match cfg.strategy.as_str() {
        "static-grid" | "sg" => cfg.sg.as_ref().map(|p| p.max_position_usdt),
        "layered-grid" | "lg" => cfg.lg.as_ref().map(|p| p.max_position_usdt),
        "spread-scalp" | "ss" => cfg.spread_scalp.as_ref().map(|p| p.max_position_usdt),
        "liq-fade" | "lf" => cfg.liq_fade.as_ref().map(|p| p.max_position_usdt),
        "hydra" | "hd" | "hy" => cfg.hydra.as_ref().map(|p| p.max_position_usdt),
        // Strategies without a cap concept: never override.
        "ladder-reentry"
        | "lr"
        | "simple-gap"
        | "sgap"
        | "micro-mean-reversion"
        | "mmr"
        | "touch-refill"
        | "tr" => None,
        other => return Err(anyhow::anyhow!("unknown strategy '{other}'")),
    };
    Ok(cap.filter(|v| *v > Decimal::ZERO))
}

fn per_bot_state_dir(base: &std::path::Path, symbol: &str) -> PathBuf {
    base.join(symbol.to_lowercase())
}

fn build_sg(
    cfg: &BotConfig,
    symbol: &Symbol,
    venue: &BinanceClient,
    default_notional: Decimal,
    max_position_usdt_default: Decimal,
) -> Result<StrategyChoice> {
    let sg = cfg.sg.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "bot {} strategy=static-grid but [bot.sg] missing",
            cfg.symbol
        )
    })?;
    let notional = autobump_notional(sg.notional.unwrap_or(default_notional), symbol, venue)?;
    let step_size = venue.step_size(symbol).unwrap_or(Decimal::ONE);
    let min_notional = venue.min_notional(symbol).unwrap_or(Decimal::ZERO);
    Ok(StrategyChoice::StaticGrid(StaticGridConfig {
        notional_per_order: notional,
        levels_per_side: sg.levels,
        inner_bps: sg.inner_bps,
        step_bps: sg.step_bps,
        step_size,
        min_notional,
        target_fills_per_min: sg.target_fills_per_min,
        fillrate_window_secs: sg.fillrate_window_secs,
        scale_min: sg.scale_min,
        scale_max: sg.scale_max,
        auto_skew: sg.auto_skew,
        regime_window_secs: sg.regime_window_secs,
        regime_trend_threshold_bps: sg.regime_trend_threshold_bps,
        regime_efficiency_threshold: sg.regime_efficiency_threshold,
        max_position_usdt: if sg.max_position_usdt > Decimal::ZERO {
            sg.max_position_usdt
        } else {
            max_position_usdt_default
        },
        take_profit_bps: sg.take_profit_bps,
        stop_loss_bps: sg.stop_loss_bps,
    }))
}

fn build_lg(
    cfg: &BotConfig,
    symbol: &Symbol,
    venue: &BinanceClient,
    default_notional: Decimal,
    max_position_usdt_default: Decimal,
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
        max_position_usdt: if lg.max_position_usdt > Decimal::ZERO {
            lg.max_position_usdt
        } else {
            max_position_usdt_default
        },
        take_profit_bps: lg.take_profit_bps,
        stop_loss_bps: lg.stop_loss_bps,
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

fn build_touch_refill(
    cfg: &BotConfig,
    symbol: &Symbol,
    venue: &BinanceClient,
    default_notional: Decimal,
) -> Result<StrategyChoice> {
    // touch_refill block is optional — strategy only needs notional,
    // and that can come from the account-wide default.
    let notional_override = cfg.touch_refill.as_ref().and_then(|p| p.notional);
    let notional = autobump_notional(notional_override.unwrap_or(default_notional), symbol, venue)?;
    let step_size = venue.step_size(symbol).unwrap_or(Decimal::ONE);
    let min_notional = venue.min_notional(symbol).unwrap_or(Decimal::ZERO);
    Ok(StrategyChoice::TouchRefill(TouchRefillConfig {
        notional_per_order: notional,
        step_size,
        min_notional,
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
    max_position_usdt_default: Decimal,
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
    let step_size = venue.step_size(symbol).unwrap_or(tick_size);
    let min_notional = venue.min_notional(symbol).unwrap_or(Decimal::ZERO);
    Ok(StrategyChoice::SpreadScalp(SpreadScalpConfig {
        notional_per_order: notional,
        tick_size,
        step_size,
        min_notional,
        min_spread_bps: spread_scalp.min_spread_bps,
        requote_interval_ms: spread_scalp.requote_interval_ms,
        max_position_usdt: if spread_scalp.max_position_usdt > Decimal::ZERO {
            spread_scalp.max_position_usdt
        } else {
            max_position_usdt_default
        },
        take_profit_usdt: spread_scalp.take_profit_usdt,
        reject_cooldown_ms: spread_scalp.reject_cooldown_ms,
        price_tolerance_ticks: spread_scalp.price_tolerance_ticks,
        take_profit_bps: spread_scalp.take_profit_bps,
        stop_loss_bps: spread_scalp.stop_loss_bps,
        adverse: if spread_scalp.adverse_window_ms > 0 {
            tikr_strategy::spread_scalp::adverse_tracker::AdverseConfig {
                snapshot_window_ms: spread_scalp.adverse_window_ms,
                ema_half_life_fills: spread_scalp.adverse_half_life_fills,
                threshold_bps: spread_scalp.adverse_threshold_bps,
                max_widen_bps: spread_scalp.adverse_max_widen_bps,
            }
        } else {
            tikr_strategy::spread_scalp::adverse_tracker::AdverseConfig::disabled()
        },
        close_side_always_quotes: spread_scalp.close_side_always_quotes,
        close_decay_after_secs_1: spread_scalp.close_decay_after_secs_1,
        close_decay_factor_1: spread_scalp.close_decay_factor_1,
        close_decay_after_secs_2: spread_scalp.close_decay_after_secs_2,
        close_decay_factor_2: spread_scalp.close_decay_factor_2,
        adverse_stop_after_secs: spread_scalp.adverse_stop_after_secs,
        adverse_stop_drift_bps: spread_scalp.adverse_stop_drift_bps,
        quote_offset_ticks: spread_scalp.quote_offset_ticks,
        close_target_ticks: spread_scalp.close_target_ticks,
        strict_touch_quotes: spread_scalp.strict_touch_quotes,
    }))
}

fn build_liq_fade(
    cfg: &BotConfig,
    symbol: &Symbol,
    venue: &BinanceClient,
    default_notional: Decimal,
    max_position_usdt_default: Decimal,
) -> Result<StrategyChoice> {
    let lf = cfg.liq_fade.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "bot {} strategy=liq-fade but [bot.liq_fade] missing",
            cfg.symbol
        )
    })?;
    let notional = autobump_notional(lf.notional.unwrap_or(default_notional), symbol, venue)?;
    let tick_size = venue.tick_size(symbol).unwrap_or(Decimal::ONE);
    let step_size = venue.step_size(symbol).unwrap_or(tick_size);
    let min_notional = venue.min_notional(symbol).unwrap_or(Decimal::ZERO);
    Ok(StrategyChoice::LiqFade(LiqFadeConfig {
        notional_per_entry: notional,
        tick_size,
        step_size,
        min_notional,
        max_position_usdt: if lf.max_position_usdt > Decimal::ZERO {
            lf.max_position_usdt
        } else {
            max_position_usdt_default
        },
        arm_threshold_usdt: lf.arm_threshold_usdt,
        arm_dominance: lf.arm_dominance,
        capitulation_overshoot_bps: lf.capitulation_overshoot_bps,
        fade_offset_bps: lf.fade_offset_bps,
        revert_target_bps: lf.revert_target_bps,
        entry_timeout_secs: lf.entry_timeout_secs,
        position_timeout_secs: lf.position_timeout_secs,
        stop_loss_bps: lf.stop_loss_bps,
    }))
}

fn build_hydra(
    cfg: &BotConfig,
    symbol: &Symbol,
    venue: &BinanceClient,
    default_notional: Decimal,
    max_position_usdt_default: Decimal,
) -> Result<StrategyChoice> {
    let hd = cfg.hydra.as_ref().ok_or_else(|| {
        anyhow::anyhow!("bot {} strategy=hydra but [bot.hydra] missing", cfg.symbol)
    })?;
    let notional = autobump_notional(default_notional, symbol, venue)?;
    let tick_size = venue.tick_size(symbol).unwrap_or(Decimal::ONE);
    let step_size = venue.step_size(symbol).unwrap_or(tick_size);
    let min_notional = venue.min_notional(symbol).unwrap_or(Decimal::ZERO);
    Ok(StrategyChoice::Hydra(HydraConfig {
        notional_per_order: notional,
        tick_size,
        step_size,
        min_notional,
        entry_offset_bps: hd.entry_offset_bps,
        pyramid_step_bps: hd.pyramid_step_bps,
        pyramid_max_adds: hd.pyramid_max_adds,
        dca_step_bps: hd.dca_step_bps,
        dca_max_adds: hd.dca_max_adds,
        tp_bps_from_avg: hd.tp_bps_from_avg,
        sl_bps_from_first: hd.sl_bps_from_first,
        max_position_usdt: if hd.max_position_usdt > Decimal::ZERO {
            hd.max_position_usdt
        } else {
            max_position_usdt_default
        },
        add_cooldown_ms: hd.add_cooldown_ms,
        straddle_refresh_secs: hd.straddle_refresh_secs,
        straddle_drift_bps: hd.straddle_drift_bps,
        pyramid_size_mult: hd.pyramid_size_mult,
        dca_size_mult: hd.dca_size_mult,
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
