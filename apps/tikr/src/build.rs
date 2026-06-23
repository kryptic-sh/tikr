//! `BotConfig` → `BotSpec` conversion + per-symbol min-notional auto-bump.

use std::path::PathBuf;

use anyhow::Result;
use rust_decimal::Decimal;
use std::str::FromStr;
use tikr_backtest::fill_sim::{FillSim, FillSimConfig, VenueFees};
use tikr_binance::BinanceClient;
use tikr_core::Symbol;
use tikr_paper::{BotSpec, InventoryBoostConfig, RunnerConfig, StrategyChoice};
use tikr_strategy::{
    AvellanedaStoikovConfig, EwmaConfig, FlatMmConfig, GlftConfig, HydraConfig, JokerConfig,
    LadderReentryConfig, LayeredGridConfig, LiqFadeConfig, MantisConfig, MicroMeanReversionConfig,
    RsiMrConfig, SimpleGapConfig, SpreadScalpConfig, StaticGridConfig, StranglerConfig, TideConfig,
    VolleyConfig, WaveConfig,
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
    inventory_boost: Option<InventoryBoostConfig>,
    wallet_rx: Option<watch::Receiver<Decimal>>,
    take_profit_pct: Decimal,
    bagger_config: tikr_paper::bagger::BaggerConfig,
) -> Result<BotSpec> {
    // Bagger (inventory-risk flatten) — account-level, wired from the
    // `[account.bagger]` TOML table by `to_spec`'s caller. Off when no
    // mechanism is enabled (`BaggerConfig::enabled()` is false).
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
        "tide" | "td" => build_tide(cfg, &symbol, venue, default_notional)?,
        "joker" | "jk" => build_joker(cfg, &symbol, venue, default_notional)?,
        "rsi-mr" | "rsimr" => build_rsi_mr(cfg, &symbol, venue, default_notional)?,
        "wave" | "wv" => build_wave(cfg, &symbol, venue, default_notional)?,
        "flat-mm" | "fm" => build_flat_mm(cfg, &symbol, venue, default_notional)?,
        "avellaneda-stoikov" | "as" => build_as(cfg, &symbol, venue, default_notional)?,
        "glft" => build_glft(cfg, &symbol, venue, default_notional)?,
        "mantis" | "mn" => build_mantis(cfg, &symbol, venue, default_notional)?,
        "volley" | "vl" => build_volley(cfg, &symbol, venue, default_notional)?,
        "strangler" | "st" => build_strangler(cfg, &symbol, venue, default_notional)?,
        other => {
            return Err(anyhow::anyhow!(
                "unknown strategy '{other}' (supported: static-grid, layered-grid, ladder-reentry, simple-gap, micro-mean-reversion, spread-scalp, liq-fade, hydra, tide, joker, rsi-mr, wave, flat-mm, avellaneda-stoikov, glft, mantis, volley, strangler)"
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
        // Per-strategy expected max open order count. SS / SG / etc.
        // emit at most 2 (one per side); Tide emits up to
        // 2 × grid_levels. The runner's 30s orphan sweep uses this
        // as a "wipe everything if exceeded" threshold; set to 0
        // (disabled) when the strategy intentionally keeps many.
        max_expected_open_orders: max_open_orders_for(cfg),
        liquidation: None,
        mark_series: None,
        // Inventory-aware order-size boost — account-level, applied to every
        // strategy by the runner (scales the reducing side up on a curve).
        inventory_boost,
        // Take-profit — account-level, applied to every strategy: when
        // unrealized > take_profit_pct% of wallet, rest a reduce-only maker
        // limit to lock in half the bag.
        wallet_rx,
        take_profit_pct,
        // Bagger (inventory-risk flatten) — account-level. Wired from config in
        // `to_spec`'s caller once a winning preset is chosen in backtest; default
        // off until then.
        bagger: bagger_config,
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
        leverage: rust_decimal::Decimal::ZERO,
        max_position_frac: rust_decimal::Decimal::ZERO,
        silent_cancel_rate_per_min: 0.0,
        rng_seed: 0,
        latency_jitter_ms: 0,
        // Paper-mode sim mirrors the live venue's per-symbol open-order filter.
        max_open_orders: Some(tikr_backtest::fill_sim::BINANCE_MAX_OPEN_ORDERS_PER_SYMBOL),
        // Live mode discards FillSim; backtest-only queue model stays off here.
        queue_cancel_decay_per_sec: 0.0,
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
        "tide" | "td" => Ok(cfg.tide.as_ref().and_then(|p| p.notional)),
        "joker" | "jk" => Ok(cfg.joker.as_ref().and_then(|p| p.notional)),
        "rsi-mr" | "rsimr" => Ok(cfg.rsi_mr.as_ref().and_then(|p| p.notional)),
        "wave" | "wv" => Ok(cfg.wave.as_ref().and_then(|p| p.notional)),
        "flat-mm" | "fm" => Ok(cfg.flat_mm.as_ref().and_then(|p| p.notional)),
        "avellaneda-stoikov" | "as" => Ok(cfg.avellaneda_stoikov.as_ref().and_then(|p| p.notional)),
        "glft" => Ok(cfg.glft.as_ref().and_then(|p| p.notional)),
        "mantis" | "mn" => Ok(cfg.mantis.as_ref().and_then(|p| p.notional)),
        "volley" | "vl" => Ok(cfg.volley.as_ref().and_then(|p| p.notional)),
        "strangler" | "st" => Ok(cfg.strangler.as_ref().and_then(|p| p.notional)),
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
        | "tide"
        | "td"
        | "joker"
        | "jk"
        | "rsi-mr"
        | "rsimr"
        | "wave"
        | "wv"
        | "flat-mm"
        | "fm"
        | "mantis"
        | "mn"
        | "volley"
        | "vl"
        | "strangler"
        | "st" => None,
        other => return Err(anyhow::anyhow!("unknown strategy '{other}'")),
    };
    Ok(cap.filter(|v| *v > Decimal::ZERO))
}

fn per_bot_state_dir(base: &std::path::Path, symbol: &str) -> PathBuf {
    base.join(symbol.to_lowercase())
}

/// Strategy-specific expected max open-order count for the runner's
/// 30s orphan sweep. `0` = sweep disabled (caller intentionally keeps
/// many resting orders).
fn max_open_orders_for(cfg: &BotConfig) -> usize {
    match cfg.strategy.as_str() {
        "tide" | "td" => 0,
        "joker" | "jk" => 0,
        "rsi-mr" | "rsimr" => 0,
        "wave" | "wv" => 0,
        "flat-mm" | "fm" => 0,
        // Volley keeps a wall of `2 × levels` orders and refreshes it itself.
        "volley" | "vl" => 0,
        // Strangler keeps a full `2 × levels` window and reconciles it itself.
        "strangler" | "st" => 0,
        // Grid-style strategies — let the strategy manage its own
        // book without runner-level wipes.
        "static-grid" | "sg" | "layered-grid" | "lg" => 0,
        // SS: 1 entry per side + 1 close-side after fills + transient
        // overlap during requote = up to 5 in flight. Disable sweep
        // entirely — SS does its own requote management.
        "spread-scalp" | "ss" => 0,
        // Default: 1-per-side strategies emit at most 2.
        _ => 2,
    }
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

fn build_tide(
    cfg: &BotConfig,
    symbol: &Symbol,
    venue: &BinanceClient,
    default_notional: Decimal,
) -> Result<StrategyChoice> {
    // tide block is optional — strategy only needs notional,
    // and that can come from the account-wide default.
    let notional_override = cfg.tide.as_ref().and_then(|p| p.notional);
    let notional = autobump_notional(notional_override.unwrap_or(default_notional), symbol, venue)?;
    let tick_size = venue.tick_size(symbol).unwrap_or(Decimal::new(1, 8));
    let step_size = venue.step_size(symbol).unwrap_or(Decimal::ONE);
    let min_notional = venue.min_notional(symbol).unwrap_or(Decimal::ZERO);
    let grid_levels = cfg.tide.as_ref().map(|p| p.grid_levels).unwrap_or(1);
    let step_bps = cfg.tide.as_ref().map(|p| p.step_bps).unwrap_or(0);
    let prune_stragglers = cfg
        .tide
        .as_ref()
        .map(|p| p.prune_stragglers)
        .unwrap_or(true);
    // Initial max_position = 0 (no cap). Live value flows in via
    // on_max_position_updated from the account balance poller's
    // max_position_rx watch channel, typically within 5s of spawn.
    Ok(StrategyChoice::Tide(TideConfig {
        notional_per_order: notional,
        tick_size,
        step_size,
        min_notional,
        grid_levels,
        step_bps,
        max_position_usdt: Decimal::ZERO,
        prune_stragglers,
        recenter_bps: cfg.tide.as_ref().map(|t| t.recenter_bps).unwrap_or(0),
        recenter_secs: cfg.tide.as_ref().map(|t| t.recenter_secs).unwrap_or(0),
        inner_steps: cfg.tide.as_ref().map(|t| t.inner_steps).unwrap_or(0),
        chase: cfg.tide.as_ref().map(|t| t.chase).unwrap_or(false),
        chase_to_avg: cfg.tide.as_ref().map(|t| t.chase_to_avg).unwrap_or(false),
        relattice_timeout_secs: cfg
            .tide
            .as_ref()
            .map(|t| t.relattice_timeout_secs)
            .unwrap_or(300),
    }))
}

fn build_joker(
    cfg: &BotConfig,
    symbol: &Symbol,
    venue: &BinanceClient,
    default_notional: Decimal,
) -> Result<StrategyChoice> {
    let notional_override = cfg.joker.as_ref().and_then(|p| p.notional);
    let notional = autobump_notional(notional_override.unwrap_or(default_notional), symbol, venue)?;
    let tick_size = venue.tick_size(symbol).unwrap_or(Decimal::new(1, 8));
    let step_size = venue.step_size(symbol).unwrap_or(Decimal::ONE);
    let min_notional = venue.min_notional(symbol).unwrap_or(Decimal::ZERO);
    let max_order_age_secs = cfg
        .joker
        .as_ref()
        .map(|p| p.max_order_age_secs)
        .unwrap_or(0);
    let order_tick_offset = cfg.joker.as_ref().map(|p| p.order_tick_offset).unwrap_or(0);
    let order_tick_tolerance = cfg
        .joker
        .as_ref()
        .map(|p| p.order_tick_tolerance)
        .unwrap_or(5);
    Ok(StrategyChoice::Joker(JokerConfig {
        notional_per_order: notional,
        tick_size,
        step_size,
        min_notional,
        max_order_age_secs,
        order_tick_offset,
        order_tick_tolerance,
    }))
}

fn build_volley(
    cfg: &BotConfig,
    symbol: &Symbol,
    venue: &BinanceClient,
    default_notional: Decimal,
) -> Result<StrategyChoice> {
    let v = cfg.volley.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "bot {} strategy=volley but [bot.volley] missing",
            cfg.symbol
        )
    })?;
    let notional = autobump_notional(v.notional.unwrap_or(default_notional), symbol, venue)?;
    let tick_size = venue.tick_size(symbol).unwrap_or(Decimal::new(1, 8));
    let step_size = venue.step_size(symbol).unwrap_or(Decimal::ONE);
    let min_notional = venue.min_notional(symbol).unwrap_or(Decimal::ZERO);
    Ok(StrategyChoice::Volley(VolleyConfig {
        notional_per_order: notional,
        tick_size,
        step_size,
        min_notional,
        levels: v.levels,
        interval_secs: v.interval_secs,
        step_ticks: v.step_ticks,
        inner_ticks: v.inner_ticks,
    }))
}

fn build_rsi_mr(
    cfg: &BotConfig,
    symbol: &Symbol,
    venue: &BinanceClient,
    default_notional: Decimal,
) -> Result<StrategyChoice> {
    let rsi_mr = cfg.rsi_mr.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "bot {} strategy=rsi-mr but [bot.rsi_mr] missing",
            cfg.symbol
        )
    })?;
    let notional = autobump_notional(rsi_mr.notional.unwrap_or(default_notional), symbol, venue)?;
    let tick_size = venue.tick_size(symbol).unwrap_or(Decimal::new(1, 8));
    let step_size = venue.step_size(symbol).unwrap_or(Decimal::ONE);
    let min_notional = venue.min_notional(symbol).unwrap_or(Decimal::ZERO);
    Ok(StrategyChoice::RsiMr(RsiMrConfig {
        notional_per_order: notional,
        tick_size,
        step_size,
        min_notional,
        bar_interval_secs: rsi_mr.bar_interval_secs,
        max_bars: rsi_mr.max_bars,
        rsi_period: rsi_mr.rsi_period,
        rsi_buy_threshold: rsi_mr.rsi_buy_threshold,
        rsi_exit_threshold: rsi_mr.rsi_exit_threshold,
        ker_period: rsi_mr.ker_period,
        ker_max_trending: rsi_mr.ker_max_trending,
        vol_zscore_period: rsi_mr.vol_zscore_period,
        vol_zscore_min: rsi_mr.vol_zscore_min,
        atr_period: rsi_mr.atr_period,
        atr_sl_mult: rsi_mr.atr_sl_mult,
        atr_tp_mult: rsi_mr.atr_tp_mult,
        max_hold_bars: rsi_mr.max_hold_bars,
    }))
}

fn build_wave(
    cfg: &BotConfig,
    symbol: &Symbol,
    venue: &BinanceClient,
    default_notional: Decimal,
) -> Result<StrategyChoice> {
    let wave = cfg.wave.as_ref().ok_or_else(|| {
        anyhow::anyhow!("bot {} strategy=wave but [bot.wave] missing", cfg.symbol)
    })?;
    let notional = autobump_notional(wave.notional.unwrap_or(default_notional), symbol, venue)?;
    let tick_size = venue.tick_size(symbol).unwrap_or(Decimal::new(1, 8));
    let step_size = venue.step_size(symbol).unwrap_or(Decimal::ONE);
    let min_notional = venue.min_notional(symbol).unwrap_or(Decimal::ZERO);
    // Maker fee for the auto-step floor (round-trip break-even = 2× this). Live:
    // from the venue's cached commissionRate. On a fetch failure the cache is
    // empty → fall back to a conservative futures tier-0 maker (2 bps) so the
    // floor never lets the step run sub-break-even.
    let maker_fee_bps = venue.maker_fee_bps(symbol).unwrap_or(Decimal::from(2));
    Ok(StrategyChoice::Wave(WaveConfig {
        notional_per_order: notional,
        tick_size,
        step_size,
        min_notional,
        levels: wave.levels,
        steps_bps: wave.steps_bps,
        steps_inner: wave.steps_inner,
        auto_inner: wave.auto_inner,
        round_trips: wave.round_trips,
        force_refill_secs: wave.force_refill_secs,
        auto_step: wave.auto_step,
        auto_step_k: wave.auto_step_k,
        maker_fee_bps,
        auto_candle_window: wave.auto_candle_window,
        relattice_drift_pct: wave.relattice_drift_pct,
        size_mult: wave.size_mult,
        size_ramp: wave.size_ramp,
        reduce_to_avg: wave.reduce_to_avg,
    }))
}

fn build_flat_mm(
    cfg: &BotConfig,
    symbol: &Symbol,
    venue: &BinanceClient,
    default_notional: Decimal,
) -> Result<StrategyChoice> {
    let flat = cfg.flat_mm.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "bot {} strategy=flat-mm but [bot.flat_mm] missing",
            cfg.symbol
        )
    })?;
    let notional = autobump_notional(flat.notional.unwrap_or(default_notional), symbol, venue)?;
    let tick_size = venue.tick_size(symbol).unwrap_or(Decimal::new(1, 8));
    let step_size = venue.step_size(symbol).unwrap_or(Decimal::ONE);
    let min_notional = venue.min_notional(symbol).unwrap_or(Decimal::ZERO);
    Ok(StrategyChoice::FlatMm(FlatMmConfig {
        notional_per_order: notional,
        tick_size,
        step_size,
        min_notional,
        inner_bps: flat.inner_bps,
        step_bps: flat.step_bps,
        levels: flat.levels,
        reservation_skew_bps: flat.reservation_skew_bps,
        imbalance_skew_bps: flat.imbalance_skew_bps,
        skew_unit_notional: flat.skew_unit.unwrap_or(notional * Decimal::from(20)),
        flush_bps: flat.flush_bps,
        chase_boost_pct: flat.chase_boost_pct,
        flush_frac: flat.flush_frac,
        underwater_reduce_frac: flat.underwater_reduce_frac,
        frozen_lattice: flat.frozen_lattice,
        lattice_band_levels: flat.lattice_band_levels,
        lattice_max_open: flat.lattice_max_open,
    }))
}

fn build_as(
    cfg: &BotConfig,
    symbol: &Symbol,
    venue: &BinanceClient,
    default_notional: Decimal,
) -> Result<StrategyChoice> {
    let p = cfg.avellaneda_stoikov.clone().unwrap_or_default();
    let notional = autobump_notional(p.notional.unwrap_or(default_notional), symbol, venue)?;
    let step_size = venue.step_size(symbol).unwrap_or(Decimal::ONE);
    Ok(StrategyChoice::AvellanedaStoikov(AvellanedaStoikovConfig {
        gamma: p.gamma,
        base_spread_bps: p.base_spread_bps,
        horizon_sec: p.horizon_sec,
        // notional-based sizing drives the live size; size_per_quote is the
        // unused fallback (lot-rounded notional/price is computed per quote).
        size_per_quote: tikr_core::Size(Decimal::ONE),
        notional_per_quote: Some(notional),
        step_size,
        min_requote_interval_ms: p.min_requote_ms,
        level_step_bps: p.level_step_bps,
        volatility: EwmaConfig {
            half_life_sec: p.ewma_half_life_sec,
            initial_var: p.ewma_initial_var,
        },
    }))
}

fn build_glft(
    cfg: &BotConfig,
    symbol: &Symbol,
    venue: &BinanceClient,
    default_notional: Decimal,
) -> Result<StrategyChoice> {
    let p = cfg.glft.clone().unwrap_or_default();
    let notional = autobump_notional(p.notional.unwrap_or(default_notional), symbol, venue)?;
    let step_size = venue.step_size(symbol).unwrap_or(Decimal::ONE);
    Ok(StrategyChoice::Glft(GlftConfig {
        gamma: p.gamma,
        base_spread_bps: p.base_spread_bps,
        size_per_quote: tikr_core::Size(Decimal::ONE),
        notional_per_quote: Some(notional),
        step_size,
        min_requote_interval_ms: p.min_requote_ms,
        level_step_bps: p.level_step_bps,
        volatility: EwmaConfig {
            half_life_sec: p.ewma_half_life_sec,
            initial_var: p.ewma_initial_var,
        },
    }))
}

fn build_strangler(
    cfg: &BotConfig,
    symbol: &Symbol,
    venue: &BinanceClient,
    default_notional: Decimal,
) -> Result<StrategyChoice> {
    let s = cfg.strangler.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "bot {} strategy=strangler but [bot.strangler] missing",
            cfg.symbol
        )
    })?;
    let notional = autobump_notional(s.notional.unwrap_or(default_notional), symbol, venue)?;
    let tick_size = venue.tick_size(symbol).unwrap_or(Decimal::new(1, 8));
    let step_size = venue.step_size(symbol).unwrap_or(Decimal::ONE);
    let min_notional = venue.min_notional(symbol).unwrap_or(Decimal::ZERO);
    Ok(StrategyChoice::Strangler(StranglerConfig {
        notional_per_order: notional,
        tick_size,
        step_size,
        min_notional,
        levels: s.levels,
        step_ticks: s.step_ticks,
        inner_ticks: s.inner_ticks,
    }))
}

fn build_mantis(
    cfg: &BotConfig,
    symbol: &Symbol,
    venue: &BinanceClient,
    default_notional: Decimal,
) -> Result<StrategyChoice> {
    let mantis = cfg.mantis.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "bot {} strategy=mantis but [bot.mantis] missing",
            cfg.symbol
        )
    })?;
    let notional = autobump_notional(mantis.notional.unwrap_or(default_notional), symbol, venue)?;
    let tick_size = venue.tick_size(symbol).unwrap_or(Decimal::new(1, 8));
    let step_size = venue.step_size(symbol).unwrap_or(Decimal::ONE);
    let min_notional = venue.min_notional(symbol).unwrap_or(Decimal::ZERO);
    Ok(StrategyChoice::Mantis(MantisConfig {
        notional_per_order: notional,
        tick_size,
        step_size,
        min_notional,
        min_spread_bps: mantis.min_spread_bps,
        tick_offset: mantis.tick_offset,
        reopen_distance_ticks: mantis.reopen_distance_ticks,
        // Account-derived cap arrives via on_max_position_updated (live
        // channel); seed 0 = uncapped until the first update lands.
        max_position_usdt: Decimal::ZERO,
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
            confirm_touch: mmr.confirm_touch,
            tp_relax_trigger_bps: mmr.tp_relax_trigger_bps,
            tp_relax_floor_bps: mmr.tp_relax_floor_bps,
            add_block_bps: mmr.add_block_bps,
            entry_cooldown_ms: mmr.entry_cooldown_ms,
            entry_from_touch: mmr.entry_from_touch,
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
