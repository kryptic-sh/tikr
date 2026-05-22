//! Run a fixed suite of strategy presets against the same recorded parquet
//! data and print a comparison table.
//!
//! Each preset gets a fresh `ParquetReplay` + `FillSim` + `run_with_resume`
//! pass, so results are apples-to-apples on identical historical events.

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use clap::Parser;
use futures::stream::{self, BoxStream};
use tikr_backtest::fill_sim::{FillSim, FillSimConfig, VenueFees};
use tikr_backtest::replay::{LoadedReplayData, ParquetReplay, ReplayConfig};
use tikr_core::{
    Asset, Decimal, Fill, MarketEvent, MarketKind, Position, SignedSize, Size, Snapshot, Symbol,
    VenueId,
};
use tikr_paper::{FundingConfig, PaperReport, RunnerConfig, SkimConfig, run_with_resume};
use tikr_strategy::{
    AvellanedaStoikov, AvellanedaStoikovConfig, EwmaConfig, Glft, GlftConfig, LadderReentry,
    LadderReentryConfig, LayeredGrid, LayeredGridConfig, MicroMeanReversion,
    MicroMeanReversionConfig, MicroPrice, MicroPriceConfig, SimpleGap, SimpleGapConfig,
    SpreadScalp, SpreadScalpConfig, StaticGrid, StaticGridConfig, Strategy, TopOfBook,
    TopOfBookConfig,
};
use tikr_venue::{QuoteId, QuoteIntent, Venue, VenueError};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::info;

#[derive(Parser, Debug)]
#[command(
    name = "compare_strategies",
    about = "Run a strategy suite over recorded parquet data and print a comparison"
)]
struct Args {
    /// Directory containing `book_<BASE>_*.parquet` + `trades_<BASE>_*.parquet`.
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,

    /// Binance-style symbol (e.g. `BTCUSDT`).
    #[arg(long, default_value = "BTCUSDT")]
    symbol: String,

    /// Order size per quote (applied to ALL presets).
    #[arg(long, default_value = "0.001")]
    size: String,

    /// Maker fee in bps (default = Binance Futures USD-M tier 0).
    #[arg(long, default_value_t = 2i32)]
    maker_bps: i32,

    /// Taker fee in bps.
    #[arg(long, default_value_t = 5u32)]
    taker_bps: u32,

    /// Heartbeat synthesis cadence (ms).
    #[arg(long, default_value_t = 1000u64)]
    heartbeat_ms: u64,

    /// Tick size for TopOfBook presets.
    #[arg(long, default_value = "0.1")]
    tick_size: String,

    /// Skim mode: starting USDT budget per preset. `0` disables (default).
    /// When enabled, each preset's perp account starts at this budget and
    /// profit is moved to spot in `skim_pct` chunks.
    #[arg(long, default_value_t = 0.0_f64)]
    budget: f64,

    /// Skim threshold as percent of budget. `5` = skim every 5% gain.
    #[arg(long, default_value_t = 5.0_f64)]
    skim_pct: f64,

    /// Fraction of each skim chunk that moves to spot. `1.0` = classic
    /// (all → spot). `0.5` = half to spot, half compounds in perp.
    /// `0.0` = no spot buys, all profits stay in perp.
    #[arg(long, default_value_t = 1.0_f64)]
    skim_ratio: f64,

    /// LayeredGrid sweep: comma-separated `bps` values (single spacing param).
    #[arg(long, default_value = "2,4,6,8,10")]
    lg_bps_list: String,

    /// LayeredGrid sweep: comma-separated `levels` values.
    #[arg(long, default_value = "1,2,3,4,5")]
    lg_levels_list: String,

    /// StaticGrid sweep: comma-separated `inner_bps` values.
    #[arg(long, default_value = "3,6,10")]
    sg_inner_bps_list: String,

    /// StaticGrid sweep: comma-separated `step_bps` values.
    #[arg(long, default_value = "3,6")]
    sg_step_bps_list: String,

    /// StaticGrid sweep: comma-separated `levels_per_side` values.
    #[arg(long, default_value = "3,5")]
    sg_levels_list: String,

    /// StaticGrid sweep: comma-separated `target_fills_per_min` values
    /// (decimals). `0` disables the adaptive scaler. Default `0` keeps
    /// the scaler off so baseline sweeps are comparable.
    #[arg(long, default_value = "0")]
    sg_target_fpm_list: String,

    /// StaticGrid sweep: comma-separated `fillrate_window_secs` values.
    #[arg(long, default_value = "60")]
    sg_fpm_window_list: String,

    /// StaticGrid sweep: comma-separated `scale_min` values (decimals).
    #[arg(long, default_value = "1.0")]
    sg_scale_min_list: String,

    /// StaticGrid sweep: comma-separated `scale_max` values (decimals).
    #[arg(long, default_value = "4.0")]
    sg_scale_max_list: String,

    /// SimpleGap sweep: comma-separated fixed gaps from mid, in bps.
    #[arg(long, default_value = "4")]
    simple_gap_bps_list: String,

    /// SimpleGap notional per order.
    #[arg(long, default_value = "100")]
    simple_gap_notional: String,

    /// LadderReentry notional per order.
    #[arg(long, default_value = "100")]
    ladder_reentry_notional: String,

    /// MicroMeanReversion notional per order.
    #[arg(long, default_value = "100")]
    micro_mean_reversion_notional: String,

    /// MicroMeanReversion sweep: comma-separated trigger distances in bps.
    #[arg(long, default_value = "8,10,12")]
    mmr_trigger_bps_list: String,

    /// MicroMeanReversion sweep: comma-separated passive entry distances in bps.
    #[arg(long, default_value = "1,2,3")]
    mmr_entry_bps_list: String,

    /// MicroMeanReversion sweep: comma-separated exit distances from fill in bps.
    #[arg(long, default_value = "4,6,8")]
    mmr_exit_bps_list: String,

    /// SpreadScalp notional per order.
    #[arg(long, default_value = "100")]
    spread_scalp_notional: String,

    /// SpreadScalp sweep: comma-separated min spread in bps.
    #[arg(long, default_value = "5,7,10")]
    spread_scalp_min_spread_bps_list: String,

    /// Perp funding rate per 8h in bps (signed). Default 1 (~0.01%/8h,
    /// typical Binance mid-cap). Positive = longs pay shorts. Set to 0
    /// to disable funding accrual entirely.
    #[arg(long, default_value_t = 1i32)]
    funding_bps_per_8h: i32,

    /// FillSim: submit-ack latency in ms. Bumping this from 0 exposes
    /// post-only crosses on fast moves (book ticks through our intended
    /// price between decision and ack). Realistic NA → AWS-Tokyo ~50ms.
    #[arg(long, default_value_t = 50u64)]
    sim_submit_latency_ms: u64,

    /// FillSim: cancel-ack latency in ms.
    #[arg(long, default_value_t = 10u64)]
    sim_cancel_latency_ms: u64,

    /// FillSim: synthetic `-2019` margin cap in USDT notional (signed
    /// position abs). `0` = unlimited.
    #[arg(long, default_value_t = 0.0_f64)]
    sim_max_position_notional: f64,

    /// FillSim: silent-cancel rate per minute per live quote (simulates
    /// venue cancel/expire events the WS misses; runner reconciliation
    /// eventually purges them). `0.0` = disabled.
    #[arg(long, default_value_t = 0.0_f64)]
    sim_silent_cancel_rate_per_min: f64,

    /// FillSim: deterministic RNG seed for silent-cancel rolls.
    #[arg(long, default_value_t = 0u64)]
    sim_rng_seed: u64,
}

fn parse_u32_list(s: &str) -> Result<Vec<u32>, String> {
    s.split(',')
        .map(|t| t.trim())
        .filter(|t| !t.is_empty())
        .map(|t| t.parse::<u32>().map_err(|e| format!("bad u32 '{t}': {e}")))
        .collect()
}

fn parse_decimal_list(s: &str) -> Result<Vec<Decimal>, String> {
    s.split(',')
        .map(|t| t.trim())
        .filter(|t| !t.is_empty())
        .map(|t| Decimal::from_str(t).map_err(|e| format!("bad decimal '{t}': {e}")))
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let (base_str, quote_str) = split_symbol(&args.symbol);
    let symbol = Symbol {
        base: Asset::new(base_str),
        quote: Asset::new(quote_str),
        venue: VenueId::new("binance"),
        kind: MarketKind::Perp,
    };

    let size_per_quote = Size(Decimal::from_str(&args.size)?);
    let tick = Decimal::from_str(&args.tick_size)?;
    let fees = VenueFees {
        maker_bps: args.maker_bps,
        taker_bps: args.taker_bps,
    };
    // Binance Futures USD-M with BNB-pay enabled = 10% discount on both
    // sides → 1.8 bps maker, 4.5 bps taker (rounded). See [[binance-fees]].
    let bnb_fees = VenueFees {
        maker_bps: ((args.maker_bps * 9) as f64 / 10.0).round() as i32,
        taker_bps: ((args.taker_bps as f64 * 0.9).round()) as u32,
    };
    let ewma = EwmaConfig {
        half_life_sec: 60.0,
        initial_var: Decimal::from_str("0.000001")?,
    };

    // Skim mode (per-preset, shared config). Disabled when budget==0.
    let skim_cfg: Option<SkimConfig> = if args.budget > 0.0 {
        Some(SkimConfig {
            budget: Decimal::try_from(args.budget)?,
            skim_pct: Decimal::try_from(args.skim_pct / 100.0)?,
            skim_ratio: Decimal::try_from(args.skim_ratio)?,
        })
    } else {
        None
    };
    // Perp funding model. Disabled when rate == 0.
    let funding_cfg: Option<FundingConfig> = if args.funding_bps_per_8h != 0 {
        Some(FundingConfig {
            rate_bps_per_8h: args.funding_bps_per_8h,
        })
    } else {
        None
    };

    let sim_cfg_template = FillSimConfig {
        submit_latency_ms: args.sim_submit_latency_ms,
        cancel_latency_ms: args.sim_cancel_latency_ms,
        fees,
        max_position_notional_usdt: if args.sim_max_position_notional > 0.0 {
            Some(Decimal::try_from(args.sim_max_position_notional)?)
        } else {
            None
        },
        silent_cancel_rate_per_min: args.sim_silent_cancel_rate_per_min,
        rng_seed: args.sim_rng_seed,
    };
    let simple_gap_notional = Decimal::from_str(&args.simple_gap_notional)?;
    let ladder_reentry_notional = Decimal::from_str(&args.ladder_reentry_notional)?;
    let micro_mean_reversion_notional = Decimal::from_str(&args.micro_mean_reversion_notional)?;
    let spread_scalp_notional = Decimal::from_str(&args.spread_scalp_notional)?;

    // Load + sort + validate parquet once; share across all presets via Arc.
    let load_start = std::time::Instant::now();
    let shared_data = LoadedReplayData::load(ReplayConfig {
        heartbeat_ms: args.heartbeat_ms,
        symbols: vec![symbol.clone()],
        data_dir: args.data_dir.clone(),
        tick_size: tick,
        allow_seq_gaps: true,
    })?;
    info!(
        events = shared_data.len(),
        elapsed_ms = load_start.elapsed().as_millis() as u64,
        "parquet load done"
    );

    // Build all preset handles up front; each runs as a tokio task. The
    // multi-thread runtime fans them across cores. State dirs are unique
    // per preset (derived from the preset name) so concurrent snapshot /
    // resume writes don't collide.
    let mut handles: Vec<JoinHandle<(String, PaperReport)>> = Vec::new();

    spawn_preset(
        &mut handles,
        &shared_data,
        &symbol,
        "A-S γ=0.1 5bps",
        AvellanedaStoikov::new(AvellanedaStoikovConfig {
            gamma: Decimal::from_str("0.1")?,
            base_spread_bps: 5,
            horizon_sec: 3600,
            size_per_quote,
            min_requote_interval_ms: 1000,
            level_step_bps: 1,
            volatility: ewma.clone(),
        }),
        fees,
        skim_cfg,
        funding_cfg,
        sim_cfg_template.clone(),
    );

    spawn_preset(
        &mut handles,
        &shared_data,
        &symbol,
        "GLFT γ=0.1 5bps",
        Glft::new(GlftConfig {
            gamma: Decimal::from_str("0.1")?,
            base_spread_bps: 5,
            size_per_quote,
            min_requote_interval_ms: 1000,
            level_step_bps: 1,
            volatility: ewma.clone(),
        }),
        fees,
        skim_cfg,
        funding_cfg,
        sim_cfg_template.clone(),
    );

    spawn_preset(
        &mut handles,
        &shared_data,
        &symbol,
        "TOB improve=1 noskew",
        TopOfBook::new(TopOfBookConfig {
            size_per_quote,
            tick_size: tick,
            improve_when_spread_gt_ticks: 1,
            min_requote_interval_ms: 1000,
            max_skew_ticks: 0,
            skew_unit: Size(Decimal::from(1)),
            max_imbalance_ticks: 0,
        }),
        fees,
        skim_cfg,
        funding_cfg,
        sim_cfg_template.clone(),
    );

    spawn_preset(
        &mut handles,
        &shared_data,
        &symbol,
        "TOB pure-join",
        TopOfBook::new(TopOfBookConfig {
            size_per_quote,
            tick_size: tick,
            improve_when_spread_gt_ticks: 1_000_000,
            min_requote_interval_ms: 1000,
            max_skew_ticks: 0,
            skew_unit: Size(Decimal::from(1)),
            max_imbalance_ticks: 0,
        }),
        fees,
        skim_cfg,
        funding_cfg,
        sim_cfg_template.clone(),
    );

    spawn_preset(
        &mut handles,
        &shared_data,
        &symbol,
        "TOB improve=1 skew(10,0.005)",
        TopOfBook::new(TopOfBookConfig {
            size_per_quote,
            tick_size: tick,
            improve_when_spread_gt_ticks: 1,
            min_requote_interval_ms: 1000,
            max_skew_ticks: 10,
            skew_unit: Size(Decimal::from_str("0.005")?),
            max_imbalance_ticks: 0,
        }),
        fees,
        skim_cfg,
        funding_cfg,
        sim_cfg_template.clone(),
    );

    spawn_preset(
        &mut handles,
        &shared_data,
        &symbol,
        "TOB improve=1 skew(20,0.005)",
        TopOfBook::new(TopOfBookConfig {
            size_per_quote,
            tick_size: tick,
            improve_when_spread_gt_ticks: 1,
            min_requote_interval_ms: 1000,
            max_skew_ticks: 20,
            skew_unit: Size(Decimal::from_str("0.005")?),
            max_imbalance_ticks: 0,
        }),
        fees,
        skim_cfg,
        funding_cfg,
        sim_cfg_template.clone(),
    );

    for max_imb in [3u32, 5, 7, 10, 20] {
        let name = format!("TOB improve=1 imb({max_imb})");
        spawn_preset(
            &mut handles,
            &shared_data,
            &symbol,
            &name,
            TopOfBook::new(TopOfBookConfig {
                size_per_quote,
                tick_size: tick,
                improve_when_spread_gt_ticks: 1,
                min_requote_interval_ms: 1000,
                max_skew_ticks: 0,
                skew_unit: Size(Decimal::from(1)),
                max_imbalance_ticks: max_imb,
            }),
            fees,
            skim_cfg,
            funding_cfg,
            sim_cfg_template.clone(),
        );
    }

    spawn_preset(
        &mut handles,
        &shared_data,
        &symbol,
        "TOB improve=1 skew(10) + imb(5)",
        TopOfBook::new(TopOfBookConfig {
            size_per_quote,
            tick_size: tick,
            improve_when_spread_gt_ticks: 1,
            min_requote_interval_ms: 1000,
            max_skew_ticks: 10,
            skew_unit: Size(Decimal::from_str("0.005")?),
            max_imbalance_ticks: 5,
        }),
        fees,
        skim_cfg,
        funding_cfg,
        sim_cfg_template.clone(),
    );

    spawn_preset(
        &mut handles,
        &shared_data,
        &symbol,
        "TOB improve=1 noskew (BNB)",
        TopOfBook::new(TopOfBookConfig {
            size_per_quote,
            tick_size: tick,
            improve_when_spread_gt_ticks: 1,
            min_requote_interval_ms: 1000,
            max_skew_ticks: 0,
            skew_unit: Size(Decimal::from(1)),
            max_imbalance_ticks: 0,
        }),
        bnb_fees,
        skim_cfg,
        funding_cfg,
        sim_cfg_template.clone(),
    );

    spawn_preset(
        &mut handles,
        &shared_data,
        &symbol,
        "TOB improve=1 skew(10,0.005) (BNB)",
        TopOfBook::new(TopOfBookConfig {
            size_per_quote,
            tick_size: tick,
            improve_when_spread_gt_ticks: 1,
            min_requote_interval_ms: 1000,
            max_skew_ticks: 10,
            skew_unit: Size(Decimal::from_str("0.005")?),
            max_imbalance_ticks: 0,
        }),
        bnb_fees,
        skim_cfg,
        funding_cfg,
        sim_cfg_template.clone(),
    );

    // Micro-price sweep: half-spread 1/2/3/5 ticks. Direct comparable against
    // the TOB imbalance sweep — both react to top-of-book size imbalance, but
    // micro-price uses a continuous weighted mid instead of discrete tick shifts.
    for half in [1u32, 2, 3, 5] {
        let name = format!("micro-price half={half}t");
        spawn_preset(
            &mut handles,
            &shared_data,
            &symbol,
            &name,
            MicroPrice::new(MicroPriceConfig {
                size_per_quote,
                tick_size: tick,
                half_spread_ticks: half,
                min_requote_interval_ms: 1000,
                max_skew_ticks: 0,
                skew_unit: Size(Decimal::from(1)),
            }),
            fees,
            skim_cfg,
            funding_cfg,
            sim_cfg_template.clone(),
        );
    }

    // Micro-price + inventory skew combined.
    spawn_preset(
        &mut handles,
        &shared_data,
        &symbol,
        "micro-price half=2t skew(10,0.005)",
        MicroPrice::new(MicroPriceConfig {
            size_per_quote,
            tick_size: tick,
            half_spread_ticks: 2,
            min_requote_interval_ms: 1000,
            max_skew_ticks: 10,
            skew_unit: Size(Decimal::from_str("0.005")?),
        }),
        fees,
        skim_cfg,
        funding_cfg,
        sim_cfg_template.clone(),
    );

    // Layered grid sweep — re-entry-scalping ladder, fill-driven. Re-entry
    // bps dominates per-cycle PnL (must clear 2× maker fee or it's a loser).
    // notional_per_order is dollars per limit; coin qty = notional / price.
    // Levels-per-side sweep at the best re-entry from the prior sweep
    // (re=20 peaked on the 2h BTC sample). More levels = more capital
    // committed (each level adds $100 of resting orders on both sides),
    // but also more chances to catch the spread.
    let lg_bps_sweep = parse_u32_list(&args.lg_bps_list)?;
    let lg_levels_sweep = parse_u32_list(&args.lg_levels_list)?;
    for &bps in &lg_bps_sweep {
        for &levels in &lg_levels_sweep {
            let label = format!("LG bps={bps} lv={levels}");
            spawn_preset(
                &mut handles,
                &shared_data,
                &symbol,
                &label,
                LayeredGrid::new(LayeredGridConfig {
                    notional_per_order: Decimal::from(100),
                    levels_per_side: levels,
                    inner_bps: bps,
                }),
                fees,
                skim_cfg,
                funding_cfg,
                sim_cfg_template.clone(),
            );
        }
    }

    spawn_preset(
        &mut handles,
        &shared_data,
        &symbol,
        "LadderReentry in=5 st=1 lv=10 re=5 cont=11",
        LadderReentry::new(LadderReentryConfig {
            notional_per_order: ladder_reentry_notional,
            levels_per_side: 10,
            inner_bps: 5,
            step_bps: 1,
            reentry_bps: 5,
            continuation_bps: 11,
        }),
        fees,
        skim_cfg,
        funding_cfg,
        sim_cfg_template.clone(),
    );

    // SimpleGap — one fixed-distance bid/ask pair, then another pair after
    // every fill. No cancels, skew, requotes, or inventory logic.
    let simple_gap_sweep = parse_u32_list(&args.simple_gap_bps_list)?;
    for &gap in &simple_gap_sweep {
        let label = format!("SimpleGap gap={gap}bps");
        spawn_preset(
            &mut handles,
            &shared_data,
            &symbol,
            &label,
            SimpleGap::new(SimpleGapConfig {
                notional_per_order: simple_gap_notional,
                gap_bps: gap,
            }),
            fees,
            skim_cfg,
            funding_cfg,
            sim_cfg_template.clone(),
        );
    }

    let mmr_trigger_sweep = parse_u32_list(&args.mmr_trigger_bps_list)?;
    let mmr_entry_sweep = parse_u32_list(&args.mmr_entry_bps_list)?;
    let mmr_exit_sweep = parse_u32_list(&args.mmr_exit_bps_list)?;
    for &trigger in &mmr_trigger_sweep {
        for &entry in &mmr_entry_sweep {
            for &exit in &mmr_exit_sweep {
                let label = format!("MMR trig={trigger} entry={entry} exit={exit}");
                spawn_preset(
                    &mut handles,
                    &shared_data,
                    &symbol,
                    &label,
                    MicroMeanReversion::new(MicroMeanReversionConfig {
                        notional_per_order: micro_mean_reversion_notional,
                        trigger_bps: trigger,
                        entry_bps: entry,
                        exit_bps: exit,
                        max_open_entries: 1,
                    }),
                    fees,
                    skim_cfg,
                    funding_cfg,
                    sim_cfg_template.clone(),
                );
            }
        }
    }

    let spread_scalp_spread_sweep = parse_decimal_list(&args.spread_scalp_min_spread_bps_list)?;
    for &min_spread_bps in &spread_scalp_spread_sweep {
        let label = format!("SpreadScalp spread>={min_spread_bps}bps");
        spawn_preset(
            &mut handles,
            &shared_data,
            &symbol,
            &label,
            SpreadScalp::new(SpreadScalpConfig {
                notional_per_order: spread_scalp_notional,
                tick_size: tick,
                step_size: tick,
                min_notional: Decimal::ZERO,
                min_spread_bps,
                requote_interval_ms: 1000,
                max_position_usdt: Decimal::ZERO,
                take_profit_usdt: Decimal::ZERO,
                reject_cooldown_ms: 0,
            }),
            fees,
            skim_cfg,
            funding_cfg,
            sim_cfg_template.clone(),
        );
    }

    // StaticGrid sweep — place-once-then-sit grid. Triggers a fresh batch
    // when remaining open quotes are <= 2 OR one side is empty. Pure passive
    // accumulation vs the rolling re-anchor of LG.
    let sg_inner_sweep = parse_u32_list(&args.sg_inner_bps_list)?;
    let sg_step_sweep = parse_u32_list(&args.sg_step_bps_list)?;
    let sg_levels_sweep = parse_u32_list(&args.sg_levels_list)?;
    let sg_fpm_sweep = parse_decimal_list(&args.sg_target_fpm_list)?;
    let sg_fpm_window_sweep = parse_u32_list(&args.sg_fpm_window_list)?;
    let sg_scale_min_sweep = parse_decimal_list(&args.sg_scale_min_list)?;
    let sg_scale_max_sweep = parse_decimal_list(&args.sg_scale_max_list)?;
    for &inner in &sg_inner_sweep {
        for &step in &sg_step_sweep {
            for &levels in &sg_levels_sweep {
                for &fpm_target in &sg_fpm_sweep {
                    for &fpm_window in &sg_fpm_window_sweep {
                        for &sc_min in &sg_scale_min_sweep {
                            for &sc_max in &sg_scale_max_sweep {
                                if sc_min > sc_max {
                                    continue;
                                }
                                let label = format!(
                                    "SG in={inner} st={step} lv={levels} fpm={fpm_target} w={fpm_window} sm={sc_min} sM={sc_max}",
                                );
                                spawn_preset(
                                    &mut handles,
                                    &shared_data,
                                    &symbol,
                                    &label,
                                    StaticGrid::new(StaticGridConfig {
                                        notional_per_order: Decimal::from(100),
                                        levels_per_side: levels,
                                        inner_bps: inner,
                                        step_bps: step,
                                        step_size: Decimal::from(1),
                                        min_notional: Decimal::ZERO,
                                        target_fills_per_min: fpm_target,
                                        fillrate_window_secs: fpm_window,
                                        scale_min: sc_min,
                                        scale_max: sc_max,
                                        auto_skew: true,
                                    }),
                                    fees,
                                    skim_cfg,
                                    funding_cfg,
                                    sim_cfg_template.clone(),
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    let sweep_start = std::time::Instant::now();
    info!(
        presets = handles.len(),
        "awaiting parallel preset completion"
    );
    let mut results: Vec<(String, PaperReport)> = Vec::with_capacity(handles.len());
    for h in handles {
        results.push(h.await?);
    }
    info!(
        elapsed_ms = sweep_start.elapsed().as_millis() as u64,
        "all presets done"
    );

    print_table(&results);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_one<S: Strategy>(
    shared_data: Arc<LoadedReplayData>,
    symbol: Symbol,
    state_id: String,
    strategy: S,
    fees: VenueFees,
    skim: Option<SkimConfig>,
    funding: Option<FundingConfig>,
    sim_cfg: FillSimConfig,
) -> PaperReport {
    let replay = ParquetReplay::from_shared(shared_data);
    let venue = BacktestVenue::new(replay);
    let fill_sim = FillSim::new(FillSimConfig { fees, ..sim_cfg });
    let runner_config = RunnerConfig {
        state_dir: PathBuf::from(format!("./state/backtest_compare/{}", state_id)),
        snapshot_every_n_events: 0,
        skim,
        funding,
        snapshot_tap: None,
        live_tap: None,
        notional_rx: None,
    };
    let (_tx, rx) = watch::channel(false);
    let external_fills: Option<tokio::sync::mpsc::UnboundedReceiver<Fill>> = None;
    info!(strategy = strategy.name(), preset = %state_id, "preset start");
    let report = run_with_resume(
        venue,
        strategy,
        fill_sim,
        symbol,
        rx,
        runner_config,
        None,
        None,
        None,
        external_fills,
    )
    .await;
    info!(
        preset = %state_id,
        events = report.events_processed,
        fills = report.fills_emitted,
        "preset done"
    );
    report
}

#[allow(clippy::too_many_arguments)]
fn spawn_preset<S: Strategy + Send + 'static>(
    handles: &mut Vec<JoinHandle<(String, PaperReport)>>,
    shared_data: &Arc<LoadedReplayData>,
    symbol: &Symbol,
    name: &str,
    strategy: S,
    fees: VenueFees,
    skim: Option<SkimConfig>,
    funding: Option<FundingConfig>,
    sim_cfg: FillSimConfig,
) {
    let sd = Arc::clone(shared_data);
    let sym = symbol.clone();
    let display = name.to_string();
    let state_id = name
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect::<String>();
    handles.push(tokio::spawn(async move {
        let r = run_one(sd, sym, state_id, strategy, fees, skim, funding, sim_cfg).await;
        (display, r)
    }));
}

fn print_table(results: &[(String, PaperReport)]) {
    let skim_active = results
        .iter()
        .any(|(_, r)| r.skim_count > 0 || decimal_to_f64(&r.final_perp_balance.0) != 0.0);

    // Detect base asset label from any preset with skim active. Empty
    // (no skim) → header reads generic "base stack".
    let base_label = results
        .iter()
        .find_map(|(_, r)| {
            if r.base_asset.is_empty() {
                None
            } else {
                Some(format!("{} stack", r.base_asset))
            }
        })
        .unwrap_or_else(|| "base stack".to_string());

    println!();
    if skim_active {
        println!(
            "{:<36} {:>7} {:>9} {:>11} {:>11} {:>6} {:>11} {:>12} {:>12}",
            "preset",
            "fills",
            "fills/min",
            "realized",
            "fees",
            "skims",
            base_label,
            "perp+unreal",
            "TOTAL ACCT"
        );
        println!("{}", "-".repeat(120));
    } else {
        println!(
            "{:<36} {:>7} {:>9} {:>11} {:>10} {:>11} {:>11} {:>11}",
            "preset", "fills", "fills/min", "realized", "unrealized", "fees", "NET", "$/fill"
        );
        println!("{}", "-".repeat(110));
    }
    for (name, r) in results {
        // Use sim_duration (data-time span) not runtime_secs (wall-clock
        // replay speed) so fills/min reflects market-time throughput.
        let sim_min = (r.sim_duration_secs as f64) / 60.0;
        let fills_per_min = if sim_min > 0.0 {
            r.fills_emitted as f64 / sim_min
        } else {
            0.0
        };
        let net = decimal_to_f64(&r.net.0);
        if skim_active {
            let perp = decimal_to_f64(&r.final_perp_balance.0);
            let btc_v = decimal_to_f64(&r.final_base_value.0);
            let total = perp + btc_v;
            println!(
                "{:<36} {:>7} {:>9.2} {:>11.4} {:>11.4} {:>6} {:>10.6} {:>12.4} {:>12.4}",
                name,
                r.fills_emitted,
                fills_per_min,
                decimal_to_f64(&r.realized.0),
                decimal_to_f64(&r.fees.0),
                r.skim_count,
                decimal_to_f64(&r.base_stacked.0),
                perp,
                total,
            );
        } else {
            let dollars_per_fill = if r.fills_emitted > 0 {
                net / r.fills_emitted as f64
            } else {
                0.0
            };
            println!(
                "{:<36} {:>7} {:>9.2} {:>11.4} {:>10.4} {:>11.4} {:>11.4} {:>11.5}",
                name,
                r.fills_emitted,
                fills_per_min,
                decimal_to_f64(&r.realized.0),
                decimal_to_f64(&r.unrealized.0),
                decimal_to_f64(&r.fees.0),
                net,
                dollars_per_fill,
            );
        }
    }
    println!();
    // Footer: best/worst NET.
    if let (Some(best), Some(worst)) = (
        results.iter().max_by(|a, b| {
            decimal_to_f64(&a.1.net.0)
                .partial_cmp(&decimal_to_f64(&b.1.net.0))
                .unwrap()
        }),
        results.iter().min_by(|a, b| {
            decimal_to_f64(&a.1.net.0)
                .partial_cmp(&decimal_to_f64(&b.1.net.0))
                .unwrap()
        }),
    ) {
        println!(
            "best:  {:<36} NET = {:>11.4}",
            best.0,
            decimal_to_f64(&best.1.net.0)
        );
        println!(
            "worst: {:<36} NET = {:>11.4}",
            worst.0,
            decimal_to_f64(&worst.1.net.0)
        );
    }
    println!();
}

fn decimal_to_f64(d: &Decimal) -> f64 {
    d.to_string().parse::<f64>().unwrap_or(0.0)
}

fn split_symbol(sym: &str) -> (&str, &str) {
    for suffix in &["USDT", "BUSD", "USDC", "TUSD"] {
        if let Some(base) = sym.strip_suffix(suffix)
            && !base.is_empty()
        {
            return (base, suffix);
        }
    }
    let n = sym.len();
    if n > 4 {
        (&sym[..n - 4], &sym[n - 4..])
    } else {
        (sym, "USDT")
    }
}

// ---------------------------------------------------------------------------
// BacktestVenue (mirrors run_backtest.rs)
// ---------------------------------------------------------------------------

struct BacktestVenue {
    replay: Mutex<Option<ParquetReplay>>,
}

impl BacktestVenue {
    fn new(replay: ParquetReplay) -> Self {
        Self {
            replay: Mutex::new(Some(replay)),
        }
    }
}

#[async_trait]
impl Venue for BacktestVenue {
    fn id(&self) -> &str {
        "backtest"
    }

    async fn snapshot(&self, _symbol: &Symbol) -> Result<Snapshot, VenueError> {
        Err(VenueError::Internal(Box::new(std::io::Error::other(
            "BacktestVenue::snapshot not supported",
        ))))
    }

    async fn subscribe(&self, _symbol: &Symbol) -> Result<BoxStream<'_, MarketEvent>, VenueError> {
        let replay = self.replay.lock().unwrap().take().ok_or_else(|| {
            VenueError::Internal(Box::new(std::io::Error::other(
                "BacktestVenue::subscribe called twice",
            )))
        })?;
        let s = stream::unfold(replay, |mut r| async move {
            use tikr_backtest::replay::Replay;
            r.next().await.map(|ev| (ev, r))
        });
        Ok(Box::pin(s))
    }

    async fn quote(&self, _intent: QuoteIntent) -> Result<QuoteId, VenueError> {
        Ok(QuoteId::new())
    }
    async fn requote(&self, _id: QuoteId, _intent: QuoteIntent) -> Result<(), VenueError> {
        Ok(())
    }
    async fn cancel(&self, _id: QuoteId) -> Result<(), VenueError> {
        Ok(())
    }
    async fn cancel_all(&self, _symbol: &Symbol) -> Result<(), VenueError> {
        Ok(())
    }
    async fn position(&self, symbol: &Symbol) -> Result<Position, VenueError> {
        Ok(Position {
            symbol: symbol.clone(),
            size: SignedSize(Decimal::ZERO),
            avg_entry: tikr_core::Price(Decimal::ZERO),
            realized_pnl: tikr_core::Notional(Decimal::ZERO),
        })
    }
    async fn fills_since(&self, _since_ts: u64) -> Result<Vec<Fill>, VenueError> {
        Ok(Vec::new())
    }
}
