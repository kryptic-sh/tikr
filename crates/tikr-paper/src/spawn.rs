//! Bot orchestration helpers — wrap [`run_with_resume`] in a tokio task
//! and expose a live snapshot handle for dashboards / supervisors.
//!
//! Strategy choice is bundled in [`StrategyChoice`] so callers can describe
//! "give me a StaticGrid with these params" without juggling concrete
//! [`Strategy`] types and their associated [`Strategy::Config`] types
//! through trait-object gymnastics.

use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use tikr_backtest::fill_sim::FillSim;
use tikr_core::{Fill, Symbol};
use tikr_strategy::{
    AvellanedaStoikov, AvellanedaStoikovConfig, Glft, GlftConfig, Hydra, HydraConfig,
    LadderReentry, LadderReentryConfig, LayeredGrid, LayeredGridConfig, LiqFade, LiqFadeConfig,
    MicroMeanReversion, MicroMeanReversionConfig, SimpleGap, SimpleGapConfig, SpreadScalp,
    SpreadScalpConfig, StaticGrid, StaticGridConfig, Strategy, TopOfBook, TopOfBookConfig,
    TouchRefill, TouchRefillConfig,
};
use tikr_venue::Venue;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use tracing::Instrument;

use crate::live::LiveSnapshot;
use crate::report::PaperReport;
use crate::runner::{RunnerConfig, run_with_resume};

/// Strategy variants the bot orchestrator can construct from configuration.
///
/// Add new strategies here when integrating them with the dashboard /
/// supervisor. Each variant carries its strategy-specific config struct.
#[derive(Debug, Clone)]
pub enum StrategyChoice {
    /// [`StaticGrid`] — passive batched grid; rebuilds only when consumed.
    StaticGrid(StaticGridConfig),
    /// [`LayeredGrid`] — rolling re-anchored ladder.
    LayeredGrid(LayeredGridConfig),
    /// [`LadderReentry`] — seeded ladder with opposite-side reentry.
    LadderReentry(LadderReentryConfig),
    /// [`AvellanedaStoikov`] — finite-horizon inventory-aware MM.
    AvellanedaStoikov(AvellanedaStoikovConfig),
    /// [`Glft`] — Guéant-Lehalle-Fernandez-Tapia infinite-horizon MM.
    Glft(GlftConfig),
    /// [`TopOfBook`] — join/improve at best bid/ask.
    TopOfBook(TopOfBookConfig),
    /// [`SimpleGap`] — fixed-gap pair, add another pair after each fill.
    SimpleGap(SimpleGapConfig),
    /// [`MicroMeanReversion`] — overshoot capture with passive reversion exits.
    MicroMeanReversion(MicroMeanReversionConfig),
    /// [`SpreadScalp`] — quote inside wide spreads for passive scalp fills.
    SpreadScalp(SpreadScalpConfig),
    /// [`LiqFade`] — liquidation-cascade mean-revert stat-arb.
    LiqFade(LiqFadeConfig),
    /// [`Hydra`] — straddle-bracket entry + pyramid/DCA + maker TP / IOC SL.
    Hydra(HydraConfig),
    /// [`TouchRefill`] — minimal at-touch both-sided MM, refill on fill.
    TouchRefill(TouchRefillConfig),
}

impl StrategyChoice {
    /// Human-friendly strategy tag for logs/UI.
    pub fn label(&self) -> &'static str {
        match self {
            Self::StaticGrid(_) => "static-grid",
            Self::LayeredGrid(_) => "layered-grid",
            Self::LadderReentry(_) => "ladder-reentry",
            Self::AvellanedaStoikov(_) => "avellaneda-stoikov",
            Self::Glft(_) => "glft",
            Self::TopOfBook(_) => "top-of-book",
            Self::SimpleGap(_) => "simple-gap",
            Self::MicroMeanReversion(_) => "micro-mean-reversion",
            Self::SpreadScalp(_) => "spread-scalp",
            Self::LiqFade(_) => "liq-fade",
            Self::Hydra(_) => "hydra",
            Self::TouchRefill(_) => "touch-refill",
        }
    }
}

/// Specification handed to [`spawn_bot`].
pub struct BotSpec {
    /// Human-readable label (e.g. `"BTCUSDT/sg"`). Surfaced in dashboards
    /// and the supervisor's restart log.
    pub label: String,
    /// Symbol the bot trades.
    pub symbol: Symbol,
    /// Strategy + per-strategy config.
    pub strategy: StrategyChoice,
    /// Runner-level config. The bot will install its own `snapshot_tap`
    /// on top of whatever the caller passed (callers should leave that
    /// field `None`).
    pub runner_config: RunnerConfig,
    /// FillSim instance — unused in live mode but the runner takes it
    /// unconditionally.
    pub fill_sim: FillSim,
}

/// Handle returned by [`spawn_bot`]. Lets the supervisor read live state,
/// stop the bot, and await final completion.
pub struct BotHandle {
    /// Bot label (for display + logs).
    pub label: String,
    /// Symbol the bot is trading.
    pub symbol: Symbol,
    /// Tag of the strategy running inside the bot.
    pub strategy_label: &'static str,
    /// Live snapshot of the bot's current [`PaperReport`]. Updated on
    /// the same cadence as on-disk snapshots
    /// ([`RunnerConfig::snapshot_every_n_events`]). `None` until the
    /// first snapshot lands.
    pub state: Arc<RwLock<Option<PaperReport>>>,
    /// Live, fill-granular snapshot of position + open orders + last
    /// fill. Updated on every fill AND every regular snapshot tick.
    pub live: Arc<RwLock<Option<LiveSnapshot>>>,
    /// Shutdown signal. Send `true` to ask the bot to wind down.
    pub shutdown_tx: watch::Sender<bool>,
    /// Join handle for the underlying task. Resolves to the final
    /// [`PaperReport`] after a clean shutdown.
    pub join: JoinHandle<PaperReport>,
}

/// Spawn `spec` against `venue` in a fresh tokio task.
///
/// Returns immediately with a [`BotHandle`] the supervisor can use to
/// monitor + control the bot. Internally:
///
/// 1. Installs an `Arc<RwLock<Option<PaperReport>>>` snapshot tap on
///    `spec.runner_config` so the supervisor can read live state.
/// 2. Constructs the concrete [`Strategy`] from `spec.strategy`.
/// 3. Calls [`run_with_resume`] inside the spawned task.
///
/// `external_fills`: pass `Some(rx)` for live trading (real venue fills
/// feed the tracker) or `None` for paper-mode (FillSim drives fills).
pub fn spawn_bot<V>(
    spec: BotSpec,
    venue: V,
    external_fills: Option<mpsc::UnboundedReceiver<Fill>>,
    external_liqs: Option<mpsc::UnboundedReceiver<tikr_core::LiqEvent>>,
) -> BotHandle
where
    V: Venue + Send + 'static,
{
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let state: Arc<RwLock<Option<PaperReport>>> = Arc::new(RwLock::new(None));
    let live: Arc<RwLock<Option<LiveSnapshot>>> = Arc::new(RwLock::new(None));
    let mut config = spec.runner_config;
    config.snapshot_tap = Some(state.clone());
    config.live_tap = Some(live.clone());

    let strategy_label = spec.strategy.label();
    let symbol = spec.symbol.clone();
    let label = spec.label.clone();
    let fill_sim = spec.fill_sim;
    let choice = spec.strategy;

    // Tracing span: every log line emitted from inside the spawned
    // bot task carries `symbol = "BTCUSDT"` so a span-aware log
    // subscriber (e.g. the dashboard's per-tab capture) can route it
    // to the right bucket. Without this, `tokio::spawn` would detach
    // the task from any parent span and all bot logs would dump into
    // a shared "system" bucket — visible on every dashboard tab.
    let symbol_str =
        format!("{}{}", symbol.base.0.as_ref(), symbol.quote.0.as_ref()).to_uppercase();
    let bot_span = tracing::info_span!("bot", symbol = %symbol_str);

    let join = tokio::spawn(
        async move {
            match choice {
                StrategyChoice::StaticGrid(cfg) => {
                    let strategy = StaticGrid::new(cfg);
                    run_with_resume(
                        venue,
                        strategy,
                        fill_sim,
                        symbol,
                        shutdown_rx,
                        config,
                        None,
                        None,
                        None,
                        external_fills,
                        None,
                    )
                    .await
                }
                StrategyChoice::LayeredGrid(cfg) => {
                    let strategy = LayeredGrid::new(cfg);
                    run_with_resume(
                        venue,
                        strategy,
                        fill_sim,
                        symbol,
                        shutdown_rx,
                        config,
                        None,
                        None,
                        None,
                        external_fills,
                        None,
                    )
                    .await
                }
                StrategyChoice::LadderReentry(cfg) => {
                    let strategy = LadderReentry::new(cfg);
                    run_with_resume(
                        venue,
                        strategy,
                        fill_sim,
                        symbol,
                        shutdown_rx,
                        config,
                        None,
                        None,
                        None,
                        external_fills,
                        None,
                    )
                    .await
                }
                StrategyChoice::AvellanedaStoikov(cfg) => {
                    let strategy = AvellanedaStoikov::new(cfg);
                    run_with_resume(
                        venue,
                        strategy,
                        fill_sim,
                        symbol,
                        shutdown_rx,
                        config,
                        None,
                        None,
                        None,
                        external_fills,
                        None,
                    )
                    .await
                }
                StrategyChoice::Glft(cfg) => {
                    let strategy = Glft::new(cfg);
                    run_with_resume(
                        venue,
                        strategy,
                        fill_sim,
                        symbol,
                        shutdown_rx,
                        config,
                        None,
                        None,
                        None,
                        external_fills,
                        None,
                    )
                    .await
                }
                StrategyChoice::TopOfBook(cfg) => {
                    let strategy = TopOfBook::new(cfg);
                    run_with_resume(
                        venue,
                        strategy,
                        fill_sim,
                        symbol,
                        shutdown_rx,
                        config,
                        None,
                        None,
                        None,
                        external_fills,
                        None,
                    )
                    .await
                }
                StrategyChoice::SimpleGap(cfg) => {
                    let strategy = SimpleGap::new(cfg);
                    run_with_resume(
                        venue,
                        strategy,
                        fill_sim,
                        symbol,
                        shutdown_rx,
                        config,
                        None,
                        None,
                        None,
                        external_fills,
                        None,
                    )
                    .await
                }
                StrategyChoice::MicroMeanReversion(cfg) => {
                    let strategy = MicroMeanReversion::new(cfg);
                    run_with_resume(
                        venue,
                        strategy,
                        fill_sim,
                        symbol,
                        shutdown_rx,
                        config,
                        None,
                        None,
                        None,
                        external_fills,
                        None,
                    )
                    .await
                }
                StrategyChoice::SpreadScalp(cfg) => {
                    let strategy = SpreadScalp::new(cfg);
                    run_with_resume(
                        venue,
                        strategy,
                        fill_sim,
                        symbol,
                        shutdown_rx,
                        config,
                        None,
                        None,
                        None,
                        external_fills,
                        None,
                    )
                    .await
                }
                StrategyChoice::LiqFade(cfg) => {
                    // Live LiqFade consumes the `external_liqs`
                    // channel — caller spawns the venue-side
                    // `@forceOrder` subscription task and forwards
                    // events here. Backtest mode (`compare` binary)
                    // pre-loads the channel from
                    // `LiqEventStream::into_events()`.
                    let strategy = LiqFade::new(cfg);
                    run_with_resume(
                        venue,
                        strategy,
                        fill_sim,
                        symbol,
                        shutdown_rx,
                        config,
                        None,
                        None,
                        None,
                        external_fills,
                        external_liqs,
                    )
                    .await
                }
                StrategyChoice::Hydra(cfg) => {
                    let strategy = Hydra::new(cfg);
                    run_with_resume(
                        venue,
                        strategy,
                        fill_sim,
                        symbol,
                        shutdown_rx,
                        config,
                        None,
                        None,
                        None,
                        external_fills,
                        None,
                    )
                    .await
                }
                StrategyChoice::TouchRefill(cfg) => {
                    let strategy = TouchRefill::new(cfg);
                    run_with_resume(
                        venue,
                        strategy,
                        fill_sim,
                        symbol,
                        shutdown_rx,
                        config,
                        None,
                        None,
                        None,
                        external_fills,
                        None,
                    )
                    .await
                }
            }
        }
        .instrument(bot_span),
    );

    BotHandle {
        label,
        symbol: spec.symbol,
        strategy_label,
        state,
        live,
        shutdown_tx,
        join,
    }
}

/// Convenience: build a default [`RunnerConfig`] for a live bot, given
/// the on-disk state directory.
pub fn live_runner_config(state_dir: PathBuf) -> RunnerConfig {
    RunnerConfig {
        state_dir,
        snapshot_every_n_events: 100,
        skim: None,
        funding: None,
        snapshot_tap: None,
        live_tap: None,
        notional_rx: None,
        max_position_rx: None,
        liq_window_secs: 0,
        seed_position: None,
        equity_csv_path: None,
        initial_balance: tikr_core::Decimal::ZERO,
        order_balance_pct: tikr_core::Decimal::ZERO,
        max_position_pct: tikr_core::Decimal::ZERO,
        min_notional: tikr_core::Decimal::ZERO,
        max_expected_open_orders: 2,
    }
}
