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
    AvellanedaStoikov, AvellanedaStoikovConfig, Glft, GlftConfig, LayeredGrid, LayeredGridConfig,
    StaticGrid, StaticGridConfig, Strategy, TopOfBook, TopOfBookConfig,
};
use tikr_venue::Venue;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

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
    /// [`AvellanedaStoikov`] — finite-horizon inventory-aware MM.
    AvellanedaStoikov(AvellanedaStoikovConfig),
    /// [`Glft`] — Guéant-Lehalle-Fernandez-Tapia infinite-horizon MM.
    Glft(GlftConfig),
    /// [`TopOfBook`] — join/improve at best bid/ask.
    TopOfBook(TopOfBookConfig),
}

impl StrategyChoice {
    /// Human-friendly strategy tag for logs/UI.
    pub fn label(&self) -> &'static str {
        match self {
            Self::StaticGrid(_) => "static-grid",
            Self::LayeredGrid(_) => "layered-grid",
            Self::AvellanedaStoikov(_) => "avellaneda-stoikov",
            Self::Glft(_) => "glft",
            Self::TopOfBook(_) => "top-of-book",
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
) -> BotHandle
where
    V: Venue + Send + 'static,
{
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let state: Arc<RwLock<Option<PaperReport>>> = Arc::new(RwLock::new(None));
    let mut config = spec.runner_config;
    config.snapshot_tap = Some(state.clone());

    let strategy_label = spec.strategy.label();
    let symbol = spec.symbol.clone();
    let label = spec.label.clone();
    let fill_sim = spec.fill_sim;
    let choice = spec.strategy;

    let join = tokio::spawn(async move {
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
                )
                .await
            }
        }
    });

    BotHandle {
        label,
        symbol: spec.symbol,
        strategy_label,
        state,
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
    }
}
