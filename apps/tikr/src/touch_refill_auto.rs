//! Auto-rotation manager for TouchRefill across all USD-M perps that
//! meet a minimum tick-bps threshold.
//!
//! Every `recheck_interval_secs` (default 60s):
//! 1. Query Binance Futures exchangeInfo + ticker/price + ticker/24hr.
//! 2. Filter to symbols with `tick_size / price × 10000 ≥ min_tick_bps`
//!    and `24h quote volume ≥ min_volume_usdt`.
//! 3. Diff against currently-running set:
//!    - New symbols → spawn a TouchRefill supervisor.
//!    - Symbols that fell below threshold → signal shutdown, then
//!      cancel-all + flatten via `reset_symbol_state`.
//!
//! No slot limit — every qualifying symbol gets a bot. Per-bot notional
//! is the account-wide default split by the count of active bots
//! (handled by the existing wallet poller).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use rust_decimal::Decimal;
use tikr_binance::{BinanceEnv, BinanceKeyMaterial};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::config::{BotConfig, TouchRefillAutoConfig, TouchRefillParams};
use crate::state::{BotStatus, BotView, SharedBotState};
use crate::supervisor::{SupervisorCtx, reset_symbol_state, spawn_supervisor};
use crate::venue;

/// Account/env context shared by all spawned TouchRefill supervisors.
pub struct TouchRefillAutoAccountCtx {
    pub env: BinanceEnv,
    pub api_key: String,
    pub key_material: Arc<BinanceKeyMaterial>,
    pub base_state_dir: std::path::PathBuf,
    pub order_balance_pct: Decimal,
    pub leverage: u32,
    pub max_position_pct: Decimal,
    pub notional_rx: watch::Receiver<Decimal>,
    pub max_position_rx: watch::Receiver<Decimal>,
    pub bnb_price_rx: watch::Receiver<Decimal>,
}

struct ActiveBot {
    shutdown_tx: watch::Sender<bool>,
    handle: JoinHandle<()>,
}

/// Spawn the auto-rotation manager. Returns immediately; manager runs
/// in the background until global shutdown fires.
pub fn spawn_touch_refill_auto_manager(
    cfg: TouchRefillAutoConfig,
    account: TouchRefillAutoAccountCtx,
    shared_state: SharedBotState,
    mut global_shutdown: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let http = reqwest::Client::new();
        let mut active: HashMap<String, ActiveBot> = HashMap::new();
        let recheck = cfg.recheck_interval_secs.max(10);
        let mut tick = tokio::time::interval(Duration::from_secs(recheck));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        info!(
            min_tick_bps = %cfg.min_tick_bps,
            min_volume_usdt = %cfg.min_volume_usdt,
            recheck_interval_secs = recheck,
            quote_asset = %cfg.quote_asset,
            "touch_refill_auto: starting discovery loop"
        );

        loop {
            tokio::select! {
                _ = tick.tick() => {}
                _ = global_shutdown.changed() => {
                    if *global_shutdown.borrow() {
                        info!("touch_refill_auto: global shutdown — flushing all bots");
                        let symbols: Vec<String> = active.keys().cloned().collect();
                        for (_, bot) in active.drain() {
                            let _ = bot.shutdown_tx.send(true);
                            let _ = tokio::time::timeout(
                                Duration::from_secs(5),
                                bot.handle,
                            )
                            .await;
                        }
                        flatten_symbols(&symbols, &account).await;
                        return;
                    }
                }
            }

            // 1. Discover qualifying symbols.
            let discovered = match tikr_binance::futs::list_perp_tick_info(
                &http,
                account.env.rest_base_url(),
                &cfg.quote_asset,
            )
            .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    warn!(error = ?e, "touch_refill_auto: discovery failed, retrying next cycle");
                    continue;
                }
            };

            let allowlist: HashSet<&str> =
                cfg.symbols_allowlist.iter().map(|s| s.as_str()).collect();
            let mut qualifying: HashSet<String> = HashSet::new();
            for row in discovered {
                if !allowlist.is_empty() {
                    // Allowlist mode: operator chose these symbols
                    // explicitly. Skip tick_bps + volume filters —
                    // min_self_spread_bps synthesizes the required
                    // spread regardless of book spread.
                    if allowlist.contains(row.symbol.as_str()) {
                        qualifying.insert(row.symbol);
                    }
                } else if row.tick_bps >= cfg.min_tick_bps
                    && row.quote_volume_24h >= cfg.min_volume_usdt
                {
                    qualifying.insert(row.symbol);
                }
            }
            info!(
                qualifying = qualifying.len(),
                running = active.len(),
                "touch_refill_auto: discovery tick"
            );

            // 2. Spawn missing symbols.
            for symbol in &qualifying {
                if active.contains_key(symbol) {
                    continue;
                }
                let bot = spawn_one_bot(
                    symbol,
                    &account,
                    &shared_state,
                    cfg.grid_levels,
                    cfg.min_self_spread_bps,
                    cfg.close_profit_bps,
                    cfg.grid_step_bps,
                );
                info!(symbol, "touch_refill_auto: spawned new bot");
                active.insert(symbol.clone(), bot);
            }

            // 3. Tear down symbols that fell off the qualifying list.
            let to_remove: Vec<String> = active
                .keys()
                .filter(|s| !qualifying.contains(s.as_str()))
                .cloned()
                .collect();
            for symbol in to_remove {
                if let Some(bot) = active.remove(&symbol) {
                    warn!(
                        symbol = %symbol,
                        "touch_refill_auto: tick_bps below threshold — shutting down + flattening"
                    );
                    let _ = bot.shutdown_tx.send(true);
                    let _ = tokio::time::timeout(Duration::from_secs(5), bot.handle).await;
                    flatten_symbols(std::slice::from_ref(&symbol), &account).await;
                    shared_state.set_status(
                        &symbol,
                        BotStatus::Crashed("removed: tick_bps below threshold".into()),
                    );
                }
            }
        }
    })
}

async fn flatten_symbols(symbols: &[String], account: &TouchRefillAutoAccountCtx) {
    for symbol_str in symbols {
        let symbol = venue::perp_symbol(symbol_str);
        match venue::build_venue(
            account.env,
            &account.api_key,
            &account.key_material,
            &symbol,
            account.leverage,
        )
        .await
        {
            Ok(v) => {
                info!(symbol = symbol_str, "touch_refill_auto: cancel + flatten");
                reset_symbol_state(&v, &symbol).await;
            }
            Err(e) => warn!(
                symbol = symbol_str,
                error = ?e,
                "touch_refill_auto: venue build for flatten failed"
            ),
        }
    }
}

fn spawn_one_bot(
    symbol: &str,
    account: &TouchRefillAutoAccountCtx,
    shared_state: &SharedBotState,
    grid_levels: u32,
    min_self_spread_bps: u32,
    close_profit_bps: u32,
    grid_step_bps: u32,
) -> ActiveBot {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let cfg = BotConfig {
        symbol: symbol.to_string(),
        strategy: "touch-refill".to_string(),
        // Strategy gets all its knobs from venue exchangeInfo + the
        // account-wide order_balance_pct.
        touch_refill: Some(TouchRefillParams {
            notional: None,
            grid_levels,
            min_self_spread_bps,
            close_profit_bps,
            grid_step_bps,
        }),
        sg: None,
        lg: None,
        ladder_reentry: None,
        simple_gap: None,
        micro_mean_reversion: None,
        spread_scalp: None,
        liq_fade: None,
        hydra: None,
    };
    shared_state.insert(
        symbol,
        BotView {
            label: format!("{symbol}/touch-refill"),
            symbol: symbol.to_string(),
            strategy: "touch-refill".to_string(),
            status: BotStatus::Starting,
            snapshot: Arc::new(RwLock::new(None)),
            live: Arc::new(RwLock::new(None)),
            shutdown_tx: None,
            api_position: Arc::new(RwLock::new(None)),
        },
    );
    let handle = spawn_supervisor(
        SupervisorCtx {
            cfg,
            env: account.env,
            api_key: account.api_key.clone(),
            key_material: account.key_material.clone(),
            base_state_dir: account.base_state_dir.clone(),
            order_balance_pct: account.order_balance_pct,
            leverage: account.leverage,
            max_position_pct: account.max_position_pct,
            // bot_count: best-effort estimate. Real per-bot notional
            // comes from notional_rx which is wallet-derived divided
            // by total_slots in main.rs; for auto-mode we lean on
            // the per-bot cap rather than precise notional sizing.
            bot_count: 1,
            notional_rx: account.notional_rx.clone(),
            max_position_rx: account.max_position_rx.clone(),
            bnb_price_rx: account.bnb_price_rx.clone(),
            // Auto-managed bots always start fresh — no inherited
            // state to preserve.
            clear_on_start: true,
        },
        shared_state.clone(),
        shutdown_rx,
    );
    ActiveBot {
        shutdown_tx,
        handle,
    }
}
