//! Generic auto-rotation manager — replaces both `wave_auto` and `tide_auto`.
//!
//! Every `recheck_interval_secs` (default 60s):
//! 1. Query Binance Futures exchangeInfo + ticker/price + ticker/24hr via
//!    `list_perp_tick_info`.
//! 2. Pre-filter to liquid symbols: `24h quote volume ≥ min_volume_usdt`,
//!    optional allowlist.
//! 3. Score survivors by the configured `ScoreMode`:
//!    - `CandleHeight`: concurrent `get_1m_avg_candle_pct` over candidates,
//!      filtered `≥ min_candle_pct`.
//!    - `TickBps`: score from the already-fetched `tick_bps` field, filtered
//!      `≥ min_tick_bps` (no extra HTTP calls).
//! 4. Sort desc by score, truncate to `top_n`.
//! 5. Diff against the running set: spawn the configured `RampageStrategy` on
//!    new entrants, shut down + flatten symbols that fell out.
//!
//! All features from `wave_auto` are preserved: orphan-position adoption,
//! `defer_underwater`, retired-tab GC (`RETIRE_AFTER_CYCLES`), `stop_bot`
//! abort+reap, `flatten_symbols`, and graceful global-shutdown that leaves
//! positions intact.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use futures::StreamExt;
use rust_decimal::Decimal;
use tikr_binance::{BinanceEnv, BinanceKeyMaterial};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::config::{BotConfig, RampageConfig, RampageStrategy, ScoreMode, TideParams, WaveParams};
use crate::state::{BotStatus, BotView, SharedBotState};
use crate::supervisor::{SupervisorCtx, reset_symbol_state, spawn_supervisor};
use crate::venue;

/// Recheck cycles a rotated-out (off) bot lingers in the dashboard before its
/// tab is removed. If it rotates back in before this, the counter resets.
const RETIRE_AFTER_CYCLES: u32 = 5;

/// Account/env context shared by all spawned supervisors under the rampage
/// manager.
pub struct RampageAccountCtx {
    pub env: BinanceEnv,
    pub api_key: String,
    pub key_material: Arc<BinanceKeyMaterial>,
    pub base_state_dir: std::path::PathBuf,
    pub order_balance_pct: Decimal,
    pub leverage: u32,
    pub max_position_pct: Decimal,
    pub inventory_boost: Option<tikr_paper::InventoryBoostConfig>,
    pub notional_rx: watch::Receiver<Decimal>,
    pub max_position_rx: watch::Receiver<Decimal>,
    pub bnb_price_rx: watch::Receiver<Decimal>,
}

struct ActiveBot {
    shutdown_tx: watch::Sender<bool>,
    handle: JoinHandle<()>,
}

/// Spawn the rampage auto-rotation manager. Returns immediately; runs in the
/// background until global shutdown fires.
pub fn spawn_rampage_manager(
    cfg: RampageConfig,
    account: RampageAccountCtx,
    shared_state: SharedBotState,
    mut global_shutdown: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let http = reqwest::Client::new();
        let mut active: HashMap<String, ActiveBot> = HashMap::new();
        // Off-bots (rotated out) → consecutive recheck cycles spent not-active.
        // Dropped from the dashboard once they hit RETIRE_AFTER_CYCLES.
        let mut retired: HashMap<String, u32> = HashMap::new();
        let recheck = cfg.recheck_interval_secs.max(10);
        let mut tick = tokio::time::interval(Duration::from_secs(recheck));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let score_label = match &cfg.score {
            ScoreMode::CandleHeight {
                candle_count,
                min_candle_pct,
            } => format!("candle_height(count={candle_count}, min_pct={min_candle_pct})"),
            ScoreMode::TickBps { min_tick_bps } => {
                format!("tick_bps(min={min_tick_bps})")
            }
        };
        let strategy_label = match &cfg.strategy {
            RampageStrategy::Wave { .. } => "wave",
            RampageStrategy::Tide { .. } => "tide",
        };
        info!(
            min_volume_usdt = %cfg.min_volume_usdt,
            score = %score_label,
            strategy = %strategy_label,
            top_n = cfg.top_n,
            recheck_interval_secs = recheck,
            quote_asset = %cfg.quote_asset,
            "rampage: starting discovery loop"
        );

        loop {
            tokio::select! {
                _ = tick.tick() => {}
                _ = global_shutdown.changed() => {
                    if *global_shutdown.borrow() {
                        // Stop the bots but leave their resting orders AND open
                        // positions UNTOUCHED on shutdown — no cancel, no
                        // flatten. Positions persist across restarts; the next
                        // start cancels the orphan orders (clear_on_start=false)
                        // and resumes managing the inherited position.
                        info!("rampage: global shutdown — stopping bots (orders + positions left intact)");
                        for (_, bot) in active.drain() {
                            stop_bot(bot).await;
                        }
                        return;
                    }
                }
            }

            // 1. Universe: every TRADING perp + its price (for the underwater
            // mark check) and 24h volume (liquidity pre-filter).
            let discovered = match tikr_binance::futs::list_perp_tick_info(
                &http,
                account.env.rest_base_url(),
                &cfg.quote_asset,
            )
            .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    warn!(error = ?e, "rampage: discovery failed, retrying next cycle");
                    continue;
                }
            };

            // Mark map (symbol → last price) for the underwater check below —
            // built across ALL perps, so a dropped symbol's mark is available.
            let price_map: HashMap<String, Decimal> = discovered
                .iter()
                .map(|r| (r.symbol.clone(), r.price))
                .collect();

            // 2. Pre-filter to liquid symbols (volume floor + optional
            // allowlist) — this bounds how many klines we fetch.
            let allowlist: HashSet<&str> =
                cfg.symbols_allowlist.iter().map(|s| s.as_str()).collect();
            let candidates: Vec<String> = discovered
                .iter()
                .filter(|r| {
                    (allowlist.is_empty() || allowlist.contains(r.symbol.as_str()))
                        && r.quote_volume_24h >= cfg.min_volume_usdt
                })
                .map(|r| r.symbol.clone())
                .collect();

            // 3. Score candidates according to the configured ScoreMode.
            let mut scored: Vec<(String, Decimal)> = match &cfg.score {
                ScoreMode::CandleHeight {
                    candle_count,
                    min_candle_pct,
                } => {
                    // Fetch avg 1m candle height concurrently (cap 16).
                    let base_url = account.env.rest_base_url().to_string();
                    let n_candles = *candle_count;
                    let min_pct = *min_candle_pct;
                    let http_ref = &http;
                    let raw: Vec<(String, Decimal)> = futures::stream::iter(candidates)
                        .map(|sym| {
                            let base = base_url.clone();
                            async move {
                                let score = tikr_binance::futs::get_1m_avg_candle_pct(
                                    http_ref, &base, &sym, n_candles,
                                )
                                .await
                                .unwrap_or_default();
                                (sym, score)
                            }
                        })
                        .buffer_unordered(16)
                        .collect()
                        .await;
                    raw.into_iter().filter(|(_, s)| *s >= min_pct).collect()
                }
                ScoreMode::TickBps { min_tick_bps } => {
                    // Score from the already-fetched tick_bps — no extra HTTP.
                    let tick_map: HashMap<String, Decimal> = discovered
                        .iter()
                        .map(|r| (r.symbol.clone(), r.tick_bps))
                        .collect();
                    let min = *min_tick_bps;
                    candidates
                        .into_iter()
                        .filter_map(|sym| {
                            let bps = tick_map.get(&sym).copied().unwrap_or_default();
                            if bps >= min { Some((sym, bps)) } else { None }
                        })
                        .collect()
                }
            };

            // 4. Rank desc, take top_n.
            scored.sort_by_key(|(_, sc)| std::cmp::Reverse(*sc));
            scored.truncate(cfg.top_n);
            let qualifying: HashSet<String> = scored.iter().map(|(s, _)| s.clone()).collect();
            info!(
                candidates = qualifying.len(),
                running = active.len(),
                top = ?scored.iter().map(|(s, sc)| format!("{s}:{sc:.1}")).collect::<Vec<_>>(),
                "rampage: discovery tick"
            );

            // 4-pre. Adopt orphan positions. Any symbol with an open position
            // on the venue but NO active bot gets one spawned to MANAGE/DRAIN
            // it — covers positions inherited across a restart (rampage leaves
            // positions intact on shutdown) whose symbol has since fallen off
            // the top set. Without this they'd sit with no orders, unmanaged.
            // Once adopted they obey the normal rules below: deferred while
            // underwater, rotated out (flattened) once flat/green. Runs every
            // cycle (cheap: one positionRisk call) and is a no-op once adopted
            // (the symbol is then in `active`). Dust / untradeable (no mark)
            // skipped so we don't churn a bot we can't meaningfully run.
            match tikr_binance::futs::list_open_positions(
                &http,
                account.env.rest_base_url(),
                &account.api_key,
                &account.key_material,
            )
            .await
            {
                Ok(positions) => {
                    for (sym, amt) in positions {
                        if active.contains_key(&sym) {
                            continue;
                        }
                        let mark = price_map.get(&sym).copied().unwrap_or_default();
                        if mark <= Decimal::ZERO {
                            continue; // untradeable / unknown price — can't manage
                        }
                        // ~min-notional floor (USDT); below this it's dust that
                        // reset_symbol_state can't close anyway.
                        if amt.abs() * mark < Decimal::from(5) {
                            continue;
                        }
                        warn!(
                            symbol = %sym,
                            amount = %amt,
                            notional = %(amt.abs() * mark),
                            "rampage: adopting orphan position (no active bot) — spawning manager"
                        );
                        let bot = spawn_one_bot(&sym, &account, &shared_state, &cfg);
                        active.insert(sym.clone(), bot);
                        retired.remove(&sym); // it's live again — stop the GC clock
                    }
                }
                Err(e) => {
                    warn!(error = ?e, "rampage: orphan-position scan failed (skipping this cycle)")
                }
            }

            // 4a. Teardown FIRST so freed slots can be filled this cycle.
            // A symbol that fell out of the top set rotates ONLY when its bot's
            // NET PnL (realized + unrealized − fees) is green, OR its NET loss is
            // within the acceptable `rotate_loss_pct` of total wallet. A NET loss
            // bigger than that defers rotation (defer_underwater on) — the bot
            // keeps running and works the bag off, so rotation never crystallizes
            // more than the tolerated loss on these mean-reverting markets.
            let dropped: Vec<String> = active
                .keys()
                .filter(|s| !qualifying.contains(s.as_str()))
                .cloned()
                .collect();
            for symbol in dropped {
                if let Some(reason) = should_defer_rotation(&symbol, &cfg, &shared_state) {
                    info!(
                        symbol = %symbol,
                        net = ?shared_state.net_for(&symbol),
                        bag = ?shared_state.bag_for(&symbol),
                        reason,
                        "rampage: out of top set — deferring rotation"
                    );
                    continue; // keep the bot; it holds its slot until recovered
                }
                if let Some(bot) = active.remove(&symbol) {
                    warn!(
                        symbol = %symbol,
                        net = ?shared_state.net_for(&symbol),
                        "rampage: rotating out (NET green or within rotate_loss_pct) — shutting down + flattening"
                    );
                    // Ensure the bot is fully STOPPED before flattening — a
                    // still-running bot would re-quote and re-open the position
                    // we're about to close, orphaning it.
                    stop_bot(bot).await;
                    flatten_symbols(std::slice::from_ref(&symbol), &account).await;
                    shared_state.set_status(&symbol, BotStatus::Rotated);
                    // Start the retirement clock — its [off] tab lingers up to
                    // RETIRE_AFTER_CYCLES rechecks before we drop it.
                    retired.insert(symbol.clone(), 0);
                }
            }

            // 4b. Spawn new entrants into free slots, highest score first,
            // capped so total active (incl. deferred drainers) ≤ top_n. A
            // deferred underwater bot holds its slot, so a stuck bag delays a
            // new entrant — it never forces a realized loss.
            for (symbol, _score) in &scored {
                if active.len() >= cfg.top_n {
                    break;
                }
                if active.contains_key(symbol) {
                    continue;
                }
                let bot = spawn_one_bot(symbol, &account, &shared_state, &cfg);
                info!(symbol = %symbol, "rampage: spawned new bot");
                active.insert(symbol.clone(), bot);
            }

            // 5. GC stale off-bots from the TUI. Only symbols WE rotated out
            // are tracked in `retired` (never foreign views). A rotated symbol
            // lingers up to RETIRE_AFTER_CYCLES rechecks (so a quick round-trip
            // back keeps its tab); if it comes back into `active`, stop
            // tracking; once it ages out, drop the tab.
            retired.retain(|sym, cycles| {
                if active.contains_key(sym) {
                    return false; // rotated back in — reset
                }
                *cycles += 1;
                if *cycles >= RETIRE_AFTER_CYCLES {
                    shared_state.remove(sym);
                    info!(
                        symbol = %sym,
                        cycles = RETIRE_AFTER_CYCLES,
                        "rampage: dropped stale off-bot from the dashboard"
                    );
                    return false;
                }
                true
            });
        }
    })
}

/// Decide whether `symbol`'s bot should DEFER rotation rather than rotate out
/// now, returning `Some(reason)` to defer or `None` to rotate. Two holds:
///
/// 1. **Underwater hold:** a bot rotates only when its NET PnL (`realized +
///    unrealized − fees`) is green or its NET loss is within `rotate_loss_pct`
///    of total wallet; a larger NET loss defers, so rotation never crystallizes
///    more than the tolerated loss.
/// 2. **Big-bag hold:** even a bot that would otherwise rotate is held when it
///    sits on a POSITIVE unrealized PnL and its gross position notional is ≥
///    `big_bag_pct` of total wallet — let it work a large profitable bag down
///    instead of market-closing it. Gated on unrealized only (NET-independent).
///
/// Conservative: when `defer_underwater` is off, never defer. When the bot has
/// no NET snapshot yet, defer — we can't confirm it's safe to realize.
fn should_defer_rotation(
    symbol_str: &str,
    cfg: &RampageConfig,
    shared_state: &SharedBotState,
) -> Option<&'static str> {
    if !cfg.defer_underwater {
        return None; // deferral disabled → always rotate
    }

    // Big-bag hold: large position currently in profit → keep working it down
    // rather than dump it. Checked first so it applies even to NET-green bots.
    if cfg.big_bag_pct > Decimal::ZERO
        && let Some((unrealized, gross_notional)) = shared_state.bag_for(symbol_str)
        && unrealized > Decimal::ZERO
    {
        let big = total_wallet_balance(shared_state) * cfg.big_bag_pct / Decimal::from(100);
        if big > Decimal::ZERO && gross_notional >= big {
            return Some("big bag in profit — working it down");
        }
    }

    let net = match shared_state.net_for(symbol_str) {
        Some(n) => n,
        None => return Some("no NET snapshot yet"), // can't confirm → defer
    };
    if net >= Decimal::ZERO {
        return None; // green NET → safe to rotate
    }
    // Underwater: tolerate a NET loss up to rotate_loss_pct % of total wallet.
    let total_wallet = total_wallet_balance(shared_state);
    let tolerance = total_wallet * cfg.rotate_loss_pct / Decimal::from(100);
    if -net > tolerance {
        Some("NET loss exceeds rotate_loss_pct")
    } else {
        None // within tolerance → rotate
    }
}

/// Total wallet balance for the `rotate_loss_pct` tolerance: futures wallet
/// balance + BNB value (when BNB-fee mode is on), mirroring the account poller's
/// sizing base. `0` if no account snapshot has landed yet (→ zero tolerance →
/// any underwater bot defers, the safe default).
fn total_wallet_balance(shared_state: &SharedBotState) -> Decimal {
    let wallet = shared_state
        .api_account()
        .map(|a| a.wallet_balance)
        .unwrap_or_default();
    let bnb = shared_state.bnb_snapshot();
    let bnb_value = if bnb.enabled {
        bnb.balance * bnb.price_usdt
    } else {
        Decimal::ZERO
    };
    wallet + bnb_value
}

/// Signal a bot to shut down and GUARANTEE it has stopped before the caller
/// touches its symbol (cancel-all / flatten). A bare `timeout(.., handle)` that
/// elapses merely drops the `JoinHandle`, which DETACHES the task — it keeps
/// running and can re-quote / re-open the very position we're about to flatten,
/// orphaning it. So on timeout we abort and reap, ensuring the bot is dead first.
async fn stop_bot(bot: ActiveBot) {
    let _ = bot.shutdown_tx.send(true);
    let mut handle = bot.handle;
    if tokio::time::timeout(Duration::from_secs(5), &mut handle)
        .await
        .is_err()
    {
        warn!("rampage: bot did not stop in 5s — aborting before cancel/flatten");
        handle.abort();
        let _ = handle.await;
    }
}

async fn flatten_symbols(symbols: &[String], account: &RampageAccountCtx) {
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
                info!(symbol = symbol_str, "rampage: cancel + flatten");
                reset_symbol_state(&v, &symbol).await;
            }
            Err(e) => warn!(
                symbol = symbol_str,
                error = ?e,
                "rampage: venue build for flatten failed"
            ),
        }
    }
}

fn spawn_one_bot(
    symbol: &str,
    account: &RampageAccountCtx,
    shared_state: &SharedBotState,
    cfg: &RampageConfig,
) -> ActiveBot {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (bot_cfg, strategy_name, label) = match &cfg.strategy {
        RampageStrategy::Wave {
            levels,
            steps_bps,
            steps_inner,
            round_trips,
        } => {
            let bc = BotConfig {
                symbol: symbol.to_string(),
                strategy: "wave".to_string(),
                wave: Some(WaveParams {
                    notional: None,
                    levels: *levels,
                    steps_bps: *steps_bps,
                    steps_inner: *steps_inner,
                    round_trips: *round_trips,
                }),
                tide: None,
                sg: None,
                lg: None,
                ladder_reentry: None,
                simple_gap: None,
                micro_mean_reversion: None,
                spread_scalp: None,
                liq_fade: None,
                hydra: None,
                joker: None,
                rsi_mr: None,
                mantis: None,
                volley: None,
            };
            (bc, "wave", format!("{symbol}/wave"))
        }
        RampageStrategy::Tide {
            grid_levels,
            step_bps,
            inner_steps,
            chase,
            chase_to_avg,
            prune_stragglers,
        } => {
            let bc = BotConfig {
                symbol: symbol.to_string(),
                strategy: "tide".to_string(),
                tide: Some(TideParams {
                    notional: None,
                    grid_levels: *grid_levels,
                    step_bps: *step_bps,
                    prune_stragglers: *prune_stragglers,
                    recenter_bps: 0,
                    recenter_secs: 0,
                    inner_steps: *inner_steps,
                    chase: *chase,
                    chase_to_avg: *chase_to_avg,
                    relattice_timeout_secs: 300,
                }),
                wave: None,
                sg: None,
                lg: None,
                ladder_reentry: None,
                simple_gap: None,
                micro_mean_reversion: None,
                spread_scalp: None,
                liq_fade: None,
                hydra: None,
                joker: None,
                rsi_mr: None,
                mantis: None,
                volley: None,
            };
            (bc, "tide", format!("{symbol}/tide"))
        }
    };
    shared_state.insert(
        symbol,
        BotView {
            label,
            symbol: symbol.to_string(),
            strategy: strategy_name.to_string(),
            status: BotStatus::Starting,
            snapshot: Arc::new(RwLock::new(None)),
            live: Arc::new(RwLock::new(None)),
            shutdown_tx: None,
            api_position: Arc::new(RwLock::new(None)),
        },
    );
    let handle = spawn_supervisor(
        SupervisorCtx {
            cfg: bot_cfg,
            env: account.env,
            api_key: account.api_key.clone(),
            key_material: account.key_material.clone(),
            base_state_dir: account.base_state_dir.clone(),
            order_balance_pct: account.order_balance_pct,
            leverage: account.leverage,
            max_position_pct: account.max_position_pct,
            inventory_boost: account.inventory_boost,
            notional_rx: account.notional_rx.clone(),
            max_position_rx: account.max_position_rx.clone(),
            bnb_price_rx: account.bnb_price_rx.clone(),
            // Restart cancels orphan orders but PRESERVES the open position
            // (clear_on_start=true would flatten). A symbol that re-enters the
            // top set after a restart resumes managing its inherited bag.
            clear_on_start: false,
        },
        shared_state.clone(),
        shutdown_rx,
    );
    ActiveBot {
        shutdown_tx,
        handle,
    }
}
