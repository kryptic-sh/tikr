//! Auto-rotation manager for Wave — hunts the "GUN regime": volatile +
//! wide-spread + mean-reverting + liquid markets where the frozen lattice
//! oscillates and banks round-trips with near-zero inventory.
//!
//! Every `recheck_interval_secs` (default 60s):
//! 1. Query Binance Futures exchangeInfo + ticker/price + ticker/24hr +
//!    all-symbols bookTicker (one call each) via `list_perp_wave_info`.
//! 2. Keep symbols passing the floors: `24h quote volume ≥ min_volume_usdt`,
//!    `24h range% ≥ min_range_pct`, live `spread_bps ≥ min_spread_bps`, and
//!    `chop_ratio = range% / |24h net change%| ≥ min_chop_ratio`
//!    (oscillation, not a one-way trend → low bag risk).
//! 3. Score survivors by `range% × chop_ratio × spread_bps`, take the top
//!    `top_n`.
//! 4. Diff against the running set: spawn Wave on new entrants, shut down +
//!    flatten symbols that fell out of the top set.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use futures::StreamExt;
use rust_decimal::Decimal;
use tikr_binance::{BinanceEnv, BinanceKeyMaterial};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::config::{BotConfig, WaveAutoConfig, WaveParams};
use crate::state::{BotStatus, BotView, SharedBotState};
use crate::supervisor::{SupervisorCtx, reset_symbol_state, spawn_supervisor};
use crate::venue;

/// Recheck cycles a rotated-out (off) bot lingers in the dashboard before its
/// tab is removed. If it rotates back in before this, the counter resets.
const RETIRE_AFTER_CYCLES: u32 = 5;

/// Account/env context shared by all spawned Wave supervisors.
pub struct WaveAutoAccountCtx {
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

/// Spawn the Wave auto-rotation manager. Returns immediately; runs in the
/// background until global shutdown fires.
pub fn spawn_wave_auto_manager(
    cfg: WaveAutoConfig,
    account: WaveAutoAccountCtx,
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

        info!(
            min_volume_usdt = %cfg.min_volume_usdt,
            candle_count = cfg.candle_count,
            min_candle_pct = %cfg.min_candle_pct,
            top_n = cfg.top_n,
            recheck_interval_secs = recheck,
            quote_asset = %cfg.quote_asset,
            "wave_auto: starting discovery loop (score = avg 1m candle height %)"
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
                        info!("wave_auto: global shutdown — stopping bots (orders + positions left intact)");
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
                    warn!(error = ?e, "wave_auto: discovery failed, retrying next cycle");
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

            // 3. Score each candidate by RECENT PRICE ACTION: the avg height of
            // its last `candle_count` 1m candles, as a percent (wicks included).
            // Fetched concurrently (cap 16) to keep the recheck fast.
            let base_url = account.env.rest_base_url().to_string();
            let n_candles = cfg.candle_count;
            let http_ref = &http;
            let raw_scores: Vec<(String, Decimal)> = futures::stream::iter(candidates)
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

            // 4. Floor on the candle score, rank desc, take top_n.
            let mut scored: Vec<(String, Decimal)> = raw_scores
                .into_iter()
                .filter(|(_, s)| *s >= cfg.min_candle_pct)
                .collect();
            scored.sort_by_key(|(_, sc)| std::cmp::Reverse(*sc));
            scored.truncate(cfg.top_n);
            let qualifying: HashSet<String> = scored.iter().map(|(s, _)| s.clone()).collect();
            info!(
                candidates = qualifying.len(),
                running = active.len(),
                top = ?scored.iter().map(|(s, sc)| format!("{s}:{sc:.1}")).collect::<Vec<_>>(),
                "wave_auto: discovery tick"
            );

            // 4-pre. Adopt orphan positions. Any symbol with an open position
            // on the venue but NO active bot gets one spawned to MANAGE/DRAIN
            // it — covers positions inherited across a restart (wave_auto leaves
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
                            "wave_auto: adopting orphan position (no active bot) — spawning manager"
                        );
                        let bot = spawn_one_bot(&sym, &account, &shared_state, &cfg);
                        active.insert(sym.clone(), bot);
                        retired.remove(&sym); // it's live again — stop the GC clock
                    }
                }
                Err(e) => {
                    warn!(error = ?e, "wave_auto: orphan-position scan failed (skipping this cycle)")
                }
            }

            // 4a. Teardown FIRST so freed slots can be filled this cycle.
            // A symbol that fell out of the top set rotates ONLY when its bot is
            // flat or holding a GREEN bag. If it's holding an UNDERWATER bag
            // (and defer_underwater is on), keep the bot running — its grid +
            // chase_to_avg work the bag off; it rotates once recovered. This
            // stops rotation from crystallizing a loss on a bag that, on these
            // mean-reverting markets, usually comes back.
            let dropped: Vec<String> = active
                .keys()
                .filter(|s| !qualifying.contains(s.as_str()))
                .cloned()
                .collect();
            for symbol in dropped {
                let mark = price_map.get(&symbol).copied().unwrap_or_default();
                if cfg.defer_underwater && holds_underwater_bag(&symbol, mark, &account).await {
                    info!(
                        symbol = %symbol,
                        "wave_auto: out of top set but holding an underwater bag — deferring rotation"
                    );
                    continue; // keep the bot; it holds its slot until recovered
                }
                if let Some(bot) = active.remove(&symbol) {
                    warn!(
                        symbol = %symbol,
                        "wave_auto: rotating out (flat/green) — shutting down + flattening"
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
                info!(symbol = %symbol, "wave_auto: spawned new bot");
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
                        "wave_auto: dropped stale off-bot from the dashboard"
                    );
                    return false;
                }
                true
            });
        }
    })
}

/// `true` if `symbol`'s live position is a significant (≥ minNotional) bag
/// whose mark is underwater vs `avg_entry` — i.e. rotating it would realize a
/// loss. Flat / green / dust-below-minNotional → `false` (safe to rotate). On
/// ANY read error, returns `true` (defer — never crystallize a loss we can't
/// confirm is safe; the bot keeps managing itself meanwhile).
async fn holds_underwater_bag(
    symbol_str: &str,
    mark: Decimal,
    account: &WaveAutoAccountCtx,
) -> bool {
    use tikr_venue::Venue;
    let symbol = venue::perp_symbol(symbol_str);
    let v = match venue::build_venue(
        account.env,
        &account.api_key,
        &account.key_material,
        &symbol,
        account.leverage,
    )
    .await
    {
        Ok(v) => v,
        Err(e) => {
            warn!(symbol = symbol_str, error = ?e, "wave_auto: venue build for bag check failed — deferring");
            return true;
        }
    };
    let pos = match v.position(&symbol).await {
        Ok(p) => p,
        Err(e) => {
            warn!(symbol = symbol_str, error = ?e, "wave_auto: position read for bag check failed — deferring");
            return true;
        }
    };
    let size = pos.size.0;
    if size == Decimal::ZERO {
        return false; // flat → safe to rotate
    }
    let avg = pos.avg_entry.0;
    if avg <= Decimal::ZERO || mark <= Decimal::ZERO {
        return true; // unknown cost/mark → defer
    }
    // Dust below minNotional: reset_symbol_state can't close it anyway, so let
    // it rotate (the slot frees; dust is left for the next bot to trade on top).
    if let Some(min_n) = v.min_notional(&symbol)
        && size.abs() * avg < min_n
    {
        return false;
    }
    // Underwater? long: mark < avg; short: mark > avg.
    if size > Decimal::ZERO {
        mark < avg
    } else {
        mark > avg
    }
}

/// Signal a bot to shut down and GUARANTEE it has stopped before the caller
/// touches its symbol (cancel-all / flatten). A bare `timeout(.., handle)` that
/// elapses merely drops the `JoinHandle`, which DETACHES the task — it keeps
/// running and can re-quote / re-open the very position we're about to flatten,
/// orphaning it on the venue. So on timeout we abort and reap, ensuring the bot
/// is dead first.
async fn stop_bot(bot: ActiveBot) {
    let _ = bot.shutdown_tx.send(true);
    let mut handle = bot.handle;
    if tokio::time::timeout(Duration::from_secs(5), &mut handle)
        .await
        .is_err()
    {
        warn!("wave_auto: bot did not stop in 5s — aborting before cancel/flatten");
        handle.abort();
        let _ = handle.await;
    }
}

async fn flatten_symbols(symbols: &[String], account: &WaveAutoAccountCtx) {
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
                info!(symbol = symbol_str, "wave_auto: cancel + flatten");
                reset_symbol_state(&v, &symbol).await;
            }
            Err(e) => warn!(
                symbol = symbol_str,
                error = ?e,
                "wave_auto: venue build for flatten failed"
            ),
        }
    }
}

fn spawn_one_bot(
    symbol: &str,
    account: &WaveAutoAccountCtx,
    shared_state: &SharedBotState,
    cfg: &WaveAutoConfig,
) -> ActiveBot {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let bot_cfg = BotConfig {
        symbol: symbol.to_string(),
        strategy: "wave".to_string(),
        // Notional + cap come from the account-wide pollers; geometry from
        // the wave_auto config.
        wave: Some(WaveParams {
            notional: None,
            grid_levels: cfg.grid_levels,
            step_bps: cfg.step_bps,
            inner_steps: cfg.inner_steps,
            refill_threshold: cfg.refill_threshold,
            inventory_skew_slots: 0,
            chase_to_avg: cfg.chase_to_avg,
            chase: cfg.chase,
            tp_bps: 0,
            tp_close_pct: 100,
            sl_bps: 0,
            sl_close_pct: 100,
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
    };
    shared_state.insert(
        symbol,
        BotView {
            label: format!("{symbol}/wave"),
            symbol: symbol.to_string(),
            strategy: "wave".to_string(),
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
