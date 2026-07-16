//! Auto-restart supervisor for bots. One supervisor task per bot.
//!
//! Wraps `spawn_bot` in a loop: when the underlying JoinHandle resolves
//! (either clean shutdown or crash), the supervisor decides whether to
//! restart with exponential backoff or stay down (if global shutdown
//! requested).

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use rust_decimal::Decimal;
use tikr_binance::BinanceClient;
use tikr_paper::{BotHandle, InventoryBoostConfig};
use tikr_venue::Venue;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{Instrument, info, warn};

use crate::build::to_spec;
use crate::config::BotConfig;
use crate::state::{BotStatus, SharedBotState};
use crate::venue;

/// Inputs needed to (re)build a bot incarnation.
pub struct SupervisorCtx {
    /// User-supplied bot config (parsed from TOML).
    pub cfg: BotConfig,
    /// Account environment.
    pub env: tikr_binance::BinanceEnv,
    /// API key string (shared across bots).
    pub api_key: String,
    /// Key material (Arc shared across bots).
    pub key_material: Arc<tikr_binance::BinanceKeyMaterial>,
    /// Base on-disk state directory; per-bot subdirs hang off this.
    pub base_state_dir: std::path::PathBuf,
    /// Wallet percent allocated to all bot order sizes.
    pub order_balance_pct: Decimal,
    /// Per-symbol Binance leverage, sent at venue build time.
    pub leverage: u32,
    /// Wallet percent for the per-bot peak-position cap (per-bot, NOT split).
    /// Drives `max_position_usdt` defaults handed to strategies that don't set
    /// their own cap in TOML.
    pub max_position_pct: Decimal,
    /// Optional account-level inventory-aware order-size boost, forwarded to
    /// every bot's `RunnerConfig`. `None` = disabled.
    pub inventory_boost: Option<InventoryBoostConfig>,
    /// Live per-bot notional updates from the account balance poller.
    pub notional_rx: watch::Receiver<Decimal>,
    /// Live per-bot position-cap updates from the account balance poller.
    /// Recomputed every 5s alongside `notional_rx` so the strategy's
    /// `max_position_usdt` tracks compounded wallet growth, not just
    /// per-order size.
    pub max_position_rx: watch::Receiver<Decimal>,
    /// Live account wallet balance from the poller — drives the take-profit
    /// threshold (`take_profit_pct` of this).
    pub wallet_rx: watch::Receiver<Decimal>,
    /// Take-profit threshold as a percent of wallet (`0` = disabled). When a
    /// bot's unrealized P&L exceeds this, the runner rests a reduce-only maker
    /// limit to lock in half the bag.
    pub take_profit_pct: Decimal,
    /// Account-level bagger (inventory-risk flatten), built from the
    /// `[account.bagger]` TOML table. Off when no mechanism is enabled.
    pub bagger: tikr_paper::bagger::BaggerConfig,
    /// Live BNBUSDT mid; user-stream parser uses this to convert BNB
    /// commissions → USDT-equivalent fee_quote. When BNB-fee mode is
    /// off (or autodetect fails), this stays at ZERO and the parser
    /// keeps the raw commission. Always provided so the supervisor
    /// doesn't need conditional plumbing.
    pub bnb_price_rx: watch::Receiver<Decimal>,
    /// When `true`, the supervisor cancels all resting orders + flattens
    /// any open position for this symbol on each spawn cycle, giving
    /// the bot a clean slate. When `false` (default for `tikr` CLI),
    /// the bot resumes against whatever live state exists — handy for
    /// fast code-change / restart cycles without churning positions.
    /// Rotation always sets this `true` because each rotation cycle
    /// hands the slot to a different symbol with no shared history.
    pub clear_on_start: bool,
}

/// Spawn the supervisor for a single bot. Returns the supervisor's join
/// handle (resolves when global shutdown fires).
pub fn spawn_supervisor(
    ctx: SupervisorCtx,
    shared_state: SharedBotState,
    mut global_shutdown: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let symbol_str = ctx.cfg.symbol.clone();
        let span = tracing::info_span!("bot", symbol = %symbol_str);
        async move {
            let mut attempt: u32 = 0;
            loop {
                if *global_shutdown.borrow() {
                    info!("global shutdown observed before respawn — exiting supervisor");
                    return;
                }

                // Account-wide rate-limit gate: if ANY component (another bot's
                // spawn, discovery, BNB refill, account poll) hit a venue
                // RateLimited, hold off here too — Binance limits are
                // per-account, so spawning into an active ban just extends it.
                let gate_ms = shared_state.rate_limit_remaining_ms();
                if gate_ms > 0 {
                    warn!(
                        wait_ms = gate_ms,
                        "account rate-limited — holding spawn until the gate clears"
                    );
                    shared_state.set_status(
                        &symbol_str,
                        BotStatus::Restarting(format!("rate-limited {}s", gate_ms / 1000)),
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(gate_ms)) => {}
                        _ = global_shutdown.changed() => {
                            if *global_shutdown.borrow() { return; }
                        }
                    }
                    continue;
                }

                shared_state.set_status(&symbol_str, BotStatus::Starting);

                // Honored rate-limit respawn floor (ms): set when a spawn fails
                // with a venue RateLimited so we wait out `retry_after` instead
                // of the (shorter) exponential backoff, which would just re-hit
                // the limit.
                let mut rate_limit_delay_ms: Option<u64> = None;
                let handle_result = run_once(&ctx).await;
                match handle_result {
                    Ok(spawned) => {
                        // Backoff is reset only after the bot proves it's
                        // actually healthy (see below) — NOT just because it
                        // spawned. A bot that spawns and crashes within
                        // MIN_HEALTHY_UPTIME must keep growing the backoff,
                        // or a crash-loop hammers the venue at a constant
                        // ~1s cadence forever.
                        let spawn_time = std::time::Instant::now();
                        if let Some(d) = spawned.price_decimals {
                            shared_state.set_price_decimals(&symbol_str, d);
                        }
                        shared_state.attach_handle(&symbol_str, &spawned.handle);
                        let shutdown_tx = spawned.handle.shutdown_tx.clone();
                        let us_shutdown_tx = spawned.us_shutdown_tx;
                        let mut join = spawned.handle.join;
                        // Wait for either the bot to finish OR global shutdown.
                        loop {
                            tokio::select! {
                                res = &mut join => {
                                    match res {
                                        Ok(report) => {
                                            info!(
                                                realized = %report.realized.0,
                                                unrealized = %report.unrealized.0,
                                                fees = %report.fees.0,
                                                funding = %report.funding.0,
                                                net = %report.net.0,
                                                fills = report.fills_emitted,
                                                "bot ended cleanly"
                                            );
                                            shared_state.set_final(&symbol_str, report.clone());
                                            shared_state.set_status(&symbol_str, BotStatus::Crashed("clean exit".into()));
                                        }
                                        Err(e) => {
                                            warn!("bot task join error: {e}");
                                            shared_state.set_status(&symbol_str, BotStatus::Crashed(format!("join error: {e}")));
                                        }
                                    }
                                    // Bot is done — tell its dedicated user-stream
                                    // tasks (keepalive + WS pump) to wind down so
                                    // they don't outlive this incarnation.
                                    let _ = us_shutdown_tx.send(true);
                                    break;
                                }
                                _ = global_shutdown.changed() => {
                                    if *global_shutdown.borrow() {
                                        info!("global shutdown — stopping bot");
                                        let _ = shutdown_tx.send(true);
                                        // A bare timeout that elapses merely drops
                                        // the JoinHandle, which DETACHES the task —
                                        // it keeps running and can re-quote /
                                        // re-open a position after rampage flattens
                                        // it. Abort + reap on timeout so the task
                                        // cannot outlive the supervisor.
                                        if tokio::time::timeout(Duration::from_secs(5), &mut join)
                                            .await
                                            .is_err()
                                        {
                                            warn!(
                                                "bot did not stop within 5s — aborting task to prevent a zombie re-quoting after shutdown"
                                            );
                                            join.abort();
                                            let _ = join.await;
                                        }
                                        let _ = us_shutdown_tx.send(true);
                                        return;
                                    }
                                }
                            }
                        }
                        // Only forgive prior crash-loop backoff once the bot has
                        // proven itself healthy for a minimum uptime — otherwise
                        // a spawn-then-immediately-crash cycle would reset to a
                        // ~1s backoff every time and hammer the venue REST API.
                        // Below the threshold, `attempt` is left untouched so the
                        // saturating_add below keeps growing it.
                        const MIN_HEALTHY_UPTIME: Duration = Duration::from_secs(60);
                        if spawn_time.elapsed() >= MIN_HEALTHY_UPTIME {
                            attempt = 0;
                        }
                    }
                    Err(e) => {
                        warn!("bot spawn failed: {e}");
                        // Find a venue RateLimited anywhere in the error chain.
                        rate_limit_delay_ms = e.chain().find_map(|c| {
                            match c.downcast_ref::<tikr_venue::VenueError>() {
                                Some(tikr_venue::VenueError::RateLimited { retry_after_ms }) => {
                                    Some(*retry_after_ms)
                                }
                                _ => None,
                            }
                        });
                        // Publish to the account-wide gate so every OTHER bot's
                        // supervisor (and the pollers) also waits this out.
                        if let Some(ms) = rate_limit_delay_ms {
                            shared_state.note_rate_limit(ms);
                        }
                        shared_state.set_status(&symbol_str, BotStatus::Crashed(format!("spawn: {e}")));
                    }
                }

                // Exponential backoff: 2^attempt seconds, capped at 60. If the
                // spawn was rate limited, honor `retry_after` as a floor (capped
                // at 10min) so we don't respawn into the same active limit.
                let backoff = (1u64 << attempt.min(6)).min(60);
                let delay = match rate_limit_delay_ms {
                    Some(ms) => backoff.max((ms / 1000).clamp(1, 600)),
                    None => backoff,
                };
                warn!(delay_secs = delay, "respawning bot in {delay}s");
                shared_state.set_status(
                    &symbol_str,
                    BotStatus::Restarting(format!("in {delay}s")),
                );
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(delay)) => {}
                    _ = global_shutdown.changed() => {
                        if *global_shutdown.borrow() {
                            return;
                        }
                    }
                }
                attempt = attempt.saturating_add(1);
            }
        }
        .instrument(span)
        .await
    })
}

/// A bot + the shutdown sender for its dedicated user-stream tasks.
///
/// `us_shutdown_tx` is the per-incarnation watch sender that signals the
/// keepalive + WS pump tasks spawned by `subscribe_user_data_stream` to
/// exit. The supervisor fires it after the bot's join resolves (success
/// OR crash) so those tasks don't outlive the bot.
struct SpawnedBot {
    handle: BotHandle,
    us_shutdown_tx: watch::Sender<bool>,
    /// Price decimal places from the venue tick size, for coin-precision
    /// rendering in the TUI. `None` if the venue had no tick for the symbol.
    price_decimals: Option<u32>,
}

/// Decimal places implied by a price tick size (e.g. `0.0001` → 4, `1` → 0).
fn decimals_from_tick(tick: tikr_core::Decimal) -> u32 {
    tick.normalize().scale()
}

/// One spawn cycle: build venue, subscribe fills, spawn bot.
async fn run_once(ctx: &SupervisorCtx) -> Result<SpawnedBot> {
    let symbol = venue::perp_symbol(&ctx.cfg.symbol);

    // Reset uses a throwaway client so the run_with_resume venue can be
    // moved into spawn_bot below. Skipped when clear_on_start is false
    // (default for `tikr` CLI invocations) so a fast code-change /
    // restart cycle doesn't churn the open position.
    if ctx.clear_on_start {
        info!("startup reset (cancel + flatten) — --clear was set");
        let reset_venue = venue::build_venue(
            ctx.env,
            &ctx.api_key,
            &ctx.key_material,
            &symbol,
            ctx.leverage,
        )
        .await?;
        reset_symbol_state(&reset_venue, &symbol).await;
        drop(reset_venue);
    } else {
        // Always cancel pre-existing open orders even when resuming —
        // the strategy boots with an empty resting tracker and would
        // otherwise leave orphans on the venue forever (no Cancel ever
        // issued for orders it doesn't know about). Position is left
        // intact; only resting orders are wiped.
        info!("resuming live state — cancel-all orphan orders, position preserved");
        let cancel_venue = venue::build_venue(
            ctx.env,
            &ctx.api_key,
            &ctx.key_material,
            &symbol,
            ctx.leverage,
        )
        .await?;
        if let Err(e) = cancel_venue.cancel_all(&symbol).await {
            warn!(error = ?e, "startup cancel_all failed (continuing)");
        }
        drop(cancel_venue);
    }

    info!("building venue for runner");
    let venue_for_run = venue::build_venue(
        ctx.env,
        &ctx.api_key,
        &ctx.key_material,
        &symbol,
        ctx.leverage,
    )
    .await?;

    info!("subscribing user data stream");
    let (us_shutdown_tx, us_shutdown_rx) = watch::channel(false);
    let fill_rx = venue::subscribe_fills(
        ctx.env,
        &ctx.api_key,
        ctx.key_material.clone(),
        &symbol,
        us_shutdown_rx,
        Some(ctx.bnb_price_rx.clone()),
    )
    .await?;

    // Seed the bot's order size from the live poller value (which is sized off
    // the all-asset USD wallet) when it has already reported. Only fall back to
    // a direct USDT-only fetch on cold start, before the first poll lands — a
    // USDT-only seed under-sizes first orders in a multi-asset wallet.
    let live_notional = *ctx.notional_rx.borrow();
    let default_notional = if live_notional > Decimal::ZERO {
        live_notional
    } else {
        default_order_notional(&venue_for_run, ctx).await?
    };
    // Derive max_pos from the same wallet source: prefer the live poller value,
    // else mirror default_notional via the pct ratio.
    let live_max_pos = *ctx.max_position_rx.borrow();
    let max_pos_default = if live_max_pos > Decimal::ZERO {
        live_max_pos
    } else if ctx.order_balance_pct > Decimal::ZERO {
        default_notional * ctx.max_position_pct / ctx.order_balance_pct
    } else {
        Decimal::ZERO
    };
    let mut spec = to_spec(
        &ctx.cfg,
        symbol.clone(),
        &venue_for_run,
        &ctx.base_state_dir,
        default_notional,
        Some(ctx.notional_rx.clone()),
        Some(ctx.max_position_rx.clone()),
        max_pos_default,
        ctx.inventory_boost,
        Some(ctx.wallet_rx.clone()),
        ctx.take_profit_pct,
        ctx.bagger,
    )?;

    // Resume path (--clear OFF): seed the strategy's local position
    // tracker from the venue's positionRisk so the bot doesn't think
    // it's flat while the venue still holds inherited inventory. Bug
    // reported 2026-05-24: dashboard showed local position +0.0000 but
    // unrealized +0.5384 because the local tracker was empty while
    // Binance positionRisk reported a residual position from a prior
    // bot incarnation. Without this seed, strategies would over-quote
    // (they reason against the wrong inventory state) and `max_position
    // _usdt` caps wouldn't engage.
    if !ctx.clear_on_start {
        match venue_for_run.position_risk(&symbol).await {
            Ok(pr) if pr.position_amount != Decimal::ZERO => {
                info!(
                    size = %pr.position_amount,
                    entry = %pr.entry_price,
                    mark = %pr.mark_price,
                    unreal = %pr.unrealized_profit,
                    "seeding tracker from venue positionRisk (resume path)"
                );
                spec.runner_config.seed_position = Some(tikr_core::Position {
                    symbol: spec.symbol.clone(),
                    size: tikr_core::SignedSize(pr.position_amount),
                    avg_entry: tikr_core::Price(pr.entry_price),
                    realized_pnl: tikr_core::Notional(Decimal::ZERO),
                });
            }
            Ok(_) => {
                info!("venue position is flat — no tracker seed needed");
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "position_risk lookup failed — tracker starts flat (may desync if venue holds inventory)"
                );
            }
        }
    }

    info!(strategy = %spec.strategy.label(), default_notional = %default_notional, "spawning bot");
    // LiqFade needs the `@forceOrder` mainnet stream. For other
    // strategies the channel is unused, so we only subscribe when the
    // strategy is liq-fade AND env is futures-mainnet (testnet returns
    // an error from `subscribe_liq_fade`).
    let external_liqs = if spec.strategy.label() == "liq-fade"
        && ctx.env == tikr_binance::BinanceEnv::FuturesMainnet
    {
        let raw_symbol = format!(
            "{}{}",
            spec.symbol.base.0.as_ref(),
            spec.symbol.quote.0.as_ref()
        )
        .to_uppercase();
        match tikr_binance::liquidation_stream::subscribe_liq_fade(ctx.env, raw_symbol.clone())
            .await
        {
            Ok(rx) => {
                info!(symbol = %raw_symbol, "subscribed @forceOrder for LiqFade");
                Some(rx)
            }
            Err(e) => {
                tracing::warn!(error = %e, "subscribe_liq_fade failed — LiqFade will see no liqs");
                None
            }
        }
    } else {
        None
    };
    // Capture the coin's price precision before the venue is moved into the bot.
    let price_decimals = venue_for_run.tick_size(&symbol).map(decimals_from_tick);
    let handle = tikr_paper::spawn_bot(spec, venue_for_run, Some(fill_rx), external_liqs);
    Ok(SpawnedBot {
        handle,
        us_shutdown_tx,
        price_decimals,
    })
}

async fn default_order_notional(venue: &BinanceClient, ctx: &SupervisorCtx) -> Result<Decimal> {
    let balance = venue.futures_balance("USDT").await?;
    // Per-bot, NOT split across bots — 1% means each bot orders 1% of wallet,
    // mirroring max_position_pct. Leverage only sets the margin backstop.
    Ok(balance.wallet_balance * ctx.order_balance_pct / Decimal::from(100))
}

/// Cancel resting orders and close any open position for `symbol`. Returns
/// `true` only when the position is confirmed flat (or was already flat)
/// afterward; `false` if a cancel/close step failed and the position may
/// still be open — callers MUST NOT treat the symbol as safely retired when
/// this returns `false` (see `flatten_symbols` in rampage.rs).
pub(crate) async fn reset_symbol_state(venue: &BinanceClient, symbol: &tikr_core::Symbol) -> bool {
    use rust_decimal::Decimal;
    use tikr_core::{Side, Size};
    use tracing::info;

    let mut ok = true;
    if let Err(e) = venue.cancel_all(symbol).await {
        warn!(error = ?e, "cancel_all failed (continuing)");
        ok = false;
    }
    match venue.position(symbol).await {
        Ok(pos) if pos.size.0 != Decimal::ZERO => {
            let qty = pos.size.0.abs();
            let close_side = if pos.size.0 > Decimal::ZERO {
                Side::Ask
            } else {
                Side::Bid
            };
            let notional_approx = qty * pos.avg_entry.0;
            let avg_entry_known = pos.avg_entry.0 > Decimal::ZERO;
            if let Some(min_n) = venue.min_notional(symbol)
                && avg_entry_known
                && notional_approx < min_n
            {
                // Dust (below minNotional). Do NOT route through the limit
                // fallback — its `quote()` would bump the size back UP to
                // minNotional and OVER-close (flip the position). Close it
                // directly with a reduce-only MARKET order, which is exempt
                // from the minNotional filter. Leaving it (the old behavior)
                // orphaned dust on the account when the bot rotated out.
                info!(
                    qty = %qty,
                    entry = %pos.avg_entry.0,
                    notional = %notional_approx,
                    min_notional = %min_n,
                    "flattening dust position below minNotional via reduce-only market"
                );
                if let Err(e) = venue.market_close(symbol, close_side, Size(qty)).await {
                    warn!(error = ?e, qty = %qty, "dust reduce-only market close FAILED — position left open");
                    return false;
                }
                return ok;
            }
            if !flatten_with_limit_fallback(venue, symbol, close_side, Size(qty)).await {
                ok = false;
            }
        }
        Ok(_) => {}
        Err(e) => {
            warn!(error = ?e, "venue.position failed");
            ok = false;
        }
    }
    ok
}

/// Try to close with a limit order at mid; fall back to market after 10s.
/// Returns `true` only if the position is confirmed closed (or the close
/// order was accepted without a follow-up failure); `false` if any close
/// attempt errored and the position may still be open.
async fn flatten_with_limit_fallback(
    venue: &BinanceClient,
    symbol: &tikr_core::Symbol,
    side: tikr_core::Side,
    qty: tikr_core::Size,
) -> bool {
    use std::time::Duration;
    use tikr_core::{Price, QuoteKind, Size, TimeInForce};
    use tikr_venue::QuoteIntent;
    use tikr_venue::Venue;
    use tracing::info;

    let limit_price = match venue.snapshot(symbol).await {
        Ok(snap) => {
            let bid = snap
                .bids
                .first()
                .map(|l| l.price.0)
                .unwrap_or(Decimal::ZERO);
            let ask = snap
                .asks
                .first()
                .map(|l| l.price.0)
                .unwrap_or(Decimal::ZERO);
            if bid <= Decimal::ZERO || ask <= Decimal::ZERO || ask <= bid {
                info!("invalid book for limit close, using market order");
                return venue.market_close(symbol, side, qty).await.is_ok();
            }
            // Use mid price — aggressive enough to likely fill within 10s
            Price((bid + ask) / Decimal::from(2))
        }
        Err(e) => {
            warn!(error = ?e, "snapshot failed for limit close, using market order");
            return venue.market_close(symbol, side, qty).await.is_ok();
        }
    };

    info!(
        symbol = %tikr_venue::Venue::id(venue),
        side = ?side,
        qty = %qty.0,
        price = %limit_price.0,
        "flattening with limit order at mid"
    );

    let intent = QuoteIntent {
        symbol: symbol.clone(),
        side,
        price: limit_price,
        size: qty,
        tif: TimeInForce::GTC,
        kind: QuoteKind::Point,
    };

    match venue.quote(intent).await {
        Ok(_) => {
            tokio::time::sleep(Duration::from_secs(10)).await;

            match venue.position(symbol).await {
                Ok(new_pos) if new_pos.size.0.abs() > Decimal::ZERO => {
                    let remaining = new_pos.size.0.abs();
                    info!(
                        remaining = %remaining,
                        "limit close did not fully fill in 10s, using market order for remainder"
                    );
                    let _ = venue.cancel_all(symbol).await;
                    if let Err(e) = venue.market_close(symbol, side, Size(remaining)).await {
                        warn!(error = ?e, remaining = %remaining,
                            "market close of flatten remainder FAILED — position left open (will be re-adopted next wave_auto cycle)");
                        return false;
                    }
                    true
                }
                Ok(_) => {
                    info!("limit close fully filled");
                    true
                }
                Err(e) => {
                    warn!(error = ?e, "position check after limit close failed");
                    false
                }
            }
        }
        Err(e) => {
            warn!(error = ?e, "limit close failed, falling back to market order");
            if let Err(e) = venue.market_close(symbol, side, qty).await {
                warn!(error = ?e, qty = %qty.0,
                    "market close fallback FAILED — position left open (will be re-adopted next wave_auto cycle)");
                return false;
            }
            true
        }
    }
}
