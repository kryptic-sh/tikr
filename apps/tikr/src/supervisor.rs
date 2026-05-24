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
use tikr_paper::BotHandle;
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
    /// Account margin percent allocated to all bot order sizes.
    pub order_balance_pct: Decimal,
    /// Multiplier applied to wallet balance before sizing, typically leverage.
    pub margin_multiplier: Decimal,
    /// Account margin percent for the per-bot peak-position cap (split by
    /// `bot_count`). Drives `max_position_usdt` defaults handed to
    /// strategies that don't set their own cap in TOML.
    pub max_position_pct: Decimal,
    /// Number of configured bots sharing the account allocation.
    pub bot_count: usize,
    /// Live per-bot notional updates from the account balance poller.
    pub notional_rx: watch::Receiver<Decimal>,
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

                shared_state.set_status(&symbol_str, BotStatus::Starting);

                let handle_result = run_once(&ctx).await;
                match handle_result {
                    Ok(spawned) => {
                        attempt = 0; // reset backoff on successful spawn
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
                                        let _ = tokio::time::timeout(Duration::from_secs(5), &mut join).await;
                                        let _ = us_shutdown_tx.send(true);
                                        return;
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("bot spawn failed: {e}");
                        shared_state.set_status(&symbol_str, BotStatus::Crashed(format!("spawn: {e}")));
                    }
                }

                // Exponential backoff: 2^attempt seconds, capped at 60.
                let delay = (1u64 << attempt.min(6)).min(60);
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
        let reset_venue =
            venue::build_venue(ctx.env, &ctx.api_key, &ctx.key_material, &symbol).await?;
        reset_symbol_state(&reset_venue, &symbol).await;
        drop(reset_venue);
    } else {
        info!("resuming live state — pass --clear to flatten + cancel-all at startup");
    }

    info!("building venue for runner");
    let venue_for_run =
        venue::build_venue(ctx.env, &ctx.api_key, &ctx.key_material, &symbol).await?;

    info!("subscribing user data stream");
    let (us_shutdown_tx, us_shutdown_rx) = watch::channel(false);
    let fill_rx = venue::subscribe_fills(
        ctx.env,
        &ctx.api_key,
        ctx.key_material.clone(),
        &symbol,
        us_shutdown_rx,
    )
    .await?;

    let default_notional = default_order_notional(&venue_for_run, ctx).await?;
    // Derive max_pos from the same wallet × mult source as default_notional:
    //   max_pos = wallet × mult × max_position_pct / 100 / bot_count
    //          = default_notional × (max_position_pct / order_balance_pct)
    let max_pos_default = if ctx.order_balance_pct > Decimal::ZERO {
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
        max_pos_default,
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
    let handle = tikr_paper::spawn_bot(spec, venue_for_run, Some(fill_rx), external_liqs);
    Ok(SpawnedBot {
        handle,
        us_shutdown_tx,
    })
}

async fn default_order_notional(venue: &BinanceClient, ctx: &SupervisorCtx) -> Result<Decimal> {
    let balance = venue.futures_balance("USDT").await?;
    let bot_count = Decimal::from(ctx.bot_count.max(1) as u64);
    Ok(
        balance.wallet_balance * ctx.margin_multiplier * ctx.order_balance_pct
            / Decimal::from(100)
            / bot_count,
    )
}

pub(crate) async fn reset_symbol_state(venue: &BinanceClient, symbol: &tikr_core::Symbol) {
    use rust_decimal::Decimal;
    use tikr_core::{Side, Size};
    use tracing::info;

    if let Err(e) = venue.cancel_all(symbol).await {
        warn!(error = ?e, "cancel_all failed (continuing)");
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
                info!(
                    qty = %qty,
                    entry = %pos.avg_entry.0,
                    notional = %notional_approx,
                    min_notional = %min_n,
                    "skipping flatten: dust position below minNotional — bot will trade on top"
                );
                return;
            }
            flatten_with_limit_fallback(venue, symbol, close_side, Size(qty)).await;
        }
        Ok(_) => {}
        Err(e) => warn!(error = ?e, "venue.position failed"),
    }
}

/// Try to close with a limit order at mid; fall back to market after 10s.
async fn flatten_with_limit_fallback(
    venue: &BinanceClient,
    symbol: &tikr_core::Symbol,
    side: tikr_core::Side,
    qty: tikr_core::Size,
) {
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
                let _ = venue.market_close(symbol, side, qty).await;
                return;
            }
            // Use mid price — aggressive enough to likely fill within 10s
            Price((bid + ask) / Decimal::from(2))
        }
        Err(e) => {
            warn!(error = ?e, "snapshot failed for limit close, using market order");
            let _ = venue.market_close(symbol, side, qty).await;
            return;
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
                    let _ = venue.market_close(symbol, side, Size(remaining)).await;
                }
                Ok(_) => {
                    info!("limit close fully filled");
                }
                Err(e) => {
                    warn!(error = ?e, "position check after limit close failed");
                }
            }
        }
        Err(e) => {
            warn!(error = ?e, "limit close failed, falling back to market order");
            let _ = venue.market_close(symbol, side, qty).await;
        }
    }
}
