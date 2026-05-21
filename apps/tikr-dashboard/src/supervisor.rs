//! Auto-restart supervisor for bots. One supervisor task per bot.
//!
//! Wraps `spawn_bot` in a loop: when the underlying JoinHandle resolves
//! (either clean shutdown or crash), the supervisor decides whether to
//! restart with exponential backoff or stay down (if global shutdown
//! requested).

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
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
                                            info!(net = %report.net.0, "bot ended cleanly");
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
    // moved into spawn_bot below.
    info!("startup reset (cancel + flatten)");
    let reset_venue = venue::build_venue(ctx.env, &ctx.api_key, &ctx.key_material, &symbol).await?;
    reset_symbol_state(&reset_venue, &symbol).await;
    drop(reset_venue);

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

    let spec = to_spec(&ctx.cfg, symbol, &venue_for_run, &ctx.base_state_dir)?;
    info!(strategy = %spec.strategy.label(), "spawning bot");
    let handle = tikr_paper::spawn_bot(spec, venue_for_run, Some(fill_rx));
    Ok(SpawnedBot {
        handle,
        us_shutdown_tx,
    })
}

async fn reset_symbol_state(venue: &BinanceClient, symbol: &tikr_core::Symbol) {
    use rust_decimal::Decimal;
    use tikr_core::{QuoteKind, Side, Size, TimeInForce};
    use tikr_venue::QuoteIntent;

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
            let snap = venue.snapshot(symbol).await.ok();
            let close_price = snap.as_ref().and_then(|s| match close_side {
                Side::Bid => s.asks.first().map(|l| l.price),
                Side::Ask => s.bids.first().map(|l| l.price),
            });
            if let Some(price) = close_price {
                let intent = QuoteIntent {
                    symbol: symbol.clone(),
                    side: close_side,
                    price,
                    size: Size(qty),
                    tif: TimeInForce::IOC,
                    kind: QuoteKind::Point,
                };
                if let Err(e) = venue.quote(intent).await {
                    warn!(error = ?e, "flatten failed");
                }
            }
        }
        Ok(_) => {}
        Err(e) => warn!(error = ?e, "venue.position failed"),
    }
}
