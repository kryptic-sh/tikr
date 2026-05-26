//! Bagboy — MEXC spot accumulator.
//!
//! Maintains one resting LIMIT BUY at best_bid for a single MEXC spot
//! symbol. Refills on fill, repositions when book moves. No sells, no
//! closes — pure accumulation.
//!
//! Lifecycle (per `poll_interval_ms`):
//! 1. Fetch best_bid/ask via `/api/v3/ticker/bookTicker`.
//! 2. Detect fills: compare current base balance to the last seen value.
//! 3. If no resting order: place LIMIT BUY at best_bid (size = usdt_per_order / price).
//! 4. If resting order's price ≠ best_bid: cancel + replace at new best_bid.
//! 5. If hard cap (USDT or base) hit: stop placing, just monitor.
//!
//! Credentials loaded from env: `MEXC_API_KEY`, `MEXC_API_SECRET`.
//! TUI integration: pushes a BotView for the symbol so it appears as a
//! tab alongside Binance bots.

use std::sync::Arc;
use std::sync::RwLock as StdRwLock;
use std::time::Duration;

use rust_decimal::Decimal;
use tikr_mexc::MexcClient;
use tikr_paper::live::LiveSnapshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{info, warn};
use uuid::Uuid;

use crate::config::BagboyConfig;
use crate::state::{BotStatus, BotView, SharedBotState};

pub fn spawn_bagboy(
    cfg: BagboyConfig,
    shared_state: SharedBotState,
    mut global_shutdown: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        // Resolve credentials.
        let api_key = match std::env::var("MEXC_API_KEY") {
            Ok(v) => v,
            Err(_) => {
                warn!("bagboy: MEXC_API_KEY missing — bot disabled");
                return;
            }
        };
        let api_secret = match std::env::var("MEXC_API_SECRET") {
            Ok(v) => v,
            Err(_) => {
                warn!("bagboy: MEXC_API_SECRET missing — bot disabled");
                return;
            }
        };

        let client = MexcClient::new(api_key, api_secret);
        let symbol = cfg.symbol.clone();
        let poll = cfg.poll_interval_ms.max(250);
        info!(symbol = %symbol, "bagboy: starting");

        // Fetch symbol filters for size/notional bumping.
        let filters = match client.symbol_filters(&symbol).await {
            Ok(f) => f,
            Err(e) => {
                warn!(error = ?e, "bagboy: symbol_filters failed — bot disabled");
                return;
            }
        };
        let (base_asset, quote_asset) = split_symbol_assets(&symbol);

        // Register in TUI.
        let live: Arc<StdRwLock<Option<LiveSnapshot>>> = Arc::new(StdRwLock::new(None));
        let snapshot: Arc<StdRwLock<Option<tikr_paper::PaperReport>>> =
            Arc::new(StdRwLock::new(None));
        shared_state.insert(
            &symbol,
            BotView {
                label: format!("{symbol}/bagboy"),
                symbol: symbol.clone(),
                strategy: "bagboy".to_string(),
                status: BotStatus::Starting,
                snapshot: snapshot.clone(),
                live: live.clone(),
                shutdown_tx: None,
                api_position: Arc::new(StdRwLock::new(None)),
            },
        );
        shared_state.set_status(&symbol, BotStatus::Running);

        // State tracked in-loop.
        let mut resting_coid: Option<String> = None;
        let mut resting_price: Option<Decimal> = None;
        let mut total_spent_usdt = Decimal::ZERO;
        let mut total_base_acquired = Decimal::ZERO;
        let mut last_seen_base = Decimal::ZERO;
        // Seed initial balance so we don't count pre-existing holdings as fills.
        if let Ok(b) = client.balance(&base_asset).await {
            last_seen_base = b.free + b.locked;
            info!(symbol = %symbol, base = %base_asset, seed_balance = %last_seen_base, "bagboy: seeded");
        }

        let mut tick = tokio::time::interval(Duration::from_millis(poll));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = tick.tick() => {}
                _ = global_shutdown.changed() => {
                    if *global_shutdown.borrow() {
                        info!(symbol = %symbol, "bagboy: shutdown — canceling resting order");
                        if let Some(coid) = resting_coid.as_ref() {
                            let _ = client.cancel_order(&symbol, coid).await;
                        }
                        return;
                    }
                }
            }

            // 1. Detect fills via base balance delta.
            let bal = match client.balance(&base_asset).await {
                Ok(b) => b,
                Err(e) => {
                    warn!(error = ?e, "bagboy: balance fetch failed");
                    continue;
                }
            };
            let cur_base = bal.free + bal.locked;
            if cur_base > last_seen_base {
                let delta = cur_base - last_seen_base;
                total_base_acquired += delta;
                if let Some(p) = resting_price {
                    total_spent_usdt += delta * p;
                }
                info!(
                    symbol = %symbol, fill_base = %delta,
                    total_base = %total_base_acquired,
                    total_usdt = %total_spent_usdt,
                    "bagboy: fill detected"
                );
                // Order is fully filled (or partially — we'll let next cycle
                // re-emit if it's gone from openOrders). Clear local state.
                resting_coid = None;
                resting_price = None;
                last_seen_base = cur_base;
            }

            // 2. Cap check.
            let capped_usdt = cfg
                .max_total_usdt
                .is_some_and(|cap| total_spent_usdt >= cap);
            let capped_base = cfg
                .max_total_base
                .is_some_and(|cap| total_base_acquired >= cap);
            if capped_usdt || capped_base {
                if resting_coid.is_some() {
                    info!(symbol = %symbol, "bagboy: cap reached — canceling resting order");
                    if let Some(coid) = resting_coid.as_ref() {
                        let _ = client.cancel_order(&symbol, coid).await;
                    }
                    resting_coid = None;
                    resting_price = None;
                }
                publish(
                    &live,
                    &symbol,
                    Decimal::ZERO,
                    total_base_acquired,
                    total_spent_usdt,
                    0,
                );
                continue;
            }

            // 3. Get book.
            let book = match client.book_ticker(&symbol).await {
                Ok(b) => b,
                Err(e) => {
                    warn!(error = ?e, "bagboy: book fetch failed");
                    continue;
                }
            };
            if book.bid_price <= Decimal::ZERO {
                publish(
                    &live,
                    &symbol,
                    Decimal::ZERO,
                    total_base_acquired,
                    total_spent_usdt,
                    0,
                );
                continue;
            }

            // 4. Reposition if price moved.
            if let Some(rp) = resting_price
                && rp != book.bid_price
            {
                info!(
                    symbol = %symbol,
                    from = %rp, to = %book.bid_price,
                    "bagboy: book moved — canceling stale order"
                );
                if let Some(coid) = resting_coid.as_ref() {
                    let _ = client.cancel_order(&symbol, coid).await;
                }
                resting_coid = None;
                resting_price = None;
            }

            // 5. Place if none resting.
            if resting_coid.is_none() {
                let qty = compute_quantity(
                    cfg.usdt_per_order,
                    book.bid_price,
                    filters.step_size,
                    filters.min_notional,
                    filters.min_qty,
                );
                if qty > Decimal::ZERO {
                    let price_str = format_decimal(book.bid_price, filters.tick_size);
                    let qty_str = format_decimal(qty, filters.step_size);
                    let coid = format!("bb_{}", Uuid::new_v4().as_simple());
                    match client
                        .place_limit_buy(&symbol, &price_str, &qty_str, &coid)
                        .await
                    {
                        Ok(_) => {
                            resting_coid = Some(coid);
                            resting_price = Some(book.bid_price);
                            info!(
                                symbol = %symbol,
                                price = %book.bid_price, qty = %qty,
                                "bagboy: order placed"
                            );
                        }
                        Err(e) => {
                            warn!(error = ?e, symbol = %symbol, "bagboy: place failed");
                        }
                    }
                }
            }

            publish(
                &live,
                &symbol,
                book.bid_price,
                total_base_acquired,
                total_spent_usdt,
                if resting_coid.is_some() { 1 } else { 0 },
            );

            // Silence unused suspect on quote_asset.
            let _ = &quote_asset;
        }
    })
}

fn publish(
    live: &Arc<StdRwLock<Option<LiveSnapshot>>>,
    _symbol: &str,
    bid: Decimal,
    total_base: Decimal,
    total_usdt: Decimal,
    open_buys: u32,
) {
    let snap = LiveSnapshot {
        position_size: total_base,
        avg_entry: if total_base > Decimal::ZERO {
            total_usdt / total_base
        } else {
            Decimal::ZERO
        },
        last_mid: bid,
        last_bid: bid,
        last_ask: bid,
        buy_fills: 0,
        sell_fills: 0,
        buy_volume: total_usdt,
        sell_volume: Decimal::ZERO,
        open_quotes: open_buys,
        open_buys,
        open_sells: 0,
        last_fill_ts: None,
        last_fill_side: None,
        last_fill_price: Decimal::ZERO,
        last_fill_size: Decimal::ZERO,
        inventory_usdt: total_base * bid,
    };
    if let Ok(mut g) = live.write() {
        *g = Some(snap);
    }
}

/// Compute order qty from USDT budget; auto-bump to min_notional/min_qty.
fn compute_quantity(
    usdt: Decimal,
    price: Decimal,
    step: Decimal,
    min_notional: Decimal,
    min_qty: Decimal,
) -> Decimal {
    if price <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let raw = usdt / price;
    let stepped = if step > Decimal::ZERO {
        (raw / step).floor() * step
    } else {
        raw
    };
    let bumped = if min_qty > Decimal::ZERO && stepped < min_qty {
        min_qty
    } else {
        stepped
    };
    if min_notional > Decimal::ZERO && bumped * price < min_notional && step > Decimal::ZERO {
        (min_notional / price / step).ceil() * step
    } else {
        bumped
    }
}

/// Format a Decimal with the precision dictated by `unit` (a step or
/// tick value). Uses unit's scale to pick decimal places.
fn format_decimal(value: Decimal, unit: Decimal) -> String {
    let scale = unit.scale();
    format!("{value:.scale$}", scale = scale as usize)
}

/// Split a MEXC symbol like "NAVUSDT" into (base, quote) — assumes
/// USDT/USDC/BUSD/TUSD 4-char suffix; falls back to splitting at the
/// last position where the suffix matches.
fn split_symbol_assets(symbol: &str) -> (String, String) {
    for suffix in ["USDT", "USDC", "BUSD", "TUSD"] {
        if let Some(base) = symbol.strip_suffix(suffix) {
            return (base.to_string(), suffix.to_string());
        }
    }
    // Fallback: assume last 4 chars are quote.
    let split = symbol.len().saturating_sub(4);
    (symbol[..split].to_string(), symbol[split..].to_string())
}
