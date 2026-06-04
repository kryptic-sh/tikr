//! Bagboy — MEXC spot accumulator with optional laddered BUYs.
//!
//! Maintains a ladder of `ladder_levels` resting LIMIT BUYs spaced
//! `ladder_step_bps` apart, starting at best_bid:
//!   level 0: best_bid
//!   level 1: best_bid − step
//!   ...
//!   level N: best_bid − N×step
//!
//! Each cycle:
//! 1. Detect fills via base-balance delta.
//! 2. Check USDT balance — skip placements if too low (avoids -2010 spam).
//! 3. Check optional hard cap (USDT spent or base accumulated).
//! 4. Fetch best_bid.
//! 5. Reconcile ladder via openOrders:
//!    - Cancel any resting BID outside the active window
//!      `[best_bid − N×step, best_bid]` (snapped to tick).
//!    - For each missing level in window, place LIMIT BUY.
//!
//! Credentials loaded from env: `MEXC_API_KEY`, `MEXC_API_SECRET`.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::RwLock as StdRwLock;
use std::time::Duration;

use rust_decimal::Decimal;
use tikr_core::Side;
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
        let poll = cfg.poll_interval_ms.max(200);
        let levels = cfg.ladder_levels.max(1);
        let step_bps = cfg.ladder_step_bps;
        info!(symbol = %symbol, levels, step_bps, "bagboy: starting");

        let filters = match client.symbol_filters(&symbol).await {
            Ok(f) => f,
            Err(e) => {
                warn!(error = ?e, "bagboy: symbol_filters failed — bot disabled");
                return;
            }
        };
        let (base_asset, quote_asset) = split_symbol_assets(&symbol);

        let live: Arc<StdRwLock<Option<LiveSnapshot>>> = Arc::new(StdRwLock::new(None));
        let snapshot: Arc<StdRwLock<Option<tikr_paper::PaperReport>>> =
            Arc::new(StdRwLock::new(None));
        shared_state.insert(
            &symbol,
            BotView {
                symbol: symbol.clone(),
                strategy: "bagboy".to_string(),
                status: BotStatus::Starting,
                snapshot: snapshot.clone(),
                live: live.clone(),
                shutdown_tx: None,
                api_position: Arc::new(StdRwLock::new(None)),
                banked: false,
            },
        );
        shared_state.set_status(&symbol, BotStatus::Running);

        let mut total_spent_usdt = Decimal::ZERO;
        let mut total_base_acquired = Decimal::ZERO;
        let mut last_seen_base = Decimal::ZERO;
        let mut last_known_bid = Decimal::ZERO;
        if let Ok(b) = client.balance(&base_asset).await {
            last_seen_base = b.free + b.locked;
            info!(
                symbol = %symbol, base = %base_asset,
                seed_balance = %last_seen_base, "bagboy: seeded"
            );
        }

        let mut tick = tokio::time::interval(Duration::from_millis(poll));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = tick.tick() => {}
                _ = global_shutdown.changed() => {
                    if *global_shutdown.borrow() {
                        // Leave resting orders intact on shutdown — they persist
                        // across restarts. Closing is not a shutdown side effect.
                        info!(symbol = %symbol, "bagboy: shutdown — leaving orders intact");
                        return;
                    }
                }
            }

            // 1. Detect fills via base balance delta.
            let bal_base = match client.balance(&base_asset).await {
                Ok(b) => b,
                Err(e) => {
                    warn!(error = ?e, "bagboy: base balance fetch failed");
                    continue;
                }
            };
            let cur_base = bal_base.free + bal_base.locked;
            if cur_base > last_seen_base {
                let delta = cur_base - last_seen_base;
                total_base_acquired += delta;
                info!(
                    symbol = %symbol, fill_base = %delta,
                    total_base = %total_base_acquired,
                    "bagboy: fill detected"
                );
                // Record fill on LiveSnapshot so the watcher task picks
                // it up. Approximate price = last known bid.
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                if let Ok(mut g) = live.write()
                    && let Some(s) = g.as_mut()
                {
                    s.last_fill_ts = Some(now_ms * 1_000_000); // ns
                    s.last_fill_side = Some(tikr_core::Side::Bid);
                    s.last_fill_price = last_known_bid;
                    s.last_fill_size = delta;
                }
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
                info!(symbol = %symbol, "bagboy: cap reached — canceling all + monitoring");
                let _ = client.cancel_all(&symbol).await;
                publish(
                    &live,
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
                    Decimal::ZERO,
                    total_base_acquired,
                    total_spent_usdt,
                    0,
                );
                continue;
            }
            last_known_bid = book.bid_price;

            // 4. USDT balance gate — skip placements if we can't afford
            //    even one min-notional order. Avoids -2010 / insufficient
            //    funds error spam.
            let bal_quote = match client.balance(&quote_asset).await {
                Ok(b) => b,
                Err(e) => {
                    warn!(error = ?e, "bagboy: quote balance fetch failed");
                    continue;
                }
            };
            let quote_free = bal_quote.free;
            let min_order_cost = cfg.usdt_per_order.max(filters.min_notional);
            if quote_free < min_order_cost {
                // Update spent total from accumulated base × current price.
                if total_base_acquired > Decimal::ZERO {
                    total_spent_usdt = total_base_acquired * book.bid_price;
                }
                // Reconcile open count for display.
                let open_count = match client.open_orders(&symbol).await {
                    Ok(orders) => orders.len() as u32,
                    Err(_) => 0,
                };
                publish(
                    &live,
                    book.bid_price,
                    total_base_acquired,
                    total_spent_usdt,
                    open_count,
                );
                continue;
            }

            // 5. Reconcile ladder via openOrders.
            let open_orders = match client.open_orders(&symbol).await {
                Ok(o) => o,
                Err(e) => {
                    warn!(error = ?e, "bagboy: openOrders fetch failed");
                    continue;
                }
            };
            let existing_bids: BTreeSet<Decimal> = open_orders
                .iter()
                .filter(|o| o.side == Side::Bid)
                .map(|o| o.price)
                .collect();

            // Compute target ladder prices, snapped to tick.
            let tick = if filters.tick_size > Decimal::ZERO {
                filters.tick_size
            } else {
                Decimal::new(1, 8)
            };
            let step = if step_bps > 0 {
                let raw = book.bid_price * Decimal::from(step_bps) / Decimal::from(10_000);
                if raw > tick {
                    (raw / tick).ceil() * tick
                } else {
                    tick
                }
            } else {
                tick
            };
            let mut target_prices: Vec<Decimal> = Vec::with_capacity(levels as usize);
            for i in 0..levels {
                let p = book.bid_price - Decimal::from(i) * step;
                if p > Decimal::ZERO {
                    let snapped = (p / tick).floor() * tick;
                    if snapped > Decimal::ZERO {
                        target_prices.push(snapped);
                    }
                }
            }
            let target_set: BTreeSet<Decimal> = target_prices.iter().copied().collect();

            // Cancel orders outside target window (price > top or below floor).
            let target_top = *target_set.iter().max().unwrap_or(&Decimal::ZERO);
            let target_floor = *target_set.iter().min().unwrap_or(&Decimal::ZERO);
            for o in &open_orders {
                if o.side != Side::Bid {
                    continue;
                }
                if o.price > target_top || o.price < target_floor {
                    let _ = client.cancel_order(&symbol, &o.client_order_id).await;
                    info!(
                        symbol = %symbol, price = %o.price,
                        "bagboy: cancel stale ladder order"
                    );
                }
            }

            // Place missing ladder levels (respecting USDT budget).
            let mut remaining_budget = quote_free;
            for tgt_price in &target_prices {
                if existing_bids.contains(tgt_price) {
                    continue;
                }
                let qty = compute_quantity(
                    cfg.usdt_per_order,
                    *tgt_price,
                    filters.step_size,
                    filters.min_notional,
                    filters.min_qty,
                );
                if qty <= Decimal::ZERO {
                    continue;
                }
                let cost = qty * *tgt_price;
                if cost > remaining_budget {
                    // Skip rather than reject from venue.
                    break;
                }
                let price_str = format_decimal(*tgt_price, tick);
                let qty_str = format_decimal(qty, filters.step_size);
                let coid = format!("bb_{}", Uuid::new_v4().as_simple());
                match client
                    .place_limit_buy(&symbol, &price_str, &qty_str, &coid)
                    .await
                {
                    Ok(_) => {
                        info!(
                            symbol = %symbol,
                            price = %tgt_price, qty = %qty,
                            "bagboy: ladder order placed"
                        );
                        remaining_budget -= cost;
                    }
                    Err(e) => {
                        warn!(error = ?e, symbol = %symbol, price = %tgt_price, "bagboy: place failed");
                    }
                }
            }

            // Update displayed metrics.
            if total_base_acquired > Decimal::ZERO {
                total_spent_usdt = total_base_acquired * book.bid_price;
            }
            let open_count_after = match client.open_orders(&symbol).await {
                Ok(o) => o.len() as u32,
                Err(_) => 0,
            };
            publish(
                &live,
                book.bid_price,
                total_base_acquired,
                total_spent_usdt,
                open_count_after,
            );
        }
    })
}

fn publish(
    live: &Arc<StdRwLock<Option<LiveSnapshot>>>,
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
        best_buy_price: Decimal::ZERO,
        best_buy_size: Decimal::ZERO,
        best_sell_price: Decimal::ZERO,
        best_sell_size: Decimal::ZERO,
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

fn format_decimal(value: Decimal, unit: Decimal) -> String {
    let scale = unit.scale();
    format!("{value:.scale$}", scale = scale as usize)
}

fn split_symbol_assets(symbol: &str) -> (String, String) {
    for suffix in ["USDT", "USDC", "BUSD", "TUSD"] {
        if let Some(base) = symbol.strip_suffix(suffix) {
            return (base.to_string(), suffix.to_string());
        }
    }
    let split = symbol.len().saturating_sub(4);
    (symbol[..split].to_string(), symbol[split..].to_string())
}
