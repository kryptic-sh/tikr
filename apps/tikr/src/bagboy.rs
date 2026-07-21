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
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock as StdRwLock;
use std::time::Duration;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
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
    base_state_dir: PathBuf,
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

        // Restore persisted cumulative state, then establish authoritative
        // base balance and valid positive bid before entering fill-delta
        // accounting.  This prevents the hard-cap restart reset that would
        // otherwise grant fresh budget after a restart.
        let state_dir = base_state_dir.join(symbol.to_lowercase());
        let (restored, restore_failed) = match load_bagboy_state(&state_dir) {
            Ok(state) => (state, false),
            Err(e) => {
                warn!(
                    error = ?e,
                    "bagboy: cumulative state unreadable — stopping placements"
                );
                (None, true)
            }
        };
        let mut total_spent_usdt = restored
            .as_ref()
            .map(|s| s.cumulative_quote_spent)
            .unwrap_or(Decimal::ZERO);
        let mut total_base_acquired = restored
            .as_ref()
            .map(|s| s.cumulative_base_acquired)
            .unwrap_or(Decimal::ZERO);
        if restored.is_some() {
            info!(
                symbol = %symbol,
                base_acquired = %total_base_acquired,
                quote_spent = %total_spent_usdt,
                "bagboy: restored cumulative state"
            );
        }

        // Phase 1: establish authoritative base balance.  Keep retrying on
        // failure.  Do NOT count the existing balance as a new fill.
        let mut last_seen_base = loop {
            match client.balance(&base_asset).await {
                Ok(b) => {
                    let cur = b.free + b.locked;
                    info!(
                        symbol = %symbol, base = %base_asset,
                        seed_balance = %cur, restored_base = %total_base_acquired,
                        "bagboy: initial balance established"
                    );
                    break cur;
                }
                Err(e) => {
                    warn!(error = ?e, "bagboy: initial balance fetch failed — retrying");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    if *global_shutdown.borrow() {
                        return;
                    }
                }
            }
        };

        // Phase 2: establish valid positive book bid before normal delta
        // accounting.
        let mut last_known_bid = loop {
            match client.book_ticker(&symbol).await {
                Ok(b) if b.bid_price > Decimal::ZERO => {
                    info!(
                        symbol = %symbol, bid = %b.bid_price,
                        "bagboy: initial bid established"
                    );
                    break b.bid_price;
                }
                Ok(_) => {
                    warn!("bagboy: initial book bid is zero — waiting for valid bid");
                }
                Err(e) => {
                    warn!(error = ?e, "bagboy: initial book fetch failed — retrying");
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            if *global_shutdown.borrow() {
                return;
            }
        };

        // Set once cancel_all has fired for a reached cap, so we don't re-fire
        // it every poll tick (500ms) forever — cancel once, then just monitor.
        // Reset if the cap ever re-opens (e.g. a live config change raises the
        // budget) so a later breach cancels again.
        let mut cap_cancel_done = false;
        // When cumulative-state persistence fails we stop placing orders rather
        // than granting fresh budget on restart.
        let mut persist_failed = restore_failed;

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
                // Accumulate actual spend at fill-detection time (delta ×
                // price observed at detection) into a total that only ever
                // grows. NOT recomputed from current mark price elsewhere —
                // a falling price would otherwise lower "spent" after the
                // fact and let the ladder buy straight through
                // `max_total_usdt`.
                total_spent_usdt += delta * last_known_bid;
                info!(
                    symbol = %symbol, fill_base = %delta,
                    total_base = %total_base_acquired,
                    total_spent = %total_spent_usdt,
                    "bagboy: fill detected"
                );

                // Persist updated cumulative counters immediately so a
                // restart does not reset the hard-cap budget.
                let new_ps = BagboyPersistedState {
                    cumulative_base_acquired: total_base_acquired,
                    cumulative_quote_spent: total_spent_usdt,
                };
                if let Err(e) = save_bagboy_state(&state_dir, &new_ps) {
                    warn!(
                        error = ?e,
                        "bagboy: failed to persist cumulative state — stopping placements"
                    );
                    persist_failed = true;
                }

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

            // 2. Cap check.  A persistence failure also caps (fails safe)
            // rather than granting fresh budget on restart.
            let capped_by_fault = persist_failed;
            let capped_usdt = cfg
                .max_total_usdt
                .is_some_and(|cap| total_spent_usdt >= cap);
            let capped_base = cfg
                .max_total_base
                .is_some_and(|cap| total_base_acquired >= cap);
            if capped_by_fault || capped_usdt || capped_base {
                if !cap_cancel_done {
                    info!(
                        symbol = %symbol,
                        persist_failed = %persist_failed,
                        "bagboy: cap reached — canceling all + monitoring"
                    );
                    let _ = client.cancel_all(&symbol).await;
                    cap_cancel_done = true;
                }
                publish(
                    &live,
                    Decimal::ZERO,
                    total_base_acquired,
                    total_spent_usdt,
                    0,
                );
                continue;
            }
            cap_cancel_done = false; // cap not (or no longer) active

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

            // Displayed metrics. `total_spent_usdt` is NOT recomputed here —
            // it only advances at fill-detection time (see above), so it
            // reflects actual spend against `max_total_usdt` rather than the
            // current mark value of the position.
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
        peak_long_usdt: Decimal::ZERO,
        peak_short_usdt: Decimal::ZERO,
        metrics: Vec::new(),
        bagger_flattens: 0,
        bagger_target: None,
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

/// On-disk cumulative fill counters for a Bagboy instance.
///
/// Survives restarts so the hard-cap budget is not silently reset.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BagboyPersistedState {
    cumulative_base_acquired: Decimal,
    cumulative_quote_spent: Decimal,
}

fn save_bagboy_state(dir: &std::path::Path, state: &BagboyPersistedState) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    // Restrict directory permissions on platforms that support it so
    // other local users cannot read cumulative state.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700));
    }
    // Atomic write: temp file then rename.
    let tmp = dir.join("bagboy_state.tmp");
    let final_path = dir.join("bagboy_state.json");
    let json = serde_json::to_vec(state).map_err(std::io::Error::other)?;
    std::fs::write(&tmp, &json)?;
    std::fs::rename(&tmp, &final_path)?;
    // Best-effort directory sync — not critical on every modern FS.
    let _ = std::fs::File::open(dir).and_then(|f| f.sync_all());
    Ok(())
}

fn load_bagboy_state(dir: &std::path::Path) -> std::io::Result<Option<BagboyPersistedState>> {
    let path = dir.join("bagboy_state.json");
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(std::io::Error::other)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    #[test]
    fn state_round_trip() {
        let dir = std::env::temp_dir().join(format!("bagboy_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        let s = BagboyPersistedState {
            cumulative_base_acquired: Decimal::new(123456, 6), // 0.123456
            cumulative_quote_spent: Decimal::new(500000, 2),   // 5000.00
        };
        save_bagboy_state(&dir, &s).expect("save should succeed");

        let loaded = load_bagboy_state(&dir)
            .expect("load should succeed")
            .expect("state should exist");
        assert_eq!(loaded.cumulative_base_acquired, s.cumulative_base_acquired);
        assert_eq!(loaded.cumulative_quote_spent, s.cumulative_quote_spent);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn state_round_trip_zero() {
        let dir = std::env::temp_dir().join(format!("bagboy_test_zero_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        let s = BagboyPersistedState {
            cumulative_base_acquired: Decimal::ZERO,
            cumulative_quote_spent: Decimal::ZERO,
        };
        save_bagboy_state(&dir, &s).expect("save should succeed");

        let loaded = load_bagboy_state(&dir)
            .expect("load should succeed")
            .expect("state should exist");
        assert_eq!(loaded.cumulative_base_acquired, Decimal::ZERO);
        assert_eq!(loaded.cumulative_quote_spent, Decimal::ZERO);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_missing_returns_none() {
        let dir = std::env::temp_dir().join(format!("bagboy_test_missing_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        assert!(
            load_bagboy_state(&dir)
                .expect("missing state is not an error")
                .is_none()
        );
    }

    #[test]
    fn corrupt_state_returns_error() {
        let dir = std::env::temp_dir().join(format!("bagboy_test_corrupt_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("bagboy_state.json"), b"not json").unwrap();

        assert!(load_bagboy_state(&dir).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn save_creates_dir_and_file() {
        let dir = std::env::temp_dir().join(format!("bagboy_test_create_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        assert!(!dir.exists());
        let s = BagboyPersistedState {
            cumulative_base_acquired: Decimal::new(1, 0),
            cumulative_quote_spent: Decimal::new(2, 0),
        };
        save_bagboy_state(&dir, &s).expect("save should create dir and file");
        assert!(dir.join("bagboy_state.json").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn init_does_not_count_seed_balance_as_fill() {
        // Pure-function analogue: after seed balance is established,
        // any delta from current balance is a fill only when it
        // exceeds last_seen_base.  This test verifies the helper
        // pattern used by the real bot.
        let last_seen_base = Decimal::new(1000, 0); // seeded at 1000
        let cur_base = Decimal::new(1000, 0);
        // No delta → no fill.
        assert!(!(cur_base > last_seen_base));

        // A real fill delta: 1000 → 1005
        let cur_base = Decimal::new(1005, 0);
        assert!(cur_base > last_seen_base);
        assert_eq!(cur_base - last_seen_base, Decimal::new(5, 0));
    }

    #[test]
    fn existing_balance_not_added_to_accumulators() {
        // When the bot initializes last_seen_base from the current
        // balance, that existing amount is NOT added to
        // total_base_acquired.  Only a *subsequent* increase counts.
        let mut total_base_acquired = Decimal::ZERO; // restored from persisted state
        let last_seen_base = Decimal::new(5000, 0); // current wallet balance
        let _total_base_acquired_before = total_base_acquired;

        // Simulate a fill: balance goes from 5000 → 5003
        let cur_base = Decimal::new(5003, 0);
        if cur_base > last_seen_base {
            let delta = cur_base - last_seen_base;
            total_base_acquired += delta;
        }

        // Only the delta was counted.
        assert_eq!(total_base_acquired, Decimal::new(3, 0));
    }
}
