//! BNB auto-refill for live BNB-pays-fees accounts.
//!
//! Every 60s, when BNB-pays-fees is on, it does a **live** API read of the
//! BNB futures-wallet balance + BNBUSDT mid (not the shared snapshot, which can
//! lag stale-low). When the USDT-equivalent value falls below `min_balance_usdt`
//! it tops the wallet back up to `target_balance_usdt` by converting USDT → BNB
//! **directly on the futures wallet** via Binance's Futures Convert API
//! (`/fapi/v1/convert/*`) — no spot buy or spot→futures transfer required. The
//! buy is capped at `target_balance_usdt` so a bad reading can never size a
//! runaway convert.
//!
//! All work no-ops when:
//!   - BNB-fee mode is off (auto-detected at startup)
//!   - `bnb_refill_enabled = false` in TOML
//!   - BNB value hasn't dropped below `min_balance_usdt`

use std::sync::Arc;
use std::time::{Duration, Instant};

use rust_decimal::Decimal;
use tikr_binance::{BinanceEnv, BinanceKeyMaterial};
use tokio::sync::watch;
use tracing::{info, warn};

use crate::state::SharedBotState;

/// Config for the BNB auto-refill task.
pub struct BnbMonitorConfig {
    pub shared_state: SharedBotState,
    pub env: BinanceEnv,
    pub api_key: String,
    pub key_material: Arc<BinanceKeyMaterial>,
    /// Low bound: refill when `bnb_value_usdt < min_balance_usdt`.
    pub min_balance_usdt: Decimal,
    /// Target: convert enough USDT→BNB to bring the value up to this.
    pub target_balance_usdt: Decimal,
    pub refill_enabled: bool,
    pub shutdown: watch::Receiver<bool>,
}

/// Don't convert again within this window of a prior convert — even with a live
/// balance re-read, the Convert API can take a moment to settle; this is a
/// second backstop against a re-buy.
const CONVERT_COOLDOWN: Duration = Duration::from_secs(300);

/// Floor on a single convert. Below this there's nothing meaningful to top up
/// (and it's near Binance's dust/minimum-convert size), so we skip.
const MIN_CONVERT_USDT: Decimal = Decimal::ONE;

/// Live BNB futures-wallet balance + BNBUSDT mid, fetched directly from the API.
/// Returns `None` (skip the tick — never convert on uncertainty) if either call
/// fails or the price is non-positive.
async fn live_bnb_value(
    http: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    key_material: &BinanceKeyMaterial,
) -> Option<(Decimal, Decimal)> {
    let balance =
        match tikr_binance::futs::get_balance(http, base_url, api_key, key_material, "BNB").await {
            Ok(b) => b.wallet_balance,
            Err(e) => {
                warn!(error = ?e, "bnb_refill: live BNB balance fetch failed — skipping tick");
                return None;
            }
        };
    let price = match tikr_binance::futs::get_book_ticker(http, base_url, "BNBUSDT").await {
        Ok(t) => (t.bid_price + t.ask_price) / Decimal::from(2),
        Err(e) => {
            warn!(error = ?e, "bnb_refill: live BNBUSDT price fetch failed — skipping tick");
            return None;
        }
    };
    (price > Decimal::ZERO).then_some((balance, price))
}

/// Spawn the BNB auto-refill task. Cheap — one timer + RwLock read per tick,
/// and a Convert API round-trip only when actually refilling.
pub fn spawn_bnb_monitor(cfg: BnbMonitorConfig) {
    tokio::spawn(async move {
        if !cfg.refill_enabled {
            info!("bnb_refill: disabled in TOML (bnb_refill_enabled=false)");
            return;
        }
        info!(
            min_usdt = %cfg.min_balance_usdt,
            target_usdt = %cfg.target_balance_usdt,
            "bnb_refill: started (auto-convert USDT→BNB on the futures wallet)"
        );
        let http = reqwest::Client::new();
        let base_url = cfg.env.rest_base_url();
        let mut shutdown = cfg.shutdown;
        let mut last_convert: Option<Instant> = None;
        loop {
            // Gate on the poller's feeBurn detection only. The conversion
            // decision itself uses a LIVE balance + price read below — never the
            // shared snapshot, which can lag the poller and read stale-low (or
            // 0 before the BNB asset is first populated), which would otherwise
            // trigger an unnecessary convert on bad data.
            if cfg.shared_state.bnb_snapshot().enabled {
                let cooling = last_convert
                    .map(|t| t.elapsed() < CONVERT_COOLDOWN)
                    .unwrap_or(false);
                if !cooling
                    && let Some((balance, price)) =
                        live_bnb_value(&http, base_url, &cfg.api_key, &cfg.key_material).await
                {
                    let usdt_value = balance * price;
                    if usdt_value < cfg.min_balance_usdt {
                        // Top up to target (e.g. value $0.5, target $10 → buy
                        // $9.50). Capped at `target` so a bad reading can never
                        // size a runaway buy, and floored at MIN_CONVERT_USDT.
                        let needed = (cfg.target_balance_usdt - usdt_value)
                            .max(Decimal::ZERO)
                            .min(cfg.target_balance_usdt);
                        if needed >= MIN_CONVERT_USDT {
                            let from_amount = format!("{needed:.2}"); // USDT, 2 dp
                            info!(
                                bnb_balance = %balance,
                                bnb_price = %price,
                                bnb_value_usdt = %usdt_value.round_dp(4),
                                low_usdt = %cfg.min_balance_usdt,
                                target_usdt = %cfg.target_balance_usdt,
                                buy_usdt = %from_amount,
                                "BNB low — auto-converting USDT→BNB on futures (live read)"
                            );
                            match tikr_binance::futs::convert_futures(
                                &http,
                                base_url,
                                &cfg.api_key,
                                &cfg.key_material,
                                "USDT",
                                "BNB",
                                &from_amount,
                            )
                            .await
                            {
                                Ok(bnb_received) => {
                                    last_convert = Some(Instant::now());
                                    // Project the new balance from the delivered
                                    // amount — an immediate API re-read races
                                    // Binance's balance settlement (lags a few
                                    // seconds) and would log a stale 0.
                                    let new_balance = balance + bnb_received;
                                    info!(
                                        bnb_received = %bnb_received,
                                        usdt_spent = %from_amount,
                                        new_bnb_balance = %new_balance,
                                        new_value_usdt = %(new_balance * price).round_dp(4),
                                        "bnb_refill: convert succeeded"
                                    );
                                }
                                Err(e) => {
                                    // Brief cooldown on failure too so a hard
                                    // error (e.g. below-min convert amount)
                                    // doesn't retry every minute.
                                    last_convert = Some(Instant::now());
                                    warn!(error = ?e, "bnb_refill: convert USDT→BNB failed");
                                }
                            }
                        }
                    }
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(60)) => {}
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("bnb_refill: shutdown");
                        return;
                    }
                }
            }
        }
    });
}
