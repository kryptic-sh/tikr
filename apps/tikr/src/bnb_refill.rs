//! BNB auto-refill for live BNB-pays-fees accounts.
//!
//! Reads the latest BNB state from `SharedBotState` every 60s. When the
//! USDT-equivalent value falls below `min_balance_usdt`, it tops the wallet
//! back up to `target_balance_usdt` by converting USDT → BNB **directly on the
//! futures wallet** via Binance's Futures Convert API (`/fapi/v1/convert/*`) —
//! no spot buy or spot→futures transfer required.
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

/// Don't convert again within this window of a prior convert — the BNB snapshot
/// (refreshed by the account poller) can lag the convert, and if that poll is
/// itself rate-limited the value would read stale-low; this stops a re-buy.
const CONVERT_COOLDOWN: Duration = Duration::from_secs(300);

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
        let mut shutdown = cfg.shutdown;
        let mut last_convert: Option<Instant> = None;
        loop {
            let bnb = cfg.shared_state.bnb_snapshot();
            if bnb.enabled && bnb.price_usdt > Decimal::ZERO {
                let usdt_value = bnb.balance * bnb.price_usdt;
                let cooling = last_convert
                    .map(|t| t.elapsed() < CONVERT_COOLDOWN)
                    .unwrap_or(false);
                if usdt_value < cfg.min_balance_usdt && !cooling {
                    // Buy enough to reach target (e.g. value $0.5, target $10 →
                    // convert $9.50 of USDT into BNB).
                    let needed = (cfg.target_balance_usdt - usdt_value).max(Decimal::ZERO);
                    if needed > Decimal::ZERO {
                        let from_amount = format!("{needed:.2}"); // USDT, 2 dp
                        info!(
                            bnb_balance = %bnb.balance,
                            bnb_value_usdt = %usdt_value.round_dp(4),
                            low_usdt = %cfg.min_balance_usdt,
                            target_usdt = %cfg.target_balance_usdt,
                            buy_usdt = %from_amount,
                            "BNB low — auto-converting USDT→BNB on futures"
                        );
                        match tikr_binance::futs::convert_futures(
                            &http,
                            cfg.env.rest_base_url(),
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
                                info!(
                                    bnb_received = %bnb_received,
                                    usdt_spent = %from_amount,
                                    "bnb_refill: convert succeeded"
                                );
                            }
                            Err(e) => {
                                // Brief cooldown on failure too so a hard error
                                // (e.g. below-min convert amount) doesn't retry
                                // every minute.
                                last_convert = Some(Instant::now());
                                warn!(error = ?e, "bnb_refill: convert USDT→BNB failed");
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
