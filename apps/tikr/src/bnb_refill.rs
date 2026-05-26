//! BNB-balance monitor for live BNB-pays-fees accounts.
//!
//! Reads the latest BNB state from `SharedBotState` every 60s and
//! warns when the USDT-equivalent value falls below the configured
//! `bnb_min_balance_usdt`. Auto-purchase wiring (spot buy + spot→futures
//! transfer) is intentionally deferred — those code paths need a SAPI
//! module that tikr-binance doesn't have yet. Until then, this module
//! surfaces the low-balance event loudly so the operator can refill
//! manually OR enable Binance UI's "Auto-Purchase BNB" toggle.
//!
//! All work no-ops when:
//!   - BNB-fee mode is off (auto-detected at startup)
//!   - `bnb_refill_enabled = false` in TOML
//!   - BNB balance × price hasn't dropped below threshold

use rust_decimal::Decimal;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{info, warn};

use crate::state::SharedBotState;

/// Config for the BNB monitor task.
pub struct BnbMonitorConfig {
    pub shared_state: SharedBotState,
    pub min_balance_usdt: Decimal,
    pub target_balance_usdt: Decimal,
    pub refill_enabled: bool,
    pub shutdown: watch::Receiver<bool>,
}

/// Spawn the BNB monitor task. Cheap — one timer + RwLock read per tick.
pub fn spawn_bnb_monitor(cfg: BnbMonitorConfig) {
    tokio::spawn(async move {
        // Bail early if refill is disabled in TOML. The BNB state
        // poll runs regardless (it powers the TUI display) — only
        // the warn-on-low-balance is gated by this flag.
        if !cfg.refill_enabled {
            info!("bnb_monitor: disabled in TOML (bnb_refill_enabled=false)");
            return;
        }
        info!(
            min_usdt = %cfg.min_balance_usdt,
            target_usdt = %cfg.target_balance_usdt,
            "bnb_monitor: started"
        );
        let mut shutdown = cfg.shutdown;
        let mut last_warn = std::time::Instant::now()
            .checked_sub(Duration::from_secs(3600))
            .unwrap_or_else(std::time::Instant::now);
        loop {
            let bnb = cfg.shared_state.bnb_snapshot();
            if bnb.enabled && bnb.price_usdt > Decimal::ZERO {
                let usdt_value = bnb.balance * bnb.price_usdt;
                if usdt_value < cfg.min_balance_usdt {
                    // Throttle the warning to once per 5min so a stuck
                    // low-balance state doesn't spam logs forever.
                    if last_warn.elapsed() >= Duration::from_secs(300) {
                        let needed_usdt = cfg.target_balance_usdt - usdt_value;
                        let needed_bnb = if bnb.price_usdt > Decimal::ZERO {
                            needed_usdt / bnb.price_usdt
                        } else {
                            Decimal::ZERO
                        };
                        warn!(
                            bnb_balance = %bnb.balance,
                            bnb_value_usdt = %usdt_value,
                            threshold_usdt = %cfg.min_balance_usdt,
                            target_usdt = %cfg.target_balance_usdt,
                            needed_bnb = %needed_bnb,
                            "BNB BALANCE LOW — refill needed. Buy ~{} BNB on spot \
                             then transfer spot→futures. Or enable Binance UI's \
                             'Auto-Purchase BNB' toggle for hands-off operation.",
                            needed_bnb.round_dp(4)
                        );
                        last_warn = std::time::Instant::now();
                    }
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(60)) => {}
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("bnb_monitor: shutdown");
                        return;
                    }
                }
            }
        }
    });
}
