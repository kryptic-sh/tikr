//! Simulated fill engine — turns strategy [`Action`]s + market events into
//! [`Fill`]s under a trade-through model with post-only correctness.
//! See [issue #11] for the optimistic fill bias note.
//!
//! [issue #11]: https://github.com/kryptic-sh/tikr/issues/11

use tikr_core::{Fill, MarketEvent, Timestamp};
use tikr_strategy::Action;

/// Per-venue fee schedule. Negative maker = rebate.
#[derive(Debug, Clone, Copy)]
pub struct VenueFees {
    /// Maker fee in basis points. Negative = rebate paid TO the maker.
    pub maker_bps: i32,
    /// Taker fee in basis points (always positive in practice).
    pub taker_bps: u32,
}

/// Configuration for [`FillSim`].
#[derive(Debug, Clone)]
pub struct FillSimConfig {
    /// Latency between action submission and venue ack, in milliseconds.
    pub submit_latency_ms: u64,
    /// Latency between cancel submission and venue ack, in milliseconds.
    pub cancel_latency_ms: u64,
    /// Per-venue fee schedule.
    pub fees: VenueFees,
}

/// Trade-through fill simulator. Phase 1 stub.
pub struct FillSim {
    _cfg: FillSimConfig,
}

impl FillSim {
    /// Construct a new fill simulator from `cfg`.
    pub fn new(cfg: FillSimConfig) -> Self {
        Self { _cfg: cfg }
    }

    /// Schedule a strategy action for venue submission at `now + submit_latency_ms`.
    pub fn on_action(&mut self, action: Action, now: Timestamp) {
        let _ = (action, now);
        todo!("issue #11: queue action with latency, handle post-only reject at submit-time")
    }

    /// Match queued open quotes against `ev`; emit fills for any quotes
    /// taken out by the trade-through model.
    pub fn on_market_event(&mut self, ev: &MarketEvent, now: Timestamp) -> Vec<Fill> {
        let _ = (ev, now);
        todo!("issue #11: trade-through matching, partial fills, fee assignment")
    }
}
