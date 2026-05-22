//! Live (sub-snapshot-interval) bot state for dashboards / supervisors.
//!
//! [`PaperReport`][crate::report::PaperReport] is the persistable view that
//! the runner writes to disk every `snapshot_every_n_events`. That cadence
//! is fine for resume but too coarse for an "ops dashboard" feel — operators
//! want position size + open-order counts + last fill to update on every
//! fill, not every 100 events.
//!
//! [`LiveSnapshot`] is the cheap, in-memory mirror published into a
//! `Arc<RwLock<Option<LiveSnapshot>>>` on every fill *and* every regular
//! snapshot tick. Nothing in here is persisted — restarts start fresh.

use tikr_core::{Decimal, Side};

/// Live, point-in-time snapshot of bot state. Updated on every fill +
/// every `snapshot_every_n_events`.
#[derive(Debug, Clone, Default)]
pub struct LiveSnapshot {
    /// Signed position size (positive = long, negative = short).
    pub position_size: Decimal,
    /// Volume-weighted average entry price. `0` when flat.
    pub avg_entry: Decimal,
    /// Last seen mid (best bid + best ask)/2. `0` until first
    /// `BookUpdate`.
    pub last_mid: Decimal,
    /// Last seen best bid. `0` until first `BookUpdate`.
    pub last_bid: Decimal,
    /// Last seen best ask. `0` until first `BookUpdate`.
    pub last_ask: Decimal,
    /// Cumulative buy-side fill count this session.
    pub buy_fills: u64,
    /// Cumulative sell-side fill count this session.
    pub sell_fills: u64,
    /// Cumulative buy-side volume (price × size) in quote currency.
    pub buy_volume: Decimal,
    /// Cumulative sell-side volume (price × size) in quote currency.
    pub sell_volume: Decimal,
    /// Total resting open quotes (buys + sells).
    pub open_quotes: u32,
    /// Resting buy-side quotes.
    pub open_buys: u32,
    /// Resting sell-side quotes.
    pub open_sells: u32,
    /// Timestamp (ns since epoch) of the most recent fill applied to
    /// this bot's tracker, if any.
    pub last_fill_ts: Option<u64>,
    /// Side of the most recent fill.
    pub last_fill_side: Option<Side>,
    /// Price of the most recent fill.
    pub last_fill_price: Decimal,
    /// Size of the most recent fill.
    pub last_fill_size: Decimal,
    /// Inventory marked at `last_mid`: `position_size × last_mid`.
    /// Signed (positive = long, negative = short).
    pub inventory_usdt: Decimal,
}
