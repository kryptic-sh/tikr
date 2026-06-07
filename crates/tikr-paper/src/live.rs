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
    /// Price of our best (highest) resting BUY order — the one nearest the
    /// touch, most likely to fill. `0` when we have no resting buy.
    pub best_buy_price: Decimal,
    /// Total size resting at `best_buy_price`. `0` when no resting buy.
    pub best_buy_size: Decimal,
    /// Price of our best (lowest) resting SELL order — nearest the touch.
    /// `0` when we have no resting sell.
    pub best_sell_price: Decimal,
    /// Total size resting at `best_sell_price`. `0` when no resting sell.
    pub best_sell_size: Decimal,
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
    /// Peak LONG inventory notional reached this session (≥ 0). Max of
    /// `position_notional` while the position was long.
    pub peak_long_usdt: Decimal,
    /// Peak SHORT inventory notional reached this session (≥ 0). Max of
    /// `position_notional` while the position was short.
    pub peak_short_usdt: Decimal,
    /// Strategy-supplied `(label, value)` introspection pairs (e.g. Wave's
    /// effective step/inner, static-vs-auto). Empty for strategies that don't
    /// implement `status_metrics`. Rendered in the TUI bot-detail panel.
    pub metrics: Vec<(String, String)>,
    /// Number of bagger flatten actions this bot has issued this session
    /// (every enabled mechanism — equity giveback, cap, SL, etc.). `0` when the
    /// bagger is disabled or has not fired. Session-scoped (not persisted).
    pub bagger_flattens: u64,
    /// Active bagger target, preformatted for display (e.g. `"eqv 10%"`,
    /// `"lock ±2%"`). `None` when the bagger is disabled. Same for every bot on
    /// the account; the TUI aggregate shows one.
    pub bagger_target: Option<String>,
}
