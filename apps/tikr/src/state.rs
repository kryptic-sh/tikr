//! Dashboard-side aggregate state shared between the supervisors and
//! the TUI render loop.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use rust_decimal::Decimal;
use tikr_paper::{BotHandle, LiveSnapshot, PaperReport};
use tokio::sync::watch;

/// Account balance read directly from Binance REST.
#[derive(Debug, Clone, Default)]
pub struct ApiAccountSnapshot {
    /// Margin asset, usually `USDT`.
    pub asset: String,
    /// Futures wallet balance reported by Binance.
    pub wallet_balance: Decimal,
    /// Balance available for new orders / withdrawals.
    pub available_balance: Decimal,
    /// Binance cross-position unrealized PnL for this asset.
    pub cross_unrealized_pnl: Decimal,
    /// Local unix timestamp in ms when this value was fetched.
    pub fetched_at_ms: u64,
}

/// Per-symbol position values read directly from Binance REST.
#[derive(Debug, Clone, Default)]
pub struct ApiPositionSnapshot {
    /// Signed position amount. Positive = long, negative = short.
    pub position_amount: Decimal,
    /// Binance entry price.
    pub entry_price: Decimal,
    /// Binance break-even price.
    pub break_even_price: Decimal,
    /// Binance mark price used for unrealized PnL.
    pub mark_price: Decimal,
    /// Binance unrealized PnL for this symbol.
    pub unrealized_profit: Decimal,
    /// Local unix timestamp in ms when this value was fetched.
    pub fetched_at_ms: u64,
}

/// Bot lifecycle status as the supervisor sees it.
#[derive(Debug, Clone)]
pub enum BotStatus {
    /// Supervisor is initializing — building venue, subscribing fills.
    Starting,
    /// Bot task is running; live PnL flows through the snapshot tap.
    Running,
    /// Bot task is done; carries a reason string.
    Crashed(String),
    /// Sleeping before next respawn (carries human-readable delay).
    Restarting(String),
    /// Intentionally stopped by an auto-manager (rotated out / removed) — not a
    /// fault. Lingers in the dashboard for a few cycles before GC.
    Rotated,
}

impl BotStatus {
    /// Short word for the tabs header — `on` / `off` / `restarting`
    /// / `starting`. Lower-case to match the user-facing convention.
    pub fn tag(&self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Running => "on",
            Self::Crashed(_) => "off",
            Self::Restarting(_) => "restarting",
            Self::Rotated => "off",
        }
    }
}

/// Per-bot live view used by the TUI.
pub struct BotView {
    /// Display label (e.g. `"BTCUSDT/static-grid"`).
    pub label: String,
    /// Symbol string (e.g. `"BTCUSDT"`).
    pub symbol: String,
    /// Strategy tag.
    pub strategy: String,
    /// Supervisor lifecycle status.
    pub status: BotStatus,
    /// Snapshot tap shared with the bot task. `None` initially.
    pub snapshot: Arc<RwLock<Option<PaperReport>>>,
    /// Fill-granular live tap shared with the bot task.
    pub live: Arc<RwLock<Option<LiveSnapshot>>>,
    /// Shutdown sender for the current incarnation (None between restarts).
    pub shutdown_tx: Option<watch::Sender<bool>>,
    /// Last Binance REST positionRisk snapshot for this symbol.
    pub api_position: Arc<RwLock<Option<ApiPositionSnapshot>>>,
}

impl BotView {
    /// Read the current snapshot, if any.
    #[allow(dead_code)]
    pub fn snapshot(&self) -> Option<PaperReport> {
        self.snapshot.read().ok().and_then(|g| g.clone())
    }
}

/// Account-wide BNB-fee mode snapshot. When `enabled = true`, every
/// order's commission is debited in BNB from the futures wallet (with
/// the 10% maker/taker discount). The user-stream parser uses
/// `price_usdt` to convert BNB commissions → USDT-equivalent for the
/// tracker, and the refill task watches `balance` against
/// `min_balance_usdt` / `target_balance_usdt`.
#[derive(Debug, Clone, Default)]
pub struct BnbState {
    /// Whether `/fapi/v1/feeBurn` returned `true` for this account.
    /// When `false`, all BNB-aware logic is bypassed.
    pub enabled: bool,
    /// Most recent BNB free balance in the futures wallet (BNB units).
    pub balance: Decimal,
    /// Most recent BNBUSDT mid (USDT per BNB).
    pub price_usdt: Decimal,
    /// Local unix timestamp in ms when these values were fetched.
    #[allow(dead_code)]
    pub fetched_at_ms: u64,
}

/// Per-symbol rolling price + fill-marker history for the TUI chart.
/// Capped ring buffer — drops oldest when at capacity.
#[derive(Debug, Clone, Default)]
pub struct PriceHistory {
    /// `(ts_ms, price)` samples, oldest first.
    pub samples: Vec<(u64, Decimal)>,
    /// `(ts_ms, price, is_buy)` fill markers, oldest first.
    pub fills: Vec<(u64, Decimal, bool)>,
    /// Last sample timestamp — used to downsample to 1/sec.
    pub last_sample_ms: u64,
}

impl PriceHistory {
    /// Max samples retained (≈10 samples/sec * 3 min = 1800; chart
    /// window is 60s so headroom for stalls).
    pub const MAX_SAMPLES: usize = 1800;
    /// Max fill markers retained.
    pub const MAX_FILLS: usize = 500;

    /// Push a price sample. Dedupes consecutive identical prices to
    /// keep the buffer tight when the book is quiet. Trims to MAX_SAMPLES.
    pub fn push_sample(&mut self, ts_ms: u64, price: Decimal) {
        if price <= Decimal::ZERO {
            return;
        }
        if let Some(&(_, last_price)) = self.samples.last()
            && last_price == price
        {
            return;
        }
        self.samples.push((ts_ms, price));
        self.last_sample_ms = ts_ms;
        if self.samples.len() > Self::MAX_SAMPLES {
            let drop = self.samples.len() - Self::MAX_SAMPLES;
            self.samples.drain(0..drop);
        }
    }

    /// Push a fill marker. Trims to MAX_FILLS.
    pub fn push_fill(&mut self, ts_ms: u64, price: Decimal, is_buy: bool) {
        self.fills.push((ts_ms, price, is_buy));
        if self.fills.len() > Self::MAX_FILLS {
            let drop = self.fills.len() - Self::MAX_FILLS;
            self.fills.drain(0..drop);
        }
    }
}

/// Shared state keyed by symbol. Wrap in `Arc` for cross-task sharing.
#[derive(Clone)]
pub struct SharedBotState {
    inner: Arc<Mutex<HashMap<String, BotView>>>,
    /// Per-symbol price + fill history for the TUI chart.
    history: Arc<RwLock<HashMap<String, PriceHistory>>>,
    /// Ordered list of symbols — the TUI's tab order. Kept in sync with
    /// the order bots were inserted.
    order: Arc<Mutex<Vec<String>>>,
    api_account: Arc<RwLock<Option<ApiAccountSnapshot>>>,
    /// First wallet balance reading for API net calc.
    start_balance: Arc<RwLock<Option<Decimal>>>,
    /// Account-wide BNB-fee state. Updated by the account-poll task.
    /// Shared with the user-stream parser (fee conversion) and the
    /// optional BNB auto-refill task.
    bnb: Arc<RwLock<BnbState>>,
    /// First BNB balance reading × first BNB price = USDT-equivalent
    /// starting BNB value. Used for BNB-aware NET calculation
    /// (subtract BNB-value delta from realized PnL → real banked).
    bnb_start_value_usdt: Arc<RwLock<Option<Decimal>>>,
    /// Running P&L totals of bots removed from the dashboard (rotated out +
    /// GC'd), so the account summary keeps counting their realized P&L (which
    /// the exchange wallet retains).
    retired: Arc<Mutex<RetiredTotals>>,
}

impl Default for SharedBotState {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedBotState {
    /// Construct an empty shared state.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            history: Arc::new(RwLock::new(HashMap::new())),
            order: Arc::new(Mutex::new(Vec::new())),
            api_account: Arc::new(RwLock::new(None)),
            start_balance: Arc::new(RwLock::new(None)),
            bnb: Arc::new(RwLock::new(BnbState::default())),
            bnb_start_value_usdt: Arc::new(RwLock::new(None)),
            retired: Arc::new(Mutex::new(RetiredTotals::default())),
        }
    }

    /// Running totals of departed (removed) bots, for the account summary.
    pub fn retired_totals(&self) -> RetiredTotals {
        self.retired.lock().ok().map(|t| *t).unwrap_or_default()
    }

    /// Update the BNB state snapshot. Captures the starting BNB-value
    /// on first call where `enabled = true` AND `balance × price > 0`,
    /// so the BNB-aware NET row in the TUI can compute the delta.
    pub fn set_bnb(&self, state: BnbState) {
        if state.enabled
            && state.balance > Decimal::ZERO
            && state.price_usdt > Decimal::ZERO
            && let Ok(mut start) = self.bnb_start_value_usdt.write()
            && start.is_none()
        {
            *start = Some(state.balance * state.price_usdt);
        }
        if let Ok(mut g) = self.bnb.write() {
            *g = state;
        }
    }

    /// Read the latest BNB snapshot.
    pub fn bnb_snapshot(&self) -> BnbState {
        self.bnb.read().ok().map(|g| g.clone()).unwrap_or_default()
    }

    /// Starting BNB USDT-value (locked on first non-zero reading).
    pub fn bnb_start_value_usdt(&self) -> Option<Decimal> {
        self.bnb_start_value_usdt.read().ok().and_then(|g| *g)
    }

    /// Update account-wide API balance snapshot. Captures start balance on first call.
    pub fn set_api_account(&self, snapshot: ApiAccountSnapshot) {
        if let Ok(mut start) = self.start_balance.write()
            && start.is_none()
            && snapshot.wallet_balance > Decimal::ZERO
        {
            *start = Some(snapshot.wallet_balance);
        }
        if let Ok(mut g) = self.api_account.write() {
            *g = Some(snapshot);
        }
    }

    /// First wallet balance reading for API net calculation.
    pub fn start_balance(&self) -> Option<Decimal> {
        self.start_balance.read().ok().and_then(|g| *g)
    }

    /// Read latest account-wide API balance snapshot.
    pub fn api_account(&self) -> Option<ApiAccountSnapshot> {
        self.api_account.read().ok().and_then(|g| g.clone())
    }

    /// Insert a fresh bot view (called once per bot before its
    /// supervisor starts).
    pub fn insert(&self, symbol: &str, view: BotView) {
        if let Ok(mut g) = self.inner.lock() {
            g.insert(symbol.to_string(), view);
        }
        if let Ok(mut o) = self.order.lock()
            && !o.iter().any(|s| s == symbol)
        {
            o.push(symbol.to_string());
        }
    }

    /// Remove a symbol from the dashboard state. Folds the departing bot's
    /// realized P&L into the retired totals first, so the account summary keeps
    /// counting it (the exchange wallet does) instead of dropping it on rotation.
    pub fn remove(&self, symbol: &str) {
        if let Ok(mut g) = self.inner.lock()
            && let Some(view) = g.remove(symbol)
            && let Ok(snap) = view.snapshot.read()
            && let Some(r) = snap.as_ref()
            && let Ok(mut t) = self.retired.lock()
        {
            t.realized += r.realized.0;
            t.unrealized += r.unrealized.0;
            t.fees += r.fees.0;
            t.funding += r.funding.0;
            t.net += r.net.0;
            t.events = t.events.saturating_add(r.events_processed);
            t.fills = t.fills.saturating_add(r.fills_emitted);
            t.count += 1;
        }
        if let Ok(mut o) = self.order.lock() {
            o.retain(|s| s != symbol);
        }
    }

    /// Update status for a symbol.
    pub fn set_status(&self, symbol: &str, status: BotStatus) {
        if let Ok(mut g) = self.inner.lock()
            && let Some(v) = g.get_mut(symbol)
        {
            v.status = status;
        }
    }

    /// Wire a freshly-spawned bot's handle into the view.
    pub fn attach_handle(&self, symbol: &str, handle: &BotHandle) {
        if let Ok(mut g) = self.inner.lock()
            && let Some(v) = g.get_mut(symbol)
        {
            v.snapshot = handle.state.clone();
            v.live = handle.live.clone();
            v.shutdown_tx = Some(handle.shutdown_tx.clone());
            v.status = BotStatus::Running;
        }
    }

    /// Stamp a final report (after a bot crash/clean exit) into the view.
    pub fn set_final(&self, symbol: &str, report: PaperReport) {
        if let Ok(mut g) = self.inner.lock()
            && let Some(v) = g.get_mut(symbol)
            && let Ok(mut s) = v.snapshot.write()
        {
            *s = Some(report);
        }
    }

    /// Snapshot a vec of views (clones) in insertion order. Used by the
    /// TUI's draw loop.
    pub fn views(&self) -> Vec<BotViewSnapshot> {
        let order = self.order.lock().map(|o| o.clone()).unwrap_or_default();
        let map = match self.inner.lock() {
            Ok(g) => g,
            Err(_) => return Vec::new(),
        };
        let hist = self.history.read().ok();
        let mut out: Vec<BotViewSnapshot> = order
            .into_iter()
            .filter_map(|sym| {
                let v = map.get(&sym)?;
                let history = hist.as_ref().and_then(|h| h.get(&sym).cloned());
                Some(BotViewSnapshot {
                    symbol: v.symbol.clone(),
                    label: v.label.clone(),
                    strategy: v.strategy.clone(),
                    status: v.status.clone(),
                    snapshot: v.snapshot.read().ok().and_then(|g| g.clone()),
                    live: v.live.read().ok().and_then(|g| g.clone()),
                    api_position: v.api_position.read().ok().and_then(|g| g.clone()),
                    history,
                })
            })
            .collect();
        // Tab order: ON (Running) bots first, then everything else; alphabetical
        // by symbol within each group. Stable sort keeps it deterministic.
        out.sort_by(|a, b| {
            let on = |s: &BotStatus| !matches!(s, BotStatus::Running);
            on(&a.status)
                .cmp(&on(&b.status))
                .then_with(|| a.symbol.cmp(&b.symbol))
        });
        out
    }

    /// Symbols currently known to the dashboard state.
    pub fn symbols(&self) -> Vec<String> {
        self.order.lock().map(|o| o.clone()).unwrap_or_default()
    }
}

impl SharedBotState {
    /// Update per-symbol API position snapshot.
    pub fn set_api_position(&self, symbol: &str, snapshot: ApiPositionSnapshot) {
        if let Ok(g) = self.inner.lock()
            && let Some(v) = g.get(symbol)
            && let Ok(mut pos) = v.api_position.write()
        {
            *pos = Some(snapshot);
        }
    }

    /// Append a price sample for `symbol` (downsampled to 1/sec by
    /// PriceHistory::push_sample).
    pub fn push_price_sample(&self, symbol: &str, ts_ms: u64, price: Decimal) {
        if let Ok(mut g) = self.history.write() {
            g.entry(symbol.to_string())
                .or_default()
                .push_sample(ts_ms, price);
        }
    }

    /// Append a fill marker for `symbol`.
    pub fn push_fill_marker(&self, symbol: &str, ts_ms: u64, price: Decimal, is_buy: bool) {
        if let Ok(mut g) = self.history.write() {
            g.entry(symbol.to_string())
                .or_default()
                .push_fill(ts_ms, price, is_buy);
        }
    }

    /// Clone the current price history for `symbol` for read-only render.
    #[allow(dead_code)]
    pub fn price_history(&self, symbol: &str) -> Option<PriceHistory> {
        self.history.read().ok()?.get(symbol).cloned()
    }
}

/// Cloneable point-in-time view for the renderer.
#[derive(Clone)]
pub struct BotViewSnapshot {
    /// Symbol.
    pub symbol: String,
    /// Display label.
    #[allow(dead_code)]
    pub label: String,
    /// Strategy tag.
    pub strategy: String,
    /// Current lifecycle status.
    pub status: BotStatus,
    /// Latest PaperReport snapshot, if any.
    pub snapshot: Option<PaperReport>,
    /// Latest fill-granular live snapshot, if any.
    pub live: Option<LiveSnapshot>,
    /// Latest Binance REST positionRisk snapshot, if any.
    pub api_position: Option<ApiPositionSnapshot>,
    /// Rolling price + fill history for the chart panel.
    pub history: Option<PriceHistory>,
}

/// Account-wide aggregate computed from all bot views.
/// Running totals for bots that have been REMOVED from the dashboard (rotated
/// out by an auto-manager and GC'd). Their realized P&L stays in the exchange
/// wallet, so the account summary must keep counting it — otherwise the bot's
/// totals drift below the API/wallet truth as symbols rotate. Folded in on
/// [`SharedBotState::remove`].
#[derive(Debug, Default, Clone, Copy)]
pub struct RetiredTotals {
    /// Σ realized PnL of departed bots.
    pub realized: Decimal,
    /// Σ unrealized PnL at removal (≈0 — rotated bots are flattened first).
    pub unrealized: Decimal,
    /// Σ fees paid by departed bots.
    pub fees: Decimal,
    /// Σ funding accrued by departed bots.
    pub funding: Decimal,
    /// Σ NET of departed bots.
    pub net: Decimal,
    /// Σ events processed by departed bots.
    pub events: u64,
    /// Σ fills emitted by departed bots.
    pub fills: u64,
    /// Number of departed bots folded in.
    pub count: usize,
}

#[derive(Default)]
pub struct AccountAggregate {
    /// Σ realized PnL across all bots.
    pub realized: Decimal,
    /// Σ unrealized PnL.
    pub unrealized: Decimal,
    /// Σ fees paid.
    pub fees: Decimal,
    /// Σ funding accrued (Phase 3: always zero).
    pub funding: Decimal,
    /// Σ NET (realized + unrealized − fees + funding).
    pub net: Decimal,
    /// Σ events processed.
    pub events: u64,
    /// Σ fills emitted.
    pub fills: u64,
    /// Σ buy-side fills.
    pub buy_fills: u64,
    /// Σ sell-side fills.
    pub sell_fills: u64,
    /// Σ buy-side volume in quote currency.
    pub buy_volume: Decimal,
    /// Σ sell-side volume in quote currency.
    pub sell_volume: Decimal,
    /// Σ resting buy quotes.
    pub open_buys: u64,
    /// Σ resting sell quotes.
    pub open_sells: u64,
    /// Σ |position × mid| in USDT (gross inventory exposure).
    pub gross_inventory: Decimal,
    /// Signed Σ position × mid in USDT (net directional bias).
    pub net_inventory: Decimal,
    /// Σ Binance per-symbol unrealized PnL from positionRisk.
    pub api_unrealized: Decimal,
    /// Σ local unrealized PnL re-marked with Binance mark prices.
    pub mark_unrealized: Decimal,
    /// At least one view has an API position snapshot (reliable API-data flag).
    pub has_api_positions: bool,
    /// Count of bots currently in `Running` state.
    pub running_count: usize,
    /// Count of bots in `Crashed` state.
    pub crashed_count: usize,
    /// Count of bots in `Restarting` state.
    pub restarting_count: usize,
    /// Count of bots in `Starting` state.
    pub starting_count: usize,
    /// Count of bots intentionally rotated out (`Rotated` state) — not faults.
    pub rotated_count: usize,
    /// NET P&L of bots that were REMOVED (rotated out + GC'd), already folded
    /// into the realized/net totals above. Surfaced separately so the operator
    /// can see how much of the total came from departed bots.
    pub retired_net: Decimal,
    /// Number of departed bots folded in.
    pub retired_count: usize,
}

impl AccountAggregate {
    /// Compute from a snapshot of bot views, seeded with the running totals of
    /// already-departed (rotated-out + GC'd) bots so the account summary tracks
    /// the exchange wallet rather than drifting down as symbols rotate.
    pub fn compute(views: &[BotViewSnapshot], retired: RetiredTotals) -> Self {
        let mut a = AccountAggregate::default();
        // Seed with departed-bot totals (their P&L is still in the wallet).
        a.realized += retired.realized;
        a.unrealized += retired.unrealized;
        a.fees += retired.fees;
        a.funding += retired.funding;
        a.net += retired.net;
        a.events = a.events.saturating_add(retired.events);
        a.fills = a.fills.saturating_add(retired.fills);
        a.retired_net = retired.net;
        a.retired_count = retired.count;
        for v in views {
            match v.status {
                BotStatus::Running => a.running_count += 1,
                BotStatus::Crashed(_) => a.crashed_count += 1,
                BotStatus::Restarting(_) => a.restarting_count += 1,
                BotStatus::Starting => a.starting_count += 1,
                BotStatus::Rotated => a.rotated_count += 1,
            }
            if let Some(ref r) = v.snapshot {
                a.realized += r.realized.0;
                a.unrealized += r.unrealized.0;
                a.fees += r.fees.0;
                a.funding += r.funding.0;
                a.net += r.net.0;
                a.events = a.events.saturating_add(r.events_processed);
                a.fills = a.fills.saturating_add(r.fills_emitted);
            }
            if let Some(ref lv) = v.live {
                a.buy_fills = a.buy_fills.saturating_add(lv.buy_fills);
                a.sell_fills = a.sell_fills.saturating_add(lv.sell_fills);
                a.buy_volume += lv.buy_volume;
                a.sell_volume += lv.sell_volume;
                a.open_buys = a.open_buys.saturating_add(lv.open_buys as u64);
                a.open_sells = a.open_sells.saturating_add(lv.open_sells as u64);
                a.net_inventory += lv.inventory_usdt;
                a.gross_inventory += lv.inventory_usdt.abs();
            }
            if let Some(ref api) = v.api_position {
                a.has_api_positions = true;
                a.api_unrealized += api.unrealized_profit;
                if let (Some(r), Some(lv)) = (&v.snapshot, &v.live) {
                    a.mark_unrealized += mark_unrealized(r.unrealized.0, lv, api.mark_price);
                }
            }
        }
        a
    }
}

pub fn mark_unrealized(
    mid_unrealized: Decimal,
    live: &tikr_paper::live::LiveSnapshot,
    mark_price: Decimal,
) -> Decimal {
    mid_unrealized + live.position_size * (mark_price - live.last_mid)
}
