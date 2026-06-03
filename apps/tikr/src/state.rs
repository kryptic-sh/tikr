//! Dashboard-side aggregate state shared between the supervisors and
//! the TUI render loop.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
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
    /// Binance estimated liquidation price (`0` when flat / no liq risk).
    pub liquidation_price: Decimal,
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
    /// Retention window: the chart shows up to 5 minutes of one-second candles,
    /// so we never keep samples (or fills) older than 300s behind the newest.
    /// Anything past this is pruned on every push.
    pub const WINDOW_MS: u64 = 300_000;
    /// Hard safety cap on retained samples (a burst within the window shouldn't
    /// grow unbounded). Time-pruning is the primary bound.
    pub const MAX_SAMPLES: usize = 12_000;
    /// Hard safety cap on retained fill markers within the window.
    pub const MAX_FILLS: usize = 2_000;

    /// Push a price sample. Dedupes consecutive identical prices to keep the
    /// buffer tight when the book is quiet, then prunes everything older than
    /// the retention window. Quiet seconds are filled by `advance`, not here.
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
        self.prune(ts_ms);
    }

    /// Push a fill marker, then prune to the window.
    pub fn push_fill(&mut self, ts_ms: u64, price: Decimal, is_buy: bool) {
        self.fills.push((ts_ms, price, is_buy));
        self.prune(ts_ms.max(self.last_sample_ms));
    }

    /// Advance the timeline to `now_ms`: if no sample has landed in the current
    /// second, seed a flat carry-forward sample (the last known price) at the
    /// second boundary, then prune. Called on a steady tick so a quiet book
    /// still emits blank (flat) candles into storage — the buffer keeps sliding
    /// and candles older than the window drop off even with zero activity.
    /// The seeded point is the second's OPEN; a real trade later in the same
    /// second still updates high/low/close via `push_sample`.
    pub fn advance(&mut self, now_ms: u64) {
        let sec = now_ms / 1000;
        let last = self.samples.last().copied();
        if let Some((last_ts, last_price)) = last
            && last_ts / 1000 < sec
        {
            // Open the new second at the prior close — a true flat candle.
            self.samples.push((sec * 1000, last_price));
            self.last_sample_ms = sec * 1000;
        }
        self.prune(now_ms);
    }

    /// Drop samples + fills older than `now_ms − WINDOW_MS`, then enforce the
    /// hard count caps. `samples`/`fills` are kept oldest-first (ascending ts).
    fn prune(&mut self, now_ms: u64) {
        let cutoff = now_ms.saturating_sub(Self::WINDOW_MS);
        let s = self.samples.partition_point(|(t, _)| *t < cutoff);
        if s > 0 {
            self.samples.drain(0..s);
        }
        let fcut = self.fills.partition_point(|(t, _, _)| *t < cutoff);
        if fcut > 0 {
            self.fills.drain(0..fcut);
        }
        if self.samples.len() > Self::MAX_SAMPLES {
            let drop = self.samples.len() - Self::MAX_SAMPLES;
            self.samples.drain(0..drop);
        }
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
    /// Wall-clock instant this process started (for account uptime).
    process_started: std::time::Instant,
    /// Accumulated account uptime from PRIOR sessions, in seconds. Total uptime
    /// = this + `process_started.elapsed()`. Restored from the session manifest
    /// so the `$/hour` rate divides NET by cumulative uptime across restarts.
    uptime_offset_secs: Arc<std::sync::atomic::AtomicU64>,
    /// Base session/snapshot dir, set once at startup. Lets `remove` delete a
    /// retired bot's per-symbol snapshot so its realized P&L can't be resumed
    /// (double-counting it once it's already folded into the retired totals).
    state_dir: Arc<RwLock<Option<std::path::PathBuf>>>,
    /// Per-symbol price decimal places (from the venue's tick size), so the TUI
    /// renders prices at the coin's precision — matching what Binance shows.
    price_decimals: Arc<RwLock<HashMap<String, u32>>>,
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
            process_started: std::time::Instant::now(),
            uptime_offset_secs: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            state_dir: Arc::new(RwLock::new(None)),
            price_decimals: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Record `symbol`'s price decimal places (derived from the venue tick
    /// size), surfaced on [`BotViewSnapshot`] for coin-precision price rendering.
    pub fn set_price_decimals(&self, symbol: &str, decimals: u32) {
        if let Ok(mut g) = self.price_decimals.write() {
            g.insert(symbol.to_string(), decimals);
        }
    }

    /// Record the base session/snapshot dir so `remove` can delete a retired
    /// bot's per-symbol snapshot. Per-bot snapshots live at
    /// `<base>/<symbol_lowercase>/` (see `per_bot_state_dir`).
    pub fn set_state_dir(&self, dir: std::path::PathBuf) {
        if let Ok(mut g) = self.state_dir.write() {
            *g = Some(dir);
        }
    }

    /// Delete a symbol's per-bot snapshot dir, so a future incarnation of the
    /// same symbol starts fresh instead of resuming P&L already banked into the
    /// retired totals. No-op if the base dir isn't set or the dir is absent.
    fn delete_bot_snapshot(&self, symbol: &str) {
        if let Ok(g) = self.state_dir.read()
            && let Some(base) = g.as_ref()
        {
            let bot_dir = base.join(symbol.to_lowercase());
            let _ = std::fs::remove_dir_all(&bot_dir);
        }
    }

    /// Delete per-symbol snapshots for every bot currently ROTATED out. Called
    /// at shutdown so a graceful restart (which banks lingering-rotated bots
    /// into the retired totals and drops them from the roster) can't later
    /// resume their already-counted P&L if the symbol rotates back in.
    pub fn purge_rotated_snapshots(&self) {
        for v in self.views() {
            if matches!(v.status, BotStatus::Rotated) {
                self.delete_bot_snapshot(&v.symbol);
            }
        }
    }

    /// Running totals of departed (removed) bots, for the account summary.
    pub fn retired_totals(&self) -> RetiredTotals {
        self.retired.lock().ok().map(|t| *t).unwrap_or_default()
    }

    /// Total account uptime in seconds = prior-sessions offset + this process's
    /// elapsed time. Drives the TUI uptime line + the `$/hour` rate so both span
    /// cumulative runtime across restarts.
    pub fn uptime_secs(&self) -> u64 {
        self.uptime_offset_secs
            .load(std::sync::atomic::Ordering::Relaxed)
            .saturating_add(self.process_started.elapsed().as_secs())
    }

    /// Snapshot the current manager-level state for persistence: balance
    /// baselines, retired totals, and the live roster.
    ///
    /// A bot that has ROTATED out but is still lingering in `views` (awaiting
    /// GC) is folded into the PERSISTED retired totals here and left OUT of the
    /// roster — mirroring exactly what the TUI shows (`AccountAggregate::compute`
    /// display-folds the same lingering-rotated bots). Without this the persisted
    /// retired only counted GC'd bots, so the retired line dropped after a
    /// restart while the rotated bot got re-spawned and its P&L moved back into
    /// the running total. Excluding rotated bots from the roster also stops them
    /// being re-spawned (and resuming their already-banked P&L) on restart.
    pub fn session_state(&self) -> SessionState {
        let mut retired = self.retired_totals();
        let mut roster = Vec::new();
        for v in self.views() {
            if matches!(v.status, BotStatus::Rotated) {
                if let Some(r) = v.snapshot.as_ref() {
                    retired.realized += r.realized.0;
                    retired.unrealized += r.unrealized.0;
                    retired.fees += r.fees.0;
                    retired.funding += r.funding.0;
                    retired.net += r.net.0;
                    retired.events = retired.events.saturating_add(r.events_processed);
                    retired.fills = retired.fills.saturating_add(r.fills_emitted);
                    retired.count += 1;
                }
                continue; // rotated → banked into retired, not the roster
            }
            roster.push(SessionBot {
                symbol: v.symbol,
                strategy: v.strategy,
                status: v.status.tag().to_string(),
            });
        }
        SessionState {
            start_balance: self.start_balance(),
            bnb_start_value_usdt: self.bnb_start_value_usdt(),
            retired,
            uptime_secs: self.uptime_secs(),
            roster,
        }
    }

    /// Restore a persisted session (on restart, unless `--clear`). Pre-seeds the
    /// balance baselines so the first live poll does NOT re-baseline to the
    /// current wallet (keeping "$ since start" continuous), and restores the
    /// retired totals. Returns the saved roster symbols so the caller can
    /// re-spawn the rotation lineup. Does not insert bot views — managers do
    /// that as they (re-)spawn.
    pub fn restore_session(&self, s: SessionState) -> Vec<String> {
        if let Some(bal) = s.start_balance
            && let Ok(mut g) = self.start_balance.write()
        {
            *g = Some(bal);
        }
        if let Some(bnb) = s.bnb_start_value_usdt
            && let Ok(mut g) = self.bnb_start_value_usdt.write()
        {
            *g = Some(bnb);
        }
        if let Ok(mut g) = self.retired.lock() {
            *g = s.retired;
        }
        // Resume the uptime timer from the persisted cumulative total (this
        // process's elapsed is added on top by `uptime_secs`).
        self.uptime_offset_secs
            .store(s.uptime_secs, std::sync::atomic::Ordering::Relaxed);
        s.roster.into_iter().map(|b| b.symbol).collect()
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
    /// Then deletes its per-symbol snapshot so the same realized P&L can't be
    /// resumed (and double-counted) if the symbol later rotates back in.
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
        self.delete_bot_snapshot(symbol);
    }

    /// Current NET PnL (`realized + unrealized − fees`) for `symbol`, read from
    /// its bot's latest snapshot. `None` if the symbol is unknown or has no
    /// snapshot yet. Used by the rampage rotator to gate rotation on NET.
    pub fn net_for(&self, symbol: &str) -> Option<Decimal> {
        let g = self.inner.lock().ok()?;
        let view = g.get(symbol)?;
        let snap = view.snapshot.read().ok()?;
        snap.as_ref().map(|r| r.net.0)
    }

    /// Live `(unrealized_pnl, gross_bag_notional)` for `symbol` from its latest
    /// Binance positionRisk snapshot. Gross notional = `|position| × mark`.
    /// `None` if the symbol is unknown or no API position snapshot has landed.
    /// Used by the rampage rotator's big-bag hold.
    pub fn bag_for(&self, symbol: &str) -> Option<(Decimal, Decimal)> {
        let g = self.inner.lock().ok()?;
        let view = g.get(symbol)?;
        let pos = view.api_position.read().ok()?;
        pos.as_ref()
            .map(|p| (p.unrealized_profit, p.position_amount.abs() * p.mark_price))
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
        let decimals = self.price_decimals.read().ok();
        let mut out: Vec<BotViewSnapshot> = order
            .into_iter()
            .filter_map(|sym| {
                let v = map.get(&sym)?;
                let history = hist.as_ref().and_then(|h| h.get(&sym).cloned());
                let price_decimals = decimals.as_ref().and_then(|d| d.get(&sym).copied());
                Some(BotViewSnapshot {
                    symbol: v.symbol.clone(),
                    strategy: v.strategy.clone(),
                    status: v.status.clone(),
                    snapshot: v.snapshot.read().ok().and_then(|g| g.clone()),
                    live: v.live.read().ok().and_then(|g| g.clone()),
                    api_position: v.api_position.read().ok().and_then(|g| g.clone()),
                    history,
                    price_decimals,
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

    /// Seed `symbol`'s chart history from 1-second OHLC klines, so the candle
    /// graph is populated on startup instead of building up from blank. Each
    /// kline `(open_time_ms, o, h, l, c)` is expanded into four sub-second
    /// samples (open, high, low, close) within its bucket so the chart's
    /// per-second OHLC aggregation reconstructs the candle shape. No-op if the
    /// symbol already has samples (don't clobber live data).
    pub fn seed_history(&self, symbol: &str, klines: &[(u64, Decimal, Decimal, Decimal, Decimal)]) {
        if let Ok(mut g) = self.history.write() {
            let hist = g.entry(symbol.to_string()).or_default();
            if !hist.samples.is_empty() {
                return;
            }
            for &(t, o, h, l, c) in klines {
                // Order open→high→low→close at +0/+250/+500/+750ms so the bucket
                // keeps open first, close last, and max/min capture the wicks.
                hist.push_sample(t, o);
                hist.push_sample(t + 250, h);
                hist.push_sample(t + 500, l);
                hist.push_sample(t + 750, c);
            }
        }
    }

    /// Advance `symbol`'s chart timeline to `now_ms`, seeding a flat carry-
    /// forward candle for any elapsed quiet second and pruning to the window.
    /// No-op if the symbol has no history yet (nothing to carry forward).
    pub fn advance_history(&self, symbol: &str, now_ms: u64) {
        if let Ok(mut g) = self.history.write()
            && let Some(h) = g.get_mut(symbol)
        {
            h.advance(now_ms);
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
}

/// Cloneable point-in-time view for the renderer.
#[derive(Clone)]
pub struct BotViewSnapshot {
    /// Symbol.
    pub symbol: String,
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
    /// Price decimal places for this symbol (venue tick size), for coin-precision
    /// price rendering. `None` until the bot has spawned at least once.
    pub price_decimals: Option<u32>,
}

/// Account-wide aggregate computed from all bot views.
/// Running totals for bots that have been REMOVED from the dashboard (rotated
/// out by an auto-manager and GC'd). Their realized P&L stays in the exchange
/// wallet, so the account summary must keep counting it — otherwise the bot's
/// totals drift below the API/wallet truth as symbols rotate. Folded in on
/// [`SharedBotState::remove`].
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
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

/// One running bot recorded in the session manifest — enough to re-spawn it and
/// show its last-known state on restart. Per-bot PnL itself resumes separately
/// from the per-symbol snapshot files.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionBot {
    pub symbol: String,
    pub strategy: String,
    /// Lifecycle tag at save time (`on` / `off` / …), for display continuity.
    pub status: String,
}

/// Persisted manager-level session state, written to `<session_dir>/session.json`.
/// Restored on restart (unless `--clear`) so the account summary stays continuous
/// (balance baselines + retired totals) and the rotation lineup resumes. Decimals
/// serialize natively via `rust_decimal/serde`.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SessionState {
    /// First wallet balance ever seen — the baseline for "$ since start" / NET.
    pub start_balance: Option<Decimal>,
    /// Starting BNB USDT-value baseline (BNB-fee accounting).
    pub bnb_start_value_usdt: Option<Decimal>,
    /// Running totals of bots rotated out + GC'd (account summary keeps these).
    pub retired: RetiredTotals,
    /// Accumulated ACCOUNT uptime in seconds across all sessions. On restart the
    /// uptime timer resumes from this offset so the `$/hour` rate stays correct
    /// (it divides cumulative NET by cumulative uptime, not just this session's).
    #[serde(default)]
    pub uptime_secs: u64,
    /// Bots that were running at save time (symbol + strategy + status).
    pub roster: Vec<SessionBot>,
}

/// Manifest file name under the session dir.
pub const SESSION_FILE: &str = "session.json";

/// Load the session manifest from `<dir>/session.json`, or `None` if absent /
/// unreadable / malformed (treated as a cold start — never fatal).
pub fn load_session(dir: &std::path::Path) -> Option<SessionState> {
    let path = dir.join(SESSION_FILE);
    let bytes = std::fs::read(&path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Write the session manifest to `<dir>/session.json`, creating `dir` if needed.
pub fn save_session(dir: &std::path::Path, state: &SessionState) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let json =
        serde_json::to_string_pretty(state).map_err(|e| std::io::Error::other(e.to_string()))?;
    std::fs::write(dir.join(SESSION_FILE), json)
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
                // A rotated bot still lingering in the list is already "done" —
                // surface it in the retired line immediately (not only after GC
                // folds it into RetiredTotals). Display-only: its P&L is already
                // in the grand total above, so this never double-counts, and the
                // count/total are continuous when GC later folds it in.
                if matches!(v.status, BotStatus::Rotated) {
                    a.retired_net += r.net.0;
                    a.retired_count += 1;
                }
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
                // Only mark a position that the venue still holds. A flat /
                // rotated bot has no unrealized — its stale live tap would
                // otherwise contribute a bogus mark.
                if !api.position_amount.is_zero()
                    && let (Some(r), Some(lv)) = (&v.snapshot, &v.live)
                {
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
    // A zero / invalid mark (e.g. a flat or just-rotated position whose
    // positionRisk reports mark=0) would turn the (mark − last_mid) drift into
    // −last_mid × size — a huge bogus "unrealized". Skip the adjustment and fall
    // back to the mid-marked value when the mark isn't usable.
    if mark_price <= Decimal::ZERO || live.last_mid <= Decimal::ZERO {
        return mid_unrealized;
    }
    mid_unrealized + live.position_size * (mark_price - live.last_mid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advance_seeds_one_flat_candle_per_quiet_second() {
        let mut h = PriceHistory::default();
        h.push_sample(1_000, Decimal::from(100)); // a real sample in second 1

        // Several ticks across second 2 with no price change → exactly one flat.
        h.advance(2_100);
        h.advance(2_400);
        h.advance(2_900);
        let in_sec2 = h.samples.iter().filter(|(t, _)| t / 1000 == 2).count();
        assert_eq!(in_sec2, 1, "one flat candle seeded for the quiet second");
        let (ts, px) = *h.samples.iter().find(|(t, _)| t / 1000 == 2).unwrap();
        assert_eq!(ts, 2_000, "flat seeded at the second boundary (open)");
        assert_eq!(
            px,
            Decimal::from(100),
            "flat carries the prior close forward"
        );

        // A real sample later in a second is NOT overwritten by a flat seed.
        h.push_sample(3_500, Decimal::from(101));
        h.advance(3_900);
        let in_sec3: Vec<_> = h.samples.iter().filter(|(t, _)| t / 1000 == 3).collect();
        assert_eq!(in_sec3.len(), 1);
        assert_eq!(in_sec3[0].1, Decimal::from(101));
    }

    #[test]
    fn advance_prunes_candles_older_than_the_window() {
        let mut h = PriceHistory::default();
        h.push_sample(1_000, Decimal::from(50));
        // Jump far past the window — the old candle must be pruned out.
        let far = 1_000 + PriceHistory::WINDOW_MS + 10_000;
        h.advance(far);
        assert!(
            h.samples
                .iter()
                .all(|(t, _)| *t >= far - PriceHistory::WINDOW_MS),
            "samples older than the window are dropped"
        );
    }

    #[test]
    fn advance_noop_without_history() {
        let mut h = PriceHistory::default();
        h.advance(5_000); // nothing to carry forward
        assert!(h.samples.is_empty());
    }
}
