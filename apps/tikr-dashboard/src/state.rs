//! Dashboard-side aggregate state shared between the supervisors and
//! the TUI render loop.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use rust_decimal::Decimal;
use tikr_paper::{BotHandle, PaperReport};
use tokio::sync::watch;

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
}

impl BotStatus {
    /// Single-letter compact tag for the tabs header.
    pub fn tag(&self) -> &'static str {
        match self {
            Self::Starting => "···",
            Self::Running => "ON",
            Self::Crashed(_) => "X",
            Self::Restarting(_) => "↻",
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
    /// Shutdown sender for the current incarnation (None between restarts).
    pub shutdown_tx: Option<watch::Sender<bool>>,
}

impl BotView {
    /// Read the current snapshot, if any.
    #[allow(dead_code)]
    pub fn snapshot(&self) -> Option<PaperReport> {
        self.snapshot.read().ok().and_then(|g| g.clone())
    }
}

/// Shared state keyed by symbol. Wrap in `Arc` for cross-task sharing.
#[derive(Clone)]
pub struct SharedBotState {
    inner: Arc<Mutex<HashMap<String, BotView>>>,
    /// Ordered list of symbols — the TUI's tab order. Kept in sync with
    /// the order bots were inserted.
    order: Arc<Mutex<Vec<String>>>,
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
            order: Arc::new(Mutex::new(Vec::new())),
        }
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
        order
            .into_iter()
            .filter_map(|sym| {
                let v = map.get(&sym)?;
                Some(BotViewSnapshot {
                    symbol: v.symbol.clone(),
                    label: v.label.clone(),
                    strategy: v.strategy.clone(),
                    status: v.status.clone(),
                    snapshot: v.snapshot.read().ok().and_then(|g| g.clone()),
                })
            })
            .collect()
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
    /// Latest live snapshot, if any.
    pub snapshot: Option<PaperReport>,
}

/// Account-wide aggregate computed from all bot views.
#[derive(Default)]
pub struct AccountAggregate {
    /// Σ realized PnL across all bots.
    pub realized: Decimal,
    /// Σ unrealized PnL.
    pub unrealized: Decimal,
    /// Σ fees paid.
    pub fees: Decimal,
    /// Σ NET (realized + unrealized − fees + funding).
    pub net: Decimal,
    /// Σ events processed.
    pub events: u64,
    /// Σ fills emitted.
    pub fills: u64,
    /// Count of bots currently in `Running` state.
    pub running_count: usize,
    /// Count of bots in `Crashed` state.
    pub crashed_count: usize,
    /// Count of bots in `Restarting` state.
    pub restarting_count: usize,
}

impl AccountAggregate {
    /// Compute from a snapshot of bot views.
    pub fn compute(views: &[BotViewSnapshot]) -> Self {
        let mut a = AccountAggregate::default();
        for v in views {
            match v.status {
                BotStatus::Running => a.running_count += 1,
                BotStatus::Crashed(_) => a.crashed_count += 1,
                BotStatus::Restarting(_) => a.restarting_count += 1,
                _ => {}
            }
            if let Some(ref r) = v.snapshot {
                a.realized += r.realized.0;
                a.unrealized += r.unrealized.0;
                a.fees += r.fees.0;
                a.net += r.net.0;
                a.events = a.events.saturating_add(r.events_processed);
                a.fills = a.fills.saturating_add(r.fills_emitted);
            }
        }
        a
    }
}
