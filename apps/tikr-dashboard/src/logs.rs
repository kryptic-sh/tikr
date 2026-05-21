//! Per-symbol log capture for the TUI.
//!
//! Custom tracing Layer that walks each event's span chain looking for a
//! `symbol` field and appends the formatted line to that symbol's ring
//! buffer. Events without a symbol span are dropped (TUI doesn't need
//! account-wide noise).

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use tracing::field::Visit;
use tracing::{Event, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

/// Max log lines kept per symbol in the ring buffer.
pub const LOG_RING_SIZE: usize = 1000;

/// Key used by [`LogLayer`] for events that have no `symbol` span in
/// their chain. These come from library tasks spawned with
/// `tokio::spawn` from inside `tikr-binance` (user stream, keepalive,
/// reconnect loops) which detach from their parent span. We capture
/// them and merge them into every symbol's view in
/// [`LogStore::snapshot_merged`] so the user sees the full log stream
/// — same as running `run_perp` manually.
pub const SYSTEM_KEY: &str = "_system";

/// One captured log line.
#[derive(Clone)]
pub struct LogLine {
    /// Severity (drives color in the TUI).
    pub level: tracing::Level,
    /// Wall-clock nanoseconds since UNIX epoch — used for merging
    /// system + per-symbol streams in chronological order.
    pub ts_ns: u64,
    /// Formatted `HH:MM:SS` prefix.
    pub ts: String,
    /// Body (message + serialized fields).
    pub body: String,
    /// True when this line came from the global system buffer (no
    /// symbol span). Renderers can dim or tag it.
    pub from_system: bool,
}

/// Shared per-symbol log store. Keyed by `BTCUSDT`-style symbol string.
#[derive(Clone, Default)]
pub struct LogStore {
    inner: Arc<Mutex<HashMap<String, VecDeque<LogLine>>>>,
}

impl LogStore {
    /// Construct a fresh empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a log line to `symbol`'s ring; trims to [`LOG_RING_SIZE`].
    pub fn append(&self, symbol: &str, line: LogLine) {
        if let Ok(mut guard) = self.inner.lock() {
            let buf = guard.entry(symbol.to_string()).or_default();
            buf.push_back(line);
            while buf.len() > LOG_RING_SIZE {
                buf.pop_front();
            }
        }
    }

    /// Snapshot the lines for `symbol` (oldest first).
    pub fn snapshot(&self, symbol: &str) -> Vec<LogLine> {
        match self.inner.lock() {
            Ok(guard) => guard
                .get(symbol)
                .map(|d| d.iter().cloned().collect())
                .unwrap_or_default(),
            Err(_) => Vec::new(),
        }
    }

    /// Snapshot `symbol`'s ring AND the [`SYSTEM_KEY`] ring, merged in
    /// chronological order. This is what the TUI actually shows so the
    /// user sees a full `run_perp`-style log including events from
    /// detached library tasks.
    pub fn snapshot_merged(&self, symbol: &str) -> Vec<LogLine> {
        let mut combined = self.snapshot(symbol);
        combined.extend(self.snapshot(SYSTEM_KEY));
        combined.sort_by_key(|l| l.ts_ns);
        combined
    }
}

/// Tracing layer that routes events into a [`LogStore`].
pub struct LogLayer {
    store: LogStore,
}

impl LogLayer {
    /// Construct a layer that writes into `store`.
    pub fn new(store: LogStore) -> Self {
        Self { store }
    }
}

struct SymbolFinder(Option<String>);

impl Visit for SymbolFinder {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "symbol" {
            self.0 = Some(value.to_string());
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "symbol" && self.0.is_none() {
            self.0 = Some(format!("{value:?}"));
        }
    }
}

struct MessageVisitor(String);

impl Visit for MessageVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.0 = value.to_string();
        } else {
            if !self.0.is_empty() {
                self.0.push(' ');
            }
            self.0.push_str(&format!("{}={value}", field.name()));
        }
    }
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        } else {
            if !self.0.is_empty() {
                self.0.push(' ');
            }
            self.0.push_str(&format!("{}={value:?}", field.name()));
        }
    }
}

impl<S> Layer<S> for LogLayer
where
    S: Subscriber + for<'l> LookupSpan<'l>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        // Walk span chain looking for a `symbol` field.
        let mut sym = SymbolFinder(None);
        if let Some(scope) = ctx.event_scope(event) {
            for span in scope.from_root() {
                if let Some(ext) = span.extensions().get::<SymbolMarker>() {
                    sym.0 = Some(ext.0.clone());
                    break;
                }
            }
        }
        let mut msg = MessageVisitor(String::new());
        event.record(&mut msg);

        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let secs = now_ns / 1_000_000_000;
        let ts = format!(
            "{:02}:{:02}:{:02}",
            (secs / 3600) % 24,
            (secs / 60) % 60,
            secs % 60,
        );
        let (key, from_system) = match sym.0 {
            Some(s) => (s, false),
            None => (SYSTEM_KEY.to_string(), true),
        };
        self.store.append(
            &key,
            LogLine {
                level: *event.metadata().level(),
                ts_ns: now_ns,
                ts,
                body: msg.0,
                from_system,
            },
        );
    }

    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: Context<'_, S>,
    ) {
        // Cache the span's `symbol` field on first sight so on_event
        // doesn't re-walk + re-format on every log line.
        let mut sym = SymbolFinder(None);
        attrs.record(&mut sym);
        if let Some(s) = sym.0
            && let Some(span) = ctx.span(id)
        {
            span.extensions_mut().insert(SymbolMarker(s));
        }
    }
}

#[derive(Clone)]
struct SymbolMarker(String);
