//! Lightweight metric registry — counters + gauges, Prometheus text format export.
//!
//! # v0 scope
//!
//! Registry + text format only. The HTTP `/metrics` endpoint is deliberately
//! deferred to a follow-up issue to keep this change small. Operators wanting
//! Prometheus scrape today integrate [`MetricRegistry::prometheus_text`] into
//! a custom endpoint (e.g. an existing `hyper`/`axum` server in their
//! deployment harness).
//!
//! # Usage
//!
//! ```no_run
//! use tikr_paper::MetricRegistry;
//! use std::sync::atomic::Ordering;
//!
//! let reg = MetricRegistry::new();
//! reg.counter("paper_fills_total").fetch_add(1, Ordering::Relaxed);
//! reg.gauge("paper_position_size").store(42, Ordering::Relaxed);
//! println!("{}", reg.prometheus_text());
//! ```

use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// Concurrent counter + gauge registry. Cheap to share across tasks via
/// `Arc<MetricRegistry>`. Counter / gauge handles are themselves `Arc`-wrapped
/// atomics so callers can cache them and avoid the `DashMap` lookup on every
/// increment.
#[derive(Default)]
pub struct MetricRegistry {
    counters: DashMap<&'static str, Arc<AtomicU64>>,
    gauges: DashMap<&'static str, Arc<AtomicI64>>,
}

impl MetricRegistry {
    /// Construct an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get-or-create the named counter. Cheap on repeat access.
    pub fn counter(&self, name: &'static str) -> Arc<AtomicU64> {
        self.counters
            .entry(name)
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone()
    }

    /// Get-or-create the named gauge.
    pub fn gauge(&self, name: &'static str) -> Arc<AtomicI64> {
        self.gauges
            .entry(name)
            .or_insert_with(|| Arc::new(AtomicI64::new(0)))
            .clone()
    }

    /// Render the registry in Prometheus text exposition format. Output is
    /// sorted by metric name (counters first, then gauges) for deterministic
    /// scraping + test assertions.
    pub fn prometheus_text(&self) -> String {
        let mut out = String::new();
        let mut counters: Vec<_> = self
            .counters
            .iter()
            .map(|kv| (*kv.key(), kv.value().load(Ordering::Relaxed)))
            .collect();
        counters.sort_by_key(|(k, _)| *k);
        for (name, value) in counters {
            out.push_str(&format!("# TYPE {name} counter\n{name} {value}\n"));
        }
        let mut gauges: Vec<_> = self
            .gauges
            .iter()
            .map(|kv| (*kv.key(), kv.value().load(Ordering::Relaxed)))
            .collect();
        gauges.sort_by_key(|(k, _)| *k);
        for (name, value) in gauges {
            out.push_str(&format!("# TYPE {name} gauge\n{name} {value}\n"));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_inc_and_format() {
        let reg = MetricRegistry::new();
        reg.counter("test_counter").fetch_add(5, Ordering::Relaxed);
        let text = reg.prometheus_text();
        assert!(
            text.contains("# TYPE test_counter counter"),
            "missing TYPE line; got:\n{text}"
        );
        assert!(
            text.contains("test_counter 5"),
            "missing value line; got:\n{text}"
        );
    }

    #[test]
    fn gauge_set_and_format() {
        let reg = MetricRegistry::new();
        reg.gauge("test_gauge").store(-42, Ordering::Relaxed);
        let text = reg.prometheus_text();
        assert!(
            text.contains("# TYPE test_gauge gauge"),
            "missing TYPE line; got:\n{text}"
        );
        assert!(
            text.contains("test_gauge -42"),
            "missing value line; got:\n{text}"
        );
    }
}
