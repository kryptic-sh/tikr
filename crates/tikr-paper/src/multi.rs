//! Multi-symbol coordination — spawn N runners, aggregate per-symbol
//! [`PaperReport`]s.
//!
//! Each [`MultiSymbolRun`] is a pre-constructed runner future (typically a
//! [`crate::runner::run`] / [`crate::runner::run_with_resume`] call boxed +
//! pinned). [`run_multi`] drives them concurrently via [`futures::future::join_all`]
//! and folds the results into a per-symbol map plus a header-style aggregate
//! [`PaperReport`].

use crate::report::{PaperReport, SCHEMA_VERSION};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use tikr_core::{Decimal, Notional, Symbol};
use tokio::sync::watch;

/// One symbol's worth of runner future plus bookkeeping for [`run_multi`].
pub struct MultiSymbolRun {
    /// The symbol this run belongs to.
    pub symbol: Symbol,
    /// The pre-constructed runner future. Typically built by boxing
    /// [`crate::runner::run`] / [`crate::runner::run_with_resume`].
    pub future: Pin<Box<dyn Future<Output = PaperReport> + Send>>,
}

/// Aggregate report across all symbols.
///
/// `per_symbol` carries each runner's raw [`PaperReport`] keyed by [`Symbol`].
/// `sum` is a synthetic aggregate: P&L fields are summed, counters are summed,
/// `runtime_secs` is the max (wall-clock parallel runtime). `risk_state` on the
/// aggregate is always `None` — per-symbol risk lives in `per_symbol[sym].risk_state`.
#[derive(Debug, Clone)]
pub struct MultiPaperReport {
    /// Per-symbol final reports.
    pub per_symbol: HashMap<Symbol, PaperReport>,
    /// Aggregate across all symbols (see struct docs for fold semantics).
    pub sum: PaperReport,
}

/// Drive N per-symbol runner futures concurrently. Shared shutdown lives in
/// the caller (passed through to each future at construction time).
///
/// `_shutdown` is accepted for API symmetry / extension; each future already
/// holds its own clone of the shutdown receiver.
pub async fn run_multi(
    runs: Vec<MultiSymbolRun>,
    _shutdown: watch::Receiver<bool>,
) -> MultiPaperReport {
    let symbols: Vec<Symbol> = runs.iter().map(|r| r.symbol.clone()).collect();
    let futures: Vec<_> = runs.into_iter().map(|r| r.future).collect();
    let reports: Vec<PaperReport> = futures::future::join_all(futures).await;

    let mut per_symbol = HashMap::new();
    for (sym, rep) in symbols.into_iter().zip(reports.iter()) {
        per_symbol.insert(sym, rep.clone());
    }

    let sum = aggregate_sum(&reports);
    MultiPaperReport { per_symbol, sum }
}

fn aggregate_sum(reports: &[PaperReport]) -> PaperReport {
    let mut realized = Decimal::ZERO;
    let mut unrealized = Decimal::ZERO;
    let mut fees = Decimal::ZERO;
    let mut funding = Decimal::ZERO;
    let mut net = Decimal::ZERO;
    let mut max_runtime = 0u64;
    let mut total_events = 0u64;
    let mut total_fills = 0u64;
    for r in reports {
        realized += r.realized.0;
        unrealized += r.unrealized.0;
        fees += r.fees.0;
        funding += r.funding.0;
        net += r.net.0;
        max_runtime = max_runtime.max(r.runtime_secs);
        total_events += r.events_processed;
        total_fills += r.fills_emitted;
    }
    PaperReport {
        schema_version: SCHEMA_VERSION,
        realized: Notional(realized),
        unrealized: Notional(unrealized),
        fees: Notional(fees),
        funding: Notional(funding),
        net: Notional(net),
        runtime_secs: max_runtime,
        events_processed: total_events,
        fills_emitted: total_fills,
        risk_state: None,
    }
}
