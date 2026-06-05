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
    let mut max_sim_duration = 0u64;
    let mut total_events = 0u64;
    let mut total_fills = 0u64;
    // Skim aggregation: sum across symbols. Note base_stacked sums across
    // symbols on the assumption all skim runs accumulate the SAME base
    // asset (e.g. BTC). For mixed-asset skim portfolios this aggregate is
    // misleading — caller should use per_symbol[s].base_stacked instead.
    let mut total_skim_count = 0u64;
    let mut total_skim_usdt = Decimal::ZERO;
    let mut total_base_stacked = Decimal::ZERO;
    let mut total_perp_balance = Decimal::ZERO;
    let mut total_base_value = Decimal::ZERO;
    let mut total_buy_volume = Decimal::ZERO;
    let mut total_sell_volume = Decimal::ZERO;
    let mut total_buy_fills = 0u64;
    let mut total_sell_fills = 0u64;
    let mut max_peak_position = Decimal::ZERO;
    let mut max_peak_long = Decimal::ZERO;
    let mut max_peak_short = Decimal::ZERO;
    // Mean inventory + full/partial fills sum across symbols (mean = total
    // typical notional deployed across the portfolio; peak stays a MAX).
    let mut total_mean_position = Decimal::ZERO;
    let mut total_full_fills = 0u64;
    let mut total_partial_fills = 0u64;
    let mut total_liquidations = 0u64;
    // Peak fills/min is a per-symbol burst rate; the portfolio's worst minute
    // is the MAX single-symbol burst, not the sum (symbols rarely burst in
    // lockstep), consistent with peak_position_usdt.
    let mut max_peak_fills_per_min = 0u64;
    // Rejections sum across symbols (counts, unlike the peak rate).
    let mut total_rejected_orders = 0u64;
    let mut bases: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for r in reports {
        realized += r.realized.0;
        unrealized += r.unrealized.0;
        fees += r.fees.0;
        funding += r.funding.0;
        net += r.net.0;
        max_runtime = max_runtime.max(r.runtime_secs);
        max_sim_duration = max_sim_duration.max(r.sim_duration_secs);
        total_events += r.events_processed;
        total_fills += r.fills_emitted;
        total_skim_count += r.skim_count;
        total_skim_usdt += r.skim_total_usdt.0;
        total_base_stacked += r.base_stacked.0;
        total_perp_balance += r.final_perp_balance.0;
        total_base_value += r.final_base_value.0;
        total_buy_volume += r.buy_volume_usdt.0;
        total_sell_volume += r.sell_volume_usdt.0;
        total_buy_fills += r.buy_fills;
        total_sell_fills += r.sell_fills;
        // Peak across symbols is the MAX, not the sum (caps are
        // per-symbol; summing implies all peaked simultaneously which
        // is misleading for capital-deployed reasoning).
        if r.peak_position_usdt.0 > max_peak_position {
            max_peak_position = r.peak_position_usdt.0;
        }
        max_peak_long = max_peak_long.max(r.peak_long_usdt.0);
        max_peak_short = max_peak_short.max(r.peak_short_usdt.0);
        total_mean_position += r.mean_position_usdt.0;
        total_full_fills += r.full_fills;
        total_partial_fills += r.partial_fills;
        total_liquidations += r.liquidations;
        max_peak_fills_per_min = max_peak_fills_per_min.max(r.peak_fills_per_min);
        total_rejected_orders += r.rejected_orders;
        if !r.base_asset.is_empty() {
            bases.insert(r.base_asset.as_str());
        }
    }
    // Aggregate base_asset label: empty if no skim, single name if all
    // symbols share a base, "MIXED" if heterogeneous (in which case the
    // summed base_stacked is meaningless — see struct docs).
    let aggregate_base = match bases.len() {
        0 => String::new(),
        1 => bases.iter().next().unwrap().to_string(),
        _ => "MIXED".to_string(),
    };
    PaperReport {
        schema_version: SCHEMA_VERSION,
        realized: Notional(realized),
        unrealized: Notional(unrealized),
        fees: Notional(fees),
        funding: Notional(funding),
        net: Notional(net),
        runtime_secs: max_runtime,
        sim_duration_secs: max_sim_duration,
        events_processed: total_events,
        fills_emitted: total_fills,
        risk_state: None,
        skim_count: total_skim_count,
        skim_total_usdt: Notional(total_skim_usdt),
        base_stacked: Notional(total_base_stacked),
        final_perp_balance: Notional(total_perp_balance),
        final_base_value: Notional(total_base_value),
        base_asset: aggregate_base,
        buy_volume_usdt: Notional(total_buy_volume),
        sell_volume_usdt: Notional(total_sell_volume),
        buy_fills: total_buy_fills,
        sell_fills: total_sell_fills,
        peak_position_usdt: Notional(max_peak_position),
        peak_long_usdt: Notional(max_peak_long),
        peak_short_usdt: Notional(max_peak_short),
        mean_position_usdt: Notional(total_mean_position),
        full_fills: total_full_fills,
        partial_fills: total_partial_fills,
        liquidations: total_liquidations,
        peak_fills_per_min: max_peak_fills_per_min,
        rejected_orders: total_rejected_orders,
    }
}
