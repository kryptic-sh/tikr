//! Backtest runner — ties Replay + Strategy + FillSim + PositionTracker
//! together and returns a final P&L report. Phase 1 stub.

use crate::{fill_sim::FillSim, pnl::PnLReport, replay::Replay};
use tikr_strategy::Strategy;

/// Run `strategy` against `replay` with `fill_sim` and return the final report.
///
/// Phase 1 stub — wiring lands with the golden regression test (#15).
pub async fn run<R, S>(replay: R, strategy: S, fill_sim: FillSim) -> PnLReport
where
    R: Replay,
    S: Strategy,
{
    let _ = (replay, strategy, fill_sim);
    todo!("issue #15: drive replay → strategy.on_event → fill_sim → tracker → report")
}
