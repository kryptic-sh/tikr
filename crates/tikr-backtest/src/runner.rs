//! Backtest runner — ties Replay + Strategy + FillSim + PositionTracker
//! together and returns a final P&L report.

use crate::{
    fill_sim::FillSim,
    pnl::{PnLReport, PositionTracker},
    replay::Replay,
};
use tikr_core::{Decimal, MarketEvent, Price, Snapshot, Symbol, Timestamp};
use tikr_strategy::{Strategy, StrategyContext};

/// Drive `replay` end-to-end against `strategy` and `fill_sim`, accumulating
/// fills into a [`PositionTracker`] for `symbol`. Returns the final report
/// marked at the last observed mid-price.
///
/// Phase 1 v0 limitations:
/// - `StrategyContext.recent_fills` is always empty (no rolling fill buffer yet)
/// - `StrategyContext.open_quotes` is always empty (FillSim doesn't yet expose open quotes externally)
/// - Single-symbol per run; multi-symbol = run N times in parallel
/// - Final report uses last observed book mid; if no `BookUpdate` ever arrived, unrealized = 0
pub async fn run<R, S>(
    mut replay: R,
    mut strategy: S,
    mut fill_sim: FillSim,
    symbol: Symbol,
) -> PnLReport
where
    R: Replay,
    S: Strategy,
{
    let mut tracker = PositionTracker::new(symbol.clone());
    let mut current_book = Snapshot {
        symbol: symbol.clone(),
        bids: Vec::new(),
        asks: Vec::new(),
        ts: Timestamp(0),
    };
    let mut last_mid = Price(Decimal::ZERO);

    loop {
        let Some(event) = replay.next().await else {
            break;
        };
        let ts = event_ts(&event);

        if let MarketEvent::BookUpdate { snapshot } = &event {
            current_book = snapshot.clone();
            if let (Some(b), Some(a)) = (snapshot.bids.first(), snapshot.asks.first()) {
                last_mid = Price((b.price.0 + a.price.0) / Decimal::from(2));
            }
        }

        let pos = tracker.snapshot();
        let ctx = StrategyContext {
            symbol: &symbol,
            now: ts,
            position: &pos,
            recent_fills: &[],
            latest_book: &current_book,
            open_quotes: &[],
            recent_liqs: &[],
        };

        let actions = strategy.on_event(&ctx, &event);
        for action in actions {
            fill_sim.on_action(action, ts);
        }
        let fills = fill_sim.on_market_event(&event, ts);
        for fill in fills {
            tracker.apply(&fill);
        }
    }

    tracker.report(last_mid)
}

fn event_ts(event: &MarketEvent) -> Timestamp {
    match event {
        MarketEvent::BookUpdate { snapshot } => snapshot.ts,
        MarketEvent::Trade { ts, .. } => *ts,
        MarketEvent::Fill(f) => f.ts,
        MarketEvent::Heartbeat { ts } => *ts,
    }
}
