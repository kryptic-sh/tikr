//! Paper-trading runner — live `Venue` → `Strategy` → `FillSim` → `PaperReport`.

use crate::report::PaperReport;
use crate::state;
use futures::StreamExt;
use std::path::PathBuf;
use std::time::Instant;
use tikr_backtest::fill_sim::FillSim;
use tikr_backtest::pnl::PositionTracker;
use tikr_core::{Decimal, MarketEvent, Price, Snapshot, Symbol, Timestamp};
use tikr_strategy::{Strategy, StrategyContext};
use tikr_venue::Venue;
use tokio::sync::watch;
use tracing::{info, warn};
use uuid::Uuid;

/// Runtime configuration for [`run`].
#[derive(Debug, Clone)]
pub struct RunnerConfig {
    /// Directory where state snapshots land. Default `./paper_state`.
    pub state_dir: PathBuf,
    /// Snapshot cadence in events. Default 100.
    pub snapshot_every_n_events: u32,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            state_dir: PathBuf::from("./paper_state"),
            snapshot_every_n_events: 100,
        }
    }
}

/// Drive `strategy` against `venue.subscribe(symbol)`, returning the final
/// [`PaperReport`] when the stream ends or `shutdown` fires.
///
/// # v0 limitations
///
/// - `StrategyContext.recent_fills` is always empty
/// - `StrategyContext.open_quotes` is always empty
/// - Single-symbol per call
/// - `last_mid` is zero if no `BookUpdate` ever arrived
pub async fn run<V, S>(
    venue: V,
    mut strategy: S,
    mut fill_sim: FillSim,
    symbol: Symbol,
    mut shutdown: watch::Receiver<bool>,
    config: RunnerConfig,
) -> PaperReport
where
    V: Venue,
    S: Strategy,
{
    let mut tracker = PositionTracker::new(symbol.clone());
    let mut current_book = empty_snapshot(&symbol);
    let mut last_mid = Price(Decimal::ZERO);
    let mut events_processed: u64 = 0;
    let mut fills_emitted: u64 = 0;
    let started = Instant::now();
    let run_id = make_run_id(&symbol);

    info!(symbol = %symbol.base.0, run_id = %run_id, "paper runner starting");

    // First-connect: if subscribe fails synchronously, return a zero report.
    let mut stream = match venue.subscribe(&symbol).await {
        Ok(s) => s,
        Err(e) => {
            warn!("subscribe failed: {}", e);
            return finalize(&tracker, last_mid, started, 0, 0);
        }
    };

    loop {
        tokio::select! {
            ev = stream.next() => {
                let Some(event) = ev else {
                    info!("event stream ended");
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
                };

                let actions = strategy.on_event(&ctx, &event);
                for action in actions {
                    fill_sim.on_action(action, ts);
                }
                let fills = fill_sim.on_market_event(&event, ts);
                for fill in fills {
                    info!(price = %fill.price.0, size = %fill.size.0, side = ?fill.side, "fill");
                    tracker.apply(&fill);
                    fills_emitted += 1;
                }
                events_processed += 1;

                if events_processed > 0
                    && config.snapshot_every_n_events > 0
                    && events_processed.is_multiple_of(config.snapshot_every_n_events as u64)
                {
                    let report = finalize(&tracker, last_mid, started, events_processed, fills_emitted);
                    if let Err(e) = state::write_snapshot(&report, &config.state_dir, &run_id) {
                        warn!("snapshot write failed: {}", e);
                    }
                }
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("shutdown signal received");
                    break;
                }
            }
        }
    }

    // Drain pending FillSim actions and emit strategy on_shutdown actions.
    let pos = tracker.snapshot();
    let now_ts = current_book.ts;
    let ctx = StrategyContext {
        symbol: &symbol,
        now: now_ts,
        position: &pos,
        recent_fills: &[],
        latest_book: &current_book,
        open_quotes: &[],
    };
    let shutdown_actions = strategy.on_shutdown(&ctx);
    for action in shutdown_actions {
        fill_sim.on_action(action, now_ts);
    }

    let report = finalize(&tracker, last_mid, started, events_processed, fills_emitted);
    if let Err(e) = state::write_snapshot(&report, &config.state_dir, &run_id) {
        warn!("final snapshot write failed: {}", e);
    }
    info!(
        runtime_secs = report.runtime_secs,
        events = events_processed,
        fills = fills_emitted,
        net = %report.net.0,
        "paper runner done"
    );
    report
}

fn finalize(
    tracker: &PositionTracker,
    last_mid: Price,
    started: Instant,
    events_processed: u64,
    fills_emitted: u64,
) -> PaperReport {
    let base = tracker.report(last_mid);
    PaperReport {
        realized: base.realized,
        unrealized: base.unrealized,
        fees: base.fees,
        funding: base.funding,
        net: base.net,
        runtime_secs: started.elapsed().as_secs(),
        events_processed,
        fills_emitted,
    }
}

fn empty_snapshot(symbol: &Symbol) -> Snapshot {
    Snapshot {
        symbol: symbol.clone(),
        bids: Vec::new(),
        asks: Vec::new(),
        ts: Timestamp(0),
    }
}

fn event_ts(event: &MarketEvent) -> Timestamp {
    match event {
        MarketEvent::BookUpdate { snapshot } => snapshot.ts,
        MarketEvent::Trade { ts, .. } => *ts,
        MarketEvent::Fill(f) => f.ts,
        MarketEvent::Heartbeat { ts } => *ts,
    }
}

fn make_run_id(symbol: &Symbol) -> String {
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let uuid_short = Uuid::new_v4().simple().to_string();
    let uuid_short = &uuid_short[..8];
    let base = symbol.base.0.to_string();
    format!("{base}_{now_secs}_{uuid_short}")
}

// --- mock Venue + tests below ---

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use futures::stream::{self, BoxStream};
    use std::sync::Mutex;
    use tempfile::TempDir;
    use tikr_backtest::fill_sim::{FillSim, FillSimConfig, VenueFees};
    use tikr_core::{Asset, Level, Size, VenueId};
    use tikr_strategy::{NaiveGrid, NaiveGridConfig, Strategy};
    use tikr_venue::{QuoteId, QuoteIntent, Venue, VenueError};
    use tokio::sync::watch;

    struct MockVenue {
        events: Mutex<Option<Vec<MarketEvent>>>,
        infinite: bool,
    }

    impl MockVenue {
        fn finite(events: Vec<MarketEvent>) -> Self {
            Self {
                events: Mutex::new(Some(events)),
                infinite: false,
            }
        }
        fn infinite_heartbeats() -> Self {
            Self {
                events: Mutex::new(Some(Vec::new())),
                infinite: true,
            }
        }
    }

    #[async_trait]
    impl Venue for MockVenue {
        fn id(&self) -> &str {
            "mock"
        }
        async fn snapshot(&self, _symbol: &Symbol) -> Result<Snapshot, VenueError> {
            unimplemented!()
        }
        async fn subscribe(
            &self,
            _symbol: &Symbol,
        ) -> Result<BoxStream<'_, MarketEvent>, VenueError> {
            if self.infinite {
                // Yield between items so the select! loop can poll the
                // shutdown branch; a purely-synchronous `repeat_with` would
                // hog the executor and the shutdown task would never run.
                let s = stream::unfold((), |()| async {
                    tokio::task::yield_now().await;
                    Some((MarketEvent::Heartbeat { ts: Timestamp(0) }, ()))
                });
                Ok(Box::pin(s))
            } else {
                let events = self.events.lock().unwrap().take().unwrap_or_default();
                Ok(Box::pin(stream::iter(events)))
            }
        }
        async fn quote(&self, _intent: QuoteIntent) -> Result<QuoteId, VenueError> {
            unimplemented!()
        }
        async fn requote(&self, _id: QuoteId, _intent: QuoteIntent) -> Result<(), VenueError> {
            unimplemented!()
        }
        async fn cancel(&self, _id: QuoteId) -> Result<(), VenueError> {
            unimplemented!()
        }
        async fn cancel_all(&self, _symbol: &Symbol) -> Result<(), VenueError> {
            unimplemented!()
        }
        async fn position(&self, _symbol: &Symbol) -> Result<tikr_core::Position, VenueError> {
            unimplemented!()
        }
        async fn fills_since(&self, _since_ts: u64) -> Result<Vec<tikr_core::Fill>, VenueError> {
            unimplemented!()
        }
    }

    fn make_symbol() -> Symbol {
        Symbol {
            base: Asset::new("BTC"),
            quote: Asset::new("USDC"),
            venue: VenueId::new("mock"),
        }
    }

    fn naive_grid() -> NaiveGrid {
        NaiveGrid::new(NaiveGridConfig {
            levels_per_side: 1,
            base_spread_bps: 50,
            level_step_bps: 10,
            size_per_quote: Size(Decimal::from(1)),
            min_requote_interval_ms: 100_000,
        })
    }

    fn fill_sim() -> FillSim {
        FillSim::new(FillSimConfig {
            submit_latency_ms: 0,
            cancel_latency_ms: 0,
            fees: VenueFees {
                maker_bps: 0,
                taker_bps: 0,
            },
        })
    }

    fn test_config(state_dir: PathBuf) -> RunnerConfig {
        RunnerConfig {
            state_dir,
            snapshot_every_n_events: 100,
        }
    }

    #[tokio::test]
    async fn runner_handles_empty_event_stream() {
        let temp = TempDir::new().unwrap();
        let venue = MockVenue::finite(Vec::new());
        let (_tx, rx) = watch::channel(false);
        let report = run(
            venue,
            naive_grid(),
            fill_sim(),
            make_symbol(),
            rx,
            test_config(temp.path().into()),
        )
        .await;
        assert_eq!(report.events_processed, 0);
        assert_eq!(report.fills_emitted, 0);
    }

    #[tokio::test]
    async fn runner_shutdown_signal_exits_promptly() {
        let temp = TempDir::new().unwrap();
        let venue = MockVenue::infinite_heartbeats();
        let (tx, rx) = watch::channel(false);
        let cfg = test_config(temp.path().into());

        // Trigger shutdown after a brief delay.
        let shutdown_handle = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let _ = tx.send(true);
        });

        let report = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            run(venue, naive_grid(), fill_sim(), make_symbol(), rx, cfg),
        )
        .await
        .expect("runner did not exit within 2s");

        shutdown_handle.await.unwrap();
        // Should have processed at least some heartbeats before shutdown.
        assert!(report.events_processed > 0);
    }

    #[tokio::test]
    async fn snapshot_writes_to_disk() {
        let temp = TempDir::new().unwrap();
        // Build 100+ BookUpdate events to trigger one snapshot write.
        let symbol = make_symbol();
        let events: Vec<MarketEvent> = (0..120u64)
            .map(|i| MarketEvent::BookUpdate {
                snapshot: Snapshot {
                    symbol: symbol.clone(),
                    bids: vec![Level {
                        price: Price(Decimal::from(100)),
                        size: Size(Decimal::from(1)),
                    }],
                    asks: vec![Level {
                        price: Price(Decimal::from(101)),
                        size: Size(Decimal::from(1)),
                    }],
                    ts: Timestamp(i * 1_000_000),
                },
            })
            .collect();

        let venue = MockVenue::finite(events);
        let (_tx, rx) = watch::channel(false);
        let cfg = test_config(temp.path().into());
        let report = run(venue, naive_grid(), fill_sim(), symbol, rx, cfg).await;

        assert_eq!(report.events_processed, 120);
        // Check that at least one snapshot was written to disk.
        let entries: Vec<_> = std::fs::read_dir(temp.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("json"))
            .collect();
        assert!(
            !entries.is_empty(),
            "expected at least one JSON snapshot file"
        );

        // Verify the file parses back.
        let path = entries[0].path();
        let txt = std::fs::read_to_string(&path).unwrap();
        let parsed: PaperReport = serde_json::from_str(&txt).unwrap();
        assert!(parsed.events_processed >= 100);
    }
}
