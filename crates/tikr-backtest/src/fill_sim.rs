//! Simulated fill engine — turns strategy [`Action`]s + market events into
//! [`Fill`]s under a trade-through model with post-only correctness.
//! See [issue #11] for the optimistic fill bias note.
//!
//! [issue #11]: https://github.com/kryptic-sh/tikr/issues/11

use std::collections::HashMap;

use tikr_core::{
    Decimal, Fill, MarketEvent, Notional, Price, Side, Size, Snapshot, Symbol, TimeInForce,
    Timestamp,
};
use tikr_strategy::Action;
use tikr_venue::{OpenOrder, QuoteId, QuoteIntent};

/// Per-venue fee schedule. Negative maker = rebate.
#[derive(Debug, Clone, Copy)]
pub struct VenueFees {
    /// Maker fee in basis points. Negative = rebate paid TO the maker.
    pub maker_bps: i32,
    /// Taker fee in basis points (always positive in practice).
    pub taker_bps: u32,
}

/// Configuration for [`FillSim`].
#[derive(Debug, Clone)]
pub struct FillSimConfig {
    /// Latency between action submission and venue ack, in milliseconds.
    /// Realistic Binance: ~50ms one-way (NA → AWS-Tokyo). Set non-zero
    /// to exercise post-only rejects on fast-moving markets: the book
    /// can move through our intended price between decision and apply.
    pub submit_latency_ms: u64,
    /// Latency between cancel submission and venue ack, in milliseconds.
    pub cancel_latency_ms: u64,
    /// Per-venue fee schedule.
    pub fees: VenueFees,
    /// Hard cap on signed position USDT notional. `None` = unlimited.
    /// When set, Place ops are rejected with a synthetic "margin
    /// insufficient" reason if applying the quote would push
    /// |position| past this cap. Simulates Binance `-2019`.
    pub max_position_notional_usdt: Option<Decimal>,
    /// Per-minute probability that any individual live quote gets
    /// silently dropped, simulating venue-side cancel/expire that the
    /// user_stream WS misses (the live reconciliation loop normally
    /// catches these via `Venue::open_orders`). `0.0` = disabled.
    /// Typical realistic value: `0.005` (0.5% per quote per minute).
    pub silent_cancel_rate_per_min: f64,
    /// Deterministic RNG seed for silent cancellations. Same seed =
    /// same dropped quotes for reproducible backtests.
    pub rng_seed: u64,
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

struct PendingOp {
    scheduled_ts_ns: u64,
    op: Op,
}

enum Op {
    Place {
        intent: QuoteIntent,
        /// Live mode: the venue already returned its own `QuoteId`. Use it
        /// here so `live_quotes_for` exposes venue ids back to the strategy.
        /// `None` in paper mode → FillSim mints a fresh id.
        override_id: Option<QuoteId>,
    },
    Replace {
        id: QuoteId,
        intent: QuoteIntent,
    },
    Cancel(QuoteId),
    CancelAll,
}

struct LiveQuote {
    id: QuoteId,
    symbol: Symbol,
    side: Side,
    price: Price,
    size_remaining: Size,
    /// Aggregate size resting at our price level when we were placed.
    /// Trades at our price level consume this BEFORE consuming our size
    /// (FIFO/price-time priority approximation; cancels are not modeled).
    queue_ahead: Decimal,
    #[allow(dead_code)]
    ts_submitted: Timestamp,
}

/// Per-symbol book aggregates for queue-priority + cancel attribution.
///
/// Stored as `HashMap` rather than `BTreeMap` — the only ordered operations
/// we need are `best_bid` / `best_ask`, which we cache explicitly. Decimal
/// keys make BTreeMap comparisons dominate the profile; HashMap sidesteps
/// that entirely.
#[derive(Default, Clone)]
struct BookState {
    /// Per-level aggregate size on the bid side, keyed by price.
    bids: HashMap<Decimal, Decimal>,
    /// Per-level aggregate size on the ask side, keyed by price.
    asks: HashMap<Decimal, Decimal>,
    /// Cached top-of-book. Refreshed by `set_top` on snapshot rebuild.
    best_bid: Option<Price>,
    best_ask: Option<Price>,
}

impl BookState {
    fn best_bid(&self) -> Option<Price> {
        self.best_bid
    }
    fn best_ask(&self) -> Option<Price> {
        self.best_ask
    }
    fn set_top(&mut self, best_bid: Option<Price>, best_ask: Option<Price>) {
        self.best_bid = best_bid;
        self.best_ask = best_ask;
    }
    fn level_size(&self, side: Side, price: Price) -> Decimal {
        let map = match side {
            Side::Bid => &self.bids,
            Side::Ask => &self.asks,
        };
        map.get(&price.0).copied().unwrap_or(Decimal::ZERO)
    }
    fn decrement_level(&mut self, side: Side, price: Price, amount: Decimal) {
        let map = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        if let Some(v) = map.get_mut(&price.0) {
            *v -= amount;
            if *v <= Decimal::ZERO {
                map.remove(&price.0);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// FillSim
// ---------------------------------------------------------------------------

/// Trade-through fill simulator with configurable latency, post-only
/// correctness, partial fills, and maker-rebate fees.
pub struct FillSim {
    cfg: FillSimConfig,
    pending: Vec<PendingOp>,
    live_quotes: Vec<LiveQuote>,
    book_state: HashMap<Symbol, BookState>,
    /// Intents rejected during the last `apply_pending` pass (post-only
    /// crosses the touch). Paper-mode runner drains this after each
    /// `on_market_event` and routes each entry through
    /// `strategy.on_quote_rejected` so backtests exercise the same
    /// recovery path live mode uses.
    pending_rejections: Vec<(QuoteIntent, String)>,
    /// Signed position notional in USDT per symbol. Positive = long.
    /// Maintained by maker fills (`match_trade`) and taker fills
    /// (`place_or_reject` IOC arm). Used for synthetic margin rejects.
    position_notional: HashMap<Symbol, Decimal>,
    /// xorshift64 state for silent-cancellation rolls.
    rng_state: u64,
    /// Last `on_market_event` timestamp, in nanoseconds. Used to compute
    /// the elapsed window for `silent_cancel_rate_per_min`.
    last_event_ts_ns: Option<u64>,
}

impl FillSim {
    /// Construct a new fill simulator from `cfg`.
    pub fn new(cfg: FillSimConfig) -> Self {
        // xorshift64 cannot start at 0 — degenerate fixed point.
        let rng_state = if cfg.rng_seed == 0 {
            0x9E37_79B9_7F4A_7C15
        } else {
            cfg.rng_seed
        };
        Self {
            cfg,
            pending: Vec::new(),
            live_quotes: Vec::new(),
            book_state: HashMap::new(),
            pending_rejections: Vec::new(),
            position_notional: HashMap::new(),
            rng_state,
            last_event_ts_ns: None,
        }
    }

    /// Drain post-only rejections accumulated since the last call.
    /// Returns `(intent, reason)` pairs the paper-mode runner can pass
    /// straight to `strategy.on_quote_rejected`.
    pub fn drain_rejections(&mut self) -> Vec<(QuoteIntent, String)> {
        std::mem::take(&mut self.pending_rejections)
    }

    /// Snapshot of currently-resting orders for `symbol` as
    /// `(quote_id, intent)` pairs. Mirrors what
    /// [`tikr_strategy::StrategyContext::open_quotes`] expects so the
    /// runner can populate strategy context for fill-aware logic
    /// (e.g. LayeredGrid's ladder roll emits Cancel for specific orders
    /// rather than CancelAll + replay).
    ///
    /// `size_remaining` carries the live remaining size, NOT the original
    /// intent size — important for strategies that look up partial state.
    pub fn live_quotes_for(&self, symbol: &Symbol) -> Vec<(QuoteId, QuoteIntent)> {
        let mut out: Vec<(QuoteId, QuoteIntent)> = self
            .live_quotes
            .iter()
            .filter(|q| &q.symbol == symbol)
            .map(|q| {
                (
                    q.id,
                    QuoteIntent {
                        symbol: q.symbol.clone(),
                        side: q.side,
                        price: q.price,
                        size: q.size_remaining,
                        // Live resting orders are by construction post-only
                        // here (FillSim's IOC/FOK paths return immediately
                        // and never enter live_quotes). Stamp PostOnly so
                        // strategies don't have to special-case.
                        tif: TimeInForce::PostOnly,
                        kind: tikr_core::QuoteKind::Point,
                    },
                )
            })
            .collect();
        // Live mode: include in-flight Place ops that already carry a
        // venue-issued id. They're physically resting on the exchange
        // book even though `apply_pending` hasn't promoted them into
        // `live_quotes` yet. Excluding them makes the strategy think
        // the side is empty between back-to-back fills and trigger a
        // spurious RefillSide — open-order count then balloons past
        // `levels_per_side`. Pure-paper Place ops (override_id=None)
        // stay excluded since the venue doesn't know about them.
        for p in &self.pending {
            if let Op::Place {
                intent,
                override_id: Some(qid),
            } = &p.op
                && &intent.symbol == symbol
            {
                out.push((*qid, intent.clone()));
            }
        }
        out
    }

    /// Schedule a strategy action for venue submission at `now + appropriate_latency_ms`.
    pub fn on_action(&mut self, action: Action, now: Timestamp) {
        let submit_ns = self.cfg.submit_latency_ms.saturating_mul(1_000_000);
        let cancel_ns = self.cfg.cancel_latency_ms.saturating_mul(1_000_000);
        let (scheduled, op) = match action {
            Action::Quote(intent) => (
                now.0.saturating_add(submit_ns),
                Op::Place {
                    intent,
                    override_id: None,
                },
            ),
            Action::Requote { id, intent } => {
                (now.0.saturating_add(submit_ns), Op::Replace { id, intent })
            }
            Action::Cancel(id) => (now.0.saturating_add(cancel_ns), Op::Cancel(id)),
            Action::CancelAll => (now.0.saturating_add(cancel_ns), Op::CancelAll),
            Action::NoOp => return,
        };
        self.pending.push(PendingOp {
            scheduled_ts_ns: scheduled,
            op,
        });
        // Stable sort preserves FIFO within identical scheduled_ts_ns.
        self.pending.sort_by_key(|p| p.scheduled_ts_ns);
    }

    /// Live-mode variant: enqueue a Place using a venue-supplied `QuoteId`
    /// instead of letting FillSim mint a fresh one. Use this from the runner
    /// when the venue has already returned an id for a successful `quote()`.
    /// `live_quotes_for` will then return venue ids — so strategy-emitted
    /// `Cancel(id)` actions reference ids the venue knows about.
    pub fn enqueue_place_with_id(
        &mut self,
        intent: QuoteIntent,
        now: Timestamp,
        venue_id: QuoteId,
    ) {
        let submit_ns = self.cfg.submit_latency_ms.saturating_mul(1_000_000);
        self.pending.push(PendingOp {
            scheduled_ts_ns: now.0.saturating_add(submit_ns),
            op: Op::Place {
                intent,
                override_id: Some(venue_id),
            },
        });
        self.pending.sort_by_key(|p| p.scheduled_ts_ns);
    }

    /// Match queued open quotes against `ev`; emit fills for any quotes
    /// taken out by the trade-through model. Also emits taker fills for any
    /// pending IOC/FOK ops that became eligible this tick.
    pub fn on_market_event(&mut self, ev: &MarketEvent, now: Timestamp) -> Vec<Fill> {
        self.silent_cancel_tick(now);
        let mut fills = self.apply_pending(now);
        match ev {
            MarketEvent::BookUpdate { snapshot } => {
                self.update_book_state(snapshot);
            }
            MarketEvent::Trade {
                symbol,
                price,
                size,
                side: taker_side,
                ts,
            } => {
                fills.extend(self.match_trade(symbol, *price, *size, *taker_side, *ts));
            }
            MarketEvent::Heartbeat { .. } | MarketEvent::Fill(_) => {}
        }
        fills
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Drop random subset of `live_quotes` to simulate venue-side silent
    /// cancels (cancel/expire events the user-stream WS misses; live mode
    /// catches these via the `Venue::open_orders` reconciliation tick).
    /// The strategy is NOT notified — runner reconciliation eventually
    /// purges the stale ids from its view, mirroring live behaviour.
    fn silent_cancel_tick(&mut self, now: Timestamp) {
        let rate = self.cfg.silent_cancel_rate_per_min;
        if rate <= 0.0 {
            self.last_event_ts_ns = Some(now.0);
            return;
        }
        let elapsed_ns = match self.last_event_ts_ns {
            Some(prev) => now.0.saturating_sub(prev),
            None => 0,
        };
        self.last_event_ts_ns = Some(now.0);
        if elapsed_ns == 0 || self.live_quotes.is_empty() {
            return;
        }
        let elapsed_min = elapsed_ns as f64 / 60_000_000_000.0;
        let p = (rate * elapsed_min).clamp(0.0, 1.0);
        if p <= 0.0 {
            return;
        }
        let mut state = self.rng_state;
        self.live_quotes.retain(|_| {
            let r = next_unit_f64(&mut state);
            r >= p
        });
        self.rng_state = state;
    }

    fn apply_pending(&mut self, now: Timestamp) -> Vec<Fill> {
        let pivot = self.pending.partition_point(|p| p.scheduled_ts_ns <= now.0);
        let ready: Vec<_> = self.pending.drain(..pivot).collect();
        let mut fills = Vec::new();
        for p in ready {
            match p.op {
                Op::Place {
                    intent,
                    override_id,
                } => {
                    if let Some(f) =
                        self.place_or_reject(intent, Timestamp(p.scheduled_ts_ns), override_id)
                    {
                        fills.push(f);
                    }
                }
                Op::Replace { id, intent } => {
                    self.cancel_id(id);
                    if let Some(f) =
                        self.place_or_reject(intent, Timestamp(p.scheduled_ts_ns), None)
                    {
                        fills.push(f);
                    }
                }
                Op::Cancel(id) => self.cancel_id(id),
                Op::CancelAll => self.live_quotes.clear(),
            }
        }
        fills
    }

    fn place_or_reject(
        &mut self,
        intent: QuoteIntent,
        ts: Timestamp,
        override_id: Option<QuoteId>,
    ) -> Option<Fill> {
        if matches!(intent.tif, TimeInForce::PostOnly) && self.would_cross(&intent) {
            self.pending_rejections.push((
                intent.clone(),
                "post-only would cross touch (paper)".to_string(),
            ));
            return None;
        }
        // Synthetic Binance `-2019` (margin insufficient). At place-time,
        // if applying this intent *as a fill* would push |position| past
        // the configured cap, reject. Cheap approximation of the live
        // pre-trade margin check.
        if let Some(cap) = self.cfg.max_position_notional_usdt {
            let pos = self
                .position_notional
                .get(&intent.symbol)
                .copied()
                .unwrap_or(Decimal::ZERO);
            let delta = intent.price.0 * intent.size.0;
            let projected = match intent.side {
                Side::Bid => pos + delta,
                Side::Ask => pos - delta,
            };
            if projected.abs() > cap {
                self.pending_rejections.push((
                    intent.clone(),
                    "margin insufficient (paper -2019)".to_string(),
                ));
                return None;
            }
        }
        // IOC / FOK: if the intent crosses the live touch, fill immediately
        // at the touch price as a taker. If it doesn't cross, drop silently
        // (IOC = unfilled remainder gets cancelled; we treat 0 fill as full
        // cancel). Partial-fill modeling for IOC is a future refinement.
        if matches!(intent.tif, TimeInForce::IOC | TimeInForce::FOK) {
            let st = self.book_state.entry(intent.symbol.clone()).or_default();
            let touch = match intent.side {
                Side::Bid => st.best_ask(),
                Side::Ask => st.best_bid(),
            };
            let touch_price = touch?;
            let crosses = match intent.side {
                Side::Bid => intent.price.0 >= touch_price.0,
                Side::Ask => intent.price.0 <= touch_price.0,
            };
            if !crosses {
                return None;
            }
            let fill_size = intent.size.0;
            // Taker fee is always positive (no rebate). cfg.fees.taker_bps is u32.
            let fee_amount = touch_price.0 * fill_size * Decimal::from(self.cfg.fees.taker_bps)
                / Decimal::from(10_000);
            // Decrement the touched side's aggregate so subsequent cancel
            // attribution doesn't see the consumed liquidity as a cancel.
            let touched_side = match intent.side {
                Side::Bid => Side::Ask,
                Side::Ask => Side::Bid,
            };
            st.decrement_level(touched_side, touch_price, fill_size);
            let delta = touch_price.0 * fill_size;
            let entry = self
                .position_notional
                .entry(intent.symbol.clone())
                .or_insert(Decimal::ZERO);
            match intent.side {
                Side::Bid => *entry += delta,
                Side::Ask => *entry -= delta,
            }
            return Some(Fill {
                quote_id: QuoteId::new(),
                price: touch_price,
                size: Size(fill_size),
                fee_asset: intent.symbol.quote.clone(),
                fee_amount,
                fee_quote: Notional(fee_amount),
                side: intent.side,
                ts,
                // IOC taker fills the full intent in one shot (model
                // simplification: no partial IOC).
                is_full: true,
            });
        }
        // Snapshot queue position at our price level when placed. We're
        // appended to the back, so queue_ahead = current aggregate at that
        // level. New price levels (improve mode) have zero ahead of us.
        let queue_ahead = self
            .book_state
            .get(&intent.symbol)
            .map(|b| b.level_size(intent.side, intent.price))
            .unwrap_or(Decimal::ZERO);
        let id = override_id.unwrap_or_default();
        self.live_quotes.push(LiveQuote {
            id,
            symbol: intent.symbol,
            side: intent.side,
            price: intent.price,
            size_remaining: intent.size,
            queue_ahead,
            ts_submitted: ts,
        });
        None
    }

    fn would_cross(&self, intent: &QuoteIntent) -> bool {
        let Some(book) = self.book_state.get(&intent.symbol) else {
            return false;
        };
        match intent.side {
            Side::Bid => book.best_ask().is_some_and(|ask| intent.price.0 >= ask.0),
            Side::Ask => book.best_bid().is_some_and(|bid| intent.price.0 <= bid.0),
        }
    }

    fn cancel_id(&mut self, id: QuoteId) {
        self.live_quotes.retain(|q| q.id != id);
        // Also drop any not-yet-applied Place / Replace whose venue id
        // matches — otherwise a pending entry would get promoted into
        // `live_quotes` by the next `apply_pending`, even though the
        // strategy already cancelled (or the venue already filled) the
        // underlying order.
        self.pending.retain(|p| match &p.op {
            Op::Place {
                override_id: Some(oid),
                ..
            } => *oid != id,
            Op::Replace { id: rid, .. } => *rid != id,
            _ => true,
        });
    }

    /// Live mode: an external venue fill consumed one of our resting orders.
    /// Drop the corresponding `LiveQuote` so `live_quotes_for` and queue
    /// state stay in sync with the real exchange. Also evicts any
    /// in-flight Place/Replace op with the same venue id so a fill
    /// arriving BEFORE `apply_pending` doesn't leave a ghost behind.
    pub fn drop_quote(&mut self, id: QuoteId) {
        self.cancel_id(id);
    }

    /// Reconcile in-memory `live_quotes` against the venue's authoritative
    /// view for `symbol`. Any tracked quote whose id is NOT in `valid_ids`
    /// is a ghost (silently cancelled / expired / lost across a WS
    /// reconnect) and gets dropped. Returns the number of ghosts removed.
    ///
    /// Only affects quotes for `symbol`; other symbols' state is left
    /// untouched so this is safe to call per-bot in a multi-symbol
    /// process.
    pub fn retain_quotes_for(
        &mut self,
        symbol: &Symbol,
        valid_ids: &std::collections::HashSet<QuoteId>,
    ) -> usize {
        let before = self.live_quotes.len();
        self.live_quotes
            .retain(|q| &q.symbol != symbol || valid_ids.contains(&q.id));
        before - self.live_quotes.len()
    }

    /// Reconcile in-memory live quote state to the venue's authoritative open
    /// order list. Unlike [`Self::retain_quotes_for`], this is bidirectional:
    /// it drops local ghosts AND imports venue-resting orders missing from the
    /// local mirror.
    ///
    /// Returns `(removed_ghosts, added_missing)`.
    pub fn reconcile_quotes_for(
        &mut self,
        symbol: &Symbol,
        orders: &[OpenOrder],
    ) -> (usize, usize) {
        let valid_ids: std::collections::HashSet<QuoteId> = orders.iter().map(|o| o.id).collect();
        let removed = self.retain_quotes_for(symbol, &valid_ids);

        let mut known_ids: std::collections::HashSet<QuoteId> = self
            .live_quotes
            .iter()
            .filter(|q| &q.symbol == symbol)
            .map(|q| q.id)
            .collect();
        for p in &self.pending {
            if let Op::Place {
                intent,
                override_id: Some(qid),
            } = &p.op
                && &intent.symbol == symbol
            {
                known_ids.insert(*qid);
            }
        }

        let mut added = 0;
        for order in orders {
            if &order.symbol != symbol || known_ids.contains(&order.id) {
                continue;
            }
            self.live_quotes.push(LiveQuote {
                id: order.id,
                symbol: order.symbol.clone(),
                side: order.side,
                price: order.price,
                size_remaining: order.size,
                // We do not know queue position for imported live orders.
                // `0` is conservative for local open-count accuracy; these
                // imports are only used in live-mode bookkeeping/UI and cancel
                // targeting, not paper-mode fill simulation.
                queue_ahead: Decimal::ZERO,
                ts_submitted: Timestamp(0),
            });
            known_ids.insert(order.id);
            added += 1;
        }

        (removed, added)
    }

    fn update_book_state(&mut self, snapshot: &Snapshot) {
        // Cancel attribution: any LiveQuote at a level whose aggregate
        // SHRANK between the previous BookState snapshot and this one had
        // orders cancelled ahead of it (proportionally — assume cancels are
        // uniformly distributed across the queue). queue_ahead scales by
        // (new_agg / prev_agg). Trade-attributed shrinkage is excluded
        // because match_trade decrements book_state aggregates inline before
        // the next BookUpdate arrives.
        //
        // Window check: the replay caps snapshot depth, so quotes resting
        // outside the visible price range can't be distinguished from "level
        // vanished due to cancel" — skip attribution for those to avoid
        // phantom queue collapses. In-window edges: deepest visible bid /
        // ask price on each side.
        let st = self.book_state.entry(snapshot.symbol.clone()).or_default();
        let deepest_bid = snapshot.bids.last().map(|l| l.price.0);
        let deepest_ask = snapshot.asks.last().map(|l| l.price.0);
        let prev_aggs: Vec<(usize, Decimal)> = self
            .live_quotes
            .iter()
            .enumerate()
            .filter(|(_, q)| q.symbol == snapshot.symbol)
            .filter(|(_, q)| match q.side {
                Side::Bid => deepest_bid.is_some_and(|d| q.price.0 >= d),
                Side::Ask => deepest_ask.is_some_and(|d| q.price.0 <= d),
            })
            .map(|(i, q)| (i, st.level_size(q.side, q.price)))
            .collect();

        st.bids.clear();
        st.asks.clear();
        for lvl in &snapshot.bids {
            st.bids.insert(lvl.price.0, lvl.size.0);
        }
        for lvl in &snapshot.asks {
            st.asks.insert(lvl.price.0, lvl.size.0);
        }
        // Refresh cached top-of-book — snapshot.bids is sorted descending,
        // snapshot.asks ascending (invariant declared on Snapshot).
        st.set_top(
            snapshot.bids.first().map(|l| l.price),
            snapshot.asks.first().map(|l| l.price),
        );

        for (i, prev_agg) in prev_aggs {
            if prev_agg <= Decimal::ZERO {
                continue;
            }
            let q = &mut self.live_quotes[i];
            let new_agg = st.level_size(q.side, q.price);
            if new_agg >= prev_agg {
                // Aggregate grew (or held) — only new arrivals behind us,
                // no impact on queue_ahead.
                continue;
            }
            // Aggregate dropped without explanation — assume cancels
            // uniformly distributed. Scale queue_ahead proportionally.
            // Note: if the level vanished (new_agg == 0), queue_ahead → 0
            // (everyone ahead of us cancelled; we're sole resting quote).
            let new_queue = q.queue_ahead * new_agg / prev_agg;
            q.queue_ahead = new_queue;
        }
    }

    fn match_trade(
        &mut self,
        symbol: &Symbol,
        trade_price: Price,
        trade_size: Size,
        taker_side: Side,
        trade_ts: Timestamp,
    ) -> Vec<Fill> {
        // Defence: a zero-price or zero-size trade would cross every
        // resting buy (since any positive buy price > 0). Bad upstream
        // data — drop here as last line of defence.
        if trade_price.0 <= Decimal::ZERO || trade_size.0 <= Decimal::ZERO {
            return Vec::new();
        }
        let mut out = Vec::new();
        let mut trade_remaining = trade_size.0;

        let mut i = 0;
        while i < self.live_quotes.len() && trade_remaining > Decimal::ZERO {
            let q = &mut self.live_quotes[i];
            let eligible =
                q.symbol == *symbol && quote_takes_trade(q.side, q.price, taker_side, trade_price);
            if !eligible {
                i += 1;
                continue;
            }

            // Queue priority: trade consumes the orders RESTING AHEAD of us
            // at our level before reaching our quote. queue_ahead drops to
            // zero before we can fill. Trades at deeper prices (sweeps that
            // walk through our level) implicitly cleared queue ahead by the
            // time they reach us, but we model this conservatively: ONLY
            // trades AT our exact price level decrement our queue_ahead.
            // (Sweeping trades will print at multiple prices including ours
            // if size is sufficient; the prints AT our level decrement the
            // queue and fill us together.)
            let q_side = q.side;
            let q_price = q.price;
            let ate = q.queue_ahead.min(trade_remaining);
            if ate > Decimal::ZERO {
                q.queue_ahead -= ate;
                trade_remaining -= ate;
                // Decrement book aggregate at our level so the next
                // BookUpdate doesn't mis-attribute this trade-shrinkage as
                // cancels.
                if let Some(b) = self.book_state.get_mut(symbol) {
                    b.decrement_level(q_side, q_price, ate);
                }
                if trade_remaining == Decimal::ZERO {
                    break;
                }
            }
            let q = &mut self.live_quotes[i];

            let fill_amount = q.size_remaining.0.min(trade_remaining);
            let fill_price = q.price;
            // fee_amount is signed; positive = paid, negative = rebated.
            let fee_amount = fill_price.0 * fill_amount * Decimal::from(self.cfg.fees.maker_bps)
                / Decimal::from(10_000);
            let is_full = fill_amount >= q.size_remaining.0;
            out.push(Fill {
                quote_id: q.id,
                price: fill_price,
                size: Size(fill_amount),
                fee_asset: symbol.quote.clone(),
                fee_amount,
                fee_quote: Notional(fee_amount),
                side: q.side,
                ts: trade_ts,
                is_full,
            });
            let delta = fill_price.0 * fill_amount;
            let entry = self
                .position_notional
                .entry(symbol.clone())
                .or_insert(Decimal::ZERO);
            match q.side {
                Side::Bid => *entry += delta,
                Side::Ask => *entry -= delta,
            }
            q.size_remaining = Size(q.size_remaining.0 - fill_amount);
            trade_remaining -= fill_amount;
            if q.size_remaining.0 == Decimal::ZERO {
                self.live_quotes.remove(i);
            } else {
                i += 1;
            }
        }

        out
    }
}

/// xorshift64 step. Deterministic given the seed.
fn next_u64(s: &mut u64) -> u64 {
    let mut x = *s;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *s = x;
    x
}

/// Uniform `[0.0, 1.0)` from xorshift64. Uses 53 top bits of the draw,
/// matching standard double-precision conversion.
fn next_unit_f64(s: &mut u64) -> f64 {
    let x = next_u64(s);
    (x >> 11) as f64 / ((1u64 << 53) as f64)
}

/// Whether a resting quote on `quote_side` at `quote_price` would be taken by
/// a public trade printed at `trade_price` with the given aggressor side.
fn quote_takes_trade(
    quote_side: Side,
    quote_price: Price,
    taker_side: Side,
    trade_price: Price,
) -> bool {
    match (quote_side, taker_side) {
        // Our Bid (we buy) is taken when taker sold (Side::Ask) at or below our bid.
        (Side::Bid, Side::Ask) => trade_price.0 <= quote_price.0,
        // Our Ask (we sell) is taken when taker bought (Side::Bid) at or above our ask.
        (Side::Ask, Side::Bid) => trade_price.0 >= quote_price.0,
        // Same-side: taker hit the OTHER side of the book, not ours.
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::{Asset, Decimal, Level, MarketKind, QuoteKind, Snapshot, TimeInForce, VenueId};
    use tikr_venue::QuoteIntent;

    fn make_symbol() -> Symbol {
        Symbol {
            base: Asset::new("BTC"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("test"),
            kind: MarketKind::Spot,
        }
    }

    fn make_book(symbol: &Symbol, best_bid: i64, best_ask: i64) -> Snapshot {
        Snapshot {
            symbol: symbol.clone(),
            bids: vec![Level {
                price: Price(Decimal::from(best_bid)),
                size: Size(Decimal::from(1)),
            }],
            asks: vec![Level {
                price: Price(Decimal::from(best_ask)),
                size: Size(Decimal::from(1)),
            }],
            ts: Timestamp(0),
        }
    }

    fn make_intent(
        symbol: &Symbol,
        side: Side,
        price: i64,
        size: i64,
        tif: TimeInForce,
    ) -> QuoteIntent {
        QuoteIntent {
            symbol: symbol.clone(),
            side,
            price: Price(Decimal::from(price)),
            size: Size(Decimal::from(size)),
            tif,
            kind: QuoteKind::Point,
        }
    }

    fn make_trade(
        symbol: &Symbol,
        price: i64,
        size: i64,
        taker_side: Side,
        ts_ns: u64,
    ) -> MarketEvent {
        MarketEvent::Trade {
            symbol: symbol.clone(),
            price: Price(Decimal::from(price)),
            size: Size(Decimal::from(size)),
            side: taker_side,
            ts: Timestamp(ts_ns),
        }
    }

    fn default_cfg() -> FillSimConfig {
        FillSimConfig {
            submit_latency_ms: 10,
            cancel_latency_ms: 50,
            fees: VenueFees {
                maker_bps: 0,
                taker_bps: 0,
            },
            max_position_notional_usdt: None,
            silent_cancel_rate_per_min: 0.0,
            rng_seed: 0,
        }
    }

    #[test]
    fn post_only_rejected_when_would_cross_at_submit() {
        let sym = make_symbol();
        let mut sim = FillSim::new(default_cfg());

        // Seed book state: bid=100, ask=101.
        let book = MarketEvent::BookUpdate {
            snapshot: make_book(&sym, 100, 101),
        };
        let _ = sim.on_market_event(&book, Timestamp(0));

        // PostOnly bid at 102 crosses the 101 ask.
        sim.on_action(
            Action::Quote(make_intent(&sym, Side::Bid, 102, 1, TimeInForce::PostOnly)),
            Timestamp(0),
        );

        // Advance past submit_latency (10ms = 10_000_000ns) to fire pending Place.
        let hb = MarketEvent::Heartbeat {
            ts: Timestamp(20_000_000),
        };
        let _ = sim.on_market_event(&hb, Timestamp(20_000_000));

        // Trade that would have hit the rejected quote.
        let trade = make_trade(&sym, 102, 1, Side::Ask, 30_000_000);
        let fills = sim.on_market_event(&trade, Timestamp(30_000_000));

        assert!(fills.is_empty(), "rejected post-only must not fill");
        assert_eq!(sim.live_quotes.len(), 0, "rejected quote not in book");
    }

    #[test]
    fn partial_fill_leaves_quote_open_with_reduced_size() {
        let sym = make_symbol();
        let mut sim = FillSim::new(default_cfg());

        let book = MarketEvent::BookUpdate {
            snapshot: make_book(&sym, 99, 101),
        };
        let _ = sim.on_market_event(&book, Timestamp(0));

        // Place a Bid PostOnly at 100, size 5 (does not cross 99/101 book).
        sim.on_action(
            Action::Quote(make_intent(&sym, Side::Bid, 100, 5, TimeInForce::PostOnly)),
            Timestamp(0),
        );

        // Fire pending Place at t=20ms.
        let hb = MarketEvent::Heartbeat {
            ts: Timestamp(20_000_000),
        };
        let _ = sim.on_market_event(&hb, Timestamp(20_000_000));

        // 1-unit Ask-side taker print at 100 → partial fill on the 5-unit bid.
        let trade = make_trade(&sym, 100, 1, Side::Ask, 30_000_000);
        let fills = sim.on_market_event(&trade, Timestamp(30_000_000));

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].size, Size(Decimal::from(1)));
        assert_eq!(fills[0].price, Price(Decimal::from(100)));
        assert_eq!(sim.live_quotes.len(), 1);
        assert_eq!(sim.live_quotes[0].size_remaining, Size(Decimal::from(4)));
    }

    #[test]
    fn cancel_after_fill_race() {
        let sym = make_symbol();
        let mut sim = FillSim::new(default_cfg());

        let book = MarketEvent::BookUpdate {
            snapshot: make_book(&sym, 99, 101),
        };
        let _ = sim.on_market_event(&book, Timestamp(0));

        // Place at t=0, scheduled for t=10ms.
        sim.on_action(
            Action::Quote(make_intent(&sym, Side::Bid, 100, 1, TimeInForce::PostOnly)),
            Timestamp(0),
        );
        // Drive pending Place via heartbeat at t=15ms.
        let hb1 = MarketEvent::Heartbeat {
            ts: Timestamp(15_000_000),
        };
        let _ = sim.on_market_event(&hb1, Timestamp(15_000_000));

        // Cancel-all at t=20ms; cancel_latency=50ms so it lands at t=70ms.
        sim.on_action(Action::CancelAll, Timestamp(20_000_000));

        // Trade at t=30ms races ahead of the cancel.
        let trade = make_trade(&sym, 100, 1, Side::Ask, 30_000_000);
        let fills = sim.on_market_event(&trade, Timestamp(30_000_000));
        assert_eq!(fills.len(), 1, "race-lost cancel: fill still happens");

        // Advance past cancel landing time (t=80ms); cancel applies to empty book.
        let hb2 = MarketEvent::Heartbeat {
            ts: Timestamp(80_000_000),
        };
        let fills2 = sim.on_market_event(&hb2, Timestamp(80_000_000));
        assert!(fills2.is_empty(), "no fills from a stale cancel");
    }

    fn make_book_with_size(
        symbol: &Symbol,
        bid_price: i64,
        bid_size: i64,
        ask_price: i64,
        ask_size: i64,
    ) -> Snapshot {
        Snapshot {
            symbol: symbol.clone(),
            bids: vec![Level {
                price: Price(Decimal::from(bid_price)),
                size: Size(Decimal::from(bid_size)),
            }],
            asks: vec![Level {
                price: Price(Decimal::from(ask_price)),
                size: Size(Decimal::from(ask_size)),
            }],
            ts: Timestamp(0),
        }
    }

    /// Queue-priority: a join order at an existing best-bid level must wait
    /// for queue_ahead to drain before filling.
    #[test]
    fn queue_priority_join_waits_for_queue_to_drain() {
        let sym = make_symbol();
        let mut sim = FillSim::new(default_cfg());

        // Seed book: best bid at 100 with 5 units resting, ask at 101.
        let book = MarketEvent::BookUpdate {
            snapshot: make_book_with_size(&sym, 100, 5, 101, 1),
        };
        let _ = sim.on_market_event(&book, Timestamp(0));

        // Place a JOIN bid at 100 (size 1). queue_ahead = 5.
        sim.on_action(
            Action::Quote(make_intent(&sym, Side::Bid, 100, 1, TimeInForce::PostOnly)),
            Timestamp(0),
        );
        let hb = MarketEvent::Heartbeat {
            ts: Timestamp(20_000_000),
        };
        let _ = sim.on_market_event(&hb, Timestamp(20_000_000));

        // Trade of size 3 at 100 — consumes 3 of the 5 ahead. We don't fill.
        let t1 = make_trade(&sym, 100, 3, Side::Ask, 30_000_000);
        let fills1 = sim.on_market_event(&t1, Timestamp(30_000_000));
        assert!(
            fills1.is_empty(),
            "join order behind queue must not fill yet"
        );

        // Trade of size 3 at 100 — consumes remaining 2 of queue then fills 1 of ours.
        let t2 = make_trade(&sym, 100, 3, Side::Ask, 40_000_000);
        let fills2 = sim.on_market_event(&t2, Timestamp(40_000_000));
        assert_eq!(fills2.len(), 1, "queue exhausted, our order should fill");
        assert_eq!(fills2[0].size, Size(Decimal::from(1)));
    }

    /// Queue-priority: an IMPROVE order (new price level) has queue_ahead = 0
    /// and fills immediately on the first adverse trade at its price.
    #[test]
    fn queue_priority_improve_fills_immediately() {
        let sym = make_symbol();
        let mut sim = FillSim::new(default_cfg());

        // Book has best bid at 99 (size 5), ask at 102. No level at 100.
        let book = MarketEvent::BookUpdate {
            snapshot: make_book_with_size(&sym, 99, 5, 102, 1),
        };
        let _ = sim.on_market_event(&book, Timestamp(0));

        // Place a 1-tick IMPROVE bid at 100 (size 1). queue_ahead = 0.
        sim.on_action(
            Action::Quote(make_intent(&sym, Side::Bid, 100, 1, TimeInForce::PostOnly)),
            Timestamp(0),
        );
        let hb = MarketEvent::Heartbeat {
            ts: Timestamp(20_000_000),
        };
        let _ = sim.on_market_event(&hb, Timestamp(20_000_000));

        // Trade of size 1 at 100 — fills our order immediately (no queue).
        let trade = make_trade(&sym, 100, 1, Side::Ask, 30_000_000);
        let fills = sim.on_market_event(&trade, Timestamp(30_000_000));
        assert_eq!(
            fills.len(),
            1,
            "improve order at new level should fill immediately"
        );
        assert_eq!(fills[0].price, Price(Decimal::from(100)));
    }

    /// Book updates that GROW aggregate at our level do NOT shift queue
    /// position (new arrivals are behind us).
    #[test]
    fn book_update_grow_does_not_shift_queue_position() {
        let sym = make_symbol();
        let mut sim = FillSim::new(default_cfg());

        let book1 = MarketEvent::BookUpdate {
            snapshot: make_book_with_size(&sym, 100, 5, 101, 1),
        };
        let _ = sim.on_market_event(&book1, Timestamp(0));
        sim.on_action(
            Action::Quote(make_intent(&sym, Side::Bid, 100, 1, TimeInForce::PostOnly)),
            Timestamp(0),
        );
        let hb = MarketEvent::Heartbeat {
            ts: Timestamp(20_000_000),
        };
        let _ = sim.on_market_event(&hb, Timestamp(20_000_000));
        assert_eq!(sim.live_quotes[0].queue_ahead, Decimal::from(5));

        // Aggregate at 100 grows to 10. Our queue_ahead must stay 5 (new
        // arrivals are behind us).
        let book2 = MarketEvent::BookUpdate {
            snapshot: make_book_with_size(&sym, 100, 10, 101, 1),
        };
        let _ = sim.on_market_event(&book2, Timestamp(25_000_000));
        assert_eq!(sim.live_quotes[0].queue_ahead, Decimal::from(5));
    }

    /// Cancels: aggregate SHRINKS without an explaining trade → queue_ahead
    /// scales proportionally (uniform-cancel assumption).
    #[test]
    fn cancels_shrink_queue_proportionally() {
        let sym = make_symbol();
        let mut sim = FillSim::new(default_cfg());

        // Best bid 100 with 10 resting.
        let book1 = MarketEvent::BookUpdate {
            snapshot: make_book_with_size(&sym, 100, 10, 101, 1),
        };
        let _ = sim.on_market_event(&book1, Timestamp(0));

        // Place JOIN bid; queue_ahead = 10.
        sim.on_action(
            Action::Quote(make_intent(&sym, Side::Bid, 100, 1, TimeInForce::PostOnly)),
            Timestamp(0),
        );
        let hb = MarketEvent::Heartbeat {
            ts: Timestamp(20_000_000),
        };
        let _ = sim.on_market_event(&hb, Timestamp(20_000_000));
        assert_eq!(sim.live_quotes[0].queue_ahead, Decimal::from(10));

        // Aggregate drops 10 → 4 without trades (cancels). queue_ahead
        // should scale to 10 * 4/10 = 4.
        let book2 = MarketEvent::BookUpdate {
            snapshot: make_book_with_size(&sym, 100, 4, 101, 1),
        };
        let _ = sim.on_market_event(&book2, Timestamp(25_000_000));
        assert_eq!(sim.live_quotes[0].queue_ahead, Decimal::from(4));
    }

    /// Trade-shrinkage at our level must NOT be double-counted as cancels:
    /// match_trade decrements book aggregate inline so the next book update
    /// compares against the trade-adjusted value.
    #[test]
    fn trade_shrinkage_not_double_counted_as_cancels() {
        let sym = make_symbol();
        let mut sim = FillSim::new(default_cfg());

        let book1 = MarketEvent::BookUpdate {
            snapshot: make_book_with_size(&sym, 100, 10, 101, 1),
        };
        let _ = sim.on_market_event(&book1, Timestamp(0));
        sim.on_action(
            Action::Quote(make_intent(&sym, Side::Bid, 100, 1, TimeInForce::PostOnly)),
            Timestamp(0),
        );
        let hb = MarketEvent::Heartbeat {
            ts: Timestamp(20_000_000),
        };
        let _ = sim.on_market_event(&hb, Timestamp(20_000_000));
        assert_eq!(sim.live_quotes[0].queue_ahead, Decimal::from(10));

        // Trade of 3 at 100 consumes 3 from our queue. queue_ahead → 7.
        let trade = make_trade(&sym, 100, 3, Side::Ask, 25_000_000);
        let _ = sim.on_market_event(&trade, Timestamp(25_000_000));
        assert_eq!(sim.live_quotes[0].queue_ahead, Decimal::from(7));

        // Next book update reflects the trade: aggregate now 7.
        let book2 = MarketEvent::BookUpdate {
            snapshot: make_book_with_size(&sym, 100, 7, 101, 1),
        };
        let _ = sim.on_market_event(&book2, Timestamp(30_000_000));
        // queue_ahead must stay 7 — the drop 10→7 is fully explained by
        // the trade, NOT cancels. Without inline decrement we'd over-shrink.
        assert_eq!(sim.live_quotes[0].queue_ahead, Decimal::from(7));
    }

    #[test]
    fn maker_rebate_produces_negative_fee_quote() {
        let sym = make_symbol();
        let cfg = FillSimConfig {
            submit_latency_ms: 10,
            cancel_latency_ms: 50,
            fees: VenueFees {
                maker_bps: -10,
                taker_bps: 0,
            },
            max_position_notional_usdt: None,
            silent_cancel_rate_per_min: 0.0,
            rng_seed: 0,
        };
        let mut sim = FillSim::new(cfg);

        let book = MarketEvent::BookUpdate {
            snapshot: make_book(&sym, 99, 101),
        };
        let _ = sim.on_market_event(&book, Timestamp(0));

        sim.on_action(
            Action::Quote(make_intent(&sym, Side::Bid, 100, 1, TimeInForce::PostOnly)),
            Timestamp(0),
        );
        let hb = MarketEvent::Heartbeat {
            ts: Timestamp(20_000_000),
        };
        let _ = sim.on_market_event(&hb, Timestamp(20_000_000));

        let trade = make_trade(&sym, 100, 1, Side::Ask, 30_000_000);
        let fills = sim.on_market_event(&trade, Timestamp(30_000_000));

        assert_eq!(fills.len(), 1);
        let expected =
            Decimal::from(100) * Decimal::from(1) * Decimal::from(-10) / Decimal::from(10_000);
        assert_eq!(fills[0].fee_quote, Notional(expected));
        assert_eq!(fills[0].fee_amount, expected);
        assert!(expected < Decimal::ZERO, "rebate must be negative");
    }

    #[tokio::test]
    async fn ioc_fills_at_touch_when_crosses() {
        let sym = make_symbol();
        let cfg = FillSimConfig {
            submit_latency_ms: 0,
            cancel_latency_ms: 0,
            fees: VenueFees {
                maker_bps: 0,
                taker_bps: 5,
            },
            max_position_notional_usdt: None,
            silent_cancel_rate_per_min: 0.0,
            rng_seed: 0,
        };
        let mut sim = FillSim::new(cfg);
        // Seed the book via a snapshot: best_bid=99, best_ask=101.
        let snap = MarketEvent::BookUpdate {
            snapshot: Snapshot {
                symbol: sym.clone(),
                bids: vec![tikr_core::Level {
                    price: Price(Decimal::from(99)),
                    size: Size(Decimal::from(10)),
                }],
                asks: vec![tikr_core::Level {
                    price: Price(Decimal::from(101)),
                    size: Size(Decimal::from(10)),
                }],
                ts: Timestamp(0),
            },
        };
        let _ = sim.on_market_event(&snap, Timestamp(0));
        // IOC bid at 200 — way above ask 101 → fills at touch 101.
        sim.on_action(
            Action::Quote(make_intent(&sym, Side::Bid, 200, 1, TimeInForce::IOC)),
            Timestamp(1_000_000),
        );
        // Trigger apply_pending via a heartbeat.
        let fills = sim.on_market_event(
            &MarketEvent::Heartbeat {
                ts: Timestamp(2_000_000),
            },
            Timestamp(2_000_000),
        );
        assert_eq!(fills.len(), 1, "IOC should fill immediately at touch");
        assert_eq!(fills[0].price.0, Decimal::from(101));
        assert_eq!(fills[0].size.0, Decimal::from(1));
        assert_eq!(fills[0].side, Side::Bid);
        // Taker fee = 101 * 1 * 5 / 10000 = 0.0505
        let expected_fee =
            Decimal::from(101) * Decimal::from(1) * Decimal::from(5) / Decimal::from(10_000);
        assert_eq!(fills[0].fee_amount, expected_fee);
        // No resting quote left.
        assert_eq!(sim.live_quotes.len(), 0);
    }

    #[tokio::test]
    async fn ioc_drops_silently_when_does_not_cross() {
        let sym = make_symbol();
        let mut sim = FillSim::new(default_cfg());
        let snap = MarketEvent::BookUpdate {
            snapshot: Snapshot {
                symbol: sym.clone(),
                bids: vec![tikr_core::Level {
                    price: Price(Decimal::from(99)),
                    size: Size(Decimal::from(10)),
                }],
                asks: vec![tikr_core::Level {
                    price: Price(Decimal::from(101)),
                    size: Size(Decimal::from(10)),
                }],
                ts: Timestamp(0),
            },
        };
        let _ = sim.on_market_event(&snap, Timestamp(0));
        // IOC bid at 50 — below ask 101 → does not cross → no fill, no rest.
        sim.on_action(
            Action::Quote(make_intent(&sym, Side::Bid, 50, 1, TimeInForce::IOC)),
            Timestamp(1_000_000),
        );
        let fills = sim.on_market_event(
            &MarketEvent::Heartbeat {
                ts: Timestamp(2_000_000),
            },
            Timestamp(2_000_000),
        );
        assert!(fills.is_empty());
        assert_eq!(sim.live_quotes.len(), 0);
    }
}
