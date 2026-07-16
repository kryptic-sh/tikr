//! Simulated fill engine — turns strategy [`Action`]s + market events into
//! [`Fill`]s under a trade-through model with post-only correctness.
//! See [issue #11] for the optimistic fill bias note.
//!
//! [issue #11]: https://github.com/kryptic-sh/tikr/issues/11

use std::collections::HashMap;
use std::collections::HashSet;

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

/// Binance's per-symbol `MAX_NUM_ORDERS` exchange filter: the maximum number
/// of simultaneously-open (resting) orders allowed on a single symbol. 200 on
/// both USD-M Futures and Spot. Used as the default open-order cap so paper
/// backtests reject the 201st resting order exactly as the live venue does —
/// without it, grid/ladder strategies that never cancel can accumulate orders
/// unboundedly and turn the per-event open-order scan into an O(events²) hang.
pub const BINANCE_MAX_OPEN_ORDERS_PER_SYMBOL: u32 = 200;

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
    /// Account leverage. With the running wallet (see [`FillSim::set_wallet`]),
    /// the margin gate rejects any order whose worst-case position would exceed
    /// `wallet × leverage` (the venue's real buying-power limit). `0` (default)
    /// disables the wallet-based gate (falls back to `max_position_notional_usdt`).
    pub leverage: Decimal,
    /// Strategy position cap as a FRACTION of the running wallet (e.g. `3.0` =
    /// 300% = cap the bag at 3× wallet). Dynamic — tracks the wallet via
    /// [`FillSim::set_wallet`], so it grows as the account does. `0` (default) =
    /// no `%` cap.
    pub max_position_frac: Decimal,
    /// Per-minute probability that any individual live quote gets
    /// silently dropped, simulating venue-side cancel/expire that the
    /// user_stream WS misses (the live reconciliation loop normally
    /// catches these via `Venue::open_orders`). `0.0` = disabled.
    /// Typical realistic value: `0.005` (0.5% per quote per minute).
    pub silent_cancel_rate_per_min: f64,
    /// Deterministic RNG seed for silent cancellations. Same seed =
    /// same dropped quotes for reproducible backtests.
    pub rng_seed: u64,
    /// Mean additional latency (ms) drawn per submitted op on top of the
    /// fixed `submit_latency_ms` / `cancel_latency_ms` base. Modelled as an
    /// exponential distribution, so most ops see a little extra delay while a
    /// few hit a long tail — capturing real network jitter + occasional
    /// spikes with one knob. Jitter naturally reorders the pending queue
    /// (a spiked order can land after a later one), exercising
    /// cancel/replace races. Drawn from a dedicated RNG stream seeded off
    /// `rng_seed`, so runs stay reproducible. `0` (default) = no jitter
    /// (fixed latency, fully deterministic).
    pub latency_jitter_ms: u64,
    /// Max simultaneously-resting orders per symbol. `None` = unlimited.
    /// When set, a Place that would push the symbol's resting-order count
    /// past this cap is rejected with a synthetic Binance `-1015`
    /// ("too many orders") reason, routed through `on_quote_rejected` like
    /// other paper rejections. Defaults to
    /// [`BINANCE_MAX_OPEN_ORDERS_PER_SYMBOL`] on the live-shaped backtest
    /// entry points; mirrors the venue's `MAX_NUM_ORDERS` filter and bounds
    /// runaway open-order accumulation.
    pub max_open_orders: Option<u32>,
    /// Front-of-queue cancellation decay rate, per second. Models the orders
    /// resting AHEAD of ours getting cancel-replaced over time — invisible in
    /// L2 data (we only see net level depth, never gross adds vs cancels), so
    /// a net-growing level otherwise freezes our `queue_ahead` forever and
    /// starves the resting side (the ask-in-a-dump peg). Each event decays
    /// every live order's `queue_ahead` by `exp(-rate · dt_secs)`, on top of
    /// the trade + level-shrink decrements. `0.0` (default) = no time decay
    /// (prior behaviour). Calibrate to live fill rates.
    pub queue_cancel_decay_per_sec: f64,
    /// SPOT account mode. When `true`:
    /// - Gate 1 (margin / buying-power) and Gate 2 (position cap) are replaced
    ///   by spot cash + asset balance gates (no shorting, no leverage, no
    ///   liquidation / funding).
    /// - Every fill updates `spot_cash` and `spot_units` (seeded via
    ///   [`FillSim::seed_spot`]).
    ///
    /// `false` (default) = futures mode (existing behaviour unchanged).
    pub spot: bool,
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

    /// Walk the book for an IOC taker, consuming liquidity level by level
    /// from the touch outward. Returns the consumed levels (`(price,
    /// qty)`) so the caller can decrement aggregates AND compute the
    /// weighted-average fill price + total filled size.
    ///
    /// `taker_side` is the SIDE OF THE INTENT (Bid or Ask). The side of
    /// the book that gets consumed is the opposite.
    ///
    /// `limit_price` caps how far the walk goes: Bid IOC won't pay above
    /// it, Ask IOC won't sell below. For IOC at touch (intent.price ==
    /// touch), this typically allows full traversal until size exhausted
    /// or book runs out — but a strategy that places IOC with a tighter
    /// limit price stops earlier (partial fill, rest cancelled).
    ///
    /// Returns empty Vec when the touch doesn't cross the limit (no
    /// fill) or the book side is empty.
    fn walk_book_ioc(
        &self,
        taker_side: Side,
        size: Decimal,
        limit_price: Price,
    ) -> Vec<(Price, Decimal)> {
        let book_side_map = match taker_side {
            Side::Bid => &self.asks,
            Side::Ask => &self.bids,
        };
        if book_side_map.is_empty() || size <= Decimal::ZERO {
            return Vec::new();
        }
        // Sort levels in the direction the IOC walks. Bid walks asks
        // cheapest first (ascending). Ask walks bids highest first
        // (descending).
        let mut levels: Vec<(Decimal, Decimal)> =
            book_side_map.iter().map(|(p, s)| (*p, *s)).collect();
        match taker_side {
            Side::Bid => levels.sort_by_key(|(p, _)| *p),
            Side::Ask => levels.sort_by_key(|(p, _)| std::cmp::Reverse(*p)),
        }
        let mut consumed = Vec::new();
        let mut remaining = size;
        for (level_price, level_size) in levels {
            if remaining <= Decimal::ZERO {
                break;
            }
            // Honor the IOC's limit price — stop walking when next
            // level would breach. Bid can't pay above limit, Ask can't
            // sell below.
            let breaches = match taker_side {
                Side::Bid => level_price > limit_price.0,
                Side::Ask => level_price < limit_price.0,
            };
            if breaches {
                break;
            }
            let take = remaining.min(level_size);
            if take > Decimal::ZERO {
                consumed.push((Price(level_price), take));
                remaining -= take;
            }
        }
        consumed
    }
}

// ---------------------------------------------------------------------------
// FillSim
// ---------------------------------------------------------------------------

/// Audit counters (TEMP, behind `TIKR_FILLSIM_DIAG` env): per-side tally of
/// how often a resting quote was eligible for an incoming trade, how much of
/// the trade got absorbed by `queue_ahead` before reaching us, and how much
/// actually filled. Lets us see if the queue model starves one side.
#[derive(Default)]
struct FillDiag {
    bid_eligible: u64,
    ask_eligible: u64,
    bid_queue_eaten: Decimal,
    ask_queue_eaten: Decimal,
    bid_filled_qty: Decimal,
    ask_filled_qty: Decimal,
    bid_fills: u64,
    ask_fills: u64,
    // Raw recorded trade flow by taker side (independent of our orders) — tells
    // us if the window was genuinely sell-dominated (real dump) vs balanced.
    taker_buy_trades: u64,
    taker_sell_trades: u64,
    taker_buy_qty: Decimal,
    taker_sell_qty: Decimal,
}

/// Trade-through fill simulator with configurable latency, post-only
/// correctness, partial fills, and maker-rebate fees.
pub struct FillSim {
    cfg: FillSimConfig,
    diag: FillDiag,
    pending: Vec<PendingOp>,
    live_quotes: Vec<LiveQuote>,
    /// IDs of orders placed by the MANAGER (runner-level take-profit / bagger),
    /// not the strategy. These are hidden from the strategy-facing views
    /// (`live_quotes_for`, `open_quotes`, `committed_notional_by_side`) so the
    /// strategy never sees, re-quotes against, or cancels them. The runner still
    /// sees them via `all_live_quotes_for` for its own fill tracking.
    manager_ids: HashSet<QuoteId>,
    book_state: HashMap<Symbol, BookState>,
    /// Intents rejected during the last `apply_pending` pass (post-only
    /// crosses the touch). Paper-mode runner drains this after each
    /// `on_market_event` and routes each entry through
    /// `strategy.on_quote_rejected` so backtests exercise the same
    /// recovery path live mode uses.
    pending_rejections: Vec<(QuoteIntent, String)>,
    /// Signed position size in BASE UNITS per symbol (NOT notional).
    /// Positive = long. Maintained by maker fills (`match_trade`) and taker
    /// fills (`place_or_reject` IOC arm). Gates convert this to notional at
    /// check time via the `position_notional` method (mark price × units) —
    /// accumulating notional directly here would track signed CASH FLOW
    /// instead of position size, drifting by the gross realized PnL of
    /// every round trip (buy 1 @ 100 / sell 1 @ 110 nets a flat position
    /// but a −10 "notional" under the old cash-flow scheme) and eventually
    /// mis-gating one side of the margin/position caps.
    position_units: HashMap<Symbol, Decimal>,
    /// Running account wallet (margin balance), pushed by the runner via
    /// [`FillSim::set_wallet`]. With `cfg.leverage` it forms the buying-power
    /// margin gate. `0` = unset (gate falls back to the configured cap).
    current_wallet: Decimal,
    /// SPOT mode: available USD cash. Bid fills consume it; Ask fills add
    /// to it. Seeded via [`Self::seed_spot`]. Ignored when
    /// `cfg.spot == false`.
    spot_cash: Decimal,
    /// SPOT mode: held asset units. Ask fills consume them; Bid fills add
    /// to them. Seeded via [`Self::seed_spot`]. Ignored when
    /// `cfg.spot == false`. Enforces no-shorting: Ask gate rejects when
    /// committed + intent would exceed this.
    spot_units: Decimal,
    /// Cached per-symbol resting commitment: `(bid_notional, ask_notional,
    /// ask_size)`. Recomputed lazily (one O(N) pass) only when
    /// `committed_dirty`. The gate (spot cash/asset + futures Gate 1) reads
    /// this instead of re-scanning `live_quotes` on every placement — a
    /// reject storm (cash-pinned grid re-quoting a depleted side) rests no
    /// orders, so the flag stays clean and each reject is O(1).
    committed: HashMap<Symbol, (Decimal, Decimal, Decimal)>,
    /// True when `live_quotes` changed since the last `committed` recompute.
    committed_dirty: bool,
    /// xorshift64 state for silent-cancellation rolls.
    rng_state: u64,
    /// Separate xorshift64 state for latency-jitter draws. Kept distinct from
    /// `rng_state` so jitter values don't shift when silent-cancel config
    /// changes (the two would otherwise interleave draws).
    latency_rng_state: u64,
    /// Last `on_market_event` timestamp, in nanoseconds. Used to compute
    /// the elapsed window for `silent_cancel_rate_per_min`.
    last_event_ts_ns: Option<u64>,
    /// Cache backing [`Self::open_quotes`]. The per-event runner loop rebuilds
    /// the open-quote list on every market event, and each entry clones a
    /// `Symbol` (3× `Arc<str>` refcount atomics) — under a concurrent
    /// `compare` sweep those `lock`-prefixed atomics dominate the profile.
    /// Most events (pure book updates) leave the resting-order set untouched,
    /// so we fingerprint it and only rebuild (and re-clone) when the
    /// fingerprint changes.
    open_quotes_cache: Vec<(QuoteId, QuoteIntent)>,
    /// Fingerprint of the live + in-flight order set the cache was built from.
    /// `None` forces a rebuild on first use.
    open_quotes_cache_sig: Option<u128>,
    /// Symbol the cache was built for; a mismatch forces a rebuild (defends
    /// the cache against multi-symbol callers).
    open_quotes_cache_symbol: Option<Symbol>,
    /// Live-mode mirror: the sim only REGISTERS resting orders (so the
    /// runner/strategy can see them); fills, silent cancels, and queue decay
    /// come from the real venue. When set, public trades must NOT consume
    /// mirrored quotes — the real order still rests on the venue, and a
    /// simulated fill here would make the strategy re-quote a duplicate.
    live_mirror: bool,
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
        // Derive a distinct, non-zero latency-jitter stream from the same seed.
        let latency_rng_state = rng_state ^ 0xA5A5_A5A5_A5A5_A5A5;
        Self {
            cfg,
            diag: FillDiag::default(),
            pending: Vec::new(),
            live_quotes: Vec::new(),
            manager_ids: HashSet::new(),
            book_state: HashMap::new(),
            pending_rejections: Vec::new(),
            position_units: HashMap::new(),
            current_wallet: Decimal::ZERO,
            spot_cash: Decimal::ZERO,
            spot_units: Decimal::ZERO,
            committed: HashMap::new(),
            committed_dirty: true,
            rng_state,
            latency_rng_state,
            last_event_ts_ns: None,
            open_quotes_cache: Vec::new(),
            open_quotes_cache_sig: None,
            open_quotes_cache_symbol: None,
            live_mirror: false,
        }
    }

    /// Update the running account wallet (margin balance) used by the
    /// buying-power margin gate (`wallet × leverage`). The runner calls this as
    /// the wallet moves (initial + realized − fees). No-op effect until
    /// `cfg.leverage > 0`.
    pub fn set_wallet(&mut self, wallet: Decimal) {
        self.current_wallet = wallet;
    }

    /// SPOT mode: seed the initial cash + asset balances. Call once at runner
    /// start, before the first event, when `cfg.spot == true`.
    pub fn seed_spot(&mut self, cash: Decimal, units: Decimal) {
        self.spot_cash = cash;
        self.spot_units = units;
    }

    /// SPOT mode: current available USD cash (after fills + committed resting bids).
    pub fn spot_cash(&self) -> Decimal {
        self.spot_cash
    }

    /// SPOT mode: current held asset units (after fills).
    pub fn spot_units(&self) -> Decimal {
        self.spot_units
    }

    /// Resting commitment for `symbol`: `(bid_notional, ask_notional,
    /// ask_size)` summed over `live_quotes`. Recomputes the whole-book cache
    /// (one O(N) pass over all symbols) only when `committed_dirty`; otherwise
    /// returns the cached value in O(1). Keeps the placement gate cheap during
    /// a reject storm, where no order rests so the flag stays clean.
    fn committed_for(&mut self, symbol: &Symbol) -> (Decimal, Decimal, Decimal) {
        if self.committed_dirty {
            self.committed.clear();
            for q in &self.live_quotes {
                let e = self.committed.entry(q.symbol.clone()).or_insert((
                    Decimal::ZERO,
                    Decimal::ZERO,
                    Decimal::ZERO,
                ));
                let notional = (q.price.0 * q.size_remaining.0).round_dp(8);
                match q.side {
                    Side::Bid => e.0 = (e.0 + notional).round_dp(8),
                    Side::Ask => {
                        e.1 = (e.1 + notional).round_dp(8);
                        e.2 += q.size_remaining.0;
                    }
                }
            }
            self.committed_dirty = false;
        }
        self.committed
            .get(symbol)
            .copied()
            .unwrap_or((Decimal::ZERO, Decimal::ZERO, Decimal::ZERO))
    }

    /// Signed position notional for `symbol`: `position_units × mark price`.
    /// Mark price prefers the live book mid (`(best_bid + best_ask) / 2`)
    /// when both sides are known, falls back to whichever touch is known,
    /// and finally to `fallback_price` (the placing intent's own price)
    /// when the book hasn't been seeded yet. Computing notional from units
    /// at gate-check time — rather than accumulating signed cash flow —
    /// keeps the margin gates immune to realized PnL: a flat position after
    /// a profitable round trip must read as zero exposure, not the trade's
    /// profit (see `position_units` field docs).
    fn position_notional(&self, symbol: &Symbol, fallback_price: Price) -> Decimal {
        let units = self
            .position_units
            .get(symbol)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let mark = match self.book_state.get(symbol) {
            Some(b) => match (b.best_bid(), b.best_ask()) {
                (Some(bid), Some(ask)) => (bid.0 + ask.0) / Decimal::from(2),
                (Some(p), None) | (None, Some(p)) => p.0,
                (None, None) => fallback_price.0,
            },
            None => fallback_price.0,
        };
        (units * mark).round_dp(8)
    }

    /// Maker fee in bps (for runner-side synthetic fills, e.g. the bagger).
    pub fn maker_bps(&self) -> i32 {
        self.cfg.fees.maker_bps
    }

    /// Taker fee in bps (for runner-side synthetic flatten fills).
    pub fn taker_bps(&self) -> u32 {
        self.cfg.fees.taker_bps
    }

    /// Total scheduling delay in nanoseconds for an op submitted now: the
    /// fixed `base_ms` plus an exponential jitter draw with mean
    /// `cfg.latency_jitter_ms` (capped at 20× the mean to bound outliers).
    /// Returns exactly `base_ms` (no RNG draw) when jitter is disabled, so
    /// fixed-latency runs stay bit-for-bit deterministic.
    fn sample_latency_ns(&mut self, base_ms: u64) -> u64 {
        let jitter_ms = if self.cfg.latency_jitter_ms == 0 {
            0
        } else {
            let mean = self.cfg.latency_jitter_ms as f64;
            // Inverse-CDF of Exp(mean): -mean * ln(1-u), u in [0,1).
            let u = next_unit_f64(&mut self.latency_rng_state);
            let sample = -mean * (1.0 - u).ln();
            sample.min(mean * 20.0).round() as u64
        };
        base_ms.saturating_add(jitter_ms).saturating_mul(1_000_000)
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
        let mut out = Vec::new();
        self.live_quotes_into(symbol, &mut out);
        out
    }

    /// Tag an order id as MANAGER-owned (take-profit / bagger) — hidden from the
    /// strategy-facing views. Call right after placing a runner-level order.
    pub fn mark_manager(&mut self, id: QuoteId) {
        self.manager_ids.insert(id);
    }

    /// Untag a manager order (it resolved — filled/cancelled). Keeps the set
    /// bounded; safe to call with an unknown id.
    pub fn unmark_manager(&mut self, id: QuoteId) {
        self.manager_ids.remove(&id);
    }

    /// ALL live quotes for `symbol`, INCLUDING manager orders — for the runner's
    /// own order-lifecycle tracking (e.g. detecting a take-profit fill). The
    /// strategy must NOT use this; it should see only [`Self::live_quotes_for`].
    pub fn all_live_quotes_for(&self, symbol: &Symbol) -> Vec<(QuoteId, QuoteIntent)> {
        self.live_quotes
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
                        tif: TimeInForce::PostOnly,
                        kind: tikr_core::QuoteKind::Point,
                    },
                )
            })
            .collect()
    }

    /// Same as [`Self::live_quotes_for`] but writes into a caller-owned buffer
    /// (cleared first), so the hot per-event path can reuse one allocation
    /// instead of allocating a fresh Vec every call.
    pub fn live_quotes_into(&self, symbol: &Symbol, out: &mut Vec<(QuoteId, QuoteIntent)>) {
        out.clear();
        for q in self
            .live_quotes
            .iter()
            .filter(|q| &q.symbol == symbol && !self.manager_ids.contains(&q.id))
        {
            out.push((
                q.id,
                QuoteIntent {
                    symbol: q.symbol.clone(),
                    side: q.side,
                    price: q.price,
                    size: q.size_remaining,
                    // Live resting orders are by construction post-only here
                    // (FillSim's IOC/FOK paths return immediately and never
                    // enter live_quotes). Stamp PostOnly so strategies don't
                    // have to special-case.
                    tif: TimeInForce::PostOnly,
                    kind: tikr_core::QuoteKind::Point,
                },
            ));
        }
        // Include ALL in-flight Place ops — both live (venue id) and pure-paper
        // (no id yet). They occupy their lattice slot the moment they're sent;
        // excluding them makes the strategy think the slot is empty during the
        // submit-latency window and re-emit a DUPLICATE order at the same price
        // (acute on a fast book where many events fire before the ack). Live
        // ops carry their venue id; paper ops get the nil placeholder (the
        // strategy dedups by price, so the id is only cosmetic here).
        for p in &self.pending {
            match &p.op {
                Op::Place {
                    intent,
                    override_id,
                } if &intent.symbol == symbol
                    && !override_id.is_some_and(|id| self.manager_ids.contains(&id)) =>
                {
                    out.push((override_id.unwrap_or_else(QuoteId::nil), intent.clone()));
                }
                // A pending Replace's NEW intent occupies its slot the moment
                // it's sent, exactly like a Place — surface it so a
                // requote-driven strategy doesn't re-quote the slot mid-flight.
                Op::Replace { id, intent } if &intent.symbol == symbol => {
                    out.push((*id, intent.clone()));
                }
                _ => {}
            }
        }
    }

    /// Total committed order notional per side for `symbol` — both resting
    /// (`live_quotes`) AND in-flight pending `Place` ops — as `(bids, asks)`.
    /// The runner's order-placement inventory cap uses this so orders the bot
    /// has already SENT but the venue hasn't acked yet (submit latency) still
    /// count against the cap. Counting only resting orders lets a strategy
    /// pile up unbounded in-flight orders during the latency window, which all
    /// then promote and fill far past the intended cap.
    pub fn committed_notional_by_side(&self, symbol: &Symbol) -> (Decimal, Decimal) {
        let mut bid = Decimal::ZERO;
        let mut ask = Decimal::ZERO;
        // Manager (reduce-only) orders are excluded — they REDUCE inventory, so
        // they must not count toward the bot's adding-side committed exposure.
        for q in self
            .live_quotes
            .iter()
            .filter(|q| &q.symbol == symbol && !self.manager_ids.contains(&q.id))
        {
            let n = q.price.0 * q.size_remaining.0;
            match q.side {
                Side::Bid => bid += n,
                Side::Ask => ask += n,
            }
        }
        for p in &self.pending {
            if let Op::Place {
                intent,
                override_id,
            } = &p.op
                && &intent.symbol == symbol
                && !override_id.is_some_and(|id| self.manager_ids.contains(&id))
            {
                let n = intent.price.0 * intent.size.0;
                match intent.side {
                    Side::Bid => bid += n,
                    Side::Ask => ask += n,
                }
            }
        }
        (bid, ask)
    }

    /// 128-bit fingerprint of exactly the state [`Self::live_quotes_into`]
    /// reads to produce its output for `symbol`: the resting `live_quotes`
    /// (id, side, price, remaining size) plus in-flight `Place` ops carrying a
    /// venue id. Cheap (integer folds, no allocation, no `Symbol` clone) so it
    /// can run every event to gate the expensive rebuild. `price`/`side` never
    /// mutate in place — only `size_remaining` does, and entries are
    /// added/removed wholesale — so this captures every change that can alter
    /// the output. Order is stable (both loops iterate `Vec`s in place), so
    /// the fold is deterministic.
    fn open_quotes_signature(&self, symbol: &Symbol) -> u128 {
        // Two independent FNV-1a-style accumulators → 128-bit combined width,
        // making an aliasing collision (which would deterministically return a
        // stale set for one event) astronomically unlikely.
        let mut h1: u64 = 0xcbf2_9ce4_8422_2325;
        let mut h2: u64 = 0x1000_0000_01b3_27d4;
        #[inline]
        fn mix(h: &mut u64, x: u64) {
            *h = (*h ^ x).wrapping_mul(0x0000_0100_0000_01b3);
        }
        let mut count: u64 = 0;
        for q in self.live_quotes.iter().filter(|q| &q.symbol == symbol) {
            count += 1;
            let id = q.id.0.as_u128();
            mix(&mut h1, id as u64);
            mix(&mut h2, (id >> 64) as u64);
            let side = match q.side {
                Side::Bid => 0,
                Side::Ask => 1,
            };
            mix(&mut h1, q.price.0.mantissa() as u64);
            mix(&mut h2, (q.price.0.scale() as u64) << 1 | side);
            mix(&mut h1, q.size_remaining.0.mantissa() as u64);
            mix(&mut h2, q.size_remaining.0.scale() as u64);
        }
        for p in &self.pending {
            // Mirror live_quotes_into: fold every in-flight Place AND Replace
            // (their new intent) for `symbol`. Fold price/side/size + venue id
            // when present (paper has none → 0, still stable + change-sensitive).
            let folded = match &p.op {
                Op::Place {
                    intent,
                    override_id,
                } if &intent.symbol == symbol => {
                    Some((intent, override_id.map(|q| q.0.as_u128()).unwrap_or(0)))
                }
                Op::Replace { id, intent } if &intent.symbol == symbol => {
                    Some((intent, id.0.as_u128()))
                }
                _ => None,
            };
            if let Some((intent, id)) = folded {
                count += 1;
                let side_bit = match intent.side {
                    Side::Bid => 0u64,
                    Side::Ask => 1u64,
                };
                mix(&mut h1, id as u64);
                mix(&mut h2, (id >> 64) as u64);
                mix(&mut h1, intent.price.0.mantissa() as u64);
                mix(&mut h2, (intent.price.0.scale() as u64) << 1 | side_bit);
                mix(&mut h1, intent.size.0.mantissa() as u64);
                mix(&mut h2, intent.size.0.scale() as u64);
            }
        }
        mix(&mut h1, count);
        mix(&mut h2, count);
        ((h1 as u128) << 64) | (h2 as u128)
    }

    /// Resting + in-flight open quotes for `symbol`, as the per-event strategy
    /// loop consumes them. Returns a borrowed slice from an internal cache that
    /// is rebuilt only when the order set actually changes (see
    /// [`Self::open_quotes_signature`]); on the common pure-book-update event
    /// the cached slice is returned without re-cloning any `Symbol`. Bit-for-
    /// bit equivalent to [`Self::live_quotes_for`].
    pub fn open_quotes(&mut self, symbol: &Symbol) -> &[(QuoteId, QuoteIntent)] {
        let sig = self.open_quotes_signature(symbol);
        let fresh = self.open_quotes_cache_sig == Some(sig)
            && self.open_quotes_cache_symbol.as_ref() == Some(symbol);
        if !fresh {
            // Take the cache out so `live_quotes_into` can borrow `&self`
            // alongside `&mut buf`, then put it back.
            let mut buf = std::mem::take(&mut self.open_quotes_cache);
            self.live_quotes_into(symbol, &mut buf);
            self.open_quotes_cache = buf;
            self.open_quotes_cache_sig = Some(sig);
            self.open_quotes_cache_symbol = Some(symbol.clone());
        }
        &self.open_quotes_cache
    }

    /// Schedule a strategy action for venue submission at `now + appropriate_latency_ms`.
    pub fn on_action(&mut self, action: Action, now: Timestamp) {
        // Each op draws its own latency (base + jitter), so a spiked submit
        // can land after a later, faster one — the pending queue is re-sorted
        // below, which models real out-of-order arrival.
        let submit_base = self.cfg.submit_latency_ms;
        let cancel_base = self.cfg.cancel_latency_ms;
        let (scheduled, op) = match action {
            Action::Quote(intent) => (
                // `.max(1)`: with `submit_latency_ms == 0` and no jitter,
                // `sample_latency_ns` returns exactly 0, which would let
                // `apply_pending`'s `<=` promotion make the order live in
                // THIS SAME `on_market_event` call — before a Trade event
                // that triggered it (e.g. a strategy quoting in reaction to
                // a print) has been matched. No real venue acks an order
                // before the event that caused it; forcing at least 1ns of
                // delay guarantees the op only becomes live strictly after
                // the current event.
                now.0
                    .saturating_add(self.sample_latency_ns(submit_base).max(1)),
                Op::Place {
                    intent,
                    override_id: None,
                },
            ),
            Action::Requote { id, intent } => (
                now.0
                    .saturating_add(self.sample_latency_ns(submit_base).max(1)),
                Op::Replace { id, intent },
            ),
            Action::Cancel(id) => (
                now.0.saturating_add(self.sample_latency_ns(cancel_base)),
                Op::Cancel(id),
            ),
            Action::CancelAll => (
                now.0.saturating_add(self.sample_latency_ns(cancel_base)),
                Op::CancelAll,
            ),
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
        let submit_base = self.cfg.submit_latency_ms;
        // `.max(1)`: same same-tick look-ahead guard as `on_action` — see
        // its comment on the `Action::Quote` arm.
        let submit_ns = self.sample_latency_ns(submit_base).max(1);
        self.pending.push(PendingOp {
            scheduled_ts_ns: now.0.saturating_add(submit_ns),
            op: Op::Place {
                intent,
                override_id: Some(venue_id),
            },
        });
        self.pending.sort_by_key(|p| p.scheduled_ts_ns);
    }

    /// Switch the sim into live-mirror mode (see the `live_mirror` field):
    /// resting-order registry + book tracking only; no simulated fills,
    /// silent cancels, or queue decay.
    pub fn set_live_mirror(&mut self, on: bool) {
        self.live_mirror = on;
    }

    /// Match queued open quotes against `ev`; emit fills for any quotes
    /// taken out by the trade-through model. Also emits taker fills for any
    /// pending IOC/FOK ops that became eligible this tick.
    pub fn on_market_event(&mut self, ev: &MarketEvent, now: Timestamp) -> Vec<Fill> {
        if self.live_mirror {
            // Real venue owns fills/cancels; only advance the book mirror
            // and promote pending ops so resting orders become visible.
            self.last_event_ts_ns = Some(now.0);
            let fills = self.apply_pending(now);
            if let MarketEvent::BookUpdate { snapshot } = ev {
                self.update_book_state(snapshot);
            }
            return fills;
        }
        // Front-cancel queue decay must read the prior event ts BEFORE
        // silent_cancel_tick advances `last_event_ts_ns`.
        self.decay_queue_ahead(now);
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
    /// Decay every live order's `queue_ahead` by `exp(-rate · dt_secs)` to
    /// model front-of-queue cancellations that L2 net-depth can't reveal.
    /// Reads `last_event_ts_ns` for dt; does NOT advance it (silent_cancel_tick
    /// owns that, and runs right after).
    fn decay_queue_ahead(&mut self, now: Timestamp) {
        let lambda = self.cfg.queue_cancel_decay_per_sec;
        if lambda <= 0.0 || self.live_quotes.is_empty() {
            return;
        }
        let Some(prev) = self.last_event_ts_ns else {
            return;
        };
        let dt_ns = now.0.saturating_sub(prev);
        if dt_ns == 0 {
            return;
        }
        let dt_secs = dt_ns as f64 / 1_000_000_000.0;
        let factor = (-lambda * dt_secs).exp();
        if factor >= 1.0 {
            return;
        }
        let Some(f) = Decimal::from_f64_retain(factor) else {
            return;
        };
        for q in &mut self.live_quotes {
            if q.queue_ahead > Decimal::ZERO {
                q.queue_ahead *= f;
            }
        }
    }

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
        self.committed_dirty = true;
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
                    // PRESERVE the id across a replace. A requote amends in place
                    // (WS/REST `order.modify` keeps the venue `orderId`), so the
                    // tracked id must stay equal to the venue id — otherwise the
                    // replaced quote gets a fresh random `QuoteId::default()`,
                    // which the next requote recovers as a garbage orderId and
                    // the venue rejects with `-1102`.
                    //
                    // Snapshot + evict the order being replaced BEFORE gating
                    // the new intent, so the gates/cross checks run net of the
                    // slot it's replacing (a real `order.modify` doesn't
                    // double-reserve margin/cash for the same order) — and so
                    // a REJECT can restore it, matching live Binance modify
                    // semantics where a rejected modify leaves the original
                    // order resting untouched. Any stale in-flight op still
                    // referencing `id` is purged either way.
                    let restore = self
                        .live_quotes
                        .iter()
                        .position(|q| q.id == id)
                        .map(|idx| self.live_quotes.remove(idx));
                    if restore.is_some() {
                        self.committed_dirty = true;
                    }
                    self.purge_pending_id(id);
                    let rejections_before = self.pending_rejections.len();
                    match self.place_or_reject(intent, Timestamp(p.scheduled_ts_ns), Some(id)) {
                        Some(f) => fills.push(f),
                        None if self.pending_rejections.len() > rejections_before => {
                            // Rejected — restore the original resting order
                            // verbatim rather than leaving the strategy
                            // blind to a quote it thinks is still live.
                            if let Some(orig) = restore {
                                self.live_quotes.push(orig);
                                self.committed_dirty = true;
                            }
                        }
                        None => {
                            // Accepted (now resting under `id`), or a
                            // non-crossing IOC/FOK Requote that legitimately
                            // drops with no fill and no rejection — either
                            // way the replace deliberately consumed the old
                            // order.
                        }
                    }
                }
                Op::Cancel(id) => self.cancel_id(id),
                Op::CancelAll => {
                    // Strategy-issued CancelAll must not remove MANAGER-owned
                    // resting orders (take-profit / bagger) — same invariant
                    // as `drop_quotes_for`: dropping a manager id here would
                    // false-latch the runner's fill detection (id-absence ==
                    // "the TP filled"). `Action::CancelAll` carries no symbol
                    // to scope by, but every `FillSim` instance is
                    // single-symbol in practice (`tikr_backtest::runner::run`
                    // and `tikr_paper::runner::run` both take one fixed
                    // `symbol` for the whole run — see their doc comments),
                    // so the real bug here was manager exposure, not
                    // cross-symbol leakage.
                    self.live_quotes
                        .retain(|q| self.manager_ids.contains(&q.id));
                    self.committed_dirty = true;
                }
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
        // Synthetic Binance `-1015` (too many orders). Resting orders count
        // against the venue's per-symbol `MAX_NUM_ORDERS` filter; a Place that
        // would exceed the cap is rejected before it can rest. Only orders that
        // actually rest count — IOC/FOK fill-or-cancel immediately and never
        // occupy a slot, so they bypass this gate (checked below). Without the
        // cap, strategies that place-and-never-cancel grow `live_quotes`
        // unboundedly → the O(open_orders) per-event scan degrades to
        // O(events²) and the backtest effectively hangs.
        if let Some(cap) = self.cfg.max_open_orders
            && !matches!(intent.tif, TimeInForce::IOC | TimeInForce::FOK)
        {
            let resting = self
                .live_quotes
                .iter()
                .filter(|q| q.symbol == intent.symbol)
                .count();
            if resting >= cap as usize {
                self.pending_rejections
                    .push((intent.clone(), "too many orders (paper -1015)".to_string()));
                return None;
            }
        }
        if matches!(intent.tif, TimeInForce::PostOnly) && self.would_cross(&intent) {
            // Match the real Binance -5022 reject verbatim so strategies gate
            // on one reason string across sim and live — a sim-only string
            // would let reject handling diverge in backtest vs the exchange.
            self.pending_rejections.push((
                intent.clone(),
                "binance error (code -5022): Due to the order could not be \
                 executed as maker, the Post Only order will be rejected. The \
                 order will not be recorded in the order history"
                    .to_string(),
            ));
            return None;
        }
        if self.cfg.spot {
            // SPOT mode: replace the futures Gate 1 + Gate 2 with cash + asset
            // balance gates. No margin, no leverage, no shorting.
            //
            // Bid gate: committed resting-bid notional + this intent's notional
            // must fit within available spot_cash.
            // Ask gate: committed resting-ask size + this intent's size must fit
            // within held spot_units (no shorting — can't sell more than held).
            //
            // IOC/FOK bypass the resting-order commitment check (they fill
            // immediately or are dropped; they never occupy a resting slot) but
            // still need the balance to be available NOW for the fill.
            let intent_notional = (intent.price.0 * intent.size.0).round_dp(8);
            // IOC/FOK never rest, so they reserve no resting commitment. Cached
            // committed sums (`committed_for`) keep this O(1) during a reject
            // storm — see `committed` field docs.
            let is_immediate = matches!(intent.tif, TimeInForce::IOC | TimeInForce::FOK);
            let (committed_bid_notional, _, committed_ask_size) = if is_immediate {
                (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO)
            } else {
                self.committed_for(&intent.symbol)
            };
            match intent.side {
                Side::Bid => {
                    // Reserve the maker fee alongside the notional: a fill
                    // deducts BOTH from spot_cash (see `match_trade`'s SPOT
                    // block), so gating on notional alone under-reserves and
                    // lets a dense bid ladder fill its cash balance negative.
                    let fee_frac = Decimal::from(self.cfg.fees.maker_bps) / Decimal::from(10_000);
                    let reserved = (intent_notional * (Decimal::ONE + fee_frac)).round_dp(8);
                    if (committed_bid_notional + reserved).round_dp(8) > self.spot_cash {
                        self.pending_rejections
                            .push((intent.clone(), "spot: insufficient cash".to_string()));
                        return None;
                    }
                }
                Side::Ask => {
                    if (committed_ask_size + intent.size.0) > self.spot_units {
                        self.pending_rejections
                            .push((intent.clone(), "spot: insufficient asset".to_string()));
                        return None;
                    }
                }
            }
        } else {
            // FUTURES mode: Gate 1 + Gate 2 (margin + position cap).
            //
            // Two DISTINCT gates, both dynamic vs the running wallet. Conflating
            // them (one worst-case-all-fill check against the tightest cap) made a
            // dense frozen-lattice unusable: ~50 resting bids of $5 sum to ~$250, so
            // a 200%-of-$100 position cap ($200) rejected EVERY bid on every
            // reconcile even at zero position — stranding the grid one-sided. Split:
            //
            //   Gate 1 — MARGIN / buying power (the real Binance `-2019`): total
            //   EXPOSURE (open position + all resting orders on a side, worst-case
            //   all-fill) must fit `wallet × leverage`. Resting orders reserve margin
            //   until they fill/cancel, so the additive worst-case is right HERE — a
            //   grid genuinely can't rest more notional than its buying power backs.
            //   At 25× this is loose ($100 → $2,500), biting only over-leveraged
            //   ladders.
            //
            //   Gate 2 — POSITION cap (`max_position_frac` × wallet, and the optional
            //   fixed `max_position_notional_usdt`): bounds ACTUAL signed position,
            //   PER-ORDER (projected `pos ± this order`), NOT all-resting-fill —
            //   resting maker orders aren't position until they fill. `frozen_reconcile`
            //   re-checks on every fill, so per-order holds |pos| at the cap plus at
            //   most a few orders of slippage between reconciles, without mass-
            //   rejecting the adding side.
            //
            // `pos` is `position_units × mark price` (see the
            // `position_notional` method) — computed fresh at gate time
            // rather than accumulated as cash flow, so realized PnL from
            // round trips never leaks into the margin check.
            //
            // Scale note: `position_notional` pre-rounds to 8 dp, but
            // `price × size` can independently produce scale 8+ (e.g. DOGE
            // 0.20123 × 5-dp size). Round both operands before adding so neither the
            // product nor the sum overflows rust_decimal's 96-bit mantissa.
            let pos = self.position_notional(&intent.symbol, intent.price);
            let intent_delta = (intent.price.0 * intent.size.0).round_dp(8);

            // Gate 1 — margin / buying power (worst-case exposure vs wallet × lev).
            // `current_wallet == 0` (unset) skips this gate (test/back-compat path).
            if self.cfg.leverage > Decimal::ZERO && self.current_wallet > Decimal::ZERO {
                let buying_power = (self.current_wallet * self.cfg.leverage).round_dp(8);
                // Cached resting bid/ask notional (O(1) outside a live_quotes
                // mutation) — see `committed` field docs.
                let (mut resting_bids, resting_ask_notional, _) =
                    self.committed_for(&intent.symbol);
                let mut resting_asks = resting_ask_notional;
                match intent.side {
                    Side::Bid => resting_bids = (resting_bids + intent_delta).round_dp(8),
                    Side::Ask => resting_asks = (resting_asks + intent_delta).round_dp(8),
                }
                let worst_long = pos + resting_bids;
                let worst_short = pos - resting_asks;
                if worst_long > buying_power || worst_short < -buying_power {
                    self.pending_rejections.push((
                        intent.clone(),
                        "margin insufficient (paper -2019)".to_string(),
                    ));
                    return None;
                }
            }

            // Gate 2 — strategy position cap (per-order projected |position|). The
            // tightest of the `%`-of-wallet cap and the explicit fixed cap binds.
            let pos_cap = {
                let frac = (self.cfg.max_position_frac > Decimal::ZERO
                    && self.current_wallet > Decimal::ZERO)
                    .then(|| (self.current_wallet * self.cfg.max_position_frac).round_dp(8));
                match (self.cfg.max_position_notional_usdt, frac) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (Some(a), None) => Some(a),
                    (None, b) => b,
                }
            };
            if let Some(cap) = pos_cap {
                // Only THIS order's fill moves position; resting siblings aren't
                // position yet. Reject if filling it would push signed position past
                // ±cap. At the cap the adding side is rejected while the reducing
                // side still rests — correct risk behaviour, not stranding.
                let projected = match intent.side {
                    Side::Bid => pos + intent_delta,
                    Side::Ask => pos - intent_delta,
                };
                if projected > cap || projected < -cap {
                    self.pending_rejections
                        .push((intent.clone(), "position cap reached (paper)".to_string()));
                    return None;
                }
            }
        }
        // IOC / FOK: if the intent crosses the live touch, fill immediately
        // at the touch price as a taker. If it doesn't cross, drop silently
        // (IOC = unfilled remainder gets cancelled; we treat 0 fill as full
        // cancel). FOK additionally requires the FULL size to be fillable
        // right now — see the all-or-nothing check below. Partial-fill
        // modeling for IOC is a future refinement.
        if matches!(intent.tif, TimeInForce::IOC | TimeInForce::FOK) {
            if !self.book_state.contains_key(&intent.symbol) {
                self.book_state
                    .insert(intent.symbol.clone(), BookState::default());
            }
            let st = self
                .book_state
                .get_mut(&intent.symbol)
                .expect("inserted above");
            // Walk the book: IOC consumes liquidity level by level from
            // the touch outward. Average fill price worsens as we eat
            // deeper into the order book — the realistic taker
            // experience that's invisible when modelling fills as
            // "single-price at touch". On thin books or large IOCs the
            // PnL impact is significant (Hydra SL exits, pyramid adds in
            // fast moves both go through this path).
            let consumed = st.walk_book_ioc(intent.side, intent.size.0, intent.price);
            if consumed.is_empty() {
                return None;
            }
            let total_qty: Decimal = consumed.iter().map(|(_, q)| *q).sum();
            if matches!(intent.tif, TimeInForce::FOK) && total_qty < intent.size.0 {
                // FOK is all-or-nothing: the book can't fill the whole size
                // right now, so the order dies untouched — no partial fill,
                // no book mutation (unlike IOC, which takes what it can get
                // and cancels the remainder).
                return None;
            }
            // Weighted-average price = Σ(p × q) / Σq. Round at each
            // multiplication to bound Decimal scale (same scale-overflow
            // story as the original at-touch fill below).
            let notional: Decimal = consumed.iter().map(|(p, q)| (p.0 * *q).round_dp(8)).sum();
            let avg_price = (notional / total_qty).round_dp(8);
            let fee_amount = (notional * Decimal::from(self.cfg.fees.taker_bps)
                / Decimal::from(10_000))
            .round_dp(8);
            // Decrement each consumed level so subsequent fills (and the
            // next BookUpdate's cancel-attribution) see correct
            // remaining depth.
            let touched_side = match intent.side {
                Side::Bid => Side::Ask,
                Side::Ask => Side::Bid,
            };
            for (lvl_price, lvl_qty) in &consumed {
                st.decrement_level(touched_side, *lvl_price, *lvl_qty);
            }
            // Position tracked in signed BASE UNITS, not cash flow — see
            // `position_units` field docs.
            if !self.position_units.contains_key(&intent.symbol) {
                self.position_units
                    .insert(intent.symbol.clone(), Decimal::ZERO);
            }
            let entry = self
                .position_units
                .get_mut(&intent.symbol)
                .expect("inserted above");
            match intent.side {
                Side::Bid => *entry += total_qty,
                Side::Ask => *entry -= total_qty,
            }
            // SPOT mode: update cash + asset balances on every taker fill.
            // The fee is quote-denominated (USDC) and always paid, so it
            // reduces cash on both buys and sells (a negative maker rebate
            // would add). Modelled in quote for both sides for simplicity.
            if self.cfg.spot {
                match intent.side {
                    Side::Bid => {
                        self.spot_cash = (self.spot_cash - notional).round_dp(8);
                        self.spot_units = (self.spot_units + total_qty).round_dp(8);
                    }
                    Side::Ask => {
                        self.spot_cash = (self.spot_cash + notional).round_dp(8);
                        self.spot_units = (self.spot_units - total_qty).round_dp(8);
                    }
                }
                self.spot_cash = (self.spot_cash - fee_amount).round_dp(8);
            }
            return Some(Fill {
                quote_id: QuoteId::new(),
                price: Price(avg_price),
                size: Size(total_qty),
                fee_asset: intent.symbol.quote.clone(),
                fee_amount,
                fee_quote: Notional(fee_amount),
                side: intent.side,
                ts,
                // FOK reaching here always fills the full size (checked
                // above — otherwise it already returned `None`). IOC may
                // PARTIALLY fill if the book runs out of depth (or the
                // limit price is breached) before the intent size is
                // consumed. `is_full = (consumed == intent.size)`.
                is_full: total_qty >= intent.size.0,
                trade_id: None,
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
        self.committed_dirty = true;
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
        self.committed_dirty = true;
        self.purge_pending_id(id);
    }

    /// Drop any not-yet-applied Place / Replace whose venue id matches `id`
    /// — otherwise a pending entry would get promoted into `live_quotes` by
    /// the next `apply_pending`, even though the strategy already cancelled
    /// (or the venue already filled) the underlying order. Shared by
    /// `cancel_id` and the `Op::Replace` handler in `apply_pending`.
    fn purge_pending_id(&mut self, id: QuoteId) {
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

    /// Live mode: venue confirmed all symbol orders were cancelled. Drop all
    /// matching live and in-flight quotes from the local mirror immediately.
    /// MANAGER-owned entries (runner take-profit / bagger) survive: a
    /// strategy-triggered cancel-all must not make the runner's TP id vanish
    /// from the mirror — id-absence is how the runner detects a TP *fill*,
    /// so dropping it here would false-latch `tp_taken`. The runner cancels
    /// its own orders explicitly via [`Self::drop_quote`].
    pub fn drop_quotes_for(&mut self, symbol: &Symbol) {
        self.live_quotes
            .retain(|q| &q.symbol != symbol || self.manager_ids.contains(&q.id));
        self.committed_dirty = true;
        self.pending.retain(|p| match &p.op {
            Op::Place {
                intent,
                override_id,
            } => {
                &intent.symbol != symbol
                    || override_id.is_some_and(|id| self.manager_ids.contains(&id))
            }
            Op::Replace { intent, .. } => &intent.symbol != symbol,
            Op::Cancel(_) | Op::CancelAll => true,
        });
    }

    /// True if `id` rests in the mirror OR is still in-flight in the pending
    /// queue (submit latency not yet elapsed). Live mode distinguishes a
    /// FILLED order (gone from both) from one merely not yet promoted — on a
    /// quiet symbol an enqueued place can sit pending for seconds.
    pub fn is_tracked(&self, id: QuoteId) -> bool {
        self.live_quotes.iter().any(|q| q.id == id)
            || self.pending.iter().any(|p| match &p.op {
                Op::Place { override_id, .. } => *override_id == Some(id),
                Op::Replace { id: rid, .. } => *rid == id,
                Op::Cancel(_) | Op::CancelAll => false,
            })
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
        let removed = before - self.live_quotes.len();
        if removed > 0 {
            // The spot-cash / margin gates read the cached commitment; a
            // stale cache keeps counting the dropped ghosts as resting.
            self.committed_dirty = true;
        }
        removed
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

        self.committed_dirty = true;
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
        if !self.book_state.contains_key(&snapshot.symbol) {
            self.book_state
                .insert(snapshot.symbol.clone(), BookState::default());
        }
        let st = self
            .book_state
            .get_mut(&snapshot.symbol)
            .expect("inserted above");
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
            if new_agg <= Decimal::ZERO {
                // Level VANISHED from the L2 snapshot. This is ambiguous and
                // almost never means "everyone ahead of us cancelled" — far
                // more often price ticked past the level, or the depth simply
                // fell out of the capped/throttled top-N snapshot. The L2 feed
                // under-represents true book depth, so zeroing queue_ahead here
                // would falsely promote us to front-of-queue and systematically
                // OVER-FILL — catastrophic for dense grids whose levels empty
                // and refill on every wiggle (sim churned 50x the live fill
                // count). Preserve queue_ahead; only an OBSERVABLE partial
                // shrink (new_agg > 0, below) is credited as cancels-ahead.
                continue;
            }
            // Partial shrink with the level still present — observable cancels
            // ahead of us. Scale queue_ahead proportionally (uniform-cancel).
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
        match taker_side {
            Side::Bid => {
                self.diag.taker_buy_trades += 1;
                self.diag.taker_buy_qty += trade_size.0;
            }
            Side::Ask => {
                self.diag.taker_sell_trades += 1;
                self.diag.taker_sell_qty += trade_size.0;
            }
        }
        let mut out = Vec::new();
        let mut trade_remaining = trade_size.0;

        // Price priority: gather every quote this trade is eligible to
        // take, sorted best-price-first, so a worse-priced quote can never
        // steal the trade ahead of a better one purely because it sits
        // earlier in `live_quotes` (insertion order). Eligibility is
        // one-sided per event — `quote_takes_trade` only matches our Bids
        // against an Ask taker or our Asks against a Bid taker — so this is
        // a single sort, never a merge of both sides.
        let mut eligible: Vec<usize> = self
            .live_quotes
            .iter()
            .enumerate()
            .filter(|(_, q)| {
                q.symbol == *symbol && quote_takes_trade(q.side, q.price, taker_side, trade_price)
            })
            .map(|(i, _)| i)
            .collect();
        match taker_side {
            // Taker sold — hits our Bids. Highest (best) bid first.
            Side::Ask => eligible.sort_by(|&a, &b| {
                self.live_quotes[b]
                    .price
                    .0
                    .cmp(&self.live_quotes[a].price.0)
            }),
            // Taker bought — hits our Asks. Lowest (best) ask first.
            Side::Bid => eligible.sort_by(|&a, &b| {
                self.live_quotes[a]
                    .price
                    .0
                    .cmp(&self.live_quotes[b].price.0)
            }),
        }

        let mut drained: Vec<usize> = Vec::new();
        for i in eligible {
            if trade_remaining <= Decimal::ZERO {
                break;
            }
            let q = &mut self.live_quotes[i];
            match q.side {
                Side::Bid => self.diag.bid_eligible += 1,
                Side::Ask => self.diag.ask_eligible += 1,
            }

            // Queue priority: a trade printed AT our exact price consumes
            // the orders resting ahead of us before reaching our quote —
            // decrement queue_ahead 1:1, and the book aggregate at our
            // level (so the next BookUpdate doesn't mis-attribute this as a
            // cancel). A trade printed STRICTLY THROUGH our price (a sell
            // below our bid / a buy above our ask) means the market already
            // walked past our level to get here, so whatever was ahead of
            // us is necessarily gone — the queue is swept to zero. That
            // sweep happened at OTHER price levels, not ours, so it must
            // NOT decrement our book aggregate: that volume was never at
            // our price to begin with.
            let q_side = q.side;
            let q_price = q.price;
            if trade_price.0 == q_price.0 {
                let ate = q.queue_ahead.min(trade_remaining);
                if ate > Decimal::ZERO {
                    match q_side {
                        Side::Bid => self.diag.bid_queue_eaten += ate,
                        Side::Ask => self.diag.ask_queue_eaten += ate,
                    }
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
            } else if q.queue_ahead > Decimal::ZERO {
                match q_side {
                    Side::Bid => self.diag.bid_queue_eaten += q.queue_ahead,
                    Side::Ask => self.diag.ask_queue_eaten += q.queue_ahead,
                }
                q.queue_ahead = Decimal::ZERO;
            }
            let q = &mut self.live_quotes[i];

            let fill_amount = q.size_remaining.0.min(trade_remaining);
            if fill_amount > Decimal::ZERO {
                match q_side {
                    Side::Bid => {
                        self.diag.bid_filled_qty += fill_amount;
                        self.diag.bid_fills += 1;
                    }
                    Side::Ask => {
                        self.diag.ask_filled_qty += fill_amount;
                        self.diag.ask_fills += 1;
                    }
                }
            }
            let fill_price = q.price;
            // fee_amount is signed; positive = paid, negative = rebated.
            let fee_amount = (fill_price.0.round_dp(8)
                * fill_amount.round_dp(8)
                * Decimal::from(self.cfg.fees.maker_bps)
                / Decimal::from(10_000))
            .round_dp(8);
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
                trade_id: None,
            });
            // Position tracked in signed BASE UNITS, not cash flow — see
            // `position_units` field docs.
            if !self.position_units.contains_key(symbol) {
                self.position_units.insert(symbol.clone(), Decimal::ZERO);
            }
            let entry = self.position_units.get_mut(symbol).expect("inserted above");
            match q.side {
                Side::Bid => *entry += fill_amount,
                Side::Ask => *entry -= fill_amount,
            }
            // Same scale-bound treatment as the IOC arm; see comment above.
            let delta = (fill_price.0 * fill_amount).round_dp(8);
            // SPOT mode: update cash + asset balances on every maker fill.
            // `fee_amount` is quote-denominated and signed (positive = paid,
            // negative = maker rebate); subtract it from cash on both sides.
            if self.cfg.spot {
                match q.side {
                    Side::Bid => {
                        self.spot_cash = (self.spot_cash - delta).round_dp(8);
                        self.spot_units = (self.spot_units + fill_amount).round_dp(8);
                    }
                    Side::Ask => {
                        self.spot_cash = (self.spot_cash + delta).round_dp(8);
                        self.spot_units = (self.spot_units - fill_amount).round_dp(8);
                    }
                }
                self.spot_cash = (self.spot_cash - fee_amount).round_dp(8);
            }
            q.size_remaining = Size(q.size_remaining.0 - fill_amount);
            self.committed_dirty = true;
            trade_remaining -= fill_amount;
            if q.size_remaining.0 == Decimal::ZERO {
                drained.push(i);
            }
        }

        // Remove fully-filled quotes highest-index-first so earlier indices
        // still queued for removal stay valid.
        drained.sort_unstable_by(|a, b| b.cmp(a));
        for i in drained {
            self.live_quotes.remove(i);
        }

        out
    }
}

impl Drop for FillSim {
    fn drop(&mut self) {
        // TEMP audit: dump per-side eligible/queue/fill tallies when the env
        // flag is set, to diagnose queue-model fill asymmetry.
        if std::env::var("TIKR_FILLSIM_DIAG").is_ok() {
            let d = &self.diag;
            let rate = |filled: Decimal, queue: Decimal| -> f64 {
                use rust_decimal::prelude::ToPrimitive;
                let tot = (filled + queue).to_f64().unwrap_or(0.0);
                if tot > 0.0 {
                    filled.to_f64().unwrap_or(0.0) / tot * 100.0
                } else {
                    0.0
                }
            };
            eprintln!(
                "FILLSIM_DIAG bid: eligible={} queue_eaten={} filled={} fills={} fill_share={:.1}%",
                d.bid_eligible,
                d.bid_queue_eaten,
                d.bid_filled_qty,
                d.bid_fills,
                rate(d.bid_filled_qty, d.bid_queue_eaten)
            );
            eprintln!(
                "FILLSIM_DIAG ask: eligible={} queue_eaten={} filled={} fills={} fill_share={:.1}%",
                d.ask_eligible,
                d.ask_queue_eaten,
                d.ask_filled_qty,
                d.ask_fills,
                rate(d.ask_filled_qty, d.ask_queue_eaten)
            );
            eprintln!(
                "FILLSIM_DIAG raw-trades: taker_BUY={} (qty {}) taker_SELL={} (qty {})  [BUY lifts asks, SELL hits bids]",
                d.taker_buy_trades, d.taker_buy_qty, d.taker_sell_trades, d.taker_sell_qty
            );
        }
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
            leverage: rust_decimal::Decimal::ZERO,
            max_position_frac: rust_decimal::Decimal::ZERO,
            silent_cancel_rate_per_min: 0.0,
            rng_seed: 0,
            latency_jitter_ms: 0,
            max_open_orders: None,
            queue_cancel_decay_per_sec: 0.0,
            spot: false,
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
    fn max_open_orders_rejects_beyond_cap() {
        let sym = make_symbol();
        let mut cfg = default_cfg();
        cfg.submit_latency_ms = 0;
        cfg.max_open_orders = Some(3);
        let mut sim = FillSim::new(cfg);

        // Seed book: bid=100, ask=101. Resting bids below 100 never cross.
        let book = MarketEvent::BookUpdate {
            snapshot: make_book(&sym, 100, 101),
        };
        let _ = sim.on_market_event(&book, Timestamp(0));

        // Submit 5 resting post-only bids; only the first 3 may rest.
        for (i, px) in [90, 89, 88, 87, 86].into_iter().enumerate() {
            sim.on_action(
                Action::Quote(make_intent(&sym, Side::Bid, px, 1, TimeInForce::PostOnly)),
                Timestamp(i as u64),
            );
        }
        // Promote the pending Places (latency 0 → all due immediately).
        let _ = sim.on_market_event(
            &MarketEvent::Heartbeat {
                ts: Timestamp(1_000_000),
            },
            Timestamp(1_000_000),
        );

        assert_eq!(
            sim.live_quotes.len(),
            3,
            "cap must bound resting orders at 3"
        );
        let rejections = sim.drain_rejections();
        assert_eq!(rejections.len(), 2, "2 places beyond the cap rejected");
        assert!(
            rejections.iter().all(|(_, r)| r.contains("-1015")),
            "rejection reason is the synthetic too-many-orders code"
        );

        // IOC orders bypass the cap (they never rest). One that crosses fills;
        // resting count is unchanged.
        sim.on_action(
            Action::Quote(make_intent(&sym, Side::Bid, 200, 1, TimeInForce::IOC)),
            Timestamp(2_000_000),
        );
        let fills = sim.on_market_event(
            &MarketEvent::Heartbeat {
                ts: Timestamp(3_000_000),
            },
            Timestamp(3_000_000),
        );
        assert_eq!(fills.len(), 1, "IOC crosses and fills despite full book");
        assert_eq!(
            sim.live_quotes.len(),
            3,
            "IOC does not occupy a resting slot"
        );
    }

    // ── Margin / position-cap gates ──────────────────────────────────────────

    /// A dense bid ladder whose SUMMED notional exceeds the position cap must
    /// still all rest, as long as each order's own fill stays under the cap at
    /// the current (zero) position. Regression for the old worst-case-all-fill
    /// gate, which summed every resting bid and mass-rejected one whole side of
    /// a frozen-lattice grid (stranding it) on a small-wallet `%` cap.
    #[test]
    fn dense_ladder_not_mass_rejected_under_position_cap() {
        let sym = make_symbol();
        let mut cfg = default_cfg();
        cfg.submit_latency_ms = 0;
        cfg.leverage = Decimal::from(25); // buying power 25 × 100 = 2500, loose
        cfg.max_position_frac = Decimal::from(2); // cap = 2 × wallet = 200
        let mut sim = FillSim::new(cfg);
        sim.set_wallet(Decimal::from(100));

        // bid=100/ask=101. Five resting bids ~ $90 each → Σ ≈ $450 > $200 cap,
        // but each single fill (≤ $90) leaves |pos| under the cap.
        let _ = sim.on_market_event(
            &MarketEvent::BookUpdate {
                snapshot: make_book(&sym, 100, 101),
            },
            Timestamp(0),
        );
        for (i, px) in [90, 89, 88, 87, 86].into_iter().enumerate() {
            sim.on_action(
                Action::Quote(make_intent(&sym, Side::Bid, px, 1, TimeInForce::PostOnly)),
                Timestamp(i as u64),
            );
        }
        let _ = sim.on_market_event(
            &MarketEvent::Heartbeat {
                ts: Timestamp(1_000_000),
            },
            Timestamp(1_000_000),
        );

        assert_eq!(
            sim.live_quotes.len(),
            5,
            "all 5 bids rest — per-order cap, not summed"
        );
        assert!(
            sim.drain_rejections().is_empty(),
            "no position-cap rejections at zero position"
        );
    }

    /// The position cap still bounds: an order whose OWN fill would push signed
    /// position past the cap is rejected (gate 2), while leverage is loose enough
    /// that the margin gate (gate 1) does not fire.
    #[test]
    fn position_cap_rejects_single_order_past_cap() {
        let sym = make_symbol();
        let mut cfg = default_cfg();
        cfg.submit_latency_ms = 0;
        cfg.leverage = Decimal::from(25); // buying power 2500 — won't bind
        cfg.max_position_frac = Decimal::new(5, 1); // 0.5 × 100 = cap 50
        let mut sim = FillSim::new(cfg);
        sim.set_wallet(Decimal::from(100));

        let _ = sim.on_market_event(
            &MarketEvent::BookUpdate {
                snapshot: make_book(&sym, 100, 101),
            },
            Timestamp(0),
        );
        // One bid ~ $90 notional > $50 cap → rejected by the position gate.
        sim.on_action(
            Action::Quote(make_intent(&sym, Side::Bid, 90, 1, TimeInForce::PostOnly)),
            Timestamp(0),
        );
        let _ = sim.on_market_event(
            &MarketEvent::Heartbeat {
                ts: Timestamp(1_000_000),
            },
            Timestamp(1_000_000),
        );

        assert_eq!(sim.live_quotes.len(), 0, "order past cap does not rest");
        let rej = sim.drain_rejections();
        assert_eq!(rej.len(), 1);
        assert!(
            rej[0].1.contains("position cap"),
            "rejected by the position gate, got: {}",
            rej[0].1
        );
    }

    /// The margin gate (gate 1) still rejects when worst-case exposure exceeds
    /// buying power = wallet × leverage, with the synthetic `-2019` reason.
    #[test]
    fn margin_gate_rejects_over_buying_power() {
        let sym = make_symbol();
        let mut cfg = default_cfg();
        cfg.submit_latency_ms = 0;
        cfg.leverage = Decimal::from(2); // buying power 2 × 10 = 20
        // no max_position_frac → only the margin gate is active
        let mut sim = FillSim::new(cfg);
        sim.set_wallet(Decimal::from(10));

        let _ = sim.on_market_event(
            &MarketEvent::BookUpdate {
                snapshot: make_book(&sym, 100, 101),
            },
            Timestamp(0),
        );
        // One bid ~ $90 ≫ $20 buying power → margin reject.
        sim.on_action(
            Action::Quote(make_intent(&sym, Side::Bid, 90, 1, TimeInForce::PostOnly)),
            Timestamp(0),
        );
        let _ = sim.on_market_event(
            &MarketEvent::Heartbeat {
                ts: Timestamp(1_000_000),
            },
            Timestamp(1_000_000),
        );

        assert_eq!(sim.live_quotes.len(), 0, "order past buying power rejected");
        let rej = sim.drain_rejections();
        assert_eq!(rej.len(), 1);
        assert!(
            rej[0].1.contains("-2019"),
            "rejected by the margin gate, got: {}",
            rej[0].1
        );
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

    /// A level VANISHING from the L2 snapshot (new_agg == 0) is ambiguous —
    /// usually price moved past it, not a full cancel-out — so queue_ahead is
    /// PRESERVED, not zeroed. Zeroing here would falsely promote a resting
    /// order to front-of-queue and over-fill dense grids.
    #[test]
    fn vanished_level_preserves_queue_ahead() {
        let sym = make_symbol();
        let mut sim = FillSim::new(default_cfg());

        // Best bid 100 with 10 resting. Place JOIN bid → queue_ahead = 10.
        let book1 = MarketEvent::BookUpdate {
            snapshot: make_book_with_size(&sym, 100, 10, 102, 1),
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

        // New book brackets 100 (bids 101 / 99) but has NO order AT 100 →
        // level_size(Bid, 100) == 0, yet 100 is still in-window (>= deepest 99).
        let book2 = MarketEvent::BookUpdate {
            snapshot: Snapshot {
                symbol: sym.clone(),
                bids: vec![
                    Level {
                        price: Price(Decimal::from(101)),
                        size: Size(Decimal::from(1)),
                    },
                    Level {
                        price: Price(Decimal::from(99)),
                        size: Size(Decimal::from(1)),
                    },
                ],
                asks: vec![Level {
                    price: Price(Decimal::from(102)),
                    size: Size(Decimal::from(1)),
                }],
                ts: Timestamp(25_000_000),
            },
        };
        let _ = sim.on_market_event(&book2, Timestamp(25_000_000));
        assert_eq!(
            sim.live_quotes[0].queue_ahead,
            Decimal::from(10),
            "vanished level must preserve queue_ahead, not zero it"
        );
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
            leverage: rust_decimal::Decimal::ZERO,
            max_position_frac: rust_decimal::Decimal::ZERO,
            silent_cancel_rate_per_min: 0.0,
            rng_seed: 0,
            latency_jitter_ms: 0,
            max_open_orders: None,
            queue_cancel_decay_per_sec: 0.0,
            spot: false,
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
            leverage: rust_decimal::Decimal::ZERO,
            max_position_frac: rust_decimal::Decimal::ZERO,
            silent_cancel_rate_per_min: 0.0,
            rng_seed: 0,
            latency_jitter_ms: 0,
            max_open_orders: None,
            queue_cancel_decay_per_sec: 0.0,
            spot: false,
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

    #[test]
    fn latency_jitter_adds_spread_but_respects_base() {
        let sym = make_symbol();
        let base_ms = 10u64;
        let cfg = FillSimConfig {
            submit_latency_ms: base_ms,
            cancel_latency_ms: 0,
            fees: VenueFees {
                maker_bps: 0,
                taker_bps: 0,
            },
            max_position_notional_usdt: None,
            leverage: rust_decimal::Decimal::ZERO,
            max_position_frac: rust_decimal::Decimal::ZERO,
            silent_cancel_rate_per_min: 0.0,
            rng_seed: 42,
            latency_jitter_ms: 50,
            max_open_orders: None,
            queue_cancel_decay_per_sec: 0.0,
            spot: false,
        };
        let base_ns = base_ms * 1_000_000;
        let mut sim = FillSim::new(cfg.clone());
        for _ in 0..40 {
            sim.on_action(
                Action::Quote(make_intent(&sym, Side::Bid, 100, 1, TimeInForce::PostOnly)),
                Timestamp(0),
            );
        }
        let sched: Vec<u64> = sim.pending.iter().map(|p| p.scheduled_ts_ns).collect();
        // Jitter only ADDS delay — never schedules before the fixed base.
        assert!(
            sched.iter().all(|&s| s >= base_ns),
            "jitter went below base"
        );
        // The exponential draw produces a spread, not a constant.
        assert!(
            sched.iter().any(|&s| s > base_ns),
            "expected some jittered delay above base"
        );

        // Same seed → identical schedule (reproducible).
        let mut sim2 = FillSim::new(cfg);
        for _ in 0..40 {
            sim2.on_action(
                Action::Quote(make_intent(&sym, Side::Bid, 100, 1, TimeInForce::PostOnly)),
                Timestamp(0),
            );
        }
        let sched2: Vec<u64> = sim2.pending.iter().map(|p| p.scheduled_ts_ns).collect();
        assert_eq!(sched, sched2, "same seed must reproduce the same latencies");
    }

    #[test]
    fn zero_jitter_is_exact_base_latency() {
        let sym = make_symbol();
        let mut sim = FillSim::new(default_cfg()); // latency_jitter_ms = 0
        sim.on_action(
            Action::Quote(make_intent(&sym, Side::Bid, 100, 1, TimeInForce::PostOnly)),
            Timestamp(0),
        );
        // submit_latency_ms = 10 in default_cfg, no jitter → exactly 10ms.
        assert_eq!(sim.pending[0].scheduled_ts_ns, 10 * 1_000_000);
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
