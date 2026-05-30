//! Liquidation-cascade fade — stat-arb on forced-order overshoot.
//!
//! Forced liquidations on USD-M Futures land on the `@forceOrder` stream
//! as already-crossed market orders. A large cluster on one side
//! depletes the book briefly, MMs back away, and price overshoots its
//! pre-cascade fair value by 10-50 bps. The classic mean-revert wick
//! that follows is the alpha: post passive deep inside the dislocated
//! touch, hold until price recovers most of the overshoot, exit.
//!
//! # State machine
//!
//! ```text
//! Idle ──liq_sum > trigger (per side)──> Armed
//! Armed ──price overshoots ≥ capit_bps──> Capitulation (post fade quote)
//! Armed ──window expired (no overshoot)──> Idle
//! Capitulation ──fill──> Holding (post TP at pre-liq mid +/- target)
//! Capitulation ──entry_timeout──> Idle (cancel)
//! Holding ──TP fill OR time_stop──> Idle (flatten via IOC if needed)
//! ```
//!
//! Re-arm during Holding is intentionally suppressed — one trade per
//! cascade keeps the inventory bounded and avoids stacking into a
//! continued one-sided event.
//!
//! # Input
//!
//! `StrategyContext::recent_liqs` is a runner-maintained rolling window
//! of `LiqEvent`s. The runner prunes by `liq_window_secs`. The strategy
//! only sums what's visible to it on each tick — no internal buffering
//! beyond the state machine.
//!
//! # Not implemented in v0
//!
//! - Multi-symbol cross-liq (BTC liq → ETH fade). Single-symbol only.
//! - Compounding into the same direction (re-arm during Holding).
//! - Latency-aware quote price adjustment for placement delay.

use std::collections::VecDeque;

use tikr_core::{
    Decimal, LiqEvent, MarketEvent, Price, QuoteKind, Side, Size, Symbol, TimeInForce,
};
use tikr_venue::QuoteIntent;

use crate::risk::{self, RiskConfig, RiskDecision};
use crate::{Action, Strategy, StrategyContext};

/// Configuration for [`LiqFade`].
#[derive(Debug, Clone)]
pub struct LiqFadeConfig {
    /// Fiat notional per fade entry.
    pub notional_per_entry: Decimal,
    /// Venue tick size — used to round quote prices to grid.
    pub tick_size: Decimal,
    /// Venue lot step size — used to round quantity.
    pub step_size: Decimal,
    /// Minimum order notional required by the venue (price × size).
    pub min_notional: Decimal,
    /// Hard inventory cap in USDT notional. `0` disables.
    pub max_position_usdt: Decimal,
    /// Per-side liquidation notional threshold (USDT) summed over the
    /// runner's `recent_liqs` window. Bot arms when one side exceeds
    /// this AND the other side is < `arm_dominance` × this. Default
    /// `5_000_000` for BTC, smaller for alts.
    pub arm_threshold_usdt: Decimal,
    /// Required dominance of the heavy side over the light side. The
    /// arm fires only when `heavy_sum >= threshold AND light_sum <
    /// heavy_sum × arm_dominance`. `0.5` = heavy ≥ 2× light. Bounded
    /// in `(0, 1)`.
    pub arm_dominance: Decimal,
    /// How far past the pre-liq mid (in bps) the price must move before
    /// the strategy posts its fade quote. Higher = wait longer, fewer
    /// entries, better revert geometry. Default `15`.
    pub capitulation_overshoot_bps: u32,
    /// Quote offset (in bps) from the dislocated touch into the
    /// direction the price came from. Higher = deeper post, larger
    /// fade target. Default `5`.
    pub fade_offset_bps: u32,
    /// Take-profit target (in bps) measured from the entry price back
    /// toward the pre-liq mid. Should be < `capitulation_overshoot_bps`
    /// so the trade has positive expected revert capture. Default `10`.
    pub revert_target_bps: u32,
    /// Maximum time (seconds) the fade quote may rest before being
    /// cancelled when unfilled. Default `30`.
    pub entry_timeout_secs: u32,
    /// Hard time-stop on a held position (seconds). If no TP fill in
    /// this window the bot IOC-flattens regardless of PnL. Default `120`.
    pub position_timeout_secs: u32,
    /// Stop-loss in bps of position notional. `0` disables — the
    /// time-stop alone bounds adverse holds. Set to a multiple of
    /// `capitulation_overshoot_bps` (e.g. `2×`) for a safety net.
    pub stop_loss_bps: u32,
}

/// Internal state-machine phases. Public for `LiqFade::state()` tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiqFadeState {
    /// Watching for an armed-trigger liq cluster.
    Idle,
    /// Liq trigger fired; waiting for price overshoot to confirm.
    Armed,
    /// Fade quote posted; waiting on fill OR entry timeout.
    Capitulation,
    /// Got a fill; TP order posted, waiting on close OR position timeout.
    Holding,
}

/// Snapshot of an armed-cascade trigger — pre-liq baseline + heavy side.
#[derive(Debug, Clone, Copy)]
struct ArmedSnapshot {
    /// Mid price at the moment the arm fired — the revert target.
    pre_liq_mid: Price,
    /// Side the liquidated traders crossed — the direction price moved.
    heavy_side: Side,
    /// Timestamp of the arm event (ns). Used for armed-window expiry.
    armed_ts: u64,
}

/// In-flight trade state — populated when [`LiqFadeState::Capitulation`]
/// or [`LiqFadeState::Holding`] is active.
#[derive(Debug, Clone, Copy)]
struct TradeState {
    /// Pre-liq mid that drove entry — TP target reference.
    pre_liq_mid: Price,
    /// Side of OUR entry quote (opposite of heavy_side — we fade).
    entry_side: Side,
    /// Timestamp when entry quote was placed OR when fill landed.
    /// Repurposed across Capitulation → Holding by `apply_fill`.
    phase_started_ts: u64,
}

/// Liquidation-cascade fade strategy.
pub struct LiqFade {
    config: LiqFadeConfig,
    state: LiqFadeState,
    armed: Option<ArmedSnapshot>,
    trade: Option<TradeState>,
    /// Last seen best bid + ask + mid. Required for arming (snapshot
    /// pre-liq mid), capitulation check, and exit price construction.
    last_bid: Option<Price>,
    last_ask: Option<Price>,
    last_mid: Option<Price>,
    last_event_ts: u64,
    /// Ts of the most recent LiqEvent we've consumed from
    /// `ctx.recent_liqs`. Used as the dedup cursor so the same event
    /// isn't re-tallied across consecutive ticks. Strictly monotonic.
    last_liq_ts_seen: u64,
    /// Running per-side liq notional sum, decayed by the runner's
    /// rolling-window prune. Lives here (not on context) so the
    /// strategy can re-evaluate on every tick without depending on the
    /// runner's prune cadence.
    side_sum: SideSumWindow,
}

/// Rolling per-side notional window — keeps `(ts, side, notional)`
/// entries and prunes older than `window_ns` on every observe call.
/// Independent of the runner's buffer so the strategy can keep its own
/// dedup state cleanly.
#[derive(Debug, Clone, Default)]
struct SideSumWindow {
    window_ns: u64,
    entries: VecDeque<(u64, Side, Decimal)>,
}

impl SideSumWindow {
    fn new(window_secs: u32) -> Self {
        Self {
            window_ns: (window_secs as u64).saturating_mul(1_000_000_000),
            entries: VecDeque::new(),
        }
    }

    fn observe(&mut self, ts_ns: u64, side: Side, notional: Decimal) {
        self.entries.push_back((ts_ns, side, notional));
        self.prune(ts_ns);
    }

    fn prune(&mut self, now_ns: u64) {
        let cutoff = now_ns.saturating_sub(self.window_ns);
        while let Some(&(ts, _, _)) = self.entries.front() {
            if ts < cutoff {
                self.entries.pop_front();
            } else {
                break;
            }
        }
    }

    fn sum_for(&self, side: Side) -> Decimal {
        let mut total = Decimal::ZERO;
        for &(_, s, n) in self.entries.iter() {
            if s == side {
                total += n;
            }
        }
        total
    }
}

impl LiqFade {
    fn risk_cfg(&self) -> RiskConfig {
        RiskConfig {
            take_profit_bps: 0,
            stop_loss_bps: self.config.stop_loss_bps,
            take_profit_usdt_legacy: Decimal::ZERO,
        }
    }

    /// Compute fade-quote price given the pre-liq mid + heavy side.
    /// Fade-side is OPPOSITE of heavy: a long-liq cascade (heavy Ask)
    /// dumps price → fade with a Bid below the bottom. `fade_offset_bps`
    /// pushes deeper into the dislocation (lower bid / higher ask).
    fn fade_price(
        &self,
        _pre_liq_mid: Price,
        heavy_side: Side,
        best_bid: Price,
        best_ask: Price,
    ) -> (Side, Price) {
        let bp = Decimal::from(self.config.fade_offset_bps) / Decimal::from(10_000);
        match heavy_side {
            // Heavy Ask = sellers dumped, price overshot down → fade with Bid
            // posted `fade_offset_bps` BELOW current best_bid.
            Side::Ask => (Side::Bid, Price(best_bid.0 * (Decimal::ONE - bp))),
            // Heavy Bid = buyers ripped, price overshot up → fade with Ask
            // posted `fade_offset_bps` ABOVE current best_ask.
            Side::Bid => (Side::Ask, Price(best_ask.0 * (Decimal::ONE + bp))),
        }
        // pre_liq_mid retained on TradeState for the TP target. Kept out
        // of this fn's output to keep the signature focused.
    }

    fn quote_size_at(&self, price: Price) -> Size {
        let raw = self.config.notional_per_entry / price.0;
        let step = self.config.step_size;
        let mut qty = if step > Decimal::ZERO {
            (raw / step).floor() * step
        } else {
            raw
        };
        if self.config.min_notional > Decimal::ZERO && step > Decimal::ZERO {
            let min_qty = (self.config.min_notional / price.0 / step).ceil() * step;
            if qty < min_qty {
                qty = min_qty;
            }
        }
        Size(qty)
    }

    fn make_quote(&self, symbol: &Symbol, side: Side, price: Price, tif: TimeInForce) -> Action {
        Action::Quote(QuoteIntent {
            symbol: symbol.clone(),
            side,
            price,
            size: self.quote_size_at(price),
            tif,
            kind: QuoteKind::Point,
        })
    }

    /// Compute the TP exit price for a held position.
    /// `entry_side = Bid` (we bought the dip) → TP is an Ask above mid by
    /// `(overshoot - target)` from pre_liq_mid up. Actually simpler:
    /// just close at `pre_liq_mid + revert_target_bps` (Bid entry) or
    /// `pre_liq_mid - revert_target_bps` (Ask entry) — we capture
    /// `revert_target_bps` of the `overshoot`.
    fn tp_price(&self, trade: &TradeState) -> Price {
        let bp = Decimal::from(self.config.revert_target_bps) / Decimal::from(10_000);
        match trade.entry_side {
            // We bought the dip; exit ABOVE pre_liq_mid by `revert_target_bps`.
            // pre_liq_mid is the price BEFORE the cascade — bouncing past it
            // is the bonus, not the base case. Settling on
            // `pre_liq_mid * (1 - bp)` (i.e. STILL below pre_liq_mid by the
            // target offset) means we accept partial revert as profit.
            Side::Bid => Price(trade.pre_liq_mid.0 * (Decimal::ONE - bp)),
            Side::Ask => Price(trade.pre_liq_mid.0 * (Decimal::ONE + bp)),
        }
    }

    /// Tally fresh liq events from the context window into our sum
    /// window. Dedup via `last_liq_ts_seen`. Assumes the context buffer
    /// is sorted oldest-first (runner contract).
    fn ingest_liqs(&mut self, recent: &[LiqEvent]) {
        for ev in recent {
            if ev.ts.0 <= self.last_liq_ts_seen {
                continue;
            }
            self.side_sum.observe(ev.ts.0, ev.side, ev.notional.0);
            self.last_liq_ts_seen = ev.ts.0;
        }
    }

    /// True iff the per-side window meets the arm trigger.
    fn check_arm(&self) -> Option<Side> {
        let bid_sum = self.side_sum.sum_for(Side::Bid);
        let ask_sum = self.side_sum.sum_for(Side::Ask);
        let trig = self.config.arm_threshold_usdt;
        let dom = self.config.arm_dominance;
        if bid_sum >= trig && ask_sum <= bid_sum * dom {
            return Some(Side::Bid);
        }
        if ask_sum >= trig && bid_sum <= ask_sum * dom {
            return Some(Side::Ask);
        }
        None
    }

    /// True iff current mid has overshot `pre_liq_mid` by
    /// `capitulation_overshoot_bps` in the heavy direction.
    fn check_capitulation(&self, pre_liq_mid: Price, heavy_side: Side, current_mid: Price) -> bool {
        let bp = Decimal::from(self.config.capitulation_overshoot_bps) / Decimal::from(10_000);
        match heavy_side {
            // Heavy Ask (sellers) → price moved DOWN; need
            // current_mid <= pre_liq_mid × (1 - bp).
            Side::Ask => current_mid.0 <= pre_liq_mid.0 * (Decimal::ONE - bp),
            // Heavy Bid (buyers) → price moved UP; need
            // current_mid >= pre_liq_mid × (1 + bp).
            Side::Bid => current_mid.0 >= pre_liq_mid.0 * (Decimal::ONE + bp),
        }
    }

    /// Hold-position helper — checks SL + time-stop, returns IOC close
    /// when either fires. None means keep holding.
    fn check_exit(
        &self,
        ctx: &StrategyContext<'_>,
        mid: Price,
        best_bid: Price,
        best_ask: Price,
        now_ns: u64,
    ) -> Option<Vec<Action>> {
        let trade = self.trade?;
        // Time-stop unconditional flatten.
        let elapsed_ns = now_ns.saturating_sub(trade.phase_started_ts);
        let timeout_ns = (self.config.position_timeout_secs as u64).saturating_mul(1_000_000_000);
        if elapsed_ns >= timeout_ns {
            if ctx.position.size.0 == Decimal::ZERO {
                return None;
            }
            let abs_qty = if ctx.position.size.0 < Decimal::ZERO {
                -ctx.position.size.0
            } else {
                ctx.position.size.0
            };
            let close_side = if ctx.position.size.0 > Decimal::ZERO {
                Side::Ask
            } else {
                Side::Bid
            };
            return Some(vec![
                Action::CancelAll,
                risk::build_close(ctx.symbol, close_side, Size(abs_qty), best_bid, best_ask),
            ]);
        }
        // SL via shared risk module (cap-side only — TP handled by
        // resting opposite-side TP order, not by IOC).
        if self.config.stop_loss_bps == 0 {
            return None;
        }
        match risk::evaluate(ctx.position, mid, self.risk_cfg()) {
            RiskDecision::Hold => None,
            RiskDecision::Close { side, qty, .. } => Some(vec![
                Action::CancelAll,
                risk::build_close(ctx.symbol, side, qty, best_bid, best_ask),
            ]),
        }
    }
}

impl Strategy for LiqFade {
    type Config = LiqFadeConfig;

    fn new(config: Self::Config) -> Self {
        // Sum window length = max(entry timeout, position timeout) so
        // we keep enough history across both arming + holding. Capped
        // at 600s to bound memory.
        let window_secs = config
            .entry_timeout_secs
            .max(config.position_timeout_secs)
            .min(600);
        let side_sum = SideSumWindow::new(window_secs);
        Self {
            config,
            state: LiqFadeState::Idle,
            armed: None,
            trade: None,
            last_bid: None,
            last_ask: None,
            last_mid: None,
            last_event_ts: 0,
            last_liq_ts_seen: 0,
            side_sum,
        }
    }

    fn name(&self) -> &str {
        "liq-fade"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        // Pull ts off whichever event variant arrived; drives both
        // window prune + state-machine timers.
        let event_ts = match event {
            MarketEvent::BookUpdate { snapshot } => snapshot.ts.0,
            MarketEvent::Trade { ts, .. } => ts.0,
            MarketEvent::Fill(f) => f.ts.0,
            MarketEvent::Heartbeat { ts } => ts.0,
        };
        self.last_event_ts = event_ts;
        self.side_sum.prune(event_ts);
        self.ingest_liqs(ctx.recent_liqs);

        match event {
            MarketEvent::BookUpdate { snapshot } => {
                let bid = snapshot.bids.first().map(|l| l.price.0).map(Price);
                let ask = snapshot.asks.first().map(|l| l.price.0).map(Price);
                let (Some(b), Some(a)) = (bid, ask) else {
                    return Vec::new();
                };
                let mid = Price((b.0 + a.0) / Decimal::from(2));
                self.last_bid = Some(b);
                self.last_ask = Some(a);
                self.last_mid = Some(mid);
                self.tick_state_machine(ctx, mid, b, a, event_ts)
            }
            MarketEvent::Fill(f) => {
                // Cap fill — transition Capitulation → Holding, place
                // resting TP at the revert target. If we were already
                // Holding, the resting TP fired and we go back to Idle.
                match self.state {
                    LiqFadeState::Capitulation => {
                        let Some(trade) = self.trade else {
                            // Defensive: stray fill with no trade state.
                            return Vec::new();
                        };
                        // Only react to the entry-side fill (the one we placed).
                        if f.side != trade.entry_side {
                            return Vec::new();
                        }
                        self.state = LiqFadeState::Holding;
                        let new_trade = TradeState {
                            phase_started_ts: f.ts.0,
                            ..trade
                        };
                        self.trade = Some(new_trade);
                        let tp = self.tp_price(&new_trade);
                        let tp_side = match trade.entry_side {
                            Side::Bid => Side::Ask,
                            Side::Ask => Side::Bid,
                        };
                        vec![self.make_quote(ctx.symbol, tp_side, tp, TimeInForce::PostOnly)]
                    }
                    LiqFadeState::Holding => {
                        // TP filled (or partial — we accept partials as
                        // close-enough exit; the position tracker will
                        // reflect residual size if any).
                        if ctx.position.size.0 == Decimal::ZERO {
                            self.reset_to_idle();
                        }
                        Vec::new()
                    }
                    _ => Vec::new(),
                }
            }
            // Trade / Heartbeat advance time only — handled by ts ingest
            // above. The arm/capitulation poll runs on BookUpdate.
            _ => Vec::new(),
        }
    }

    fn on_quote_rejected(
        &mut self,
        _ctx: &StrategyContext<'_>,
        _intent: &QuoteIntent,
        _reason: &str,
    ) -> Vec<Action> {
        // Reject during entry post → bail back to Idle. Don't chase —
        // the cascade is over by the time the venue says no.
        if matches!(self.state, LiqFadeState::Capitulation) {
            self.reset_to_idle();
            return vec![Action::CancelAll];
        }
        Vec::new()
    }

    fn on_notional_updated(
        &mut self,
        _ctx: &StrategyContext<'_>,
        notional_per_order: Decimal,
    ) -> Vec<Action> {
        if notional_per_order > Decimal::ZERO {
            self.config.notional_per_entry = notional_per_order;
        }
        Vec::new()
    }

    fn on_max_position_updated(
        &mut self,
        _ctx: &StrategyContext<'_>,
        max_position_usdt: Decimal,
    ) -> Vec<Action> {
        if max_position_usdt > Decimal::ZERO {
            self.config.max_position_usdt = max_position_usdt;
        }
        Vec::new()
    }
}

impl LiqFade {
    /// Run the per-tick state machine. Called from BookUpdate handler;
    /// no-op when book is stale or unavailable.
    fn tick_state_machine(
        &mut self,
        ctx: &StrategyContext<'_>,
        mid: Price,
        best_bid: Price,
        best_ask: Price,
        now_ns: u64,
    ) -> Vec<Action> {
        match self.state {
            LiqFadeState::Idle => {
                if let Some(heavy) = self.check_arm() {
                    self.state = LiqFadeState::Armed;
                    self.armed = Some(ArmedSnapshot {
                        pre_liq_mid: mid,
                        heavy_side: heavy,
                        armed_ts: now_ns,
                    });
                }
                Vec::new()
            }
            LiqFadeState::Armed => {
                let Some(snap) = self.armed else {
                    self.reset_to_idle();
                    return Vec::new();
                };
                // Window expired without capitulation → drop arm.
                let elapsed_ns = now_ns.saturating_sub(snap.armed_ts);
                let arm_timeout_ns =
                    (self.config.entry_timeout_secs as u64).saturating_mul(1_000_000_000);
                if elapsed_ns >= arm_timeout_ns {
                    self.reset_to_idle();
                    return Vec::new();
                }
                if !self.check_capitulation(snap.pre_liq_mid, snap.heavy_side, mid) {
                    return Vec::new();
                }
                // Capitulation confirmed — post fade quote.
                let (entry_side, entry_price) =
                    self.fade_price(snap.pre_liq_mid, snap.heavy_side, best_bid, best_ask);
                // Inventory cap: refuse to add if it would deepen position
                // past `max_position_usdt`. Mirrors SG/LG helper inline.
                if self.config.max_position_usdt > Decimal::ZERO {
                    let pos_usdt = ctx.position.size.0 * mid.0;
                    let cap = self.config.max_position_usdt;
                    let would_deepen = match entry_side {
                        Side::Bid => pos_usdt >= cap,
                        Side::Ask => pos_usdt <= -cap,
                    };
                    if would_deepen {
                        self.reset_to_idle();
                        return Vec::new();
                    }
                }
                self.state = LiqFadeState::Capitulation;
                self.trade = Some(TradeState {
                    pre_liq_mid: snap.pre_liq_mid,
                    entry_side,
                    phase_started_ts: now_ns,
                });
                vec![self.make_quote(ctx.symbol, entry_side, entry_price, TimeInForce::PostOnly)]
            }
            LiqFadeState::Capitulation => {
                // Entry timeout — bail.
                let Some(trade) = self.trade else {
                    self.reset_to_idle();
                    return Vec::new();
                };
                let elapsed_ns = now_ns.saturating_sub(trade.phase_started_ts);
                let entry_timeout_ns =
                    (self.config.entry_timeout_secs as u64).saturating_mul(1_000_000_000);
                if elapsed_ns >= entry_timeout_ns {
                    self.reset_to_idle();
                    return vec![Action::CancelAll];
                }
                Vec::new()
            }
            LiqFadeState::Holding => {
                if let Some(close) = self.check_exit(ctx, mid, best_bid, best_ask, now_ns) {
                    self.reset_to_idle();
                    return close;
                }
                Vec::new()
            }
        }
    }

    fn reset_to_idle(&mut self) {
        self.state = LiqFadeState::Idle;
        self.armed = None;
        self.trade = None;
    }

    /// Test/diagnostic accessor.
    pub fn state(&self) -> LiqFadeState {
        self.state
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::{
        Asset, Fill, Level, MarketKind, Notional, Position, SignedSize, Snapshot, Timestamp,
        VenueId,
    };
    use tikr_venue::QuoteId;

    fn sym() -> Symbol {
        Symbol {
            base: Asset::new("BTC"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("test"),
            kind: MarketKind::Perp,
        }
    }

    fn cfg() -> LiqFadeConfig {
        LiqFadeConfig {
            notional_per_entry: Decimal::from(100),
            tick_size: Decimal::from_str_exact("0.1").unwrap(),
            step_size: Decimal::from_str_exact("0.001").unwrap(),
            min_notional: Decimal::ZERO,
            max_position_usdt: Decimal::ZERO,
            arm_threshold_usdt: Decimal::from(1_000_000),
            arm_dominance: Decimal::from_str_exact("0.5").unwrap(),
            capitulation_overshoot_bps: 15,
            fade_offset_bps: 5,
            revert_target_bps: 10,
            entry_timeout_secs: 30,
            position_timeout_secs: 120,
            stop_loss_bps: 0,
        }
    }

    fn snap(bid: i64, ask: i64, ts: u64) -> Snapshot {
        Snapshot {
            symbol: sym(),
            ts: Timestamp(ts),
            bids: vec![Level {
                price: Price(Decimal::from(bid)),
                size: Size(Decimal::ONE),
            }],
            asks: vec![Level {
                price: Price(Decimal::from(ask)),
                size: Size(Decimal::ONE),
            }],
        }
    }

    fn ctx<'a>(
        s: &'a Symbol,
        p: &'a Position,
        snap: &'a Snapshot,
        liqs: &'a [LiqEvent],
        ts: u64,
    ) -> StrategyContext<'a> {
        StrategyContext {
            symbol: s,
            now: Timestamp(ts),
            position: p,
            recent_fills: &[],
            latest_book: snap,
            open_quotes: &[],
            recent_liqs: liqs,
        }
    }

    fn flat() -> Position {
        Position {
            symbol: sym(),
            size: SignedSize(Decimal::ZERO),
            avg_entry: Price(Decimal::ZERO),
            realized_pnl: Notional(Decimal::ZERO),
        }
    }

    fn liq(ts: u64, side: Side, notional: i64) -> LiqEvent {
        LiqEvent {
            ts: Timestamp(ts),
            side,
            qty: Size(Decimal::ONE),
            price: Price(Decimal::from(100_000)),
            notional: Notional(Decimal::from(notional)),
        }
    }

    #[test]
    fn idle_arms_on_dominant_ask_liq_cluster() {
        let mut f = LiqFade::new(cfg());
        let s = sym();
        let p = flat();
        let liqs = vec![
            liq(1_000_000_000, Side::Ask, 600_000),
            liq(2_000_000_000, Side::Ask, 500_000),
        ];
        let snap = snap(100_000, 100_010, 3_000_000_000);
        let ctx = ctx(&s, &p, &snap, &liqs, 3_000_000_000);
        let actions = f.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        assert!(actions.is_empty());
        assert_eq!(f.state(), LiqFadeState::Armed);
    }

    #[test]
    fn idle_does_not_arm_below_threshold() {
        let mut f = LiqFade::new(cfg());
        let s = sym();
        let p = flat();
        let liqs = vec![liq(1_000_000_000, Side::Ask, 100_000)];
        let snap = snap(100_000, 100_010, 2_000_000_000);
        let ctx = ctx(&s, &p, &snap, &liqs, 2_000_000_000);
        f.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        assert_eq!(f.state(), LiqFadeState::Idle);
    }

    #[test]
    fn idle_does_not_arm_on_balanced_liqs() {
        // Bid + Ask both > threshold → no dominant side → no arm.
        let mut f = LiqFade::new(cfg());
        let s = sym();
        let p = flat();
        let liqs = vec![
            liq(1_000_000_000, Side::Ask, 1_200_000),
            liq(1_500_000_000, Side::Bid, 1_200_000),
        ];
        let snap = snap(100_000, 100_010, 2_000_000_000);
        let ctx = ctx(&s, &p, &snap, &liqs, 2_000_000_000);
        f.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        assert_eq!(f.state(), LiqFadeState::Idle);
    }

    #[test]
    fn armed_to_capitulation_posts_bid_below_book() {
        let mut f = LiqFade::new(cfg());
        let s = sym();
        let p = flat();
        // First tick: arm on Ask cluster, mid = 100_005.
        let liqs = vec![liq(1_000_000_000, Side::Ask, 1_500_000)];
        let s1 = snap(100_000, 100_010, 2_000_000_000);
        let c1 = ctx(&s, &p, &s1, &liqs, 2_000_000_000);
        f.on_event(
            &c1,
            &MarketEvent::BookUpdate {
                snapshot: s1.clone(),
            },
        );
        assert_eq!(f.state(), LiqFadeState::Armed);

        // Second tick: price dumps 20bps (well past 15bps capitulation).
        // mid = 99_800, well below 100_005 × (1 - 0.0015) = 99_855.
        let s2 = snap(99_795, 99_805, 3_000_000_000);
        let liqs_empty: Vec<LiqEvent> = vec![];
        let c2 = ctx(&s, &p, &s2, &liqs_empty, 3_000_000_000);
        let actions = f.on_event(
            &c2,
            &MarketEvent::BookUpdate {
                snapshot: s2.clone(),
            },
        );
        assert_eq!(f.state(), LiqFadeState::Capitulation);
        assert_eq!(actions.len(), 1);
        let Action::Quote(intent) = &actions[0] else {
            panic!("expected Quote");
        };
        assert_eq!(intent.side, Side::Bid);
        // Posted `fade_offset_bps = 5` BELOW best_bid 99_795.
        // 99_795 * (1 - 0.0005) = 99_745.1025
        assert!(intent.price.0 < Price(Decimal::from(99_795)).0);
    }

    #[test]
    fn armed_window_expires_without_capitulation() {
        let mut f = LiqFade::new(cfg());
        let s = sym();
        let p = flat();
        let liqs = vec![liq(1_000_000_000, Side::Ask, 1_500_000)];
        let s1 = snap(100_000, 100_010, 2_000_000_000);
        let c1 = ctx(&s, &p, &s1, &liqs, 2_000_000_000);
        f.on_event(
            &c1,
            &MarketEvent::BookUpdate {
                snapshot: s1.clone(),
            },
        );
        assert_eq!(f.state(), LiqFadeState::Armed);

        // 60 seconds later (>30s entry_timeout) — price hasn't moved →
        // arm expires.
        let s2 = snap(100_000, 100_010, 62_000_000_000);
        let liqs_empty: Vec<LiqEvent> = vec![];
        let c2 = ctx(&s, &p, &s2, &liqs_empty, 62_000_000_000);
        f.on_event(
            &c2,
            &MarketEvent::BookUpdate {
                snapshot: s2.clone(),
            },
        );
        assert_eq!(f.state(), LiqFadeState::Idle);
    }

    #[test]
    fn capitulation_to_holding_on_entry_fill_posts_tp() {
        let mut f = LiqFade::new(cfg());
        let s = sym();
        let p = flat();
        // Arm.
        let liqs = vec![liq(1_000_000_000, Side::Ask, 1_500_000)];
        let s1 = snap(100_000, 100_010, 2_000_000_000);
        let c1 = ctx(&s, &p, &s1, &liqs, 2_000_000_000);
        f.on_event(
            &c1,
            &MarketEvent::BookUpdate {
                snapshot: s1.clone(),
            },
        );
        // Capitulate.
        let s2 = snap(99_795, 99_805, 3_000_000_000);
        let liqs_empty: Vec<LiqEvent> = vec![];
        let c2 = ctx(&s, &p, &s2, &liqs_empty, 3_000_000_000);
        f.on_event(
            &c2,
            &MarketEvent::BookUpdate {
                snapshot: s2.clone(),
            },
        );
        assert_eq!(f.state(), LiqFadeState::Capitulation);

        // Entry fill on Bid.
        let fill = Fill {
            quote_id: QuoteId::new(),
            price: Price(Decimal::from(99_750)),
            size: Size(Decimal::from_str_exact("0.001").unwrap()),
            fee_asset: Asset::new("USDT"),
            fee_amount: Decimal::ZERO,
            fee_quote: Notional(Decimal::ZERO),
            side: Side::Bid,
            ts: Timestamp(4_000_000_000),
            is_full: true,
            trade_id: None,
        };
        let c3 = ctx(&s, &p, &s2, &liqs_empty, 4_000_000_000);
        let actions = f.on_event(&c3, &MarketEvent::Fill(fill));
        assert_eq!(f.state(), LiqFadeState::Holding);
        // TP order on Ask side.
        assert_eq!(actions.len(), 1);
        let Action::Quote(intent) = &actions[0] else {
            panic!("expected TP Quote");
        };
        assert_eq!(intent.side, Side::Ask);
        // TP price = pre_liq_mid (100_005) × (1 - 10bps/10000) = 99_905.005
        // Below pre_liq_mid by revert_target_bps.
        assert!(intent.price.0 < Price(Decimal::from(100_005)).0);
        assert!(intent.price.0 > Price(Decimal::from(99_795)).0);
    }

    #[test]
    fn capitulation_entry_timeout_cancels_and_resets() {
        let mut f = LiqFade::new(cfg());
        let s = sym();
        let p = flat();
        let liqs = vec![liq(1_000_000_000, Side::Ask, 1_500_000)];
        let s1 = snap(100_000, 100_010, 2_000_000_000);
        let c1 = ctx(&s, &p, &s1, &liqs, 2_000_000_000);
        f.on_event(
            &c1,
            &MarketEvent::BookUpdate {
                snapshot: s1.clone(),
            },
        );
        let s2 = snap(99_795, 99_805, 3_000_000_000);
        let liqs_empty: Vec<LiqEvent> = vec![];
        let c2 = ctx(&s, &p, &s2, &liqs_empty, 3_000_000_000);
        f.on_event(
            &c2,
            &MarketEvent::BookUpdate {
                snapshot: s2.clone(),
            },
        );
        assert_eq!(f.state(), LiqFadeState::Capitulation);

        // 60s later (>30s entry_timeout, no fill).
        let s3 = snap(99_795, 99_805, 63_000_000_000);
        let c3 = ctx(&s, &p, &s3, &liqs_empty, 63_000_000_000);
        let actions = f.on_event(
            &c3,
            &MarketEvent::BookUpdate {
                snapshot: s3.clone(),
            },
        );
        assert_eq!(f.state(), LiqFadeState::Idle);
        assert!(matches!(actions.as_slice(), [Action::CancelAll]));
    }

    #[test]
    fn holding_position_time_stop_ioc_flattens() {
        let mut f = LiqFade::new(cfg());
        let s = sym();
        // Arm + capitulate + fill manually by driving the state machine.
        let liqs = vec![liq(1_000_000_000, Side::Ask, 1_500_000)];
        let p0 = flat();
        let s1 = snap(100_000, 100_010, 2_000_000_000);
        let c1 = ctx(&s, &p0, &s1, &liqs, 2_000_000_000);
        f.on_event(
            &c1,
            &MarketEvent::BookUpdate {
                snapshot: s1.clone(),
            },
        );
        let s2 = snap(99_795, 99_805, 3_000_000_000);
        let liqs_empty: Vec<LiqEvent> = vec![];
        let c2 = ctx(&s, &p0, &s2, &liqs_empty, 3_000_000_000);
        f.on_event(
            &c2,
            &MarketEvent::BookUpdate {
                snapshot: s2.clone(),
            },
        );
        let fill = Fill {
            quote_id: QuoteId::new(),
            price: Price(Decimal::from(99_750)),
            size: Size(Decimal::from_str_exact("0.001").unwrap()),
            fee_asset: Asset::new("USDT"),
            fee_amount: Decimal::ZERO,
            fee_quote: Notional(Decimal::ZERO),
            side: Side::Bid,
            ts: Timestamp(4_000_000_000),
            is_full: true,
            trade_id: None,
        };
        let c3 = ctx(&s, &p0, &s2, &liqs_empty, 4_000_000_000);
        f.on_event(&c3, &MarketEvent::Fill(fill));
        assert_eq!(f.state(), LiqFadeState::Holding);

        // 200s later (>120s position_timeout), still long 0.001 → IOC close.
        let p_long = Position {
            symbol: sym(),
            size: SignedSize(Decimal::from_str_exact("0.001").unwrap()),
            avg_entry: Price(Decimal::from(99_750)),
            realized_pnl: Notional(Decimal::ZERO),
        };
        let s4 = snap(99_700, 99_710, 204_000_000_000);
        let c4 = ctx(&s, &p_long, &s4, &liqs_empty, 204_000_000_000);
        let actions = f.on_event(
            &c4,
            &MarketEvent::BookUpdate {
                snapshot: s4.clone(),
            },
        );
        assert_eq!(f.state(), LiqFadeState::Idle);
        assert_eq!(actions.len(), 2);
        assert!(matches!(actions[0], Action::CancelAll));
        let Action::Quote(intent) = &actions[1] else {
            panic!("expected close Quote");
        };
        assert_eq!(intent.side, Side::Ask);
        assert_eq!(intent.tif, TimeInForce::IOC);
    }
}
