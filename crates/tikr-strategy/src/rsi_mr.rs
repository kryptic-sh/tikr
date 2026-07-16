//! Long-only RSI mean-reversion with Kaufman Efficiency Ratio regime gate.
//!
//! ## Strategy
//! - Aggregate 1m OHLCV bars from book + trade events.
//! - Compute RSI(N), KER(M), ATR(K), volume-zscore(V) on closed bars.
//! - **Entry (long only):** when flat, all of the following on the latest closed bar:
//!   * `RSI < rsi_buy_threshold` (oversold)
//!   * `KER < ker_max_trending` (chop, not trend) — kills the falling-knife mode
//!   * `volume_zscore > vol_zscore_min` (real spike, not algo noise)
//!
//!   → emit one post-only BID at current `best_bid`.
//! - **TP (maker):** as soon as the entry fills, emit a post-only ASK at
//!   `entry_price + atr_tp_mult × ATR`.
//! - **SL (taker IOC):** every event, if `mid < entry - atr_sl_mult × ATR`,
//!   cancel TP and emit an IOC SELL at `best_bid - tick` (cross to take).
//! - **RSI-exit (maker):** when `RSI > rsi_exit_threshold` AND TP hasn't filled,
//!   cancel TP and emit a fresh ASK at `best_ask` (give up on TP, take what
//!   the book offers).
//! - **Timeout (taker IOC):** after `max_hold_bars` bars without exit,
//!   IOC out.
//!
//! ## Fee model
//! 0-bps maker (USDC promo) + 5-bps taker. Entry+TP = pure maker.
//! Entry+SL = maker+taker = 5bps round-trip cost.
//!
//! ## Why long-only
//! Symmetric short side is left out by design — the KER gate exists to
//! protect long-only from trending downs; the mirror "skip-when-trending-up"
//! costs you the easiest wins (bull-market dips that bounce). Add the
//! short side later if regime classifier proves robust.

use std::collections::VecDeque;

use tikr_core::{
    Decimal, MarketEvent, Price, QuoteKind, Side, Size, Symbol, TimeInForce, Timestamp,
};
use tikr_venue::QuoteIntent;

use crate::{Action, Strategy, StrategyContext};

/// Configuration for [`RsiMr`].
#[derive(Debug, Clone)]
pub struct RsiMrConfig {
    /// Notional in quote currency per entry.
    pub notional_per_order: Decimal,
    /// Venue tick size.
    pub tick_size: Decimal,
    /// Venue lot step.
    pub step_size: Decimal,
    /// Venue min order notional in quote.
    pub min_notional: Decimal,

    /// Bar interval in seconds (e.g. 60 = 1-minute).
    pub bar_interval_secs: u64,
    /// Max closed bars to retain (must be ≥ max(rsi_period, ker_period,
    /// atr_period, vol_zscore_period) + 1).
    pub max_bars: usize,

    /// RSI period (e.g. 14).
    pub rsi_period: u32,
    /// Enter long when latest closed-bar RSI < this (e.g. 25).
    pub rsi_buy_threshold: u32,
    /// Exit long when latest closed-bar RSI > this (e.g. 50). Falls back
    /// to a fresh maker ASK at touch if TP hasn't filled.
    pub rsi_exit_threshold: u32,

    /// Kaufman Efficiency Ratio period (e.g. 20).
    pub ker_period: u32,
    /// Skip entry when KER > this — market is trending, MR will bleed.
    /// `0.4` is a common chop/trend boundary.
    pub ker_max_trending: Decimal,

    /// Volume-zscore lookback (e.g. 20 bars).
    pub vol_zscore_period: u32,
    /// Skip entry when volume z-score < this (e.g. 1.5σ).
    pub vol_zscore_min: Decimal,

    /// ATR period (e.g. 14).
    pub atr_period: u32,
    /// SL distance from entry in ATR multiples (e.g. 2.0).
    pub atr_sl_mult: Decimal,
    /// TP distance from entry in ATR multiples (e.g. 3.0).
    pub atr_tp_mult: Decimal,

    /// Max bars to hold before timeout-IOC exit.
    pub max_hold_bars: u32,
}

/// A closed OHLCV bar. `open` is unused by current indicators but
/// retained as part of the canonical OHLCV record for future strategies.
#[derive(Debug, Clone, Copy)]
struct Bar {
    #[allow(dead_code)]
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    volume: Decimal,
}

/// Per-position state. Created ONLY when the entry order actually fills —
/// creating it at order placement would arm the SL/timeout exits against a
/// phantom position (an unfilled entry bid), and an IOC "flatten" of a
/// position that doesn't exist opens a naked short.
#[derive(Debug, Clone)]
struct Position {
    entry_price: Decimal,
    entry_bar_idx: u64,
    atr_at_entry: Decimal,
    /// Whether the TP ASK has been posted for this position. Exits use
    /// `CancelAll` (the strategy rests at most one order at a time), so no
    /// per-order id tracking is needed — a locally minted QuoteId could
    /// never match the venue-assigned id anyway, making `Cancel(id)` a
    /// silent no-op that leaves the TP resting to fill into a naked short.
    tp_posted: bool,
}

/// Entry order in flight: placed but not (yet) filled. Indicator context is
/// captured at signal time so the eventual fill anchors to the right ATR/bar.
#[derive(Debug, Clone, Copy)]
struct PendingEntry {
    entry_bar_idx: u64,
    atr_at_entry: Decimal,
}

/// Long-only RSI mean-reversion with KER gate.
pub struct RsiMr {
    config: RsiMrConfig,
    closed: VecDeque<Bar>,
    /// In-progress bar; flushed to `closed` on bucket rollover.
    open_bar: Option<Bar>,
    /// Current bar bucket index (ts / interval_ns).
    current_bucket: Option<u64>,
    /// Monotonic bar counter for hold-bar timeouts.
    bars_seen: u64,
    position: Option<Position>,
    /// Resting entry bid not yet filled. Expires after one bar (the entry
    /// signal is per-bar) via CancelAll.
    pending_entry: Option<PendingEntry>,
}

impl RsiMr {
    fn quote_size(&self, price: Price) -> Size {
        if price.0 <= Decimal::ZERO {
            return Size(Decimal::ZERO);
        }
        let raw = self.config.notional_per_order / price.0;
        let stepped = if self.config.step_size > Decimal::ZERO {
            (raw / self.config.step_size).floor() * self.config.step_size
        } else {
            raw
        };
        let min = self.config.min_notional;
        if min > Decimal::ZERO && stepped * price.0 < min && self.config.step_size > Decimal::ZERO {
            let needed = (min / price.0 / self.config.step_size).ceil() * self.config.step_size;
            Size(needed)
        } else {
            Size(stepped)
        }
    }

    fn bucket_of(&self, ts: Timestamp) -> u64 {
        let interval_ns = self.config.bar_interval_secs.saturating_mul(1_000_000_000);
        if interval_ns == 0 {
            return 0;
        }
        ts.0 / interval_ns
    }

    /// Roll the open bar into `closed` if the bucket changed, then start
    /// a fresh bar at `price`. Returns true if a bar was just closed.
    fn maybe_roll_bar(&mut self, ts: Timestamp, price: Decimal) -> bool {
        let bucket = self.bucket_of(ts);
        match self.current_bucket {
            None => {
                // First-ever event — start a new bar, no roll yet.
                self.current_bucket = Some(bucket);
                self.open_bar = Some(Bar {
                    open: price,
                    high: price,
                    low: price,
                    close: price,
                    volume: Decimal::ZERO,
                });
                false
            }
            Some(b) if b == bucket => false,
            Some(_) => {
                // Bucket changed → close current bar, start new.
                if let Some(bar) = self.open_bar.take() {
                    self.closed.push_back(bar);
                    while self.closed.len() > self.config.max_bars {
                        self.closed.pop_front();
                    }
                    self.bars_seen = self.bars_seen.saturating_add(1);
                }
                self.current_bucket = Some(bucket);
                self.open_bar = Some(Bar {
                    open: price,
                    high: price,
                    low: price,
                    close: price,
                    volume: Decimal::ZERO,
                });
                true
            }
        }
    }

    /// Update the open bar's close/high/low with `price`.
    fn update_open_bar_price(&mut self, price: Decimal) {
        if let Some(bar) = self.open_bar.as_mut() {
            bar.close = price;
            if price > bar.high {
                bar.high = price;
            }
            if price < bar.low {
                bar.low = price;
            }
        }
    }

    fn add_trade_volume(&mut self, size: Decimal) {
        if let Some(bar) = self.open_bar.as_mut() {
            bar.volume += size;
        }
    }

    // ----- indicators -----

    /// Wilder RSI on closes. Returns None until period+1 closed bars.
    fn rsi(&self) -> Option<Decimal> {
        let n = self.config.rsi_period as usize;
        if self.closed.len() < n + 1 {
            return None;
        }
        let bars: Vec<&Bar> = self.closed.iter().rev().take(n + 1).collect();
        let mut gain = Decimal::ZERO;
        let mut loss = Decimal::ZERO;
        for w in bars.windows(2) {
            // w[0] is newer, w[1] is older in this reversed view.
            let diff = w[0].close - w[1].close;
            if diff > Decimal::ZERO {
                gain += diff;
            } else {
                loss -= diff;
            }
        }
        if loss == Decimal::ZERO {
            return Some(Decimal::from(100));
        }
        let rs = gain / loss;
        Some(Decimal::from(100) - (Decimal::from(100) / (Decimal::ONE + rs)))
    }

    /// Kaufman Efficiency Ratio = |sum_of_changes| / sum_of_|changes|.
    /// 1.0 = pure trend, 0.0 = pure chop.
    fn ker(&self) -> Option<Decimal> {
        let n = self.config.ker_period as usize;
        if self.closed.len() < n + 1 {
            return None;
        }
        let bars: Vec<&Bar> = self.closed.iter().rev().take(n + 1).collect();
        let net = (bars[0].close - bars[n].close).abs();
        let mut path = Decimal::ZERO;
        for w in bars.windows(2) {
            path += (w[0].close - w[1].close).abs();
        }
        if path == Decimal::ZERO {
            return Some(Decimal::ZERO);
        }
        Some(net / path)
    }

    /// Simple ATR (average true range), unWilderized for simplicity.
    fn atr(&self) -> Option<Decimal> {
        let n = self.config.atr_period as usize;
        // n == 0 would divide by zero below (rust_decimal panics).
        if n == 0 || self.closed.len() < n + 1 {
            return None;
        }
        let bars: Vec<&Bar> = self.closed.iter().rev().take(n + 1).collect();
        let mut tr_sum = Decimal::ZERO;
        for w in bars.windows(2) {
            let h_l = w[0].high - w[0].low;
            let h_pc = (w[0].high - w[1].close).abs();
            let l_pc = (w[0].low - w[1].close).abs();
            let tr = h_l.max(h_pc).max(l_pc);
            tr_sum += tr;
        }
        Some(tr_sum / Decimal::from(n as u64))
    }

    /// Volume z-score of the latest closed bar over the prior `period`.
    fn vol_zscore(&self) -> Option<Decimal> {
        let n = self.config.vol_zscore_period as usize;
        if self.closed.len() < n + 1 {
            return None;
        }
        let latest = self.closed.back()?.volume;
        let window: Vec<Decimal> = self
            .closed
            .iter()
            .rev()
            .skip(1)
            .take(n)
            .map(|b| b.volume)
            .collect();
        let count = Decimal::from(window.len() as u64);
        let mean = window.iter().copied().sum::<Decimal>() / count;
        let var = window
            .iter()
            .map(|v| (*v - mean) * (*v - mean))
            .sum::<Decimal>()
            / count;
        // Decimal has no sqrt; use a Newton iteration via f64 round-trip.
        // Acceptable precision loss for a gating threshold.
        let var_f = var.to_string().parse::<f64>().unwrap_or(0.0);
        if var_f <= 0.0 {
            return Some(Decimal::ZERO);
        }
        let std_f = var_f.sqrt();
        let std = Decimal::from_str_exact(&format!("{std_f:.10}")).ok()?;
        if std == Decimal::ZERO {
            return Some(Decimal::ZERO);
        }
        Some((latest - mean) / std)
    }

    // ----- helpers -----

    fn snap_to_tick(&self, price: Decimal, round_up: bool) -> Decimal {
        let tick = self.config.tick_size;
        if tick <= Decimal::ZERO {
            return price;
        }
        if round_up {
            (price / tick).ceil() * tick
        } else {
            (price / tick).floor() * tick
        }
    }

    fn make_quote(&self, symbol: &Symbol, side: Side, price: Price, tif: TimeInForce) -> Action {
        Action::Quote(QuoteIntent {
            symbol: symbol.clone(),
            side,
            price,
            size: self.quote_size(price),
            tif,
            kind: QuoteKind::Point,
        })
    }

    /// Exit ASK sized from the ACTUAL position (step-floored), not from the
    /// configured notional — a notional-sized exit over-closes after a
    /// partial entry fill (naked short) or under-closes after price moved.
    /// Falls back to notional sizing when the position report is unavailable.
    fn make_exit_quote(&self, ctx: &StrategyContext<'_>, price: Price, tif: TimeInForce) -> Action {
        let pos_abs = ctx.position.size.0.abs();
        let step = self.config.step_size;
        let qty = if step > Decimal::ZERO {
            (pos_abs / step).floor() * step
        } else {
            pos_abs
        };
        if qty <= Decimal::ZERO {
            return self.make_quote(ctx.symbol, Side::Ask, price, tif);
        }
        Action::Quote(QuoteIntent {
            symbol: ctx.symbol.clone(),
            side: Side::Ask,
            price,
            size: Size(qty),
            tif,
            kind: QuoteKind::Point,
        })
    }
}

impl Strategy for RsiMr {
    type Config = RsiMrConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            closed: VecDeque::new(),
            open_bar: None,
            current_bucket: None,
            bars_seen: 0,
            position: None,
            pending_entry: None,
        }
    }

    fn name(&self) -> &str {
        "rsi-mr"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        // 1) Maintain bar buffer.
        let mid = ctx
            .latest_book
            .bids
            .first()
            .zip(ctx.latest_book.asks.first())
            .map(|(b, a)| (b.price.0 + a.price.0) / Decimal::from(2));
        let event_ts = ctx.now;
        let event_price = match event {
            MarketEvent::BookUpdate { .. } => mid,
            MarketEvent::Trade { price, .. } => Some(price.0),
            _ => None,
        };
        if let Some(p) = event_price
            && p > Decimal::ZERO
        {
            self.maybe_roll_bar(event_ts, p);
            self.update_open_bar_price(p);
        }
        if let MarketEvent::Trade { size, .. } = event {
            self.add_trade_volume(size.0);
        }

        let mut actions: Vec<Action> = Vec::new();

        // 2) Pending-entry expiry: the entry signal is per-bar, so an entry
        // bid still unfilled after the bar rolls is stale — pull it. (If it
        // filled in the meantime, the Fill handler below converted it to a
        // position first.)
        if let Some(pending) = self.pending_entry
            && self.position.is_none()
            && self.bars_seen > pending.entry_bar_idx
        {
            self.pending_entry = None;
            actions.push(Action::CancelAll);
        }

        // 3) Position management (exits) — independent of bar-close. Exits
        // use CancelAll (at most one order rests at a time), never a locally
        // minted Cancel(id) that the venue could not match.
        if let Some(pos) = self.position.clone()
            && let Some(m) = mid
        {
            let sl_distance = pos.atr_at_entry * self.config.atr_sl_mult;
            let sl_trigger = pos.entry_price - sl_distance;
            // (a) SL — IOC SELL at touch when mid < trigger.
            if m < sl_trigger
                && let Some(bp) = ctx.latest_book.bids.first()
            {
                actions.push(Action::CancelAll);
                let exit_price = Price(self.snap_to_tick(bp.price.0, false));
                actions.push(self.make_exit_quote(ctx, exit_price, TimeInForce::IOC));
                self.position = None;
                return actions;
            }
            // (b) Timeout — IOC out after max_hold_bars.
            let held_bars = self.bars_seen.saturating_sub(pos.entry_bar_idx);
            if held_bars >= self.config.max_hold_bars as u64
                && let Some(bp) = ctx.latest_book.bids.first()
            {
                actions.push(Action::CancelAll);
                let exit_price = Price(self.snap_to_tick(bp.price.0, false));
                actions.push(self.make_exit_quote(ctx, exit_price, TimeInForce::IOC));
                self.position = None;
                return actions;
            }
            // (c) RSI-exit — replace the TP with a fresh ASK at best_ask.
            if let Some(rsi) = self.rsi()
                && rsi > Decimal::from(self.config.rsi_exit_threshold)
                && let Some(ap) = ctx.latest_book.asks.first()
            {
                actions.push(Action::CancelAll);
                let exit_price = Price(self.snap_to_tick(ap.price.0, true));
                actions.push(self.make_exit_quote(ctx, exit_price, TimeInForce::PostOnly));
                if let Some(p) = self.position.as_mut() {
                    p.tp_posted = true;
                }
            }
        }

        // 4) Entry check (long-only, only on flat with no entry in flight).
        if self.position.is_none()
            && self.pending_entry.is_none()
            && let MarketEvent::BookUpdate { .. } = event
            && let Some(rsi) = self.rsi()
            && let Some(ker) = self.ker()
            && let Some(atr) = self.atr()
            && let Some(volz) = self.vol_zscore()
            && let Some(bp) = ctx.latest_book.bids.first()
        {
            let buy_thr = Decimal::from(self.config.rsi_buy_threshold);
            if rsi < buy_thr
                && ker < self.config.ker_max_trending
                && volz > self.config.vol_zscore_min
                && atr > Decimal::ZERO
            {
                let entry_price = Price(self.snap_to_tick(bp.price.0, false));
                actions.push(self.make_quote(
                    ctx.symbol,
                    Side::Bid,
                    entry_price,
                    TimeInForce::PostOnly,
                ));
                // Only the ORDER exists so far — the position is created by
                // the Fill handler. Capture the signal context for it.
                self.pending_entry = Some(PendingEntry {
                    entry_bar_idx: self.bars_seen,
                    atr_at_entry: atr,
                });
            }
        }

        // 5) Fill handling — entry BID filled: create the position, post TP.
        if let MarketEvent::Fill(fill) = event
            && fill.side == Side::Bid
            && self.position.is_none()
        {
            let pending = self.pending_entry.take();
            let atr_at_entry = pending
                .map(|p| p.atr_at_entry)
                .or_else(|| self.atr())
                .unwrap_or(Decimal::ZERO);
            let entry_bar_idx = pending.map(|p| p.entry_bar_idx).unwrap_or(self.bars_seen);
            self.position = Some(Position {
                entry_price: fill.price.0,
                entry_bar_idx,
                atr_at_entry,
                tp_posted: true,
            });
            let tp_offset = atr_at_entry * self.config.atr_tp_mult;
            let target = fill.price.0 + tp_offset;
            let tp_price = Price(self.snap_to_tick(target, true));
            // CancelAll first: a partially-filled entry remainder must not
            // keep resting alongside the TP.
            actions.push(Action::CancelAll);
            actions.push(self.make_exit_quote(ctx, tp_price, TimeInForce::PostOnly));
        }

        // 6) Ask-fill → position (fully) closed. A PARTIAL TP fill leaves
        // real inventory and the TP remainder resting — clearing state on it
        // would orphan that inventory with no SL/timeout coverage.
        if let MarketEvent::Fill(fill) = event
            && fill.side == Side::Ask
            && fill.is_full
        {
            self.position = None;
        }

        actions
    }

    fn on_quote_rejected(
        &mut self,
        _ctx: &StrategyContext<'_>,
        _intent: &QuoteIntent,
        _reason: &str,
    ) -> Vec<Action> {
        Vec::new()
    }

    fn on_notional_updated(
        &mut self,
        _ctx: &StrategyContext<'_>,
        notional_per_order: Decimal,
    ) -> Vec<Action> {
        if notional_per_order > Decimal::ZERO {
            self.config.notional_per_order = notional_per_order;
        }
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::{
        Asset, Level, MarketKind, Notional, Position as CorePos, SignedSize, Snapshot, VenueId,
    };
    use tikr_venue::QuoteId;

    fn sym() -> Symbol {
        Symbol {
            base: Asset::new("ETH"),
            quote: Asset::new("USDC"),
            venue: VenueId::new("test"),
            kind: MarketKind::Perp,
        }
    }

    fn cfg() -> RsiMrConfig {
        RsiMrConfig {
            notional_per_order: Decimal::from(50),
            tick_size: Decimal::new(1, 2), // 0.01
            step_size: Decimal::new(1, 3), // 0.001
            min_notional: Decimal::from(5),
            bar_interval_secs: 60,
            max_bars: 64,
            rsi_period: 14,
            rsi_buy_threshold: 25,
            rsi_exit_threshold: 50,
            ker_period: 20,
            ker_max_trending: Decimal::new(4, 1), // 0.4
            vol_zscore_period: 20,
            vol_zscore_min: Decimal::new(15, 1), // 1.5
            atr_period: 14,
            atr_sl_mult: Decimal::from(2),
            atr_tp_mult: Decimal::from(3),
            max_hold_bars: 60,
        }
    }

    fn pos_flat() -> CorePos {
        CorePos {
            symbol: sym(),
            size: SignedSize(Decimal::ZERO),
            avg_entry: Price(Decimal::ZERO),
            realized_pnl: Notional(Decimal::ZERO),
        }
    }

    fn snap(bid: Decimal, ask: Decimal) -> Snapshot {
        Snapshot {
            symbol: sym(),
            bids: vec![Level {
                price: Price(bid),
                size: Size(Decimal::from(10)),
            }],
            asks: vec![Level {
                price: Price(ask),
                size: Size(Decimal::from(10)),
            }],
            ts: Timestamp(1),
        }
    }

    fn ctx_for<'a>(
        symbol: &'a Symbol,
        s: &'a Snapshot,
        p: &'a CorePos,
        open: &'a [(QuoteId, QuoteIntent)],
        now: Timestamp,
    ) -> StrategyContext<'a> {
        StrategyContext {
            symbol,
            now,
            position: p,
            recent_fills: &[],
            latest_book: s,
            open_quotes: open,
            recent_liqs: &[],
        }
    }

    #[test]
    fn no_emit_when_buffer_warming_up() {
        let mut s = RsiMr::new(cfg());
        let snap = snap(Decimal::from(2000), Decimal::new(200001, 2));
        let p = pos_flat();
        let sym_ = sym();
        let ctx = ctx_for(&sym_, &snap, &p, &[], Timestamp(0));
        let actions = s.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        assert!(actions.is_empty());
    }

    #[test]
    fn bar_rolls_on_minute_boundary() {
        let mut s = RsiMr::new(cfg());
        let snap = snap(Decimal::from(2000), Decimal::new(200001, 2));
        let p = pos_flat();
        let sym_ = sym();
        // First event at t=0 → start bar.
        let ctx0 = ctx_for(&sym_, &snap, &p, &[], Timestamp(0));
        s.on_event(
            &ctx0,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        assert_eq!(s.closed.len(), 0);
        // Event 61s later → cross bucket → bar should close.
        let ctx1 = ctx_for(&sym_, &snap, &p, &[], Timestamp(61_000_000_000));
        s.on_event(
            &ctx1,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        assert_eq!(s.closed.len(), 1);
    }
}
