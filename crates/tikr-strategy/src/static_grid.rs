//! Static grid — place once, never move, only rebuild when batch is mostly consumed.
//!
//! Differs from [`LayeredGrid`] in that there is NO rolling, NO TP, NO mid-tracking
//! after cold start. Each order is a passive limit that sits until filled or
//! the batch is consumed enough to warrant a fresh placement around the new mid.
//!
//! # Layout
//!
//! For `levels_per_side = N`, `inner_bps`, `step_bps`, cold start places `2N`
//! orders symmetrically around the current mid:
//!
//! ```text
//! sell @ mid + (inner + 2·step) bps   (outer)
//! sell @ mid + (inner + 1·step) bps
//! sell @ mid +  inner bps             (inner)
//!                MID
//! buy  @ mid −  inner bps             (inner)
//! buy  @ mid − (inner + 1·step) bps
//! buy  @ mid − (inner + 2·step) bps   (outer)
//! ```
//!
//! Default `inner_bps = 3, step_bps = 3, N = 3` gives the user's example
//! layout: orders at ±3, ±6, ±9 bps from mid (gap-between-inner-pair = 6bps,
//! adjacent levels = 3bps apart).
//!
//! # Rebuild rule
//!
//! After cold start, on each fully-filled fill the strategy counts the
//! remaining open quotes. Rebuilds when EITHER:
//!
//! - Total open quotes `<= 2`
//! - One side is empty (i.e. all remaining orders on the same side)
//!
//! Rebuild = `CancelAll` + fresh `2N` orders around the latest book mid.
//! Anchored on the current book mid each time — the grid "follows" big
//! moves in a coarse step-wise way, not smoothly like [`LayeredGrid`].

use std::collections::VecDeque;

use tikr_core::{Decimal, MarketEvent, Price, QuoteKind, Side, Size, Symbol, TimeInForce};
use tikr_venue::{QuoteId, QuoteIntent};

use crate::{Action, Strategy, StrategyContext};

/// Configuration for [`StaticGrid`].
#[derive(Debug, Clone)]
pub struct StaticGridConfig {
    /// Fixed fiat notional per order.
    pub notional_per_order: Decimal,
    /// Number of orders per side (total open at start = `2 × levels_per_side`).
    pub levels_per_side: u32,
    /// Inner spread from mid in bps (closest level on each side).
    pub inner_bps: u32,
    /// Step between consecutive levels on the same side in bps.
    pub step_bps: u32,
    /// Position USDT magnitude at which skew saturates (clamped to ±1).
    /// Smaller value = grid responds to small inventory; larger = needs
    /// big position to skew. Pick close to your acceptable inventory cap.
    pub target_inventory_usdt: Decimal,
    /// Rebuild threshold for inventory-ratio drift. When the position
    /// ratio changes by more than this since the last rebuild, force a
    /// fresh batch placement at the newly-skewed prices.
    /// Default 0.3 (i.e. 30% saturation move). Set high to disable.
    pub rebuild_pos_ratio_delta: Decimal,
    /// Adaptive fill-rate scaler. Widens `inner_bps`/`step_bps` when
    /// realised fills/min exceed `target_fills_per_min`. `0` disables
    /// (no scaling applied). Default `5.0` fills/min target — tune to
    /// match the symbol's natural taker-flow rate.
    pub target_fills_per_min: Decimal,
    /// Rolling window (seconds) over which fills/min is measured.
    /// Default `60` — one minute of memory.
    pub fillrate_window_secs: u32,
    /// Lower bound on the adaptive scale multiplier. `1.0` = never
    /// tighten below configured bps (safer); `<1.0` allows tightening
    /// when flow is slow. Default `1.0`.
    pub scale_min: Decimal,
    /// Upper bound on the adaptive scale multiplier. Default `4.0` =
    /// up to 4× widening under heavy fill pressure.
    pub scale_max: Decimal,
}

/// Static grid state.
pub struct StaticGrid {
    config: StaticGridConfig,
    /// `true` once the initial batch has been placed.
    placed: bool,
    /// Last seen book mid — used to rebuild around the current mid when the
    /// rebuild trigger fires (since rebuild fires inside `on_event(Fill)` which
    /// doesn't carry a fresh book snapshot of its own).
    last_mid: Option<Price>,
    /// Position ratio captured at the last batch placement. Clamped to
    /// `[-1.0, 1.0]`. Used by `rebuild_needed` to fire a fresh batch when
    /// inventory has drifted by more than `rebuild_pos_ratio_delta`.
    last_pos_ratio: Decimal,
    /// Rolling window of fill timestamps (ns since epoch). Pruned to
    /// `fillrate_window_secs` on every fill; used to compute the
    /// adaptive bps scale.
    fill_ts: VecDeque<u64>,
    /// Timestamp of the first event seen (ns). Used for the adaptive
    /// scale ramp-up: during the first `fillrate_window_secs` the
    /// effective window is `now - session_start`, not the full window
    /// — so a fast-fill open isn't masked by dividing by a 60s window
    /// when only 5s have elapsed.
    session_start_ts: Option<u64>,
    /// Latest event timestamp seen. Used as "now" by `adaptive_scale`
    /// when computing effective window length (since the scaler is
    /// called from `build_batch` which doesn't take ts directly).
    last_event_ts: Option<u64>,
}

impl StaticGrid {
    fn make_quote(&self, symbol: &Symbol, side: Side, price: Price) -> Action {
        let qty = Size(self.config.notional_per_order / price.0);
        Action::Quote(QuoteIntent {
            symbol: symbol.clone(),
            side,
            price,
            size: qty,
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        })
    }

    /// Prune fill_ts to entries within the rolling window of `now_ns`.
    fn prune_fills(&mut self, now_ns: u64) {
        let window_ns = (self.config.fillrate_window_secs as u64).saturating_mul(1_000_000_000);
        let cutoff = now_ns.saturating_sub(window_ns);
        while let Some(&front) = self.fill_ts.front() {
            if front < cutoff {
                self.fill_ts.pop_front();
            } else {
                break;
            }
        }
    }

    /// Adaptive scale multiplier from rolling fill rate vs target.
    ///
    /// `scale = (actual_fpm / target_fpm).clamp(scale_min, scale_max)`.
    /// Returns `1.0` when adaptive is disabled (target = 0), when the
    /// window is empty, or when no events have been seen yet (neutral
    /// boot — let the strategy start at configured bps).
    ///
    /// During the first `fillrate_window_secs` of a session the
    /// effective denominator is `now - session_start` (capped at the
    /// window) — so a hot open with 5 fills in 3 seconds reports
    /// `5 / (3/60) = 100 fpm`, not `5 / 1.0 = 5 fpm`.
    fn adaptive_scale(&self) -> Decimal {
        if self.config.target_fills_per_min <= Decimal::ZERO {
            return Decimal::ONE;
        }
        let count = self.fill_ts.len();
        if count == 0 {
            return Decimal::ONE;
        }
        let (Some(start), Some(now)) = (self.session_start_ts, self.last_event_ts) else {
            return Decimal::ONE;
        };
        let window_ns = (self.config.fillrate_window_secs as u64).saturating_mul(1_000_000_000);
        if window_ns == 0 {
            return Decimal::ONE;
        }
        let elapsed_ns = now.saturating_sub(start);
        let effective_ns = elapsed_ns.min(window_ns).max(1);
        let effective_min = Decimal::from(effective_ns) / Decimal::from(60u64 * 1_000_000_000u64);
        let actual_fpm = Decimal::from(count as u64) / effective_min;
        let raw = actual_fpm / self.config.target_fills_per_min;
        raw.clamp(self.config.scale_min, self.config.scale_max)
    }

    /// Compute position ratio in `[-1, 1]`. Positive = long, negative = short.
    /// `target_inventory_usdt = 0` disables skew (returns 0).
    fn pos_ratio(&self, pos_usdt: Decimal) -> Decimal {
        if self.config.target_inventory_usdt <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        let raw = pos_usdt / self.config.target_inventory_usdt;
        raw.clamp(-Decimal::ONE, Decimal::ONE)
    }

    /// Emit `2N` orders around `mid` skewed by current inventory.
    ///
    /// Asymmetric: only the side that's filling faster (the one driving
    /// the position away from flat) gets pushed wider. The opposite
    /// side keeps its configured `inner_bps + k·step_bps` so it can
    /// close the imbalance at the normal edge. Floor at 1 bp on the
    /// widened side keeps post-only safety regardless of saturation.
    fn build_batch(&self, symbol: &Symbol, mid: Price, pos_ratio: Decimal) -> Vec<Action> {
        let mut actions = Vec::with_capacity(self.config.levels_per_side as usize * 2);
        for k in 0..self.config.levels_per_side {
            actions.push(self.make_level(symbol, mid, pos_ratio, Side::Bid, k));
            actions.push(self.make_level(symbol, mid, pos_ratio, Side::Ask, k));
        }
        actions
    }

    /// Build only the `side` half of the ladder. Used by the
    /// "refill only the empty side" path so we don't cancel the
    /// surviving closing orders.
    fn build_one_side(
        &self,
        symbol: &Symbol,
        mid: Price,
        pos_ratio: Decimal,
        side: Side,
    ) -> Vec<Action> {
        let mut actions = Vec::with_capacity(self.config.levels_per_side as usize);
        for k in 0..self.config.levels_per_side {
            actions.push(self.make_level(symbol, mid, pos_ratio, side, k));
        }
        actions
    }

    fn make_level(
        &self,
        symbol: &Symbol,
        mid: Price,
        pos_ratio: Decimal,
        side: Side,
        k: u32,
    ) -> Action {
        let bp_unit = Decimal::from(10_000);
        let adaptive = self.adaptive_scale();
        let base_bps = Decimal::from(self.config.inner_bps + self.config.step_bps * k) * adaptive;
        // Asymmetric inventory skew: only push the side that's
        // ACCUMULATING away from mid. The other side keeps its
        // configured distance so it can close the imbalance at the
        // normal edge.
        //
        // Position long (pos_ratio > 0)  ⇒ buys are filling faster
        //   buy_scale  = 1 + pos_ratio  (widen — slow further accumulation)
        //   sell_scale = 1              (keep — let sells close at normal edge)
        //
        // Position short (pos_ratio < 0) ⇒ sells are filling faster
        //   buy_scale  = 1              (keep)
        //   sell_scale = 1 + |pos_ratio| (widen)
        //
        // No `skew_strength` multiplier — the magnitude IS |pos_ratio|.
        // `target_inventory_usdt` still controls how fast pos_ratio
        // saturates (smaller target → reacts to small positions).
        let (buy_scale, sell_scale) = if pos_ratio > Decimal::ZERO {
            (Decimal::ONE + pos_ratio, Decimal::ONE)
        } else if pos_ratio < Decimal::ZERO {
            (Decimal::ONE, Decimal::ONE - pos_ratio)
        } else {
            (Decimal::ONE, Decimal::ONE)
        };
        match side {
            Side::Bid => {
                let bps = (base_bps * buy_scale).max(Decimal::ONE);
                let price = Price(mid.0 * (Decimal::ONE - bps / bp_unit));
                self.make_quote(symbol, Side::Bid, price)
            }
            Side::Ask => {
                let bps = (base_bps * sell_scale).max(Decimal::ONE);
                let price = Price(mid.0 * (Decimal::ONE + bps / bp_unit));
                self.make_quote(symbol, Side::Ask, price)
            }
        }
    }

    /// What the bot should do on a full fill — refill only the empty
    /// side, or rebuild the whole batch?
    fn rebuild_decision(
        &self,
        open_quotes: &[(QuoteId, QuoteIntent)],
        cur_pos_ratio: Decimal,
    ) -> RebuildDecision {
        let buys = open_quotes
            .iter()
            .filter(|(_, q)| q.side == Side::Bid)
            .count();
        let sells = open_quotes.len() - buys;

        // Inventory drift past the configured threshold: re-anchor the
        // whole grid at the new skew.
        let drift = (cur_pos_ratio - self.last_pos_ratio).abs();
        if drift > self.config.rebuild_pos_ratio_delta {
            return RebuildDecision::FullRebuild;
        }

        // Both sides empty (or wiped by external cancel): full rebuild.
        if buys == 0 && sells == 0 {
            return RebuildDecision::FullRebuild;
        }

        // One side empty: refill only that side. CRITICAL: do NOT
        // cancel the surviving side — those are the closing orders for
        // the inventory we accumulated. The pre-fix design did a
        // CancelAll on side-empty which cancelled the very orders that
        // would have flattened the position, causing runaway one-sided
        // fills (PROMPT 165 buys / 5 sells, DOGE realized = 0).
        if buys == 0 {
            return RebuildDecision::RefillSide(Side::Bid);
        }
        if sells == 0 {
            return RebuildDecision::RefillSide(Side::Ask);
        }

        RebuildDecision::None
    }
}

/// Outcome of `rebuild_decision`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RebuildDecision {
    None,
    /// Wipe and re-place both sides around current mid.
    FullRebuild,
    /// Place only this side's `levels_per_side` orders, leaving the
    /// opposite side untouched.
    RefillSide(Side),
}

impl Strategy for StaticGrid {
    type Config = StaticGridConfig;

    fn new(config: Self::Config) -> Self {
        assert!(
            config.scale_min <= config.scale_max,
            "StaticGridConfig: scale_min ({}) must be <= scale_max ({})",
            config.scale_min,
            config.scale_max
        );
        Self {
            config,
            placed: false,
            last_mid: None,
            last_pos_ratio: Decimal::ZERO,
            fill_ts: VecDeque::new(),
            session_start_ts: None,
            last_event_ts: None,
        }
    }

    fn name(&self) -> &str {
        "static-grid"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        // Session timestamps: anchor session_start on first event, refresh
        // last_event_ts on every event. Both feed adaptive_scale's
        // effective-window calc so a fast-fill open isn't masked by
        // dividing observed fills by the full configured window.
        let event_ts = match event {
            MarketEvent::BookUpdate { snapshot } => Some(snapshot.ts.0),
            MarketEvent::Trade { ts, .. } => Some(ts.0),
            MarketEvent::Fill(f) => Some(f.ts.0),
            MarketEvent::Heartbeat { ts } => Some(ts.0),
        };
        if let Some(ts) = event_ts {
            self.session_start_ts.get_or_insert(ts);
            self.last_event_ts = Some(ts);
        }

        match event {
            MarketEvent::BookUpdate { snapshot } => {
                let bid = snapshot.bids.first().map(|l| l.price.0);
                let ask = snapshot.asks.first().map(|l| l.price.0);
                let (Some(b), Some(a)) = (bid, ask) else {
                    return Vec::new();
                };
                let mid = Price((b + a) / Decimal::from(2));
                self.last_mid = Some(mid);
                if !self.placed {
                    let pos_usdt = ctx.position.size.0 * mid.0;
                    let ratio = self.pos_ratio(pos_usdt);
                    self.last_pos_ratio = ratio;
                    self.placed = true;
                    return self.build_batch(ctx.symbol, mid, ratio);
                }
                vec![Action::NoOp]
            }
            MarketEvent::Fill(f) => {
                // Count EVERY fill (partial + full) in the fpm window —
                // partials are real toxic-flow signal too. Only full fills
                // trigger the rebuild check below.
                self.fill_ts.push_back(f.ts.0);
                self.prune_fills(f.ts.0);
                if !f.is_full {
                    return Vec::new();
                }
                let Some(mid) = self.last_mid else {
                    return Vec::new();
                };
                let pos_usdt = ctx.position.size.0 * mid.0;
                let cur_ratio = self.pos_ratio(pos_usdt);
                match self.rebuild_decision(ctx.open_quotes, cur_ratio) {
                    RebuildDecision::None => Vec::new(),
                    RebuildDecision::FullRebuild => {
                        self.last_pos_ratio = cur_ratio;
                        let mut actions =
                            Vec::with_capacity(1 + self.config.levels_per_side as usize * 2);
                        actions.push(Action::CancelAll);
                        actions.extend(self.build_batch(ctx.symbol, mid, cur_ratio));
                        actions
                    }
                    RebuildDecision::RefillSide(side) => {
                        // Don't touch last_pos_ratio — only the drift /
                        // full-rebuild paths anchor the skew snapshot.
                        // Don't CancelAll — the surviving side's orders
                        // are the ones that will close the inventory.
                        self.build_one_side(ctx.symbol, mid, cur_ratio, side)
                    }
                }
            }
            _ => Vec::new(),
        }
    }

    fn on_quote_rejected(
        &mut self,
        ctx: &StrategyContext<'_>,
        _intent: &QuoteIntent,
        _reason: &str,
    ) -> Vec<Action> {
        // Recovery path: a Quote we just emitted (typically from a rebuild
        // after a fill) was post-only rejected because the market moved
        // through our intended price. Read the FRESHEST book the runner
        // can give us, refresh `last_mid`, and emit a new symmetric batch
        // anchored on the current top-of-book mid. The runner retries
        // recovery up to MAX_RECOVERY_ROUNDS until both sides land or the
        // cap fires.
        let bid = ctx.latest_book.bids.first().map(|l| l.price.0);
        let ask = ctx.latest_book.asks.first().map(|l| l.price.0);
        let (Some(b), Some(a)) = (bid, ask) else {
            return Vec::new();
        };
        let mid = Price((b + a) / Decimal::from(2));
        self.last_mid = Some(mid);
        let pos_usdt = ctx.position.size.0 * mid.0;
        let ratio = self.pos_ratio(pos_usdt);
        self.last_pos_ratio = ratio;

        let mut actions = Vec::with_capacity(1 + self.config.levels_per_side as usize * 2);
        actions.push(Action::CancelAll);
        actions.extend(self.build_batch(ctx.symbol, mid, ratio));
        actions
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn cfg(target_fpm: &str, window_secs: u32, sc_min: &str, sc_max: &str) -> StaticGridConfig {
        StaticGridConfig {
            notional_per_order: Decimal::from(100),
            levels_per_side: 2,
            inner_bps: 3,
            step_bps: 2,
            target_inventory_usdt: Decimal::from(50),
            rebuild_pos_ratio_delta: Decimal::from_str("0.3").unwrap(),
            target_fills_per_min: Decimal::from_str(target_fpm).unwrap(),
            fillrate_window_secs: window_secs,
            scale_min: Decimal::from_str(sc_min).unwrap(),
            scale_max: Decimal::from_str(sc_max).unwrap(),
        }
    }

    #[test]
    fn scaler_disabled_returns_one() {
        let g = StaticGrid::new(cfg("0", 60, "1", "4"));
        assert_eq!(g.adaptive_scale(), Decimal::ONE);
    }

    #[test]
    fn scaler_empty_window_returns_one() {
        let g = StaticGrid::new(cfg("5", 60, "1", "4"));
        assert_eq!(g.adaptive_scale(), Decimal::ONE);
    }

    #[test]
    fn scaler_no_session_returns_one() {
        // Has a fill but no session ts (shouldn't happen via on_event,
        // but the guard exists).
        let mut g = StaticGrid::new(cfg("5", 60, "1", "4"));
        g.fill_ts.push_back(1_000_000_000);
        assert_eq!(g.adaptive_scale(), Decimal::ONE);
    }

    #[test]
    fn scaler_at_target_returns_one() {
        let mut g = StaticGrid::new(cfg("5", 60, "1", "4"));
        // Session spans 60s (full window). 5 fills = 5 fpm = target.
        g.session_start_ts = Some(0);
        g.last_event_ts = Some(60_000_000_000);
        for i in 0..5 {
            g.fill_ts.push_back(i * 12_000_000_000);
        }
        assert_eq!(g.adaptive_scale(), Decimal::ONE);
    }

    #[test]
    fn scaler_above_target_widens() {
        let mut g = StaticGrid::new(cfg("5", 60, "1", "4"));
        // 20 fills in 60s = 20 fpm, target 5 → raw 4.0 → clamped at 4.0.
        g.session_start_ts = Some(0);
        g.last_event_ts = Some(60_000_000_000);
        for _ in 0..20 {
            g.fill_ts.push_back(60_000_000_000);
        }
        assert_eq!(g.adaptive_scale(), Decimal::from(4));
    }

    #[test]
    fn scaler_clamps_at_max() {
        let mut g = StaticGrid::new(cfg("5", 60, "1", "4"));
        // 200 fills → 40fpm → raw 8.0 → clamped at 4.0.
        g.session_start_ts = Some(0);
        g.last_event_ts = Some(60_000_000_000);
        for _ in 0..200 {
            g.fill_ts.push_back(60_000_000_000);
        }
        assert_eq!(g.adaptive_scale(), Decimal::from(4));
    }

    #[test]
    fn scaler_clamps_at_min_below_one() {
        let mut g = StaticGrid::new(cfg("10", 60, "0.5", "4"));
        // 1 fill in 60s = 1 fpm, target 10 → raw 0.1 → clamped at 0.5.
        g.session_start_ts = Some(0);
        g.last_event_ts = Some(60_000_000_000);
        g.fill_ts.push_back(60_000_000_000);
        assert_eq!(g.adaptive_scale(), Decimal::from_str("0.5").unwrap());
    }

    #[test]
    fn scaler_rampup_uses_elapsed_not_window() {
        let mut g = StaticGrid::new(cfg("5", 60, "1", "4"));
        // Only 6 seconds elapsed (10% of window), 5 fills.
        // Naive (count/window): 5/1.0 = 5fpm → scale 1.0
        // Correct (count/elapsed): 5/(6/60) = 50fpm → scale 4.0 (clamped)
        g.session_start_ts = Some(0);
        g.last_event_ts = Some(6_000_000_000);
        for _ in 0..5 {
            g.fill_ts.push_back(6_000_000_000);
        }
        assert_eq!(g.adaptive_scale(), Decimal::from(4));
    }

    #[test]
    #[should_panic(expected = "scale_min")]
    fn invalid_scale_bounds_panic_on_new() {
        StaticGrid::new(cfg("5", 60, "5", "2"));
    }
}
