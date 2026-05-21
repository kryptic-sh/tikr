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
    /// Inventory-skew strength. `0.0` = symmetric grid (no skew). `0.5` =
    /// each-side gap scales by ±50% at saturation. `1.0` = ±100% (closer
    /// side approaches 0 bps — floored at 1 bp for maker safety).
    ///
    /// When `position_usdt > 0` (long), buy gaps widen and sell gaps
    /// tighten — encouraging sells to fill and reduce inventory.
    pub skew_strength: Decimal,
    /// Position USDT magnitude at which skew saturates (clamped to ±1).
    /// Smaller value = grid responds to small inventory; larger = needs
    /// big position to skew. Pick close to your acceptable inventory cap.
    pub target_inventory_usdt: Decimal,
    /// Rebuild threshold for inventory-ratio drift. When the position
    /// ratio changes by more than this since the last rebuild, force a
    /// fresh batch placement at the newly-skewed prices.
    /// Default 0.3 (i.e. 30% saturation move). Set high to disable.
    pub rebuild_pos_ratio_delta: Decimal,
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
    /// When `pos_ratio > 0` (long), buy gaps widen and sell gaps tighten;
    /// when negative, the reverse. Closer-side gaps floor at 1 bp to keep
    /// the order maker-safe — without this clamp a fully-saturated grid
    /// with `skew_strength = 1.0` would try to post at the current mid and
    /// get post-only rejected.
    fn build_batch(&self, symbol: &Symbol, mid: Price, pos_ratio: Decimal) -> Vec<Action> {
        let mut actions = Vec::with_capacity(self.config.levels_per_side as usize * 2);
        let bp_unit = Decimal::from(10_000);
        let buy_scale = Decimal::ONE + pos_ratio * self.config.skew_strength;
        let sell_scale = Decimal::ONE - pos_ratio * self.config.skew_strength;
        let one_bp = Decimal::ONE / bp_unit;
        for k in 0..self.config.levels_per_side {
            let base_bps = Decimal::from(self.config.inner_bps + self.config.step_bps * k);
            let buy_bps = (base_bps * buy_scale).max(Decimal::ONE);
            let sell_bps = (base_bps * sell_scale).max(Decimal::ONE);
            let buy = Price(mid.0 * (Decimal::ONE - buy_bps / bp_unit));
            let sell = Price(mid.0 * (Decimal::ONE + sell_bps / bp_unit));
            // Defensive: keep at least 1 bp inside if floor kicked in to
            // avoid zero-spread placements.
            let _ = one_bp;
            actions.push(self.make_quote(symbol, Side::Bid, buy));
            actions.push(self.make_quote(symbol, Side::Ask, sell));
        }
        actions
    }

    fn rebuild_needed(
        &self,
        open_quotes: &[(QuoteId, QuoteIntent)],
        cur_pos_ratio: Decimal,
    ) -> bool {
        let total = open_quotes.len();
        if total <= 2 {
            return true;
        }
        let buys = open_quotes
            .iter()
            .filter(|(_, q)| q.side == Side::Bid)
            .count();
        let sells = total - buys;
        if buys == 0 || sells == 0 {
            return true;
        }
        // Inventory drifted past the rebuild threshold since the last
        // batch placement — re-anchor at the newly-skewed prices.
        let delta = (cur_pos_ratio - self.last_pos_ratio).abs();
        if delta > self.config.rebuild_pos_ratio_delta {
            return true;
        }
        false
    }
}

impl Strategy for StaticGrid {
    type Config = StaticGridConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            placed: false,
            last_mid: None,
            last_pos_ratio: Decimal::ZERO,
        }
    }

    fn name(&self) -> &str {
        "static-grid"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
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
            MarketEvent::Fill(f) if f.is_full => {
                // Defense-in-depth: runner already gates partials, but we also
                // ignore here so a leak can't rebuild prematurely.
                let Some(mid) = self.last_mid else {
                    return Vec::new();
                };
                let pos_usdt = ctx.position.size.0 * mid.0;
                let cur_ratio = self.pos_ratio(pos_usdt);
                if !self.rebuild_needed(ctx.open_quotes, cur_ratio) {
                    return Vec::new();
                }
                self.last_pos_ratio = cur_ratio;
                // CancelAll wipes the few remaining stragglers; fresh batch
                // re-anchors on current mid with inventory-aware skew.
                let mut actions = Vec::with_capacity(1 + self.config.levels_per_side as usize * 2);
                actions.push(Action::CancelAll);
                actions.extend(self.build_batch(ctx.symbol, mid, cur_ratio));
                actions
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
