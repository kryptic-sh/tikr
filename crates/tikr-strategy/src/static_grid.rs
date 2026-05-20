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

    /// Emit `2N` orders symmetric around `mid` at the configured layout.
    fn build_batch(&self, symbol: &Symbol, mid: Price) -> Vec<Action> {
        let mut actions = Vec::with_capacity(self.config.levels_per_side as usize * 2);
        let bp_unit = Decimal::from(10_000);
        for k in 0..self.config.levels_per_side {
            let bps = self.config.inner_bps + self.config.step_bps * k;
            let offset = Decimal::from(bps) / bp_unit;
            let buy = Price(mid.0 * (Decimal::ONE - offset));
            let sell = Price(mid.0 * (Decimal::ONE + offset));
            actions.push(self.make_quote(symbol, Side::Bid, buy));
            actions.push(self.make_quote(symbol, Side::Ask, sell));
        }
        actions
    }

    fn rebuild_needed(open_quotes: &[(QuoteId, QuoteIntent)]) -> bool {
        let total = open_quotes.len();
        if total <= 2 {
            return true;
        }
        let buys = open_quotes
            .iter()
            .filter(|(_, q)| q.side == Side::Bid)
            .count();
        let sells = total - buys;
        buys == 0 || sells == 0
    }
}

impl Strategy for StaticGrid {
    type Config = StaticGridConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            placed: false,
            last_mid: None,
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
                    self.placed = true;
                    return self.build_batch(ctx.symbol, mid);
                }
                vec![Action::NoOp]
            }
            MarketEvent::Fill(f) if f.is_full => {
                // Defense-in-depth: runner already gates partials, but we also
                // ignore here so a leak can't rebuild prematurely.
                if !Self::rebuild_needed(ctx.open_quotes) {
                    return Vec::new();
                }
                let Some(mid) = self.last_mid else {
                    return Vec::new();
                };
                // CancelAll wipes the few remaining stragglers; fresh batch
                // re-anchors on current mid.
                let mut actions = Vec::with_capacity(1 + self.config.levels_per_side as usize * 2);
                actions.push(Action::CancelAll);
                actions.extend(self.build_batch(ctx.symbol, mid));
                actions
            }
            _ => Vec::new(),
        }
    }
}
