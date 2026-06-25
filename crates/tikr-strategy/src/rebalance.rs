//! Constant-mix rebalancing market-maker.
//!
//! Holds a fixed target ratio (default 50% asset / 50% cash by value) and trades
//! back toward it as price drifts: price down → asset value drops below target →
//! buy; price up → sell. This is the classic constant-mix / "Shannon's Demon"
//! rebalancing portfolio applied as a maker grid — it harvests volatility (the
//! rebalancing premium) and, crucially, is **inventory-bounded**: every trade only
//! restores the target ratio, so cash never fully drains and the bag never runs
//! away (unlike a fixed-notional grid).
//!
//! # Frozen rebalance lattice
//!
//! On the first book, anchor at the current mid `p0` and seed `(cash, units)` from
//! `initial_balance` split by `target_asset_frac`. Rungs sit at geometric steps
//! `p0·(1 ± band)^k`, `k = 1..=levels`. We precompute the *balanced* unit holding
//! `T[j]` at each rung `j` recursively — walking out from the anchor, each rung
//! rebalances to `target_asset_frac` of the total value *at that rung's price*.
//! Each rung's order size is then the `T` increment across it:
//!
//! ```text
//!   BUY  at r_j  (r_j < mid):  size = T[j]   − T[j+1]   (add units crossing down)
//!   SELL at r_j  (r_j > mid):  size = T[j-1] − T[j]     (shed units crossing up)
//! ```
//!
//! Both are positive because `T` decreases with price. A buy at `r_j` and the sell
//! at `r_{j+1}` that undoes it trade the same gap increment, so an oscillation
//! within a gap round-trips cleanly and books the rebalancing spread.
//!
//! # Reconcile
//!
//! On every book update / fill the strategy recomputes the desired ladder for the
//! current mid (buys below, sells above) and diffs it against the resting orders —
//! emitting only the placements/cancels needed. As price crosses a rung the rung
//! flips buy↔sell, which is what continuously rebalances toward the target.
//!
//! Anchor is fixed (frozen lattice, like [`crate::wave::Wave`]); if price exits the
//! `±levels·band` band the ladder is exhausted on that side — the rebalancer
//! deliberately caps how far it follows a trend.

use tikr_core::{Decimal, MarketEvent, Price, QuoteKind, Side, Size, Symbol, TimeInForce};
use tikr_venue::QuoteIntent;

use crate::{Action, Strategy, StrategyContext, compute_mid_strict};

/// Configuration for [`Rebalance`].
#[derive(Debug, Clone)]
pub struct RebalanceConfig {
    /// Rung spacing from the anchor, in basis points (geometric). Default 50.
    pub band_bps: u32,
    /// Number of rungs per side. Total resting orders ≤ `2 × levels`.
    pub levels: u32,
    /// Target asset fraction by value (0..1). `0.5` = 50/50.
    pub target_asset_frac: Decimal,
    /// Total account value at seed (USD). Splits into cash + asset by
    /// `target_asset_frac` at the anchor price.
    pub initial_balance: Decimal,
    /// Skip any rung whose order notional (`size × price`) is below this.
    /// Prevents dust orders. `0` disables the floor.
    pub min_order_notional: Decimal,
}

impl Default for RebalanceConfig {
    fn default() -> Self {
        Self {
            band_bps: 50,
            levels: 10,
            target_asset_frac: Decimal::new(5, 1), // 0.5
            initial_balance: Decimal::from(10_000),
            min_order_notional: Decimal::from(5),
        }
    }
}

/// Constant-mix rebalancing market-maker.
pub struct Rebalance {
    config: RebalanceConfig,
    /// Anchor mid, set on the first book update. `None` until seeded.
    anchor: Option<Price>,
    /// Rung prices, index `0..2N+1`, ascending (index `j+levels`).
    rung_prices: Vec<Decimal>,
    /// Balanced unit holding at each rung, same indexing as `rung_prices`.
    rung_units: Vec<Decimal>,
}

impl Rebalance {
    /// Number of rungs per side as `usize`.
    fn n(&self) -> usize {
        self.config.levels as usize
    }

    /// Seed the frozen lattice from the anchor mid. Precomputes rung prices and
    /// the balanced unit holding `T[j]` at each rung.
    fn seed(&mut self, anchor: Price) {
        let n = self.n();
        let w = self.config.target_asset_frac;
        let p0 = anchor.0;
        let band = Decimal::from(self.config.band_bps) / Decimal::from(10_000);
        let up = Decimal::ONE + band;
        let down = Decimal::ONE - band;
        let v0 = self.config.initial_balance;

        // Anchor inventory, balanced at p0.
        let a0 = (v0 * w / p0).round_dp(10);
        let u0 = (v0 * (Decimal::ONE - w)).round_dp(10);

        // Index layout: idx = j + n, j ∈ [-n, n]. idx n is the anchor.
        let len = 2 * n + 1;
        let mut prices = vec![Decimal::ZERO; len];
        let mut units = vec![Decimal::ZERO; len];
        prices[n] = p0;
        units[n] = a0;

        // Walk DOWN (j = -1..-n): price falls, buy to restore the target.
        let mut u = u0;
        let mut price = p0;
        for step in 1..=n {
            price = (price * down).round_dp(10);
            let a_prev = units[n - step + 1];
            let v = u + a_prev * price;
            let buy_val = (w * v - a_prev * price).round_dp(10);
            let size = (buy_val / price).round_dp(10);
            prices[n - step] = price;
            units[n - step] = a_prev + size;
            u = (u - buy_val).round_dp(10);
        }
        // Walk UP (j = 1..n): price rises, sell to restore the target.
        let mut u = u0;
        let mut price = p0;
        for step in 1..=n {
            price = (price * up).round_dp(10);
            let a_prev = units[n + step - 1];
            let v = u + a_prev * price;
            let sell_val = (a_prev * price - w * v).round_dp(10);
            let size = (sell_val / price).round_dp(10);
            prices[n + step] = price;
            units[n + step] = a_prev - size;
            u = (u + sell_val).round_dp(10);
        }

        self.rung_prices = prices;
        self.rung_units = units;
        self.anchor = Some(anchor);
    }

    /// Desired resting orders for the current `mid`: buys at rungs below mid,
    /// sells at rungs above. Size = the balanced `T` increment across the rung.
    fn desired(&self, symbol: &Symbol, mid: Price) -> Vec<QuoteIntent> {
        let len = self.rung_prices.len();
        let mut out = Vec::with_capacity(len);
        for idx in 0..len {
            let r = self.rung_prices[idx];
            if r <= Decimal::ZERO {
                continue;
            }
            let (side, size) = if r < mid.0 {
                // BUY: add T[idx] − T[idx+1] units crossing down into this rung.
                if idx + 1 >= len {
                    continue;
                }
                (Side::Bid, self.rung_units[idx] - self.rung_units[idx + 1])
            } else if r > mid.0 {
                // SELL: shed T[idx-1] − T[idx] units crossing up into this rung.
                if idx == 0 {
                    continue;
                }
                (Side::Ask, self.rung_units[idx - 1] - self.rung_units[idx])
            } else {
                continue; // rung exactly at mid — skip
            };
            let size = size.round_dp(8);
            if size <= Decimal::ZERO {
                continue;
            }
            if self.config.min_order_notional > Decimal::ZERO
                && (size * r) < self.config.min_order_notional
            {
                continue;
            }
            out.push(QuoteIntent {
                symbol: symbol.clone(),
                side,
                price: Price(r),
                size: Size(size),
                tif: TimeInForce::PostOnly,
                kind: QuoteKind::Point,
            });
        }
        out
    }

    /// Diff the desired ladder against the resting orders: cancel anything not
    /// desired (by side+price), place anything desired but not resting.
    fn reconcile(&self, ctx: &StrategyContext<'_>, mid: Price) -> Vec<Action> {
        let desired = self.desired(ctx.symbol, mid);
        let mut actions = Vec::new();

        // Cancel resting orders that are not in the desired set.
        for (id, intent) in ctx.open_quotes {
            let keep = desired
                .iter()
                .any(|d| d.side == intent.side && d.price.0 == intent.price.0);
            if !keep {
                actions.push(Action::Cancel(*id));
            }
        }
        // Place desired orders that are not already resting (matched by
        // side+price; size drift inside a rung is left alone to avoid churn).
        for d in desired {
            let resting = ctx
                .open_quotes
                .iter()
                .any(|(_, q)| q.side == d.side && q.price.0 == d.price.0);
            if !resting {
                actions.push(Action::Quote(d));
            }
        }
        actions
    }
}

impl Strategy for Rebalance {
    type Config = RebalanceConfig;

    fn new(config: Self::Config) -> Self {
        Self {
            config,
            anchor: None,
            rung_prices: Vec::new(),
            rung_units: Vec::new(),
        }
    }

    fn name(&self) -> &str {
        "rebalance"
    }

    fn on_event(&mut self, ctx: &StrategyContext<'_>, event: &MarketEvent) -> Vec<Action> {
        match event {
            MarketEvent::BookUpdate { snapshot } => {
                let Some(mid) = compute_mid_strict(snapshot) else {
                    return Vec::new();
                };
                if self.anchor.is_none() {
                    self.seed(mid);
                }
                self.reconcile(ctx, mid)
            }
            MarketEvent::Fill(_) => {
                if self.anchor.is_none() {
                    return Vec::new();
                }
                let Some(mid) = compute_mid_strict(ctx.latest_book) else {
                    return Vec::new();
                };
                self.reconcile(ctx, mid)
            }
            MarketEvent::Heartbeat { .. } | MarketEvent::Trade { .. } => Vec::new(),
        }
    }

    fn on_quote_rejected(
        &mut self,
        ctx: &StrategyContext<'_>,
        _intent: &QuoteIntent,
        _reason: &str,
    ) -> Vec<Action> {
        if self.anchor.is_none() {
            return Vec::new();
        }
        let Some(mid) = compute_mid_strict(ctx.latest_book) else {
            return Vec::new();
        };
        self.reconcile(ctx, mid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::{
        Asset, Level, MarketKind, Notional, Position, SignedSize, Snapshot, Timestamp, VenueId,
    };

    fn sym() -> Symbol {
        Symbol {
            base: Asset::new("BTC"),
            quote: Asset::new("USDC"),
            venue: VenueId::new("test"),
            kind: MarketKind::Perp,
        }
    }

    fn book(symbol: &Symbol, bid: Decimal, ask: Decimal) -> Snapshot {
        Snapshot {
            symbol: symbol.clone(),
            bids: vec![Level {
                price: Price(bid),
                size: Size(Decimal::ONE),
            }],
            asks: vec![Level {
                price: Price(ask),
                size: Size(Decimal::ONE),
            }],
            ts: Timestamp(1),
        }
    }

    fn pos(symbol: &Symbol) -> Position {
        Position {
            symbol: symbol.clone(),
            size: SignedSize(Decimal::ZERO),
            avg_entry: Price(Decimal::ZERO),
            realized_pnl: Notional(Decimal::ZERO),
        }
    }

    fn cfg() -> RebalanceConfig {
        RebalanceConfig {
            band_bps: 50,
            levels: 3,
            target_asset_frac: Decimal::new(5, 1),
            initial_balance: Decimal::from(10_000),
            min_order_notional: Decimal::ZERO,
        }
    }

    #[test]
    fn seed_balances_at_anchor() {
        let mut r = Rebalance::new(cfg());
        r.seed(Price(Decimal::from(100)));
        let n = r.n();
        // Anchor: 50% of $10k in asset at $100 = $5000 / 100 = 50 units.
        assert_eq!(r.rung_units[n], Decimal::from(50));
        // Units strictly decrease as price (index) rises.
        for i in 1..r.rung_units.len() {
            assert!(
                r.rung_units[i] < r.rung_units[i - 1],
                "T must decrease with price at idx {i}"
            );
        }
    }

    #[test]
    fn each_rung_restores_target_fraction() {
        // Walking out from the anchor, the precomputed (cash, units) at each rung
        // must hold exactly target_frac of the value AT that rung. Reconstruct
        // cash by undoing each trade and check the 50/50 split.
        let mut r = Rebalance::new(cfg());
        let p0 = Decimal::from(100);
        r.seed(Price(p0));
        let n = r.n();
        let w = Decimal::new(5, 1);
        // Recompute cash forward along the DOWN walk and assert balance.
        let mut cash = r.config.initial_balance * (Decimal::ONE - w);
        for step in 1..=n {
            let idx = n - step;
            let price = r.rung_prices[idx];
            let bought = r.rung_units[idx] - r.rung_units[idx + 1];
            cash -= bought * price;
            let asset_val = r.rung_units[idx] * price;
            let total = cash + asset_val;
            let frac = asset_val / total;
            assert!(
                (frac - w).abs() < Decimal::new(1, 4),
                "rung {idx}: asset frac {frac} != {w}"
            );
        }
    }

    #[test]
    fn first_book_seeds_and_places_ladder() {
        let symbol = sym();
        let snap = book(&symbol, Decimal::from(100), Decimal::from(100));
        let position = pos(&symbol);
        let mut r = Rebalance::new(cfg());
        let ctx = StrategyContext {
            symbol: &symbol,
            now: Timestamp(1),
            position: &position,
            recent_fills: &[],
            latest_book: &snap,
            open_quotes: &[],
            recent_liqs: &[],
        };
        let actions = r.on_event(
            &ctx,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        let buys = actions
            .iter()
            .filter(|a| matches!(a, Action::Quote(q) if q.side == Side::Bid))
            .count();
        let sells = actions
            .iter()
            .filter(|a| matches!(a, Action::Quote(q) if q.side == Side::Ask))
            .count();
        // mid == 100 == anchor: 3 rungs below (buys), 3 above (sells).
        assert_eq!(buys, 3, "expected 3 buy rungs");
        assert_eq!(sells, 3, "expected 3 sell rungs");
    }

    #[test]
    fn buy_rung_flips_to_sell_when_price_drops_below_it() {
        // Anchor at 100; drop mid below the first down-rung. That rung must now
        // be a SELL (price recovered would rebalance back), proving the round-trip
        // capture mechanism.
        let symbol = sym();
        let mut r = Rebalance::new(cfg());
        r.seed(Price(Decimal::from(100)));
        let n = r.n();
        let first_down = r.rung_prices[n - 1]; // ~99.5
        // mid below that rung.
        let low_mid = first_down - Decimal::ONE;
        let desired = r.desired(&symbol, Price(low_mid));
        let at_rung: Vec<_> = desired.iter().filter(|q| q.price.0 == first_down).collect();
        assert_eq!(at_rung.len(), 1);
        assert_eq!(at_rung[0].side, Side::Ask, "rung above mid must be a sell");
    }
}
