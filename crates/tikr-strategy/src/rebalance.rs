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
//! rebalances to `target_asset_frac` of the total value *at that rung's price*
//! (`T` decreases with price: hold fewer units when it's expensive).
//!
//! # Inventory-aware sizing
//!
//! Side is fixed by price — **sells above mid, buys below** — and the *size* is
//! the gap between current holdings and the rung's target `T[j]`, walking outward
//! from mid (sells shed down to each lower target as price rises; buys add up to
//! each higher target as price falls). A rung is quoted only when there is
//! inventory to move toward its target.
//!
//! This is what keeps it a *rebalancer*. A naive "buy if rung < mid" rule breaks
//! when price runs past the band: every rung ends up below mid and flips to a buy,
//! so the bot accumulates into the rally (observed: WLD +28% → ended 94% asset).
//! Sizing against the target instead means that once we have shed to the
//! top-of-band target, no further buys fire above where they belong — the
//! rebalancer holds (and stays cash-heavy) through a runaway trend.
//!
//! # Reconcile
//!
//! On every book update / fill the strategy recomputes the desired ladder for the
//! current mid + `held` (anchor seed + runner-tracked net position) and diffs it
//! against the resting orders, emitting only the needed placements/cancels.
//!
//! Anchor is fixed (frozen lattice, like [`crate::wave::Wave`]).

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

    /// Desired resting orders given the current `mid` and `held` units.
    ///
    /// Inventory-aware: each rung drives the holding toward that rung's balanced
    /// target `T[j]`. Walking the sell side UP from mid, we shed down to each
    /// (lower) target; walking the buy side DOWN, we add up to each (higher)
    /// target. A rung is only quoted when there is inventory to move toward its
    /// target — so once we have sold out to the band top (high price), no buys
    /// are placed above where they belong, and a runaway rally leaves us holding
    /// the (small) top-of-band target rather than buying into it.
    ///
    /// Side is fixed by price (sells above mid, buys below); the size is the
    /// signed gap to the target, skipped when already past it. This cannot flip
    /// a rung to the wrong side the way a pure mid-vs-rung rule did.
    fn desired(&self, symbol: &Symbol, mid: Price, held: Decimal) -> Vec<QuoteIntent> {
        let len = self.rung_prices.len();
        let min_n = self.config.min_order_notional;
        let mut out = Vec::with_capacity(len);

        let mk = |side: Side, r: Decimal, size: Decimal| QuoteIntent {
            symbol: symbol.clone(),
            side,
            price: Price(r),
            size: Size(size),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        };

        // SELLS — rungs strictly above mid, ascending price. As price rises we
        // shed holdings down to each rung's (lower) target.
        let mut proj = held;
        for idx in 0..len {
            let r = self.rung_prices[idx];
            if r <= mid.0 {
                continue;
            }
            let target = self.rung_units[idx];
            if proj <= target {
                continue; // hold too little to sell here
            }
            let sell = (proj - target).round_dp(8);
            if min_n > Decimal::ZERO && sell * r < min_n {
                continue; // dust — let it roll into a higher rung
            }
            out.push(mk(Side::Ask, r, sell));
            proj = target;
        }

        // BUYS — rungs strictly below mid, descending price. As price falls we
        // add holdings up to each rung's (higher) target.
        let mut proj = held;
        for idx in (0..len).rev() {
            let r = self.rung_prices[idx];
            if r >= mid.0 || r <= Decimal::ZERO {
                continue;
            }
            let target = self.rung_units[idx];
            if proj >= target {
                continue; // already hold enough for this rung
            }
            let buy = (target - proj).round_dp(8);
            if min_n > Decimal::ZERO && buy * r < min_n {
                continue; // dust — let it roll into a lower rung
            }
            out.push(mk(Side::Bid, r, buy));
            proj = target;
        }
        out
    }

    /// Current held units = anchor-balanced seed (`T` at the anchor) plus the
    /// runner-tracked net signed position from this strategy's fills.
    fn held_units(&self, ctx: &StrategyContext<'_>) -> Decimal {
        let a0 = self.rung_units.get(self.n()).copied().unwrap_or_default();
        a0 + ctx.position.size.0
    }

    /// Diff the desired ladder against the resting orders: cancel anything not
    /// desired (by side+price), place anything desired but not resting.
    fn reconcile(&self, ctx: &StrategyContext<'_>, mid: Price) -> Vec<Action> {
        let held = self.held_units(ctx);
        let desired = self.desired(ctx.symbol, mid, held);
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
    fn runaway_rally_does_not_buy_into_it() {
        // Regression: when price runs far above the band (exits the top), every
        // rung sits below mid. A naive mid-vs-rung rule would flip them ALL to
        // buys and accumulate into the rally (the WLD bug). Inventory-aware
        // sizing must instead place NO sells (none above mid) and buys ONLY at
        // rungs below the anchor — never buying above where we already hold the
        // balanced amount.
        let symbol = sym();
        let mut r = Rebalance::new(cfg());
        r.seed(Price(Decimal::from(100)));
        let n = r.n();
        let held = r.rung_units[n]; // a0, balanced at the anchor
        let high_mid = Decimal::from(150); // +50%, above all rungs
        let desired = r.desired(&symbol, Price(high_mid), held);

        assert!(
            desired.iter().all(|q| q.side == Side::Bid),
            "no sells when the band is exhausted above"
        );
        let anchor_price = r.rung_prices[n];
        for q in &desired {
            assert!(
                q.price.0 < anchor_price,
                "must not buy at/above the anchor while already at target (rung {})",
                q.price.0
            );
        }
    }

    #[test]
    fn sells_down_to_target_as_price_rises() {
        // With balanced holdings at the anchor, rungs above mid are sells sized
        // to shed toward each (lower) target — the rebalancer sheds into a rally.
        let symbol = sym();
        let mut r = Rebalance::new(cfg());
        r.seed(Price(Decimal::from(100)));
        let n = r.n();
        let held = r.rung_units[n];
        let desired = r.desired(&symbol, Price(Decimal::from(100)), held);
        let first_up = r.rung_prices[n + 1];
        let sell = desired
            .iter()
            .find(|q| q.price.0 == first_up)
            .expect("sell at first up-rung");
        assert_eq!(sell.side, Side::Ask);
        // Sheds exactly a0 − T[first up-rung].
        assert_eq!(sell.size.0, (held - r.rung_units[n + 1]).round_dp(8));
    }
}
