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
//! # Frozen orders, mirror on fill
//!
//! The ladder is placed exactly once, at the anchor: a BUY at every rung below
//! and a SELL at every rung above, each sized to its gap's `T`-increment. Because
//! `T` is built recursively, a level's size already accounts for every order
//! between it and the anchor — the precomputed quantity is exactly the trade that
//! restores 50/50 at that rung *given the inner rungs have filled*.
//!
//! Orders then **rest untouched**. The strategy does nothing on subsequent book
//! updates — it never recomputes or cancels. Only a fill moves the lattice: a
//! filled order is mirrored one rung toward where price came from, carrying the
//! same quantity (buy at `r_j` → sell at `r_{j+1}`; sell at `r_j` → buy at
//! `r_{j-1}`). That mirror is the offsetting leg that books the round-trip and
//! settles inventory back to balance, and it keeps the sizes frozen forever.
//!
//! This is what makes it correct through a runaway trend: a rung above the anchor
//! is a SELL until it fills, then it becomes a BUY *one rung lower* — it never
//! turns into a buy at its own (high) price, so the bot can't accumulate into a
//! rally. Within the band the holding always equals the balanced `T` for the
//! current price, so cash ≈ asset value; only past the band (ladder exhausted)
//! does the ratio drift.
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
    /// Whether the frozen ladder has been placed. The ladder waits until the
    /// anchor long (`a0` units) is fully open — on spot the runner pre-seeds it
    /// so this is immediate; on futures the strategy opens it with a taker buy
    /// first (see `on_event`).
    ladder_placed: bool,
    /// Rungs rejected at placement (post-only would-cross while price sat on
    /// them). Without recovery a reject is a permanent hole in the frozen
    /// ladder. Re-placed on later BookUpdates once no longer crossing.
    rejected: Vec<QuoteIntent>,
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

    fn mk(&self, symbol: &Symbol, side: Side, idx: usize, size: Decimal) -> QuoteIntent {
        QuoteIntent {
            symbol: symbol.clone(),
            side,
            price: Price(self.rung_prices[idx]),
            size: Size(size),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        }
    }

    /// The one-time frozen ladder placed at the anchor: a BUY at every rung below
    /// the anchor and a SELL at every rung above. Each order is sized to the
    /// `T`-increment of the gap it guards — i.e. the exact quantity that, when it
    /// fills, restores the 50/50 balance at that rung (the recursive `T` already
    /// folds in every inner rung's fill, so a level "knows about" the orders
    /// closer to the anchor). Orders then rest untouched; fills are mirrored one
    /// rung over by [`Self::mirror`].
    fn initial_ladder(&self, symbol: &Symbol) -> Vec<Action> {
        let len = self.rung_prices.len();
        let min_n = self.config.min_order_notional;
        let anchor_idx = self.n();
        let mut out = Vec::with_capacity(len);
        for idx in 0..len {
            let r = self.rung_prices[idx];
            if r <= Decimal::ZERO {
                continue;
            }
            // Classify by rung position relative to the FROZEN anchor, not
            // the live mid: the futures open path takes several books, and
            // if mid drifted a band in that window, a mid-based split would
            // give rungs between the anchor and the new mid the wrong role —
            // breaking the constant-mix invariant from the very first fill
            // (holdings would exceed the balanced `T` for that price).
            let (side, size) = if idx < anchor_idx && idx + 1 < len {
                // BUY guarding the gap above it: T[idx] − T[idx+1].
                (Side::Bid, self.rung_units[idx] - self.rung_units[idx + 1])
            } else if idx > anchor_idx && idx >= 1 {
                // SELL guarding the gap below it: T[idx-1] − T[idx].
                (Side::Ask, self.rung_units[idx - 1] - self.rung_units[idx])
            } else {
                continue; // the anchor rung itself
            };
            let size = size.round_dp(8);
            if size <= Decimal::ZERO {
                continue;
            }
            if min_n > Decimal::ZERO && size * r < min_n {
                continue;
            }
            out.push(Action::Quote(self.mk(symbol, side, idx, size)));
        }
        out
    }

    /// Mirror a fill one rung toward where price came from, with the SAME size —
    /// the offsetting leg that books the round-trip and settles inventory back to
    /// the balance for the new band. A filled BUY at `r_j` (price dipped) becomes
    /// a SELL at `r_{j+1}`; a filled SELL at `r_j` (price rose) becomes a BUY at
    /// `r_{j-1}`. Sizes never drift because each mirror carries the filled
    /// quantity, so the lattice stays the frozen ladder it was seeded as.
    fn mirror(
        &self,
        symbol: &Symbol,
        fill_price: Decimal,
        side: Side,
        qty: Decimal,
    ) -> Vec<Action> {
        let Some(idx) = self.rung_prices.iter().position(|p| *p == fill_price) else {
            return Vec::new();
        };
        let len = self.rung_prices.len();
        let qty = qty.round_dp(8);
        if qty <= Decimal::ZERO {
            return Vec::new();
        }
        // Ownership guard: a ladder/mirror order at this rung never exceeds
        // the increment it guards. A larger fill — e.g. the opening IOC
        // landing exactly on a rung price after `ladder_placed` flipped —
        // is not ours to mirror; mirroring it would dump the entire anchor
        // position one band over.
        let increment = match side {
            Side::Bid if idx + 1 < len => self.rung_units[idx] - self.rung_units[idx + 1],
            Side::Ask if idx >= 1 => self.rung_units[idx - 1] - self.rung_units[idx],
            _ => return Vec::new(),
        }
        .round_dp(8);
        let dust = Decimal::new(1, 8);
        if qty > increment + dust {
            return Vec::new();
        }
        match side {
            // Buy filled (price came down) → sell back one rung up.
            Side::Bid => vec![Action::Quote(self.mk(symbol, Side::Ask, idx + 1, qty))],
            // Sell filled (price came up) → buy back one rung down.
            Side::Ask => vec![Action::Quote(self.mk(symbol, Side::Bid, idx - 1, qty))],
        }
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
            ladder_placed: false,
            rejected: Vec::new(),
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
                if self.ladder_placed {
                    // Orders rest; only fills move the lattice. The exception:
                    // re-place rejected rungs once they'd rest post-only again.
                    if self.rejected.is_empty() {
                        return Vec::new();
                    }
                    let best_bid = snapshot.bids.first().map(|l| l.price.0);
                    let best_ask = snapshot.asks.first().map(|l| l.price.0);
                    let mut out = Vec::new();
                    self.rejected.retain(|intent| {
                        let ok = match intent.side {
                            Side::Bid => best_ask.is_some_and(|a| intent.price.0 < a),
                            Side::Ask => best_bid.is_some_and(|b| intent.price.0 > b),
                        };
                        if ok {
                            out.push(Action::Quote(intent.clone()));
                        }
                        !ok
                    });
                    return out;
                }
                // Open (or top up) the anchor long `a0` before placing the ladder.
                // Spot pre-seeds the position so `a0` is already held → this is a
                // no-op and the ladder goes down immediately. Futures starts flat,
                // so we open the long with a marketable IOC buy first — that is the
                // "open a long at the asset allocation" step that lets the grid then
                // trade it exactly like a spot inventory.
                let a0 = self.rung_units.get(self.n()).copied().unwrap_or_default();
                let to_open = (a0 - ctx.position.size.0).round_dp(8);
                let open_floor = self.config.min_order_notional.max(Decimal::ONE);
                if to_open > Decimal::ZERO && to_open * mid.0 >= open_floor {
                    let ask = ctx
                        .latest_book
                        .asks
                        .first()
                        .map(|l| l.price.0)
                        .unwrap_or(mid.0);
                    // Cross generously so the IOC clears available depth; it tops
                    // up over successive books until the full long is open.
                    let limit = (ask * Decimal::new(101, 2)).round_dp(8);
                    return vec![Action::Quote(QuoteIntent {
                        symbol: ctx.symbol.clone(),
                        side: Side::Bid,
                        price: Price(limit),
                        size: Size(to_open),
                        tif: TimeInForce::IOC,
                        kind: QuoteKind::Point,
                    })];
                }
                self.ladder_placed = true;
                self.initial_ladder(ctx.symbol)
            }
            // A resting ladder order filled → place its offsetting mirror. Opening
            // (taker) fills happen before the ladder and never match a rung price,
            // so they don't trigger a mirror.
            MarketEvent::Fill(fill) => {
                if !self.ladder_placed {
                    return Vec::new();
                }
                self.mirror(ctx.symbol, fill.price.0, fill.side, fill.size.0)
            }
            MarketEvent::Heartbeat { .. } | MarketEvent::Trade { .. } => Vec::new(),
        }
    }

    /// Park a rejected rung for re-placement (see the `rejected` field). Only
    /// ladder/mirror rungs matter — an opening IOC reject just retries on the
    /// next book via the top-up path.
    fn on_quote_rejected(
        &mut self,
        _ctx: &StrategyContext<'_>,
        intent: &QuoteIntent,
        _reason: &str,
    ) -> Vec<Action> {
        if self.ladder_placed && intent.tif == TimeInForce::PostOnly {
            self.rejected.push(intent.clone());
        }
        Vec::new()
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

    /// Position holding `units` long (spot pre-seeds the anchor `a0`).
    fn pos_long(symbol: &Symbol, units: Decimal) -> Position {
        Position {
            symbol: symbol.clone(),
            size: SignedSize(units),
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
        // Spot pre-seeds a0 = 50% × $10k / $100 = 50 units long.
        let position = pos_long(&symbol, Decimal::from(50));
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
    fn buy_fill_mirrors_to_sell_one_rung_up_same_size() {
        // A filled BUY at r_j must mirror to a SELL at r_{j+1} carrying the SAME
        // quantity — the offsetting leg that books the round-trip. Sizes never
        // drift; the lattice stays frozen.
        let symbol = sym();
        let mut r = Rebalance::new(cfg());
        r.seed(Price(Decimal::from(100)));
        let n = r.n();
        let j = n - 1; // first rung below anchor
        let qty = (r.rung_units[j] - r.rung_units[j + 1]).round_dp(8);
        let out = r.mirror(&symbol, r.rung_prices[j], Side::Bid, qty);
        assert_eq!(out.len(), 1);
        match &out[0] {
            Action::Quote(q) => {
                assert_eq!(q.side, Side::Ask);
                assert_eq!(q.price.0, r.rung_prices[j + 1]);
                assert_eq!(q.size.0, qty);
            }
            _ => panic!("expected a mirror quote"),
        }
    }

    #[test]
    fn sell_fill_mirrors_to_buy_one_rung_down_same_size() {
        let symbol = sym();
        let mut r = Rebalance::new(cfg());
        r.seed(Price(Decimal::from(100)));
        let n = r.n();
        let j = n + 1; // first rung above anchor
        let qty = (r.rung_units[j - 1] - r.rung_units[j]).round_dp(8);
        let out = r.mirror(&symbol, r.rung_prices[j], Side::Ask, qty);
        assert_eq!(out.len(), 1);
        match &out[0] {
            Action::Quote(q) => {
                assert_eq!(q.side, Side::Bid);
                assert_eq!(q.price.0, r.rung_prices[j - 1]);
                assert_eq!(q.size.0, qty);
            }
            _ => panic!("expected a mirror quote"),
        }
    }

    #[test]
    fn second_book_update_is_a_noop() {
        // The ladder is placed once; later book updates must NOT re-quote or
        // cancel — orders rest in place until filled.
        let symbol = sym();
        let snap = book(&symbol, Decimal::from(100), Decimal::from(100));
        let position = pos_long(&symbol, Decimal::from(50)); // spot pre-seed
        let mut r = Rebalance::new(cfg());
        let ctx1 = StrategyContext {
            symbol: &symbol,
            now: Timestamp(1),
            position: &position,
            recent_fills: &[],
            latest_book: &snap,
            open_quotes: &[],
            recent_liqs: &[],
        };
        let first = r.on_event(
            &ctx1,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        assert!(!first.is_empty(), "first book seeds the ladder");
        let snap2 = book(&symbol, Decimal::new(1001, 1), Decimal::new(1002, 1));
        let ctx2 = StrategyContext {
            symbol: &symbol,
            now: Timestamp(2),
            position: &position,
            recent_fills: &[],
            latest_book: &snap2,
            open_quotes: &[],
            recent_liqs: &[],
        };
        let second = r.on_event(
            &ctx2,
            &MarketEvent::BookUpdate {
                snapshot: snap2.clone(),
            },
        );
        assert!(
            second.is_empty(),
            "later book updates leave orders in place"
        );
    }

    #[test]
    fn futures_opens_long_before_placing_ladder() {
        // Starting flat (futures), the first book must emit a marketable IOC BUY
        // to open the anchor long a0 — NOT the ladder yet. Only once the long is
        // held does the ladder go down (proven by feeding the position back).
        let symbol = sym();
        let snap = book(&symbol, Decimal::from(100), Decimal::from(100));
        let flat = pos_long(&symbol, Decimal::ZERO);
        let mut r = Rebalance::new(cfg());
        let ctx_flat = StrategyContext {
            symbol: &symbol,
            now: Timestamp(1),
            position: &flat,
            recent_fills: &[],
            latest_book: &snap,
            open_quotes: &[],
            recent_liqs: &[],
        };
        let open = r.on_event(
            &ctx_flat,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        assert_eq!(open.len(), 1, "exactly one opening order");
        match &open[0] {
            Action::Quote(q) => {
                assert_eq!(q.side, Side::Bid);
                assert_eq!(q.tif, TimeInForce::IOC);
                assert_eq!(q.size.0, Decimal::from(50)); // a0 = 50 units
            }
            _ => panic!("expected an IOC open"),
        }
        // Now hold a0; the ladder should be placed.
        let held = pos_long(&symbol, Decimal::from(50));
        let ctx_held = StrategyContext {
            symbol: &symbol,
            now: Timestamp(2),
            position: &held,
            recent_fills: &[],
            latest_book: &snap,
            open_quotes: &[],
            recent_liqs: &[],
        };
        let ladder = r.on_event(
            &ctx_held,
            &MarketEvent::BookUpdate {
                snapshot: snap.clone(),
            },
        );
        let buys = ladder
            .iter()
            .filter(|a| matches!(a, Action::Quote(q) if q.side == Side::Bid))
            .count();
        let sells = ladder
            .iter()
            .filter(|a| matches!(a, Action::Quote(q) if q.side == Side::Ask))
            .count();
        assert_eq!((buys, sells), (3, 3), "ladder placed once long is open");
    }
}
