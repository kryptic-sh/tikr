//! Cross-margin forced-liquidation model for backtest / paper runs.
//!
//! Models Binance USDⓈ-M **cross** margin (the account default — the bot never
//! sets `marginType`, so the whole wallet backs the position). Binance
//! liquidates when **margin balance ≤ maintenance margin**:
//!
//! - `margin balance = wallet balance + unrealized PnL`
//! - `maintenance margin = position notional × maintenance-margin-rate`
//!   (single symbol, lowest tier → maintenance amount ≈ 0)
//!
//! The crucial property: liquidation depends on the **wallet**, the **position
//! notional**, and the **mark** — NOT on the position's leverage and NOT on bag
//! size in isolation. A small bag backed by a large idle wallet is nearly
//! impossible to liquidate (the wallet absorbs the loss); only a bag near full
//! buying power (`notional ≈ wallet × leverage`) liquidates on the familiar
//! ~`1/leverage` adverse move. As the strategy **adds** to the position, both
//! the avg entry and the deployed notional change, so the liq price moves each
//! check — recomputed live from the current `(wallet, entry, size, mark)`.
//!
//! (The previous model was *isolated* — `entry × (1 − 1/lev)`, size- and
//! wallet-blind — which spuriously liquidated tiny bags on a `1/lev` move and
//! contradicted the cross-margin position cap used everywhere else.)
//!
//! Linear (USDT/USDC-margined) perp, one-way mode. The trigger uses the
//! runner's current mark (book mid proxy until a real mark series is wired). On
//! trigger the model emits a forced-close [`Fill`] at the liquidation price;
//! the caller applies it (realizing the loss) and cancels resting orders.

use tikr_core::{Decimal, Fill, Notional, Position, Price, Side, Size, Timestamp};
use tikr_venue::QuoteId;

/// Cross-margin liquidation parameters for a linear (USDT/USDC-margined) perp.
#[derive(Debug, Clone, Copy)]
pub struct LiquidationConfig {
    /// Account leverage (e.g. `10` = 10×). Retained for reference and to gate
    /// the model (`<= 0` disables it), but **cross-margin liquidation does not
    /// use leverage in the trigger** — leverage only bounds the max position
    /// (the `wallet × leverage` cap enforced elsewhere). The liq price falls out
    /// of wallet, notional, and the maintenance-margin rate.
    pub leverage: Decimal,
    /// Maintenance-margin rate as a fraction (e.g. `0.005` = 0.5%). Binance
    /// USD-M tier-1 BTC is ~0.4%; small caps run 1–2.5%. Single-symbol, lowest
    /// tier assumed (maintenance amount ≈ 0).
    pub maint_margin_rate: Decimal,
    /// Taker fee (bps) charged on the forced close, approximating the
    /// liquidation-clearance fee + slippage into the liquidation. `0` = none.
    pub close_fee_bps: u32,
}

/// Stateful liquidation checker. Tracks how many times it has fired so the
/// runner can surface the count in its report.
pub struct LiquidationModel {
    cfg: LiquidationConfig,
    count: u64,
}

impl LiquidationModel {
    /// Construct a model from `cfg`.
    pub fn new(cfg: LiquidationConfig) -> Self {
        Self { cfg, count: 0 }
    }

    /// Cross-margin liquidation price for a linear-perp position backed by
    /// `wallet` (realized wallet balance). `None` when flat, the config is
    /// degenerate, or the position can never be liquidated (the wallet is large
    /// enough that the implied liq price is ≤ 0 for a long).
    ///
    /// Derived from `margin balance = maintenance margin`, i.e.
    /// `wallet + (mark − entry)·size = |size|·mark·mmr`, solved for `mark`:
    /// - Long  (`size > 0`): `(entry·size − wallet) / (size·(1 − mmr))`
    /// - Short (`size < 0`): `(wallet + |size|·entry) / (|size|·(1 + mmr))`
    ///
    /// As the position grows (averaging in), both `entry` and `size` change, so
    /// this moves every call — the liq price tracks the live bag + wallet.
    pub fn liq_price(&self, pos: &Position, wallet: Decimal) -> Option<Price> {
        if pos.size.0.is_zero() || self.cfg.leverage <= Decimal::ZERO {
            return None;
        }
        let size = pos.size.0;
        let s = size.abs();
        let entry = pos.avg_entry.0;
        let mmr = self.cfg.maint_margin_rate;
        let px = if size.is_sign_positive() {
            let denom = s * (Decimal::ONE - mmr);
            if denom.is_zero() {
                return None;
            }
            (entry * s - wallet) / denom
        } else {
            let denom = s * (Decimal::ONE + mmr);
            if denom.is_zero() {
                return None;
            }
            (wallet + s * entry) / denom
        };
        // A non-positive long liq price means the wallet fully backs the bag —
        // it can never be liquidated (mark can't fall below 0).
        if px <= Decimal::ZERO {
            return None;
        }
        Some(Price(px.round_dp(8)))
    }

    /// Check whether `mark` breaches the cross-margin liquidation price for
    /// `pos` backed by `wallet`. Returns a forced-close [`Fill`] (opposite side,
    /// full size, priced at the liq price) when triggered; `None` otherwise.
    ///
    /// A long is liquidated when the mark falls **to or below** its liq price;
    /// a short when the mark rises **to or above** it.
    pub fn check(
        &mut self,
        pos: &Position,
        mark: Price,
        wallet: Decimal,
        ts: Timestamp,
    ) -> Option<Fill> {
        let liq = self.liq_price(pos, wallet)?;
        let long = pos.size.0.is_sign_positive();
        let triggered = if long {
            mark.0 <= liq.0
        } else {
            mark.0 >= liq.0
        };
        if !triggered {
            return None;
        }
        self.count += 1;
        let size = pos.size.0.abs();
        // Close side is opposite the position: long closes by selling (Ask),
        // short closes by buying (Bid).
        let close_side = if long { Side::Ask } else { Side::Bid };
        let notional = (liq.0 * size).round_dp(8);
        let fee =
            (notional * Decimal::from(self.cfg.close_fee_bps) / Decimal::from(10_000)).round_dp(8);
        Some(Fill {
            quote_id: QuoteId::new(),
            price: liq,
            size: Size(size),
            fee_asset: pos.symbol.quote.clone(),
            fee_amount: fee,
            fee_quote: Notional(fee),
            side: close_side,
            ts,
            is_full: true,
            trade_id: None,
        })
    }

    /// Number of liquidations fired so far.
    pub fn count(&self) -> u64 {
        self.count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::{Asset, MarketKind, SignedSize, Symbol, VenueId};

    fn sym() -> Symbol {
        Symbol {
            base: Asset::new("BTC"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("test"),
            kind: MarketKind::Perp,
        }
    }

    fn pos(size: i64, entry: i64) -> Position {
        Position {
            symbol: sym(),
            size: SignedSize(Decimal::from(size)),
            avg_entry: Price(Decimal::from(entry)),
            realized_pnl: Notional(Decimal::ZERO),
        }
    }

    fn cfg(lev: i64) -> LiquidationConfig {
        LiquidationConfig {
            leverage: Decimal::from(lev),
            maint_margin_rate: Decimal::ZERO,
            close_fee_bps: 0,
        }
    }

    // Wallet $1000, lev 10 → buying power $10,000. A full bag is 100 units @
    // entry 100 = $10,000 notional; a small bag is 1 unit = $100 notional.

    #[test]
    fn flat_position_never_liquidates() {
        let mut m = LiquidationModel::new(cfg(10));
        let w = Decimal::from(1000);
        assert!(m.liq_price(&pos(0, 100), w).is_none());
        assert!(
            m.check(&pos(0, 100), Price(Decimal::ZERO), w, Timestamp(0))
                .is_none()
        );
    }

    #[test]
    fn full_buying_power_long_liquidates_near_one_over_lev() {
        // notional == wallet × lev → cross liq coincides with the ~1/lev wall.
        let m = LiquidationModel::new(cfg(10));
        // 100 units @ 100 = $10k notional, wallet $1000, mmr 0 →
        // (100·100 − 1000)/(100·1) = 90 = 10% below entry.
        assert_eq!(
            m.liq_price(&pos(100, 100), Decimal::from(1000)).unwrap().0,
            Decimal::from(90)
        );
    }

    #[test]
    fn small_long_backed_by_wallet_never_liquidates() {
        // THE fix: a tiny bag the wallet fully backs can't be liquidated.
        let m = LiquidationModel::new(cfg(10));
        // 1 unit @ 100 = $100 notional, wallet $1000 → (100 − 1000)/1 = −900 ≤ 0.
        assert!(
            m.liq_price(&pos(1, 100), Decimal::from(1000)).is_none(),
            "small bag + big wallet → no liquidation (was the isolated-model bug)"
        );
    }

    #[test]
    fn full_buying_power_short_liquidates_above_entry() {
        let m = LiquidationModel::new(cfg(10));
        // -100 units @ 100, wallet 1000, mmr 0 → (1000 + 100·100)/(100·1) = 110.
        assert_eq!(
            m.liq_price(&pos(-100, 100), Decimal::from(1000)).unwrap().0,
            Decimal::from(110)
        );
    }

    #[test]
    fn liq_price_moves_as_position_grows() {
        // Averaging in: a half bag is far from liquidation; doubling it (same
        // entry) pulls the liq price up toward the 1/lev wall.
        let m = LiquidationModel::new(cfg(10));
        let w = Decimal::from(1000);
        // 50 units @ 100 = $5k: (50·100 − 1000)/50 = 4000/50 = 80 (20% away).
        assert_eq!(m.liq_price(&pos(50, 100), w).unwrap().0, Decimal::from(80));
        // 100 units @ 100 = $10k: 90 (10% away — closer, as the bag grew).
        assert_eq!(m.liq_price(&pos(100, 100), w).unwrap().0, Decimal::from(90));
    }

    #[test]
    fn long_liquidates_when_mark_falls_to_liq() {
        let mut m = LiquidationModel::new(cfg(10));
        let p = pos(100, 100); // full bag, liq = 90 at wallet 1000
        let w = Decimal::from(1000);
        assert!(
            m.check(&p, Price(Decimal::from(91)), w, Timestamp(0))
                .is_none()
        );
        let fill = m
            .check(&p, Price(Decimal::from(90)), w, Timestamp(5))
            .expect("liq");
        assert_eq!(fill.side, Side::Ask, "long closes by selling");
        assert_eq!(fill.price.0, Decimal::from(90));
        assert_eq!(fill.size.0, Decimal::from(100));
        assert!(fill.is_full);
        assert_eq!(m.count(), 1);
    }

    #[test]
    fn short_liquidates_when_mark_rises_to_liq() {
        let mut m = LiquidationModel::new(cfg(10));
        let p = pos(-100, 100); // liq = 110
        let w = Decimal::from(1000);
        assert!(
            m.check(&p, Price(Decimal::from(109)), w, Timestamp(0))
                .is_none()
        );
        let fill = m
            .check(&p, Price(Decimal::from(110)), w, Timestamp(5))
            .expect("liq");
        assert_eq!(fill.side, Side::Bid, "short closes by buying");
        assert_eq!(fill.size.0, Decimal::from(100));
        assert_eq!(m.count(), 1);
    }

    #[test]
    fn maint_margin_pulls_long_liq_up() {
        // mmr 0.5% on a full bag → (10000 − 1000)/(100·0.995) = 90.452… (sooner).
        let m = LiquidationModel::new(LiquidationConfig {
            leverage: Decimal::from(10),
            maint_margin_rate: Decimal::from_str_exact("0.005").unwrap(),
            close_fee_bps: 0,
        });
        let px = m.liq_price(&pos(100, 100), Decimal::from(1000)).unwrap().0;
        assert!(px > Decimal::from(90), "mmr triggers liquidation sooner");
        assert!(px < Decimal::from(91));
    }

    #[test]
    fn close_fee_charged_on_forced_close() {
        let mut m = LiquidationModel::new(LiquidationConfig {
            leverage: Decimal::from(10),
            maint_margin_rate: Decimal::ZERO,
            close_fee_bps: 5,
        });
        // liq 90, size 100 → notional 9000, fee = 9000 × 5/10000 = 4.5.
        let fill = m
            .check(
                &pos(100, 100),
                Price(Decimal::from(90)),
                Decimal::from(1000),
                Timestamp(0),
            )
            .unwrap();
        assert_eq!(fill.fee_amount, Decimal::from_str_exact("4.5").unwrap());
    }

    #[test]
    fn non_positive_leverage_disables() {
        let mut m = LiquidationModel::new(cfg(0));
        let w = Decimal::from(1000);
        assert!(m.liq_price(&pos(1, 100), w).is_none());
        assert!(
            m.check(&pos(1, 100), Price(Decimal::from(1)), w, Timestamp(0))
                .is_none()
        );
    }
}
