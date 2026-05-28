//! Isolated-margin forced-liquidation model for backtest / paper runs.
//!
//! Real leveraged perp accounts get liquidated when the mark price moves far
//! enough against an open position that account equity falls to the
//! maintenance-margin requirement. Without this, a backtest lets an
//! inventory-heavy strategy ride a trend through a drawdown that a real
//! account would have been force-closed out of — inflating the apparent
//! survivability of trend-adverse strategies.
//!
//! This models **isolated** margin on a **linear** (USDT-margined) perp,
//! one-way mode. The trigger uses the runner's current mark (book mid as a
//! proxy until a real mark/index series is wired). On trigger the model emits
//! a forced-close [`Fill`] at the liquidation price; the caller applies it to
//! the position tracker (realizing the loss) and cancels resting orders,
//! mirroring venue behaviour.

use tikr_core::{Decimal, Fill, Notional, Position, Price, Side, Size, Timestamp};
use tikr_venue::QuoteId;

/// Isolated-margin liquidation parameters for a linear (USDT-margined) perp.
#[derive(Debug, Clone, Copy)]
pub struct LiquidationConfig {
    /// Position leverage (e.g. `10` = 10×). Must be `> 0`; a non-positive
    /// value disables the model (no position is ever liquidated).
    pub leverage: Decimal,
    /// Maintenance-margin rate as a fraction (e.g. `0.005` = 0.5%). Binance
    /// USD-M tier-1 BTC is ~0.4%; small caps run 1%+.
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

    /// Liquidation price for an isolated linear-perp position. `None` when the
    /// position is flat or the config is degenerate (`leverage <= 0`).
    ///
    /// Isolated-margin approximation (fees folded into `close_fee_bps`, not
    /// the trigger):
    /// - Long:  `entry × (1 − 1/lev + mmr)`
    /// - Short: `entry × (1 + 1/lev − mmr)`
    pub fn liq_price(&self, pos: &Position) -> Option<Price> {
        if pos.size.0.is_zero() || self.cfg.leverage <= Decimal::ZERO {
            return None;
        }
        let inv_lev = Decimal::ONE / self.cfg.leverage;
        let entry = pos.avg_entry.0;
        let px = if pos.size.0.is_sign_positive() {
            entry * (Decimal::ONE - inv_lev + self.cfg.maint_margin_rate)
        } else {
            entry * (Decimal::ONE + inv_lev - self.cfg.maint_margin_rate)
        };
        Some(Price(px.round_dp(8)))
    }

    /// Check whether `mark` has breached the liquidation price for `pos`.
    /// Returns a forced-close [`Fill`] (opposite side, full size, priced at
    /// the liquidation price) when triggered; `None` otherwise.
    ///
    /// A long is liquidated when the mark falls **to or below** its liq price;
    /// a short when the mark rises **to or above** it.
    pub fn check(&mut self, pos: &Position, mark: Price, ts: Timestamp) -> Option<Fill> {
        let liq = self.liq_price(pos)?;
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

    #[test]
    fn flat_position_never_liquidates() {
        let mut m = LiquidationModel::new(cfg(10));
        assert!(m.liq_price(&pos(0, 100)).is_none());
        assert!(
            m.check(&pos(0, 100), Price(Decimal::ZERO), Timestamp(0))
                .is_none()
        );
    }

    #[test]
    fn long_liq_price_is_below_entry() {
        let m = LiquidationModel::new(cfg(10));
        // 10× long at 100, mmr=0 → liq at 100 × (1 − 0.1) = 90.
        assert_eq!(m.liq_price(&pos(1, 100)).unwrap().0, Decimal::from(90));
    }

    #[test]
    fn short_liq_price_is_above_entry() {
        let m = LiquidationModel::new(cfg(10));
        // 10× short at 100, mmr=0 → liq at 100 × (1 + 0.1) = 110.
        assert_eq!(m.liq_price(&pos(-1, 100)).unwrap().0, Decimal::from(110));
    }

    #[test]
    fn long_liquidates_when_mark_falls_to_liq() {
        let mut m = LiquidationModel::new(cfg(10));
        let p = pos(2, 100); // liq = 90
        assert!(
            m.check(&p, Price(Decimal::from(91)), Timestamp(0))
                .is_none()
        );
        let fill = m
            .check(&p, Price(Decimal::from(90)), Timestamp(5))
            .expect("liq");
        assert_eq!(fill.side, Side::Ask, "long closes by selling");
        assert_eq!(fill.price.0, Decimal::from(90));
        assert_eq!(fill.size.0, Decimal::from(2));
        assert!(fill.is_full);
        assert_eq!(m.count(), 1);
    }

    #[test]
    fn short_liquidates_when_mark_rises_to_liq() {
        let mut m = LiquidationModel::new(cfg(10));
        let p = pos(-3, 100); // liq = 110
        assert!(
            m.check(&p, Price(Decimal::from(109)), Timestamp(0))
                .is_none()
        );
        let fill = m
            .check(&p, Price(Decimal::from(110)), Timestamp(5))
            .expect("liq");
        assert_eq!(fill.side, Side::Bid, "short closes by buying");
        assert_eq!(fill.size.0, Decimal::from(3));
        assert_eq!(m.count(), 1);
    }

    #[test]
    fn maint_margin_widens_long_liq_toward_entry() {
        // mmr 0.5% → long liq = 100 × (1 − 0.1 + 0.005) = 90.5 (triggers sooner).
        let m = LiquidationModel::new(LiquidationConfig {
            leverage: Decimal::from(10),
            maint_margin_rate: Decimal::from_str_exact("0.005").unwrap(),
            close_fee_bps: 0,
        });
        assert_eq!(
            m.liq_price(&pos(1, 100)).unwrap().0,
            Decimal::from_str_exact("90.5").unwrap()
        );
    }

    #[test]
    fn close_fee_charged_on_forced_close() {
        let mut m = LiquidationModel::new(LiquidationConfig {
            leverage: Decimal::from(10),
            maint_margin_rate: Decimal::ZERO,
            close_fee_bps: 5,
        });
        // liq 90, size 2 → notional 180, fee = 180 × 5/10000 = 0.09.
        let fill = m
            .check(&pos(2, 100), Price(Decimal::from(90)), Timestamp(0))
            .unwrap();
        assert_eq!(fill.fee_amount, Decimal::from_str_exact("0.09").unwrap());
    }

    #[test]
    fn non_positive_leverage_disables() {
        let mut m = LiquidationModel::new(cfg(0));
        assert!(m.liq_price(&pos(1, 100)).is_none());
        assert!(
            m.check(&pos(1, 100), Price(Decimal::from(1)), Timestamp(0))
                .is_none()
        );
    }
}
