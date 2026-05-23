//! Take-profit / stop-loss policy evaluated every event.
//!
//! Pre-refactor, TP was bolted into `emit_requote` — only checked on
//! the requote path, only triggered above an absolute USDT threshold,
//! and there was no stop-loss path at all. A favourable spike between
//! requote intervals was missed; an adverse spike could grind the
//! position indefinitely.
//!
//! Stage 5 fixes:
//!
//! - **bps-of-notional triggers**: position size cancels out, same
//!   threshold works at $10 and $10k notional
//! - **checked every event** (BookUpdate, Heartbeat, Fill): can fire
//!   between requote intervals
//! - **stop-loss path**: bounded downside, IOC-flatten at opposing
//!   touch, same shape as TP

use tikr_core::{Decimal, Position, Price, QuoteKind, Side, Size, Symbol, TimeInForce};
use tikr_venue::QuoteIntent;

use crate::Action;

/// What the risk policy decided this tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RiskDecision {
    /// Position outside TP/SL bounds OR flat — do nothing.
    Hold,
    /// Close the position via IOC at the opposing touch.
    /// Caller wraps into `Action::Quote` with the supplied [`Side`]
    /// + IOC TIF.
    Close {
        /// Reducing side (long → Ask, short → Bid).
        side: Side,
        /// Absolute quantity to close (== |position size|).
        qty: Size,
        /// Reason tag for logging — Stage 6+ will pipe this into the
        /// telemetry layer.
        reason: CloseReason,
    },
}

/// Why a Close fired. Lets the strategy log / count separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReason {
    /// Unrealized PnL exceeded the take-profit threshold.
    TakeProfit,
    /// Unrealized PnL fell below the stop-loss threshold.
    StopLoss,
}

/// Configuration for the risk policy.
#[derive(Debug, Clone, Copy)]
pub struct RiskConfig {
    /// Take-profit threshold in bps of position notional (entry × qty).
    /// `0` disables — falls back to the legacy `take_profit_usdt` knob
    /// on the caller side.
    pub take_profit_bps: u32,
    /// Stop-loss threshold in bps of position notional (entry × qty).
    /// `0` disables.
    pub stop_loss_bps: u32,
    /// Legacy absolute-USDT take-profit; honoured only when
    /// `take_profit_bps == 0`. Lets existing live configs keep firing
    /// while users migrate to the bps knob.
    pub take_profit_usdt_legacy: Decimal,
}

/// Evaluate the risk policy against `position` at the current `mid`.
///
/// Stateless — every input is passed explicitly. Caller drives this
/// on every event (BookUpdate / Heartbeat / Fill) so a favourable
/// spike between requote intervals can't be missed.
pub fn evaluate(position: &Position, mid: Price, cfg: RiskConfig) -> RiskDecision {
    if position.size.0 == Decimal::ZERO || position.avg_entry.0 <= Decimal::ZERO {
        return RiskDecision::Hold;
    }
    let long = position.size.0 > Decimal::ZERO;
    let pos_abs = position.size.0.abs();
    // Signed bps of mid drift from entry, viewed from a LONG position.
    // Short position negates the sign so the same threshold semantics
    // apply.
    let drift = mid.0 - position.avg_entry.0;
    let signed_drift = if long { drift } else { -drift };
    let drift_bps = signed_drift / position.avg_entry.0 * Decimal::from(10_000);
    // Take-profit (positive drift = profit).
    if cfg.take_profit_bps > 0 {
        let tp_bps = Decimal::from(cfg.take_profit_bps);
        if drift_bps >= tp_bps {
            return RiskDecision::Close {
                side: if long { Side::Ask } else { Side::Bid },
                qty: Size(pos_abs),
                reason: CloseReason::TakeProfit,
            };
        }
    } else if cfg.take_profit_usdt_legacy > Decimal::ZERO {
        // Legacy path: PnL in absolute USDT. profit = signed_drift × qty.
        let unrealized = signed_drift * pos_abs;
        if unrealized >= cfg.take_profit_usdt_legacy {
            return RiskDecision::Close {
                side: if long { Side::Ask } else { Side::Bid },
                qty: Size(pos_abs),
                reason: CloseReason::TakeProfit,
            };
        }
    }
    // Stop-loss (negative drift past −sl_bps).
    if cfg.stop_loss_bps > 0 {
        let sl_bps = Decimal::from(cfg.stop_loss_bps);
        if -drift_bps >= sl_bps {
            return RiskDecision::Close {
                side: if long { Side::Ask } else { Side::Bid },
                qty: Size(pos_abs),
                reason: CloseReason::StopLoss,
            };
        }
    }
    RiskDecision::Hold
}

/// Build the IOC close action for the given symbol + top-of-book.
/// Reducing side fires at the OPPOSING touch so the order crosses
/// the book and fills as taker.
pub fn build_close(
    symbol: &Symbol,
    side: Side,
    qty: Size,
    best_bid: Price,
    best_ask: Price,
) -> Action {
    let price = match side {
        Side::Bid => best_ask,
        Side::Ask => best_bid,
    };
    Action::Quote(QuoteIntent {
        symbol: symbol.clone(),
        side,
        price,
        size: qty,
        tif: TimeInForce::IOC,
        kind: QuoteKind::Point,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::{Asset, MarketKind, Notional, Position, SignedSize, Symbol, VenueId};

    fn sym() -> Symbol {
        Symbol {
            base: Asset::new("BTC"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("test"),
            kind: MarketKind::Perp,
        }
    }

    fn pos(size: Decimal, entry: i64) -> Position {
        Position {
            symbol: sym(),
            size: SignedSize(size),
            avg_entry: Price(Decimal::from(entry)),
            realized_pnl: Notional(Decimal::ZERO),
        }
    }

    fn cfg(tp_bps: u32, sl_bps: u32) -> RiskConfig {
        RiskConfig {
            take_profit_bps: tp_bps,
            stop_loss_bps: sl_bps,
            take_profit_usdt_legacy: Decimal::ZERO,
        }
    }

    #[test]
    fn flat_holds() {
        let p = pos(Decimal::ZERO, 100);
        let d = evaluate(&p, Price(Decimal::from(110)), cfg(10, 10));
        assert_eq!(d, RiskDecision::Hold);
    }

    #[test]
    fn long_take_profit_fires_at_threshold() {
        // 100 → 110 = +1000 bps. tp_bps=500 → fires.
        let p = pos(Decimal::ONE, 100);
        let d = evaluate(&p, Price(Decimal::from(110)), cfg(500, 0));
        assert!(matches!(
            d,
            RiskDecision::Close {
                side: Side::Ask,
                reason: CloseReason::TakeProfit,
                ..
            }
        ));
    }

    #[test]
    fn short_take_profit_fires_at_threshold() {
        // Short at 110, mid 100 = +909 bps for short (110 - 100)/110.
        let p = pos(-Decimal::ONE, 110);
        let d = evaluate(&p, Price(Decimal::from(100)), cfg(500, 0));
        assert!(matches!(
            d,
            RiskDecision::Close {
                side: Side::Bid,
                reason: CloseReason::TakeProfit,
                ..
            }
        ));
    }

    #[test]
    fn long_stop_loss_fires_on_adverse_move() {
        // Long at 100, mid 90 = -1000 bps. sl_bps=500 → fires.
        let p = pos(Decimal::ONE, 100);
        let d = evaluate(&p, Price(Decimal::from(90)), cfg(0, 500));
        assert!(matches!(
            d,
            RiskDecision::Close {
                side: Side::Ask,
                reason: CloseReason::StopLoss,
                ..
            }
        ));
    }

    #[test]
    fn short_stop_loss_fires_on_adverse_move() {
        // Short at 100, mid 110 = -1000 bps for short. sl_bps=500 → fires.
        let p = pos(-Decimal::ONE, 100);
        let d = evaluate(&p, Price(Decimal::from(110)), cfg(0, 500));
        assert!(matches!(
            d,
            RiskDecision::Close {
                side: Side::Bid,
                reason: CloseReason::StopLoss,
                ..
            }
        ));
    }

    #[test]
    fn legacy_usdt_path_when_bps_disabled() {
        // pos size=1, entry=100, mid=105 → unrealized = 5. legacy = 4 → fires.
        let p = pos(Decimal::ONE, 100);
        let cfg = RiskConfig {
            take_profit_bps: 0,
            stop_loss_bps: 0,
            take_profit_usdt_legacy: Decimal::from(4),
        };
        let d = evaluate(&p, Price(Decimal::from(105)), cfg);
        assert!(matches!(
            d,
            RiskDecision::Close {
                reason: CloseReason::TakeProfit,
                ..
            }
        ));
    }

    #[test]
    fn build_close_fires_ioc_at_opposing_touch() {
        let action = build_close(
            &sym(),
            Side::Ask,
            Size(Decimal::ONE),
            Price(Decimal::from(100)),
            Price(Decimal::from(110)),
        );
        let Action::Quote(intent) = action else {
            panic!("expected Quote");
        };
        // Reducing-long via Ask → cross into the bid at 100.
        assert_eq!(intent.price.0, Decimal::from(100));
        assert_eq!(intent.tif, TimeInForce::IOC);
    }
}
