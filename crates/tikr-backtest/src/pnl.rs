//! Position + P&L accounting — WACC cost basis, separate fee tracking.
//! See [issue #12] for cost-basis rationale.
//!
//! [issue #12]: https://github.com/kryptic-sh/tikr/issues/12

use tikr_core::{Decimal, Fill, Notional, Position, Price, Side, SignedSize, Size, Symbol};

/// Sign of a non-zero `Decimal` as `+1` or `-1`. Zero returns `0`.
fn sign(d: Decimal) -> Decimal {
    if d.is_zero() {
        Decimal::ZERO
    } else if d.is_sign_negative() {
        -Decimal::ONE
    } else {
        Decimal::ONE
    }
}

/// Cap stored-state precision to 8dp so subsequent multiplications stay
/// inside rust_decimal's 96-bit mantissa. Without this, WACC division
/// produces a 28dp avg_entry whose next price-multiply overflows on
/// small-tick assets (DOGE/HYPER) under runaway TP/SL loops. Venue tick
/// is at most 8dp in practice, so 8dp is lossless for any real price.
/// fill_sim already uses the same 8dp ceiling.
const STATE_DP: u32 = 8;

/// Running position + P&L state for one symbol. WACC cost basis.
pub struct PositionTracker {
    symbol: Symbol,
    size: SignedSize,
    avg_entry: Price,
    realized_pnl: Notional,
    fees_paid: Notional,
    /// Phase 1: always zero. Phase 2 wires real funding.
    funding_accrued: Notional,
}

/// Translate a fill `side` + unsigned `size` into a signed position delta.
///
/// `Bid` is our buy-side quote (MM perspective): a fill adds to long.
/// `Ask` is our sell-side quote: a fill adds to short.
fn side_delta(side: Side, size: Size) -> Decimal {
    match side {
        Side::Bid => size.0,
        Side::Ask => -size.0,
    }
}

impl PositionTracker {
    /// Construct a new flat tracker for `symbol`.
    pub fn new(symbol: Symbol) -> Self {
        Self {
            symbol,
            size: SignedSize(Decimal::ZERO),
            avg_entry: Price(Decimal::ZERO),
            realized_pnl: Notional(Decimal::ZERO),
            fees_paid: Notional(Decimal::ZERO),
            funding_accrued: Notional(Decimal::ZERO),
        }
    }

    /// Reconstruct a tracker from persisted state. Resume path (#32).
    ///
    /// Used by [`tikr_paper::runner::run_with_resume`] to seed tracker state
    /// from a prior `PaperReport`. Caller is responsible for ensuring the
    /// snapshot fields are self-consistent (e.g. came from a previous
    /// `tracker.snapshot()` plus the matching `report()` aggregates).
    pub fn from_snapshot(
        symbol: Symbol,
        position: Position,
        realized: Notional,
        fees: Notional,
        funding: Notional,
    ) -> Self {
        Self {
            symbol,
            size: position.size,
            avg_entry: position.avg_entry,
            realized_pnl: realized,
            fees_paid: fees,
            funding_accrued: funding,
        }
    }

    /// Apply a perp funding payment. `amount` is signed in quote currency:
    /// positive means we received funding (short with positive rate, or long
    /// with negative rate), negative means we paid. Caller computes the
    /// amount as `position × mark_price × funding_rate × direction_sign`.
    pub fn accrue_funding(&mut self, amount: Decimal) {
        self.funding_accrued = Notional(self.funding_accrued.0 + amount);
    }

    /// Apply `fill` to the running state. WACC math, fees accumulated separately.
    pub fn apply(&mut self, fill: &Fill) {
        // Bound input precision so downstream products stay inside
        // rust_decimal's 96-bit mantissa. Venue tick + lot precision
        // are well under STATE_DP — no information lost.
        let delta = side_delta(fill.side, fill.size).round_dp(STATE_DP);

        // Always accrue fees, regardless of which state-update case fires.
        // `fee_quote` is signed: positive = paid, negative = rebate.
        self.fees_paid =
            Notional((self.fees_paid.0 + fill.fee_quote.0.round_dp(STATE_DP)).round_dp(STATE_DP));

        // Defensive: a zero-size fill shouldn't happen but if it does, no state update.
        if delta == Decimal::ZERO {
            return;
        }

        let cur = self.size.0.round_dp(STATE_DP);
        let entry = self.avg_entry.0.round_dp(STATE_DP);
        let fp = fill.price.0.round_dp(STATE_DP);
        let new_size = (cur + delta).round_dp(STATE_DP);

        if cur == Decimal::ZERO {
            // Case A: opening from flat.
            self.size = SignedSize(new_size);
            self.avg_entry = Price(fp);
            return;
        }

        let cur_sign = sign(cur);
        let delta_sign = sign(delta);

        if cur_sign == delta_sign {
            // Case B: same-direction add — WACC the entry price.
            let new_entry =
                ((cur.abs() * entry + delta.abs() * fp) / new_size.abs()).round_dp(STATE_DP);
            self.size = SignedSize(new_size);
            self.avg_entry = Price(new_entry);
            return;
        }

        // Opposite direction: either reducing (Case C) or flipping (Case D).
        let new_sign = sign(new_size);

        if new_size == Decimal::ZERO || new_sign == cur_sign {
            // Case C: reducing within the same side (or down to flat).
            let closed = delta.abs();
            let realized_delta = (closed * (fp - entry) * cur_sign).round_dp(STATE_DP);
            self.realized_pnl = Notional((self.realized_pnl.0 + realized_delta).round_dp(STATE_DP));
            self.size = SignedSize(new_size);
            if new_size == Decimal::ZERO {
                self.avg_entry = Price(Decimal::ZERO);
            }
            // else: avg_entry unchanged
        } else {
            // Case D: flipping past zero. Close existing fully, then open leftover.
            let closed = cur.abs();
            let realized_delta = (closed * (fp - entry) * cur_sign).round_dp(STATE_DP);
            self.realized_pnl = Notional((self.realized_pnl.0 + realized_delta).round_dp(STATE_DP));
            self.size = SignedSize(new_size);
            self.avg_entry = Price(fp);
        }
    }

    /// Realized P&L accumulator (gross, before fees).
    pub fn realized(&self) -> Notional {
        self.realized_pnl
    }

    /// Force-reconcile tracker to a known authoritative position
    /// (e.g. from a venue `position_risk` REST poll when WS fills
    /// were lost). Overwrites `size` + `avg_entry` directly;
    /// `realized_pnl` and `fees_paid` are LEFT AS-IS — caller is
    /// responsible for understanding that lost-fill PnL is now
    /// invisible to the tracker. The strategy's next event will
    /// see the corrected position via `ctx.position`.
    ///
    /// Returns the delta `(old_size, old_avg)` for logging.
    pub fn force_reconcile(&mut self, new_size: SignedSize, new_avg: Price) -> (SignedSize, Price) {
        let prev = (self.size, self.avg_entry);
        self.size = new_size;
        self.avg_entry = new_avg;
        prev
    }

    /// Total fees paid (positive) or rebated (negative).
    pub fn fees(&self) -> Notional {
        self.fees_paid
    }

    /// Current position snapshot (immutable view).
    pub fn snapshot(&self) -> Position {
        Position {
            symbol: self.symbol.clone(),
            size: self.size,
            avg_entry: self.avg_entry,
            realized_pnl: self.realized_pnl,
        }
    }

    /// Aggregate report at the given mark price.
    pub fn report(&self, last_mid: Price) -> PnLReport {
        let unrealized = ((last_mid.0 - self.avg_entry.0) * self.size.0).round_dp(STATE_DP);
        let net = self.realized_pnl.0 + unrealized - self.fees_paid.0 + self.funding_accrued.0;
        PnLReport {
            realized: self.realized_pnl,
            unrealized: Notional(unrealized),
            fees: self.fees_paid,
            funding: self.funding_accrued,
            net: Notional(net),
        }
    }
}

/// Aggregate P&L report at a point in time.
#[derive(Debug, Clone, Copy)]
pub struct PnLReport {
    /// Realized P&L (gross, before fees).
    pub realized: Notional,
    /// Unrealized P&L marked at the report price.
    pub unrealized: Notional,
    /// Total fees paid (positive) or rebated (negative).
    pub fees: Notional,
    /// Funding accrued. v0: always zero; Phase 2 wires real funding.
    pub funding: Notional,
    /// `realized + unrealized - fees + funding`.
    pub net: Notional,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::{Asset, Decimal, MarketKind, Side, Symbol, Timestamp, VenueId};
    use tikr_venue::QuoteId;

    fn make_symbol() -> Symbol {
        Symbol {
            base: Asset::new("BTC"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("test"),
            kind: MarketKind::Spot,
        }
    }

    fn fill(_symbol: &Symbol, side: Side, price: i64, size_units: i64, fee_quote: i64) -> Fill {
        Fill {
            quote_id: QuoteId::new(),
            price: Price(Decimal::from(price)),
            size: Size(Decimal::from(size_units)),
            fee_asset: Asset::new("USDT"),
            fee_amount: Decimal::ZERO,
            fee_quote: Notional(Decimal::from(fee_quote)),
            side,
            ts: Timestamp(0),
            is_full: true,
            trade_id: None,
        }
    }

    #[test]
    fn flat_open_long_sets_avg_entry() {
        let sym = make_symbol();
        let mut t = PositionTracker::new(sym.clone());
        t.apply(&fill(&sym, Side::Bid, 100, 1, 0));
        let snap = t.snapshot();
        assert_eq!(snap.size.0, Decimal::from(1));
        assert_eq!(snap.avg_entry.0, Decimal::from(100));
        assert_eq!(snap.realized_pnl.0, Decimal::ZERO);
    }

    #[test]
    fn wacc_same_side_adds() {
        let sym = make_symbol();
        let mut t = PositionTracker::new(sym.clone());
        t.apply(&fill(&sym, Side::Bid, 100, 1, 0));
        t.apply(&fill(&sym, Side::Bid, 110, 1, 0));
        let snap = t.snapshot();
        assert_eq!(snap.size.0, Decimal::from(2));
        assert_eq!(snap.avg_entry.0, Decimal::from(105));
        assert_eq!(snap.realized_pnl.0, Decimal::ZERO);
    }

    #[test]
    fn reduce_long_realizes_profit() {
        let sym = make_symbol();
        let mut t = PositionTracker::new(sym.clone());
        t.apply(&fill(&sym, Side::Bid, 100, 2, 0));
        t.apply(&fill(&sym, Side::Ask, 110, 1, 0));
        let snap = t.snapshot();
        assert_eq!(snap.size.0, Decimal::from(1));
        assert_eq!(snap.avg_entry.0, Decimal::from(100));
        assert_eq!(snap.realized_pnl.0, Decimal::from(10));
    }

    #[test]
    fn flip_long_to_short_realizes_full_close_then_opens() {
        let sym = make_symbol();
        let mut t = PositionTracker::new(sym.clone());
        t.apply(&fill(&sym, Side::Bid, 100, 2, 0));
        t.apply(&fill(&sym, Side::Ask, 110, 5, 0));
        let snap = t.snapshot();
        assert_eq!(snap.size.0, Decimal::from(-3));
        assert_eq!(snap.avg_entry.0, Decimal::from(110));
        // close 2 @ profit 10 = 20 realized
        assert_eq!(snap.realized_pnl.0, Decimal::from(20));
    }

    #[test]
    fn maker_rebate_credits_net() {
        let sym = make_symbol();
        let mut t = PositionTracker::new(sym.clone());
        t.apply(&fill(&sym, Side::Bid, 100, 1, -2));
        let r = t.report(Price(Decimal::from(100)));
        assert_eq!(r.unrealized.0, Decimal::ZERO);
        assert_eq!(r.realized.0, Decimal::ZERO);
        assert_eq!(r.fees.0, Decimal::from(-2));
        assert_eq!(r.funding.0, Decimal::ZERO);
        // net = 0 + 0 - (-2) + 0 = 2
        assert_eq!(r.net.0, Decimal::from(2));
    }

    #[test]
    fn unrealized_marks_at_mid() {
        let sym = make_symbol();
        let mut t = PositionTracker::new(sym.clone());
        t.apply(&fill(&sym, Side::Bid, 100, 2, 0));
        let r = t.report(Price(Decimal::from(105)));
        assert_eq!(r.unrealized.0, Decimal::from(10));
        assert_eq!(r.realized.0, Decimal::ZERO);
        assert_eq!(r.fees.0, Decimal::ZERO);
        assert_eq!(r.funding.0, Decimal::ZERO);
        assert_eq!(r.net.0, Decimal::from(10));
    }

    #[test]
    fn fully_flat_resets_avg_entry() {
        let sym = make_symbol();
        let mut t = PositionTracker::new(sym.clone());
        t.apply(&fill(&sym, Side::Bid, 100, 1, 0));
        t.apply(&fill(&sym, Side::Ask, 110, 1, 0));
        let snap = t.snapshot();
        assert_eq!(snap.size.0, Decimal::ZERO);
        assert_eq!(snap.avg_entry.0, Decimal::ZERO);
        assert_eq!(snap.realized_pnl.0, Decimal::from(10));
    }

    #[test]
    fn from_snapshot_round_trips() {
        let symbol = make_symbol();
        let pos = Position {
            symbol: symbol.clone(),
            size: SignedSize(Decimal::from(5)),
            avg_entry: Price(Decimal::from(100)),
            realized_pnl: Notional(Decimal::from(20)),
        };
        let tracker = PositionTracker::from_snapshot(
            symbol.clone(),
            pos.clone(),
            Notional(Decimal::from(50)),
            Notional(Decimal::from(3)),
            Notional(Decimal::ZERO),
        );
        let report = tracker.report(Price(Decimal::from(110)));
        assert_eq!(report.realized.0, Decimal::from(50));
        // (110 - 100) * 5 = 50
        assert_eq!(report.unrealized.0, Decimal::from(50));
        assert_eq!(report.fees.0, Decimal::from(3));
    }

    #[test]
    fn round_trip_three_fills_hand_computed() {
        let sym = make_symbol();
        let mut t = PositionTracker::new(sym.clone());
        t.apply(&fill(&sym, Side::Bid, 100, 2, 1));
        t.apply(&fill(&sym, Side::Bid, 120, 1, 1));
        t.apply(&fill(&sym, Side::Ask, 130, 2, 1));

        // Match `STATE_DP`-rounded math: avg_entry persists at 8dp so the
        // hand-computed expectations downstream must round at each step too.
        let expected_avg = ((Decimal::from(2) * Decimal::from(100) + Decimal::from(120))
            / Decimal::from(3))
        .round_dp(8);
        let expected_realized =
            (Decimal::from(2) * (Decimal::from(130) - expected_avg)).round_dp(8);
        let expected_unrealized =
            ((Decimal::from(125) - expected_avg) * Decimal::from(1)).round_dp(8);
        let expected_net = expected_realized + expected_unrealized - Decimal::from(3);

        let snap = t.snapshot();
        assert_eq!(snap.size.0, Decimal::from(1));
        assert_eq!(snap.avg_entry.0, expected_avg);
        assert_eq!(snap.realized_pnl.0, expected_realized);

        let r = t.report(Price(Decimal::from(125)));
        assert_eq!(r.realized.0, expected_realized);
        assert_eq!(r.unrealized.0, expected_unrealized);
        assert_eq!(r.fees.0, Decimal::from(3));
        assert_eq!(r.funding.0, Decimal::ZERO);
        assert_eq!(r.net.0, expected_net);
    }
}
