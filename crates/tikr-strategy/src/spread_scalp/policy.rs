//! Diff resting quotes against target prices → minimum-action sequence.
//!
//! The pre-refactor `emit_requote` always emitted `CancelAll` + place
//! both sides — every requote, regardless of whether anything actually
//! moved. That paid cancel + place fees on the unchanged side and
//! amplified hot-loop risk during fast-move reject storms.
//!
//! `diff` compares one side's resting quote (from [`RestingOrders`])
//! against a fresh `(price, size)` target and returns the smallest set
//! of [`Action`]s that closes the gap:
//!
//! - No resting + want to place → `[Quote]`
//! - Resting matches target (price + size within tolerance) → `[]`
//! - Resting at wrong price/size → `[Cancel(id), Quote]`
//!   (when the runner has stamped a venue id), otherwise `[Quote]`
//!   (in-flight; let the runner reconcile via FillSim ghost cleanup)
//!
//! Future: when we add a venue with native order amend (Bybit, dYdX),
//! emit `Action::Requote { id, intent }` instead of `Cancel + Quote`
//! for in-place updates.

use tikr_core::{Decimal, Price, Size};
use tikr_venue::{QuoteId, QuoteIntent};

use super::resting_orders::Resting;
use crate::Action;

/// Outcome of a per-side diff.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffDecision {
    /// Resting matches target; emit nothing.
    Unchanged,
    /// Nothing resting; emit a fresh place.
    Place,
    /// Resting differs; cancel the old id and emit a fresh place.
    /// Caller wraps `Action::Cancel(id)` + `Action::Quote(intent)`.
    Replace(QuoteId),
    /// Resting differs but has no venue id yet (still in-flight). Emit
    /// only the new place — the in-flight intent will either land OR
    /// the runner's reconciliation tick will drop it.
    PlaceShadow,
}

/// Compute the per-side diff decision.
///
/// `price_tolerance_ticks` lets callers absorb a small Decimal jitter
/// without churning a requote (e.g. microprice oscillating ±0.5 tick
/// around best_bid + tick). Pass `Decimal::ZERO` for exact match.
pub fn diff(
    resting: Option<&Resting>,
    target_price: Price,
    target_size: Size,
    price_tolerance_ticks: Decimal,
    tick_size: Decimal,
) -> DiffDecision {
    match resting {
        None => DiffDecision::Place,
        Some(r) => {
            let tol = price_tolerance_ticks * tick_size;
            let price_diff = (r.price.0 - target_price.0).abs();
            // Size match — tighter: byte-equal, since sizes come from
            // the same lot-step-rounded path.
            let size_match = r.size.0 == target_size.0;
            if price_diff <= tol && size_match {
                DiffDecision::Unchanged
            } else {
                match r.id {
                    Some(id) => DiffDecision::Replace(id),
                    None => DiffDecision::PlaceShadow,
                }
            }
        }
    }
}

/// Helper: turn a [`DiffDecision`] into the concrete action list for a
/// given intent. Returns empty `Vec` for [`DiffDecision::Unchanged`].
pub fn apply(decision: DiffDecision, intent: QuoteIntent) -> Vec<Action> {
    match decision {
        DiffDecision::Unchanged => Vec::new(),
        DiffDecision::Place | DiffDecision::PlaceShadow => vec![Action::Quote(intent)],
        DiffDecision::Replace(id) => vec![Action::Cancel(id), Action::Quote(intent)],
    }
}

/// Convenience: drop a tracked side that isn't desired any more.
/// Returns `[Cancel(id)]` when there's a venue id to cancel,
/// otherwise empty (in-flight quotes can't be cancelled by id).
pub fn drop_side(resting: Option<&Resting>) -> Vec<Action> {
    match resting.and_then(|r| r.id) {
        Some(id) => vec![Action::Cancel(id)],
        None => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::{Asset, MarketKind, QuoteKind, Side, Symbol, TimeInForce, VenueId};

    fn resting(price: i64, size: i64, id: Option<QuoteId>) -> Resting {
        Resting {
            id,
            side: Side::Bid,
            price: Price(Decimal::from(price)),
            size: Size(Decimal::from(size)),
        }
    }

    fn intent(price: i64, size: i64) -> QuoteIntent {
        QuoteIntent {
            symbol: Symbol {
                base: Asset::new("BTC"),
                quote: Asset::new("USDT"),
                venue: VenueId::new("test"),
                kind: MarketKind::Perp,
            },
            side: Side::Bid,
            price: Price(Decimal::from(price)),
            size: Size(Decimal::from(size)),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        }
    }

    #[test]
    fn nothing_resting_returns_place() {
        let d = diff(
            None,
            Price(Decimal::from(100)),
            Size(Decimal::from(1)),
            Decimal::ZERO,
            Decimal::from(1),
        );
        assert_eq!(d, DiffDecision::Place);
        assert_eq!(apply(d, intent(100, 1)).len(), 1);
    }

    #[test]
    fn exact_match_returns_unchanged() {
        let id = QuoteId::new();
        let r = resting(100, 1, Some(id));
        let d = diff(
            Some(&r),
            Price(Decimal::from(100)),
            Size(Decimal::from(1)),
            Decimal::ZERO,
            Decimal::from(1),
        );
        assert_eq!(d, DiffDecision::Unchanged);
        assert!(apply(d, intent(100, 1)).is_empty());
    }

    #[test]
    fn price_diff_within_tolerance_unchanged() {
        let id = QuoteId::new();
        let r = resting(100, 1, Some(id));
        // Target 101, tolerance 1 tick — within band.
        let d = diff(
            Some(&r),
            Price(Decimal::from(101)),
            Size(Decimal::from(1)),
            Decimal::ONE,
            Decimal::from(1),
        );
        assert_eq!(d, DiffDecision::Unchanged);
    }

    #[test]
    fn price_diff_beyond_tolerance_replaces() {
        let id = QuoteId::new();
        let r = resting(100, 1, Some(id));
        let d = diff(
            Some(&r),
            Price(Decimal::from(105)),
            Size(Decimal::from(1)),
            Decimal::ONE,
            Decimal::from(1),
        );
        assert_eq!(d, DiffDecision::Replace(id));
        let actions = apply(d, intent(105, 1));
        assert_eq!(actions.len(), 2);
        assert!(matches!(actions[0], Action::Cancel(_)));
        assert!(matches!(actions[1], Action::Quote(_)));
    }

    #[test]
    fn shadow_path_when_no_venue_id_yet() {
        let r = resting(100, 1, None);
        let d = diff(
            Some(&r),
            Price(Decimal::from(105)),
            Size(Decimal::from(1)),
            Decimal::ZERO,
            Decimal::from(1),
        );
        assert_eq!(d, DiffDecision::PlaceShadow);
        // Just one action — let the in-flight intent get reconciled.
        assert_eq!(apply(d, intent(105, 1)).len(), 1);
    }

    #[test]
    fn drop_side_emits_cancel_when_id_known() {
        let id = QuoteId::new();
        let r = resting(100, 1, Some(id));
        let actions = drop_side(Some(&r));
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], Action::Cancel(_)));
    }

    #[test]
    fn drop_side_noop_when_inflight() {
        let r = resting(100, 1, None);
        assert!(drop_side(Some(&r)).is_empty());
    }
}
