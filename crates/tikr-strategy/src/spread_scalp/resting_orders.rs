//! Strategy-owned view of resting quotes.
//!
//! The old impl kept two parallel views: `last_bid/last_ask` on the
//! strategy, and `ctx.open_quotes` from the runner (sourced from
//! FillSim / venue). The two drifted constantly — that's how the
//! 6/3 open-count balloon happened: a fill arrived before
//! `FillSim::apply_pending` had promoted the just-placed quote into
//! `live_quotes`, the strategy saw the side as empty, and refilled
//! against a stale view.
//!
//! `RestingOrders` is the strategy's authoritative view: every action
//! it emits is recorded here on the way out; every fill ingest /
//! reconciliation against `ctx.open_quotes` updates it. Two helpers:
//!
//! - [`RestingOrders::current_for`] — does the strategy think it has a
//!   live quote on `side`? Returns the recorded target price + size.
//! - [`RestingOrders::reconcile`] — refresh from `ctx.open_quotes`
//!   when the runner provides venue truth.

use std::collections::HashMap;
use tikr_core::{Price, Side, Size};
use tikr_venue::{QuoteId, QuoteIntent};

/// A quote the strategy believes is resting on the venue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Resting {
    /// Venue-issued id once the runner places it. `None` while the
    /// action is in-flight — set by `record_ack` after the runner
    /// reports success.
    pub id: Option<QuoteId>,
    /// Quoted side.
    pub side: Side,
    /// Quoted price.
    pub price: Price,
    /// Current resting size on the venue. Set to the original intent
    /// size on `record_place`, then refreshed by `reconcile` from
    /// `ctx.open_quotes` so partial fills (which leave a shrunken
    /// resting order on the venue) shrink this field too. The next
    /// `policy::diff` then sees `r.size < target_size` and fires a
    /// Replace to top the side back up.
    pub size: Size,
}

/// Per-side strategy-owned quote book.
///
/// Indexed by [`Side`] because spread-scalp tops out at one quote per
/// side. Future strategies that need ladders should use a
/// `HashMap<QuoteId, Resting>` directly.
#[derive(Debug, Default, Clone)]
pub struct RestingOrders {
    by_side: HashMap<Side, Resting>,
}

impl RestingOrders {
    /// Fresh tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Currently-tracked quote on `side`, if any.
    pub fn current_for(&self, side: Side) -> Option<&Resting> {
        self.by_side.get(&side)
    }

    /// Mark a fresh action as in-flight. Records the intent's price /
    /// size so the diff path in Stage 4 can decide whether the next
    /// requote moves anything.
    pub fn record_place(&mut self, intent: &QuoteIntent) {
        self.by_side.insert(
            intent.side,
            Resting {
                id: None,
                side: intent.side,
                price: intent.price,
                size: intent.size,
            },
        );
    }

    /// Drop the recorded quote on `side`. Use after a Cancel /
    /// CancelAll action or after a confirmed full fill.
    pub fn drop_side(&mut self, side: Side) {
        self.by_side.remove(&side);
    }

    /// Wipe both sides — Stage 1+ `emit_requote` still uses CancelAll,
    /// so the tracker has to match. Stage 4 will replace this with
    /// per-side diff.
    pub fn drop_all(&mut self) {
        self.by_side.clear();
    }

    /// Refresh from the runner's view of `ctx.open_quotes`. Drops any
    /// tracked side that no longer appears in the venue truth; preserves
    /// price + size for sides that match.
    ///
    /// NOTE: in-flight (just-emitted) quotes won't appear in
    /// `ctx.open_quotes` until the runner places them and FillSim's
    /// pending promotion runs. Don't reconcile until at least one
    /// market event has flowed past the emit point.
    pub fn reconcile(&mut self, ctx_open: &[(QuoteId, QuoteIntent)]) {
        let venue_sides: std::collections::HashSet<Side> =
            ctx_open.iter().map(|(_, q)| q.side).collect();
        self.by_side.retain(|s, _| venue_sides.contains(s));
        // Refresh `id` and `size` from venue truth. `ctx.open_quotes`
        // exposes `size_remaining` (NOT the original intent size — see
        // `FillSim::live_quotes_for` which stamps `size: q.size_remaining`).
        // Partial fills shrink `r.size` here so the next `policy::diff`
        // sees the gap vs the strategy's target and fires a Replace.
        for (id, q) in ctx_open {
            if let Some(r) = self.by_side.get_mut(&q.side) {
                r.id = Some(*id);
                r.size = q.size;
            }
        }
    }

    /// Number of sides currently tracked (0, 1, or 2).
    pub fn len(&self) -> usize {
        self.by_side.len()
    }

    /// Whether neither side is tracked.
    pub fn is_empty(&self) -> bool {
        self.by_side.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::{Asset, Decimal, MarketKind, QuoteKind, Symbol, TimeInForce, VenueId};

    fn intent(side: Side, price: i64, size: i64) -> QuoteIntent {
        QuoteIntent {
            symbol: Symbol {
                base: Asset::new("BTC"),
                quote: Asset::new("USDT"),
                venue: VenueId::new("test"),
                kind: MarketKind::Perp,
            },
            side,
            price: Price(Decimal::from(price)),
            size: Size(Decimal::from(size)),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        }
    }

    #[test]
    fn record_and_lookup() {
        let mut ro = RestingOrders::new();
        ro.record_place(&intent(Side::Bid, 100, 1));
        ro.record_place(&intent(Side::Ask, 110, 1));
        assert_eq!(ro.len(), 2);
        assert_eq!(
            ro.current_for(Side::Bid).unwrap().price.0,
            Decimal::from(100)
        );
        assert_eq!(
            ro.current_for(Side::Ask).unwrap().price.0,
            Decimal::from(110)
        );
    }

    #[test]
    fn record_place_overwrites_same_side() {
        let mut ro = RestingOrders::new();
        ro.record_place(&intent(Side::Bid, 100, 1));
        ro.record_place(&intent(Side::Bid, 101, 1));
        assert_eq!(
            ro.current_for(Side::Bid).unwrap().price.0,
            Decimal::from(101)
        );
    }

    #[test]
    fn drop_side_removes() {
        let mut ro = RestingOrders::new();
        ro.record_place(&intent(Side::Bid, 100, 1));
        ro.record_place(&intent(Side::Ask, 110, 1));
        ro.drop_side(Side::Bid);
        assert!(ro.current_for(Side::Bid).is_none());
        assert!(ro.current_for(Side::Ask).is_some());
    }

    #[test]
    fn reconcile_drops_missing_sides() {
        let mut ro = RestingOrders::new();
        ro.record_place(&intent(Side::Bid, 100, 1));
        ro.record_place(&intent(Side::Ask, 110, 1));
        // Venue only reports the bid as still resting.
        let id = QuoteId::new();
        let venue_view = vec![(id, intent(Side::Bid, 100, 1))];
        ro.reconcile(&venue_view);
        assert!(ro.current_for(Side::Bid).is_some());
        assert!(ro.current_for(Side::Ask).is_none());
        assert_eq!(ro.current_for(Side::Bid).unwrap().id, Some(id));
    }

    /// Partial fill: venue reports the resting order with shrunken
    /// size. Reconcile must copy the new size into the tracker so the
    /// next policy::diff sees the gap and fires a Replace.
    #[test]
    fn reconcile_refreshes_size_for_partial_fills() {
        let mut ro = RestingOrders::new();
        ro.record_place(&intent(Side::Bid, 100, 5));
        let id = QuoteId::new();
        // Half-filled: venue now holds 2 of the original 5.
        let venue_view = vec![(id, intent(Side::Bid, 100, 2))];
        ro.reconcile(&venue_view);
        let r = ro.current_for(Side::Bid).expect("bid still tracked");
        assert_eq!(r.size, Size(Decimal::from(2)));
        assert_eq!(r.id, Some(id));
    }
}
