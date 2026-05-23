//! Book + price arithmetic helpers used by spread-scalp.
//!
//! Stateless — every helper takes a [`Snapshot`] explicitly. Keeps the
//! `Strategy` impl in `mod.rs` free of arithmetic noise and makes each
//! transform unit-testable in isolation.

use tikr_core::{Decimal, Price, Size, Snapshot};

/// Top-of-book view: best bid + ask prices and sizes. Returns `None`
/// when either side is empty.
#[derive(Debug, Clone, Copy)]
pub struct Top {
    /// Best bid price.
    pub bid: Price,
    /// Best ask price.
    pub ask: Price,
    /// Aggregate size resting at the best bid.
    pub bid_size: Size,
    /// Aggregate size resting at the best ask.
    pub ask_size: Size,
}

impl Top {
    /// Extract top-of-book from a [`Snapshot`]. Returns `None` if
    /// either side is empty.
    pub fn from_snapshot(snap: &Snapshot) -> Option<Self> {
        let b = snap.bids.first()?;
        let a = snap.asks.first()?;
        Some(Self {
            bid: b.price,
            ask: a.price,
            bid_size: b.size,
            ask_size: a.size,
        })
    }

    /// Midpoint between best bid and best ask.
    pub fn mid(&self) -> Price {
        Price((self.bid.0 + self.ask.0) / Decimal::from(2))
    }

    /// `(ask - bid) / mid * 10_000`. Returns `None` if mid is zero.
    pub fn spread_bps(&self) -> Option<Decimal> {
        let mid = self.mid().0;
        if mid <= Decimal::ZERO {
            return None;
        }
        Some((self.ask.0 - self.bid.0) / mid * Decimal::from(10_000))
    }

    /// Microprice = `(bid_size · ask + ask_size · bid) / (bid_size + ask_size)`.
    /// Better predictor of next mid than the raw mid when book is
    /// imbalanced. Returns `mid` as a safe fallback if both sides are
    /// zero-size (degenerate snapshot).
    pub fn microprice(&self) -> Price {
        let b = self.bid_size.0;
        let a = self.ask_size.0;
        let total = b + a;
        if total <= Decimal::ZERO {
            return self.mid();
        }
        Price((b * self.ask.0 + a * self.bid.0) / total)
    }
}

/// Round-up size so `size × price >= min_notional`. A single step
/// bump can still leave the order under min_notional when the gap is
/// larger than `step_size × price`, which then re-triggers the venue
/// rejection → recovery hot loop. Compute the exact ceil instead.
pub fn size_at_least_min_notional(
    raw: Decimal,
    price: Price,
    min_notional: Decimal,
    step_size: Decimal,
) -> Decimal {
    if min_notional <= Decimal::ZERO || price.0 <= Decimal::ZERO {
        return raw;
    }
    let current = raw * price.0;
    if current >= min_notional {
        return raw;
    }
    if step_size <= Decimal::ZERO {
        return min_notional / price.0;
    }
    let gap = min_notional - current;
    let step_value = price.0 * step_size;
    if step_value <= Decimal::ZERO {
        return raw;
    }
    let mut needed_steps = (gap / step_value).floor();
    if needed_steps * step_value < gap {
        needed_steps += Decimal::ONE;
    }
    raw + needed_steps * step_size
}

/// `notional / price`, lot-step-rounded toward zero. Multiplied by
/// `size_multiplier` for inventory-bias sizing.
pub fn quote_size(
    notional: Decimal,
    price: Price,
    size_multiplier: Decimal,
    step_size: Decimal,
) -> Decimal {
    let raw = notional / price.0 * size_multiplier;
    if step_size > Decimal::ZERO {
        (raw / step_size).floor() * step_size
    } else {
        raw
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::{Asset, Level, MarketKind, Symbol, Timestamp, VenueId};

    fn snap(bid: i64, bsz: i64, ask: i64, asz: i64) -> Snapshot {
        let symbol = Symbol {
            base: Asset::new("BTC"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("test"),
            kind: MarketKind::Perp,
        };
        Snapshot {
            symbol,
            bids: vec![Level {
                price: Price(Decimal::from(bid)),
                size: Size(Decimal::from(bsz)),
            }],
            asks: vec![Level {
                price: Price(Decimal::from(ask)),
                size: Size(Decimal::from(asz)),
            }],
            ts: Timestamp(0),
        }
    }

    #[test]
    fn mid_and_spread_bps() {
        let top = Top::from_snapshot(&snap(100, 1, 110, 1)).unwrap();
        assert_eq!(top.mid().0, Decimal::from(105));
        // (110 - 100) / 105 × 10000 ≈ 952.4
        let bps = top.spread_bps().unwrap();
        assert!(bps > Decimal::from(950) && bps < Decimal::from(955));
    }

    #[test]
    fn microprice_pulls_toward_thinner_side() {
        // ask-heavy book: thinner bid → microprice pulled toward ask.
        let top = Top::from_snapshot(&snap(100, 9, 110, 1)).unwrap();
        let micro = top.microprice().0;
        let mid = top.mid().0;
        // (9·110 + 1·100) / 10 = 109. Mid = 105. Micro > mid.
        assert!(micro > mid);
        assert_eq!(micro, Decimal::from(109));
    }

    #[test]
    fn microprice_falls_back_to_mid_on_empty_book() {
        let top = Top::from_snapshot(&snap(100, 0, 110, 0)).unwrap();
        assert_eq!(top.microprice().0, top.mid().0);
    }

    #[test]
    fn empty_book_returns_none() {
        let symbol = Symbol {
            base: Asset::new("BTC"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("test"),
            kind: MarketKind::Perp,
        };
        let s = Snapshot {
            symbol,
            bids: vec![],
            asks: vec![],
            ts: Timestamp(0),
        };
        assert!(Top::from_snapshot(&s).is_none());
    }

    #[test]
    fn min_notional_bumps_in_one_shot() {
        // raw=1, price=1, min=10, step=1 → need 9 extra steps, not 1.
        let bumped = size_at_least_min_notional(
            Decimal::from(1),
            Price(Decimal::from(1)),
            Decimal::from(10),
            Decimal::from(1),
        );
        assert_eq!(bumped, Decimal::from(10));
    }

    #[test]
    fn min_notional_passthrough_when_already_satisfied() {
        let raw = Decimal::from(5);
        let bumped = size_at_least_min_notional(
            raw,
            Price(Decimal::from(3)),
            Decimal::from(10),
            Decimal::from(1),
        );
        assert_eq!(bumped, raw);
    }
}
