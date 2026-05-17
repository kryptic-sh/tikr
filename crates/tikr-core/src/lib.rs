//! Canonical type vocabulary for the tikr market-making engine.
//!
//! All other crates consume these types.
//!
//! # Perspective
//!
//! [`Side`] is expressed from the market-maker's perspective:
//! [`Side::Bid`] is our buy quote, [`Side::Ask`] is our sell quote.
//!
//! # Arithmetic
//!
//! `Price * Size = Notional` via [`Mul`] impls on [`Price`] and [`Size`].
//! No other cross-type multiplication is defined.
//!
//! # Asset normalization
//!
//! [`Asset::new`] uppercases and trims whitespace. [`VenueId::new`] trims
//! only (venue ids are lowercase by convention; case is preserved).

#![deny(missing_docs)]

use std::{
    ops::{Add, Mul, Neg, Sub},
    sync::Arc,
};

pub use uuid::Uuid;

pub use rust_decimal::Decimal;

// ---------------------------------------------------------------------------
// Timestamp
// ---------------------------------------------------------------------------

/// Nanoseconds since the UNIX epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(pub u64);

// ---------------------------------------------------------------------------
// Price
// ---------------------------------------------------------------------------

/// A quoted price, represented as a [`Decimal`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Price(pub Decimal);

impl Add<Price> for Price {
    type Output = Price;
    fn add(self, rhs: Price) -> Price {
        Price(self.0 + rhs.0)
    }
}

impl Sub<Price> for Price {
    type Output = Price;
    fn sub(self, rhs: Price) -> Price {
        Price(self.0 - rhs.0)
    }
}

// ---------------------------------------------------------------------------
// Size
// ---------------------------------------------------------------------------

/// An unsigned order/fill size.
///
/// Semantically non-negative; the inner [`Decimal`] does not enforce this.
/// Subtraction may yield a negative inner value — the caller is responsible.
/// A `try_sub` may be added in a future version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Size(pub Decimal);

impl Add<Size> for Size {
    type Output = Size;
    fn add(self, rhs: Size) -> Size {
        Size(self.0 + rhs.0)
    }
}

impl Sub<Size> for Size {
    type Output = Size;
    fn sub(self, rhs: Size) -> Size {
        Size(self.0 - rhs.0)
    }
}

// ---------------------------------------------------------------------------
// SignedSize
// ---------------------------------------------------------------------------

/// A signed size, used for net position accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SignedSize(pub Decimal);

impl Add<SignedSize> for SignedSize {
    type Output = SignedSize;
    fn add(self, rhs: SignedSize) -> SignedSize {
        SignedSize(self.0 + rhs.0)
    }
}

impl Sub<SignedSize> for SignedSize {
    type Output = SignedSize;
    fn sub(self, rhs: SignedSize) -> SignedSize {
        SignedSize(self.0 - rhs.0)
    }
}

impl Neg for SignedSize {
    type Output = SignedSize;
    fn neg(self) -> SignedSize {
        SignedSize(-self.0)
    }
}

// ---------------------------------------------------------------------------
// Notional
// ---------------------------------------------------------------------------

/// A currency-denominated value (price × size).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Notional(pub Decimal);

impl Add<Notional> for Notional {
    type Output = Notional;
    fn add(self, rhs: Notional) -> Notional {
        Notional(self.0 + rhs.0)
    }
}

impl Sub<Notional> for Notional {
    type Output = Notional;
    fn sub(self, rhs: Notional) -> Notional {
        Notional(self.0 - rhs.0)
    }
}

// ---------------------------------------------------------------------------
// Cross-type multiplication: Price × Size = Notional
// ---------------------------------------------------------------------------

impl Mul<Size> for Price {
    type Output = Notional;
    /// `Price × Size = Notional`.
    fn mul(self, rhs: Size) -> Notional {
        Notional(self.0 * rhs.0)
    }
}

impl Mul<Price> for Size {
    type Output = Notional;
    /// `Size × Price = Notional` (symmetric for ergonomics).
    fn mul(self, rhs: Price) -> Notional {
        Notional(self.0 * rhs.0)
    }
}

// ---------------------------------------------------------------------------
// Side
// ---------------------------------------------------------------------------

/// Market-maker quoting side.
///
/// [`Bid`][Side::Bid] is our buy quote; [`Ask`][Side::Ask] is our sell quote.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    /// Buy-side quote (market-maker perspective).
    Bid,
    /// Sell-side quote (market-maker perspective).
    Ask,
}

// ---------------------------------------------------------------------------
// Asset
// ---------------------------------------------------------------------------

/// A normalized asset ticker (e.g. `"BTC"`, `"ETH"`).
///
/// Constructed via [`Asset::new`] which uppercases and trims whitespace.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Asset(pub Arc<str>);

impl Asset {
    /// Normalize `raw`: trim ASCII whitespace and uppercase, then intern as `Arc<str>`.
    pub fn new(raw: &str) -> Self {
        Asset(raw.trim().to_uppercase().into())
    }
}

// ---------------------------------------------------------------------------
// VenueId
// ---------------------------------------------------------------------------

/// An opaque venue identifier (e.g. `"hyperliquid"`).
///
/// Constructed via [`VenueId::new`] which trims whitespace but preserves case
/// (venue ids are lowercase by convention; normalization is the caller's responsibility).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VenueId(pub Arc<str>);

impl VenueId {
    /// Trim whitespace from `raw` and intern as `Arc<str>`. Case is preserved.
    pub fn new(raw: &str) -> Self {
        VenueId(raw.trim().into())
    }
}

// ---------------------------------------------------------------------------
// Symbol
// ---------------------------------------------------------------------------

/// A trading pair on a specific venue.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Symbol {
    /// Base asset (e.g. `BTC`).
    pub base: Asset,
    /// Quote asset (e.g. `USDT`).
    pub quote: Asset,
    /// Venue where this symbol is traded.
    pub venue: VenueId,
}

// ---------------------------------------------------------------------------
// TimeInForce
// ---------------------------------------------------------------------------

/// Order time-in-force policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeInForce {
    /// Post-only: reject if the order would immediately match.
    PostOnly,
    /// Good-till-cancelled.
    GTC,
    /// Immediate-or-cancel.
    IOC,
    /// Fill-or-kill.
    FOK,
}

// ---------------------------------------------------------------------------
// QuoteKind
// ---------------------------------------------------------------------------

/// Shape of a market-making quote.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteKind {
    /// Single-price point quote.
    Point,
    /// Range quote with a specified width in basis points.
    Range {
        /// Width of the quote range in basis points.
        width_bps: u32,
    },
}

// ---------------------------------------------------------------------------
// QuoteId
// ---------------------------------------------------------------------------

/// Adapter-assigned quote identifier, backed by a UUID v4.
///
/// Use [`QuoteId::new`] to mint a fresh id, or [`QuoteId::from_uuid`] when
/// converting a venue-native UUID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QuoteId(pub Uuid);

impl QuoteId {
    /// Mint a new, globally unique quote identifier.
    pub fn new() -> Self {
        QuoteId(Uuid::new_v4())
    }

    /// Wrap an existing [`Uuid`] as a [`QuoteId`].
    pub fn from_uuid(u: Uuid) -> Self {
        QuoteId(u)
    }
}

impl Default for QuoteId {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Level
// ---------------------------------------------------------------------------

/// A single price level in an order book.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Level {
    /// Price at this level.
    pub price: Price,
    /// Aggregated size available at this level.
    pub size: Size,
}

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

/// A full order-book snapshot for a symbol at a point in time.
///
/// Invariant (adapter-guaranteed, not enforced at construction):
/// `bids` sorted descending by price, `asks` sorted ascending by price.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Symbol this snapshot belongs to.
    pub symbol: Symbol,
    /// Bid levels, sorted descending by price.
    pub bids: Vec<Level>,
    /// Ask levels, sorted ascending by price.
    pub asks: Vec<Level>,
    /// Timestamp of the snapshot.
    pub ts: Timestamp,
}

// ---------------------------------------------------------------------------
// Fill
// ---------------------------------------------------------------------------

/// A completed fill on one of our quotes.
#[derive(Debug, Clone)]
pub struct Fill {
    /// Adapter-assigned quote id that was filled.
    pub quote_id: QuoteId,
    /// Fill price.
    pub price: Price,
    /// Fill size.
    pub size: Size,
    /// Asset in which the fee is denominated.
    pub fee_asset: Asset,
    /// Raw fee amount in `fee_asset` units.
    pub fee_amount: Decimal,
    /// Fee expressed as quote-currency notional.
    pub fee_quote: Notional,
    /// Side of the fill from the market-maker's perspective.
    pub side: Side,
    /// Timestamp of the fill.
    pub ts: Timestamp,
}

// ---------------------------------------------------------------------------
// Position
// ---------------------------------------------------------------------------

/// Current position in a symbol.
#[derive(Debug, Clone, PartialEq)]
pub struct Position {
    /// Symbol held.
    pub symbol: Symbol,
    /// Net signed size (positive = long, negative = short).
    pub size: SignedSize,
    /// Volume-weighted average entry price.
    pub avg_entry: Price,
    /// Cumulative realized PnL.
    pub realized_pnl: Notional,
}

// ---------------------------------------------------------------------------
// MarketEvent
// ---------------------------------------------------------------------------

/// An event emitted by a market-data or execution adapter.
#[derive(Debug, Clone)]
pub enum MarketEvent {
    /// A full order-book snapshot update.
    BookUpdate {
        /// The new snapshot.
        snapshot: Snapshot,
    },
    /// An individual trade observed on the venue.
    Trade {
        /// Symbol that traded.
        symbol: Symbol,
        /// Execution price.
        price: Price,
        /// Execution size.
        size: Size,
        /// Aggressor side.
        side: Side,
        /// Timestamp of the trade.
        ts: Timestamp,
    },
    /// A fill on one of our resting quotes.
    Fill(Fill),
    /// Liveness heartbeat from the adapter.
    Heartbeat {
        /// Timestamp of the heartbeat.
        ts: Timestamp,
    },
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    fn dec(mantissa: i64, scale: u32) -> Decimal {
        Decimal::new(mantissa, scale)
    }

    #[test]
    fn asset_normalizes_case_and_whitespace() {
        assert_eq!(Asset::new("  btc  "), Asset::new("BTC"));
    }

    #[test]
    fn venue_id_trims_but_keeps_case() {
        assert_eq!(VenueId::new("  Hyperliquid  "), VenueId::new("Hyperliquid"));
        assert_ne!(VenueId::new("Hyperliquid"), VenueId::new("hyperliquid"));
    }

    #[test]
    fn price_size_mul_gives_notional() {
        // 100 * 0.5 = 50.0
        let p = Price(Decimal::new(100, 0));
        let s = Size(Decimal::new(5, 1));
        assert_eq!(p * s, Notional(Decimal::new(500, 1)));
    }

    #[test]
    fn notional_mul_commutative() {
        let p = Price(Decimal::new(100, 0));
        let s = Size(Decimal::new(5, 1));
        assert_eq!(p * s, s * p);
    }

    #[test]
    fn decimal_chain_no_rounding() {
        // 0.1^10 should be representable exactly in Decimal
        let mut val = Decimal::new(1, 1); // 0.1
        let factor = Decimal::new(1, 1);
        for _ in 0..9 {
            val *= factor;
        }
        // 0.1^10 = 1e-10
        let expected = Decimal::new(1, 10);
        assert_eq!(val, expected);
    }

    #[test]
    fn signed_size_negate() {
        let pos = SignedSize(Decimal::new(5, 0));
        let neg = -pos;
        assert_eq!(neg, SignedSize(Decimal::new(-5, 0)));
    }

    #[test]
    fn position_round_trip() {
        let sym = Symbol {
            base: Asset::new("BTC"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("hyperliquid"),
        };
        let pos = Position {
            symbol: sym.clone(),
            size: SignedSize(dec(1, 0)),
            avg_entry: Price(dec(60_000, 0)),
            realized_pnl: Notional(dec(0, 0)),
        };
        let cloned = pos.clone();
        assert_eq!(pos, cloned);
    }

    #[test]
    fn market_event_variants() {
        let sym = Symbol {
            base: Asset::new("ETH"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("hyperliquid"),
        };
        let ts = Timestamp(0);

        let book = MarketEvent::BookUpdate {
            snapshot: Snapshot {
                symbol: sym.clone(),
                bids: vec![],
                asks: vec![],
                ts,
            },
        };
        let trade = MarketEvent::Trade {
            symbol: sym.clone(),
            price: Price(dec(3000, 0)),
            size: Size(dec(1, 0)),
            side: Side::Ask,
            ts,
        };
        let fill = MarketEvent::Fill(Fill {
            quote_id: QuoteId::new(),
            price: Price(dec(3000, 0)),
            size: Size(dec(1, 0)),
            fee_asset: Asset::new("USDT"),
            fee_amount: dec(3, 0),
            fee_quote: Notional(dec(3, 0)),
            side: Side::Bid,
            ts,
        });
        let hb = MarketEvent::Heartbeat { ts };

        // Exhaustive pattern match — compiler enforces all variants covered.
        let mut variant_count = 0u32;
        for event in [book, trade, fill, hb] {
            match event {
                MarketEvent::BookUpdate { .. } => variant_count += 1,
                MarketEvent::Trade { .. } => variant_count += 1,
                MarketEvent::Fill(_) => variant_count += 1,
                MarketEvent::Heartbeat { .. } => variant_count += 1,
            }
        }
        assert_eq!(variant_count, 4);
    }

    #[test]
    fn side_inequality() {
        assert_ne!(Side::Bid, Side::Ask);
    }
}
