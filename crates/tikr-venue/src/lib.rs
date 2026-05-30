//! Venue trait surface for the tikr market-making engine.
//!
//! A [`Venue`] abstracts the full lifecycle of a quoting session on any venue
//! type — CEX orderbook, DEX orderbook, or AMM range position. Callers work
//! with [`QuoteIntent`] values; each adapter translates them into the
//! venue's native primitives (REST order, on-chain calldata, etc.).
//!
//! # Push vs pull
//!
//! Market data is **push-primary**: [`Venue::subscribe`] returns a live stream
//! that the strategy consumes in a select! loop. [`Venue::snapshot`] and
//! [`Venue::fills_since`] are **pull/reconciliation** calls — used at startup
//! or after a reconnect to resync state, not in the hot path.
//!
//! # Error handling
//!
//! [`VenueError`] variants are split into **retryable** ([`VenueError::Network`],
//! [`VenueError::RateLimited`]) and **terminal** ([`VenueError::InsufficientBalance`],
//! [`VenueError::UnknownQuote`], [`VenueError::Rejected`], [`VenueError::Internal`]).
//! Strategy code should inspect the variant before deciding whether to retry.

#![deny(missing_docs)]

use async_trait::async_trait;
use futures::stream::BoxStream;
use tikr_core::{
    Fill, MarketEvent, Position, Price, QuoteKind, Side, Size, Snapshot, Symbol, TimeInForce,
};

pub use tikr_core::QuoteId;

// ---------------------------------------------------------------------------
// QuoteIntent
// ---------------------------------------------------------------------------

/// Parameters for a single quoting action sent to a [`Venue`].
///
/// Strategies build a `QuoteIntent` and pass it to [`Venue::quote`] or
/// [`Venue::requote`]; adapters translate it into the venue's native order
/// type (limit order, LP position, etc.).
#[derive(Debug, Clone)]
pub struct QuoteIntent {
    /// Symbol to quote on.
    pub symbol: Symbol,
    /// Side (bid or ask) from the market-maker's perspective.
    pub side: Side,
    /// Quoted price.
    pub price: Price,
    /// Quoted size.
    pub size: Size,
    /// Time-in-force policy for the quote.
    pub tif: TimeInForce,
    /// Shape of the quote (point or range).
    pub kind: QuoteKind,
}

// ---------------------------------------------------------------------------
// OpenOrder
// ---------------------------------------------------------------------------

/// A venue's view of a single resting order. Returned by
/// [`Venue::open_orders`] for periodic reconciliation against in-memory
/// fill-sim state.
#[derive(Debug, Clone)]
pub struct OpenOrder {
    /// Venue-assigned id.
    pub id: QuoteId,
    /// Symbol the order rests on.
    pub symbol: Symbol,
    /// Side (bid or ask).
    pub side: Side,
    /// Resting price.
    pub price: Price,
    /// Remaining size (origQty − executedQty for partials).
    pub size: Size,
}

// ---------------------------------------------------------------------------
// VenueError
// ---------------------------------------------------------------------------

/// Errors returned by [`Venue`] methods.
///
/// **Retryable**: [`VenueError::Network`], [`VenueError::RateLimited`].
///
/// **Terminal**: [`VenueError::InsufficientBalance`], [`VenueError::UnknownQuote`],
/// [`VenueError::Rejected`], [`VenueError::Internal`].
#[derive(thiserror::Error, Debug)]
pub enum VenueError {
    /// I/O or transport failure. Retryable with backoff.
    #[error("network: {0}")]
    Network(#[from] std::io::Error),
    /// Venue-side rate limit. Retryable after the indicated delay.
    #[error("rate limited, retry after {retry_after_ms}ms")]
    RateLimited {
        /// Minimum wait before retrying, in milliseconds.
        retry_after_ms: u64,
    },
    /// Insufficient balance to place the quote. Terminal.
    #[error("insufficient balance: need {need:?}, have {have:?}")]
    InsufficientBalance {
        /// Required size.
        need: Size,
        /// Available size.
        have: Size,
    },
    /// The provided [`QuoteId`] is not known to the venue. Terminal.
    #[error("unknown quote id")]
    UnknownQuote,
    /// Venue explicitly rejected the request. Terminal.
    #[error("venue rejected: {reason}")]
    Rejected {
        /// Human-readable rejection reason from the venue.
        reason: String,
    },
    /// Unexpected internal error. Terminal.
    #[error("internal: {0}")]
    Internal(#[source] Box<dyn std::error::Error + Send + Sync>),
}

// ---------------------------------------------------------------------------
// Venue trait
// ---------------------------------------------------------------------------

/// Abstracts a trading venue behind a single async surface.
///
/// Implementors cover CEX orderbooks, DEX orderbooks, and AMM range positions.
/// The trait is object-safe via `async_trait`; strategies hold `Box<dyn Venue>`.
///
/// # Market data
///
/// [`subscribe`][Venue::subscribe] is the push-primary path. Use
/// [`snapshot`][Venue::snapshot] and [`fills_since`][Venue::fills_since]
/// for pull/reconciliation at startup or after reconnect.
#[async_trait]
pub trait Venue: Send + Sync {
    /// Stable, lowercase identifier for this venue (e.g. `"hyperliquid"`, `"uniswap-v3-arbitrum"`).
    fn id(&self) -> &str;

    /// Fetch a full order-book snapshot for `symbol`. Pull/reconciliation path.
    async fn snapshot(&self, symbol: &Symbol) -> Result<Snapshot, VenueError>;

    /// Subscribe to a live stream of [`MarketEvent`]s for `symbol`. Push-primary path.
    async fn subscribe(&self, symbol: &Symbol) -> Result<BoxStream<'_, MarketEvent>, VenueError>;

    /// Submit a new quote intent. Returns a [`QuoteId`] that can be used to
    /// update or cancel the quote later.
    async fn quote(&self, intent: QuoteIntent) -> Result<QuoteId, VenueError>;

    /// Replace an existing quote (identified by `id`) with a new intent.
    async fn requote(&self, id: QuoteId, intent: QuoteIntent) -> Result<(), VenueError>;

    /// Cancel the quote identified by `id`.
    async fn cancel(&self, id: QuoteId) -> Result<(), VenueError>;

    /// Cancel all outstanding quotes on `symbol`.
    async fn cancel_all(&self, symbol: &Symbol) -> Result<(), VenueError>;

    /// Return the current position for `symbol`. Pull path.
    async fn position(&self, symbol: &Symbol) -> Result<Position, VenueError>;

    /// Return fills for `symbol` timestamped at or after `since_ts`
    /// (nanoseconds since UNIX epoch). Pull/reconciliation path used by the
    /// runner to gap-fill trades the WS user-data stream missed — each
    /// returned [`Fill`] should carry its venue `trade_id` so the caller can
    /// deduplicate against fills already applied from the WS stream.
    ///
    /// Default returns an empty vec — venues without a trade-history REST
    /// endpoint (paper/backtest, hyperliquid v0 where needed) opt out without
    /// breaking the trait.
    async fn fills_since(&self, _symbol: &Symbol, _since_ts: u64) -> Result<Vec<Fill>, VenueError> {
        Ok(Vec::new())
    }

    /// Return the venue's current view of resting orders for `symbol`.
    ///
    /// Used by the runner for periodic reconciliation: any
    /// `FillSim::live_quotes` entry whose `QuoteId` is NOT in the venue's
    /// returned set is a **ghost** (silently cancelled, expired, or lost
    /// across a `listenKey` reconnect) and gets dropped.
    ///
    /// Default returns an empty vec — venues that don't support
    /// reconciliation (paper backtest, hyperliquid v0) opt out without
    /// breaking the trait.
    async fn open_orders(&self, _symbol: &Symbol) -> Result<Vec<OpenOrder>, VenueError> {
        Ok(Vec::new())
    }

    /// Close the current position on `symbol` with a market order.
    /// Default uses an IOC limit at the worst price (0 or max) as a fallback.
    async fn market_close(&self, symbol: &Symbol, side: Side, qty: Size) -> Result<(), VenueError> {
        let intent = QuoteIntent {
            symbol: symbol.clone(),
            side,
            price: Price(tikr_core::Decimal::ZERO),
            size: qty,
            tif: TimeInForce::IOC,
            kind: QuoteKind::Point,
        };
        let _ = self.quote(intent).await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::{Asset, Decimal, MarketKind, VenueId};

    // -----------------------------------------------------------------------
    // No-op test implementation
    // -----------------------------------------------------------------------

    struct NoopVenue;

    #[async_trait]
    impl Venue for NoopVenue {
        fn id(&self) -> &str {
            "noop"
        }

        async fn snapshot(&self, _symbol: &Symbol) -> Result<Snapshot, VenueError> {
            unimplemented!()
        }

        async fn subscribe(
            &self,
            _symbol: &Symbol,
        ) -> Result<BoxStream<'_, MarketEvent>, VenueError> {
            unimplemented!()
        }

        async fn quote(&self, _intent: QuoteIntent) -> Result<QuoteId, VenueError> {
            unimplemented!()
        }

        async fn requote(&self, _id: QuoteId, _intent: QuoteIntent) -> Result<(), VenueError> {
            unimplemented!()
        }

        async fn cancel(&self, _id: QuoteId) -> Result<(), VenueError> {
            unimplemented!()
        }

        async fn cancel_all(&self, _symbol: &Symbol) -> Result<(), VenueError> {
            unimplemented!()
        }

        async fn position(&self, _symbol: &Symbol) -> Result<Position, VenueError> {
            unimplemented!()
        }
    }

    // -----------------------------------------------------------------------

    #[test]
    fn trait_is_object_safe_via_async_trait() {
        // Compile-time proof that `dyn Venue` is usable.
        let _v: Box<dyn Venue> = Box::new(NoopVenue);
    }

    #[test]
    fn quote_intent_clone_debug() {
        let intent = QuoteIntent {
            symbol: Symbol {
                base: Asset::new("BTC"),
                quote: Asset::new("USDT"),
                venue: VenueId::new("test"),
                kind: MarketKind::Spot,
            },
            side: Side::Bid,
            price: Price(Decimal::new(60_000, 0)),
            size: Size(Decimal::new(1, 1)),
            tif: TimeInForce::PostOnly,
            kind: QuoteKind::Point,
        };
        let cloned = intent.clone();
        let dbg = format!("{cloned:?}");
        assert!(!dbg.is_empty());
    }

    #[test]
    fn venue_error_display_variants() {
        let errors: Vec<VenueError> = vec![
            VenueError::Network(std::io::Error::other("fail")),
            VenueError::RateLimited {
                retry_after_ms: 500,
            },
            VenueError::InsufficientBalance {
                need: Size(Decimal::new(1, 0)),
                have: Size(Decimal::new(0, 0)),
            },
            VenueError::UnknownQuote,
            VenueError::Rejected {
                reason: "bad price".into(),
            },
            VenueError::Internal(Box::new(std::io::Error::other("internal"))),
        ];
        for e in errors {
            assert!(!format!("{e}").is_empty());
        }
    }

    #[test]
    fn venue_error_from_io() {
        let e: VenueError = std::io::Error::other("x").into();
        assert!(matches!(e, VenueError::Network(_)));
    }

    #[test]
    fn quote_id_round_trip() {
        let q = QuoteId::new();
        assert_eq!(q, QuoteId::from_uuid(q.0));
    }
}
