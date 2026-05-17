//! Hyperliquid Venue adapter — Phase 0 compile-only stub.
//!
//! **Status: stub.** All trait methods are `todo!()`. This crate exists to
//! prove the [`tikr_venue::Venue`] trait can be implemented for an on-chain
//! orderbook venue. Real HTTP/WS plumbing lands in Phase 1 (market data)
//! and Phase 3 (order placement).
//!
//! See `README.md` for status and roadmap notes.

#![deny(missing_docs)]

use async_trait::async_trait;
use futures::stream::BoxStream;
use std::marker::PhantomData;
use tikr_core::{Fill, MarketEvent, Position, Snapshot, Symbol};
use tikr_venue::{QuoteId, QuoteIntent, Venue, VenueError};

/// Hyperliquid on-chain orderbook adapter (Phase 0 stub).
#[derive(Default)]
pub struct Hyperliquid {
    _marker: PhantomData<()>,
}

impl Hyperliquid {
    /// Construct a new stub adapter. No I/O is performed.
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl Venue for Hyperliquid {
    fn id(&self) -> &str {
        "hyperliquid"
    }

    async fn snapshot(&self, _symbol: &Symbol) -> Result<Snapshot, VenueError> {
        todo!("Phase 1: fetch via https://api.hyperliquid.xyz/info l2Book")
    }

    async fn subscribe(&self, _symbol: &Symbol) -> Result<BoxStream<'_, MarketEvent>, VenueError> {
        todo!("Phase 1: connect wss://api.hyperliquid.xyz/ws, subscribe l2Book + trades")
    }

    async fn quote(&self, _intent: QuoteIntent) -> Result<QuoteId, VenueError> {
        todo!("Phase 3: sign + POST /exchange order action")
    }

    async fn requote(&self, _id: QuoteId, _intent: QuoteIntent) -> Result<(), VenueError> {
        todo!("Phase 3: cancel + re-place (Hyperliquid has no native modify)")
    }

    async fn cancel(&self, _id: QuoteId) -> Result<(), VenueError> {
        todo!("Phase 3: POST /exchange cancel action")
    }

    async fn cancel_all(&self, _symbol: &Symbol) -> Result<(), VenueError> {
        todo!("Phase 3: POST /exchange cancel-all for symbol")
    }

    async fn position(&self, _symbol: &Symbol) -> Result<Position, VenueError> {
        todo!("Phase 1: GET /info clearinghouseState")
    }

    async fn fills_since(&self, _since_ts: u64) -> Result<Vec<Fill>, VenueError> {
        todo!("Phase 1: GET /info userFills, filter by timestamp")
    }
}
