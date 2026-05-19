//! Hyperliquid [`Venue`] adapter — read-side implementation (Phase 3).
//!
//! Implements the four pull/push read paths of [`tikr_venue::Venue`]:
//!
//! - [`Venue::snapshot`] — POST `/info { "type": "l2Book", "coin": ... }`.
//! - [`Venue::subscribe`] — WebSocket `l2Book` + `trades` push, multiplexed
//!   into a single [`MarketEvent`] stream with synthesized heartbeats and
//!   automatic reconnect.
//! - [`Venue::position`] — POST `/info { "type": "clearinghouseState",
//!   "user": ... }`. Requires [`HyperliquidConfig::user_address`].
//! - [`Venue::fills_since`] — POST `/info { "type": "userFills", "user": ... }`,
//!   filtered by timestamp. Requires [`HyperliquidConfig::user_address`].
//!
//! Order placement (`quote` / `requote` / `cancel` / `cancel_all`) remains
//! `todo!()` pending Phase 5 wallet signing.
//!
//! All read endpoints are public — no signing required. The adapter creates a
//! fresh [`reqwest::Client`] per HTTP method call; that's adequate for the
//! low call rate on reconciliation paths and avoids life-cycle entanglement.
//!
//! # Fills and symbols
//!
//! [`Venue::fills_since`] is symbol-less: it returns *all* fills for the
//! configured user across all coins. The [`Fill`] type itself carries no
//! symbol, so callers that need per-symbol filtering must do it externally
//! using bookkeeping context. This matches the trait surface; a future
//! `fills_since_symbol` variant could specialize.
//!
//! See issues #22 and #24 for wire-format decisions and review notes.

#![deny(missing_docs)]

pub mod mapping;
pub mod messages;

mod client;
mod ws;

use async_trait::async_trait;
use futures::stream::BoxStream;
use tikr_core::{Fill, MarketEvent, Position, Snapshot, Symbol};
use tikr_venue::{QuoteId, QuoteIntent, Venue, VenueError};

// ---------------------------------------------------------------------------
// HyperliquidEnv
// ---------------------------------------------------------------------------

/// Hyperliquid environment selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HyperliquidEnv {
    /// Production mainnet (`api.hyperliquid.xyz`).
    Mainnet,
    /// Testnet (`api.hyperliquid-testnet.xyz`).
    Testnet,
}

// ---------------------------------------------------------------------------
// HyperliquidConfig
// ---------------------------------------------------------------------------

/// Configuration for the Hyperliquid adapter.
#[derive(Debug, Clone)]
pub struct HyperliquidConfig {
    /// Which environment to target. Defaults to [`HyperliquidEnv::Mainnet`].
    pub env: HyperliquidEnv,
    /// 0x-prefixed user address used by [`Venue::position`] and
    /// [`Venue::fills_since`]. `None` puts the adapter in public-data-only
    /// mode; those two methods will return [`VenueError::Rejected`].
    pub user_address: Option<String>,
    /// Cadence (ms) for synthesized [`MarketEvent::Heartbeat`] frames on the
    /// `subscribe` stream. `0` disables synthesis.
    pub heartbeat_ms: u64,
    /// Initial reconnect backoff (ms) after WS disconnect. Doubled per
    /// failed attempt, capped at [`Self::reconnect_max_backoff_ms`].
    pub reconnect_min_backoff_ms: u64,
    /// Reconnect backoff ceiling (ms).
    pub reconnect_max_backoff_ms: u64,
}

impl Default for HyperliquidConfig {
    fn default() -> Self {
        Self {
            env: HyperliquidEnv::Mainnet,
            user_address: None,
            heartbeat_ms: 1000,
            reconnect_min_backoff_ms: 1000,
            reconnect_max_backoff_ms: 30_000,
        }
    }
}

// ---------------------------------------------------------------------------
// Hyperliquid
// ---------------------------------------------------------------------------

/// Hyperliquid on-chain orderbook [`Venue`] adapter.
#[derive(Debug, Default)]
pub struct Hyperliquid {
    config: HyperliquidConfig,
}

impl Hyperliquid {
    /// Construct an adapter with the default configuration (mainnet,
    /// public-data-only).
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct an adapter from an explicit configuration.
    pub fn with_config(config: HyperliquidConfig) -> Self {
        Self { config }
    }

    /// Borrow the active configuration.
    pub fn config(&self) -> &HyperliquidConfig {
        &self.config
    }
}

#[async_trait]
impl Venue for Hyperliquid {
    fn id(&self) -> &str {
        "hyperliquid"
    }

    async fn snapshot(&self, symbol: &Symbol) -> Result<Snapshot, VenueError> {
        let client = client::HyperliquidClient::new(self.config.env);
        client.snapshot(symbol).await
    }

    async fn subscribe(&self, symbol: &Symbol) -> Result<BoxStream<'_, MarketEvent>, VenueError> {
        ws::subscribe_stream(self.config.clone(), symbol.clone()).await
    }

    async fn quote(&self, _intent: QuoteIntent) -> Result<QuoteId, VenueError> {
        todo!("Phase 5: sign + POST /exchange order action")
    }

    async fn requote(&self, _id: QuoteId, _intent: QuoteIntent) -> Result<(), VenueError> {
        todo!("Phase 5: cancel + re-place (Hyperliquid has no native modify)")
    }

    async fn cancel(&self, _id: QuoteId) -> Result<(), VenueError> {
        todo!("Phase 5: POST /exchange cancel action")
    }

    async fn cancel_all(&self, _symbol: &Symbol) -> Result<(), VenueError> {
        todo!("Phase 5: POST /exchange cancel-all for symbol")
    }

    async fn position(&self, symbol: &Symbol) -> Result<Position, VenueError> {
        let Some(user) = self.config.user_address.as_deref() else {
            return Err(VenueError::Rejected {
                reason: "position() requires HyperliquidConfig::user_address".into(),
            });
        };
        let client = client::HyperliquidClient::new(self.config.env);
        client.position(symbol, user).await
    }

    async fn fills_since(&self, since_ts: u64) -> Result<Vec<Fill>, VenueError> {
        let Some(user) = self.config.user_address.as_deref() else {
            return Err(VenueError::Rejected {
                reason: "fills_since() requires HyperliquidConfig::user_address".into(),
            });
        };
        let client = client::HyperliquidClient::new(self.config.env);
        client.user_fills_since(user, since_ts).await
    }
}
