//! Hyperliquid [`Venue`] adapter — read + write implementation (Phase 3 + 5).
//!
//! Implements all eight [`tikr_venue::Venue`] methods:
//!
//! **Read-side (Phase 3):**
//! - [`Venue::snapshot`] — POST `/info { "type": "l2Book", "coin": ... }`.
//! - [`Venue::subscribe`] — WebSocket `l2Book` + `trades` push, multiplexed
//!   into a single [`MarketEvent`] stream with synthesized heartbeats and
//!   automatic reconnect.
//! - [`Venue::position`] — POST `/info { "type": "clearinghouseState",
//!   "user": ... }`. Requires [`HyperliquidConfig::user_address`].
//! - [`Venue::fills_since`] — POST `/info { "type": "userFills", "user": ... }`,
//!   filtered by timestamp. Requires [`HyperliquidConfig::user_address`].
//!
//! **Write-side (Phase 5):**
//! - [`Venue::quote`] — sign + POST a post-only limit order to `/exchange`.
//! - [`Venue::requote`] — cancel the existing quote then place a new one.
//! - [`Venue::cancel`] — POST cancel action; idempotent on already-canceled/filled.
//! - [`Venue::cancel_all`] — POST cancel for all open orders on a symbol.
//!
//! # Write-side architecture
//!
//! Write-side methods require a [`PrivateKeySigner`] provided via
//! [`Hyperliquid::with_wallet`]. Without it, write methods return
//! [`VenueError::Rejected`].
//!
//! ## Mainnet gate
//!
//! Write actions on [`HyperliquidEnv::Mainnet`] require the env var
//! `TIKR_HL_ENABLE_MAINNET=1`. Without it, every write call returns
//! `VenueError::Rejected { reason: "mainnet writes disabled" }` before any
//! network call. The constructor logs a `tracing::warn!` if the gate is active.
//!
//! ## Defensive cancel on startup
//!
//! When `HyperliquidConfig::defensive_cancel_all` is `true` (the default),
//! `Hyperliquid::with_wallet` will cancel all open orders for the configured
//! symbol at construction time. Set to `false` in tests to avoid network calls.
//!
//! See issues #22, #24, #36, #38 for wire-format decisions and review notes.

#![deny(missing_docs)]

pub mod mapping;
pub mod messages;
pub mod ws;

mod client;
pub(crate) mod exchange;

pub use exchange::{ExchangeClient, cloid_from_quote_id, oid_from_quote_id, quote_id_from_oid};
pub use ws::subscribe_user_events;

use alloy_signer_local::PrivateKeySigner;
use async_trait::async_trait;
use futures::stream::BoxStream;
use tikr_core::{Decimal, Fill, MarketEvent, Position, Price, Side, Size, Snapshot, Symbol};
use tikr_venue::{QuoteId, QuoteIntent, Venue, VenueError};
use tracing::warn;

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
    /// When `true` (the default), cancel all open orders for the symbol at
    /// startup (defensive crash recovery). Set to `false` in tests.
    pub defensive_cancel_all: bool,
}

impl Default for HyperliquidConfig {
    fn default() -> Self {
        Self {
            env: HyperliquidEnv::Mainnet,
            user_address: None,
            heartbeat_ms: 1000,
            reconnect_min_backoff_ms: 1000,
            reconnect_max_backoff_ms: 30_000,
            defensive_cancel_all: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Hyperliquid
// ---------------------------------------------------------------------------

/// Hyperliquid on-chain orderbook [`Venue`] adapter.
///
/// Constructed via [`Hyperliquid::new`] (read-only) or
/// [`Hyperliquid::with_wallet`] (read + write). The write-side uses
/// [`ExchangeClient`] internally for EIP-712 signing.
pub struct Hyperliquid {
    config: HyperliquidConfig,
    /// Write-side client; `None` when constructed with [`Hyperliquid::new`]
    /// or [`Hyperliquid::with_config`].
    exchange: Option<ExchangeClient>,
}

impl std::fmt::Debug for Hyperliquid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hyperliquid")
            .field("env", &self.config.env)
            .field("user_address", &self.config.user_address)
            .field("write_enabled", &self.exchange.is_some())
            .finish()
    }
}

// Manual Default because derive requires all fields to implement Default,
// but ExchangeClient does not derive Default (it has internal async state).
// clippy::derivable_impls: suppressed because ExchangeClient is not Default.
#[allow(clippy::derivable_impls)]
impl Default for Hyperliquid {
    fn default() -> Self {
        Self {
            config: HyperliquidConfig::default(),
            exchange: None,
        }
    }
}

impl Hyperliquid {
    /// Construct an adapter with the default configuration (mainnet,
    /// public-data-only, no write capability).
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct an adapter from an explicit configuration (read-only).
    pub fn with_config(config: HyperliquidConfig) -> Self {
        Self {
            config,
            exchange: None,
        }
    }

    /// Construct a fully write-capable adapter.
    ///
    /// Performs a one-shot GET to `/info` to populate the asset index for
    /// the [`ExchangeClient`]. Returns an error if the initial metadata fetch
    /// fails.
    ///
    /// If `config.defensive_cancel_all` is true and `symbol` is provided,
    /// cancels all open orders for that symbol before returning.
    pub async fn with_wallet(
        config: HyperliquidConfig,
        signer: PrivateKeySigner,
        symbol: Option<&Symbol>,
    ) -> Result<Self, VenueError> {
        let exchange = ExchangeClient::new(config.env, signer).await?;

        // Defensive cancel-all on startup (crash recovery).
        if config.defensive_cancel_all
            && let Some(sym) = symbol
        {
            let coin = sym.base.0.as_ref();
            if let Err(e) = exchange.cancel_all(coin).await {
                warn!(
                    coin,
                    error = ?e,
                    "defensive cancel_all on startup failed; proceeding"
                );
            }
        }

        // Set leverage to 1x cross at startup.
        if let Some(sym) = symbol {
            let coin = sym.base.0.as_ref();
            if let Err(e) = exchange.update_leverage(coin, 1).await {
                warn!(
                    coin,
                    error = ?e,
                    "update_leverage(1) on startup failed; proceeding"
                );
            }
        }

        Ok(Self {
            config,
            exchange: Some(exchange),
        })
    }

    /// Borrow the active configuration.
    pub fn config(&self) -> &HyperliquidConfig {
        &self.config
    }

    /// Return the signer's Ethereum address as a checksum string, if a
    /// wallet was provided.
    pub fn address(&self) -> Option<String> {
        self.exchange.as_ref().map(|e| e.address())
    }

    /// Require the exchange client or return Rejected.
    fn require_exchange(&self) -> Result<&ExchangeClient, VenueError> {
        self.exchange.as_ref().ok_or_else(|| VenueError::Rejected {
            reason: "write operations require Hyperliquid::with_wallet".into(),
        })
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

    /// Place a post-only limit order.
    ///
    /// Maps `intent.side` → `is_buy: bool` and delegates to
    /// [`ExchangeClient::place_order`]. The returned `QuoteId` encodes the
    /// venue-assigned `oid` as `Uuid::from_u128(oid as u128)`.
    async fn quote(&self, intent: QuoteIntent) -> Result<QuoteId, VenueError> {
        let exchange = self.require_exchange()?;
        let coin = intent.symbol.base.0.as_ref();
        let is_buy = intent.side == tikr_core::Side::Bid;

        // Mint a fresh QuoteId to use as cloid, then send the order.
        // The venue returns an oid; we re-derive the QuoteId from it so that
        // oid_from_quote_id(quote_id) == oid is always true.
        let quote_id = QuoteId::new();
        let oid = exchange
            .place_order(
                coin,
                intent.price,
                intent.size,
                is_buy,
                quote_id,
                false,
                "Alo",
            )
            .await?;
        Ok(quote_id_from_oid(oid))
    }

    /// Cancel the old quote, then place a new one.
    ///
    /// Cancel errors propagate: only proceed on a successful or idempotent
    /// unknown-order cancellation (the [`ExchangeClient`] already maps those
    /// to `Ok`). Ambiguous failures retain the old local order state for
    /// reconciliation.
    async fn requote(&self, id: QuoteId, intent: QuoteIntent) -> Result<(), VenueError> {
        let exchange = self.require_exchange()?;
        let coin = intent.symbol.base.0.as_ref();

        // Cancel old.  Errors propagate — ambiguous cancel failures must not
        // silently create a duplicate resting order.
        let oid = oid_from_quote_id(id);
        exchange.cancel_order(coin, oid).await?;

        // Place new.
        let is_buy = intent.side == tikr_core::Side::Bid;
        let new_quote_id = QuoteId::new();
        exchange
            .place_order(
                coin,
                intent.price,
                intent.size,
                is_buy,
                new_quote_id,
                false,
                "Alo",
            )
            .await?;
        Ok(())
    }

    /// Cancel a single order by `QuoteId`.
    ///
    /// Idempotent: "never placed", "already canceled", "already filled" → `Ok(())`.
    async fn cancel(&self, id: QuoteId) -> Result<(), VenueError> {
        let exchange = self.require_exchange()?;
        // `quote()` returns an OID-derived QuoteId while the resting order's
        // cloid is a fresh uuid minted at place time — so a cloid derived
        // from `id` here would never match anything and the cancel would
        // "succeed" idempotently while the order kept resting. Cancel by the
        // oid encoded in the id instead; the exchange client resolves the
        // required asset index from open orders.
        let oid = oid_from_quote_id(id);
        exchange.cancel_by_oid_raw(oid).await
    }

    /// Cancel all open orders for a symbol.
    async fn cancel_all(&self, symbol: &Symbol) -> Result<(), VenueError> {
        let exchange = self.require_exchange()?;
        let coin = symbol.base.0.as_ref();
        exchange.cancel_all(coin).await
    }

    /// Close the current position with a marketable IOC order and
    /// `reduce_only=true`. Uses price 0 for sells and a sufficiently high
    /// price for buys to cross the spread immediately, then lets IOC
    /// cancellation remove any unfilled remainder.
    async fn market_close(&self, symbol: &Symbol, side: Side, qty: Size) -> Result<(), VenueError> {
        let exchange = self.require_exchange()?;
        let coin = symbol.base.0.as_ref();
        let is_buy = side == Side::Bid;

        // Hyperliquid market order conventions: 0 → market sell,
        // high price → market buy.
        let price = if is_buy {
            Price(Decimal::from(1_000_000_000u64))
        } else {
            Price(Decimal::ZERO)
        };

        let quote_id = QuoteId::new();
        exchange
            .place_order(coin, price, qty, is_buy, quote_id, true, "Ioc")
            .await?;
        Ok(())
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

    async fn fills_since(&self, symbol: &Symbol, since_ts: u64) -> Result<Vec<Fill>, VenueError> {
        let Some(user) = self.config.user_address.as_deref() else {
            return Err(VenueError::Rejected {
                reason: "fills_since() requires HyperliquidConfig::user_address".into(),
            });
        };
        let client = client::HyperliquidClient::new(self.config.env);
        let coin = symbol.base.0.as_ref();
        client.user_fills_since(user, coin, since_ts).await
    }
}
