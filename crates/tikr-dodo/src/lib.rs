//! DODO LimitOrder [`Venue`] adapter — write-side implementation (Phase 5).
//!
//! Implements the [`tikr_venue::Venue`] trait for DODO LimitOrder on BSC mainnet.
//!
//! # Write-side methods (v0)
//!
//! - [`Venue::quote`] — Sign an EIP-712 typed Order struct, POST it to the DODO
//!   API at `https://api.dodoex.io/limit-order/create?apikey=<key>`, return a
//!   [`QuoteId`] derived from the order hash.
//! - [`Venue::requote`] — Log a warn, call `quote(intent)`. The old order
//!   self-expires via the configurable `expiry_secs` (default 60s). No explicit
//!   cancel needed.
//! - [`Venue::cancel`] — **No-op + `tracing::warn!`**. DODO's cancel API
//!   requires a `signkey` whose derivation is undocumented. v0 uses short-expiry
//!   self-cancel instead. See issue #41 for the real cancel follow-up.
//! - [`Venue::cancel_all`] — Same no-op strategy as `cancel`.
//!
//! # Read-side methods (v0 stubs)
//!
//! - [`Venue::snapshot`] — `todo!()` — DODO LimitOrder has no on-chain orderbook
//!   snapshot API; feed market data from another source (e.g. a price oracle or
//!   a separate BSC AMM) and pass it to the runner externally.
//! - [`Venue::subscribe`] — `todo!()` — same rationale as `snapshot`.
//! - [`Venue::position`] — `todo!()` — position is tracked via the paper runner's
//!   fill accumulator, not queried from DODO directly (no REST endpoint).
//! - [`Venue::fills_since`] — `todo!()` — fills are pushed via the BSC log
//!   subscription in `events::subscribe_fills`; no backfill API in v0.
//!
//! # Cancel strategy (by design)
//!
//! DODO LimitOrder has no documented cancel-by-maker API in v0 (the cancel
//! endpoint uses a `signkey` whose derivation is proprietary). Every order is
//! signed with `expiration = now + expiry_secs` (default 60s), so orders
//! self-cancel after one minute. `requote()` simply places a new order; the old
//! one expires harmlessly. `cancel()` and `cancel_all()` log a `warn!` and
//! return `Ok(())` — callers (the runner, risk gate) must be aware of this
//! v0 limitation.
//!
//! # API key
//!
//! Set `TIKR_DODO_API_KEY` environment variable. No `--key-file` for the API
//! key; only wallet keys use that mechanism (per issue #40 locked decisions).
//!
//! # Wallet
//!
//! Private key loaded from `TIKR_BSC_PRIVATE_KEY` env var or `--key-file <path>`
//! in the example binary. The signer is passed as [`PrivateKeySigner`].
//!
//! # Mainnet gate
//!
//! `DodoClient::with_wallet` refuses write actions unless `TIKR_DODO_ENABLE_MAINNET=1`
//! is set. DODO LimitOrder has no testnet; all orders are real on BSC mainnet.
//!
//! # Token approvals (operator pre-step)
//!
//! Before the first order, the operator must approve the DODO approve contract
//! to spend both tokens. See `examples/run_live.rs` for the `cast send` commands.
//! The approval address on BSC is `0xa128Ba44B2738A558A1fdC06d6303d52D3Cef8c1`.
//!
//! See issues #38 (Hyperliquid sibling), #40 (this impl), #41-44 (follow-ups).

#![deny(missing_docs)]

pub mod events;
pub mod exchange;

pub use events::subscribe_fills;
pub use exchange::{DodoExchangeClient, OrderMap};

use alloy_primitives::{Address, U256};
use alloy_signer_local::PrivateKeySigner;
use async_trait::async_trait;
use futures::stream::BoxStream;
use tikr_core::{Fill, MarketEvent, Position, Symbol};
use tikr_venue::{QuoteId, QuoteIntent, Venue, VenueError};
use tracing::warn;

// ---------------------------------------------------------------------------
// DodoConfig
// ---------------------------------------------------------------------------

/// Configuration for the DODO LimitOrder adapter.
#[derive(Debug, Clone)]
pub struct DodoConfig {
    /// Default order expiry in seconds. Orders self-cancel after this duration.
    /// Default: 60 seconds (matches typical MM requote cadence).
    pub order_expiry_secs: u64,

    /// Maker token address (the token we provide — e.g. WBNB).
    pub maker_token: Address,

    /// Taker token address (the token we receive — e.g. USDT).
    pub taker_token: Address,

    /// BSC WebSocket RPC URL for fill-event subscription.
    /// Default: `TIKR_BSC_RPC_URL` env, fallback `wss://bsc-ws-node.nariox.org`.
    pub rpc_ws_url: String,
}

impl Default for DodoConfig {
    fn default() -> Self {
        let rpc_ws_url = std::env::var("TIKR_BSC_RPC_URL")
            .unwrap_or_else(|_| "wss://bsc-ws-node.nariox.org".to_string());
        Self {
            order_expiry_secs: 60,
            maker_token: Address::ZERO,
            taker_token: Address::ZERO,
            rpc_ws_url,
        }
    }
}

// ---------------------------------------------------------------------------
// DodoClient
// ---------------------------------------------------------------------------

/// DODO LimitOrder [`Venue`] adapter for BSC mainnet.
///
/// Constructed via [`DodoClient::with_wallet`].
///
/// v0 covers the write side only. Read-side methods (`snapshot`, `subscribe`,
/// `position`, `fills_since`) are `todo!()` with a clear Phase 5+ note.
/// For live trading, feed market data from an external source and use
/// `events::subscribe_fills` for fill notifications.
pub struct DodoClient {
    config: DodoConfig,
    exchange: DodoExchangeClient,
}

impl std::fmt::Debug for DodoClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DodoClient")
            .field("order_expiry_secs", &self.config.order_expiry_secs)
            .field("maker_token", &self.config.maker_token)
            .field("taker_token", &self.config.taker_token)
            // NEVER print signer or api_key in Debug output.
            .finish()
    }
}

impl DodoClient {
    /// Construct a write-capable DODO adapter.
    ///
    /// Reads `TIKR_DODO_API_KEY` from the environment (required).
    /// Reads `TIKR_DODO_ENABLE_MAINNET` for the mainnet gate.
    ///
    /// Returns `VenueError::Rejected` if the API key is not set.
    pub fn with_wallet(config: DodoConfig, signer: PrivateKeySigner) -> Result<Self, VenueError> {
        let api_key = std::env::var("TIKR_DODO_API_KEY").map_err(|_| VenueError::Rejected {
            reason: "TIKR_DODO_API_KEY env var not set (required for DODO LimitOrder API)".into(),
        })?;

        let exchange = DodoExchangeClient::new(signer, api_key);

        Ok(Self { config, exchange })
    }

    /// Return the signer's Ethereum address (checksummed string).
    pub fn address(&self) -> String {
        self.exchange.address()
    }

    /// Return a reference to the active configuration.
    pub fn config(&self) -> &DodoConfig {
        &self.config
    }

    /// Return a clone of the order map (Arc) for use by the fill subscription task.
    pub fn order_map(&self) -> OrderMap {
        self.exchange.order_map.clone()
    }
}

#[async_trait]
impl Venue for DodoClient {
    fn id(&self) -> &str {
        "dodo"
    }

    /// DODO LimitOrder has no on-chain orderbook snapshot endpoint.
    ///
    /// v0 stub — `todo!()`. For the live runner, feed market data from an
    /// external source (e.g. DODO pool price via `/route` API, or a price
    /// oracle). See `examples/run_live.rs` for guidance.
    async fn snapshot(&self, _symbol: &Symbol) -> Result<tikr_core::Snapshot, VenueError> {
        todo!(
            "Phase 5+: DODO read-side via /route API or on-chain pool price oracle. \
             For v0 live trading, feed market data externally — see run_live.rs."
        )
    }

    /// DODO LimitOrder has no on-chain orderbook stream.
    ///
    /// v0 stub — `todo!()`. See `snapshot` for rationale and workaround.
    async fn subscribe(&self, _symbol: &Symbol) -> Result<BoxStream<'_, MarketEvent>, VenueError> {
        todo!(
            "Phase 5+: DODO market data stream via /route API polling or \
             on-chain AMM pool events. Feed externally for v0."
        )
    }

    /// Sign an EIP-712 typed Order, POST to DODO API, return a `QuoteId`.
    ///
    /// The `intent.price` and `intent.size` are used to derive `makerAmount`
    /// and `takerAmount` as integer token amounts (18 decimals assumed for both
    /// tokens — operator should verify for non-standard tokens).
    ///
    /// Specifically:
    /// - `makerAmount` = `intent.size.0` scaled to 18 decimals.
    /// - `takerAmount` = `intent.size.0 * intent.price.0` scaled to 18 decimals.
    async fn quote(&self, intent: QuoteIntent) -> Result<QuoteId, VenueError> {
        let maker_amount = decimal_to_u256_18dec(intent.size.0)?;
        let taker_amount = decimal_to_u256_18dec(intent.size.0 * intent.price.0)?;

        let (quote_id, _numeric_id) = self
            .exchange
            .place_order(
                self.config.maker_token,
                self.config.taker_token,
                maker_amount,
                taker_amount,
                self.config.order_expiry_secs,
                intent.symbol,
            )
            .await?;

        Ok(quote_id)
    }

    /// Cancel the existing quote (no-op; order self-expires) and place a new one.
    ///
    /// The old order will expire in at most `config.order_expiry_secs` seconds.
    /// This is logged at `warn!` level so operators are aware of the v0 limitation.
    async fn requote(&self, _id: QuoteId, intent: QuoteIntent) -> Result<(), VenueError> {
        warn!(
            "DODO requote: cancel is no-op in v0 — old order self-expires in {}s. \
             Placing new order. See issue #41 for real cancel (requires signkey derivation).",
            self.config.order_expiry_secs
        );

        let maker_amount = decimal_to_u256_18dec(intent.size.0)?;
        let taker_amount = decimal_to_u256_18dec(intent.size.0 * intent.price.0)?;

        self.exchange
            .place_order(
                self.config.maker_token,
                self.config.taker_token,
                maker_amount,
                taker_amount,
                self.config.order_expiry_secs,
                intent.symbol,
            )
            .await?;

        Ok(())
    }

    /// Cancel a single order. **No-op in v0.**
    ///
    /// DODO's cancel API requires a `signkey` whose derivation is undocumented.
    /// Orders self-expire via the short `expiration` field (default 60s).
    /// See issue #41 for the real cancel implementation.
    ///
    /// Logs a `warn!` on every call so operators clearly see the v0 limitation.
    async fn cancel(&self, _id: QuoteId) -> Result<(), VenueError> {
        warn!(
            "DODO cancel is no-op in v0 — order self-expires in {}s. \
             Use short order_expiry_secs to bound resting time. \
             See issue #41 for real cancel API (requires signkey derivation).",
            self.config.order_expiry_secs
        );
        Ok(())
    }

    /// Cancel all orders for a symbol. **No-op in v0.**
    ///
    /// Same rationale as [`cancel`][DodoClient::cancel]. All orders self-expire.
    /// Logs a `warn!` on every call.
    async fn cancel_all(&self, _symbol: &Symbol) -> Result<(), VenueError> {
        warn!(
            "DODO cancel_all is no-op in v0 — all orders self-expire in {}s. \
             This is the short-expiry self-cancel strategy. \
             See issue #41 for real bulk cancel via approval revocation.",
            self.config.order_expiry_secs
        );
        Ok(())
    }

    /// Current position. **v0 stub — `todo!()`.**
    ///
    /// DODO LimitOrder has no position query endpoint. In the live runner,
    /// position is accumulated from fill events via the paper runner's
    /// position tracker.
    async fn position(&self, _symbol: &Symbol) -> Result<Position, VenueError> {
        todo!(
            "Phase 5+: DODO position via fill event accumulation. \
             Use the paper runner's built-in position tracker from external fills."
        )
    }

    /// Historical fills. **v0 stub — `todo!()`.**
    ///
    /// DODO LimitOrder has no backfill REST endpoint in v0. Fills are delivered
    /// via the BSC log subscription in `events::subscribe_fills`.
    async fn fills_since(&self, _since_ts: u64) -> Result<Vec<Fill>, VenueError> {
        todo!(
            "Phase 5+: DODO fills_since via BSC log replay or DODO history API. \
             For v0, use events::subscribe_fills for live fill delivery."
        )
    }
}

// ---------------------------------------------------------------------------
// Decimal → U256 helpers
// ---------------------------------------------------------------------------

/// Convert a [`tikr_core::Decimal`] to a `U256` with 18 decimal places.
///
/// Example: `1.5` → `1_500_000_000_000_000_000` (1.5 × 10^18).
///
/// Returns `VenueError::Rejected` if the value is negative or too large.
fn decimal_to_u256_18dec(d: tikr_core::Decimal) -> Result<U256, VenueError> {
    use std::str::FromStr;

    if d.is_sign_negative() {
        return Err(VenueError::Rejected {
            reason: format!("negative amount not allowed for DODO order: {}", d),
        });
    }

    // Scale by 10^18 and truncate to integer.
    let scale = tikr_core::Decimal::from(10u64.pow(18));
    let scaled = (d * scale).floor();

    let s = scaled.to_string();
    // Remove trailing ".0" from floor().
    let s = s.split('.').next().unwrap_or(&s);

    U256::from_str(s).map_err(|e| VenueError::Rejected {
        reason: format!("amount too large for U256 ({}): {}", s, e),
    })
}
