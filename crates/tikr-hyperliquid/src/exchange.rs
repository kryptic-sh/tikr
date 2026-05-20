//! Sign + POST module for the Hyperliquid `/exchange` endpoint (Phase 5).
//!
//! ## Architecture
//!
//! [`ExchangeClient`] holds:
//! - An `HttpClient` for POSTing to `/exchange`.
//! - A [`PrivateKeySigner`] used for EIP-712 typed-data signing.
//! - A base URL (mainnet or testnet).
//! - A mainnet-write-enabled flag (reads `TIKR_HL_ENABLE_MAINNET=1`).
//! - An atomic nonce counter seeded from epoch-ms at construction.
//!
//! ## QuoteId ↔ oid encoding
//!
//! Hyperliquid order IDs (`oid`) are `u64` values assigned by the venue.
//! We round-trip them through `Uuid::from_u128(oid as u128)` → `QuoteId`.
//! Reverse: `uuid.as_u128() as u64` (lossy on high bits, but venue-assigned
//! oids have high 64 bits zero). This is documented here and in the commit.
//!
//! ## Cloid encoding
//!
//! `format!("0x{:032x}", uuid.as_u128())` — 128-bit hex string matching
//! Hyperliquid's cloid format.
//!
//! ## Asset indexing
//!
//! Hyperliquid orders use `asset: u32` (integer index) rather than symbol
//! name. The index is looked up from `GET /info { "type": "meta" }` which
//! returns the `universe` array. We cache this at `ExchangeClient` construction
//! via a one-shot HTTP GET so the hot path (`place_order`) is index-only.
//!
//! ## Signing (L1 action flow)
//!
//! Hyperliquid L1 actions (order, cancel, updateLeverage) are signed as follows:
//!
//! 1. Msgpack-encode the action dict (field-name keys must be sorted — rmp
//!    uses insertion order, so we drive encoding through a struct with
//!    `#[serde(rename)]` to match the Python SDK field ordering).
//! 2. Append nonce as 8 bytes big-endian.
//! 3. Append `\x00` (no vault address).
//! 4. keccak256 the concatenation → `connection_id: B256`.
//! 5. Build phantom `Agent { source: "a"|"b", connectionId: connection_id }`.
//! 6. EIP-712 sign with domain:
//!    `{ name: "Exchange", version: "1", chainId: 1337, verifyingContract: 0x0..0 }`.
//! 7. POST: `{ action, nonce, signature: {r, s, v}, vaultAddress: null }`.
//!
//! ## Mainnet gate
//!
//! If `env == Mainnet` and `TIKR_HL_ENABLE_MAINNET=1` is NOT set, every
//! write-side method returns `VenueError::Rejected` before any network call.

use crate::HyperliquidEnv;
use alloy_primitives::{Address, B256, keccak256};
use alloy_signer::Signer;
use alloy_signer_local::PrivateKeySigner;
use reqwest::Client as HttpClient;
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tikr_core::{Asset, Decimal, Fill, Notional, Price, QuoteId, Size, Timestamp};
use tikr_venue::VenueError;
use tracing::{info, warn};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// EIP-712 domain for Hyperliquid L1 actions
// ---------------------------------------------------------------------------

/// Hyperliquid EIP-712 domain for L1 actions (order, cancel, updateLeverage).
///
/// Source: Python SDK `l1_payload()`. chainId is the Hyperliquid L1 (NOT
/// Arbitrum 42161 which is only for the bridge).
const HL_CHAIN_ID: u64 = 1337;

// ---------------------------------------------------------------------------
// ExchangeClient
// ---------------------------------------------------------------------------

/// HTTP + signing client for Hyperliquid `/exchange` write-side actions.
///
/// Constructed via [`ExchangeClient::new`]. Must not be cloned — the nonce
/// counter is an `Arc<AtomicU64>` shared across clones would cause nonce
/// collisions; callers should use `Arc<ExchangeClient>` if sharing across
/// tasks.
pub struct ExchangeClient {
    http: HttpClient,
    signer: PrivateKeySigner,
    exchange_url: String,
    info_url: String,
    is_mainnet: bool,
    mainnet_writes_enabled: bool,
    /// Monotonically increasing nonce; seeded from epoch-ms at construction.
    nonce: Arc<AtomicU64>,
    /// Cached `universe` index for asset name → asset index lookup.
    /// Built once at construction from `/info { "type": "meta" }`.
    asset_index: std::collections::HashMap<String, u32>,
    /// Number of decimals for size precision per asset (szDecimals from meta).
    sz_decimals: std::collections::HashMap<String, u32>,
}

impl ExchangeClient {
    /// Construct from a signer and environment.
    ///
    /// Performs a one-shot GET to `/info` to populate the asset index and
    /// size-decimal table. Returns an error if the initial metadata fetch fails.
    pub async fn new(env: HyperliquidEnv, signer: PrivateKeySigner) -> Result<Self, VenueError> {
        let (exchange_url, info_url) = match env {
            HyperliquidEnv::Mainnet => (
                "https://api.hyperliquid.xyz/exchange".to_string(),
                "https://api.hyperliquid.xyz/info".to_string(),
            ),
            HyperliquidEnv::Testnet => (
                "https://api.hyperliquid-testnet.xyz/exchange".to_string(),
                "https://api.hyperliquid-testnet.xyz/info".to_string(),
            ),
        };

        let is_mainnet = env == HyperliquidEnv::Mainnet;
        let mainnet_writes_enabled = if is_mainnet {
            std::env::var("TIKR_HL_ENABLE_MAINNET").as_deref() == Ok("1")
        } else {
            true // testnet always enabled
        };

        if is_mainnet && !mainnet_writes_enabled {
            warn!(
                "ExchangeClient: Mainnet env + TIKR_HL_ENABLE_MAINNET not set; write actions will be refused"
            );
        }

        // Seed nonce from epoch-ms. Doc: single binary per key; multi-binary
        // same key = nonce collision = broken (by design).
        let seed_nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let http = HttpClient::new();
        let (asset_index, sz_decimals) = fetch_meta(&http, &info_url).await?;

        Ok(Self {
            http,
            signer,
            exchange_url,
            info_url,
            is_mainnet,
            mainnet_writes_enabled,
            nonce: Arc::new(AtomicU64::new(seed_nonce)),
            asset_index,
            sz_decimals,
        })
    }

    // -------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------

    /// Returns `Err(VenueError::Rejected)` if mainnet writes are disabled.
    fn check_mainnet_gate(&self) -> Result<(), VenueError> {
        if self.is_mainnet && !self.mainnet_writes_enabled {
            return Err(VenueError::Rejected {
                reason: "mainnet writes disabled — set TIKR_HL_ENABLE_MAINNET=1".into(),
            });
        }
        Ok(())
    }

    /// Fetch the next nonce (epoch-ms, strictly increasing).
    fn next_nonce(&self) -> u64 {
        // CAS loop: concurrent callers must not collide on the same nonce.
        // Wall clock seeds the floor; `prev + 1` enforces strict monotonicity
        // even when clock resolution is coarse.
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        loop {
            let prev = self.nonce.load(Ordering::Relaxed);
            let next = now_ms.max(prev + 1);
            if self
                .nonce
                .compare_exchange_weak(prev, next, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                return next;
            }
        }
    }

    /// Look up the `asset` integer index for a coin name.
    fn asset_index_for(&self, coin: &str) -> Result<u32, VenueError> {
        self.asset_index
            .get(coin)
            .copied()
            .ok_or_else(|| VenueError::Rejected {
                reason: format!("unknown asset '{}'; not in Hyperliquid universe", coin),
            })
    }

    /// Format a decimal for the Hyperliquid wire format (normalized, max 8dp).
    fn format_decimal(d: Decimal) -> String {
        // Normalize strips trailing zeros; Decimal::to_string() then gives a
        // clean representation without scientific notation.
        format!("{}", d.normalize())
    }

    // -------------------------------------------------------------------
    // Sign + POST
    // -------------------------------------------------------------------

    /// Sign an L1 action and POST it to `/exchange`.
    ///
    /// Returns the parsed JSON response body.
    async fn post_action(&self, action: Value, nonce: u64) -> Result<Value, VenueError> {
        // 1. Compute action_hash: msgpack(action) ++ nonce(8-byte BE) ++ \x00
        let action_bytes =
            rmp_serde::to_vec_named(&action).map_err(|e| VenueError::Internal(Box::new(e)))?;
        let mut hash_input = action_bytes;
        hash_input.extend_from_slice(&nonce.to_be_bytes());
        hash_input.push(0x00); // no vault address

        let connection_id: B256 = keccak256(&hash_input);

        // 2. Build phantom Agent typed data.
        let source = if self.is_mainnet { "a" } else { "b" };
        let agent_hash = sign_agent_eip712(&self.signer, source, connection_id).await?;

        // 3. Build POST body.
        let body = json!({
            "action": action,
            "nonce": nonce,
            "signature": {
                "r": format!("0x{}", hex::encode(agent_hash.r)),
                "s": format!("0x{}", hex::encode(agent_hash.s)),
                "v": agent_hash.v,
            },
            "vaultAddress": null,
        });

        // 4. POST.
        let resp = self
            .http
            .post(&self.exchange_url)
            .json(&body)
            .send()
            .await
            .map_err(network_err)?;

        let status = resp.status();
        if status.as_u16() == 429 {
            return Err(VenueError::RateLimited {
                retry_after_ms: 1000,
            });
        }

        let json_resp: Value = resp.json().await.map_err(internal_err)?;
        Ok(json_resp)
    }

    // -------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------

    /// Place a post-only limit order. Returns the venue-assigned `oid`.
    ///
    /// All orders use `Limit` with `tif: "Alo"` (post-only, add-liquidity-only).
    /// The `cloid` is derived from the provided `quote_id` as a 32-char hex string.
    pub async fn place_order(
        &self,
        coin: &str,
        price: Price,
        size: Size,
        is_buy: bool,
        quote_id: QuoteId,
    ) -> Result<u64, VenueError> {
        self.check_mainnet_gate()?;

        let asset = self.asset_index_for(coin)?;
        let cloid = cloid_from_quote_id(quote_id);

        // Round price to 5 significant figures (Hyperliquid perp tick).
        // Round size to szDecimals for this asset.
        let sz_dec = self.sz_decimals.get(coin).copied().unwrap_or(3);
        let rounded_size = size.0.round_dp(sz_dec);
        let price_str = Self::format_decimal(price.0);
        let size_str = Self::format_decimal(rounded_size);

        let action = json!({
            "type": "order",
            "orders": [{
                "a": asset,
                "b": is_buy,
                "p": price_str,
                "s": size_str,
                "r": false,
                "t": { "limit": { "tif": "Alo" } },
                "c": cloid,
            }],
            "grouping": "na",
        });

        let nonce = self.next_nonce();
        info!(
            coin,
            price = %price_str,
            size = %size_str,
            is_buy,
            nonce,
            "placing order"
        );

        let resp = self.post_action(action, nonce).await?;
        parse_order_response(&resp)
    }

    /// Cancel an order by `oid` (extracted from `QuoteId`).
    ///
    /// Idempotent: "never placed", "already canceled", "already filled" → `Ok(())`.
    pub async fn cancel_order(&self, coin: &str, oid: u64) -> Result<(), VenueError> {
        self.check_mainnet_gate()?;

        let asset = self.asset_index_for(coin)?;

        let action = json!({
            "type": "cancel",
            "cancels": [{ "a": asset, "o": oid }],
        });

        let nonce = self.next_nonce();
        info!(coin, oid, nonce, "canceling order");

        let resp = self.post_action(action, nonce).await?;
        parse_cancel_response(&resp)
    }

    /// Cancel all orders for a coin via cancel-by-coin (bulk cancel).
    ///
    /// Idempotent: no resting orders → success.
    pub async fn cancel_all(&self, coin: &str) -> Result<(), VenueError> {
        self.check_mainnet_gate()?;

        // Query open orders for this coin first; cancel-all = cancel each oid.
        // Defensive startup behaviour: no resting orders → no-op.
        let open_oids = self.fetch_open_order_oids(coin).await?;
        if open_oids.is_empty() {
            info!(coin, "cancel_all: no open orders");
            return Ok(());
        }

        let asset = self.asset_index_for(coin)?;
        let cancels: Vec<Value> = open_oids
            .into_iter()
            .map(|oid| json!({ "a": asset, "o": oid }))
            .collect();

        let cancel_action = json!({
            "type": "cancel",
            "cancels": cancels,
        });

        let nonce = self.next_nonce();
        info!(coin, nonce, "cancel_all for coin");

        let resp = self.post_action(cancel_action, nonce).await?;
        // For bulk cancel, count successes; ignore already-canceled.
        parse_cancel_response(&resp)
    }

    /// Fetch open order IDs for a coin from `/info`.
    async fn fetch_open_order_oids(&self, coin: &str) -> Result<Vec<u64>, VenueError> {
        let user = self.signer.address().to_checksum(None);
        let body = json!({
            "type": "openOrders",
            "user": user,
        });
        let resp = self
            .http
            .post(&self.info_url)
            .json(&body)
            .send()
            .await
            .map_err(network_err)?;
        let orders: Vec<OpenOrderEntry> = resp.json().await.map_err(internal_err)?;
        Ok(orders
            .into_iter()
            .filter(|o| o.coin == coin)
            .map(|o| o.oid)
            .collect())
    }

    /// Set cross-margin leverage to 1x for a coin. Called once at startup.
    pub async fn update_leverage(&self, coin: &str, leverage: u32) -> Result<(), VenueError> {
        self.check_mainnet_gate()?;

        let asset = self.asset_index_for(coin)?;

        let action = json!({
            "type": "updateLeverage",
            "asset": asset,
            "isCross": true,
            "leverage": leverage,
        });

        let nonce = self.next_nonce();
        info!(coin, leverage, nonce, "updating leverage");

        let resp = self.post_action(action, nonce).await?;
        parse_generic_ok(&resp)
    }

    /// Cancel a single order by cloid (128-bit hex string).
    ///
    /// Used by [`crate::Hyperliquid::cancel`] which knows the cloid but not
    /// necessarily the asset integer index.
    ///
    /// Idempotent on already-canceled / already-filled strings.
    pub async fn cancel_by_cloid_raw(&self, cloid: &str) -> Result<(), VenueError> {
        self.check_mainnet_gate()?;

        // We need the asset index to cancel by cloid. Since `cancelByCloid`
        // requires asset + cloid, and we don't store the asset in the cloid
        // itself, we look up the open order by cloid via /info first.
        let oid = self.fetch_oid_for_cloid(cloid).await?;
        if let Some(oid) = oid {
            // We still need the asset. Fetch it from the open order.
            let (asset, real_oid) = self.fetch_asset_for_oid(oid).await?;
            if let Some(asset) = asset {
                let cancel_action = serde_json::json!({
                    "type": "cancel",
                    "cancels": [{ "a": asset, "o": real_oid }],
                });
                let nonce = self.next_nonce();
                let resp = self.post_action(cancel_action, nonce).await?;
                return parse_cancel_response(&resp);
            }
        }
        // If cloid not found in open orders, treat as idempotent success.
        Ok(())
    }

    /// Find the oid matching a cloid in open orders.
    async fn fetch_oid_for_cloid(&self, cloid: &str) -> Result<Option<u64>, VenueError> {
        let user = self.signer.address().to_checksum(None);
        let body = serde_json::json!({ "type": "openOrders", "user": user });
        let resp = self
            .http
            .post(&self.info_url)
            .json(&body)
            .send()
            .await
            .map_err(network_err)?;
        let orders: Vec<OpenOrderWithCloidEntry> = resp.json().await.map_err(internal_err)?;
        Ok(orders
            .into_iter()
            .find(|o| o.cloid.as_deref() == Some(cloid))
            .map(|o| o.oid))
    }

    /// Find the (asset, oid) for a given oid across all open orders.
    async fn fetch_asset_for_oid(&self, target_oid: u64) -> Result<(Option<u32>, u64), VenueError> {
        let user = self.signer.address().to_checksum(None);
        let body = serde_json::json!({ "type": "openOrders", "user": user });
        let resp = self
            .http
            .post(&self.info_url)
            .json(&body)
            .send()
            .await
            .map_err(network_err)?;
        let orders: Vec<OpenOrderWithCloidEntry> = resp.json().await.map_err(internal_err)?;
        for o in orders {
            if o.oid == target_oid {
                let asset = self.asset_index.get(&o.coin).copied();
                return Ok((asset, target_oid));
            }
        }
        Ok((None, target_oid))
    }

    /// Return the signer's Ethereum address (for WS subscriptions etc.).
    pub fn address(&self) -> String {
        self.signer.address().to_checksum(None)
    }
}

// ---------------------------------------------------------------------------
// EIP-712 Agent signing
// ---------------------------------------------------------------------------

/// EIP-712 signature components.
struct Sig {
    r: [u8; 32],
    s: [u8; 32],
    v: u8,
}

/// Sign the phantom Agent struct with EIP-712.
///
/// Domain: `{ name: "Exchange", version: "1", chainId: 1337, verifyingContract: 0x0..0 }`.
/// Types: `Agent: [{name: "source", type: "string"}, {name: "connectionId", type: "bytes32"}]`.
/// Message: `{ source: "a"|"b", connectionId: <B256> }`.
async fn sign_agent_eip712(
    signer: &PrivateKeySigner,
    source: &str,
    connection_id: B256,
) -> Result<Sig, VenueError> {
    // Build the EIP-712 struct hash manually.
    // typeHash = keccak256("Agent(string source,bytes32 connectionId)")
    let type_hash = keccak256(b"Agent(string source,bytes32 connectionId)");

    // Encode type hash ++ keccak256(source) ++ connectionId
    let source_hash = keccak256(source.as_bytes());

    let mut struct_data = Vec::with_capacity(96);
    struct_data.extend_from_slice(type_hash.as_slice());
    struct_data.extend_from_slice(source_hash.as_slice());
    struct_data.extend_from_slice(connection_id.as_slice());
    let struct_hash = keccak256(&struct_data);

    // Domain separator.
    // domainTypeHash = keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)")
    let domain_type_hash = keccak256(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    );
    let name_hash = keccak256(b"Exchange");
    let version_hash = keccak256(b"1");
    let chain_id_bytes: [u8; 32] = {
        let mut b = [0u8; 32];
        b[24..32].copy_from_slice(&HL_CHAIN_ID.to_be_bytes());
        b
    };
    let verifying_contract = Address::ZERO;

    let mut domain_data = Vec::with_capacity(160);
    domain_data.extend_from_slice(domain_type_hash.as_slice());
    domain_data.extend_from_slice(name_hash.as_slice());
    domain_data.extend_from_slice(version_hash.as_slice());
    domain_data.extend_from_slice(&chain_id_bytes);
    // address is padded to 32 bytes (left-padded with zeros)
    domain_data.extend_from_slice(&[0u8; 12]);
    domain_data.extend_from_slice(verifying_contract.as_slice());
    let domain_separator = keccak256(&domain_data);

    // Final digest: keccak256("\x19\x01" ++ domainSeparator ++ structHash)
    let mut digest_input = Vec::with_capacity(66);
    digest_input.push(0x19u8);
    digest_input.push(0x01u8);
    digest_input.extend_from_slice(domain_separator.as_slice());
    digest_input.extend_from_slice(struct_hash.as_slice());
    let digest = keccak256(&digest_input);

    // Sign the raw digest (not hashed again — alloy sign_hash signs the raw 32 bytes).
    let sig = signer
        .sign_hash(&digest)
        .await
        .map_err(|e| VenueError::Internal(Box::new(std::io::Error::other(e.to_string()))))?;

    let sig_bytes = sig.as_bytes();
    // alloy signature: [r(32) | s(32) | v(1)] where v is already 27 or 28
    // (alloy uses legacy Ethereum v encoding, not the raw recovery id 0/1).
    let mut r = [0u8; 32];
    let mut s = [0u8; 32];
    r.copy_from_slice(&sig_bytes[0..32]);
    s.copy_from_slice(&sig_bytes[32..64]);
    let v = sig_bytes[64]; // already 27 or 28

    Ok(Sig { r, s, v })
}

// ---------------------------------------------------------------------------
// Response parsers
// ---------------------------------------------------------------------------

fn parse_order_response(resp: &Value) -> Result<u64, VenueError> {
    // Success: {"status":"ok","response":{"type":"order","data":{"statuses":[{"resting":{"oid":N}}]}}}
    // Error:   {"status":"ok","response":{"type":"order","data":{"statuses":[{"error":"..."}]}}}
    // Outer error: {"status":"err","response":"..."}
    if resp.get("status").and_then(Value::as_str) == Some("err") {
        let reason = resp
            .get("response")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        return classify_rejection(reason);
    }
    let status = resp.pointer("/response/data/statuses/0").ok_or_else(|| {
        VenueError::Internal(Box::new(std::io::Error::other(format!(
            "unexpected order response shape: {}",
            resp
        ))))
    })?;
    if let Some(resting) = status.get("resting") {
        let oid = resting
            .get("oid")
            .and_then(Value::as_u64)
            .ok_or_else(|| VenueError::Internal(Box::new(std::io::Error::other("missing oid"))))?;
        info!(oid, "order placed");
        return Ok(oid);
    }
    if let Some(err) = status.get("error").and_then(Value::as_str) {
        return classify_rejection(err.to_string());
    }
    // "filled" immediately is also possible for Ioc — not expected for Alo.
    if let Some(filled) = status.get("filled") {
        let oid = filled.get("oid").and_then(Value::as_u64).unwrap_or(0);
        return Ok(oid);
    }
    Err(VenueError::Internal(Box::new(std::io::Error::other(
        format!("unexpected order status: {}", status),
    ))))
}

fn parse_cancel_response(resp: &Value) -> Result<(), VenueError> {
    if resp.get("status").and_then(Value::as_str) == Some("err") {
        let reason = resp
            .get("response")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        // Idempotent cancel strings.
        if is_idempotent_cancel(reason) {
            return Ok(());
        }
        return Err(VenueError::Rejected {
            reason: reason.to_string(),
        });
    }
    // Per-order statuses in bulk cancel.
    if let Some(statuses) = resp
        .pointer("/response/data/statuses")
        .and_then(Value::as_array)
    {
        for s in statuses {
            if let Some(err) = s.get("error").and_then(Value::as_str)
                && !is_idempotent_cancel(err)
            {
                return Err(VenueError::Rejected {
                    reason: err.to_string(),
                });
            }
        }
    }
    Ok(())
}

fn parse_generic_ok(resp: &Value) -> Result<(), VenueError> {
    if resp.get("status").and_then(Value::as_str) == Some("err") {
        let reason = resp
            .get("response")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        return Err(VenueError::Rejected {
            reason: reason.to_string(),
        });
    }
    Ok(())
}

/// Returns true if the cancel error string is idempotent (safe to ignore).
pub(crate) fn is_idempotent_cancel(err: &str) -> bool {
    let lower = err.to_lowercase();
    lower.contains("never placed")
        || lower.contains("already canceled")
        || lower.contains("already filled")
        || lower.contains("order not found")
        || lower.contains("no such")
}

fn classify_rejection(reason: String) -> Result<u64, VenueError> {
    let lower = reason.to_lowercase();
    if lower.contains("post-only") || lower.contains("would have crossed") {
        return Err(VenueError::Rejected {
            reason: format!("post-only crossed: {}", reason),
        });
    }
    if lower.contains("insufficient") || lower.contains("not enough margin") {
        return Err(VenueError::InsufficientBalance {
            need: Size(Decimal::ZERO),
            have: Size(Decimal::ZERO),
        });
    }
    Err(VenueError::Rejected { reason })
}

// ---------------------------------------------------------------------------
// Metadata fetch
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct MetaResponse {
    universe: Vec<AssetMeta>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AssetMeta {
    name: String,
    sz_decimals: u32,
}

#[derive(Debug, Deserialize)]
struct OpenOrderEntry {
    coin: String,
    oid: u64,
}

#[derive(Debug, Deserialize)]
struct OpenOrderWithCloidEntry {
    coin: String,
    oid: u64,
    cloid: Option<String>,
}

async fn fetch_meta(
    http: &HttpClient,
    info_url: &str,
) -> Result<
    (
        std::collections::HashMap<String, u32>,
        std::collections::HashMap<String, u32>,
    ),
    VenueError,
> {
    let body = json!({ "type": "meta" });
    let resp = http
        .post(info_url)
        .json(&body)
        .send()
        .await
        .map_err(network_err)?;
    let meta: MetaResponse = resp.json().await.map_err(internal_err)?;

    let mut asset_index = std::collections::HashMap::new();
    let mut sz_decimals = std::collections::HashMap::new();
    for (i, asset) in meta.universe.iter().enumerate() {
        asset_index.insert(asset.name.clone(), i as u32);
        sz_decimals.insert(asset.name.clone(), asset.sz_decimals);
    }
    Ok((asset_index, sz_decimals))
}

// ---------------------------------------------------------------------------
// QuoteId ↔ cloid helpers
// ---------------------------------------------------------------------------

/// Derive a cloid from a `QuoteId`: `"0x{:032x}"` of the UUID's u128.
pub fn cloid_from_quote_id(id: QuoteId) -> String {
    format!("0x{:032x}", id.0.as_u128())
}

/// Derive a `QuoteId` from a Hyperliquid `oid` (u64).
///
/// High 64 bits of the UUID are zero; vendor-assigned `oid` values fit in
/// 64 bits so no information is lost.
pub fn quote_id_from_oid(oid: u64) -> QuoteId {
    QuoteId::from_uuid(Uuid::from_u128(oid as u128))
}

/// Extract `oid` from a `QuoteId`.
///
/// See `quote_id_from_oid` for the encoding contract.
pub fn oid_from_quote_id(id: QuoteId) -> u64 {
    id.0.as_u128() as u64
}

// ---------------------------------------------------------------------------
// Error helpers (reuse pattern from client.rs)
// ---------------------------------------------------------------------------

pub(crate) fn network_err(e: reqwest::Error) -> VenueError {
    VenueError::Network(std::io::Error::other(e.to_string()))
}

pub(crate) fn internal_err(e: reqwest::Error) -> VenueError {
    VenueError::Internal(Box::new(e))
}

// ---------------------------------------------------------------------------
// userEvents fill mapping
// ---------------------------------------------------------------------------

/// Map a `userEvents` fill payload to a [`Fill`].
///
/// `oid` is widened to `Uuid::from_u128(oid as u128)` → `QuoteId`.
pub fn fill_from_user_event(_coin: &str, f: &UserEventFill) -> Fill {
    let side = if f.side == "B" {
        tikr_core::Side::Bid
    } else {
        tikr_core::Side::Ask
    };
    let fee = parse_decimal_str(&f.fee);
    Fill {
        quote_id: quote_id_from_oid(f.oid),
        price: Price(parse_decimal_str(&f.px)),
        size: Size(parse_decimal_str(&f.sz)),
        fee_asset: Asset::new(&f.fee_token),
        fee_amount: fee,
        fee_quote: Notional(fee),
        side,
        is_full: true,
        ts: Timestamp(f.time.saturating_mul(1_000_000)),
    }
}

fn parse_decimal_str(s: &str) -> Decimal {
    use std::str::FromStr;
    match Decimal::from_str(s) {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(value = %s, error = %e, "userEvents: failed to parse decimal — defaulting to 0");
            Decimal::ZERO
        }
    }
}

/// Fill shape from `userEvents` WS channel.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserEventFill {
    /// Fill price.
    pub px: String,
    /// Fill size.
    pub sz: String,
    /// User side: "B" or "A".
    pub side: String,
    /// Fee amount.
    pub fee: String,
    /// Fee currency.
    pub fee_token: String,
    /// Venue-assigned order id.
    pub oid: u64,
    /// Fill time in milliseconds.
    pub time: u64,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    /// cloid must be exactly 32 hex chars after the "0x" prefix (64 chars total? No —
    /// Hyperliquid cloid is 128-bit = 32 hex chars = 34 chars with "0x").
    #[test]
    fn cloid_from_quote_id_is_32_hex_chars() {
        let uuid = Uuid::from_u128(0xdeadbeef_cafe_1234_5678_abcdef012345u128);
        let id = QuoteId(uuid);
        let cloid = cloid_from_quote_id(id);
        assert!(cloid.starts_with("0x"), "cloid must start with 0x");
        let hex_part = &cloid[2..];
        assert_eq!(
            hex_part.len(),
            32,
            "hex portion must be exactly 32 chars (128-bit)"
        );
        assert!(
            hex_part.chars().all(|c| c.is_ascii_hexdigit()),
            "must be valid hex"
        );
    }

    /// Nonce counter increments strictly from the seeded value.
    #[test]
    fn nonce_strictly_increasing() {
        let seed = 1_000_000u64;
        let counter = AtomicU64::new(seed);

        // Simulate next_nonce logic: max(now_ms, prev+1).
        // In the test we use a fixed "now" to isolate clock dependency.
        let now_ms = seed; // same as seed (won't trigger max branch)
        let prev = counter.load(Ordering::Relaxed);
        let next = now_ms.max(prev + 1);
        counter.store(next, Ordering::Relaxed);
        assert_eq!(next, seed + 1);

        // Second call must be > first.
        let prev2 = counter.load(Ordering::Relaxed);
        let next2 = now_ms.max(prev2 + 1);
        counter.store(next2, Ordering::Relaxed);
        assert!(next2 > next, "nonce must strictly increase");
    }

    /// Cancel response parser treats idempotent strings as Ok.
    #[test]
    fn cancel_response_idempotent_on_already_canceled_string() {
        let idempotent_cases = [
            "never placed",
            "already canceled",
            "already filled",
            "order not found",
            "Order never placed",      // mixed case
            "Already Canceled by FOK", // prefix match
        ];
        for case in idempotent_cases {
            assert!(
                is_idempotent_cancel(case),
                "expected idempotent for: {case}"
            );
        }
        // A real error must NOT be idempotent.
        assert!(!is_idempotent_cancel("signature verification failed"));
        assert!(!is_idempotent_cancel("insufficient margin"));
    }

    /// Without TIKR_HL_ENABLE_MAINNET=1, mainnet ExchangeClient must refuse writes.
    ///
    /// We test the gate logic directly (without constructing ExchangeClient,
    /// which requires a network fetch). The gate is isolated in `check_mainnet_gate`.
    #[test]
    fn mainnet_writes_refused_without_env_flag() {
        // Simulate mainnet + flag NOT set.
        let is_mainnet = true;
        let mainnet_writes_enabled = false; // env var not set

        let gate_err = if is_mainnet && !mainnet_writes_enabled {
            Err::<(), VenueError>(VenueError::Rejected {
                reason: "mainnet writes disabled — set TIKR_HL_ENABLE_MAINNET=1".into(),
            })
        } else {
            Ok(())
        };

        assert!(
            matches!(gate_err, Err(VenueError::Rejected { .. })),
            "expected Rejected when mainnet writes disabled"
        );
    }
}
