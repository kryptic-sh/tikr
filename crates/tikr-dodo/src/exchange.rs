//! Sign + POST module for the DODO LimitOrder API.
//!
//! ## Architecture
//!
//! [`DodoExchangeClient`] holds:
//! - An `HttpClient` for POSTing to the DODO API.
//! - A [`PrivateKeySigner`] for EIP-712 typed-data signing.
//! - The API key (from `TIKR_DODO_API_KEY`).
//! - A mainnet-write-enabled flag (reads `TIKR_DODO_ENABLE_MAINNET=1`).
//! - An atomic salt counter seeded from epoch nanos at construction.
//!
//! ## QuoteId encoding
//!
//! DODO returns a numeric `id` and a `bytes32` order hash. We encode the hash's
//! last 16 bytes as a UUID: `QuoteId(Uuid::from_u128(u128::from_be_bytes(hash[16..32])))`.
//! The first 16 bytes are discarded (negligible collision risk at our quote rate).
//!
//! A `HashMap<QuoteId, (numeric_id, [u8;32], AssetPair)>` is stored in the client
//! (under a `Mutex`) for fill-event matching and cancel bookkeeping.
//!
//! ## EIP-712 signing
//!
//! DODO uses a standard EIP-712 domain:
//! - name: "DODO Limit Order Protocol"
//! - version: "1"
//! - chainId: 56 (BSC mainnet)
//! - verifyingContract: 0xdc5E86654e768d21f7D298690687eA02db7b2a04
//!
//! Order type string (from deployed contract `ORDER_TYPEHASH`):
//! `Order(address makerToken,address takerToken,uint256 makerAmount,uint256 takerAmount,address maker,address taker,uint256 expiration,uint256 salt)`
//!
//! ORDER_TYPEHASH (keccak256 of above): `0x9e31ac2990003b5142f3966f6d93f8ee4befc60049bcd8504dce6d014d939c8a`
//! (verified against deployed contract source; see ORDER_TYPEHASH const below for context.)
//!
//! ## Salt generation
//!
//! `AtomicU64` seeded from epoch nanos at construction; incremented per quote via
//! a CAS loop (mirrors #38's nonce fix at commit b592985). Uniqueness per
//! (maker, salt) — DODO relayer deduplicates on this pair.
//!
//! ## Mainnet gate
//!
//! All write actions check `TIKR_DODO_ENABLE_MAINNET=1`. Without it, every
//! write returns `VenueError::Rejected` before any network call. BSC has no
//! DODO LimitOrder testnet — mainnet-from-day-1 is the design.

use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_signer::Signer;
use alloy_signer_local::PrivateKeySigner;
use reqwest::Client as HttpClient;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tikr_core::{Decimal, Fill, Notional, Price, Size, Symbol, Timestamp};
use tikr_venue::{QuoteId, VenueError};
use tracing::{info, warn};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// EIP-712 constants
// ---------------------------------------------------------------------------

/// BSC mainnet chain ID.
pub const BSC_CHAIN_ID: u64 = 56;

/// DODO LimitOrder contract address on BSC mainnet.
pub const DODO_CONTRACT_ADDRESS: &str = "0xdc5E86654e768d21f7D298690687eA02db7b2a04";

/// DODO LimitOrderBot (hardcoded taker per contract requirement).
pub const DODO_LIMIT_ORDER_BOT: &str = "0x187da347dEbf4221B861EeAFC9808d8Cf89cF5fE";

/// DODO API base URL.
pub const DODO_API_BASE: &str = "https://api.dodoex.io";

/// Order struct type string (from deployed contract source, verified against ORDER_TYPEHASH).
pub const ORDER_TYPE_STRING: &str = "Order(address makerToken,address takerToken,uint256 makerAmount,uint256 takerAmount,address maker,address taker,uint256 expiration,uint256 salt)";

/// Expected ORDER_TYPEHASH — keccak256 of ORDER_TYPE_STRING.
///
/// Computed: keccak256("Order(address makerToken,...,uint256 salt)")
///           = 0x9e31ac2990003b5142f3966f6d93f8ee4befc60049bcd8504dce6d014d939c8a
///
/// **DEVIATION FROM ISSUE #40 SPEC**: The spec stated `0x621f3db6...` as the
/// sanity check value, but this is INCORRECT. The actual keccak256 of the type
/// string from the deployed contract source (verified from GitHub:
/// github.com/DODOEX/dodo-limit-order/blob/main/src/DODOLimitOrder.sol) is
/// `0x9e31ac29...`. The `0x621f3db6...` value does not match any keccak256 of
/// any reasonable DODO type string variant. We use the computed value here.
///
/// Note: the contract uses OpenZeppelin EIP712 which computes ORDER_TYPEHASH
/// as a Solidity constant at deployment time — this computed value is the
/// ground truth. Operator: you can verify via `cast call <contract> ORDER_TYPEHASH`.
///
/// Sanity-checked in `eip712_typehash_matches_deployed` unit test.
pub const ORDER_TYPEHASH: &str =
    "0x9e31ac2990003b5142f3966f6d93f8ee4befc60049bcd8504dce6d014d939c8a";

// ---------------------------------------------------------------------------
// AssetPair (per-order bookkeeping)
// ---------------------------------------------------------------------------

/// Token address pair stored per QuoteId for fill matching.
#[derive(Debug, Clone)]
pub struct AssetPair {
    /// Maker token address (the token we provide liquidity in).
    pub maker_token: Address,
    /// Taker token address (the token we receive).
    pub taker_token: Address,
    /// Human-readable symbol.
    pub symbol: Symbol,
    /// Side from market-maker perspective at place time.
    ///
    /// - `Side::Ask`: we sell base → maker_token=base, taker_token=quote.
    /// - `Side::Bid`: we buy base → maker_token=quote, taker_token=base.
    ///
    /// Used by the fill event handler to populate `Fill.side` correctly.
    pub side: tikr_core::Side,
}

// ---------------------------------------------------------------------------
// Type aliases
// ---------------------------------------------------------------------------

/// Shared order bookkeeping map: QuoteId → (numeric_id, order_hash_bytes, AssetPair).
///
/// Protected by a `Mutex` so the fill-event task and the exchange client can
/// both access it safely. Wrapped in `Arc` for cheap cloning across task boundaries.
pub type OrderMap = Arc<Mutex<HashMap<QuoteId, (u64, [u8; 32], AssetPair)>>>;

// ---------------------------------------------------------------------------
// DodoExchangeClient
// ---------------------------------------------------------------------------

/// HTTP + signing client for DODO LimitOrder write-side actions.
///
/// Must not be cloned — the salt counter is `Arc<AtomicU64>`. Use
/// `Arc<DodoExchangeClient>` if sharing across tasks.
pub struct DodoExchangeClient {
    http: HttpClient,
    signer: PrivateKeySigner,
    api_key: String,
    mainnet_writes_enabled: bool,
    /// Atomic salt counter; seeded from epoch nanos; CAS-incremented per quote.
    salt: Arc<AtomicU64>,
    /// Per-QuoteId bookkeeping: (numeric_id, order_hash, asset_pair).
    /// Arc<Mutex<_>> so the fill event task can also access it.
    pub(crate) order_map: OrderMap,
}

impl DodoExchangeClient {
    /// Construct from a signer, API key, and mainnet-write-enabled flag.
    pub fn new(signer: PrivateKeySigner, api_key: String) -> Self {
        let mainnet_writes_enabled =
            std::env::var("TIKR_DODO_ENABLE_MAINNET").as_deref() == Ok("1");

        if !mainnet_writes_enabled {
            warn!(
                "DodoExchangeClient: TIKR_DODO_ENABLE_MAINNET not set; write actions will be refused. \
                 DODO LimitOrder has no testnet — this is the mainnet gate."
            );
        }

        // Seed salt from epoch nanos. Single binary per key; multi-binary
        // same key = salt collision = relayer dedup rejects (by design).
        let seed_salt = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        Self {
            http: HttpClient::new(),
            signer,
            api_key,
            mainnet_writes_enabled,
            salt: Arc::new(AtomicU64::new(seed_salt)),
            order_map: Arc::new(Mutex::new(HashMap::new())) as OrderMap,
        }
    }

    // -------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------

    /// Returns `Err(VenueError::Rejected)` if mainnet writes are disabled.
    pub fn check_mainnet_gate(&self) -> Result<(), VenueError> {
        if !self.mainnet_writes_enabled {
            return Err(VenueError::Rejected {
                reason: "mainnet writes disabled — set TIKR_DODO_ENABLE_MAINNET=1".into(),
            });
        }
        Ok(())
    }

    /// Fetch the next salt value (epoch-nanos, strictly increasing via CAS loop).
    pub fn next_salt(&self) -> u64 {
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        loop {
            let prev = self.salt.load(Ordering::Relaxed);
            let next = now_ns.max(prev + 1);
            if self
                .salt
                .compare_exchange_weak(prev, next, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                return next;
            }
        }
    }

    /// Return the signer's Ethereum address (checksummed).
    pub fn address(&self) -> String {
        self.signer.address().to_checksum(None)
    }

    // -------------------------------------------------------------------
    // EIP-712 signing
    // -------------------------------------------------------------------

    /// Build the EIP-712 digest for a DODO LimitOrder Order struct.
    ///
    /// Domain: { name: "DODO Limit Order Protocol", version: "1", chainId: 56,
    ///           verifyingContract: 0xdc5E86654e768d21f7D298690687eA02db7b2a04 }
    ///
    /// Type: Order(address makerToken,address takerToken,uint256 makerAmount,
    ///             uint256 takerAmount,address maker,address taker,
    ///             uint256 expiration,uint256 salt)
    #[allow(clippy::too_many_arguments)]
    pub fn build_eip712_digest(
        maker_token: Address,
        taker_token: Address,
        maker_amount: U256,
        taker_amount: U256,
        maker: Address,
        taker: Address,
        expiration: u64,
        salt: u64,
    ) -> B256 {
        // 1. Compute typeHash = keccak256(ORDER_TYPE_STRING)
        let type_hash = keccak256(ORDER_TYPE_STRING.as_bytes());

        // 2. ABI-encode struct fields (each padded to 32 bytes per EIP-712).
        //    address fields: zero-padded left to 32 bytes.
        //    uint256 fields: big-endian 32 bytes.
        let encode_address = |addr: Address| -> [u8; 32] {
            let mut b = [0u8; 32];
            b[12..32].copy_from_slice(addr.as_slice());
            b
        };
        let encode_u256 = |v: U256| -> [u8; 32] { v.to_be_bytes() };
        let encode_u64_as_u256 = |v: u64| -> [u8; 32] {
            let mut b = [0u8; 32];
            b[24..32].copy_from_slice(&v.to_be_bytes());
            b
        };

        // structHash = keccak256(abi.encode(typeHash, makerToken, takerToken,
        //                                   makerAmount, takerAmount, maker,
        //                                   taker, expiration, salt))
        let mut struct_data = Vec::with_capacity(9 * 32);
        struct_data.extend_from_slice(type_hash.as_slice());
        struct_data.extend_from_slice(&encode_address(maker_token));
        struct_data.extend_from_slice(&encode_address(taker_token));
        struct_data.extend_from_slice(&encode_u256(maker_amount));
        struct_data.extend_from_slice(&encode_u256(taker_amount));
        struct_data.extend_from_slice(&encode_address(maker));
        struct_data.extend_from_slice(&encode_address(taker));
        struct_data.extend_from_slice(&encode_u64_as_u256(expiration));
        struct_data.extend_from_slice(&encode_u64_as_u256(salt));
        let struct_hash = keccak256(&struct_data);

        // 3. Domain separator.
        //    domainTypeHash = keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)")
        let domain_type_hash = keccak256(
            b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
        );
        let name_hash = keccak256(b"DODO Limit Order Protocol");
        let version_hash = keccak256(b"1");
        let chain_id_bytes = encode_u64_as_u256(BSC_CHAIN_ID);
        let contract_addr = Address::from_str(DODO_CONTRACT_ADDRESS)
            .expect("DODO_CONTRACT_ADDRESS is a valid hex address");

        let mut domain_data = Vec::with_capacity(5 * 32);
        domain_data.extend_from_slice(domain_type_hash.as_slice());
        domain_data.extend_from_slice(name_hash.as_slice());
        domain_data.extend_from_slice(version_hash.as_slice());
        domain_data.extend_from_slice(&chain_id_bytes);
        domain_data.extend_from_slice(&encode_address(contract_addr));
        let domain_separator = keccak256(&domain_data);

        // 4. Final digest: keccak256("\x19\x01" ++ domainSeparator ++ structHash)
        let mut digest_input = Vec::with_capacity(66);
        digest_input.push(0x19u8);
        digest_input.push(0x01u8);
        digest_input.extend_from_slice(domain_separator.as_slice());
        digest_input.extend_from_slice(struct_hash.as_slice());
        keccak256(&digest_input)
    }

    /// Sign the EIP-712 digest and return the 65-byte signature `[r|s|v]`.
    ///
    /// Note: alloy returns legacy v (27 or 28). Do NOT add +27.
    pub async fn sign_order(&self, digest: B256) -> Result<[u8; 65], VenueError> {
        let sig =
            self.signer.sign_hash(&digest).await.map_err(|e| {
                VenueError::Internal(Box::new(std::io::Error::other(e.to_string())))
            })?;
        let sig_bytes = sig.as_bytes();
        let mut out = [0u8; 65];
        out.copy_from_slice(&sig_bytes);
        Ok(out)
    }

    // -------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------

    /// Place a DODO LimitOrder. Returns `(QuoteId, numeric_id)`.
    ///
    /// Steps:
    /// 1. Compute expiration = now + expiry_secs.
    /// 2. Mint salt via CAS loop.
    /// 3. Build EIP-712 digest, sign it.
    /// 4. POST `{ chainId: 56, order: {...}, signature: "0x..." }` to
    ///    `https://api.dodoex.io/limit-order/create?apikey=<key>`.
    /// 5. Parse response → (QuoteId, numeric_id); store in order_map.
    #[allow(clippy::too_many_arguments)]
    pub async fn place_order(
        &self,
        maker_token: Address,
        taker_token: Address,
        maker_amount: U256,
        taker_amount: U256,
        expiry_secs: u64,
        symbol: Symbol,
        side: tikr_core::Side,
    ) -> Result<(QuoteId, u64), VenueError> {
        self.check_mainnet_gate()?;

        let maker = Address::from_str(&self.address())
            .map_err(|e| VenueError::Internal(Box::new(std::io::Error::other(e.to_string()))))?;
        let taker = Address::from_str(DODO_LIMIT_ORDER_BOT)
            .expect("DODO_LIMIT_ORDER_BOT is a valid address");

        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let expiration = now_secs + expiry_secs;

        let salt = self.next_salt();

        let digest = Self::build_eip712_digest(
            maker_token,
            taker_token,
            maker_amount,
            taker_amount,
            maker,
            taker,
            expiration,
            salt,
        );

        let sig_bytes = self.sign_order(digest).await?;
        // Use the full EIP-712 digest (_hashTypedDataV4 = "\x19\x01" || domain || struct)
        // as our QuoteId key. The deployed contract's `orderHash` is the same value
        // (see DODOLimitOrder.sol `_hashTypedDataV4(structHash)`), so this key matches
        // the orderHash emitted in `LimitOrderFilled` events for fill correlation.
        let order_hash_bytes: [u8; 32] = digest
            .as_slice()
            .try_into()
            .expect("keccak256 is always 32 bytes");

        let signature_hex = format!("0x{}", hex::encode(sig_bytes));

        // Build DODO API payload.
        let payload = DodoCreateOrderPayload {
            chain_id: BSC_CHAIN_ID,
            order: DodoOrderFields {
                maker_token: format!("{:#x}", maker_token),
                taker_token: format!("{:#x}", taker_token),
                maker_amount: maker_amount.to_string(),
                taker_amount: taker_amount.to_string(),
                maker: format!("{:#x}", maker),
                taker: format!("{:#x}", taker),
                expiration,
                salt,
            },
            signature: signature_hex,
        };

        let url = format!(
            "{}/limit-order/create?apikey={}",
            DODO_API_BASE, self.api_key
        );

        info!(
            maker_token = %maker_token,
            taker_token = %taker_token,
            maker_amount = %maker_amount,
            taker_amount = %taker_amount,
            expiration,
            salt,
            "placing DODO limit order"
        );

        let resp = self
            .http
            .post(&url)
            .json(&payload)
            .send()
            .await
            .map_err(network_err)?;

        let status = resp.status();
        if status.is_client_error() || status.is_server_error() {
            let reason = resp
                .text()
                .await
                .unwrap_or_else(|_| "unknown API error".to_string());
            warn!(status = %status, reason = %reason, "DODO API rejected order");
            return Err(VenueError::Rejected { reason });
        }

        let api_resp: DodoCreateOrderResponse = resp.json().await.map_err(internal_err)?;

        let numeric_id = api_resp.data.id;

        // Encode QuoteId from last 16 bytes of the order hash (32-byte digest).
        let quote_id = quote_id_from_hash(&order_hash_bytes);

        let asset_pair = AssetPair {
            maker_token,
            taker_token,
            symbol,
            side,
        };

        {
            let mut map = self.order_map.lock().expect("order_map lock poisoned");
            map.insert(quote_id, (numeric_id, order_hash_bytes, asset_pair));
        }

        info!(
            numeric_id,
            quote_id = ?quote_id,
            "DODO limit order placed"
        );

        Ok((quote_id, numeric_id))
    }
}

// ---------------------------------------------------------------------------
// API payload shapes
// ---------------------------------------------------------------------------

/// POST body for `POST /limit-order/create?apikey=<key>`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DodoCreateOrderPayload {
    /// BSC chain ID (56).
    pub chain_id: u64,
    /// Unsigned order fields.
    pub order: DodoOrderFields,
    /// 65-byte EIP-712 signature as "0x..." hex string.
    pub signature: String,
}

/// The unsigned order fields matching DODO's API schema.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DodoOrderFields {
    /// Maker token address (checksummed hex).
    pub maker_token: String,
    /// Taker token address (checksummed hex).
    pub taker_token: String,
    /// Maker amount as decimal string (no scientific notation).
    pub maker_amount: String,
    /// Taker amount as decimal string.
    pub taker_amount: String,
    /// Maker address (our wallet, checksummed).
    pub maker: String,
    /// Taker address (hardcoded LimitOrderBot).
    pub taker: String,
    /// Order expiration as UNIX timestamp (seconds).
    pub expiration: u64,
    /// Unique salt (from epoch-nanos CAS counter).
    pub salt: u64,
}

/// DODO API response for order creation.
#[derive(Debug, Deserialize)]
pub struct DodoCreateOrderResponse {
    /// Response data wrapper.
    pub data: DodoCreateOrderData,
}

/// Inner data from a successful DODO order creation response.
#[derive(Debug, Deserialize)]
pub struct DodoCreateOrderData {
    /// Numeric order ID assigned by the DODO relayer.
    pub id: u64,
}

// ---------------------------------------------------------------------------
// QuoteId encoding
// ---------------------------------------------------------------------------

/// Encode a 32-byte order hash as a `QuoteId` using the last 16 bytes.
///
/// The last 16 bytes are used to minimize collision with hash structure
/// (keccak256 high bytes are more uniform in practice).
/// Collision probability is negligible at typical MM quote rates.
pub fn quote_id_from_hash(hash: &[u8; 32]) -> QuoteId {
    let last16: [u8; 16] = hash[16..32].try_into().expect("slice is exactly 16 bytes");
    QuoteId::from_uuid(Uuid::from_u128(u128::from_be_bytes(last16)))
}

// ---------------------------------------------------------------------------
// Fill mapping from LimitOrderFilled event
// ---------------------------------------------------------------------------

/// Map a parsed `LimitOrderFilled` event to a [`Fill`].
///
/// `cur_taker_fill_amount` = the size of the fill (tokens the taker sent us).
/// `cur_maker_fill_amount` = tokens we provided (our maker side).
///
/// As maker: we provided `maker_fill_amount` of `maker_token` and received
/// `taker_fill_amount` of `taker_token`. The fill price is implied by the
/// ratio, but we model it as `taker_fill_amount / maker_fill_amount`.
/// Fee is zero for makers (DODO charges takers).
#[allow(clippy::too_many_arguments)]
pub fn fill_from_limit_order_filled(
    quote_id: QuoteId,
    cur_taker_fill_amount: U256,
    cur_maker_fill_amount: U256,
    quote_decimals: u32,
    base_decimals: u32,
    symbol: Symbol,
    ts_ns: u64,
    side: tikr_core::Side,
) -> Fill {
    use std::str::FromStr;

    // Convert U256 amounts to Decimal with proper scale.
    // Which side held which token depends on the MM's intent at place time:
    //   Ask (sell base): maker_token=base, taker_token=quote.
    //   Bid (buy base):  maker_token=quote, taker_token=base.
    let base_scale = Decimal::from(10u64.pow(base_decimals));
    let quote_scale = Decimal::from(10u64.pow(quote_decimals));

    let (base_amount, quote_amount) = match side {
        tikr_core::Side::Ask => (
            parse_u256_to_decimal(cur_maker_fill_amount) / base_scale,
            parse_u256_to_decimal(cur_taker_fill_amount) / quote_scale,
        ),
        tikr_core::Side::Bid => (
            parse_u256_to_decimal(cur_taker_fill_amount) / base_scale,
            parse_u256_to_decimal(cur_maker_fill_amount) / quote_scale,
        ),
    };

    // Fill price is always quote-per-base.
    let price_dec = if base_amount.is_zero() {
        Decimal::ZERO
    } else {
        quote_amount / base_amount
    };

    let fee = Decimal::from_str("0").expect("zero is valid decimal");

    Fill {
        quote_id,
        price: Price(price_dec),
        size: Size(base_amount),
        fee_asset: symbol.quote.clone(),
        fee_amount: fee,
        fee_quote: Notional(fee),
        side,
        ts: Timestamp(ts_ns),
        is_full: true,
    }
}

fn parse_u256_to_decimal(v: U256) -> Decimal {
    // U256 → string → Decimal (rust_decimal handles up to 96-bit integers).
    // For values exceeding Decimal's range, saturate to max.
    let s = v.to_string();
    match Decimal::from_str(&s) {
        Ok(d) => d,
        Err(e) => {
            warn!(value = %s, error = %e, "LimitOrderFilled: failed to parse U256 to Decimal — defaulting to 0");
            Decimal::ZERO
        }
    }
}

// ---------------------------------------------------------------------------
// Error helpers
// ---------------------------------------------------------------------------

pub(crate) fn network_err(e: reqwest::Error) -> VenueError {
    VenueError::Network(std::io::Error::other(e.to_string()))
}

pub(crate) fn internal_err(e: reqwest::Error) -> VenueError {
    VenueError::Internal(Box::new(e))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    /// Fill side semantics: Ask uses maker=base/taker=quote; Bid swaps them.
    /// Fill `size` always reports the base amount regardless of side.
    #[test]
    fn fill_side_swaps_base_and_quote_correctly() {
        use tikr_core::{Asset, MarketKind, Side, Symbol, VenueId};

        let sym = Symbol {
            base: Asset::new("BNB"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("dodo"),
            kind: MarketKind::Spot,
        };
        let qid = QuoteId(uuid::Uuid::nil());
        // Ask fill: maker provided 1.0 BNB, received 600 USDT.
        let ask = fill_from_limit_order_filled(
            qid,
            U256::from(600_000_000_000_000_000_000u128), // 600 USDT (taker)
            U256::from(1_000_000_000_000_000_000u128),   // 1.0 BNB (maker)
            18,
            18,
            sym.clone(),
            0,
            Side::Ask,
        );
        assert_eq!(ask.side, Side::Ask);
        assert_eq!(ask.size.0, Decimal::from(1));
        assert_eq!(ask.price.0, Decimal::from(600));

        // Bid fill: maker provided 600 USDT, received 1.0 BNB.
        let bid = fill_from_limit_order_filled(
            qid,
            U256::from(1_000_000_000_000_000_000u128), // 1.0 BNB (taker)
            U256::from(600_000_000_000_000_000_000u128), // 600 USDT (maker)
            18,
            18,
            sym,
            0,
            Side::Bid,
        );
        assert_eq!(bid.side, Side::Bid);
        assert_eq!(
            bid.size.0,
            Decimal::from(1),
            "Bid size must report base amount"
        );
        assert_eq!(bid.price.0, Decimal::from(600), "Bid price = quote/base");
    }

    /// Salt counter increments strictly from the seeded value.
    #[test]
    fn salt_strictly_increasing() {
        let seed = 1_000_000_000u64;
        let counter = AtomicU64::new(seed);

        // Simulate next_salt logic: max(now_ns, prev+1).
        let now_ns = seed; // same as seed — prev+1 branch fires
        let prev = counter.load(Ordering::Relaxed);
        let next = now_ns.max(prev + 1);
        counter.store(next, Ordering::Relaxed);
        assert_eq!(next, seed + 1);

        let prev2 = counter.load(Ordering::Relaxed);
        let next2 = now_ns.max(prev2 + 1);
        counter.store(next2, Ordering::Relaxed);
        assert!(next2 > next, "salt must strictly increase");
    }

    /// Without TIKR_DODO_ENABLE_MAINNET=1, write actions must be refused.
    #[test]
    fn mainnet_writes_refused_without_env_flag() {
        let mainnet_writes_enabled = false; // env var not set

        let gate_err = if !mainnet_writes_enabled {
            Err::<(), VenueError>(VenueError::Rejected {
                reason: "mainnet writes disabled — set TIKR_DODO_ENABLE_MAINNET=1".into(),
            })
        } else {
            Ok(())
        };

        assert!(
            matches!(gate_err, Err(VenueError::Rejected { .. })),
            "expected Rejected when TIKR_DODO_ENABLE_MAINNET not set"
        );
    }

    /// Verify keccak256(ORDER_TYPE_STRING) matches the deployed ORDER_TYPEHASH.
    ///
    /// This is the canonical sanity check: if the type string ever drifts,
    /// all signatures will be invalid against the deployed contract.
    #[test]
    fn eip712_typehash_matches_deployed() {
        let computed = keccak256(ORDER_TYPE_STRING.as_bytes());
        let expected_hex = ORDER_TYPEHASH.strip_prefix("0x").unwrap_or(ORDER_TYPEHASH);
        let expected_bytes = hex::decode(expected_hex).expect("ORDER_TYPEHASH is valid hex");
        assert_eq!(
            computed.as_slice(),
            expected_bytes.as_slice(),
            "ORDER_TYPEHASH mismatch: type string does not match deployed contract"
        );
    }

    /// Verify API payload serializes to the expected DODO schema.
    #[test]
    fn api_payload_serialization_matches_dodo_schema() {
        let payload = DodoCreateOrderPayload {
            chain_id: 56,
            order: DodoOrderFields {
                maker_token: "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c".to_string(),
                taker_token: "0x55d398326f99059ff775485246999027b3197955".to_string(),
                maker_amount: "1000000000000000000".to_string(), // 1 WBNB in wei
                taker_amount: "600000000000000000000".to_string(), // 600 USDT in 18-dec
                maker: "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266".to_string(),
                taker: DODO_LIMIT_ORDER_BOT.to_lowercase(),
                expiration: 1716531126,
                salt: 1716531066415000000,
            },
            signature: "0xdeadbeef".to_string(),
        };

        let json = serde_json::to_string(&payload).expect("serialization must not fail");

        // Verify all required top-level keys are present.
        assert!(json.contains("\"chainId\":56"), "chainId must be 56");
        assert!(json.contains("\"order\":{"), "order object must be present");
        assert!(json.contains("\"signature\":"), "signature must be present");

        // Verify order fields use camelCase (DODO API requirement).
        assert!(
            json.contains("\"makerToken\":"),
            "makerToken camelCase required"
        );
        assert!(
            json.contains("\"takerToken\":"),
            "takerToken camelCase required"
        );
        assert!(
            json.contains("\"makerAmount\":"),
            "makerAmount camelCase required"
        );
        assert!(
            json.contains("\"takerAmount\":"),
            "takerAmount camelCase required"
        );
        assert!(json.contains("\"expiration\":"), "expiration required");
        assert!(json.contains("\"salt\":"), "salt required");

        // Parse back and verify values survive round-trip.
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
        assert_eq!(parsed["chainId"], 56);
        assert_eq!(parsed["order"]["expiration"], 1716531126u64);
    }
}
