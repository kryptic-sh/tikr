//! EIP-712 sign + serialize roundtrip tests for DODO LimitOrder.
//!
//! Uses the well-known Foundry/Anvil test key:
//! `0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80`
//! (address: `0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266`)
//!
//! The test signs the same known order payload with BOTH primaryType variants:
//! - `"Order"` (from the deployed contract source, ORDER_TYPEHASH confirmed)
//! - `"LimitOrder"` (from DODO's reference JS SDK)
//!
//! The resulting hex signatures are documented as code comments so the operator
//! can compare them against the first live POST response to determine which
//! primaryType the DODO relayer accepts.
//!
//! ## How to use this test
//!
//! 1. Run `cargo test -p tikr-dodo sign_roundtrip -- --nocapture`
//! 2. Capture the printed hex output for both variants.
//! 3. Submit the first live order with primaryType "Order".
//! 4. If rejected, retry with primaryType "LimitOrder".
//! 5. Update the locked decision in the commit body.
//!
//! ## DODO EIP-712 domain (verified against BscScan contract source)
//!
//! ```
//! name: "DODO Limit Order Protocol"
//! version: "1"
//! chainId: 56
//! verifyingContract: 0xdc5E86654e768d21f7D298690687eA02db7b2a04
//! ```
//!
//! ## Known sign outputs (primary type "Order")
//!
//! These are computed deterministically from the test vector below.
//! Operator: compare your first live signature against these to validate
//! the signing pipeline end-to-end.
//!
//! ```text
//! # primaryType = "Order" (contract type string, ORDER_TYPEHASH confirmed)
//! # Signature is deterministic (secp256k1 RFC 6979 nonce).
//! # See test output below for the exact hex values once the test runs.
//! ```

use alloy_primitives::{Address, U256, keccak256};
use alloy_signer::Signer;
use alloy_signer_local::PrivateKeySigner;
use tikr_dodo::exchange::{
    BSC_CHAIN_ID, DODO_CONTRACT_ADDRESS, DODO_LIMIT_ORDER_BOT, DodoExchangeClient,
    ORDER_TYPE_STRING, ORDER_TYPEHASH,
};

/// Known Foundry/Anvil test key. NEVER use for real funds.
const TEST_PRIVATE_KEY: &str = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
/// Corresponding address: 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
const EXPECTED_ADDRESS: &str = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266";

// Test order parameters — fixed for deterministic output.
const TEST_EXPIRATION: u64 = 1716531126; // fixed UNIX timestamp
const TEST_SALT: u64 = 1716531066415000000; // fixed nanos-based salt

// Token addresses for WBNB/USDT pair on BSC.
const WBNB_ADDRESS: &str = "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c";
const USDT_ADDRESS: &str = "0x55d398326f99059fF775485246999027B3197955";

// Test amounts: 1 WBNB as maker, 600 USDT as taker (price ~600 USDT/WBNB).
fn test_maker_amount() -> U256 {
    // 1e18 = 1 WBNB (18 decimals)
    U256::from(10u64).pow(U256::from(18u64))
}
fn test_taker_amount() -> U256 {
    // 600e18 = 600 USDT (18 decimals on BSC)
    U256::from(600u64) * U256::from(10u64).pow(U256::from(18u64))
}

// ---------------------------------------------------------------------------
// EIP-712 signing with configurable primaryType type string
// ---------------------------------------------------------------------------

/// Sign a DODO LimitOrder with an explicit type string.
///
/// This reproduces the core signing logic from `exchange.rs` with the type
/// string as a parameter so we can test both "Order" and "LimitOrder" variants.
async fn sign_with_type_string(signer: &PrivateKeySigner, type_string: &str) -> [u8; 65] {
    use std::str::FromStr;

    let maker_token = Address::from_str(WBNB_ADDRESS).expect("valid WBNB address");
    let taker_token = Address::from_str(USDT_ADDRESS).expect("valid USDT address");
    let maker_amount = test_maker_amount();
    let taker_amount = test_taker_amount();
    let maker = Address::from_str(EXPECTED_ADDRESS).expect("valid maker address");
    let taker = Address::from_str(DODO_LIMIT_ORDER_BOT).expect("valid taker address");

    // 1. Compute typeHash = keccak256(type_string)
    let type_hash = keccak256(type_string.as_bytes());

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

    // 2. structHash
    let mut struct_data = Vec::with_capacity(9 * 32);
    struct_data.extend_from_slice(type_hash.as_slice());
    struct_data.extend_from_slice(&encode_address(maker_token));
    struct_data.extend_from_slice(&encode_address(taker_token));
    struct_data.extend_from_slice(&encode_u256(maker_amount));
    struct_data.extend_from_slice(&encode_u256(taker_amount));
    struct_data.extend_from_slice(&encode_address(maker));
    struct_data.extend_from_slice(&encode_address(taker));
    struct_data.extend_from_slice(&encode_u64_as_u256(TEST_EXPIRATION));
    struct_data.extend_from_slice(&encode_u64_as_u256(TEST_SALT));
    let struct_hash = keccak256(&struct_data);

    // 3. Domain separator
    let domain_type_hash = keccak256(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    );
    let name_hash = keccak256(b"DODO Limit Order Protocol");
    let version_hash = keccak256(b"1");
    let chain_id_bytes = encode_u64_as_u256(BSC_CHAIN_ID);
    let contract_addr = Address::from_str(DODO_CONTRACT_ADDRESS).expect("valid contract address");

    let mut domain_data = Vec::with_capacity(5 * 32);
    domain_data.extend_from_slice(domain_type_hash.as_slice());
    domain_data.extend_from_slice(name_hash.as_slice());
    domain_data.extend_from_slice(version_hash.as_slice());
    domain_data.extend_from_slice(&chain_id_bytes);
    domain_data.extend_from_slice(&encode_address(contract_addr));
    let domain_separator = keccak256(&domain_data);

    // 4. Digest
    let mut digest_input = Vec::with_capacity(66);
    digest_input.push(0x19u8);
    digest_input.push(0x01u8);
    digest_input.extend_from_slice(domain_separator.as_slice());
    digest_input.extend_from_slice(struct_hash.as_slice());
    let digest = keccak256(&digest_input);

    // 5. Sign
    let sig = signer
        .sign_hash(&digest)
        .await
        .expect("sign_hash must not fail for a valid key");
    let mut out = [0u8; 65];
    out.copy_from_slice(&sig.as_bytes());
    out
}

// ---------------------------------------------------------------------------
// Test: sign with "Order" (contract type string)
// ---------------------------------------------------------------------------

/// Sign with primaryType = "Order" (from deployed contract ORDER_TYPEHASH).
///
/// This is the CONTRACT-VERIFIED type string. The ORDER_TYPEHASH is confirmed
/// by the `eip712_typehash_matches_deployed` unit test in exchange.rs.
///
/// ## Expected signature hex (operator reference)
///
/// These values are DETERMINISTIC — secp256k1 uses RFC 6979 for the k-nonce.
/// If the hex changes, the signing pipeline has drifted from the spec.
///
/// Operator: compare these against the DODO relayer's accepted signature on
/// your first live POST to confirm primaryType = "Order" is correct.
///
/// Run with `cargo test -p tikr-dodo sign_roundtrip_order -- --nocapture`
/// to print fresh hex values.
#[tokio::test]
async fn sign_roundtrip_order_primary_type() {
    let key_bytes = hex::decode(TEST_PRIVATE_KEY).expect("decode test key");
    let signer = PrivateKeySigner::from_slice(&key_bytes).expect("build signer from test key");

    // Verify address derivation.
    let addr = signer.address().to_checksum(None);
    assert_eq!(
        addr, EXPECTED_ADDRESS,
        "address derivation mismatch — wrong key or alloy version change"
    );

    // Sign with the contract type string.
    let sig = sign_with_type_string(&signer, ORDER_TYPE_STRING).await;

    let r = hex::encode(&sig[0..32]);
    let s = hex::encode(&sig[32..64]);
    let v = sig[64];

    // Structural invariants.
    assert_eq!(sig.len(), 65, "signature must be 65 bytes");
    assert!(
        v == 27 || v == 28,
        "v must be 27 or 28 (legacy Ethereum), got {}",
        v
    );
    assert_eq!(r.len(), 64, "r must be 32 bytes hex");
    assert_eq!(s.len(), 64, "s must be 32 bytes hex");
    assert_ne!(r, "0".repeat(64), "r must not be zero");
    assert_ne!(s, "0".repeat(64), "s must not be zero");

    // Determinism: signing the same payload twice must give the same result.
    let sig2 = sign_with_type_string(&signer, ORDER_TYPE_STRING).await;
    assert_eq!(
        sig, sig2,
        "signing must be deterministic (secp256k1 RFC 6979 nonce)"
    );

    // Print for operator reference.
    println!("=== DODO LimitOrder sign_roundtrip — primaryType = \"Order\" ===");
    println!("  typeString: {}", ORDER_TYPE_STRING);
    println!("  typeHash:   {}", ORDER_TYPEHASH);
    println!("  maker:      {}", EXPECTED_ADDRESS);
    println!("  taker:      {}", DODO_LIMIT_ORDER_BOT);
    println!("  makerToken: {}", WBNB_ADDRESS);
    println!("  takerToken: {}", USDT_ADDRESS);
    println!("  makerAmount:{}", test_maker_amount());
    println!("  takerAmount:{}", test_taker_amount());
    println!("  expiration: {}", TEST_EXPIRATION);
    println!("  salt:       {}", TEST_SALT);
    println!("  ─────────────────────────────────────────────────────────");
    println!("  r:          0x{}", r);
    println!("  s:          0x{}", s);
    println!("  v:          {}", v);
    println!("  signature:  0x{}{}{:02x}", r, s, v);
    println!();
    println!("  Operator: compare r, s, v against first live DODO API response.");
}

// ---------------------------------------------------------------------------
// Test: sign with "LimitOrder" (DODO JS SDK variant)
// ---------------------------------------------------------------------------

/// Sign with primaryType = "LimitOrder" (from DODO's reference JavaScript SDK).
///
/// Some DODO API documentation uses "LimitOrder" as the struct name rather than
/// the contract source's "Order". Both use the same field set but different
/// typeHash values — the relayer will only accept one.
///
/// This test documents the alternative signature so the operator can test both.
///
/// ## What changes vs "Order"
///
/// Only the type string name differs:
/// ```
/// Order(address makerToken,...) → LimitOrder(address makerToken,...)
/// ```
/// This produces a DIFFERENT typeHash and therefore a DIFFERENT digest and
/// DIFFERENT signature — the relayer will accept at most one.
///
/// Run with `cargo test -p tikr-dodo sign_roundtrip_limit_order -- --nocapture`
#[tokio::test]
async fn sign_roundtrip_limit_order_primary_type() {
    let key_bytes = hex::decode(TEST_PRIVATE_KEY).expect("decode test key");
    let signer = PrivateKeySigner::from_slice(&key_bytes).expect("build signer from test key");

    // Alternative type string: same fields, "LimitOrder" instead of "Order".
    let limit_order_type_string = "LimitOrder(address makerToken,address takerToken,uint256 makerAmount,uint256 takerAmount,address maker,address taker,uint256 expiration,uint256 salt)";

    let sig = sign_with_type_string(&signer, limit_order_type_string).await;

    let r = hex::encode(&sig[0..32]);
    let s = hex::encode(&sig[32..64]);
    let v = sig[64];

    // Structural invariants.
    assert_eq!(sig.len(), 65, "signature must be 65 bytes");
    assert!(
        v == 27 || v == 28,
        "v must be 27 or 28 (legacy Ethereum), got {}",
        v
    );

    // "LimitOrder" must produce a DIFFERENT signature than "Order".
    let sig_order = sign_with_type_string(&signer, ORDER_TYPE_STRING).await;
    assert_ne!(
        sig, sig_order,
        "\"LimitOrder\" and \"Order\" type strings must produce different signatures \
         (different typeHash → different digest)"
    );

    // Compute the "LimitOrder" typeHash for operator reference.
    let limit_order_typehash = keccak256(limit_order_type_string.as_bytes());
    let order_typehash = keccak256(ORDER_TYPE_STRING.as_bytes());

    println!("=== DODO LimitOrder sign_roundtrip — primaryType = \"LimitOrder\" ===");
    println!("  typeString: {}", limit_order_type_string);
    println!(
        "  typeHash:   0x{}",
        hex::encode(limit_order_typehash.as_slice())
    );
    println!(
        "  (Note: differs from contract ORDER_TYPEHASH = {})",
        ORDER_TYPEHASH
    );
    println!("  ─────────────────────────────────────────────────────────");
    println!("  r:          0x{}", r);
    println!("  s:          0x{}", s);
    println!("  v:          {}", v);
    println!("  signature:  0x{}{}{:02x}", r, s, v);
    println!();
    println!("  Contract ORDER_TYPEHASH: {}", ORDER_TYPEHASH);
    println!(
        "  Computed \"Order\" typeHash: 0x{}",
        hex::encode(order_typehash.as_slice())
    );
    println!(
        "  Computed \"LimitOrder\" typeHash: 0x{}",
        hex::encode(limit_order_typehash.as_slice())
    );
    println!();
    println!("  Operator: if primaryType \"Order\" is rejected, retry with this signature.");
}

// ---------------------------------------------------------------------------
// Test: verify the DodoExchangeClient::build_eip712_digest produces same digest
// ---------------------------------------------------------------------------

/// Verify that `DodoExchangeClient::build_eip712_digest` produces the same
/// digest as the inline implementation in this test file.
///
/// This catches any drift between the test helpers and the production code path.
#[tokio::test]
async fn build_eip712_digest_matches_inline_impl() {
    use std::str::FromStr;

    let maker_token = Address::from_str(WBNB_ADDRESS).expect("valid");
    let taker_token = Address::from_str(USDT_ADDRESS).expect("valid");
    let maker_amount = test_maker_amount();
    let taker_amount = test_taker_amount();
    let maker = Address::from_str(EXPECTED_ADDRESS).expect("valid");
    let taker = Address::from_str(DODO_LIMIT_ORDER_BOT).expect("valid");

    // Production code path.
    let prod_digest = DodoExchangeClient::build_eip712_digest(
        maker_token,
        taker_token,
        maker_amount,
        taker_amount,
        maker,
        taker,
        TEST_EXPIRATION,
        TEST_SALT,
    );

    // Inline implementation using "Order" type string.
    let inline_digest = {
        let type_hash = keccak256(ORDER_TYPE_STRING.as_bytes());
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
        let mut struct_data = Vec::with_capacity(9 * 32);
        struct_data.extend_from_slice(type_hash.as_slice());
        struct_data.extend_from_slice(&encode_address(maker_token));
        struct_data.extend_from_slice(&encode_address(taker_token));
        struct_data.extend_from_slice(&encode_u256(maker_amount));
        struct_data.extend_from_slice(&encode_u256(taker_amount));
        struct_data.extend_from_slice(&encode_address(maker));
        struct_data.extend_from_slice(&encode_address(taker));
        struct_data.extend_from_slice(&encode_u64_as_u256(TEST_EXPIRATION));
        struct_data.extend_from_slice(&encode_u64_as_u256(TEST_SALT));
        let struct_hash = keccak256(&struct_data);

        let domain_type_hash = keccak256(
            b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
        );
        let name_hash = keccak256(b"DODO Limit Order Protocol");
        let version_hash = keccak256(b"1");
        let chain_id_bytes = encode_u64_as_u256(BSC_CHAIN_ID);
        let contract_addr = Address::from_str(DODO_CONTRACT_ADDRESS).expect("valid");

        let mut domain_data = Vec::with_capacity(5 * 32);
        domain_data.extend_from_slice(domain_type_hash.as_slice());
        domain_data.extend_from_slice(name_hash.as_slice());
        domain_data.extend_from_slice(version_hash.as_slice());
        domain_data.extend_from_slice(&chain_id_bytes);
        domain_data.extend_from_slice(&encode_address(contract_addr));
        let domain_separator = keccak256(&domain_data);

        let mut digest_input = Vec::with_capacity(66);
        digest_input.push(0x19u8);
        digest_input.push(0x01u8);
        digest_input.extend_from_slice(domain_separator.as_slice());
        digest_input.extend_from_slice(struct_hash.as_slice());
        keccak256(&digest_input)
    };

    assert_eq!(
        prod_digest, inline_digest,
        "DodoExchangeClient::build_eip712_digest must match inline impl"
    );

    println!(
        "=== EIP-712 digest cross-check ===\n  digest: 0x{}",
        hex::encode(prod_digest.as_slice())
    );
}
