//! HMAC-SHA256 and Ed25519 sign roundtrip tests.
//!
//! HMAC vector: official Binance docs example.
//! Ed25519: deterministic keypair from a fixed 32-byte seed.
//!
//! Source:
//! https://binance-docs.github.io/apidocs/spot/en/#signed-trade-and-user_data-endpoint-security
//!
//! The same signing algorithm applies to both Spot and Futures — only the
//! REST path prefix differs (`/api/v3/...` vs `/fapi/v1/...`).
//!
//! ## Known HMAC fixture
//!
//! | Field | Value |
//! |-------|-------|
//! | Secret | `NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j` |
//! | Query | `symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559` |
//! | Expected signature | `c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71` |

use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use ed25519_dalek::{SigningKey, VerifyingKey};
use tikr_binance::sign::{BinanceKeyMaterial, sign_query, sign_query_dispatch, sign_query_ed25519};

// ---------------------------------------------------------------------------
// HMAC tests
// ---------------------------------------------------------------------------

/// The official Binance HMAC test vector.
const BINANCE_HMAC_SECRET: &str =
    "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j";

const BINANCE_HMAC_QUERY: &str = "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1\
     &recvWindow=5000&timestamp=1499827319559";

const BINANCE_HMAC_EXPECTED: &str =
    "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71";

/// Bit-exact match against the Binance docs HMAC example.
///
/// If this test fails, the signing implementation has drifted from the
/// Binance specification.
#[test]
fn hmac_matches_binance_official_docs_example() {
    let sig = sign_query(BINANCE_HMAC_SECRET, BINANCE_HMAC_QUERY);

    println!("=== Binance HMAC roundtrip ===");
    println!("secret  : {BINANCE_HMAC_SECRET}");
    println!("query   : {BINANCE_HMAC_QUERY}");
    println!("expected: {BINANCE_HMAC_EXPECTED}");
    println!("got     : {sig}");

    assert_eq!(
        sig, BINANCE_HMAC_EXPECTED,
        "HMAC signature must match the official Binance docs example (bit-exact)"
    );
}

/// Same algorithm, same inputs → deterministic.
#[test]
fn hmac_is_deterministic() {
    let sig1 = sign_query(BINANCE_HMAC_SECRET, BINANCE_HMAC_QUERY);
    let sig2 = sign_query(BINANCE_HMAC_SECRET, BINANCE_HMAC_QUERY);
    assert_eq!(sig1, sig2, "HMAC must be deterministic");
}

/// Signature is lowercase hex, 64 chars (32 bytes = 256 bits).
#[test]
fn hmac_signature_is_64_char_lowercase_hex() {
    let sig = sign_query(BINANCE_HMAC_SECRET, BINANCE_HMAC_QUERY);
    assert_eq!(sig.len(), 64, "SHA-256 hex must be 64 chars");
    assert!(
        sig.chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase()),
        "signature must be lowercase hex"
    );
}

/// Different secrets produce different signatures.
#[test]
fn different_secret_produces_different_signature() {
    let sig1 = sign_query(BINANCE_HMAC_SECRET, BINANCE_HMAC_QUERY);
    let sig2 = sign_query("completely-different-secret", BINANCE_HMAC_QUERY);
    assert_ne!(sig1, sig2, "different secret must change the signature");
}

// ---------------------------------------------------------------------------
// Ed25519 tests
// ---------------------------------------------------------------------------

/// Build a deterministic Ed25519 signing key from seed [42u8; 32].
fn test_signing_key() -> SigningKey {
    SigningKey::from_bytes(&[42u8; 32])
}

/// Ed25519 roundtrip: sign a payload, verify with the corresponding public key.
///
/// This is the primary correctness test: Binance will verify incoming
/// signatures with the public key the operator registered.
#[test]
fn ed25519_roundtrip_sign_and_verify() {
    let signing_key = test_signing_key();
    let verifying_key: VerifyingKey = (&signing_key).into();

    let payload = "symbol=BTCUSDT&side=BUY&type=LIMIT&timeInForce=GTX\
                   &quantity=0.001&price=30000&recvWindow=5000&timestamp=1699999999000";

    let sig_b64 = sign_query_ed25519(&signing_key, payload);

    println!("=== Ed25519 roundtrip ===");
    println!("payload : {payload}");
    println!("sig_b64 : {sig_b64}");
    println!("sig_len : {}", sig_b64.len());

    // Must be 88 chars (64 raw bytes → base64 standard with padding).
    assert_eq!(
        sig_b64.len(),
        88,
        "Ed25519 base64 signature must be 88 chars"
    );

    // Decode and verify.
    let raw = BASE64_STANDARD
        .decode(&sig_b64)
        .expect("must decode as base64 standard");
    assert_eq!(raw.len(), 64, "Ed25519 raw signature must be 64 bytes");

    let sig_arr: [u8; 64] = raw.try_into().expect("64 bytes");
    let signature = ed25519_dalek::Signature::from_bytes(&sig_arr);

    verifying_key
        .verify_strict(payload.as_bytes(), &signature)
        .expect("Ed25519 signature must verify against matching public key");
}

/// Ed25519 signature is base64, not hex.
///
/// HMAC uses hex; Ed25519 uses base64. The dispatch must not mix them.
#[test]
fn ed25519_signature_encoding_is_base64_not_hex() {
    let signing_key = test_signing_key();
    let sig = sign_query_ed25519(&signing_key, "symbol=BTCUSDT&timestamp=1234567890");

    // 88 chars: only base64 chars (alphanumeric + + / =).
    assert_eq!(sig.len(), 88);
    assert!(
        sig.chars()
            .all(|c| c.is_alphanumeric() || c == '+' || c == '/' || c == '='),
        "must be valid base64 standard characters; got: {sig}"
    );
    // Must NOT be 64-char lowercase hex.
    assert!(
        !(sig.len() == 64
            && sig
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase())),
        "must not look like a hex HMAC signature"
    );
}

/// dispatch routes Ed25519 through base64, HMAC through hex.
#[test]
fn dispatch_routes_ed25519_vs_hmac_correctly() {
    let signing_key = test_signing_key();
    let payload = "symbol=BTCUSDT&recvWindow=5000&timestamp=9999";

    let km_ed = BinanceKeyMaterial::Ed25519 { signing_key };
    let km_hmac = BinanceKeyMaterial::Hmac {
        secret: "test-secret".to_string(),
    };

    let sig_ed = sign_query_dispatch(&km_ed, payload);
    let sig_hmac = sign_query_dispatch(&km_hmac, payload);

    // Ed25519 → 88-char base64.
    assert_eq!(
        sig_ed.len(),
        88,
        "Ed25519 dispatch must return 88-char base64"
    );
    // HMAC → 64-char hex.
    assert_eq!(sig_hmac.len(), 64, "HMAC dispatch must return 64-char hex");
    // They must not be equal.
    assert_ne!(sig_ed, sig_hmac, "Ed25519 and HMAC results must differ");
}
