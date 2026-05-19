//! HMAC-SHA256 sign roundtrip test against the official Binance docs example.
//!
//! Source:
//! https://binance-docs.github.io/apidocs/spot/en/#signed-trade-and-user_data-endpoint-security
//!
//! The same signing algorithm applies to both Spot and Futures — only the
//! REST path prefix differs (`/api/v3/...` vs `/fapi/v1/...`).
//!
//! ## Known fixture
//!
//! | Field | Value |
//! |-------|-------|
//! | Secret | `NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j` |
//! | Query | `symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559` |
//! | Expected signature | `c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71` |

use tikr_binance::sign::sign_query;

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
