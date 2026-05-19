//! HMAC-SHA256 and Ed25519 signing for Binance REST API requests.
//!
//! Binance authenticated endpoints require a `signature` query parameter
//! appended **last** to the query string.
//!
//! | Algorithm | Encoding | Binance has your secret? |
//! |-----------|----------|--------------------------|
//! | HMAC-SHA256 | lowercase hex | Yes (symmetric) |
//! | Ed25519 | base64 standard (with padding) | No (asymmetric) |
//!
//! Parameter order matters for canonical reproducibility. Always build the
//! query string with `recvWindow` before `timestamp` before `signature`.
//!
//! # Key material
//!
//! Use [`BinanceKeyMaterial`] to hold either variant. Construct via
//! [`BinanceKeyMaterial::hmac`] or load Ed25519 from PEM with
//! [`load_ed25519_from_pem`].
//!
//! # Mainnet gate
//!
//! Write actions on mainnet envs require `TIKR_BINANCE_ENABLE_MAINNET=1`.
//! The client enforces this; sign functions are unconditional.

use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use ed25519_dalek::{Signer, SigningKey};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};
use tikr_venue::VenueError;

type HmacSha256 = Hmac<Sha256>;

// ---------------------------------------------------------------------------
// BinanceKeyMaterial
// ---------------------------------------------------------------------------

/// Key material used to sign Binance API requests.
///
/// The operator selects the variant via `TIKR_BINANCE_KEY_TYPE=hmac|ed25519`.
/// Default is `hmac` for backward compatibility.
///
/// # Debug
///
/// Manual `Debug` impl — never prints the secret or signing key bytes.
pub enum BinanceKeyMaterial {
    /// HMAC-SHA256: symmetric key. Both you and Binance hold the secret.
    /// Signature is lowercase hex.
    Hmac {
        /// The raw HMAC secret string.
        secret: String,
    },
    /// Ed25519: asymmetric key. Only you hold the private key; Binance
    /// stores the public key only.
    /// Signature is base64 standard (with `=` padding).
    Ed25519 {
        /// The Ed25519 signing key loaded from PEM.
        signing_key: SigningKey,
    },
}

impl fmt::Debug for BinanceKeyMaterial {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Hmac { .. } => f
                .debug_struct("BinanceKeyMaterial::Hmac")
                .finish_non_exhaustive(),
            Self::Ed25519 { .. } => f
                .debug_struct("BinanceKeyMaterial::Ed25519")
                .finish_non_exhaustive(),
        }
    }
}

// ---------------------------------------------------------------------------
// Signing functions
// ---------------------------------------------------------------------------

/// Compute HMAC-SHA256 of `params` using `secret`. Returns lowercase hex.
///
/// `params` is the raw query string **without** the `&signature=` suffix.
/// The returned hex value is appended as `&signature=<hex>`.
///
/// # Example
///
/// ```rust
/// use tikr_binance::sign::sign_query;
/// let sig = sign_query(
///     "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j",
///     "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559",
/// );
/// assert_eq!(sig, "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71");
/// ```
pub fn sign_query(secret: &str, params: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC can accept any key size");
    mac.update(params.as_bytes());
    let result = mac.finalize();
    hex::encode(result.into_bytes())
}

/// Compute Ed25519 signature of `params`. Returns base64 standard (with `=` padding).
///
/// Signs the raw message bytes (EdDSA internally does double-SHA-512
/// prehash — do NOT pre-hash before calling this).
///
/// Returns a base64-encoded 64-byte signature (~88 chars with padding).
pub fn sign_query_ed25519(key: &SigningKey, params: &str) -> String {
    let sig = key.sign(params.as_bytes());
    BASE64_STANDARD.encode(sig.to_bytes())
}

/// Sign `params` using whichever key variant is configured.
///
/// - HMAC → lowercase hex
/// - Ed25519 → base64 standard
pub fn sign_query_dispatch(key: &BinanceKeyMaterial, params: &str) -> String {
    match key {
        BinanceKeyMaterial::Hmac { secret } => sign_query(secret, params),
        BinanceKeyMaterial::Ed25519 { signing_key } => sign_query_ed25519(signing_key, params),
    }
}

/// Load an Ed25519 signing key from a PKCS#8 PEM string.
///
/// The PEM must be in the standard PKCS#8 format produced by `openssl genpkey`:
///
/// ```text
/// -----BEGIN PRIVATE KEY-----
/// <base64 DER>
/// -----END PRIVATE KEY-----
/// ```
///
/// # Errors
///
/// Returns [`VenueError::Rejected`] if the PEM is malformed or not an Ed25519 key.
pub fn load_ed25519_from_pem(pem: &str) -> Result<SigningKey, VenueError> {
    use ed25519_dalek::pkcs8::DecodePrivateKey;
    SigningKey::from_pkcs8_pem(pem.trim()).map_err(|e| VenueError::Rejected {
        reason: format!("Ed25519 PEM parse error: {e}"),
    })
}

// ---------------------------------------------------------------------------
// Timestamp helper
// ---------------------------------------------------------------------------

/// Return current timestamp in milliseconds since Unix epoch.
pub fn timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Auth append helpers
// ---------------------------------------------------------------------------

/// Append `&recvWindow=5000&timestamp=<ts>&signature=<sig>` to the params
/// string and return the fully-signed query string. Uses HMAC-SHA256.
///
/// Ordering: `recvWindow` → `timestamp` → `signature` (as required by Binance).
pub fn append_auth(params: &str, secret: &str) -> String {
    let ts = timestamp_ms();
    append_auth_with_ts(params, secret, ts)
}

/// Variant of [`append_auth`] that accepts an explicit timestamp for testing.
pub fn append_auth_with_ts(params: &str, secret: &str, ts: u64) -> String {
    let base = if params.is_empty() {
        format!("recvWindow=5000&timestamp={ts}")
    } else {
        format!("{params}&recvWindow=5000&timestamp={ts}")
    };
    let sig = sign_query(secret, &base);
    format!("{base}&signature={sig}")
}

/// Append auth using key-material dispatch (HMAC or Ed25519).
///
/// Signs `recvWindow=5000&timestamp=<ts>` appended to `params`, then
/// appends `&signature=<result>`.
pub fn append_auth_dispatch(params: &str, key: &BinanceKeyMaterial) -> String {
    let ts = timestamp_ms();
    append_auth_with_ts_dispatch(params, key, ts)
}

/// Variant of [`append_auth_dispatch`] that accepts an explicit timestamp for testing.
pub fn append_auth_with_ts_dispatch(params: &str, key: &BinanceKeyMaterial, ts: u64) -> String {
    let base = if params.is_empty() {
        format!("recvWindow=5000&timestamp={ts}")
    } else {
        format!("{params}&recvWindow=5000&timestamp={ts}")
    };
    let sig = sign_query_dispatch(key, &base);
    format!("{base}&signature={sig}")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::VerifyingKey;

    /// Official Binance HMAC-SHA256 example from the docs.
    ///
    /// Source: https://binance-docs.github.io/apidocs/spot/en/#signed-trade-and-user_data-endpoint-security
    ///
    /// secret: NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j
    /// query:  symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559
    /// expected: c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71
    #[test]
    fn hmac_matches_binance_official_example() {
        let secret = "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j";
        let query = "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559";
        let sig = sign_query(secret, query);
        assert_eq!(
            sig, "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71",
            "HMAC must match the official Binance docs example exactly"
        );
    }

    /// signature= is always the last parameter in the signed query string.
    #[test]
    fn signature_appended_after_other_params() {
        let signed = append_auth_with_ts("symbol=BTCUSDT&side=BUY", "testsecret", 1_000_000_000);
        // Signature must come last.
        let sig_pos = signed
            .find("&signature=")
            .expect("signature param must exist");
        // Nothing else should come after the signature.
        let tail = &signed[sig_pos + "&signature=".len()..];
        assert!(
            !tail.contains('&'),
            "signature must be the last param; found '&' after it"
        );
    }

    /// recvWindow hardcoded to 5000 in append_auth.
    #[test]
    fn recv_window_default_5000() {
        let signed = append_auth_with_ts("", "secret", 1_000_000);
        assert!(
            signed.contains("recvWindow=5000"),
            "recvWindow must be 5000; got: {signed}"
        );
    }

    /// Verify recvWindow comes before timestamp comes before signature.
    #[test]
    fn param_order_recv_window_before_timestamp_before_signature() {
        let signed = append_auth_with_ts("symbol=BTCUSDT", "mysecret", 9_999_999);
        let rw_pos = signed.find("recvWindow=").expect("recvWindow");
        let ts_pos = signed.find("timestamp=").expect("timestamp");
        let sig_pos = signed.find("signature=").expect("signature");
        assert!(rw_pos < ts_pos, "recvWindow must precede timestamp");
        assert!(ts_pos < sig_pos, "timestamp must precede signature");
    }

    /// Signing is deterministic: same inputs → same output.
    #[test]
    fn sign_is_deterministic() {
        let secret = "some-secret";
        let params = "symbol=ETHUSDT&quantity=1.0&recvWindow=5000&timestamp=1234567890";
        let sig1 = sign_query(secret, params);
        let sig2 = sign_query(secret, params);
        assert_eq!(sig1, sig2);
    }

    // -----------------------------------------------------------------------
    // Ed25519 tests
    // -----------------------------------------------------------------------

    /// Build a deterministic Ed25519 signing key from a fixed 32-byte seed.
    fn test_signing_key() -> SigningKey {
        let seed = [42u8; 32];
        SigningKey::from_bytes(&seed)
    }

    /// Ed25519 signature is base64-encoded, not hex.
    #[test]
    fn ed25519_sign_returns_base64() {
        let key = test_signing_key();
        let sig = sign_query_ed25519(&key, "symbol=BTCUSDT&side=BUY&timestamp=1234567890");
        // Base64 standard with padding: 64 raw bytes → 88 chars.
        assert_eq!(
            sig.len(),
            88,
            "Ed25519 base64 sig must be 88 chars; got: {sig}"
        );
        // No hex-only chars — base64 uses A-Z a-z 0-9 + / =
        assert!(
            sig.chars()
                .all(|c| c.is_alphanumeric() || c == '+' || c == '/' || c == '='),
            "signature must be valid base64 standard; got: {sig}"
        );
    }

    /// Ed25519 signature verifies with the matching public key.
    #[test]
    fn ed25519_signature_verifies_with_public_key() {
        let key = test_signing_key();
        let verifying_key: VerifyingKey = (&key).into();
        let payload = "symbol=BTCUSDT&recvWindow=5000&timestamp=1699999999000";

        let sig = sign_query_ed25519(&key, payload);
        let sig_bytes = BASE64_STANDARD.decode(&sig).expect("base64 decode");
        let sig_arr: [u8; 64] = sig_bytes.try_into().expect("64 bytes");
        let signature = ed25519_dalek::Signature::from_bytes(&sig_arr);

        verifying_key
            .verify_strict(payload.as_bytes(), &signature)
            .expect("Ed25519 signature must verify against its own public key");
    }

    /// Dispatch routes HMAC key through hex path.
    #[test]
    fn key_material_from_hmac_signs_correctly() {
        let secret = "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j";
        let query = "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559";
        let km = BinanceKeyMaterial::Hmac {
            secret: secret.to_string(),
        };
        let sig = sign_query_dispatch(&km, query);
        // Must match the official HMAC vector.
        assert_eq!(
            sig,
            "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71"
        );
        // Must be hex, not base64.
        assert_eq!(sig.len(), 64);
        assert!(sig.chars().all(|c| c.is_ascii_hexdigit()));
    }

    /// Dispatch routes Ed25519 key through base64 path.
    #[test]
    fn key_material_from_ed25519_signs_correctly() {
        let key = test_signing_key();
        let km = BinanceKeyMaterial::Ed25519 { signing_key: key };
        let sig = sign_query_dispatch(&km, "symbol=BTCUSDT&timestamp=1234567890");
        // Must be 88-char base64.
        assert_eq!(sig.len(), 88);
        assert!(
            sig.chars()
                .all(|c| c.is_alphanumeric() || c == '+' || c == '/' || c == '='),
            "Ed25519 dispatch result must be base64"
        );
        // Must NOT look like hex (would be 64 lowercase hex chars).
        assert!(
            sig.len() != 64 || !sig.chars().all(|c| c.is_ascii_hexdigit()),
            "Ed25519 must not produce a hex signature"
        );
    }

    /// load_ed25519_from_pem parses a PKCS#8 PEM produced by openssl.
    ///
    /// Test vector: a known test PEM (openssl genpkey -algorithm ed25519).
    #[test]
    fn ed25519_pem_loads_from_pkcs8() {
        // PKCS#8 PEM for seed [42u8; 32] — test-only, NEVER use in production.
        // DER: 30 2e 02 01 00 30 05 06 03 2b 65 70 04 22 04 20 2a*32
        let test_pem = "-----BEGIN PRIVATE KEY-----\n\
            MC4CAQAwBQYDK2VwBCIEICoqKioqKioqKioqKioqKioqKioqKioqKioqKioqKioq\n\
            -----END PRIVATE KEY-----\n";

        let result = load_ed25519_from_pem(test_pem);
        assert!(
            result.is_ok(),
            "PKCS#8 PEM must parse successfully; err: {:?}",
            result.err()
        );

        // The key derived from seed [42u8; 32] must match test_signing_key().
        let loaded = result.unwrap();
        let expected = test_signing_key();
        assert_eq!(
            loaded.to_bytes(),
            expected.to_bytes(),
            "loaded PEM key must match expected seed bytes"
        );
    }

    /// append_auth_with_ts_dispatch produces a valid Ed25519 signed query string.
    #[test]
    fn append_auth_dispatch_ed25519_has_correct_structure() {
        let key = test_signing_key();
        let verifying_key: VerifyingKey = (&key).into();
        let km = BinanceKeyMaterial::Ed25519 { signing_key: key };

        let signed = append_auth_with_ts_dispatch("symbol=BTCUSDT", &km, 1_000_000_000);

        // Must contain recvWindow, timestamp, signature.
        assert!(signed.contains("recvWindow=5000"));
        assert!(signed.contains("timestamp=1000000000"));
        assert!(signed.contains("&signature="));

        // Extract signature value.
        let sig_idx = signed.find("&signature=").unwrap() + "&signature=".len();
        let sig_b64 = &signed[sig_idx..];
        let sig_bytes = BASE64_STANDARD
            .decode(sig_b64)
            .expect("base64 decode signature");
        let sig_arr: [u8; 64] = sig_bytes.try_into().expect("64 bytes");
        let signature = ed25519_dalek::Signature::from_bytes(&sig_arr);

        // The signed message is everything before &signature=.
        let msg = &signed[..signed.find("&signature=").unwrap()];
        verifying_key
            .verify_strict(msg.as_bytes(), &signature)
            .expect("dispatch-produced Ed25519 signature must verify");
    }
}
