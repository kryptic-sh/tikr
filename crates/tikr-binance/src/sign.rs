//! HMAC-SHA256 signing for Binance REST API requests.
//!
//! Binance authenticated endpoints require a `signature` query parameter
//! appended **last** to the query string. The signature is computed as:
//!
//! ```text
//! HMAC-SHA256(api_secret, query_string_without_signature) → hex
//! ```
//!
//! Parameter order matters for canonical reproducibility. Always build the
//! query string with `recvWindow` before `timestamp` before `signature`.
//!
//! # Mainnet gate
//!
//! Write actions on mainnet envs require `TIKR_BINANCE_ENABLE_MAINNET=1`.
//! The client enforces this; sign functions are unconditional.

use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

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

/// Return current timestamp in milliseconds since Unix epoch.
pub fn timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Append `&recvWindow=5000&timestamp=<ts>&signature=<sig>` to the params
/// string and return the fully-signed query string.
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Official Binance HMAC-SHA256 example from the docs.
    ///
    /// Source: https://binance-docs.github.io/apidocs/spot/en/#signed-trade-and-user_data-endpoint-security
    /// (and equivalently the futures docs — same signing algorithm).
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
}
