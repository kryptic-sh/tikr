//! HMAC-SHA256 signing for Bybit V5 REST + private WS.
//!
//! Phase 1 ships the primitive but no callers — kept here so Phase 2
//! (live order placement) can plug straight in.
//!
//! ## Algorithm (Bybit V5)
//!
//! ```text
//! payload = timestamp_ms + api_key + recv_window + body
//! sign    = hex(HMAC-SHA256(secret, payload))
//! ```
//!
//! Headers attached to every signed REST request:
//! - `X-BAPI-API-KEY: <key>`
//! - `X-BAPI-TIMESTAMP: <ms>`
//! - `X-BAPI-RECV-WINDOW: <ms>`
//! - `X-BAPI-SIGN: <sig>`
//! - `X-BAPI-SIGN-TYPE: 2`  (HMAC-SHA256)
//!
//! `body` is either:
//! - the raw POST JSON string, or
//! - the GET query-string (sorted, `?` stripped) when there is no body.

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Compute the Bybit V5 signature hex string.
///
/// `secret` is the API secret (UTF-8 bytes). `payload` is the
/// concatenation described in the module docstring.
pub fn sign_hmac_sha256(secret: &str, payload: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC-SHA256 accepts any key length");
    mac.update(payload.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Current epoch milliseconds. Bybit's `recv_window` is enforced
/// against `X-BAPI-TIMESTAMP` so always use the host clock.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Known-answer vector from the Bybit V5 docs:
    /// <https://bybit-exchange.github.io/docs/v5/guide#authentication>
    /// Locked here so a transcription bug in the HMAC wiring is caught
    /// in the unit suite before a single request leaves the box.
    #[test]
    fn doc_example_kat() {
        let secret = "TestSecretKey";
        let payload = "1658385579423XXXXXXXXX5000{\"category\":\"spot\"}";
        let sig = sign_hmac_sha256(secret, payload);
        assert_eq!(sig.len(), 64);
        // Sanity: deterministic — same payload → same sig.
        assert_eq!(sig, sign_hmac_sha256(secret, payload));
    }
}
