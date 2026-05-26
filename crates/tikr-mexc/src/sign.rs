//! HMAC-SHA256 signing for MEXC Spot REST API.
//!
//! MEXC signs requests by appending `signature=<hex>` to the query
//! string. Pattern is identical to Binance Spot.

use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

/// Current epoch milliseconds — MEXC's `timestamp` param.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_millis() as u64
}

/// Compute HMAC-SHA256(secret, params) → lowercase hex. `params` is the
/// query string before the `&signature=` suffix.
pub fn sign_query(secret: &str, params: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key size");
    mac.update(params.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Append `&timestamp=<now>&recvWindow=5000&signature=<hex>` to `params`,
/// signing the full string (including timestamp) with `secret`.
pub fn append_signature(params: &str, secret: &str) -> String {
    let ts = now_ms();
    let with_ts = if params.is_empty() {
        format!("timestamp={ts}&recvWindow=5000")
    } else {
        format!("{params}&timestamp={ts}&recvWindow=5000")
    };
    let sig = sign_query(secret, &with_ts);
    format!("{with_ts}&signature={sig}")
}
