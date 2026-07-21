//! Bybit V5 REST helpers (public surface in Phase 1).
//!
//! Only the orderbook snapshot endpoint is wired — paper-mode runners
//! use it for the initial state before the WS depth deltas take over.
//!
//! ## Endpoint
//!
//! `GET /v5/market/orderbook?category=linear&symbol={SYMBOL}&limit=50`
//!
//! Response shape:
//! ```json
//! {
//!   "retCode": 0,
//!   "result": {
//!     "s": "BTCUSDT",
//!     "b": [["62000.5","1.234"], ...],   // descending bid price
//!     "a": [["62001.0","0.987"], ...],   // ascending ask price
//!     "ts": 1731700000000,                // ms
//!     "u":  41234567
//!   }
//! }
//! ```
//!
//! ## Error mapping
//!
//! | HTTP status | VenueError variant | Notes |
//! |---|---|---|
//! | 418 / 429 | `RateLimited` | `Retry-After` header parsed as delta-seconds |
//! | other non-2xx | `Rejected` | bounded response body preserved |
//! | 2xx with `retCode != 0` | `Rejected` | Bybit business-logic rejection |

use serde::Deserialize;
use std::str::FromStr;
use tikr_core::{Decimal, Level, Price, Size, Snapshot, Symbol, Timestamp};
use tikr_venue::VenueError;

use crate::BybitEnv;
use crate::mapping::bybit_symbol;

const MAX_BODY_BYTES: usize = 512;

/// Parse the `Retry-After` header value, which may be a delta-seconds integer
/// or an HTTP-date (RFC 2822). We only support the delta-seconds form and
/// return `None` for the date form or unparseable values.
fn parse_retry_after(value: &str) -> Option<u64> {
    let trimmed = value.trim();
    // Delta-seconds: non-empty ASCII digits.
    if trimmed.is_empty() {
        return None;
    }
    trimmed.bytes().all(|b| b.is_ascii_digit()).then(|| {
        // Saturates on overflow to avoid accidental zero due to wrapping.
        trimmed.parse::<u64>().unwrap_or(u64::MAX)
    })
}

/// Truncate a response body for safe inclusion in error messages.
fn bounded_body(text: &str) -> String {
    if text.len() <= MAX_BODY_BYTES {
        return text.to_string();
    }

    let end = text
        .char_indices()
        .map(|(index, _)| index)
        .take_while(|index| *index <= MAX_BODY_BYTES)
        .last()
        .unwrap_or(0);
    format!("{}… ({} total bytes)", &text[..end], text.len())
}

/// Map a non-success HTTP status into a `VenueError` using the response
/// body and any `Retry-After` header.
fn map_http_error(
    status: reqwest::StatusCode,
    text: &str,
    retry_after: Option<&str>,
) -> VenueError {
    let body = bounded_body(text);
    match status.as_u16() {
        418 | 429 => {
            let ms = retry_after
                .and_then(parse_retry_after)
                .map(|s| s.saturating_mul(1000))
                .unwrap_or(1000);
            VenueError::RateLimited { retry_after_ms: ms }
        }
        _ => VenueError::Rejected {
            reason: format!("bybit snapshot HTTP {status}: {body}"),
        },
    }
}

#[derive(Deserialize)]
struct OrderbookEnvelope {
    #[serde(rename = "retCode")]
    ret_code: i32,
    #[serde(rename = "retMsg", default)]
    ret_msg: String,
    result: Option<OrderbookResult>,
}

#[derive(Deserialize)]
struct OrderbookResult {
    /// Descending bids.
    b: Vec<[String; 2]>,
    /// Ascending asks.
    a: Vec<[String; 2]>,
    /// Server epoch milliseconds.
    ts: u64,
}

/// Pull a top-50 orderbook snapshot for `symbol`.
///
/// Returns the parsed [`Snapshot`] with the venue-reported timestamp
/// (converted ms → ns) so it composes cleanly with WS frames.
pub async fn orderbook_snapshot(
    http: &reqwest::Client,
    env: BybitEnv,
    symbol: &Symbol,
) -> Result<Snapshot, VenueError> {
    let url = format!(
        "{}/v5/market/orderbook?category=linear&symbol={}&limit=50",
        env.rest_base_url(),
        bybit_symbol(symbol)
    );
    let resp = http
        .get(&url)
        .send()
        .await
        .map_err(|e| VenueError::Network(std::io::Error::other(e.to_string())))?;
    let status = resp.status();
    let retry_after = resp
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned());
    let text = resp
        .text()
        .await
        .map_err(|e| VenueError::Network(std::io::Error::other(e.to_string())))?;
    if !status.is_success() {
        return Err(map_http_error(status, &text, retry_after.as_deref()));
    }
    let env_json: OrderbookEnvelope = serde_json::from_str(&text).map_err(|e| {
        VenueError::Internal(format!("bybit snapshot parse: {e} (body={text})").into())
    })?;
    if env_json.ret_code != 0 {
        return Err(VenueError::Rejected {
            reason: format!(
                "bybit snapshot retCode={} retMsg={}",
                env_json.ret_code, env_json.ret_msg
            ),
        });
    }
    let result = env_json
        .result
        .ok_or_else(|| VenueError::Internal("bybit snapshot: missing result".into()))?;
    let bids = result.b.iter().filter_map(parse_level).collect();
    let asks = result.a.iter().filter_map(parse_level).collect();
    Ok(Snapshot {
        symbol: symbol.clone(),
        bids,
        asks,
        ts: Timestamp(result.ts.saturating_mul(1_000_000)),
    })
}

fn parse_level(raw: &[String; 2]) -> Option<Level> {
    let price = Decimal::from_str(&raw[0]).ok()?;
    let size = Decimal::from_str(&raw[1]).ok()?;
    Some(Level {
        price: Price(price),
        size: Size(size),
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::StatusCode;

    #[test]
    fn parse_retry_after_delta_seconds() {
        assert_eq!(parse_retry_after("30"), Some(30));
        assert_eq!(parse_retry_after("0"), Some(0));
        assert_eq!(parse_retry_after("120"), Some(120));
    }

    #[test]
    fn parse_retry_after_whitespace() {
        assert_eq!(parse_retry_after("  30  "), Some(30));
    }

    #[test]
    fn parse_retry_after_none_on_rfc2822_date() {
        // Bybit uses delta-seconds but handle the HTTP-date form gracefully.
        assert_eq!(parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT"), None);
    }

    #[test]
    fn parse_retry_after_none_on_garbage() {
        assert_eq!(parse_retry_after(""), None);
        assert_eq!(parse_retry_after("abc"), None);
        assert_eq!(parse_retry_after("-1"), None);
    }

    #[test]
    fn parse_retry_after_overflow_saturates() {
        // A huge value saturates to u64::MAX rather than wrapping.
        let huge = format!("{}", u64::MAX);
        assert_eq!(parse_retry_after(&huge), Some(u64::MAX));
    }

    #[test]
    fn bounded_body_short() {
        let s = "hello";
        assert_eq!(bounded_body(s), "hello");
    }

    #[test]
    fn bounded_body_truncates() {
        let long = "x".repeat(600);
        let result = bounded_body(&long);
        assert!(result.starts_with(&"x".repeat(MAX_BODY_BYTES)));
        assert!(result.contains("(600 total bytes)"));
    }

    #[test]
    fn bounded_body_truncates_on_utf8_boundary() {
        let long = format!("{}é", "x".repeat(MAX_BODY_BYTES - 1));
        let result = bounded_body(&long);
        assert!(result.starts_with(&"x".repeat(MAX_BODY_BYTES - 1)));
        assert!(!result.starts_with(&long));
        assert!(result.contains("(513 total bytes)"));
    }

    #[test]
    fn bounded_body_exact_fit() {
        let exact = "x".repeat(MAX_BODY_BYTES);
        assert_eq!(bounded_body(&exact), exact);
    }

    #[test]
    fn map_429_to_rate_limited() {
        let err = map_http_error(StatusCode::TOO_MANY_REQUESTS, "slow down", None);
        match err {
            VenueError::RateLimited { retry_after_ms } => {
                assert_eq!(retry_after_ms, 1000); // conservative fallback
            }
            other => panic!("expected RateLimited, got {other:?}"),
        }
    }

    #[test]
    fn map_429_with_retry_after() {
        let err = map_http_error(StatusCode::TOO_MANY_REQUESTS, "slow down", Some("30"));
        match err {
            VenueError::RateLimited { retry_after_ms } => {
                assert_eq!(retry_after_ms, 30_000);
            }
            other => panic!("expected RateLimited, got {other:?}"),
        }
    }

    #[test]
    fn map_418_to_rate_limited() {
        let err = map_http_error(StatusCode::from_u16(418).unwrap(), "banned", Some("120"));
        match err {
            VenueError::RateLimited { retry_after_ms } => {
                assert_eq!(retry_after_ms, 120_000);
            }
            other => panic!("expected RateLimited, got {other:?}"),
        }
    }

    #[test]
    fn map_500_to_rejected() {
        let err = map_http_error(StatusCode::INTERNAL_SERVER_ERROR, "server oops", None);
        match &err {
            VenueError::Rejected { reason } => {
                assert!(reason.contains("500"));
                assert!(reason.contains("server oops"));
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[test]
    fn map_404_to_rejected_bounded_body() {
        let body = "x".repeat(600);
        let err = map_http_error(StatusCode::NOT_FOUND, &body, None);
        match &err {
            VenueError::Rejected { reason } => {
                assert!(reason.contains("404"));
                assert!(reason.contains("…"));
                assert!(reason.contains("(600 total bytes)"));
                // Body truncated to max bound.
                let body_start = reason
                    .find("HTTP 404: ")
                    .map(|i| &reason[i + 10..])
                    .unwrap_or("");
                assert!(body_start.len() <= MAX_BODY_BYTES + 20);
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
    }
}
