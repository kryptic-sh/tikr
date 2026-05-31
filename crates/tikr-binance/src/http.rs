//! Shared Binance REST response handling: HTTP-status mapping, rate-limit /
//! `Retry-After` handling, and used-weight tracking.
//!
//! ## Why this exists
//!
//! Every endpoint used to special-case only HTTP 429/418 and then blindly
//! `resp.json()` the body. Any *other* non-2xx status — most importantly a
//! **403** from Binance's CloudFront edge (WAF / geo / abuse block) or a 5xx —
//! fell straight through to JSON parsing on an HTML (or empty) body, producing
//! a cryptic `serde` "expected value, line 1, column 1" decode error with no
//! rate-limit signal and no backoff hint.
//!
//! [`read_body`] is the single choke point: it checks the status generally,
//! maps every throttling status (429 / 418 / 403) to [`VenueError::RateLimited`]
//! honoring the real `Retry-After` header, surfaces any other non-2xx as a
//! clear [`VenueError::Rejected`] (never a decode error), and records the
//! `X-MBX-USED-WEIGHT-1M` header so the orchestrator can watch how close it is
//! to the IP weight cap (USD-M futures: 2400/min).

use std::sync::atomic::{AtomicU64, Ordering};

use reqwest::Response;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tikr_venue::VenueError;

use crate::errors::parse_binance_error_code;
use crate::futs::network_err;

/// Last observed `X-MBX-USED-WEIGHT-1M` (request weight consumed in the current
/// rolling minute), updated on every REST response. `0` until the first call.
static USED_WEIGHT_1M: AtomicU64 = AtomicU64::new(0);

/// Most recent `X-MBX-USED-WEIGHT-1M` Binance reported across all REST calls.
/// Lets the orchestrator surface headroom against the IP weight cap
/// (USD-M futures = 2400/min). Process-global (one IP = one budget).
pub fn used_weight_1m() -> u64 {
    USED_WEIGHT_1M.load(Ordering::Relaxed)
}

/// Default backoff when a throttling response omits `Retry-After`.
const DEFAULT_RATE_LIMIT_MS: u64 = 1_000;
/// Default backoff for an 418 IP auto-ban with no `Retry-After` — bans are
/// minutes, not seconds, so don't hammer the edge with 1s retries.
const DEFAULT_IP_BAN_MS: u64 = 60_000;

/// Read + validate a Binance REST response, returning the raw body text on a
/// 2xx status. Maps throttling / error statuses to typed [`VenueError`]s so a
/// non-JSON error body can never reach a JSON parser:
/// - **429 / 418 / 403** → [`VenueError::RateLimited`] honoring `Retry-After`
///   (403 = CloudFront block; treated as a throttle so callers back off).
/// - any other **non-2xx** → [`VenueError::Rejected`] carrying the status +
///   a body snippet (or Binance's `{code,msg}` if the body parses).
///
/// Also records `X-MBX-USED-WEIGHT-1M` into the process-global weight monitor.
pub(crate) async fn read_body(resp: Response) -> Result<String, VenueError> {
    let status = resp.status();
    let code = status.as_u16();

    if let Some(w) = resp
        .headers()
        .get("x-mbx-used-weight-1m")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
    {
        USED_WEIGHT_1M.store(w, Ordering::Relaxed);
    }

    // `Retry-After` is in seconds per RFC 7231; convert to ms.
    let retry_after_ms = resp
        .headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        .map(|secs| secs.saturating_mul(1000));

    let text = resp.text().await.map_err(network_err)?;

    if status.is_success() {
        return Ok(text);
    }

    // Throttling statuses: 429 too-many-requests, 418 IP auto-ban, 403
    // CloudFront/WAF/geo block. All signal "back off" to the caller.
    if matches!(code, 429 | 418 | 403) {
        let default_ms = if code == 418 {
            DEFAULT_IP_BAN_MS
        } else {
            DEFAULT_RATE_LIMIT_MS
        };
        return Err(VenueError::RateLimited {
            retry_after_ms: retry_after_ms.unwrap_or(default_ms),
        });
    }

    // Other non-2xx: prefer Binance's structured `{code,msg}` error if the
    // body happens to be JSON; otherwise a clear HTTP-status error with a
    // truncated snippet — never a raw JSON-decode error.
    if let Ok(v) = serde_json::from_str::<Value>(&text)
        && let Some(err) = try_parse_error(&v)
    {
        return Err(err);
    }
    let snippet: String = text.chars().take(200).collect();
    Err(VenueError::Rejected {
        reason: format!("binance HTTP {code}: {snippet}"),
    })
}

/// [`read_body`] + parse the success body as a [`serde_json::Value`].
pub(crate) async fn read_json(resp: Response) -> Result<Value, VenueError> {
    let text = read_body(resp).await?;
    serde_json::from_str(&text).map_err(json_err)
}

/// [`read_body`] + deserialize the success body into `T`.
pub(crate) async fn read_typed<T: DeserializeOwned>(resp: Response) -> Result<T, VenueError> {
    let text = read_body(resp).await?;
    serde_json::from_str(&text).map_err(json_err)
}

/// Map a `serde_json` decode error (parsing a *2xx* body) to an internal error.
pub(crate) fn json_err(e: serde_json::Error) -> VenueError {
    VenueError::Internal(Box::new(e))
}

/// Surface a Binance `{code,msg}` error body (negative code) as a [`VenueError`].
fn try_parse_error(body: &Value) -> Option<VenueError> {
    let code = body.get("code")?.as_i64()? as i32;
    if code < 0 {
        let msg = body.get("msg").and_then(Value::as_str).unwrap_or("unknown");
        Some(parse_binance_error_code(code, msg))
    } else {
        None
    }
}
