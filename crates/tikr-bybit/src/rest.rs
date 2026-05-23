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

use serde::Deserialize;
use std::str::FromStr;
use tikr_core::{Decimal, Level, Price, Size, Snapshot, Symbol, Timestamp};
use tikr_venue::VenueError;

use crate::BybitEnv;
use crate::mapping::bybit_symbol;

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
    let text = resp
        .text()
        .await
        .map_err(|e| VenueError::Network(std::io::Error::other(e.to_string())))?;
    if !status.is_success() {
        return Err(VenueError::Network(std::io::Error::other(format!(
            "bybit snapshot HTTP {status}: {text}"
        ))));
    }
    let env_json: OrderbookEnvelope = serde_json::from_str(&text).map_err(|e| {
        VenueError::Internal(
            format!("bybit snapshot parse: {e} (body={text})").into(),
        )
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
