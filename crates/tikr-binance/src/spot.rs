//! Binance Spot REST endpoint wrappers.
//!
//! All paths: `/api/v3/...`
//! Base URL: spot testnet `https://testnet.binance.vision` or
//!           spot mainnet `https://api.binance.com`.
//!
//! Write methods require auth (api-key header + signed query).
//! `GET /api/v3/exchangeInfo` is public (no auth).

use reqwest::Client as HttpClient;
use serde::Deserialize;
use serde_json::Value;
use tikr_core::{QuoteId, Side};
use tikr_venue::VenueError;
use tracing::info;
use uuid::Uuid;

use crate::errors::{is_cancel_idempotent, parse_binance_error_code};
use crate::exchange_info::ExchangeInfoResponse;
use crate::http::{read_json, read_typed};
use crate::sign::{BinanceKeyMaterial, append_auth_dispatch};

// ---------------------------------------------------------------------------
// Order response
// ---------------------------------------------------------------------------

/// Binance spot order placement response (subset of fields).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderResponse {
    /// Venue-assigned order id.
    pub order_id: u64,
    /// Client-supplied order id echo.
    #[serde(default)]
    pub client_order_id: String,
    /// Order status string.
    #[serde(default)]
    pub status: String,
}

fn try_parse_error(body: &Value) -> Option<VenueError> {
    // Binance returns {"code": -XXXX, "msg": "..."} on errors.
    let code = body.get("code")?.as_i64()? as i32;
    if code < 0 {
        let msg = body.get("msg").and_then(Value::as_str).unwrap_or("unknown");
        Some(parse_binance_error_code(code, msg))
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Spot endpoints
// ---------------------------------------------------------------------------

/// Place a post-only limit order on Spot.
///
/// Endpoint: `POST /api/v3/order`
/// Auth: API-key header + signed query.
///
/// Returns `QuoteId` derived from the venue-assigned `orderId`.
#[allow(clippy::too_many_arguments)]
pub async fn place_order(
    http: &HttpClient,
    base_url: &str,
    api_key: &str,
    key_material: &BinanceKeyMaterial,
    symbol: &str,
    side: Side,
    price: &str,
    quantity: &str,
    client_order_id: &str,
) -> Result<QuoteId, VenueError> {
    let side_str = match side {
        Side::Bid => "BUY",
        Side::Ask => "SELL",
    };

    // Post-only on Spot = `type=LIMIT_MAKER` (NO timeInForce param).
    // Futures uses `type=LIMIT&timeInForce=GTX` instead. Verified live
    // 2026-05-19: Spot rejects `timeInForce=GTX` with -1115.
    let params = format!(
        "symbol={symbol}&side={side_str}&type=LIMIT_MAKER\
         &quantity={quantity}&price={price}\
         &newClientOrderId={client_order_id}"
    );
    let signed = append_auth_dispatch(&params, key_material);

    info!(
        symbol,
        side = side_str,
        price,
        quantity,
        "spot: placing order"
    );

    let url = format!("{base_url}/api/v3/order?{signed}");
    let resp = http
        .post(&url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .map_err(network_err)?;

    let body: Value = read_json(resp).await?;
    if let Some(e) = try_parse_error(&body) {
        return Err(e);
    }

    let order_id = body.get("orderId").and_then(Value::as_u64).ok_or_else(|| {
        VenueError::Internal(Box::new(std::io::Error::other(format!(
            "spot place_order: missing orderId in response: {body}"
        ))))
    })?;

    info!(order_id, symbol, "spot: order placed");
    Ok(QuoteId::from_uuid(Uuid::from_u128(order_id as u128)))
}

/// Cancel an order by `origClientOrderId` on Spot.
///
/// Endpoint: `DELETE /api/v3/order`
/// Idempotent: `-2011` and `-2013` → `Ok(())`.
pub async fn cancel_order(
    http: &HttpClient,
    base_url: &str,
    api_key: &str,
    key_material: &BinanceKeyMaterial,
    symbol: &str,
    client_order_id: &str,
) -> Result<(), VenueError> {
    let params = format!("symbol={symbol}&origClientOrderId={client_order_id}");
    let signed = append_auth_dispatch(&params, key_material);

    info!(symbol, client_order_id, "spot: canceling order");

    let url = format!("{base_url}/api/v3/order?{signed}");
    let resp = http
        .delete(&url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .map_err(network_err)?;

    let body: Value = read_json(resp).await?;
    if let Some(code) = extract_error_code(&body) {
        if is_cancel_idempotent(code) {
            return Ok(());
        }
        let msg = body.get("msg").and_then(Value::as_str).unwrap_or("unknown");
        return Err(parse_binance_error_code(code, msg));
    }
    Ok(())
}

/// Cancel all open orders for `symbol` on Spot.
///
/// Endpoint: `DELETE /api/v3/openOrders`
pub async fn cancel_all_orders(
    http: &HttpClient,
    base_url: &str,
    api_key: &str,
    key_material: &BinanceKeyMaterial,
    symbol: &str,
) -> Result<(), VenueError> {
    let params = format!("symbol={symbol}");
    let signed = append_auth_dispatch(&params, key_material);

    info!(symbol, "spot: canceling all open orders");

    let url = format!("{base_url}/api/v3/openOrders?{signed}");
    let resp = http
        .delete(&url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .map_err(network_err)?;

    let body: Value = read_json(resp).await?;
    // cancel_all returns an array of canceled orders; a non-array is an error.
    if let Some(code) = extract_error_code(&body) {
        // -2011/-2013 on bulk cancel = no open orders → idempotent success.
        if is_cancel_idempotent(code) {
            return Ok(());
        }
        let msg = body.get("msg").and_then(Value::as_str).unwrap_or("unknown");
        return Err(parse_binance_error_code(code, msg));
    }
    Ok(())
}

/// Fetch Spot `exchangeInfo` (no auth).
///
/// Endpoint: `GET /api/v3/exchangeInfo`
pub async fn get_exchange_info(
    http: &HttpClient,
    base_url: &str,
) -> Result<ExchangeInfoResponse, VenueError> {
    let url = format!("{base_url}/api/v3/exchangeInfo");
    let resp = http.get(&url).send().await.map_err(network_err)?;
    let info: ExchangeInfoResponse = read_typed(resp).await?;
    Ok(info)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn extract_error_code(body: &Value) -> Option<i32> {
    let code = body.get("code")?.as_i64()? as i32;
    if code < 0 { Some(code) } else { None }
}

pub(crate) fn network_err(e: reqwest::Error) -> VenueError {
    VenueError::Network(std::io::Error::other(e.to_string()))
}
