//! MEXC Spot REST endpoint wrappers.
//!
//! Base URL: `https://api.mexc.com`
//! All write methods require `X-MEXC-APIKEY` header + signed query.

use reqwest::Client as HttpClient;
use serde_json::Value;
use tikr_core::{QuoteId, Side};
use tikr_venue::VenueError;
use tracing::info;
use uuid::Uuid;

use crate::sign::append_signature;

/// MEXC spot wallet balance for one asset.
#[derive(Debug, Clone, Copy, Default)]
pub struct SpotBalance {
    /// Free (available to trade) balance.
    pub free: tikr_core::Decimal,
    /// Locked (in open orders) balance.
    pub locked: tikr_core::Decimal,
}

/// MEXC spot best bid/ask for one symbol.
#[derive(Debug, Clone, Copy)]
pub struct SpotBookTicker {
    pub bid_price: tikr_core::Decimal,
    pub bid_qty: tikr_core::Decimal,
    pub ask_price: tikr_core::Decimal,
    pub ask_qty: tikr_core::Decimal,
}

fn network_err(e: reqwest::Error) -> VenueError {
    VenueError::Network(std::io::Error::other(e.to_string()))
}

fn internal_err(e: reqwest::Error) -> VenueError {
    VenueError::Internal(Box::new(e))
}

/// MEXC returns errors as `{"code": N, "msg": "..."}`. Map to VenueError.
fn try_parse_error(body: &Value) -> Option<VenueError> {
    let code = body.get("code").and_then(Value::as_i64)?;
    if code == 200 || code == 0 {
        return None;
    }
    let msg = body
        .get("msg")
        .and_then(Value::as_str)
        .unwrap_or("(no msg)");
    Some(VenueError::Rejected {
        reason: format!("mexc error (code {code}): {msg}"),
    })
}

/// Place a LIMIT order on MEXC Spot.
///
/// Endpoint: `POST /api/v3/order`
/// Auth: API-key header + signed query.
#[allow(clippy::too_many_arguments)]
pub async fn place_limit_order(
    http: &HttpClient,
    base_url: &str,
    api_key: &str,
    api_secret: &str,
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
    let params = format!(
        "symbol={symbol}&side={side_str}&type=LIMIT\
         &quantity={quantity}&price={price}\
         &newClientOrderId={client_order_id}"
    );
    let signed = append_signature(&params, api_secret);

    info!(
        symbol,
        side = side_str,
        price,
        quantity,
        "mexc: placing order"
    );

    let url = format!("{base_url}/api/v3/order?{signed}");
    let resp = http
        .post(&url)
        .header("X-MEXC-APIKEY", api_key)
        .send()
        .await
        .map_err(network_err)?;

    let status = resp.status();
    if status.as_u16() == 429 || status.as_u16() == 418 {
        return Err(VenueError::RateLimited {
            retry_after_ms: 1000,
        });
    }

    let body: Value = resp.json().await.map_err(internal_err)?;
    if let Some(e) = try_parse_error(&body) {
        return Err(e);
    }

    // MEXC returns orderId as string or number depending on version.
    let order_id_num = body
        .get("orderId")
        .and_then(|v| {
            v.as_u64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .unwrap_or_else(|| {
            // Fallback: derive from client_order_id hash.
            client_order_id
                .bytes()
                .fold(0u64, |a, b| a.wrapping_add(b as u64).wrapping_mul(31))
        });

    info!(order_id = order_id_num, symbol, "mexc: order placed");
    Ok(QuoteId::from_uuid(Uuid::from_u128(order_id_num as u128)))
}

/// Cancel an order by `origClientOrderId` on MEXC Spot.
///
/// Endpoint: `DELETE /api/v3/order`
pub async fn cancel_order(
    http: &HttpClient,
    base_url: &str,
    api_key: &str,
    api_secret: &str,
    symbol: &str,
    client_order_id: &str,
) -> Result<(), VenueError> {
    let params = format!("symbol={symbol}&origClientOrderId={client_order_id}");
    let signed = append_signature(&params, api_secret);
    let url = format!("{base_url}/api/v3/order?{signed}");
    let resp = http
        .delete(&url)
        .header("X-MEXC-APIKEY", api_key)
        .send()
        .await
        .map_err(network_err)?;
    let body: Value = resp.json().await.map_err(internal_err)?;
    // MEXC returns -2011 / -2013 for already-gone orders. Treat as success.
    if let Some(code) = body.get("code").and_then(Value::as_i64)
        && (code == -2011 || code == -2013)
    {
        return Ok(());
    }
    if let Some(e) = try_parse_error(&body) {
        return Err(e);
    }
    info!(symbol, client_order_id, "mexc: order canceled");
    Ok(())
}

/// Cancel all open orders for a symbol.
///
/// Endpoint: `DELETE /api/v3/openOrders`
pub async fn cancel_all_orders(
    http: &HttpClient,
    base_url: &str,
    api_key: &str,
    api_secret: &str,
    symbol: &str,
) -> Result<(), VenueError> {
    let params = format!("symbol={symbol}");
    let signed = append_signature(&params, api_secret);
    let url = format!("{base_url}/api/v3/openOrders?{signed}");
    let resp = http
        .delete(&url)
        .header("X-MEXC-APIKEY", api_key)
        .send()
        .await
        .map_err(network_err)?;
    // Response is an array of cancelled orders OR an error object.
    let body: Value = resp.json().await.map_err(internal_err)?;
    if body.is_object()
        && let Some(e) = try_parse_error(&body)
    {
        return Err(e);
    }
    info!(symbol, "mexc: all orders canceled");
    Ok(())
}

/// Fetch best bid/ask for one symbol.
///
/// Endpoint: `GET /api/v3/ticker/bookTicker?symbol=...`
pub async fn get_book_ticker(
    http: &HttpClient,
    base_url: &str,
    symbol: &str,
) -> Result<SpotBookTicker, VenueError> {
    let url = format!("{base_url}/api/v3/ticker/bookTicker?symbol={symbol}");
    let resp = http.get(&url).send().await.map_err(network_err)?;
    let body: Value = resp.json().await.map_err(internal_err)?;
    if let Some(e) = try_parse_error(&body) {
        return Err(e);
    }
    let parse = |k: &str| -> tikr_core::Decimal {
        body.get(k)
            .and_then(Value::as_str)
            .and_then(|s| <tikr_core::Decimal as std::str::FromStr>::from_str(s).ok())
            .unwrap_or_default()
    };
    Ok(SpotBookTicker {
        bid_price: parse("bidPrice"),
        bid_qty: parse("bidQty"),
        ask_price: parse("askPrice"),
        ask_qty: parse("askQty"),
    })
}

/// Fetch spot wallet balance for one asset.
///
/// Endpoint: `GET /api/v3/account` (returns all assets; we filter).
pub async fn get_balance(
    http: &HttpClient,
    base_url: &str,
    api_key: &str,
    api_secret: &str,
    asset: &str,
) -> Result<SpotBalance, VenueError> {
    let signed = append_signature("", api_secret);
    let url = format!("{base_url}/api/v3/account?{signed}");
    let resp = http
        .get(&url)
        .header("X-MEXC-APIKEY", api_key)
        .send()
        .await
        .map_err(network_err)?;
    let body: Value = resp.json().await.map_err(internal_err)?;
    if let Some(e) = try_parse_error(&body) {
        return Err(e);
    }
    let balances = body
        .get("balances")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            VenueError::Internal(Box::new(std::io::Error::other(
                "mexc account: missing 'balances'",
            )))
        })?;
    let parse = |row: &Value, k: &str| -> tikr_core::Decimal {
        row.get(k)
            .and_then(Value::as_str)
            .and_then(|s| <tikr_core::Decimal as std::str::FromStr>::from_str(s).ok())
            .unwrap_or_default()
    };
    for row in balances {
        if row.get("asset").and_then(Value::as_str) == Some(asset) {
            return Ok(SpotBalance {
                free: parse(row, "free"),
                locked: parse(row, "locked"),
            });
        }
    }
    Ok(SpotBalance::default())
}

/// One open order from MEXC's `openOrders` endpoint.
#[derive(Debug, Clone)]
pub struct OpenOrder {
    pub order_id: String,
    pub client_order_id: String,
    pub side: Side,
    pub price: tikr_core::Decimal,
    pub orig_qty: tikr_core::Decimal,
    pub executed_qty: tikr_core::Decimal,
}

/// Fetch all open orders for a symbol.
///
/// Endpoint: `GET /api/v3/openOrders?symbol=...`
pub async fn get_open_orders(
    http: &HttpClient,
    base_url: &str,
    api_key: &str,
    api_secret: &str,
    symbol: &str,
) -> Result<Vec<OpenOrder>, VenueError> {
    let params = format!("symbol={symbol}");
    let signed = append_signature(&params, api_secret);
    let url = format!("{base_url}/api/v3/openOrders?{signed}");
    let resp = http
        .get(&url)
        .header("X-MEXC-APIKEY", api_key)
        .send()
        .await
        .map_err(network_err)?;
    let body: Value = resp.json().await.map_err(internal_err)?;
    if body.is_object()
        && let Some(e) = try_parse_error(&body)
    {
        return Err(e);
    }
    let arr = body.as_array().ok_or_else(|| {
        VenueError::Internal(Box::new(std::io::Error::other(
            "mexc openOrders: expected array",
        )))
    })?;
    let parse_dec = |row: &Value, k: &str| -> tikr_core::Decimal {
        row.get(k)
            .and_then(Value::as_str)
            .and_then(|s| <tikr_core::Decimal as std::str::FromStr>::from_str(s).ok())
            .unwrap_or_default()
    };
    let mut out = Vec::with_capacity(arr.len());
    for row in arr {
        let side_str = row.get("side").and_then(Value::as_str).unwrap_or("");
        let side = match side_str {
            "BUY" => Side::Bid,
            "SELL" => Side::Ask,
            _ => continue,
        };
        let order_id = row
            .get("orderId")
            .map(|v| {
                v.as_str()
                    .map(str::to_string)
                    .or_else(|| v.as_u64().map(|n| n.to_string()))
                    .unwrap_or_default()
            })
            .unwrap_or_default();
        let client_order_id = row
            .get("clientOrderId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        out.push(OpenOrder {
            order_id,
            client_order_id,
            side,
            price: parse_dec(row, "price"),
            orig_qty: parse_dec(row, "origQty"),
            executed_qty: parse_dec(row, "executedQty"),
        });
    }
    Ok(out)
}

/// Per-symbol exchangeInfo filter values.
#[derive(Debug, Clone, Copy, Default)]
pub struct SymbolFilters {
    pub tick_size: tikr_core::Decimal,
    pub step_size: tikr_core::Decimal,
    pub min_notional: tikr_core::Decimal,
    pub min_qty: tikr_core::Decimal,
}

/// Fetch exchangeInfo filters for one symbol.
///
/// Endpoint: `GET /api/v3/exchangeInfo?symbol=...`
pub async fn get_symbol_filters(
    http: &HttpClient,
    base_url: &str,
    symbol: &str,
) -> Result<SymbolFilters, VenueError> {
    let url = format!("{base_url}/api/v3/exchangeInfo?symbol={symbol}");
    let resp = http.get(&url).send().await.map_err(network_err)?;
    let body: Value = resp.json().await.map_err(internal_err)?;
    if let Some(e) = try_parse_error(&body) {
        return Err(e);
    }
    let symbols = body
        .get("symbols")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            VenueError::Internal(Box::new(std::io::Error::other(
                "mexc exchangeInfo: missing 'symbols'",
            )))
        })?;
    let row = symbols.first().ok_or_else(|| {
        VenueError::Internal(Box::new(std::io::Error::other(format!(
            "mexc exchangeInfo: symbol {symbol} not found"
        ))))
    })?;
    let parse_str = |key: &str| -> tikr_core::Decimal {
        row.get(key)
            .and_then(Value::as_str)
            .and_then(|s| <tikr_core::Decimal as std::str::FromStr>::from_str(s).ok())
            .unwrap_or_default()
    };
    // MEXC v3 returns precisions + size directly on the symbol object (not in
    // FILTER blocks like Binance). `baseSizePrecision` is the min step.
    let base_size_precision = parse_str("baseSizePrecision");
    let quote_amount_precision = parse_str("quoteAmountPrecision");
    // Tick from quoteAssetPrecision (decimals → 10^-N).
    let quote_dp = row
        .get("quoteAssetPrecision")
        .and_then(Value::as_u64)
        .unwrap_or(0) as u32;
    let tick = if quote_dp > 0 {
        // Build 10^-quote_dp via Decimal::new(1, quote_dp).
        tikr_core::Decimal::new(1, quote_dp)
    } else {
        tikr_core::Decimal::ZERO
    };
    Ok(SymbolFilters {
        tick_size: tick,
        step_size: base_size_precision,
        min_notional: quote_amount_precision,
        min_qty: base_size_precision,
    })
}
