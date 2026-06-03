//! Binance USD-M Futures REST endpoint wrappers.
//!
//! All paths: `/fapi/v1/...`
//! Base URL: futures testnet `https://testnet.binancefuture.com` or
//!           futures mainnet `https://fapi.binance.com`.
//!
//! Write methods require auth (api-key header + signed query).
//! `GET /fapi/v1/exchangeInfo` is public (no auth).
//!
//! ## Leverage
//!
//! Call [`update_leverage`] once at startup with `leverage=1` to ensure
//! 1x cross-margin leverage. One-way position mode is assumed (no `positionSide`
//! param). Hedge mode requires changes to order placement (out of scope v0).

use reqwest::Client as HttpClient;
use serde_json::Value;
use tikr_core::{QuoteId, Side, TimeInForce};
use tikr_venue::VenueError;
use tracing::info;
use uuid::Uuid;

use crate::errors::{is_cancel_idempotent, parse_binance_error_code};
use crate::exchange_info::ExchangeInfoResponse;
use crate::http::{read_json, read_typed};
use crate::sign::{BinanceKeyMaterial, append_auth_dispatch};

/// USD-M futures account balance values for one margin asset.
#[derive(Debug, Clone, Copy, Default)]
pub struct FuturesBalance {
    /// Wallet balance for the asset.
    pub wallet_balance: tikr_core::Decimal,
    /// Balance available for new orders / withdrawals.
    pub available_balance: tikr_core::Decimal,
    /// Cross-position unrealized PnL included by Binance for this asset.
    pub cross_unrealized_pnl: tikr_core::Decimal,
}

/// USD-M futures position-risk values for one symbol.
#[derive(Debug, Clone, Copy, Default)]
pub struct FuturesPositionRisk {
    /// Signed position amount. Positive = long, negative = short.
    pub position_amount: tikr_core::Decimal,
    /// Binance entry price.
    pub entry_price: tikr_core::Decimal,
    /// Binance break-even price, including fee/funding adjustments when exposed.
    pub break_even_price: tikr_core::Decimal,
    /// Binance mark price used for unrealized PnL.
    pub mark_price: tikr_core::Decimal,
    /// Binance unrealized profit for this symbol.
    pub unrealized_profit: tikr_core::Decimal,
    /// Binance estimated liquidation price (`0` when flat / no liq risk).
    pub liquidation_price: tikr_core::Decimal,
}

/// USD-M futures 24h ticker stats for one symbol.
#[derive(Debug, Clone)]
pub struct FuturesTicker24h {
    /// Binance symbol, e.g. `DOGEUSDT`.
    pub symbol: String,
    /// 24h absolute price-change percent.
    pub price_change_percent_abs: tikr_core::Decimal,
    /// 24h quote volume.
    pub quote_volume: tikr_core::Decimal,
    /// 24h high price.
    pub high_price: tikr_core::Decimal,
    /// 24h low price.
    pub low_price: tikr_core::Decimal,
    /// 24h volume-weighted average price (used as the range denominator).
    pub weighted_avg_price: tikr_core::Decimal,
}

/// USD-M futures best bid/ask for one symbol.
#[derive(Debug, Clone, Copy)]
pub struct FuturesBookTicker {
    /// Best bid price.
    pub bid_price: tikr_core::Decimal,
    /// Best ask price.
    pub ask_price: tikr_core::Decimal,
}

// ---------------------------------------------------------------------------
// Futures endpoints
// ---------------------------------------------------------------------------

/// Place a post-only limit order on USD-M Futures.
///
/// Endpoint: `POST /fapi/v1/order`
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
    tif: TimeInForce,
) -> Result<QuoteId, VenueError> {
    let side_str = match side {
        Side::Bid => "BUY",
        Side::Ask => "SELL",
    };
    // Binance Futures TIF strings: GTC, IOC, FOK, GTX (post-only).
    let tif_str = match tif {
        TimeInForce::PostOnly => "GTX",
        TimeInForce::IOC => "IOC",
        TimeInForce::FOK => "FOK",
        TimeInForce::GTC => "GTC",
    };

    let params = format!(
        "symbol={symbol}&side={side_str}&type=LIMIT&timeInForce={tif_str}\
         &quantity={quantity}&price={price}\
         &newClientOrderId={client_order_id}"
    );
    let signed = append_auth_dispatch(&params, key_material);

    info!(
        symbol,
        side = side_str,
        price,
        quantity,
        "futures: placing order"
    );

    let url = format!("{base_url}/fapi/v1/order?{signed}");
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
            "futures place_order: missing orderId in response: {body}"
        ))))
    })?;

    info!(order_id, symbol, "futures: order placed");
    Ok(QuoteId::from_uuid(Uuid::from_u128(order_id as u128)))
}

/// Place a reduce-only, post-only (GTX) LIMIT order on USD-M Futures. The
/// `reduceOnly=true` flag means it can only shrink the position and is exempt
/// from the MIN_NOTIONAL filter; GTX makes it a maker order (rejected if it
/// would cross). Used by the take-profit to lock in part of a winning bag.
#[allow(clippy::too_many_arguments)]
pub async fn place_reduce_only_limit(
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
    let params = format!(
        "symbol={symbol}&side={side_str}&type=LIMIT&timeInForce=GTX\
         &quantity={quantity}&price={price}&reduceOnly=true\
         &newClientOrderId={client_order_id}"
    );
    let signed = append_auth_dispatch(&params, key_material);
    info!(
        symbol,
        side = side_str,
        price,
        quantity,
        "futures: placing reduce-only maker limit"
    );
    let url = format!("{base_url}/fapi/v1/order?{signed}");
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
            "futures place_reduce_only_limit: missing orderId in response: {body}"
        ))))
    })?;
    Ok(QuoteId::from_uuid(Uuid::from_u128(order_id as u128)))
}

/// Place a market order on USD-M Futures.
///
/// Endpoint: `POST /fapi/v1/order`
/// Auth: API-key header + signed query.
///
/// Returns `QuoteId` derived from the venue-assigned `orderId`.
pub async fn place_market_order(
    http: &HttpClient,
    base_url: &str,
    api_key: &str,
    key_material: &BinanceKeyMaterial,
    symbol: &str,
    side: Side,
    quantity: &str,
) -> Result<QuoteId, VenueError> {
    let side_str = match side {
        Side::Bid => "BUY",
        Side::Ask => "SELL",
    };
    let client_order_id = format!("mc_{}", Uuid::new_v4().as_simple());
    // reduceOnly=true: this primitive only ever CLOSES a position. It (a) makes
    // the order exempt from the MIN_NOTIONAL filter so a sub-minNotional dust
    // position can actually be closed (a plain MARKET order rejects with -4164),
    // and (b) guarantees it can never open/flip a position if the size races the
    // live position read.
    let params = format!(
        "symbol={symbol}&side={side_str}&type=MARKET\
         &quantity={quantity}&reduceOnly=true\
         &newClientOrderId={client_order_id}"
    );
    let signed = append_auth_dispatch(&params, key_material);

    info!(
        symbol,
        side = side_str,
        quantity,
        "futures: placing market order"
    );

    let url = format!("{base_url}/fapi/v1/order?{signed}");
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
            "futures place_market_order: missing orderId in response: {body}"
        ))))
    })?;

    info!(order_id, symbol, "futures: market order placed");
    Ok(QuoteId::from_uuid(Uuid::from_u128(order_id as u128)))
}

/// One leg of a batch placement. Price/quantity are pre-rounded + formatted
/// by the caller (the `Venue` impl), mirroring [`place_order`].
pub struct BatchOrderReq<'a> {
    /// Order side.
    pub side: Side,
    /// Pre-rounded, wire-formatted limit price.
    pub price: &'a str,
    /// Pre-rounded, wire-formatted quantity.
    pub quantity: &'a str,
    /// `newClientOrderId` (the venue-accepted handle for later cancel).
    pub client_order_id: &'a str,
    /// Time-in-force (`PostOnly` → `GTX`).
    pub tif: TimeInForce,
}

/// Binance Futures TIF wire string.
fn tif_str(tif: TimeInForce) -> &'static str {
    match tif {
        TimeInForce::PostOnly => "GTX",
        TimeInForce::IOC => "IOC",
        TimeInForce::FOK => "FOK",
        TimeInForce::GTC => "GTC",
    }
}

/// Build the `batchOrders` JSON array string for a place batch. Pure +
/// testable; the caller percent-encodes + signs it.
fn build_batch_orders_json(symbol: &str, orders: &[BatchOrderReq<'_>]) -> String {
    let arr: Vec<Value> = orders
        .iter()
        .map(|o| {
            serde_json::json!({
                "symbol": symbol,
                "side": match o.side { Side::Bid => "BUY", Side::Ask => "SELL" },
                "type": "LIMIT",
                "timeInForce": tif_str(o.tif),
                "quantity": o.quantity,
                "price": o.price,
                "newClientOrderId": o.client_order_id,
            })
        })
        .collect();
    Value::Array(arr).to_string()
}

/// Percent-encode every byte that isn't an RFC-3986 unreserved char. The
/// `batchOrders` / `*List` params carry JSON (`[]{}":,` etc.) that MUST be
/// encoded both for the signature and for transmission — Binance HMACs the
/// query string as sent, so the signed bytes and the wire bytes must match
/// exactly (the signer encodes the resulting query identically).
fn percent_encode_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 3);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Parse one element of a batchOrders place response: a negative `code` is a
/// per-order error, otherwise pull the `orderId` → venue [`QuoteId`].
fn parse_place_element(el: &Value) -> Result<QuoteId, VenueError> {
    if let Some(code) = el.get("code").and_then(Value::as_i64)
        && code < 0
    {
        let msg = el.get("msg").and_then(Value::as_str).unwrap_or("unknown");
        return Err(parse_binance_error_code(code as i32, msg));
    }
    let order_id = el.get("orderId").and_then(Value::as_u64).ok_or_else(|| {
        VenueError::Internal(Box::new(std::io::Error::other(format!(
            "futures batchOrders: missing orderId in element: {el}"
        ))))
    })?;
    Ok(QuoteId::from_uuid(Uuid::from_u128(order_id as u128)))
}

/// Place up to 5 orders in ONE request.
///
/// Endpoint: `POST /fapi/v1/batchOrders` (max 5 orders; weight 5).
/// Returns one `Result` per input order, in input order — the request can
/// succeed (HTTP 200) while individual legs are rejected (each element carries
/// its own `{code,msg}`). A request-level failure (auth, rate-limit, malformed)
/// returns `Err` for the whole call.
pub async fn place_batch_orders(
    http: &HttpClient,
    base_url: &str,
    api_key: &str,
    key_material: &BinanceKeyMaterial,
    symbol: &str,
    orders: &[BatchOrderReq<'_>],
) -> Result<Vec<Result<QuoteId, VenueError>>, VenueError> {
    if orders.is_empty() {
        return Ok(Vec::new());
    }
    let json = build_batch_orders_json(symbol, orders);
    let params = format!("batchOrders={}", percent_encode_value(&json));
    let signed = append_auth_dispatch(&params, key_material);

    info!(
        symbol,
        count = orders.len(),
        "futures: placing batch orders"
    );

    let url = format!("{base_url}/fapi/v1/batchOrders?{signed}");
    let resp = http
        .post(&url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .map_err(network_err)?;

    let body: Value = read_json(resp).await?;
    let arr = body.as_array().ok_or_else(|| {
        VenueError::Internal(Box::new(std::io::Error::other(format!(
            "futures batchOrders place: expected array response: {body}"
        ))))
    })?;
    Ok(arr.iter().map(parse_place_element).collect())
}

/// Parse one element of a batchOrders cancel response. `-2011`/`-2013`
/// (already gone) is idempotent success.
fn parse_cancel_element(el: &Value) -> Result<(), VenueError> {
    if let Some(code) = el.get("code").and_then(Value::as_i64)
        && code < 0
    {
        let code = code as i32;
        if is_cancel_idempotent(code) {
            return Ok(());
        }
        let msg = el.get("msg").and_then(Value::as_str).unwrap_or("unknown");
        return Err(parse_binance_error_code(code, msg));
    }
    Ok(())
}

/// Cancel up to 10 orders (by `origClientOrderId`) in ONE request.
///
/// Endpoint: `DELETE /fapi/v1/batchOrders` (max 10; weight 1). All ids must
/// belong to `symbol`. Returns one `Result` per input id, in input order;
/// `-2011`/`-2013` (already gone) map to `Ok(())` per element. A request-level
/// failure returns `Err` for the whole call.
pub async fn cancel_batch_orders(
    http: &HttpClient,
    base_url: &str,
    api_key: &str,
    key_material: &BinanceKeyMaterial,
    symbol: &str,
    client_order_ids: &[String],
) -> Result<Vec<Result<(), VenueError>>, VenueError> {
    if client_order_ids.is_empty() {
        return Ok(Vec::new());
    }
    let list = Value::Array(
        client_order_ids
            .iter()
            .map(|s| Value::String(s.clone()))
            .collect(),
    )
    .to_string();
    let params = format!(
        "symbol={symbol}&origClientOrderIdList={}",
        percent_encode_value(&list)
    );
    let signed = append_auth_dispatch(&params, key_material);

    info!(
        symbol,
        count = client_order_ids.len(),
        "futures: canceling batch orders"
    );

    let url = format!("{base_url}/fapi/v1/batchOrders?{signed}");
    let resp = http
        .delete(&url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .map_err(network_err)?;

    // A whole-request UnknownQuote (every id gone, surfaced at the top level)
    // is idempotent success for all.
    let body: Value = match read_json(resp).await {
        Ok(b) => b,
        Err(VenueError::UnknownQuote) => {
            return Ok(client_order_ids.iter().map(|_| Ok(())).collect());
        }
        Err(e) => return Err(e),
    };
    let arr = body.as_array().ok_or_else(|| {
        VenueError::Internal(Box::new(std::io::Error::other(format!(
            "futures batchOrders cancel: expected array response: {body}"
        ))))
    })?;
    Ok(arr.iter().map(parse_cancel_element).collect())
}

/// Cancel an order by `origClientOrderId` on Futures.
///
/// Endpoint: `DELETE /fapi/v1/order`
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

    info!(symbol, client_order_id, "futures: canceling order");

    let url = format!("{base_url}/fapi/v1/order?{signed}");
    let resp = http
        .delete(&url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .map_err(network_err)?;

    let body: Value = match read_json(resp).await {
        Ok(b) => b,
        // Binance returns -2011/-2013 (order not found / already canceled) as
        // HTTP 400, which read_json maps to UnknownQuote. On the cancel path
        // that's idempotent success — the order is already gone.
        Err(VenueError::UnknownQuote) => return Ok(()),
        Err(e) => return Err(e),
    };
    if let Some(code) = extract_error_code(&body) {
        if is_cancel_idempotent(code) {
            return Ok(());
        }
        let msg = body.get("msg").and_then(Value::as_str).unwrap_or("unknown");
        return Err(parse_binance_error_code(code, msg));
    }
    Ok(())
}

/// Cancel all open orders for `symbol` on Futures.
///
/// Endpoint: `DELETE /fapi/v1/allOpenOrders`
pub async fn cancel_all_orders(
    http: &HttpClient,
    base_url: &str,
    api_key: &str,
    key_material: &BinanceKeyMaterial,
    symbol: &str,
) -> Result<(), VenueError> {
    let params = format!("symbol={symbol}");
    let signed = append_auth_dispatch(&params, key_material);

    info!(symbol, "futures: canceling all open orders");

    let url = format!("{base_url}/fapi/v1/allOpenOrders?{signed}");
    let resp = http
        .delete(&url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .map_err(network_err)?;

    let body: Value = match read_json(resp).await {
        Ok(b) => b,
        // -2011/-2013 (HTTP 400) → no open orders to cancel; idempotent success.
        Err(VenueError::UnknownQuote) => return Ok(()),
        Err(e) => return Err(e),
    };
    if let Some(code) = extract_error_code(&body) {
        if is_cancel_idempotent(code) {
            return Ok(());
        }
        let msg = body.get("msg").and_then(Value::as_str).unwrap_or("unknown");
        return Err(parse_binance_error_code(code, msg));
    }
    Ok(())
}

/// Set leverage to `leverage` (1x at startup) for `symbol`.
///
/// Endpoint: `POST /fapi/v1/leverage`
/// Called once at construction to enforce 1x cross-margin leverage.
pub async fn update_leverage(
    http: &HttpClient,
    base_url: &str,
    api_key: &str,
    key_material: &BinanceKeyMaterial,
    symbol: &str,
    leverage: u32,
) -> Result<(), VenueError> {
    let params = format!("symbol={symbol}&leverage={leverage}");
    let signed = append_auth_dispatch(&params, key_material);

    info!(symbol, leverage, "futures: updating leverage");

    let url = format!("{base_url}/fapi/v1/leverage?{signed}");
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
    Ok(())
}

/// Fetch Futures `exchangeInfo` (no auth).
///
/// Endpoint: `GET /fapi/v1/exchangeInfo`
pub async fn get_exchange_info(
    http: &HttpClient,
    base_url: &str,
) -> Result<ExchangeInfoResponse, VenueError> {
    let url = format!("{base_url}/fapi/v1/exchangeInfo");
    let resp = http.get(&url).send().await.map_err(network_err)?;
    let info: ExchangeInfoResponse = read_typed(resp).await?;
    Ok(info)
}

/// Fetch all USD-M futures 24h ticker stats.
pub async fn get_24hr_tickers(
    http: &HttpClient,
    base_url: &str,
) -> Result<Vec<FuturesTicker24h>, VenueError> {
    let url = format!("{base_url}/fapi/v1/ticker/24hr");
    let resp = http.get(&url).send().await.map_err(network_err)?;
    let body: Value = read_json(resp).await?;
    if let Some(err) = try_parse_error(&body) {
        return Err(err);
    }
    let arr = body.as_array().ok_or_else(|| {
        VenueError::Internal(Box::new(std::io::Error::other(
            "ticker/24hr: expected array",
        )))
    })?;
    let mut out = Vec::with_capacity(arr.len());
    for row in arr {
        let Some(symbol) = row.get("symbol").and_then(Value::as_str) else {
            continue;
        };
        let pct = row
            .get("priceChangePercent")
            .and_then(Value::as_str)
            .and_then(|s| <tikr_core::Decimal as std::str::FromStr>::from_str(s).ok())
            .unwrap_or_default()
            .abs();
        let dec = |key: &str| {
            row.get(key)
                .and_then(Value::as_str)
                .and_then(|s| <tikr_core::Decimal as std::str::FromStr>::from_str(s).ok())
                .unwrap_or_default()
        };
        let quote_volume = dec("quoteVolume");
        out.push(FuturesTicker24h {
            symbol: symbol.to_string(),
            price_change_percent_abs: pct,
            quote_volume,
            high_price: dec("highPrice"),
            low_price: dec("lowPrice"),
            weighted_avg_price: dec("weightedAvgPrice"),
        });
    }
    Ok(out)
}

/// One USD-M perp symbol + its tick width in basis points of current
/// mid price. Used by the tide auto-rotation to discover
/// symbols where each tick is wide enough to clear maker fees.
#[derive(Debug, Clone)]
pub struct PerpTickInfo {
    /// Binance symbol (e.g. `ESPORTSUSDT`).
    pub symbol: String,
    /// Last/mid price snapshot.
    pub price: tikr_core::Decimal,
    /// Venue tick size.
    pub tick_size: tikr_core::Decimal,
    /// `tick_size / price × 10000`. Driver of round-trip economics.
    pub tick_bps: tikr_core::Decimal,
    /// 24h quote volume — used as a coarse liquidity filter.
    pub quote_volume_24h: tikr_core::Decimal,
}

/// Discover all USD-M PERPETUAL symbols quoted in `quote_asset`,
/// joining exchangeInfo (tick filter) with the latest /ticker/price
/// snapshot and 24h ticker volume. Returns one row per TRADING symbol.
///
/// `quote_asset` is typically `"USDT"` or `"USDC"`. Single REST call
/// to exchangeInfo + one to ticker/price + one to ticker/24hr.
pub async fn list_perp_tick_info(
    http: &HttpClient,
    base_url: &str,
    quote_asset: &str,
) -> Result<Vec<PerpTickInfo>, VenueError> {
    use std::collections::HashMap;
    use std::str::FromStr;

    // 1. exchangeInfo for symbol filters.
    let info = get_exchange_info(http, base_url).await?;
    let parsed = crate::exchange_info::parse_exchange_info(&info);
    let symbol_meta: HashMap<String, &crate::exchange_info::SymbolFilters> =
        parsed.iter().map(|(k, v)| (k.clone(), v)).collect();

    // 2. ticker/price for current prices.
    let url = format!("{base_url}/fapi/v1/ticker/price");
    let resp = http.get(&url).send().await.map_err(network_err)?;
    let body: Value = read_json(resp).await?;
    let arr = body.as_array().ok_or_else(|| {
        VenueError::Internal(Box::new(std::io::Error::other(
            "ticker/price: expected array",
        )))
    })?;
    let prices: HashMap<String, tikr_core::Decimal> = arr
        .iter()
        .filter_map(|row| {
            let symbol = row.get("symbol").and_then(Value::as_str)?;
            let price = row
                .get("price")
                .and_then(Value::as_str)
                .and_then(|s| tikr_core::Decimal::from_str(s).ok())?;
            Some((symbol.to_string(), price))
        })
        .collect();

    // 3. 24h volumes for liquidity filter.
    let tickers = get_24hr_tickers(http, base_url).await?;
    let volumes: HashMap<String, tikr_core::Decimal> = tickers
        .into_iter()
        .map(|t| (t.symbol, t.quote_volume))
        .collect();

    // Restrict to PERPETUAL USDT-quoted TRADING contracts.
    let mut out = Vec::new();
    for sym_raw in &info.symbols {
        let symbol = &sym_raw.symbol;
        if sym_raw.quote_asset.as_deref() != Some(quote_asset) {
            continue;
        }
        if sym_raw.contract_type.as_deref() != Some("PERPETUAL") {
            continue;
        }
        if sym_raw.status.as_deref() != Some("TRADING") {
            continue;
        }
        // Skip non-ASCII / non-alphanumeric symbols (e.g. CJK gimmick
        // listings). Their multi-byte bytes break the request signer's query
        // string → every signed request fails with -1022, and they're not
        // worth auto-trading anyway.
        if !symbol.bytes().all(|b| b.is_ascii_alphanumeric()) {
            continue;
        }
        let Some(meta) = symbol_meta.get(symbol) else {
            continue;
        };
        let Some(price) = prices.get(symbol).copied() else {
            continue;
        };
        if price <= tikr_core::Decimal::ZERO {
            continue;
        }
        if meta.tick_size <= tikr_core::Decimal::ZERO {
            continue;
        }
        let tick_bps = meta.tick_size / price * tikr_core::Decimal::from(10_000);
        let quote_volume_24h = volumes.get(symbol).copied().unwrap_or_default();
        out.push(PerpTickInfo {
            symbol: symbol.clone(),
            price,
            tick_size: meta.tick_size,
            tick_bps,
            quote_volume_24h,
        });
    }
    Ok(out)
}

/// One USD-M perp symbol scored for Wave's preferred regime: volatile,
/// wide-spread, mean-reverting, liquid. Produced by [`list_perp_wave_info`].
#[derive(Debug, Clone)]
pub struct PerpWaveInfo {
    /// Binance symbol.
    pub symbol: String,
    /// Last/mid price snapshot.
    pub price: tikr_core::Decimal,
    /// Venue tick size.
    pub tick_size: tikr_core::Decimal,
    /// Live book spread in bps of mid: `(ask − bid) / mid × 10000`.
    pub spread_bps: tikr_core::Decimal,
    /// 24h range as a percent of the weighted-avg price:
    /// `(high − low) / weightedAvg × 100`. Oscillation amplitude.
    pub range_pct: tikr_core::Decimal,
    /// 24h absolute net price-change percent. Range ≫ this = mean-reverting.
    pub change_pct_abs: tikr_core::Decimal,
    /// 24h quote volume — liquidity floor.
    pub quote_volume_24h: tikr_core::Decimal,
}

/// Fetch best bid/ask for ALL USD-M futures symbols in one call
/// (`/fapi/v1/ticker/bookTicker` with no symbol → array). Returns a map of
/// `symbol → (bid, ask)`.
pub async fn get_all_book_tickers(
    http: &HttpClient,
    base_url: &str,
) -> Result<std::collections::HashMap<String, (tikr_core::Decimal, tikr_core::Decimal)>, VenueError>
{
    use std::str::FromStr;
    let url = format!("{base_url}/fapi/v1/ticker/bookTicker");
    let resp = http.get(&url).send().await.map_err(network_err)?;
    let body: Value = read_json(resp).await?;
    if let Some(err) = try_parse_error(&body) {
        return Err(err);
    }
    let arr = body.as_array().ok_or_else(|| {
        VenueError::Internal(Box::new(std::io::Error::other(
            "ticker/bookTicker: expected array",
        )))
    })?;
    let mut out = std::collections::HashMap::new();
    for row in arr {
        let Some(symbol) = row.get("symbol").and_then(Value::as_str) else {
            continue;
        };
        let bid = row
            .get("bidPrice")
            .and_then(Value::as_str)
            .and_then(|s| tikr_core::Decimal::from_str(s).ok());
        let ask = row
            .get("askPrice")
            .and_then(Value::as_str)
            .and_then(|s| tikr_core::Decimal::from_str(s).ok());
        if let (Some(b), Some(a)) = (bid, ask) {
            out.insert(symbol.to_string(), (b, a));
        }
    }
    Ok(out)
}

/// Discover USD-M PERPETUAL `quote_asset`-quoted symbols scored for Wave's
/// preferred regime. Joins exchangeInfo (tick) + ticker/price + 24hr ticker
/// (range / net-change / volume) + the all-symbols bookTicker (live spread).
/// Four REST calls total. Caller applies the score + filters.
pub async fn list_perp_wave_info(
    http: &HttpClient,
    base_url: &str,
    quote_asset: &str,
) -> Result<Vec<PerpWaveInfo>, VenueError> {
    use std::collections::HashMap;
    use std::str::FromStr;

    let info = get_exchange_info(http, base_url).await?;
    let parsed = crate::exchange_info::parse_exchange_info(&info);
    let symbol_meta: HashMap<String, &crate::exchange_info::SymbolFilters> =
        parsed.iter().map(|(k, v)| (k.clone(), v)).collect();

    let url = format!("{base_url}/fapi/v1/ticker/price");
    let resp = http.get(&url).send().await.map_err(network_err)?;
    let body: Value = read_json(resp).await?;
    let arr = body.as_array().ok_or_else(|| {
        VenueError::Internal(Box::new(std::io::Error::other(
            "ticker/price: expected array",
        )))
    })?;
    let prices: HashMap<String, tikr_core::Decimal> = arr
        .iter()
        .filter_map(|row| {
            let symbol = row.get("symbol").and_then(Value::as_str)?;
            let price = row
                .get("price")
                .and_then(Value::as_str)
                .and_then(|s| tikr_core::Decimal::from_str(s).ok())?;
            Some((symbol.to_string(), price))
        })
        .collect();

    let tickers = get_24hr_tickers(http, base_url).await?;
    let ticker_map: HashMap<String, FuturesTicker24h> =
        tickers.into_iter().map(|t| (t.symbol.clone(), t)).collect();
    let books = get_all_book_tickers(http, base_url).await?;

    let hundred = tikr_core::Decimal::from(100);
    let ten_k = tikr_core::Decimal::from(10_000);
    let mut out = Vec::new();
    for sym_raw in &info.symbols {
        let symbol = &sym_raw.symbol;
        if sym_raw.quote_asset.as_deref() != Some(quote_asset) {
            continue;
        }
        if sym_raw.contract_type.as_deref() != Some("PERPETUAL") {
            continue;
        }
        if sym_raw.status.as_deref() != Some("TRADING") {
            continue;
        }
        // Skip non-ASCII / non-alphanumeric symbols (e.g. CJK gimmick
        // listings). Their multi-byte bytes break the request signer's query
        // string → every signed request fails with -1022, and they're not
        // worth auto-trading anyway.
        if !symbol.bytes().all(|b| b.is_ascii_alphanumeric()) {
            continue;
        }
        let Some(meta) = symbol_meta.get(symbol) else {
            continue;
        };
        let Some(price) = prices.get(symbol).copied() else {
            continue;
        };
        if price <= tikr_core::Decimal::ZERO || meta.tick_size <= tikr_core::Decimal::ZERO {
            continue;
        }
        let Some(t) = ticker_map.get(symbol) else {
            continue;
        };
        // Range %: prefer the weighted-avg denominator, fall back to price.
        let denom = if t.weighted_avg_price > tikr_core::Decimal::ZERO {
            t.weighted_avg_price
        } else {
            price
        };
        let range_pct = if t.high_price > t.low_price && denom > tikr_core::Decimal::ZERO {
            (t.high_price - t.low_price) / denom * hundred
        } else {
            tikr_core::Decimal::ZERO
        };
        // Live spread bps from the all-symbols bookTicker.
        let spread_bps = match books.get(symbol) {
            Some((bid, ask)) if *ask > *bid && *bid > tikr_core::Decimal::ZERO => {
                let mid = (*bid + *ask) / tikr_core::Decimal::from(2);
                (*ask - *bid) / mid * ten_k
            }
            _ => tikr_core::Decimal::ZERO,
        };
        out.push(PerpWaveInfo {
            symbol: symbol.clone(),
            price,
            tick_size: meta.tick_size,
            spread_bps,
            range_pct,
            change_pct_abs: t.price_change_percent_abs,
            quote_volume_24h: t.quote_volume,
        });
    }
    Ok(out)
}

/// Fetch current best bid/ask for one USD-M futures symbol.
pub async fn get_book_ticker(
    http: &HttpClient,
    base_url: &str,
    symbol: &str,
) -> Result<FuturesBookTicker, VenueError> {
    let url = format!("{base_url}/fapi/v1/ticker/bookTicker?symbol={symbol}");
    let resp = http.get(&url).send().await.map_err(network_err)?;
    let body: Value = read_json(resp).await?;
    if let Some(err) = try_parse_error(&body) {
        return Err(err);
    }
    let bid_price = body
        .get("bidPrice")
        .and_then(Value::as_str)
        .and_then(|s| <tikr_core::Decimal as std::str::FromStr>::from_str(s).ok())
        .unwrap_or_default();
    let ask_price = body
        .get("askPrice")
        .and_then(Value::as_str)
        .and_then(|s| <tikr_core::Decimal as std::str::FromStr>::from_str(s).ok())
        .unwrap_or_default();
    Ok(FuturesBookTicker {
        bid_price,
        ask_price,
    })
}

/// Fetch recent 1m close prices for short-window realized-vol scoring.
pub async fn get_1m_closes(
    http: &HttpClient,
    base_url: &str,
    symbol: &str,
    limit: u32,
) -> Result<Vec<tikr_core::Decimal>, VenueError> {
    let limit = limit.clamp(2, 100);
    let url = format!("{base_url}/fapi/v1/klines?symbol={symbol}&interval=1m&limit={limit}");
    let resp = http.get(&url).send().await.map_err(network_err)?;
    let body: Value = read_json(resp).await?;
    if let Some(err) = try_parse_error(&body) {
        return Err(err);
    }
    let Some(rows) = body.as_array() else {
        return Ok(Vec::new());
    };
    let mut closes = Vec::with_capacity(rows.len());
    for row in rows {
        let Some(close) = row
            .as_array()
            .and_then(|cols| cols.get(4))
            .and_then(Value::as_str)
            .and_then(|s| <tikr_core::Decimal as std::str::FromStr>::from_str(s).ok())
        else {
            continue;
        };
        if close > tikr_core::Decimal::ZERO {
            closes.push(close);
        }
    }
    Ok(closes)
}

/// Backfill the TUI candle chart on startup by aggregating recent aggregate
/// trades into 1-second OHLC candles, so the graph isn't blank until live
/// samples accumulate. Futures klines have no sub-minute interval (`1s` returns
/// `-1120 Invalid interval`), so we bucket `/fapi/v1/aggTrades` by trade time
/// instead. Returns `(open_time_ms, open, high, low, close)` oldest-first.
///
/// One aggTrades page is capped at 1000 trades — only ~20s on a busy symbol —
/// so to cover `window_secs` we page BACKWARD via `endTime`, oldest call last,
/// until the earliest trade seen reaches `latest − window_secs` (or we hit the
/// page cap, a safety bound on request weight). Trades within a second from
/// different pages all land in the same 1s bucket, so paging never distorts a
/// candle.
#[allow(clippy::type_complexity)]
pub async fn get_1s_agg_candles(
    http: &HttpClient,
    base_url: &str,
    symbol: &str,
    window_secs: u64,
) -> Result<
    Vec<(
        u64,
        tikr_core::Decimal,
        tikr_core::Decimal,
        tikr_core::Decimal,
        tikr_core::Decimal,
    )>,
    VenueError,
> {
    use std::collections::BTreeMap;
    use std::str::FromStr;

    // Hard cap on pages so a very busy symbol can't fan out into dozens of
    // weight-20 requests at startup.
    const MAX_PAGES: u32 = 24;
    let window_ms = window_secs.saturating_mul(1000);

    // Per-bucket OHLC tracked WITH open/close timestamps, so merging trades from
    // pages fetched out of order (newest page first) still picks the true
    // earliest trade as open and latest as close.
    struct Ohlc {
        open_ts: u64,
        open: tikr_core::Decimal,
        high: tikr_core::Decimal,
        low: tikr_core::Decimal,
        close_ts: u64,
        close: tikr_core::Decimal,
    }
    let mut buckets: BTreeMap<u64, Ohlc> = BTreeMap::new();
    // Newest trade time across all pages — fixes the window's right edge. Set on
    // the first (most-recent) page.
    let mut latest_ts: Option<u64> = None;
    // Earliest trade time seen so far — the next page fetches strictly older.
    let mut earliest_ts: Option<u64> = None;

    for _ in 0..MAX_PAGES {
        let url = match earliest_ts {
            // First page: the most recent 1000 trades.
            None => format!("{base_url}/fapi/v1/aggTrades?symbol={symbol}&limit=1000"),
            // Subsequent pages: the 1000 trades ending just before the oldest
            // one we already have.
            Some(min_t) => format!(
                "{base_url}/fapi/v1/aggTrades?symbol={symbol}&endTime={}&limit=1000",
                min_t.saturating_sub(1)
            ),
        };
        let resp = http.get(&url).send().await.map_err(network_err)?;
        let body: Value = read_json(resp).await?;
        if let Some(err) = try_parse_error(&body) {
            return Err(err);
        }
        let Some(rows) = body.as_array() else { break };
        if rows.is_empty() {
            break;
        }

        let mut page_min = u64::MAX;
        let mut page_max = 0u64;
        for row in rows {
            let price = row
                .get("p")
                .and_then(Value::as_str)
                .and_then(|s| tikr_core::Decimal::from_str(s).ok());
            let ts = row.get("T").and_then(Value::as_u64);
            let (Some(p), Some(t)) = (price, ts) else {
                continue;
            };
            if p <= tikr_core::Decimal::ZERO {
                continue;
            }
            page_min = page_min.min(t);
            page_max = page_max.max(t);
            // Open = smallest-ts trade, close = largest-ts trade in the second;
            // high/low track regardless of order, so cross-page merges are
            // order-independent.
            let bucket = (t / 1000) * 1000;
            buckets
                .entry(bucket)
                .and_modify(|c| {
                    c.high = c.high.max(p);
                    c.low = c.low.min(p);
                    if t < c.open_ts {
                        c.open_ts = t;
                        c.open = p;
                    }
                    if t > c.close_ts {
                        c.close_ts = t;
                        c.close = p;
                    }
                })
                .or_insert(Ohlc {
                    open_ts: t,
                    open: p,
                    high: p,
                    low: p,
                    close_ts: t,
                    close: p,
                });
        }
        if page_max == 0 {
            break; // no usable rows
        }
        latest_ts.get_or_insert(page_max);
        earliest_ts = Some(match earliest_ts {
            Some(prev) => prev.min(page_min),
            None => page_min,
        });

        // Covered the window? (latest_ts is set by now.)
        if let (Some(latest), Some(earliest)) = (latest_ts, earliest_ts)
            && latest.saturating_sub(earliest) >= window_ms
        {
            break;
        }
    }

    Ok(buckets
        .into_iter()
        .map(|(t, c)| (t, c.open, c.high, c.low, c.close))
        .collect())
}

/// Average full candle height over the last `limit` 1m candles, as a percent:
/// the mean of `(high − low) / low × 100`. `high`/`low` are the kline extremes,
/// so the wicks are included. A direct, responsive measure of recent
/// intra-minute volatility — large = big 1-minute swings, lots of oscillation
/// for a grid to bank. Returns `0` when no usable candles come back.
pub async fn get_1m_avg_candle_pct(
    http: &HttpClient,
    base_url: &str,
    symbol: &str,
    limit: u32,
) -> Result<tikr_core::Decimal, VenueError> {
    use std::str::FromStr;
    let limit = limit.clamp(1, 100);
    let url = format!("{base_url}/fapi/v1/klines?symbol={symbol}&interval=1m&limit={limit}");
    let resp = http.get(&url).send().await.map_err(network_err)?;
    let body: Value = read_json(resp).await?;
    if let Some(err) = try_parse_error(&body) {
        return Err(err);
    }
    let Some(rows) = body.as_array() else {
        return Ok(tikr_core::Decimal::ZERO);
    };
    let hundred = tikr_core::Decimal::from(100);
    let mut sum = tikr_core::Decimal::ZERO;
    let mut n: u32 = 0;
    for row in rows {
        // kline cols: [openTime, open, high, low, close, volume, ...]
        let cols = match row.as_array() {
            Some(c) => c,
            None => continue,
        };
        let high = cols
            .get(2)
            .and_then(Value::as_str)
            .and_then(|s| tikr_core::Decimal::from_str(s).ok());
        let low = cols
            .get(3)
            .and_then(Value::as_str)
            .and_then(|s| tikr_core::Decimal::from_str(s).ok());
        if let (Some(h), Some(l)) = (high, low)
            && h > l
            && l > tikr_core::Decimal::ZERO
        {
            sum += (h - l) / l * hundred;
            n += 1;
        }
    }
    if n == 0 {
        return Ok(tikr_core::Decimal::ZERO);
    }
    Ok(sum / tikr_core::Decimal::from(n))
}

/// Fetch the current position size for `symbol` (USD-M Perp).
///
/// Endpoint: `GET /fapi/v2/positionRisk?symbol=...`
///
/// Returns the signed `positionAmt` as a [`Decimal`]. Positive = long,
/// negative = short, zero = flat. In one-way mode the API returns a single
/// row per symbol; hedge mode returns two — we sum them which collapses to
/// the net position either way.
/// Fetch open orders for `symbol` on Futures.
///
/// Endpoint: `GET /fapi/v1/openOrders?symbol=...`
///
/// Returns `(order_id, side, price, remaining_qty)` tuples. The runner
/// uses this for periodic reconciliation against `FillSim`'s in-memory
/// `live_quotes` — Binance can silently cancel/expire post-only orders
/// (e.g. across `listenKey` reconnects) and the WS pump only emits
/// FILLED / PARTIALLY_FILLED events, so without periodic reconcile a
/// ghost would stay tracked forever.
pub async fn get_open_orders(
    http: &HttpClient,
    base_url: &str,
    api_key: &str,
    key_material: &BinanceKeyMaterial,
    symbol: &str,
) -> Result<Vec<(u64, tikr_core::Side, tikr_core::Decimal, tikr_core::Decimal)>, VenueError> {
    let params = format!("symbol={symbol}");
    let signed = append_auth_dispatch(&params, key_material);
    let url = format!("{base_url}/fapi/v1/openOrders?{signed}");
    let resp = http
        .get(&url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .map_err(network_err)?;

    let body: Value = read_json(resp).await?;
    if let Some(err) = try_parse_error(&body) {
        return Err(err);
    }

    let arr = body.as_array().ok_or_else(|| {
        VenueError::Internal(Box::new(std::io::Error::other(
            "openOrders: expected array",
        )))
    })?;

    let mut out = Vec::with_capacity(arr.len());
    for ord in arr {
        let Some(id) = ord.get("orderId").and_then(Value::as_u64) else {
            continue;
        };
        let Some(side_str) = ord.get("side").and_then(Value::as_str) else {
            continue;
        };
        let Some(price_str) = ord.get("price").and_then(Value::as_str) else {
            continue;
        };
        let orig_str = ord.get("origQty").and_then(Value::as_str).unwrap_or("0");
        let exec_str = ord
            .get("executedQty")
            .and_then(Value::as_str)
            .unwrap_or("0");
        let Ok(price) = tikr_core::Decimal::from_str_exact(price_str) else {
            continue;
        };
        let orig = tikr_core::Decimal::from_str_exact(orig_str).unwrap_or_default();
        let exec = tikr_core::Decimal::from_str_exact(exec_str).unwrap_or_default();
        let remaining = orig - exec;
        let side = if side_str == "BUY" {
            tikr_core::Side::Bid
        } else {
            tikr_core::Side::Ask
        };
        out.push((id, side, price, remaining));
    }
    Ok(out)
}

/// List the distinct symbols that currently have ANY open order on Futures, in
/// ONE call. Endpoint: `GET /fapi/v1/openOrders` with NO symbol filter (weight
/// 40). Used by `--clear` to cancel every open order account-wide BEFORE
/// flattening positions (so no resting order can fill mid-flatten and re-open a
/// position), including symbols that have orders but no position.
pub async fn list_open_order_symbols(
    http: &HttpClient,
    base_url: &str,
    api_key: &str,
    key_material: &BinanceKeyMaterial,
) -> Result<Vec<String>, VenueError> {
    let signed = append_auth_dispatch("", key_material);
    let url = format!("{base_url}/fapi/v1/openOrders?{signed}");
    let resp = http
        .get(&url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .map_err(network_err)?;

    let body: Value = read_json(resp).await?;
    if let Some(err) = try_parse_error(&body) {
        return Err(err);
    }
    let arr = body.as_array().ok_or_else(|| {
        VenueError::Internal(Box::new(std::io::Error::other(
            "openOrders(all): expected array",
        )))
    })?;
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for ord in arr {
        if let Some(sym) = ord.get("symbol").and_then(Value::as_str)
            && seen.insert(sym.to_string())
        {
            out.push(sym.to_string());
        }
    }
    Ok(out)
}

/// Fetch the net position size for `symbol` on Futures. Positive = long.
pub async fn get_position_amount(
    http: &HttpClient,
    base_url: &str,
    api_key: &str,
    key_material: &BinanceKeyMaterial,
    symbol: &str,
) -> Result<tikr_core::Decimal, VenueError> {
    let params = format!("symbol={symbol}");
    let signed = append_auth_dispatch(&params, key_material);

    let url = format!("{base_url}/fapi/v2/positionRisk?{signed}");
    let resp = http
        .get(&url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .map_err(network_err)?;

    let body: Value = read_json(resp).await?;
    if let Some(err) = try_parse_error(&body) {
        return Err(err);
    }

    let arr = body.as_array().ok_or_else(|| {
        VenueError::Internal(Box::new(std::io::Error::other(
            "positionRisk: expected array",
        )))
    })?;

    let mut net = tikr_core::Decimal::ZERO;
    for row in arr {
        let Some(amt_str) = row.get("positionAmt").and_then(Value::as_str) else {
            continue;
        };
        let amt = <tikr_core::Decimal as std::str::FromStr>::from_str(amt_str).map_err(|e| {
            VenueError::Internal(Box::new(std::io::Error::other(format!(
                "positionRisk parse '{amt_str}': {e}"
            ))))
        })?;
        net += amt;
    }
    Ok(net)
}

/// Fetch position-risk fields for `symbol` on Futures.
pub async fn get_position_risk(
    http: &HttpClient,
    base_url: &str,
    api_key: &str,
    key_material: &BinanceKeyMaterial,
    symbol: &str,
) -> Result<FuturesPositionRisk, VenueError> {
    let params = format!("symbol={symbol}");
    let signed = append_auth_dispatch(&params, key_material);
    let url = format!("{base_url}/fapi/v2/positionRisk?{signed}");
    let resp = http
        .get(&url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .map_err(network_err)?;

    let body: Value = read_json(resp).await?;
    if let Some(err) = try_parse_error(&body) {
        return Err(err);
    }

    let arr = body.as_array().ok_or_else(|| {
        VenueError::Internal(Box::new(std::io::Error::other(
            "positionRisk: expected array",
        )))
    })?;

    let mut out = FuturesPositionRisk::default();
    for row in arr {
        let parse = |field: &str| -> Result<tikr_core::Decimal, VenueError> {
            let s = row.get(field).and_then(Value::as_str).unwrap_or("0");
            <tikr_core::Decimal as std::str::FromStr>::from_str(s).map_err(|e| {
                VenueError::Internal(Box::new(std::io::Error::other(format!(
                    "positionRisk parse {field}='{s}': {e}"
                ))))
            })
        };
        out.position_amount += parse("positionAmt")?;
        out.unrealized_profit += parse("unRealizedProfit")?;
        if out.position_amount != tikr_core::Decimal::ZERO {
            out.entry_price = parse("entryPrice")?;
            out.break_even_price = parse("breakEvenPrice")?;
            out.mark_price = parse("markPrice")?;
            out.liquidation_price = parse("liquidationPrice")?;
        }
    }
    Ok(out)
}

/// List every open position on Futures in ONE call (weight 5). Returns
/// `(symbol, signed positionAmt)` for each symbol with a non-zero position.
/// Endpoint: `GET /fapi/v2/positionRisk` with NO symbol filter. Used by
/// wave_auto to adopt positions inherited across a restart whose symbol fell
/// off the top set, so they don't sit unmanaged.
pub async fn list_open_positions(
    http: &HttpClient,
    base_url: &str,
    api_key: &str,
    key_material: &BinanceKeyMaterial,
) -> Result<Vec<(String, tikr_core::Decimal)>, VenueError> {
    let signed = append_auth_dispatch("", key_material);
    let url = format!("{base_url}/fapi/v2/positionRisk?{signed}");
    let resp = http
        .get(&url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .map_err(network_err)?;

    let body: Value = read_json(resp).await?;
    if let Some(err) = try_parse_error(&body) {
        return Err(err);
    }
    let arr = body.as_array().ok_or_else(|| {
        VenueError::Internal(Box::new(std::io::Error::other(
            "positionRisk(all): expected array",
        )))
    })?;
    let mut out = Vec::new();
    for row in arr {
        let sym = row.get("symbol").and_then(Value::as_str).unwrap_or("");
        let amt_str = row
            .get("positionAmt")
            .and_then(Value::as_str)
            .unwrap_or("0");
        let amt = <tikr_core::Decimal as std::str::FromStr>::from_str(amt_str)
            .unwrap_or(tikr_core::Decimal::ZERO);
        if !sym.is_empty() && amt != tikr_core::Decimal::ZERO {
            out.push((sym.to_string(), amt));
        }
    }
    Ok(out)
}

/// Fetch the account's executed trades for `symbol` with `time >= start_ms`.
///
/// Endpoint: `GET /fapi/v1/userTrades` (signed). Each row is mapped to a
/// [`Fill`] carrying its venue `trade_id` (the `id` field) so the runner can
/// deduplicate against fills already applied from the WS user-data stream and
/// replay ONLY the ones the stream missed — preserving realized PnL + fees
/// that `force_reconcile` would otherwise discard.
///
/// `quote_id` is derived from the trade's `orderId` exactly as the WS path
/// does, so the replayed fill matches the originating resting order. `is_full`
/// is set `true` (a `userTrades` row is a completed execution; resting-order
/// liveness is reconciled separately via `open_orders`). BNB-denominated
/// commissions are NOT FX-converted here (the live USDC bots pay ~0 maker fee,
/// and the WS path handles the BNB-conversion common case) — `fee_quote`
/// carries the raw commission for non-quote assets.
pub async fn get_user_trades(
    http: &HttpClient,
    base_url: &str,
    api_key: &str,
    key_material: &BinanceKeyMaterial,
    symbol: &str,
    start_ms: u64,
) -> Result<Vec<tikr_core::Fill>, VenueError> {
    let params = format!("symbol={symbol}&startTime={start_ms}&limit=1000");
    let signed = append_auth_dispatch(&params, key_material);
    let url = format!("{base_url}/fapi/v1/userTrades?{signed}");
    let resp = http
        .get(&url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .map_err(network_err)?;

    let body: Value = read_json(resp).await?;
    if let Some(err) = try_parse_error(&body) {
        return Err(err);
    }
    parse_user_trades(&body)
}

/// Pure parser for a `GET /fapi/v1/userTrades` response body — split out from
/// [`get_user_trades`] so the row→[`Fill`] mapping (the part that matters for
/// fill-reconciliation correctness) is unit-testable without a live HTTP call.
pub(crate) fn parse_user_trades(body: &Value) -> Result<Vec<tikr_core::Fill>, VenueError> {
    use std::str::FromStr;
    use tikr_core::{Asset, Decimal, Fill, Notional, Price, Size, Timestamp};

    let arr = body.as_array().ok_or_else(|| {
        VenueError::Internal(Box::new(std::io::Error::other(
            "userTrades: expected array",
        )))
    })?;

    let parse_dec = |field: &str, raw: &str| -> Result<Decimal, VenueError> {
        Decimal::from_str(raw).map_err(|e| {
            VenueError::Internal(Box::new(std::io::Error::other(format!(
                "userTrades {field}='{raw}': {e}"
            ))))
        })
    };

    let mut out = Vec::with_capacity(arr.len());
    for row in arr {
        let trade_id = row.get("id").and_then(Value::as_u64);
        let order_id = row.get("orderId").and_then(Value::as_u64).unwrap_or(0);
        let side_str = row.get("side").and_then(Value::as_str).unwrap_or("");
        let price_s = row.get("price").and_then(Value::as_str).unwrap_or("0");
        let qty_s = row.get("qty").and_then(Value::as_str).unwrap_or("0");
        let comm_s = row.get("commission").and_then(Value::as_str).unwrap_or("0");
        let comm_asset = row
            .get("commissionAsset")
            .and_then(Value::as_str)
            .unwrap_or("USDT");
        let time_ms = row.get("time").and_then(Value::as_u64).unwrap_or(0);

        let price = parse_dec("price", price_s)?;
        let qty = parse_dec("qty", qty_s)?;
        let commission = Decimal::from_str(comm_s).unwrap_or(Decimal::ZERO);

        let side = if side_str == "BUY" {
            Side::Bid
        } else {
            Side::Ask
        };
        let quote_id = QuoteId::from_uuid(Uuid::from_u128(order_id as u128));

        out.push(Fill {
            quote_id,
            price: Price(price),
            size: Size(qty),
            fee_asset: Asset::new(comm_asset),
            fee_amount: commission,
            fee_quote: Notional(commission),
            side,
            ts: Timestamp(time_ms.saturating_mul(1_000_000)),
            is_full: true,
            trade_id,
        });
    }
    Ok(out)
}

/// Fetch USD-M futures account balance for `asset` (usually `USDT`).
///
/// Endpoint: `GET /fapi/v3/balance`
pub async fn get_balance(
    http: &HttpClient,
    base_url: &str,
    api_key: &str,
    key_material: &BinanceKeyMaterial,
    asset: &str,
) -> Result<FuturesBalance, VenueError> {
    let signed = append_auth_dispatch("", key_material);
    let url = format!("{base_url}/fapi/v3/balance?{signed}");
    let resp = http
        .get(&url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .map_err(network_err)?;

    let body: Value = read_json(resp).await?;
    if let Some(err) = try_parse_error(&body) {
        return Err(err);
    }

    let arr = body.as_array().ok_or_else(|| {
        VenueError::Internal(Box::new(std::io::Error::other("balance: expected array")))
    })?;
    let row = arr
        .iter()
        .find(|row| row.get("asset").and_then(Value::as_str) == Some(asset))
        .ok_or_else(|| {
            VenueError::Internal(Box::new(std::io::Error::other(format!(
                "balance: asset {asset} not found"
            ))))
        })?;

    let parse = |field: &str| -> Result<tikr_core::Decimal, VenueError> {
        let s = row.get(field).and_then(Value::as_str).unwrap_or("0");
        <tikr_core::Decimal as std::str::FromStr>::from_str(s).map_err(|e| {
            VenueError::Internal(Box::new(std::io::Error::other(format!(
                "balance parse {field}='{s}': {e}"
            ))))
        })
    };

    Ok(FuturesBalance {
        wallet_balance: parse("balance")?,
        available_balance: parse("availableBalance")?,
        cross_unrealized_pnl: parse("crossUnPnl")?,
    })
}

/// Get the BNB-pays-fees flag for the user's Futures account.
///
/// `GET /fapi/v1/feeBurn` — returns `{"feeBurn": true}` when the user
/// has enabled "Use BNB to pay fees". When `true`, every order's
/// commission is debited in BNB from the futures wallet (with the 10%
/// maker/taker discount) instead of being deducted from the USDT
/// margin balance. The strategy needs to know this so the fee accounting
/// + auto-refill can engage.
pub async fn get_fee_burn_status(
    http: &HttpClient,
    base_url: &str,
    api_key: &str,
    key_material: &BinanceKeyMaterial,
) -> Result<bool, VenueError> {
    let signed = append_auth_dispatch("", key_material);
    let url = format!("{base_url}/fapi/v1/feeBurn?{signed}");
    let resp = http
        .get(&url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .map_err(network_err)?;
    let body: Value = read_json(resp).await?;
    if let Some(err) = try_parse_error(&body) {
        return Err(err);
    }
    let burn = body
        .get("feeBurn")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    Ok(burn)
}

/// A USDⓈ-M Futures Convert quote (e.g. USDT → BNB), valid for ~10s. Used to
/// top up BNB for fee payment directly on the futures wallet — no spot leg or
/// transfer needed.
#[derive(Debug, Clone)]
pub struct ConvertQuote {
    /// Opaque quote id to pass to [`convert_accept_quote`].
    pub quote_id: String,
    /// `to_asset` amount this quote yields for the requested `from_amount`.
    pub to_amount: tikr_core::Decimal,
}

/// Request a Futures Convert quote: `from_amount` of `from_asset` → `to_asset`.
///
/// Endpoint: `POST /fapi/v1/convert/getQuote` (signed). The returned quote is
/// valid briefly and must be accepted via [`convert_accept_quote`].
pub async fn convert_get_quote(
    http: &HttpClient,
    base_url: &str,
    api_key: &str,
    key_material: &BinanceKeyMaterial,
    from_asset: &str,
    to_asset: &str,
    from_amount: &str,
) -> Result<ConvertQuote, VenueError> {
    use std::str::FromStr;
    let params =
        format!("fromAsset={from_asset}&toAsset={to_asset}&fromAmount={from_amount}&validTime=10s");
    let signed = append_auth_dispatch(&params, key_material);
    let url = format!("{base_url}/fapi/v1/convert/getQuote?{signed}");
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
    let quote_id = body
        .get("quoteId")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| {
            VenueError::Internal(Box::new(std::io::Error::other(format!(
                "convert getQuote: missing quoteId in response: {body}"
            ))))
        })?;
    // `toAmount` is a string; absent/garbage → 0 (caller logs the received qty).
    let to_amount = body
        .get("toAmount")
        .and_then(Value::as_str)
        .and_then(|s| tikr_core::Decimal::from_str(s).ok())
        .unwrap_or(tikr_core::Decimal::ZERO);
    Ok(ConvertQuote {
        quote_id,
        to_amount,
    })
}

/// Accept a Futures Convert quote by id.
///
/// Endpoint: `POST /fapi/v1/convert/acceptQuote` (signed). A returned
/// `orderStatus` of `FAIL` is surfaced as an error; `PROCESS` / `ACCEPT_SUCCESS`
/// / `SUCCESS` are treated as accepted.
pub async fn convert_accept_quote(
    http: &HttpClient,
    base_url: &str,
    api_key: &str,
    key_material: &BinanceKeyMaterial,
    quote_id: &str,
) -> Result<(), VenueError> {
    let params = format!("quoteId={quote_id}");
    let signed = append_auth_dispatch(&params, key_material);
    let url = format!("{base_url}/fapi/v1/convert/acceptQuote?{signed}");
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
    if let Some(status) = body.get("orderStatus").and_then(Value::as_str)
        && status.eq_ignore_ascii_case("FAIL")
    {
        return Err(VenueError::Internal(Box::new(std::io::Error::other(
            format!("convert acceptQuote returned FAIL: {body}"),
        ))));
    }
    Ok(())
}

/// Convert `from_amount` of `from_asset` into `to_asset` on the futures wallet
/// (quote + accept in one call). Returns the quoted `to_asset` amount.
pub async fn convert_futures(
    http: &HttpClient,
    base_url: &str,
    api_key: &str,
    key_material: &BinanceKeyMaterial,
    from_asset: &str,
    to_asset: &str,
    from_amount: &str,
) -> Result<tikr_core::Decimal, VenueError> {
    let quote = convert_get_quote(
        http,
        base_url,
        api_key,
        key_material,
        from_asset,
        to_asset,
        from_amount,
    )
    .await?;
    convert_accept_quote(http, base_url, api_key, key_material, &quote.quote_id).await?;
    Ok(quote.to_amount)
}

/// Per-symbol commission rate from Binance.
#[derive(Debug, Clone, Copy)]
pub struct CommissionRate {
    /// Maker fee rate (e.g. 0.0002 for 2 bps).
    pub maker: tikr_core::Decimal,
    /// Taker fee rate (e.g. 0.0004 for 4 bps).
    pub taker: tikr_core::Decimal,
}

/// Fetch the maker/taker commission rate for a single symbol.
///
/// Endpoint: `GET /fapi/v1/commissionRate?symbol=<symbol>`
pub async fn get_commission_rate(
    http: &HttpClient,
    base_url: &str,
    api_key: &str,
    key_material: &BinanceKeyMaterial,
    symbol: &str,
) -> Result<CommissionRate, VenueError> {
    use std::str::FromStr;

    let signed = append_auth_dispatch(&format!("symbol={symbol}"), key_material);
    let url = format!("{base_url}/fapi/v1/commissionRate?{signed}");
    let resp = http
        .get(&url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .map_err(network_err)?;

    let body: Value = read_json(resp).await?;
    if let Some(err) = try_parse_error(&body) {
        return Err(err);
    }

    let parse = |field: &str| -> Result<tikr_core::Decimal, VenueError> {
        let s = body.get(field).and_then(Value::as_str).unwrap_or("0");
        tikr_core::Decimal::from_str(s).map_err(|e| {
            VenueError::Internal(Box::new(std::io::Error::other(format!(
                "commissionRate parse {field}='{s}': {e}"
            ))))
        })
    };

    Ok(CommissionRate {
        maker: parse("makerCommissionRate")?,
        taker: parse("takerCommissionRate")?,
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn extract_error_code(body: &Value) -> Option<i32> {
    let code = body.get("code")?.as_i64()? as i32;
    if code < 0 { Some(code) } else { None }
}

fn try_parse_error(body: &Value) -> Option<VenueError> {
    let code = body.get("code")?.as_i64()? as i32;
    if code < 0 {
        let msg = body.get("msg").and_then(Value::as_str).unwrap_or("unknown");
        Some(parse_binance_error_code(code, msg))
    } else {
        None
    }
}

pub(crate) fn network_err(e: reqwest::Error) -> VenueError {
    VenueError::Network(std::io::Error::other(e.to_string()))
}

#[cfg(test)]
mod user_trades_tests {
    use super::parse_user_trades;
    use tikr_core::{Decimal, Side};

    #[test]
    fn parses_user_trades_with_trade_ids() {
        // Two-row userTrades response (one BUY, one SELL) shaped like the
        // real `/fapi/v1/userTrades` payload.
        let body = serde_json::json!([
            {
                "id": 698759,
                "orderId": 25851813,
                "symbol": "NEARUSDC",
                "side": "BUY",
                "price": "2.5000",
                "qty": "10",
                "commission": "0.00100000",
                "commissionAsset": "USDC",
                "time": 1569514978020u64,
                "maker": true
            },
            {
                "id": 698760,
                "orderId": 25851814,
                "symbol": "NEARUSDC",
                "side": "SELL",
                "price": "2.5100",
                "qty": "10",
                "commission": "0",
                "commissionAsset": "USDC",
                "time": 1569514979000u64,
                "maker": true
            }
        ]);

        let fills = parse_user_trades(&body).expect("parse");
        assert_eq!(fills.len(), 2);

        let buy = &fills[0];
        assert_eq!(buy.trade_id, Some(698759));
        assert_eq!(buy.side, Side::Bid);
        assert_eq!(buy.price.0, Decimal::from_str_exact("2.5000").unwrap());
        assert_eq!(buy.size.0, Decimal::from_str_exact("10").unwrap());
        assert_eq!(
            buy.fee_quote.0,
            Decimal::from_str_exact("0.00100000").unwrap()
        );
        assert!(buy.is_full, "REST-derived fills mark is_full");
        // ts is nanoseconds (ms * 1e6).
        assert_eq!(buy.ts.0, 1569514978020u64 * 1_000_000);

        let sell = &fills[1];
        assert_eq!(sell.trade_id, Some(698760));
        assert_eq!(sell.side, Side::Ask);
    }

    #[test]
    fn empty_array_yields_no_fills() {
        let body = serde_json::json!([]);
        assert!(parse_user_trades(&body).expect("parse").is_empty());
    }

    #[test]
    fn non_array_body_is_error() {
        let body = serde_json::json!({"code": -1102, "msg": "bad"});
        assert!(parse_user_trades(&body).is_err());
    }
}

#[cfg(test)]
mod batch_tests {
    use super::*;

    #[test]
    fn percent_encode_covers_json_specials() {
        // unreserved chars pass through; everything else is %XX.
        assert_eq!(percent_encode_value("aZ09-_.~"), "aZ09-_.~");
        assert_eq!(percent_encode_value("[]{}\":,"), "%5B%5D%7B%7D%22%3A%2C");
        assert_eq!(percent_encode_value(" "), "%20");
    }

    #[test]
    fn build_place_json_shape() {
        let orders = [
            BatchOrderReq {
                side: Side::Bid,
                price: "0.0331",
                quantity: "150",
                client_order_id: "abc",
                tif: TimeInForce::PostOnly,
            },
            BatchOrderReq {
                side: Side::Ask,
                price: "0.0341",
                quantity: "146",
                client_order_id: "def",
                tif: TimeInForce::PostOnly,
            },
        ];
        let json = build_batch_orders_json("PORTALUSDT", &orders);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        let arr = v.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["symbol"], "PORTALUSDT");
        assert_eq!(arr[0]["side"], "BUY");
        assert_eq!(arr[0]["type"], "LIMIT");
        assert_eq!(arr[0]["timeInForce"], "GTX");
        assert_eq!(arr[0]["price"], "0.0331");
        assert_eq!(arr[0]["quantity"], "150");
        assert_eq!(arr[0]["newClientOrderId"], "abc");
        assert_eq!(arr[1]["side"], "SELL");
    }

    #[test]
    fn place_element_ok_and_err() {
        let ok = serde_json::json!({"orderId": 123456789u64, "clientOrderId": "abc"});
        let qid = parse_place_element(&ok).unwrap();
        assert_eq!(
            qid,
            QuoteId::from_uuid(uuid::Uuid::from_u128(123456789u128))
        );
        let err = serde_json::json!({"code": -2010, "msg": "insufficient balance"});
        assert!(matches!(
            parse_place_element(&err),
            Err(VenueError::InsufficientBalance { .. })
        ));
    }

    #[test]
    fn cancel_element_idempotent() {
        // -2011/-2013 (already gone) = Ok.
        assert!(
            parse_cancel_element(&serde_json::json!({"code": -2011, "msg": "unknown"})).is_ok()
        );
        assert!(parse_cancel_element(&serde_json::json!({"code": -2013, "msg": "gone"})).is_ok());
        // a real ack = Ok.
        assert!(
            parse_cancel_element(&serde_json::json!({"orderId": 1, "status": "CANCELED"})).is_ok()
        );
        // other negative code = Err.
        assert!(parse_cancel_element(&serde_json::json!({"code": -1102, "msg": "bad"})).is_err());
    }
}
