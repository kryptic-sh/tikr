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

    let order_id = body.get("orderId").and_then(Value::as_u64).ok_or_else(|| {
        VenueError::Internal(Box::new(std::io::Error::other(format!(
            "futures place_order: missing orderId in response: {body}"
        ))))
    })?;

    info!(order_id, symbol, "futures: order placed");
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
    let params = format!(
        "symbol={symbol}&side={side_str}&type=MARKET\
         &quantity={quantity}\
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

    let order_id = body.get("orderId").and_then(Value::as_u64).ok_or_else(|| {
        VenueError::Internal(Box::new(std::io::Error::other(format!(
            "futures place_market_order: missing orderId in response: {body}"
        ))))
    })?;

    info!(order_id, symbol, "futures: market order placed");
    Ok(QuoteId::from_uuid(Uuid::from_u128(order_id as u128)))
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

    let status = resp.status();
    if status.as_u16() == 429 || status.as_u16() == 418 {
        return Err(VenueError::RateLimited {
            retry_after_ms: 1000,
        });
    }

    let body: Value = resp.json().await.map_err(internal_err)?;
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

    let status = resp.status();
    if status.as_u16() == 429 || status.as_u16() == 418 {
        return Err(VenueError::RateLimited {
            retry_after_ms: 1000,
        });
    }

    let body: Value = resp.json().await.map_err(internal_err)?;
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
    let info: ExchangeInfoResponse = resp.json().await.map_err(internal_err)?;
    Ok(info)
}

/// Fetch all USD-M futures 24h ticker stats.
pub async fn get_24hr_tickers(
    http: &HttpClient,
    base_url: &str,
) -> Result<Vec<FuturesTicker24h>, VenueError> {
    let url = format!("{base_url}/fapi/v1/ticker/24hr");
    let resp = http.get(&url).send().await.map_err(network_err)?;
    let status = resp.status();
    if status.as_u16() == 429 || status.as_u16() == 418 {
        return Err(VenueError::RateLimited {
            retry_after_ms: 1000,
        });
    }
    let body: Value = resp.json().await.map_err(internal_err)?;
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
        let quote_volume = row
            .get("quoteVolume")
            .and_then(Value::as_str)
            .and_then(|s| <tikr_core::Decimal as std::str::FromStr>::from_str(s).ok())
            .unwrap_or_default();
        out.push(FuturesTicker24h {
            symbol: symbol.to_string(),
            price_change_percent_abs: pct,
            quote_volume,
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
    let status = resp.status();
    if status.as_u16() == 429 || status.as_u16() == 418 {
        return Err(VenueError::RateLimited {
            retry_after_ms: 1000,
        });
    }
    let body: Value = resp.json().await.map_err(internal_err)?;
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
    let status = resp.status();
    if status.as_u16() == 429 || status.as_u16() == 418 {
        return Err(VenueError::RateLimited {
            retry_after_ms: 1000,
        });
    }
    let body: Value = resp.json().await.map_err(internal_err)?;
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

    let status = resp.status();
    if status.as_u16() == 429 || status.as_u16() == 418 {
        return Err(VenueError::RateLimited {
            retry_after_ms: 1000,
        });
    }

    let body: Value = resp.json().await.map_err(internal_err)?;
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

    let status = resp.status();
    if status.as_u16() == 429 || status.as_u16() == 418 {
        return Err(VenueError::RateLimited {
            retry_after_ms: 1000,
        });
    }

    let body: Value = resp.json().await.map_err(internal_err)?;
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

    let status = resp.status();
    if status.as_u16() == 429 || status.as_u16() == 418 {
        return Err(VenueError::RateLimited {
            retry_after_ms: 1000,
        });
    }

    let body: Value = resp.json().await.map_err(internal_err)?;
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
        }
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

    let status = resp.status();
    if status.as_u16() == 429 || status.as_u16() == 418 {
        return Err(VenueError::RateLimited {
            retry_after_ms: 1000,
        });
    }

    let body: Value = resp.json().await.map_err(internal_err)?;
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
    let status = resp.status();
    if status.as_u16() == 429 || status.as_u16() == 418 {
        return Err(VenueError::RateLimited {
            retry_after_ms: 1000,
        });
    }
    let body: Value = resp.json().await.map_err(internal_err)?;
    if let Some(err) = try_parse_error(&body) {
        return Err(err);
    }
    let burn = body
        .get("feeBurn")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    Ok(burn)
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

    let status = resp.status();
    if status.as_u16() == 429 || status.as_u16() == 418 {
        return Err(VenueError::RateLimited {
            retry_after_ms: 1000,
        });
    }

    let body: Value = resp.json().await.map_err(internal_err)?;
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

pub(crate) fn internal_err(e: reqwest::Error) -> VenueError {
    VenueError::Internal(Box::new(e))
}
