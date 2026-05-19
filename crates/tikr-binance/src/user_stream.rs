//! Binance userDataStream: listenKey lifecycle + fill parsing.
//!
//! ## Architecture
//!
//! 1. [`mint_listen_key`] — POST to mint a new listenKey. 60-min server expiry.
//! 2. [`keepalive_listen_key`] — PUT to extend the key. Called every 20 min.
//! 3. [`subscribe_user_data_stream`] — spawns:
//!    - A **WS task** that connects `wss://<host>/ws/<listenKey>`, reads
//!      execution reports, and sends [`Fill`]s to a channel.
//!    - A **keepalive task** that fires a PUT every 20 min.
//!
//! On WS disconnect the WS task mints a **fresh** listenKey (never reuses a
//! stale key) and reconnects.
//!
//! ## listenKey endpoints
//!
//! | Product | Mint | Keepalive |
//! |---------|------|-----------|
//! | Spot | `POST /api/v3/userDataStream` | `PUT /api/v3/userDataStream?listenKey=…` |
//! | Futures | `POST /fapi/v1/listenKey` | `PUT /fapi/v1/listenKey?listenKey=…` |
//!
//! ## Fill parsing
//!
//! - Spot: `executionReport` event with `X=TRADE` and `x=FILL`.
//! - Futures: `ORDER_TRADE_UPDATE` event with `.o.X=FILLED`.

use futures::SinkExt;
use futures::stream::StreamExt;
use reqwest::Client as HttpClient;
use serde::Deserialize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tikr_core::{
    Asset, Decimal, Fill, MarketKind, Notional, Price, QuoteId, Side, Size, Timestamp,
};
use tikr_venue::VenueError;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::BinanceEnv;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// How often to fire a listenKey keepalive PUT (20 min in ms).
pub const KEEPALIVE_INTERVAL_MS: u64 = 20 * 60 * 1000;

// ---------------------------------------------------------------------------
// listenKey management
// ---------------------------------------------------------------------------

/// Mint a new listenKey for the given environment.
///
/// Returns the raw listenKey string (no expiry info).
pub async fn mint_listen_key(
    http: &HttpClient,
    env: BinanceEnv,
    api_key: &str,
) -> Result<String, VenueError> {
    let url = listen_key_url(env);
    let resp = http
        .post(url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .map_err(|e| VenueError::Network(std::io::Error::other(e.to_string())))?;

    let status = resp.status();
    if status.as_u16() == 429 || status.as_u16() == 418 {
        return Err(VenueError::RateLimited {
            retry_after_ms: 1000,
        });
    }

    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| VenueError::Internal(Box::new(e)))?;

    let key = body
        .get("listenKey")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            VenueError::Internal(Box::new(std::io::Error::other(format!(
                "mint_listen_key: missing listenKey in response: {body}"
            ))))
        })?;

    Ok(key.to_string())
}

/// Extend the expiry of an existing listenKey (PUT).
pub async fn keepalive_listen_key(
    http: &HttpClient,
    env: BinanceEnv,
    api_key: &str,
    listen_key: &str,
) -> Result<(), VenueError> {
    let url = format!("{}?listenKey={listen_key}", listen_key_url(env));
    let resp = http
        .put(&url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .map_err(|e| VenueError::Network(std::io::Error::other(e.to_string())))?;

    let status = resp.status();
    if status.as_u16() == 429 || status.as_u16() == 418 {
        return Err(VenueError::RateLimited {
            retry_after_ms: 1000,
        });
    }
    Ok(())
}

fn listen_key_url(env: BinanceEnv) -> &'static str {
    match env {
        BinanceEnv::SpotTestnet => "https://testnet.binance.vision/api/v3/userDataStream",
        BinanceEnv::SpotMainnet => "https://api.binance.com/api/v3/userDataStream",
        BinanceEnv::FuturesTestnet => "https://testnet.binancefuture.com/fapi/v1/listenKey",
        BinanceEnv::FuturesMainnet => "https://fapi.binance.com/fapi/v1/listenKey",
    }
}

fn ws_base_url(env: BinanceEnv) -> &'static str {
    match env {
        BinanceEnv::SpotTestnet => "wss://testnet.binance.vision/ws",
        BinanceEnv::SpotMainnet => "wss://stream.binance.com:9443/ws",
        BinanceEnv::FuturesTestnet => "wss://stream.binancefuture.com/ws",
        BinanceEnv::FuturesMainnet => "wss://fstream.binance.com/ws",
    }
}

// ---------------------------------------------------------------------------
// userDataStream subscription
// ---------------------------------------------------------------------------

/// Subscribe to userDataStream fills for the given environment.
///
/// Spawns a WS task and a keepalive task. Returns an
/// `mpsc::UnboundedReceiver<Fill>` that the caller polls.
///
/// On WS reconnect a **fresh** listenKey is minted (not reused).
/// The keepalive task fires a PUT every [`KEEPALIVE_INTERVAL_MS`] ms.
pub async fn subscribe_user_data_stream(
    http: HttpClient,
    env: BinanceEnv,
    api_key: String,
    kind: MarketKind,
) -> Result<mpsc::UnboundedReceiver<Fill>, VenueError> {
    let listen_key = mint_listen_key(&http, env, &api_key).await?;
    let ws_url = format!("{}/{}", ws_base_url(env), listen_key);

    let stream = open_user_data_ws(&ws_url).await?;

    let (tx, rx) = mpsc::unbounded_channel::<Fill>();

    // Share the current listenKey between the WS task and keepalive task.
    let shared_key: Arc<Mutex<String>> = Arc::new(Mutex::new(listen_key.clone()));

    // Keepalive task: PUT every 20 min.
    let http2 = http.clone();
    let api_key2 = api_key.clone();
    let shared_key2 = shared_key.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(KEEPALIVE_INTERVAL_MS));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await; // burn first immediate tick
        loop {
            interval.tick().await;
            let key = shared_key2.lock().await.clone();
            if let Err(e) = keepalive_listen_key(&http2, env, &api_key2, &key).await {
                warn!(error = ?e, "userDataStream: keepalive PUT failed");
            } else {
                debug!("userDataStream: keepalive PUT OK");
            }
        }
    });

    // WS pump task.
    tokio::spawn(user_data_pump(
        stream, tx, http, env, api_key, kind, shared_key,
    ));

    Ok(rx)
}

async fn open_user_data_ws(ws_url: &str) -> Result<WsStream, VenueError> {
    let (stream, _) = connect_async(ws_url)
        .await
        .map_err(|e| VenueError::Network(std::io::Error::other(e.to_string())))?;
    Ok(stream)
}

async fn user_data_pump(
    mut stream: WsStream,
    tx: mpsc::UnboundedSender<Fill>,
    http: HttpClient,
    env: BinanceEnv,
    api_key: String,
    kind: MarketKind,
    shared_key: Arc<Mutex<String>>,
) {
    let reconnect_min_ms: u64 = 1_000;
    let reconnect_max_ms: u64 = 30_000;
    let mut backoff_ms = reconnect_min_ms;

    loop {
        let frame = stream.next().await;
        match frame {
            None => {
                if !reconnect_user_data(
                    &mut stream,
                    &http,
                    env,
                    &api_key,
                    &shared_key,
                    &mut backoff_ms,
                    reconnect_min_ms,
                    reconnect_max_ms,
                )
                .await
                {
                    return;
                }
            }
            Some(Err(e)) => {
                warn!(error = %e, "userDataStream WS read error; reconnecting");
                if !reconnect_user_data(
                    &mut stream,
                    &http,
                    env,
                    &api_key,
                    &shared_key,
                    &mut backoff_ms,
                    reconnect_min_ms,
                    reconnect_max_ms,
                )
                .await
                {
                    return;
                }
            }
            Some(Ok(Message::Text(txt))) => {
                backoff_ms = reconnect_min_ms;
                if let Some(fill) = parse_user_data_message(&txt, kind) {
                    info!(
                        quote_id = ?fill.quote_id,
                        price = %fill.price.0,
                        size = %fill.size.0,
                        "userDataStream: fill received"
                    );
                    if tx.send(fill).is_err() {
                        return; // receiver dropped
                    }
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = stream.send(Message::Pong(p)).await;
            }
            Some(Ok(Message::Close(_))) => {
                debug!("userDataStream WS server-initiated close; reconnecting");
                if !reconnect_user_data(
                    &mut stream,
                    &http,
                    env,
                    &api_key,
                    &shared_key,
                    &mut backoff_ms,
                    reconnect_min_ms,
                    reconnect_max_ms,
                )
                .await
                {
                    return;
                }
            }
            Some(Ok(_)) => {} // Binary / Pong — ignore
        }
    }
}

/// Reconnect: mint a fresh listenKey, update shared key, reconnect WS.
/// Returns false only when the channel receiver has been dropped.
#[allow(clippy::too_many_arguments)]
async fn reconnect_user_data(
    stream: &mut WsStream,
    http: &HttpClient,
    env: BinanceEnv,
    api_key: &str,
    shared_key: &Arc<Mutex<String>>,
    backoff_ms: &mut u64,
    reconnect_min_ms: u64,
    reconnect_max_ms: u64,
) -> bool {
    loop {
        warn!(
            backoff_ms = *backoff_ms,
            "userDataStream WS disconnected; reconnecting"
        );
        tokio::time::sleep(Duration::from_millis(*backoff_ms)).await;

        // Mint a FRESH listenKey (never reuse stale keys after disconnect).
        let new_key = match mint_listen_key(http, env, api_key).await {
            Ok(k) => k,
            Err(e) => {
                warn!(error = ?e, "userDataStream: failed to mint new listenKey");
                *backoff_ms = (*backoff_ms).saturating_mul(2).min(reconnect_max_ms);
                continue;
            }
        };

        *shared_key.lock().await = new_key.clone();
        let ws_url = format!("{}/{}", ws_base_url(env), new_key);

        match open_user_data_ws(&ws_url).await {
            Ok(new_stream) => {
                *stream = new_stream;
                *backoff_ms = reconnect_min_ms;
                return true;
            }
            Err(e) => {
                warn!(error = ?e, "userDataStream WS reconnect failed");
                *backoff_ms = (*backoff_ms).saturating_mul(2).min(reconnect_max_ms);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Fill parsing
// ---------------------------------------------------------------------------

/// Parse a userDataStream text frame into a [`Fill`], if applicable.
///
/// Spot: `executionReport` with `x=FILL`.
/// Futures: `ORDER_TRADE_UPDATE` with `.o.X=FILLED`.
pub fn parse_user_data_message(txt: &str, kind: MarketKind) -> Option<Fill> {
    let v: serde_json::Value = serde_json::from_str(txt).ok()?;
    match kind {
        MarketKind::Spot => parse_execution_report(&v),
        MarketKind::Perp => parse_order_trade_update(&v),
    }
}

// ---------------------------------------------------------------------------
// Spot: executionReport
// ---------------------------------------------------------------------------

/// Spot userDataStream execution report JSON shape.
///
/// Only `FILL` execution types represent a fill; other types (NEW, CANCELED,
/// etc.) are ignored.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionReport {
    /// Event type (`"executionReport"`).
    #[serde(rename = "e")]
    pub event_type: String,
    /// Event time (ms).
    #[serde(rename = "E")]
    pub event_time: u64,
    /// Venue-assigned order id.
    #[serde(rename = "i")]
    pub order_id: u64,
    /// Client order id.
    #[serde(rename = "c")]
    pub client_order_id: String,
    /// Side (`"BUY"` or `"SELL"`).
    #[serde(rename = "S")]
    pub side: String,
    /// Last filled price.
    #[serde(rename = "L")]
    pub last_price: String,
    /// Last filled quantity.
    #[serde(rename = "l")]
    pub last_qty: String,
    /// Commission amount.
    #[serde(rename = "n")]
    pub commission: String,
    /// Commission asset.
    #[serde(rename = "N")]
    pub commission_asset: Option<String>,
    /// Execution type (`"FILL"`, `"NEW"`, `"CANCELED"`, etc.).
    #[serde(rename = "x")]
    pub execution_type: String,
    /// Transaction time (ms).
    #[serde(rename = "T")]
    pub transaction_time: u64,
}

fn parse_execution_report(v: &serde_json::Value) -> Option<Fill> {
    let event_type = v.get("e").and_then(serde_json::Value::as_str)?;
    if event_type != "executionReport" {
        return None;
    }
    let exec_type = v.get("x").and_then(serde_json::Value::as_str)?;
    if exec_type != "FILL" {
        return None;
    }

    let order_id = v.get("i").and_then(serde_json::Value::as_u64)?;
    let side_str = v.get("S").and_then(serde_json::Value::as_str)?;
    let price_str = v.get("L").and_then(serde_json::Value::as_str)?;
    let qty_str = v.get("l").and_then(serde_json::Value::as_str)?;
    let commission_str = v
        .get("n")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("0");
    let commission_asset = v
        .get("N")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("USDT");
    let ts_ms = v.get("T").and_then(serde_json::Value::as_u64).unwrap_or(0);

    let price = Decimal::from_str(price_str).ok()?;
    let qty = Decimal::from_str(qty_str).ok()?;
    let commission = Decimal::from_str(commission_str).unwrap_or(Decimal::ZERO);

    let side = if side_str == "BUY" {
        Side::Bid
    } else {
        Side::Ask
    };
    let quote_id = QuoteId::from_uuid(Uuid::from_u128(order_id as u128));

    Some(Fill {
        quote_id,
        price: Price(price),
        size: Size(qty),
        fee_asset: Asset::new(commission_asset),
        fee_amount: commission,
        fee_quote: Notional(commission),
        side,
        ts: Timestamp(ts_ms.saturating_mul(1_000_000)),
    })
}

// ---------------------------------------------------------------------------
// Futures: ORDER_TRADE_UPDATE
// ---------------------------------------------------------------------------

fn parse_order_trade_update(v: &serde_json::Value) -> Option<Fill> {
    let event_type = v.get("e").and_then(serde_json::Value::as_str)?;
    if event_type != "ORDER_TRADE_UPDATE" {
        return None;
    }

    let o = v.get("o")?;

    // Only process FILLED status.
    let status = o.get("X").and_then(serde_json::Value::as_str)?;
    if status != "FILLED" && status != "PARTIALLY_FILLED" {
        return None;
    }

    let order_id = o.get("i").and_then(serde_json::Value::as_u64)?;
    let side_str = o.get("S").and_then(serde_json::Value::as_str)?;
    let price_str = o.get("L").and_then(serde_json::Value::as_str)?;
    let qty_str = o.get("l").and_then(serde_json::Value::as_str)?;
    let commission_str = o
        .get("n")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("0");
    let commission_asset = o
        .get("N")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("USDT");
    let ts_ms = v.get("E").and_then(serde_json::Value::as_u64).unwrap_or(0);

    let price = Decimal::from_str(price_str).ok()?;
    let qty = Decimal::from_str(qty_str).ok()?;
    let commission = Decimal::from_str(commission_str).unwrap_or(Decimal::ZERO);

    let side = if side_str == "BUY" {
        Side::Bid
    } else {
        Side::Ask
    };
    let quote_id = QuoteId::from_uuid(Uuid::from_u128(order_id as u128));

    Some(Fill {
        quote_id,
        price: Price(price),
        size: Size(qty),
        fee_asset: Asset::new(commission_asset),
        fee_amount: commission,
        fee_quote: Notional(commission),
        side,
        ts: Timestamp(ts_ms.saturating_mul(1_000_000)),
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::MarketKind;

    fn spot_fill_report() -> &'static str {
        r#"{
            "e": "executionReport",
            "E": 1499405658658,
            "s": "BTCUSDT",
            "c": "tikr_00000000000000000000000000000001",
            "S": "BUY",
            "o": "LIMIT",
            "f": "GTC",
            "q": "0.001",
            "p": "30000.00",
            "P": "0.00000000",
            "F": "0.00000000",
            "g": -1,
            "C": "",
            "x": "FILL",
            "X": "FILLED",
            "r": "NONE",
            "i": 12345678,
            "l": "0.001",
            "z": "0.001",
            "L": "30000.00",
            "n": "0.03",
            "N": "USDT",
            "T": 1499405658657,
            "t": 1,
            "I": 8641984,
            "w": false,
            "m": true,
            "M": false,
            "O": 1499405658657,
            "Z": "30.000",
            "Y": "30.000",
            "Q": "0.00000000",
            "W": 1499405658657,
            "V": "NONE"
        }"#
    }

    fn futures_fill_report() -> &'static str {
        r#"{
            "e": "ORDER_TRADE_UPDATE",
            "E": 1568879465651,
            "T": 1568879465650,
            "i": "SfsR",
            "o": {
                "s": "BTCUSDT",
                "c": "tikr_00000000000000000000000000000002",
                "S": "SELL",
                "o": "LIMIT",
                "f": "GTC",
                "q": "0.001",
                "p": "31000.00",
                "ap": "31000.00",
                "sp": "0",
                "x": "TRADE",
                "X": "FILLED",
                "i": 8886774,
                "l": "0.001",
                "z": "0.001",
                "L": "31000.00",
                "N": "USDT",
                "n": "0.031",
                "T": 1568879465650,
                "t": 1,
                "b": "0",
                "a": "0",
                "m": false,
                "R": false,
                "wt": "CONTRACT_PRICE",
                "ot": "LIMIT",
                "ps": "BOTH",
                "cp": false,
                "rp": "0",
                "pP": false,
                "si": 0,
                "ss": 0,
                "V": "NONE",
                "pm": "NONE",
                "gtd": 0
            }
        }"#
    }

    #[test]
    fn parse_execution_report_to_fill_spot() {
        let fill = parse_user_data_message(spot_fill_report(), MarketKind::Spot)
            .expect("should parse spot executionReport");

        assert_eq!(fill.price.0, Decimal::from_str("30000.00").unwrap());
        assert_eq!(fill.size.0, Decimal::from_str("0.001").unwrap());
        assert_eq!(fill.side, Side::Bid);
        assert_eq!(fill.fee_asset, Asset::new("USDT"));
        // orderId=12345678 → QuoteId(Uuid::from_u128(12345678))
        let expected_id = QuoteId::from_uuid(Uuid::from_u128(12345678));
        assert_eq!(fill.quote_id, expected_id);
    }

    #[test]
    fn parse_order_trade_update_to_fill_futures() {
        let fill = parse_user_data_message(futures_fill_report(), MarketKind::Perp)
            .expect("should parse futures ORDER_TRADE_UPDATE");

        assert_eq!(fill.price.0, Decimal::from_str("31000.00").unwrap());
        assert_eq!(fill.size.0, Decimal::from_str("0.001").unwrap());
        assert_eq!(fill.side, Side::Ask);
        assert_eq!(fill.fee_asset, Asset::new("USDT"));
        let expected_id = QuoteId::from_uuid(Uuid::from_u128(8886774));
        assert_eq!(fill.quote_id, expected_id);
    }

    #[test]
    fn listen_key_keepalive_interval_is_20min() {
        // Constants check: 20 min = 20 * 60 * 1000 ms.
        assert_eq!(KEEPALIVE_INTERVAL_MS, 20 * 60 * 1000);
    }

    #[test]
    fn non_fill_execution_report_ignored() {
        // "x": "NEW" should not produce a fill.
        let msg =
            r#"{"e":"executionReport","x":"NEW","i":999,"S":"BUY","L":"0","l":"0","n":"0","T":0}"#;
        let result = parse_user_data_message(msg, MarketKind::Spot);
        assert!(
            result.is_none(),
            "NEW execution type must not produce a fill"
        );
    }

    #[test]
    fn non_order_event_ignored() {
        // Spot balance update event should not produce a fill.
        let msg = r#"{"e":"outboundAccountPosition","E":1234567890}"#;
        let result = parse_user_data_message(msg, MarketKind::Spot);
        assert!(result.is_none());
    }
}
