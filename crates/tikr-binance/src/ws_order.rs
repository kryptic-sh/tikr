//! Persistent authenticated WebSocket order client for Binance USD-M Futures.
//!
//! Uses the Binance WS-API (`wss://ws-fapi.binance.com/ws-fapi/v1`) to
//! place, modify, and cancel orders over a single long-lived WebSocket
//! connection rather than REST round-trips.
//!
//! ## Protocol
//!
//! 1. Connect to the futures WS-API endpoint.
//! 2. Send `session.logon` (Ed25519-signed — **only Ed25519 is accepted**).
//! 3. After logon ack, order methods (`order.place`, `order.modify`,
//!    `order.cancel`) send signed-timestamp JSON frames and await numeric-id
//!    correlated replies.
//!
//! ## Design
//!
//! Actor pattern: a single background task owns the socket. `WsOrderClient`
//! holds an `mpsc::Sender<Cmd>` to the actor and an `AtomicU64` for monotonic
//! request ids. The actor:
//!
//! - Dispatches outbound frames and correlates inbound replies via a
//!   `HashMap<u64, oneshot::Sender<_>>` pending map.
//! - Handles Ping → Pong automatically.
//! - On any disconnect: drains the pending map with `Network` errors, then
//!   reconnects with exponential backoff (500ms → 4s cap) + fresh logon.
//!
//! ## Futures-only
//!
//! [`WsOrderClient::connect`] returns `VenueError::Rejected` immediately for
//! non-futures environments (`SpotMainnet`, `SpotTestnet`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use ed25519_dalek::SigningKey;
use futures::SinkExt;
use futures::stream::StreamExt;
use tikr_core::{QuoteId, Side, TimeInForce};
use tikr_venue::VenueError;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tracing::{info, warn};
use uuid::Uuid;

use crate::BinanceEnv;
use crate::errors::parse_binance_error_code;
use crate::sign::{BinanceKeyMaterial, sign_query_ed25519, timestamp_ms};
use crate::user_stream::session_logon_signed_string;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// A pending request waiting for a correlated reply from the server.
struct Cmd {
    id: u64,
    msg: String,
    reply: oneshot::Sender<Result<serde_json::Value, VenueError>>,
}

// ---------------------------------------------------------------------------
// WsOrderClient
// ---------------------------------------------------------------------------

/// Authenticated WebSocket order client for Binance USD-M Futures.
///
/// Constructed via [`WsOrderClient::connect`]. The client is `Clone`-free by
/// design — clone the enclosing `Arc<WsOrderClient>` in caller code.
///
/// # Thread-safety
///
/// The `tx` sender is `Send + Sync`; `next_id` is `AtomicU64`. Multiple async
/// tasks may call `place`/`modify`/`cancel` concurrently.
pub struct WsOrderClient {
    tx: mpsc::Sender<Cmd>,
    next_id: AtomicU64,
}

impl std::fmt::Debug for WsOrderClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WsOrderClient").finish_non_exhaustive()
    }
}

impl WsOrderClient {
    /// Connect to the Binance USD-M Futures WS-API and perform `session.logon`.
    ///
    /// Spawns a background actor task that owns the socket.
    ///
    /// # Errors
    ///
    /// - `Rejected` — non-futures env, or HMAC key (Ed25519 required).
    /// - `Network` — socket connect failure.
    /// - `Rejected` — logon rejected by the server (bad key / clock drift).
    pub async fn connect(
        env: BinanceEnv,
        api_key: &str,
        key_material: &BinanceKeyMaterial,
    ) -> Result<Self, VenueError> {
        // Futures-only.
        if !env.is_futures() {
            return Err(VenueError::Rejected {
                reason: "WsOrderClient is futures-only; use REST for spot orders".into(),
            });
        }

        // Ed25519 required.
        let signing_key = match key_material {
            BinanceKeyMaterial::Ed25519 { signing_key } => signing_key.clone(),
            BinanceKeyMaterial::Hmac { .. } => {
                return Err(VenueError::Rejected {
                    reason: "WS order API requires Ed25519 key material; \
                             HMAC is not supported by Binance for the WS-API. \
                             Set TIKR_BINANCE_KEY_TYPE=ed25519."
                        .into(),
                });
            }
        };

        // First connect + logon — surface errors synchronously before spawning.
        let url = ws_fapi_url(env);
        let stream = ws_connect_and_logon(url, api_key, &signing_key).await?;

        let (tx, rx) = mpsc::channel::<Cmd>(256);
        tokio::spawn(actor_loop(
            stream,
            rx,
            api_key.to_string(),
            signing_key,
            env,
        ));

        Ok(Self {
            tx,
            next_id: AtomicU64::new(1),
        })
    }

    // -----------------------------------------------------------------------
    // Public order methods
    // -----------------------------------------------------------------------

    /// Place a new LIMIT order.
    ///
    /// `price` and `quantity` are pre-rounded wire strings, e.g. `"29500.00"`.
    /// Returns the venue-assigned `QuoteId`.
    pub async fn place(
        &self,
        symbol: &str,
        side: Side,
        price: &str,
        quantity: &str,
        client_order_id: &str,
        tif: TimeInForce,
    ) -> Result<QuoteId, VenueError> {
        let params = build_place_params(symbol, side, price, quantity, client_order_id, tif);
        let result = self.request("order.place", params).await?;
        let order_id = result
            .get("orderId")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| {
                VenueError::Internal(Box::new(std::io::Error::other(format!(
                    "ws order.place: missing orderId in result: {result}"
                ))))
            })?;
        Ok(QuoteId::from_uuid(Uuid::from_u128(order_id as u128)))
    }

    /// Modify an existing order (price and/or quantity).
    ///
    /// Returns the updated `QuoteId` (from result.orderId; falls back to the
    /// passed `order_id` if absent).
    pub async fn modify(
        &self,
        symbol: &str,
        side: Side,
        price: &str,
        quantity: &str,
        order_id: u64,
    ) -> Result<QuoteId, VenueError> {
        let params = build_modify_params(symbol, side, price, quantity, order_id);
        let result = self.request("order.modify", params).await?;
        let returned_id = result
            .get("orderId")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(order_id);
        Ok(QuoteId::from_uuid(Uuid::from_u128(returned_id as u128)))
    }

    /// Cancel an order by client order id.
    ///
    /// Idempotent: `-2011`/`-2013` (unknown / already canceled) are treated as
    /// success, consistent with the REST cancel path.
    pub async fn cancel(&self, symbol: &str, client_order_id: &str) -> Result<(), VenueError> {
        let params = build_cancel_params(symbol, client_order_id);
        match self.request("order.cancel", params).await {
            Ok(_) => Ok(()),
            Err(VenueError::UnknownQuote) => Ok(()), // idempotent
            Err(e) => Err(e),
        }
    }

    // -----------------------------------------------------------------------
    // Internal request helper
    // -----------------------------------------------------------------------

    async fn request(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, VenueError> {
        let id = self.next_id.fetch_add(1, Relaxed);
        let msg = serde_json::json!({
            "id": id,
            "method": method,
            "params": params,
        })
        .to_string();

        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Cmd {
                id,
                msg,
                reply: reply_tx,
            })
            .await
            .map_err(|_| {
                VenueError::Network(std::io::Error::other(
                    "ws order actor closed — connection lost",
                ))
            })?;

        match tokio::time::timeout(Duration::from_secs(5), reply_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(VenueError::Network(std::io::Error::other(
                "ws order reply channel closed",
            ))),
            Err(_) => Err(VenueError::Network(std::io::Error::other(
                "ws order request timed out",
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// URL helper
// ---------------------------------------------------------------------------

fn ws_fapi_url(env: BinanceEnv) -> &'static str {
    match env {
        BinanceEnv::FuturesMainnet => "wss://ws-fapi.binance.com/ws-fapi/v1",
        BinanceEnv::FuturesTestnet => "wss://testnet.binancefuture.com/ws-fapi/v1",
        // Non-futures guarded before this is called.
        _ => unreachable!("ws_fapi_url called for non-futures env"),
    }
}

// ---------------------------------------------------------------------------
// Connect + logon helper
// ---------------------------------------------------------------------------

async fn ws_connect_and_logon(
    url: &str,
    api_key: &str,
    signing_key: &SigningKey,
) -> Result<WsStream, VenueError> {
    let (mut stream, _) = connect_async(url)
        .await
        .map_err(|e| VenueError::Network(std::io::Error::other(e.to_string())))?;

    let ts = timestamp_ms();
    let signed_str = session_logon_signed_string(api_key, ts);
    let signature = sign_query_ed25519(signing_key, &signed_str);

    // id is a string for logon (we parse numeric ids for order responses)
    let logon_msg = serde_json::json!({
        "id": "tikr-wsorder-logon",
        "method": "session.logon",
        "params": {
            "apiKey": api_key,
            "recvWindow": 5000,
            "timestamp": ts,
            "signature": signature,
        }
    });

    stream
        .send(Message::Text(logon_msg.to_string()))
        .await
        .map_err(|e| VenueError::Network(std::io::Error::other(e.to_string())))?;

    // Wait for session.logon ack.
    wait_for_string_id_ack(&mut stream, "tikr-wsorder-logon", "session.logon").await?;

    info!(env_url = url, "ws order: session.logon OK");
    Ok(stream)
}

/// Wait for a response frame with a string `id` matching `expected_id` and
/// `status == 200`. Ignores unrelated frames. Bounded by a 10s timeout so a
/// connected-but-silent server can't hang connect/reconnect forever.
async fn wait_for_string_id_ack(
    stream: &mut WsStream,
    expected_id: &str,
    method: &str,
) -> Result<(), VenueError> {
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        wait_for_string_id_ack_inner(stream, expected_id, method),
    )
    .await
    .map_err(|_| {
        VenueError::Network(std::io::Error::other(format!(
            "ws order: timed out waiting for {method} ack"
        )))
    })?
}

async fn wait_for_string_id_ack_inner(
    stream: &mut WsStream,
    expected_id: &str,
    method: &str,
) -> Result<(), VenueError> {
    loop {
        match stream.next().await {
            None => {
                return Err(VenueError::Network(std::io::Error::other(format!(
                    "ws order: connection closed waiting for {method} ack"
                ))));
            }
            Some(Err(e)) => {
                return Err(VenueError::Network(std::io::Error::other(format!(
                    "ws order: read error waiting for {method} ack: {e}"
                ))));
            }
            Some(Ok(Message::Text(txt))) => {
                let v: serde_json::Value = serde_json::from_str(&txt).map_err(|e| {
                    VenueError::Internal(Box::new(std::io::Error::other(format!(
                        "ws order: non-JSON frame waiting for {method}: {e}: {txt}"
                    ))))
                })?;
                let id = v.get("id").and_then(serde_json::Value::as_str);
                if id != Some(expected_id) {
                    continue; // not our frame
                }
                let status = v
                    .get("status")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                if status != 200 {
                    let err_msg = v
                        .get("error")
                        .map(|e| e.to_string())
                        .unwrap_or_else(|| txt.clone());
                    return Err(VenueError::Rejected {
                        reason: format!("{method} failed (status {status}): {err_msg}"),
                    });
                }
                return Ok(());
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = stream.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {} // Binary / Pong / Close — ignore for logon
        }
    }
}

// ---------------------------------------------------------------------------
// Actor loop
// ---------------------------------------------------------------------------

async fn actor_loop(
    initial_stream: WsStream,
    mut rx: mpsc::Receiver<Cmd>,
    api_key: String,
    signing_key: SigningKey,
    env: BinanceEnv,
) {
    let reconnect_min_ms: u64 = 500;
    let reconnect_max_ms: u64 = 4_000;
    let mut backoff_ms = reconnect_min_ms;

    // First iteration uses the already-connected stream from connect().
    let mut maybe_stream: Option<WsStream> = Some(initial_stream);

    'outer: loop {
        // Obtain a connected+logged-on stream.
        let stream = match maybe_stream.take() {
            Some(s) => s,
            None => {
                // Reconnect with backoff.
                loop {
                    warn!(backoff_ms, "ws order: reconnecting");
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    let url = ws_fapi_url(env);
                    match ws_connect_and_logon(url, &api_key, &signing_key).await {
                        Ok(s) => {
                            backoff_ms = reconnect_min_ms;
                            break s;
                        }
                        Err(e) => {
                            warn!(error = ?e, "ws order: reconnect+logon failed");
                            backoff_ms = backoff_ms.saturating_mul(2).min(reconnect_max_ms);
                        }
                    }
                }
            }
        };

        let (mut write, mut read) = stream.split();
        let mut pending: HashMap<u64, oneshot::Sender<Result<serde_json::Value, VenueError>>> =
            HashMap::new();

        // Inner loop: select on inbound and outbound.
        loop {
            tokio::select! {
                // --- Outbound: new command from caller ---
                cmd = rx.recv() => {
                    match cmd {
                        None => {
                            // Client dropped — drain pending and exit the task.
                            drain_pending(&mut pending);
                            return;
                        }
                        Some(c) => {
                            pending.insert(c.id, c.reply);
                            if let Err(e) = write.send(Message::Text(c.msg)).await {
                                warn!(error = %e, "ws order: send error; will reconnect");
                                // Fail this request immediately (we already inserted).
                                drain_pending(&mut pending);
                                break; // break inner to reconnect
                            }
                        }
                    }
                }

                // --- Inbound: frame from server ---
                frame = read.next() => {
                    match frame {
                        Some(Ok(Message::Text(txt))) => {
                            let v: serde_json::Value = match serde_json::from_str(&txt) {
                                Ok(v) => v,
                                Err(e) => {
                                    warn!(error = %e, frame = %txt, "ws order: non-JSON frame");
                                    continue;
                                }
                            };
                            // Correlate by numeric id. Frames with no numeric id are dropped.
                            if let Some(id) = v.get("id").and_then(serde_json::Value::as_u64)
                                && let Some(reply_tx) = pending.remove(&id)
                            {
                                let _ = reply_tx.send(interpret_response(v));
                            }
                        }
                        Some(Ok(Message::Ping(p))) => {
                            let _ = write.send(Message::Pong(p)).await;
                        }
                        Some(Ok(Message::Close(_))) => {
                            warn!("ws order: server Close; reconnecting");
                            drain_pending(&mut pending);
                            break; // break inner to reconnect
                        }
                        None | Some(Err(_)) => {
                            warn!("ws order: connection dropped; reconnecting");
                            drain_pending(&mut pending);
                            break; // break inner to reconnect
                        }
                        Some(Ok(_)) => {} // Binary / Pong — ignore
                    }
                }
            }

            // Check if the client has been dropped (tx half gone).
            if rx.is_closed() {
                drain_pending(&mut pending);
                return;
            }
        }

        // Outer loop will reconnect on next iteration.
        continue 'outer;
    }
}

/// Fail all pending requests with a Network error.
fn drain_pending(
    pending: &mut HashMap<u64, oneshot::Sender<Result<serde_json::Value, VenueError>>>,
) {
    for (_, reply_tx) in pending.drain() {
        let _ = reply_tx.send(Err(VenueError::Network(std::io::Error::other(
            "ws order connection dropped",
        ))));
    }
}

// ---------------------------------------------------------------------------
// Response interpretation
// ---------------------------------------------------------------------------

/// Map a Binance WS-API response JSON to `Ok(result)` or `Err(VenueError)`.
///
/// Success: `{"status": 200, "result": {...}}` → `Ok(result)`.
/// Error:   `{"status": <N>, "error": {"code": <i64>, "msg": "<str>"}}` → `Err(...)`.
fn interpret_response(v: serde_json::Value) -> Result<serde_json::Value, VenueError> {
    let status = v
        .get("status")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    if status == 200 {
        return Ok(v.get("result").cloned().unwrap_or(serde_json::Value::Null));
    }
    // Default to 0 (generic rejection) when the error frame lacks a numeric
    // code — a benign default like -5027 ("no need to modify") would make
    // malformed error replies read as successful no-op requotes.
    let code = v
        .get("error")
        .and_then(|e| e.get("code"))
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0) as i32;
    let msg = v
        .get("error")
        .and_then(|e| e.get("msg"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown WS-API error");
    Err(parse_binance_error_code(code, msg))
}

// ---------------------------------------------------------------------------
// Param builders (pub(crate) for unit-test access)
// ---------------------------------------------------------------------------

/// Build `order.place` params. Extracted for unit-testability.
pub(crate) fn build_place_params(
    symbol: &str,
    side: Side,
    price: &str,
    quantity: &str,
    client_order_id: &str,
    tif: TimeInForce,
) -> serde_json::Value {
    let side_str = side_to_str(side);
    let tif_str = tif_to_str(tif);
    serde_json::json!({
        "symbol": symbol,
        "side": side_str,
        "type": "LIMIT",
        "timeInForce": tif_str,
        "quantity": quantity,
        "price": price,
        "newClientOrderId": client_order_id,
        "timestamp": timestamp_ms(),
        "recvWindow": 5000,
    })
}

/// Build `order.modify` params. Extracted for unit-testability.
pub(crate) fn build_modify_params(
    symbol: &str,
    side: Side,
    price: &str,
    quantity: &str,
    order_id: u64,
) -> serde_json::Value {
    let side_str = side_to_str(side);
    serde_json::json!({
        "symbol": symbol,
        "side": side_str,
        "quantity": quantity,
        "price": price,
        "orderId": order_id,
        "timestamp": timestamp_ms(),
        "recvWindow": 5000,
    })
}

/// Build `order.cancel` params. Extracted for unit-testability.
pub(crate) fn build_cancel_params(symbol: &str, client_order_id: &str) -> serde_json::Value {
    serde_json::json!({
        "symbol": symbol,
        "origClientOrderId": client_order_id,
        "timestamp": timestamp_ms(),
        "recvWindow": 5000,
    })
}

// ---------------------------------------------------------------------------
// Mapping helpers
// ---------------------------------------------------------------------------

fn side_to_str(side: Side) -> &'static str {
    match side {
        Side::Bid => "BUY",
        Side::Ask => "SELL",
    }
}

fn tif_to_str(tif: TimeInForce) -> &'static str {
    match tif {
        TimeInForce::PostOnly => "GTX",
        TimeInForce::IOC => "IOC",
        TimeInForce::FOK => "FOK",
        TimeInForce::GTC => "GTC",
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::{Side, TimeInForce};

    // -----------------------------------------------------------------------
    // build_place_params
    // -----------------------------------------------------------------------

    #[test]
    fn place_params_method_and_fields() {
        let params = build_place_params(
            "BTCUSDT",
            Side::Bid,
            "29500.00",
            "0.001",
            "tikr_order_001",
            TimeInForce::PostOnly,
        );

        assert_eq!(params["symbol"], "BTCUSDT");
        assert_eq!(params["side"], "BUY");
        assert_eq!(params["type"], "LIMIT");
        assert_eq!(params["timeInForce"], "GTX", "PostOnly must map to GTX");
        // price and quantity must be JSON strings, not numbers.
        assert!(
            params["price"].is_string(),
            "price must be a JSON string; got: {:?}",
            params["price"]
        );
        assert_eq!(params["price"], "29500.00");
        assert!(
            params["quantity"].is_string(),
            "quantity must be a JSON string; got: {:?}",
            params["quantity"]
        );
        assert_eq!(params["quantity"], "0.001");
        assert_eq!(params["newClientOrderId"], "tikr_order_001");
        // No apiKey or signature in post-logon params.
        assert!(
            params.get("apiKey").is_none(),
            "params must not contain apiKey after session.logon"
        );
        assert!(
            params.get("signature").is_none(),
            "params must not contain signature after session.logon"
        );
        // timestamp must be present.
        assert!(
            params["timestamp"].is_number(),
            "timestamp must be present as a number"
        );
        assert_eq!(params["recvWindow"], 5000);
    }

    #[test]
    fn place_params_tif_mappings() {
        let cases = [
            (TimeInForce::PostOnly, "GTX"),
            (TimeInForce::IOC, "IOC"),
            (TimeInForce::FOK, "FOK"),
            (TimeInForce::GTC, "GTC"),
        ];
        for (tif, expected) in cases {
            let params = build_place_params("ETHUSDT", Side::Ask, "2000.00", "0.1", "cid", tif);
            assert_eq!(
                params["timeInForce"], expected,
                "TIF {:?} must map to {}",
                tif, expected
            );
        }
    }

    #[test]
    fn place_params_ask_side() {
        let params = build_place_params(
            "SOLUSDT",
            Side::Ask,
            "150.00",
            "10.0",
            "cid",
            TimeInForce::GTC,
        );
        assert_eq!(params["side"], "SELL");
    }

    // -----------------------------------------------------------------------
    // build_modify_params
    // -----------------------------------------------------------------------

    #[test]
    fn modify_params_fields() {
        let params = build_modify_params("BTCUSDT", Side::Bid, "29600.00", "0.002", 123456789u64);

        assert_eq!(params["symbol"], "BTCUSDT");
        assert_eq!(params["side"], "BUY");
        assert!(
            params["quantity"].is_string(),
            "quantity must be a JSON string"
        );
        assert_eq!(params["quantity"], "0.002");
        assert!(params["price"].is_string(), "price must be a JSON string");
        assert_eq!(params["price"], "29600.00");
        // orderId must be a JSON number.
        assert!(
            params["orderId"].is_number(),
            "orderId must be a JSON number; got: {:?}",
            params["orderId"]
        );
        assert_eq!(params["orderId"], 123456789u64);
        assert!(params.get("apiKey").is_none());
        assert!(params.get("signature").is_none());
        assert!(params["timestamp"].is_number());
        assert_eq!(params["recvWindow"], 5000);
    }

    // -----------------------------------------------------------------------
    // build_cancel_params
    // -----------------------------------------------------------------------

    #[test]
    fn cancel_params_fields() {
        let params = build_cancel_params("NEARUSDT", "tikr_cid_999");

        assert_eq!(params["symbol"], "NEARUSDT");
        assert_eq!(params["origClientOrderId"], "tikr_cid_999");
        assert!(params.get("apiKey").is_none());
        assert!(params.get("signature").is_none());
        assert!(params["timestamp"].is_number());
        assert_eq!(params["recvWindow"], 5000);
        // Must not have orderId field (cancel by client order id, not order id).
        assert!(
            params.get("orderId").is_none(),
            "cancel params must not include orderId (using origClientOrderId)"
        );
    }

    // -----------------------------------------------------------------------
    // interpret_response
    // -----------------------------------------------------------------------

    #[test]
    fn interpret_response_success_extracts_result() {
        let v = serde_json::json!({
            "id": 1,
            "status": 200,
            "result": { "orderId": 987654321u64, "symbol": "BTCUSDT" }
        });
        let r = interpret_response(v).unwrap();
        assert_eq!(r["orderId"], 987654321u64);
        assert_eq!(r["symbol"], "BTCUSDT");
    }

    #[test]
    fn interpret_response_error_maps_rate_limit() {
        let v = serde_json::json!({
            "id": 2,
            "status": 429,
            "error": { "code": -1003, "msg": "Too many requests." }
        });
        let err = interpret_response(v).unwrap_err();
        assert!(
            matches!(err, VenueError::RateLimited { .. }),
            "code -1003 must map to RateLimited; got: {err:?}"
        );
    }

    #[test]
    fn interpret_response_error_maps_unknown_order() {
        let v = serde_json::json!({
            "id": 3,
            "status": 400,
            "error": { "code": -2011, "msg": "Unknown order sent." }
        });
        let err = interpret_response(v).unwrap_err();
        assert!(
            matches!(err, VenueError::UnknownQuote),
            "code -2011 must map to UnknownQuote; got: {err:?}"
        );
    }

    #[test]
    fn interpret_response_error_maps_post_only_rejected() {
        let v = serde_json::json!({
            "id": 4,
            "status": 400,
            "error": { "code": -1013, "msg": "Filter failure: GTX_REJECTED." }
        });
        let err = interpret_response(v).unwrap_err();
        assert!(
            matches!(err, VenueError::Rejected { .. }),
            "code -1013 must map to Rejected; got: {err:?}"
        );
    }

    // -----------------------------------------------------------------------
    // URL resolution
    // -----------------------------------------------------------------------

    #[test]
    fn ws_fapi_url_mainnet() {
        assert_eq!(
            ws_fapi_url(BinanceEnv::FuturesMainnet),
            "wss://ws-fapi.binance.com/ws-fapi/v1"
        );
    }

    #[test]
    fn ws_fapi_url_testnet() {
        assert_eq!(
            ws_fapi_url(BinanceEnv::FuturesTestnet),
            "wss://testnet.binancefuture.com/ws-fapi/v1"
        );
    }

    // -----------------------------------------------------------------------
    // Non-futures env + HMAC key rejection tested via connect() at compile-time
    // shape only (no live socket in unit tests).
    // -----------------------------------------------------------------------

    #[test]
    fn side_to_str_bid_is_buy() {
        assert_eq!(side_to_str(Side::Bid), "BUY");
    }

    #[test]
    fn side_to_str_ask_is_sell() {
        assert_eq!(side_to_str(Side::Ask), "SELL");
    }

    #[test]
    fn tif_post_only_is_gtx() {
        assert_eq!(tif_to_str(TimeInForce::PostOnly), "GTX");
    }
}
