//! Binance userDataStream: listenKey lifecycle (futures) + spot WS-API path.
//!
//! ## Architecture
//!
//! ### Spot (WS-API, post-2026-02-20 deprecation)
//!
//! The REST `POST /api/v3/userDataStream` listenKey endpoint was deprecated on
//! 2026-02-20 and returns HTTP 410 Gone. Spot now uses the Binance Spot
//! WebSocket API (`wss://ws-api.testnet.binance.vision/ws-api/v3`):
//!
//! 1. Connect to the WS-API endpoint.
//! 2. Send `session.logon` (Ed25519-signed — **only Ed25519 is supported**).
//! 3. After logon ack, send `userDataStream.subscribe`.
//! 4. Events flow on the same connection, wrapped:
//!    `{"subscriptionId": N, "event": {"e": "executionReport", ...}}`.
//!
//! On WS disconnect: full reconnect (new `session.logon` + `subscribe`).
//! No keepalive needed (no listenKey to extend).
//!
//! ### Futures (unchanged)
//!
//! 1. [`mint_listen_key`] — POST to mint a new listenKey. 60-min server expiry.
//! 2. [`keepalive_listen_key`] — PUT to extend the key. Called every 20 min.
//! 3. [`subscribe_user_data_stream`] — for futures: spawns a WS task +
//!    keepalive task using the listenKey path.
//!
//! On WS disconnect the WS task mints a **fresh** listenKey (never reuses a
//! stale key) and reconnects.
//!
//! ## listenKey endpoints (futures only)
//!
//! | Product | Mint | Keepalive |
//! |---------|------|-----------|
//! | Futures | `POST /fapi/v1/listenKey` | `PUT /fapi/v1/listenKey?listenKey=…` |
//!
//! ## Fill parsing
//!
//! - Spot WS-API: `executionReport` inside `{"subscriptionId":N,"event":{...}}`.
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
use tokio::sync::{Mutex, mpsc, watch};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::BinanceEnv;
use crate::sign::{BinanceKeyMaterial, sign_query_ed25519, timestamp_ms};

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
// Spot WS-API (session.logon + userDataStream.subscribe)
// ---------------------------------------------------------------------------

/// Spot WS-API base URL for `session.logon` + `userDataStream.subscribe`.
///
/// Different from the public stream WS (`testnet.binance.vision/ws/...`).
/// Only supports Ed25519 keys — HMAC is not accepted for this endpoint.
pub fn spot_ws_api_url(env: BinanceEnv) -> &'static str {
    match env {
        BinanceEnv::SpotTestnet => "wss://ws-api.testnet.binance.vision/ws-api/v3",
        BinanceEnv::SpotMainnet => "wss://ws-api.binance.com:443/ws-api/v3",
        // Futures envs do not use the WS-API path; included for completeness.
        BinanceEnv::FuturesTestnet => "wss://ws-api.testnet.binancefuture.com/ws-api/v3",
        BinanceEnv::FuturesMainnet => "wss://ws-api.binance.com:443/ws-api/v3",
    }
}

/// Build the canonical signed string for `session.logon`.
///
/// Binance specifies that params MUST be in **alphabetical order** for
/// the WS-API signature (unlike REST which uses insertion order).
///
/// Alphabetical order: `apiKey` < `recvWindow` < `timestamp`.
/// Result: `"apiKey=<KEY>&recvWindow=5000&timestamp=<MS>"`.
pub fn session_logon_signed_string(api_key: &str, ts_ms: u64) -> String {
    format!("apiKey={api_key}&recvWindow=5000&timestamp={ts_ms}")
}

/// Connect to the Spot WS-API, perform `session.logon`, then
/// `userDataStream.subscribe`. Returns the authenticated WS stream.
///
/// # Errors
///
/// Returns `VenueError::Rejected` if `key_material` is not `Ed25519` —
/// Binance only supports Ed25519 keys for `session.logon`.
pub async fn open_spot_user_data_ws(
    env: BinanceEnv,
    api_key: &str,
    key_material: &BinanceKeyMaterial,
) -> Result<WsStream, VenueError> {
    // Ed25519 required — HMAC not supported for session.logon.
    let signing_key = match key_material {
        BinanceKeyMaterial::Ed25519 { signing_key } => signing_key,
        BinanceKeyMaterial::Hmac { .. } => {
            return Err(VenueError::Rejected {
                reason: "Spot userDataStream via WS-API requires Ed25519 key material; \
                         HMAC is not supported by Binance for session.logon. \
                         Set TIKR_BINANCE_SPOT_KEY_TYPE=ed25519 (or TIKR_BINANCE_KEY_TYPE=ed25519) \
                         and provide an Ed25519 PEM key."
                    .into(),
            });
        }
    };

    let ws_url = spot_ws_api_url(env);
    let (mut stream, _) = connect_async(ws_url)
        .await
        .map_err(|e| VenueError::Network(std::io::Error::other(e.to_string())))?;

    // --- session.logon ---
    let ts = timestamp_ms();
    let signed_str = session_logon_signed_string(api_key, ts);
    let signature = sign_query_ed25519(signing_key, &signed_str);
    // Note: base64 signature in JSON body — no URL percent-encoding needed.

    let logon_id = "tikr-logon-1";
    let logon_msg = serde_json::json!({
        "id": logon_id,
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
    wait_for_ack(&mut stream, logon_id, "session.logon").await?;

    // --- userDataStream.subscribe ---
    let subscribe_id = "tikr-sub-1";
    let sub_msg = serde_json::json!({
        "id": subscribe_id,
        "method": "userDataStream.subscribe"
    });
    stream
        .send(Message::Text(sub_msg.to_string()))
        .await
        .map_err(|e| VenueError::Network(std::io::Error::other(e.to_string())))?;

    // Wait for subscribe ack.
    wait_for_ack(&mut stream, subscribe_id, "userDataStream.subscribe").await?;

    info!(env = ?env, "spot WS-API: session.logon + userDataStream.subscribe OK");
    Ok(stream)
}

/// Wait for a JSON response frame with `"id"` matching `expected_id` and
/// `"status"` == 200. Ignores unrelated frames (ping, other events).
async fn wait_for_ack(
    stream: &mut WsStream,
    expected_id: &str,
    method: &str,
) -> Result<(), VenueError> {
    loop {
        let frame = stream.next().await;
        match frame {
            None => {
                return Err(VenueError::Network(std::io::Error::other(format!(
                    "WS-API connection closed waiting for {method} ack"
                ))));
            }
            Some(Err(e)) => {
                return Err(VenueError::Network(std::io::Error::other(format!(
                    "WS-API read error waiting for {method} ack: {e}"
                ))));
            }
            Some(Ok(Message::Text(txt))) => {
                let v: serde_json::Value = serde_json::from_str(&txt).map_err(|e| {
                    VenueError::Internal(Box::new(std::io::Error::other(format!(
                        "WS-API non-JSON frame waiting for {method}: {e}: {txt}"
                    ))))
                })?;
                let id = v.get("id").and_then(serde_json::Value::as_str);
                if id != Some(expected_id) {
                    // Not our response — could be an early event; skip.
                    debug!(frame = %txt, "WS-API: skipping non-matching frame while waiting for {method}");
                    continue;
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
                // Respond to server ping immediately.
                let _ = stream.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {} // Binary / Pong — ignore
        }
    }
}

// ---------------------------------------------------------------------------
// userDataStream subscription
// ---------------------------------------------------------------------------

/// Subscribe to userDataStream fills for the given environment.
///
/// **Routing**:
///
/// - Spot (`!env.is_futures()`): uses the Spot WS-API path (`session.logon`
///   + `userDataStream.subscribe`). Requires `BinanceKeyMaterial::Ed25519`;
///     returns `VenueError::Rejected` immediately if HMAC is passed.
/// - Futures (`env.is_futures()`): uses the existing listenKey REST path.
///
/// `symbol_filter` is the Binance symbol string (e.g. `"BTCUSDT"`).
/// Only fills whose symbol field matches are forwarded.
///
/// Returns an `mpsc::UnboundedReceiver<Fill>` that the caller polls.
pub async fn subscribe_user_data_stream(
    http: HttpClient,
    env: BinanceEnv,
    api_key: String,
    key_material: Arc<BinanceKeyMaterial>,
    kind: MarketKind,
    symbol_filter: String,
) -> Result<mpsc::UnboundedReceiver<Fill>, VenueError> {
    subscribe_user_data_stream_cancellable(
        http,
        env,
        api_key,
        key_material,
        kind,
        symbol_filter,
        None,
    )
    .await
}

/// Like [`subscribe_user_data_stream`] but accepts a `shutdown_rx` so the
/// internally-spawned tasks (keepalive PUT loop and WS read pump) can be
/// stopped cleanly when the caller is done with the subscription.
///
/// Without this, dropping the returned receiver does NOT stop those tasks
/// — they keep running forever, each holding an `Arc<HttpClient>` and the
/// listenKey mutex. The dashboard supervisor restarts bots on every
/// crash, so leaking those tasks compounds quickly.
///
/// Pass `Some(rx)` to get cancellable behavior; pass `None` (or use the
/// non-cancellable wrapper) for one-shot tools like `run_perp` that exit
/// the process on shutdown.
pub async fn subscribe_user_data_stream_cancellable(
    http: HttpClient,
    env: BinanceEnv,
    api_key: String,
    key_material: Arc<BinanceKeyMaterial>,
    kind: MarketKind,
    symbol_filter: String,
    shutdown_rx: Option<watch::Receiver<bool>>,
) -> Result<mpsc::UnboundedReceiver<Fill>, VenueError> {
    if env.is_futures() {
        subscribe_futures_user_data_stream(http, env, api_key, kind, symbol_filter, shutdown_rx)
            .await
    } else {
        subscribe_spot_user_data_stream(
            env,
            api_key,
            key_material,
            kind,
            symbol_filter,
            shutdown_rx,
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// Spot user data subscription (WS-API)
// ---------------------------------------------------------------------------

async fn subscribe_spot_user_data_stream(
    env: BinanceEnv,
    api_key: String,
    key_material: Arc<BinanceKeyMaterial>,
    kind: MarketKind,
    symbol_filter: String,
    shutdown_rx: Option<watch::Receiver<bool>>,
) -> Result<mpsc::UnboundedReceiver<Fill>, VenueError> {
    let stream = open_spot_user_data_ws(env, &api_key, &key_material).await?;
    let (tx, rx) = mpsc::unbounded_channel::<Fill>();

    tokio::spawn(spot_user_data_pump(
        stream,
        tx,
        env,
        api_key,
        key_material,
        kind,
        symbol_filter,
        shutdown_rx,
    ));

    Ok(rx)
}

#[allow(clippy::too_many_arguments)]
async fn spot_user_data_pump(
    mut stream: WsStream,
    tx: mpsc::UnboundedSender<Fill>,
    env: BinanceEnv,
    api_key: String,
    key_material: Arc<BinanceKeyMaterial>,
    kind: MarketKind,
    symbol_filter: String,
    mut shutdown_rx: Option<watch::Receiver<bool>>,
) {
    let reconnect_min_ms: u64 = 1_000;
    let reconnect_max_ms: u64 = 30_000;
    let mut backoff_ms = reconnect_min_ms;

    loop {
        let frame_fut = stream.next();
        let shut_fut = async {
            match shutdown_rx.as_mut() {
                Some(rx) => {
                    let _ = rx.changed().await;
                    *rx.borrow()
                }
                None => std::future::pending::<bool>().await,
            }
        };
        let frame;
        tokio::select! {
            f = frame_fut => { frame = f; }
            signaled = shut_fut => {
                if signaled {
                    debug!("spot WS-API: shutdown signaled");
                    return;
                }
                continue;
            }
        }
        match frame {
            None => {
                warn!(
                    backoff_ms,
                    "spot WS-API userDataStream closed (24h limit?); reconnecting"
                );
                if !reconnect_spot(
                    &mut stream,
                    env,
                    &api_key,
                    &key_material,
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
                warn!(error = %e, "spot WS-API read error; reconnecting");
                if !reconnect_spot(
                    &mut stream,
                    env,
                    &api_key,
                    &key_material,
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
                debug!(bytes = txt.len(), "spot WS-API: frame received");

                // Parse: could be a response frame (id present) or an event frame.
                // Event frames: {"subscriptionId": N, "event": {...}}
                // Response frames: {"id": "...", "status": 200, "result": ...}
                if let Some(fill) = parse_spot_ws_api_message(&txt, kind, &symbol_filter) {
                    info!(
                        quote_id = ?fill.quote_id,
                        price = %fill.price.0,
                        size = %fill.size.0,
                        "spot WS-API: fill received"
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
                debug!("spot WS-API server close; reconnecting");
                if !reconnect_spot(
                    &mut stream,
                    env,
                    &api_key,
                    &key_material,
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

#[allow(clippy::too_many_arguments)]
async fn reconnect_spot(
    stream: &mut WsStream,
    env: BinanceEnv,
    api_key: &str,
    key_material: &Arc<BinanceKeyMaterial>,
    backoff_ms: &mut u64,
    reconnect_min_ms: u64,
    reconnect_max_ms: u64,
) -> bool {
    loop {
        warn!(
            backoff_ms = *backoff_ms,
            "spot WS-API disconnected; reconnecting with new session.logon"
        );
        tokio::time::sleep(Duration::from_millis(*backoff_ms)).await;
        match open_spot_user_data_ws(env, api_key, key_material).await {
            Ok(new_stream) => {
                *stream = new_stream;
                *backoff_ms = reconnect_min_ms;
                return true;
            }
            Err(e) => {
                warn!(error = ?e, "spot WS-API reconnect failed");
                *backoff_ms = (*backoff_ms).saturating_mul(2).min(reconnect_max_ms);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Futures user data subscription (listenKey path, unchanged)
// ---------------------------------------------------------------------------

async fn subscribe_futures_user_data_stream(
    http: HttpClient,
    env: BinanceEnv,
    api_key: String,
    kind: MarketKind,
    symbol_filter: String,
    shutdown_rx: Option<watch::Receiver<bool>>,
) -> Result<mpsc::UnboundedReceiver<Fill>, VenueError> {
    let listen_key = mint_listen_key(&http, env, &api_key).await?;
    let ws_url = format!("{}/{}", ws_base_url(env), listen_key);

    let stream = open_user_data_ws(&ws_url).await?;

    let (tx, rx) = mpsc::unbounded_channel::<Fill>();

    // Share the current listenKey between the WS task and keepalive task.
    let shared_key: Arc<Mutex<String>> = Arc::new(Mutex::new(listen_key.clone()));

    // Keepalive task: PUT every 20 min. When `shutdown_rx` is provided
    // the loop exits on signal; otherwise it runs forever (matches the
    // long-standing one-shot `run_perp` behavior).
    let http2 = http.clone();
    let api_key2 = api_key.clone();
    let shared_key2 = shared_key.clone();
    let mut keepalive_shutdown = shutdown_rx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(KEEPALIVE_INTERVAL_MS));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await; // burn first immediate tick
        loop {
            let tick = interval.tick();
            let shut = async {
                match keepalive_shutdown.as_mut() {
                    Some(rx) => {
                        let _ = rx.changed().await;
                        *rx.borrow()
                    }
                    None => std::future::pending::<bool>().await,
                }
            };
            tokio::select! {
                _ = tick => {
                    let key = shared_key2.lock().await.clone();
                    if let Err(e) = keepalive_listen_key(&http2, env, &api_key2, &key).await {
                        warn!(error = ?e, "userDataStream: keepalive PUT failed");
                    } else {
                        debug!("userDataStream: keepalive PUT OK");
                    }
                }
                signaled = shut => {
                    if signaled {
                        debug!("userDataStream: keepalive shutdown");
                        return;
                    }
                }
            }
        }
    });

    // WS pump task.
    tokio::spawn(user_data_pump(
        stream,
        tx,
        http,
        env,
        api_key,
        kind,
        shared_key,
        symbol_filter,
        shutdown_rx,
    ));

    Ok(rx)
}

async fn open_user_data_ws(ws_url: &str) -> Result<WsStream, VenueError> {
    let (stream, _) = connect_async(ws_url)
        .await
        .map_err(|e| VenueError::Network(std::io::Error::other(e.to_string())))?;
    Ok(stream)
}

#[allow(clippy::too_many_arguments)]
async fn user_data_pump(
    mut stream: WsStream,
    tx: mpsc::UnboundedSender<Fill>,
    http: HttpClient,
    env: BinanceEnv,
    api_key: String,
    kind: MarketKind,
    shared_key: Arc<Mutex<String>>,
    symbol_filter: String,
    mut shutdown_rx: Option<watch::Receiver<bool>>,
) {
    let reconnect_min_ms: u64 = 1_000;
    let reconnect_max_ms: u64 = 30_000;
    let mut backoff_ms = reconnect_min_ms;

    loop {
        let frame_fut = stream.next();
        let shut_fut = async {
            match shutdown_rx.as_mut() {
                Some(rx) => {
                    let _ = rx.changed().await;
                    *rx.borrow()
                }
                None => std::future::pending::<bool>().await,
            }
        };
        let frame;
        tokio::select! {
            f = frame_fut => { frame = f; }
            signaled = shut_fut => {
                if signaled {
                    debug!("userDataStream WS: shutdown signaled");
                    return;
                }
                continue;
            }
        }
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
                // Visibility into the WS stream: log every frame at debug so
                // operators can confirm the channel is alive even when
                // no fills are happening (only fills surface at info level).
                debug!(bytes = txt.len(), "userDataStream: frame received");
                if let Some(fill) = parse_user_data_message(&txt, kind, &symbol_filter) {
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

/// Parse a Spot WS-API event frame into a [`Fill`], if applicable.
///
/// Spot WS-API wraps events as `{"subscriptionId": N, "event": {...}}`.
/// Response frames (acks) have `"id"` + `"status"` and no `"event"` — ignored.
pub fn parse_spot_ws_api_message(txt: &str, kind: MarketKind, symbol_filter: &str) -> Option<Fill> {
    let v: serde_json::Value = serde_json::from_str(txt).ok()?;
    // Only process event frames — ack frames have "id" field, not "event".
    let event = v.get("event")?;
    match kind {
        MarketKind::Spot => parse_execution_report(event, symbol_filter),
        MarketKind::Perp => parse_order_trade_update(event, symbol_filter),
    }
}

/// Parse a userDataStream text frame into a [`Fill`], if applicable.
///
/// `symbol_filter` is the Binance symbol string (e.g. `"BTCUSDT"`).
/// Returns `None` if the frame's symbol field doesn't match — necessary
/// because Binance returns ONE listenKey per account so multi-process
/// runs against the same account share an event stream.
///
/// Spot (futures path): `executionReport` with `x=FILL` and `s=<symbol>`.
/// Futures: `ORDER_TRADE_UPDATE` with `.o.X=FILLED` and `.o.s=<symbol>`.
pub fn parse_user_data_message(txt: &str, kind: MarketKind, symbol_filter: &str) -> Option<Fill> {
    let v: serde_json::Value = serde_json::from_str(txt).ok()?;
    match kind {
        MarketKind::Spot => parse_execution_report(&v, symbol_filter),
        MarketKind::Perp => parse_order_trade_update(&v, symbol_filter),
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

fn parse_execution_report(v: &serde_json::Value, symbol_filter: &str) -> Option<Fill> {
    let event_type = v.get("e").and_then(serde_json::Value::as_str)?;
    if event_type != "executionReport" {
        return None;
    }
    // Symbol filter: skip fills for other symbols on the same account.
    let frame_symbol = v.get("s").and_then(serde_json::Value::as_str)?;
    if frame_symbol != symbol_filter {
        return None;
    }
    let exec_type = v.get("x").and_then(serde_json::Value::as_str)?;
    if exec_type != "FILL" {
        return None;
    }
    // Spot order status field. Treat anything other than FILLED as partial
    // so strategies don't re-quote until the resting order is fully consumed.
    let status = v
        .get("X")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("FILLED");
    let is_full = status == "FILLED";

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
        is_full,
    })
}

// ---------------------------------------------------------------------------
// Futures: ORDER_TRADE_UPDATE
// ---------------------------------------------------------------------------

fn parse_order_trade_update(v: &serde_json::Value, symbol_filter: &str) -> Option<Fill> {
    let event_type = v.get("e").and_then(serde_json::Value::as_str)?;
    if event_type != "ORDER_TRADE_UPDATE" {
        return None;
    }

    let o = v.get("o")?;

    // Symbol filter: skip fills for other symbols on the same account.
    let frame_symbol = o.get("s").and_then(serde_json::Value::as_str)?;
    if frame_symbol != symbol_filter {
        return None;
    }

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
    let is_full = status == "FILLED";

    Some(Fill {
        quote_id,
        price: Price(price),
        size: Size(qty),
        fee_asset: Asset::new(commission_asset),
        fee_amount: commission,
        fee_quote: Notional(commission),
        side,
        ts: Timestamp(ts_ms.saturating_mul(1_000_000)),
        is_full,
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
        let fill = parse_user_data_message(spot_fill_report(), MarketKind::Spot, "BTCUSDT")
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
        let fill = parse_user_data_message(futures_fill_report(), MarketKind::Perp, "BTCUSDT")
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
        let msg = r#"{"e":"executionReport","s":"BTCUSDT","x":"NEW","i":999,"S":"BUY","L":"0","l":"0","n":"0","T":0}"#;
        let result = parse_user_data_message(msg, MarketKind::Spot, "BTCUSDT");
        assert!(
            result.is_none(),
            "NEW execution type must not produce a fill"
        );
    }

    #[test]
    fn non_order_event_ignored() {
        // Spot balance update event should not produce a fill.
        let msg = r#"{"e":"outboundAccountPosition","E":1234567890}"#;
        let result = parse_user_data_message(msg, MarketKind::Spot, "BTCUSDT");
        assert!(result.is_none());
    }

    /// Regression: spot fill for SOLUSDT must NOT produce a Fill when the
    /// process is filtering on BTCUSDT (multi-process listenKey-sharing case).
    #[test]
    fn spot_fill_other_symbol_filtered_out() {
        let mut msg = spot_fill_report().to_string();
        // Replace the symbol in the fixture.
        msg = msg.replace("\"s\": \"BTCUSDT\"", "\"s\": \"SOLUSDT\"");
        let result = parse_user_data_message(&msg, MarketKind::Spot, "BTCUSDT");
        assert!(
            result.is_none(),
            "spot fill for SOLUSDT must not pass BTCUSDT filter"
        );
    }

    /// Same regression for futures.
    #[test]
    fn futures_fill_other_symbol_filtered_out() {
        let mut msg = futures_fill_report().to_string();
        msg = msg.replace("\"s\": \"BTCUSDT\"", "\"s\": \"ETHUSDT\"");
        let result = parse_user_data_message(&msg, MarketKind::Perp, "BTCUSDT");
        assert!(
            result.is_none(),
            "futures fill for ETHUSDT must not pass BTCUSDT filter"
        );
    }

    // -----------------------------------------------------------------------
    // New spot WS-API tests
    // -----------------------------------------------------------------------

    /// Locked decision: spot WS-API URLs must match the locked values.
    #[test]
    fn spot_ws_api_url_for_envs() {
        assert_eq!(
            spot_ws_api_url(BinanceEnv::SpotTestnet),
            "wss://ws-api.testnet.binance.vision/ws-api/v3",
            "testnet URL must match locked decision"
        );
        assert_eq!(
            spot_ws_api_url(BinanceEnv::SpotMainnet),
            "wss://ws-api.binance.com:443/ws-api/v3",
            "mainnet URL must match locked decision"
        );
    }

    /// session.logon signed string must be ALPHABETICAL order.
    ///
    /// Binance spec: apiKey < recvWindow < timestamp.
    /// Result: "apiKey=<KEY>&recvWindow=5000&timestamp=<MS>".
    #[test]
    fn session_logon_signed_string_alphabetical() {
        let api_key = "vmPUZE6mv9SD5VNHk4HlbGLMkR5tEPQFAAAA";
        let ts = 1_699_999_999_000u64;
        let s = session_logon_signed_string(api_key, ts);
        let expected = format!("apiKey={api_key}&recvWindow=5000&timestamp={ts}");
        assert_eq!(
            s, expected,
            "signed string must use alphabetical param order"
        );

        // Verify order explicitly: apiKey < recvWindow < timestamp
        let pos_api = s.find("apiKey=").expect("apiKey");
        let pos_recv = s.find("recvWindow=").expect("recvWindow");
        let pos_ts = s.find("timestamp=").expect("timestamp");
        assert!(pos_api < pos_recv, "apiKey must precede recvWindow");
        assert!(pos_recv < pos_ts, "recvWindow must precede timestamp");
    }

    /// Spot + HMAC key must return VenueError::Rejected immediately.
    ///
    /// Binance only supports Ed25519 for session.logon; HMAC must be rejected
    /// with a clear error before any network call is attempted.
    #[tokio::test]
    async fn spot_hmac_rejected() {
        let km = Arc::new(BinanceKeyMaterial::Hmac {
            secret: "test-secret".to_string(),
        });
        let result = subscribe_user_data_stream(
            reqwest::Client::new(),
            BinanceEnv::SpotTestnet,
            "test-api-key".to_string(),
            km,
            MarketKind::Spot,
            "BTCUSDT".to_string(),
        )
        .await;
        assert!(
            matches!(result, Err(VenueError::Rejected { .. })),
            "spot + HMAC must return VenueError::Rejected; got: {result:?}"
        );
    }

    /// Spot WS-API event frame (wrapped) parses correctly.
    ///
    /// Format: `{"subscriptionId": N, "event": {"e":"executionReport",...}}`
    #[test]
    fn parse_spot_ws_api_fill_event() {
        // Wrap the existing spot fill fixture in the WS-API envelope.
        let inner = r#"{
            "e": "executionReport",
            "E": 1499405658658,
            "s": "BTCUSDT",
            "c": "tikr_00000000000000000000000000000001",
            "S": "BUY",
            "x": "FILL",
            "X": "FILLED",
            "i": 12345678,
            "l": "0.001",
            "L": "30000.00",
            "n": "0.03",
            "N": "USDT",
            "T": 1499405658657
        }"#;
        let inner_v: serde_json::Value = serde_json::from_str(inner).unwrap();
        let wrapped = serde_json::json!({
            "subscriptionId": 0,
            "event": inner_v
        });
        let txt = wrapped.to_string();

        let fill = parse_spot_ws_api_message(&txt, MarketKind::Spot, "BTCUSDT")
            .expect("spot WS-API wrapped executionReport must parse");

        assert_eq!(fill.price.0, Decimal::from_str("30000.00").unwrap());
        assert_eq!(fill.size.0, Decimal::from_str("0.001").unwrap());
        assert_eq!(fill.side, Side::Bid);
        assert_eq!(fill.fee_asset, Asset::new("USDT"));
    }

    /// Spot WS-API ack frame (id + status, no "event") must produce no fill.
    #[test]
    fn parse_spot_ws_api_ack_not_a_fill() {
        let ack =
            r#"{"id":"tikr-logon-1","status":200,"result":{"apiKey":"x","authorizedSince":1}}"#;
        let result = parse_spot_ws_api_message(ack, MarketKind::Spot, "BTCUSDT");
        assert!(
            result.is_none(),
            "ack frame with no 'event' key must not produce a fill"
        );
    }
}
