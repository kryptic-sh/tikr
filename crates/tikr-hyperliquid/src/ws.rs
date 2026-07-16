//! WebSocket subscription pump for Hyperliquid market data + user events.
//!
//! [`subscribe_stream`] performs a synchronous connect + subscribe + first-data
//! handshake (so callers see errors at startup), then spawns a background task
//! that owns the socket. The task multiplexes `l2Book` and `trades` pushes
//! into a single [`MarketEvent`] stream via an [`mpsc`] channel, auto-pongs
//! `Ping` frames, synthesizes heartbeats at a configurable cadence, and
//! reconnects with exponential backoff on disconnect.
//!
//! [`subscribe_user_events`] opens a *separate* WS connection to the same
//! endpoint and subscribes to `userEvents` for the given user address. Each
//! fill that arrives on the socket is parsed and sent through an
//! `mpsc::UnboundedSender<Fill>`. Reconnect logic mirrors the market-data pump.
//!
//! The returned `BoxStream` ends when the receiver is dropped (cooperative
//! shutdown via `tx.send().is_err()`).

use crate::HyperliquidConfig;
use crate::HyperliquidEnv;
use crate::exchange::{UserEventFill, fill_from_user_event};
use crate::mapping::*;
use crate::messages::*;
use futures::SinkExt;
use futures::stream::{BoxStream, StreamExt};
use serde::Deserialize;
use std::time::Duration;
use tikr_core::{Fill, MarketEvent, Symbol, Timestamp};
use tikr_venue::VenueError;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Connect, subscribe, and return a stream of market events for `symbol`.
///
/// On `Ok`, the returned stream is guaranteed to yield at least one frame
/// (the first `l2Book` or `trades` push) before any synthesized heartbeats.
pub(crate) async fn subscribe_stream(
    config: HyperliquidConfig,
    symbol: Symbol,
) -> Result<BoxStream<'static, MarketEvent>, VenueError> {
    let ws_url = ws_url_for(config.env).to_string();
    let coin = symbol.base.0.to_string();

    // ---- Synchronous connect + subscribe + first-data gate. --------------
    let mut stream = open_and_subscribe(&ws_url, &coin).await?;
    let first_events = read_first_data(&mut stream, &symbol).await?;

    // ---- Spawn the long-running pump. ------------------------------------
    let (tx, rx) = mpsc::channel::<MarketEvent>(1024);

    for ev in first_events {
        // Best-effort: if the receiver is already gone we just drop.
        if tx.send(ev).await.is_err() {
            let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
            return Ok(Box::pin(stream));
        }
    }

    tokio::spawn(pump_loop(stream, tx, ws_url, coin, symbol, config));

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    Ok(Box::pin(stream))
}

fn ws_url_for(env: HyperliquidEnv) -> &'static str {
    match env {
        HyperliquidEnv::Mainnet => "wss://api.hyperliquid.xyz/ws",
        HyperliquidEnv::Testnet => "wss://api.hyperliquid-testnet.xyz/ws",
    }
}

async fn open_and_subscribe(ws_url: &str, coin: &str) -> Result<WsStream, VenueError> {
    let (mut stream, _resp) = connect_async(ws_url).await.map_err(ws_to_network_err)?;
    for kind in ["l2Book", "trades"] {
        let msg = serde_json::json!({
            "method": "subscribe",
            "subscription": { "type": kind, "coin": coin }
        });
        stream
            .send(Message::Text(msg.to_string()))
            .await
            .map_err(ws_to_network_err)?;
    }
    Ok(stream)
}

/// Drain ack frames until the first data push lands. Returns the parsed
/// events from that push.
async fn read_first_data(
    stream: &mut WsStream,
    symbol: &Symbol,
) -> Result<Vec<MarketEvent>, VenueError> {
    loop {
        let frame = stream.next().await.ok_or_else(|| {
            VenueError::Network(std::io::Error::other(
                "hyperliquid WS closed before first data message",
            ))
        })?;
        let frame = frame.map_err(ws_to_network_err)?;
        match frame {
            Message::Text(txt) => {
                if let Some(events) = parse_ws_text(txt.as_str(), symbol) {
                    return Ok(events);
                }
                // Otherwise it was an ack / unknown channel — keep draining.
            }
            Message::Ping(p) => {
                let _ = stream.send(Message::Pong(p)).await;
            }
            Message::Close(_) => {
                return Err(VenueError::Network(std::io::Error::other(
                    "hyperliquid WS closed before first data message",
                )));
            }
            _ => {}
        }
    }
}

/// Long-running pump: read frames, fan out events, reconnect on disconnect,
/// and inject synthetic heartbeats when configured.
async fn pump_loop(
    mut stream: WsStream,
    tx: mpsc::Sender<MarketEvent>,
    ws_url: String,
    coin: String,
    symbol: Symbol,
    config: HyperliquidConfig,
) {
    let mut backoff_ms = config.reconnect_min_backoff_ms.max(1);
    let mut heartbeat = if config.heartbeat_ms > 0 {
        let mut iv = tokio::time::interval(Duration::from_millis(config.heartbeat_ms));
        iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // First tick fires immediately; consume it so we don't double-emit
        // alongside the first real data event the caller already got.
        iv.reset();
        Some(iv)
    } else {
        None
    };

    let mut ping_iv = new_ping_interval();

    loop {
        let action = match heartbeat.as_mut() {
            Some(hb) => tokio::select! {
                frame = stream.next() => PumpAction::Frame(frame),
                _ = hb.tick() => PumpAction::Heartbeat,
                _ = ping_iv.tick() => PumpAction::ClientPing,
            },
            None => tokio::select! {
                frame = stream.next() => PumpAction::Frame(frame),
                _ = ping_iv.tick() => PumpAction::ClientPing,
            },
        };

        match action {
            PumpAction::Heartbeat => {
                let ev = MarketEvent::Heartbeat {
                    ts: Timestamp(now_ns()),
                };
                if tx.send(ev).await.is_err() {
                    return;
                }
            }
            PumpAction::ClientPing => {
                // Send-failure surfaces as a read error on the next frame;
                // the reconnect path there owns recovery.
                let _ = stream.send(Message::Text(PING_FRAME.into())).await;
            }
            PumpAction::Frame(None) => {
                // Stream ended.
                if !reconnect(&mut stream, &ws_url, &coin, &mut backoff_ms, &config).await {
                    return;
                }
            }
            PumpAction::Frame(Some(Err(e))) => {
                warn!(error = %e, "hyperliquid WS read error; reconnecting");
                if !reconnect(&mut stream, &ws_url, &coin, &mut backoff_ms, &config).await {
                    return;
                }
            }
            PumpAction::Frame(Some(Ok(Message::Text(txt)))) => {
                if let Some(events) = parse_ws_text(txt.as_str(), &symbol) {
                    for ev in events {
                        if tx.send(ev).await.is_err() {
                            return;
                        }
                    }
                }
                // Reset backoff on healthy traffic.
                backoff_ms = config.reconnect_min_backoff_ms.max(1);
            }
            PumpAction::Frame(Some(Ok(Message::Ping(p)))) => {
                let _ = stream.send(Message::Pong(p)).await;
            }
            PumpAction::Frame(Some(Ok(Message::Close(_)))) => {
                debug!("hyperliquid WS server-initiated close; reconnecting");
                if !reconnect(&mut stream, &ws_url, &coin, &mut backoff_ms, &config).await {
                    return;
                }
            }
            PumpAction::Frame(Some(Ok(_))) => {
                // Binary / Pong / Frame -- ignore.
            }
        }
    }
}

/// Replace `stream` with a freshly subscribed connection, sleeping for the
/// current backoff and doubling it (capped) on each failure. Returns `false`
/// only if the caller-side receiver has been dropped (cooperative shutdown).
async fn reconnect(
    stream: &mut WsStream,
    ws_url: &str,
    coin: &str,
    backoff_ms: &mut u64,
    config: &HyperliquidConfig,
) -> bool {
    loop {
        warn!(
            backoff_ms = *backoff_ms,
            "hyperliquid WS disconnected; reconnecting"
        );
        tokio::time::sleep(Duration::from_millis(*backoff_ms)).await;
        match open_and_subscribe(ws_url, coin).await {
            Ok(new_stream) => {
                *stream = new_stream;
                *backoff_ms = config.reconnect_min_backoff_ms.max(1);
                return true;
            }
            Err(e) => {
                warn!(error = ?e, "hyperliquid WS reconnect failed");
                *backoff_ms = (*backoff_ms)
                    .saturating_mul(2)
                    .min(config.reconnect_max_backoff_ms.max(1));
            }
        }
    }
}

enum PumpAction {
    Frame(Option<Result<Message, tokio_tungstenite::tungstenite::Error>>),
    Heartbeat,
    /// Application-level keepalive is due (see `PING_INTERVAL`).
    ClientPing,
}

/// Hyperliquid closes connections that receive no client message within
/// ~60s; WS protocol pongs don't count. Both pumps send an application-level
/// `{"method":"ping"}` on this interval — without it, low-traffic sockets
/// (especially `userEvents`) churn through disconnect cycles and lose fills
/// that land in the reconnect gaps.
const PING_INTERVAL: Duration = Duration::from_secs(45);

/// The application-level keepalive frame.
const PING_FRAME: &str = r#"{"method":"ping"}"#;

fn new_ping_interval() -> tokio::time::Interval {
    let mut iv = tokio::time::interval(PING_INTERVAL);
    iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    iv
}

/// Parse a text frame to a flat list of [`MarketEvent`]. Returns `None` for
/// acks / unknown channels / parse failures (logged at debug).
fn parse_ws_text(txt: &str, symbol: &Symbol) -> Option<Vec<MarketEvent>> {
    let msg: WsMessage = match serde_json::from_str(txt) {
        Ok(m) => m,
        Err(e) => {
            debug!(error = %e, "hyperliquid WS: failed to parse frame");
            return None;
        }
    };
    match msg {
        WsMessage::L2Book(push) => Some(vec![MarketEvent::BookUpdate {
            snapshot: l2_to_snapshot(symbol, &push),
        }]),
        WsMessage::Trades(trades) => {
            Some(trades.iter().map(|t| trade_to_event(symbol, t)).collect())
        }
        WsMessage::SubscriptionResponse(_) | WsMessage::Other => None,
    }
}

fn ws_to_network_err(e: tokio_tungstenite::tungstenite::Error) -> VenueError {
    VenueError::Network(std::io::Error::other(e.to_string()))
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// userEvents subscription
// ---------------------------------------------------------------------------

/// Hyperliquid `userEvents` WS push — `fills` array payload.
#[derive(Debug, Clone, Deserialize)]
pub struct UserEventsData {
    /// Fill events in this push.
    #[serde(default)]
    pub fills: Vec<UserEventFill>,
}

/// `userEvents` channel envelope.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "channel", content = "data")]
enum UserEventsMsg {
    /// `userEvents` channel push.
    #[serde(rename = "userEvents")]
    UserEvents(UserEventsData),
    /// Subscription acknowledgement or other channels — ignored.
    #[serde(other, deserialize_with = "deserialize_ignore_user_events_any")]
    Other,
}

fn deserialize_ignore_user_events_any<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<(), D::Error> {
    serde::de::IgnoredAny::deserialize(d).map(|_| ())
}

/// Subscribe to the `userEvents` channel for `user_address`.
///
/// Spawns a background task that maintains the WS connection (reconnect +
/// heartbeat handling mirrors the market-data pump). Each `Fill` parsed from
/// the `fills` array is sent through the returned `UnboundedReceiver<Fill>`.
///
/// The task terminates cooperatively when the receiver is dropped.
pub async fn subscribe_user_events(
    config: HyperliquidConfig,
    user_address: String,
) -> Result<mpsc::UnboundedReceiver<Fill>, VenueError> {
    let ws_url = ws_url_for(config.env).to_string();

    let stream = open_and_subscribe_user_events(&ws_url, &user_address).await?;

    let (tx, rx) = mpsc::unbounded_channel::<Fill>();
    tokio::spawn(user_events_pump(stream, tx, ws_url, user_address, config));

    Ok(rx)
}

async fn open_and_subscribe_user_events(
    ws_url: &str,
    user_address: &str,
) -> Result<WsStream, VenueError> {
    let (mut stream, _resp) = connect_async(ws_url).await.map_err(ws_to_network_err)?;
    let msg = serde_json::json!({
        "method": "subscribe",
        "subscription": { "type": "userEvents", "user": user_address }
    });
    stream
        .send(Message::Text(msg.to_string()))
        .await
        .map_err(ws_to_network_err)?;
    Ok(stream)
}

async fn user_events_pump(
    mut stream: WsStream,
    tx: mpsc::UnboundedSender<Fill>,
    ws_url: String,
    user_address: String,
    config: HyperliquidConfig,
) {
    let mut backoff_ms = config.reconnect_min_backoff_ms.max(1);

    // The `userEvents` socket is the lowest-traffic one — without the
    // application-level keepalive it hits the venue's idle disconnect and
    // loses any fills landing in the reconnect gaps (no backfill exists).
    let mut ping_iv = new_ping_interval();

    loop {
        let frame = tokio::select! {
            frame = stream.next() => frame,
            _ = ping_iv.tick() => {
                let _ = stream.send(Message::Text(PING_FRAME.into())).await;
                continue;
            }
        };
        match frame {
            None => {
                // Stream ended; reconnect.
                if !reconnect_user_events(
                    &mut stream,
                    &ws_url,
                    &user_address,
                    &mut backoff_ms,
                    &config,
                )
                .await
                {
                    return;
                }
            }
            Some(Err(e)) => {
                warn!(error = %e, "userEvents WS read error; reconnecting");
                if !reconnect_user_events(
                    &mut stream,
                    &ws_url,
                    &user_address,
                    &mut backoff_ms,
                    &config,
                )
                .await
                {
                    return;
                }
            }
            Some(Ok(Message::Text(txt))) => {
                backoff_ms = config.reconnect_min_backoff_ms.max(1);
                let msg: UserEventsMsg = match serde_json::from_str(&txt) {
                    Ok(m) => m,
                    Err(e) => {
                        debug!(error = %e, "userEvents WS: failed to parse frame");
                        continue;
                    }
                };
                if let UserEventsMsg::UserEvents(data) = msg {
                    for raw_fill in &data.fills {
                        let fill = fill_from_user_event("", raw_fill);
                        info!(
                            oid = raw_fill.oid,
                            price = %raw_fill.px,
                            size = %raw_fill.sz,
                            side = %raw_fill.side,
                            "userEvents fill"
                        );
                        if tx.send(fill).is_err() {
                            // Receiver dropped; exit.
                            return;
                        }
                    }
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = stream.send(Message::Pong(p)).await;
            }
            Some(Ok(Message::Close(_))) => {
                debug!("userEvents WS server-initiated close; reconnecting");
                if !reconnect_user_events(
                    &mut stream,
                    &ws_url,
                    &user_address,
                    &mut backoff_ms,
                    &config,
                )
                .await
                {
                    return;
                }
            }
            Some(Ok(_)) => {
                // Binary / Pong / Frame -- ignore.
            }
        }
    }
}

async fn reconnect_user_events(
    stream: &mut WsStream,
    ws_url: &str,
    user_address: &str,
    backoff_ms: &mut u64,
    config: &HyperliquidConfig,
) -> bool {
    loop {
        warn!(
            backoff_ms = *backoff_ms,
            "userEvents WS disconnected; reconnecting"
        );
        tokio::time::sleep(Duration::from_millis(*backoff_ms)).await;
        match open_and_subscribe_user_events(ws_url, user_address).await {
            Ok(new_stream) => {
                *stream = new_stream;
                *backoff_ms = config.reconnect_min_backoff_ms.max(1);
                return true;
            }
            Err(e) => {
                warn!(error = ?e, "userEvents WS reconnect failed");
                *backoff_ms = (*backoff_ms)
                    .saturating_mul(2)
                    .min(config.reconnect_max_backoff_ms.max(1));
            }
        }
    }
}
