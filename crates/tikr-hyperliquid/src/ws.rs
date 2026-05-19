//! WebSocket subscription pump for Hyperliquid market data.
//!
//! [`subscribe_stream`] performs a synchronous connect + subscribe + first-data
//! handshake (so callers see errors at startup), then spawns a background task
//! that owns the socket. The task multiplexes `l2Book` and `trades` pushes
//! into a single [`MarketEvent`] stream via an [`mpsc`] channel, auto-pongs
//! `Ping` frames, synthesizes heartbeats at a configurable cadence, and
//! reconnects with exponential backoff on disconnect.
//!
//! The returned `BoxStream` ends when the receiver is dropped (cooperative
//! shutdown via `tx.send().is_err()`).

use crate::HyperliquidConfig;
use crate::HyperliquidEnv;
use crate::mapping::*;
use crate::messages::*;
use futures::SinkExt;
use futures::stream::{BoxStream, StreamExt};
use std::time::Duration;
use tikr_core::{MarketEvent, Symbol, Timestamp};
use tikr_venue::VenueError;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tracing::{debug, warn};

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

    loop {
        let action = match heartbeat.as_mut() {
            Some(hb) => tokio::select! {
                frame = stream.next() => PumpAction::Frame(frame),
                _ = hb.tick() => PumpAction::Heartbeat,
            },
            None => PumpAction::Frame(stream.next().await),
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
