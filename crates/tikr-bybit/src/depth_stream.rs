//! Bybit V5 public orderbook stream.
//!
//! Subscribes to `orderbook.50.{SYMBOL}` on the linear public WS host.
//! Bybit sends a full snapshot on subscribe, then deltas — we maintain
//! the full L2 book locally and emit a [`MarketEvent::BookUpdate`] on
//! every change so consumers see a fresh top-N snapshot per tick
//! (mirrors the Binance `@depth20@100ms` interface).
//!
//! ## Reconnect
//!
//! Mirrors `tikr-binance::depth_stream` pattern: exponential backoff
//! 1s → 30s. The local book is wiped on reconnect; the next snapshot
//! frame from Bybit re-seeds it.
//!
//! ## Wire format
//!
//! ```json
//! {
//!   "topic": "orderbook.50.BTCUSDT",
//!   "type":  "snapshot" | "delta",
//!   "ts":    1731700000000,
//!   "data": {
//!     "s": "BTCUSDT",
//!     "b": [["62000.5","1.234"], ...],
//!     "a": [["62001.0","0.987"], ...],
//!     "u": 41234567,
//!     "seq": 9876543
//!   }
//! }
//! ```
//!
//! Delta semantics: size `"0"` removes the level; any other value
//! replaces the resting size at that price.

use futures::SinkExt;
use futures::stream::{BoxStream, StreamExt};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::Duration;
use tikr_core::{Decimal, Level, MarketEvent, Price, Size, Snapshot, Symbol, Timestamp};
use tikr_venue::VenueError;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tracing::{debug, warn};

use crate::BybitEnv;
use crate::mapping::bybit_symbol;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

const DEPTH: u32 = 50;

#[derive(Deserialize)]
struct WsFrame {
    topic: Option<String>,
    #[serde(rename = "type")]
    msg_type: Option<String>,
    ts: Option<u64>,
    data: Option<OrderbookData>,
}

#[derive(Deserialize)]
struct OrderbookData {
    b: Vec<[String; 2]>,
    a: Vec<[String; 2]>,
}

/// Subscribe to the L2 depth stream for `symbol`. Returns a stream of
/// [`MarketEvent::BookUpdate`] frames; reconnects internally on socket
/// drop with exponential backoff.
pub async fn subscribe_depth(
    env: BybitEnv,
    symbol: Symbol,
) -> Result<BoxStream<'static, MarketEvent>, VenueError> {
    let ws_url = env.public_ws_url().to_string();
    let topic = format!("orderbook.{DEPTH}.{}", bybit_symbol(&symbol));
    let stream = open_and_subscribe(&ws_url, &topic).await?;

    let (tx, rx) = mpsc::channel::<MarketEvent>(512);
    tokio::spawn(depth_pump(stream, tx, ws_url, topic, symbol));
    let out = tokio_stream::wrappers::ReceiverStream::new(rx);
    Ok(Box::pin(out))
}

async fn open_and_subscribe(ws_url: &str, topic: &str) -> Result<WsStream, VenueError> {
    let (mut stream, _) = connect_async(ws_url)
        .await
        .map_err(|e| VenueError::Network(std::io::Error::other(e.to_string())))?;
    let sub_msg = serde_json::json!({
        "op": "subscribe",
        "args": [topic],
    });
    stream
        .send(Message::Text(sub_msg.to_string()))
        .await
        .map_err(|e| VenueError::Network(std::io::Error::other(e.to_string())))?;
    Ok(stream)
}

async fn depth_pump(
    mut stream: WsStream,
    tx: mpsc::Sender<MarketEvent>,
    ws_url: String,
    topic: String,
    symbol: Symbol,
) {
    let reconnect_min_ms: u64 = 1_000;
    let reconnect_max_ms: u64 = 30_000;
    let mut backoff_ms = reconnect_min_ms;
    // Local book state — Bybit emits snapshot + deltas, so callers
    // would otherwise only see partial updates. Keep the full L2 book
    // and serialise the top-N to MarketEvent each frame.
    let mut bids: BTreeMap<Decimal, Decimal> = BTreeMap::new();
    let mut asks: BTreeMap<Decimal, Decimal> = BTreeMap::new();

    loop {
        match stream.next().await {
            None => {
                bids.clear();
                asks.clear();
                if !reconnect(
                    &mut stream,
                    &ws_url,
                    &topic,
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
                warn!(error = %e, "bybit depth WS read error; reconnecting");
                bids.clear();
                asks.clear();
                if !reconnect(
                    &mut stream,
                    &ws_url,
                    &topic,
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
                let Ok(frame) = serde_json::from_str::<WsFrame>(&txt) else {
                    debug!(body = %txt, "bybit depth: non-JSON or unexpected shape");
                    continue;
                };
                let Some(topic_in) = frame.topic.as_deref() else {
                    // Sub ack / pong — Bybit returns shapes like
                    // `{"success":true,"op":"subscribe",...}`.
                    continue;
                };
                if topic_in != topic {
                    continue;
                }
                let Some(data) = frame.data else { continue };
                let is_snapshot = frame.msg_type.as_deref() == Some("snapshot");
                if is_snapshot {
                    bids.clear();
                    asks.clear();
                }
                apply_levels(&mut bids, &data.b);
                apply_levels(&mut asks, &data.a);
                let ts_ns = frame.ts.unwrap_or_else(now_ms).saturating_mul(1_000_000);
                let event = build_event(&bids, &asks, &symbol, Timestamp(ts_ns));
                if tx.send(event).await.is_err() {
                    return; // receiver dropped
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = stream.send(Message::Pong(p)).await;
            }
            Some(Ok(Message::Close(_))) => {
                debug!("bybit depth WS server close; reconnecting");
                bids.clear();
                asks.clear();
                if !reconnect(
                    &mut stream,
                    &ws_url,
                    &topic,
                    &mut backoff_ms,
                    reconnect_min_ms,
                    reconnect_max_ms,
                )
                .await
                {
                    return;
                }
            }
            Some(Ok(_)) => {}
        }
    }
}

fn apply_levels(book: &mut BTreeMap<Decimal, Decimal>, deltas: &[[String; 2]]) {
    for entry in deltas {
        let Ok(price) = Decimal::from_str(&entry[0]) else {
            continue;
        };
        let Ok(size) = Decimal::from_str(&entry[1]) else {
            continue;
        };
        if size.is_zero() {
            book.remove(&price);
        } else {
            book.insert(price, size);
        }
    }
}

fn build_event(
    bids: &BTreeMap<Decimal, Decimal>,
    asks: &BTreeMap<Decimal, Decimal>,
    symbol: &Symbol,
    ts: Timestamp,
) -> MarketEvent {
    // Bids: descending price (best first). Asks: ascending price (best first).
    let bids_top: Vec<Level> = bids
        .iter()
        .rev()
        .map(|(p, s)| Level {
            price: Price(*p),
            size: Size(*s),
        })
        .collect();
    let asks_top: Vec<Level> = asks
        .iter()
        .map(|(p, s)| Level {
            price: Price(*p),
            size: Size(*s),
        })
        .collect();
    MarketEvent::BookUpdate {
        snapshot: Snapshot {
            symbol: symbol.clone(),
            bids: bids_top,
            asks: asks_top,
            ts,
        },
    }
}

async fn reconnect(
    stream: &mut WsStream,
    ws_url: &str,
    topic: &str,
    backoff_ms: &mut u64,
    reconnect_min_ms: u64,
    reconnect_max_ms: u64,
) -> bool {
    loop {
        warn!(backoff_ms = *backoff_ms, "bybit depth WS reconnecting");
        tokio::time::sleep(Duration::from_millis(*backoff_ms)).await;
        match open_and_subscribe(ws_url, topic).await {
            Ok(new_stream) => {
                *stream = new_stream;
                *backoff_ms = reconnect_min_ms;
                return true;
            }
            Err(e) => {
                warn!(error = ?e, "bybit depth WS reconnect failed");
                *backoff_ms = (*backoff_ms).saturating_mul(2).min(reconnect_max_ms);
            }
        }
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
