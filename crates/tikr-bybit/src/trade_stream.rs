//! Bybit V5 public trade stream.
//!
//! Subscribes to `publicTrade.{SYMBOL}` and emits one
//! [`MarketEvent::Trade`] per print. Used by `tikr-paper`'s FillSim
//! to drive taker-side simulated fills on resting maker quotes.
//!
//! ## Wire format
//!
//! ```json
//! {
//!   "topic": "publicTrade.BTCUSDT",
//!   "type":  "snapshot",
//!   "ts":    1731700000000,
//!   "data": [
//!     {
//!       "i": "trade-id",
//!       "T": 1731700000123,    // ms timestamp of the trade
//!       "p": "62000.5",
//!       "v": "0.123",
//!       "S": "Buy" | "Sell"    // aggressor (taker) side
//!     }, ...
//!   ]
//! }
//! ```

use futures::SinkExt;
use futures::stream::{BoxStream, StreamExt};
use serde::Deserialize;
use std::str::FromStr;
use std::time::Duration;
use tikr_core::{Decimal, MarketEvent, Price, Size, Symbol, Timestamp};
use tikr_venue::VenueError;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tracing::{debug, warn};

use crate::BybitEnv;
use crate::mapping::{bybit_symbol, parse_side_wire};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Deserialize)]
struct WsFrame {
    topic: Option<String>,
    data: Option<Vec<TradePrint>>,
}

#[derive(Deserialize)]
struct TradePrint {
    /// Trade epoch milliseconds.
    #[serde(rename = "T")]
    t: u64,
    /// Trade price.
    p: String,
    /// Trade size (base currency).
    v: String,
    /// Aggressor side, `"Buy"` or `"Sell"`.
    #[serde(rename = "S")]
    s: String,
}

/// Subscribe to public trade prints for `symbol`. Returns a stream of
/// [`MarketEvent::Trade`]; auto-reconnects with backoff.
pub async fn subscribe_trades(
    env: BybitEnv,
    symbol: Symbol,
) -> Result<BoxStream<'static, MarketEvent>, VenueError> {
    let ws_url = env.public_ws_url().to_string();
    let topic = format!("publicTrade.{}", bybit_symbol(&symbol));
    let stream = open_and_subscribe(&ws_url, &topic).await?;

    let (tx, rx) = mpsc::channel::<MarketEvent>(1024);
    tokio::spawn(trade_pump(stream, tx, ws_url, topic, symbol));
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

async fn trade_pump(
    mut stream: WsStream,
    tx: mpsc::Sender<MarketEvent>,
    ws_url: String,
    topic: String,
    symbol: Symbol,
) {
    let reconnect_min_ms: u64 = 1_000;
    let reconnect_max_ms: u64 = 30_000;
    let mut backoff_ms = reconnect_min_ms;
    loop {
        match stream.next().await {
            None => {
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
                warn!(error = %e, "bybit trade WS read error; reconnecting");
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
                    debug!(body = %txt, "bybit trade: non-JSON or unexpected shape");
                    continue;
                };
                if frame.topic.as_deref() != Some(topic.as_str()) {
                    continue;
                }
                let Some(prints) = frame.data else { continue };
                for p in prints {
                    let Some(side) = parse_side_wire(&p.s) else {
                        continue;
                    };
                    let Ok(price) = Decimal::from_str(&p.p) else {
                        continue;
                    };
                    let Ok(size) = Decimal::from_str(&p.v) else {
                        continue;
                    };
                    let ev = MarketEvent::Trade {
                        symbol: symbol.clone(),
                        price: Price(price),
                        size: Size(size),
                        side,
                        ts: Timestamp(p.t.saturating_mul(1_000_000)),
                    };
                    if tx.send(ev).await.is_err() {
                        return;
                    }
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = stream.send(Message::Pong(p)).await;
            }
            Some(Ok(Message::Close(_))) => {
                debug!("bybit trade WS server close; reconnecting");
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

async fn reconnect(
    stream: &mut WsStream,
    ws_url: &str,
    topic: &str,
    backoff_ms: &mut u64,
    reconnect_min_ms: u64,
    reconnect_max_ms: u64,
) -> bool {
    loop {
        warn!(backoff_ms = *backoff_ms, "bybit trade WS reconnecting");
        tokio::time::sleep(Duration::from_millis(*backoff_ms)).await;
        match open_and_subscribe(ws_url, topic).await {
            Ok(new_stream) => {
                *stream = new_stream;
                *backoff_ms = reconnect_min_ms;
                return true;
            }
            Err(e) => {
                warn!(error = ?e, "bybit trade WS reconnect failed");
                *backoff_ms = (*backoff_ms).saturating_mul(2).min(reconnect_max_ms);
            }
        }
    }
}
