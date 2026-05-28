//! Binance USD-M futures mark-price WebSocket subscription.
//!
//! Connects to `<symbol>@markPrice@1s`. Mark price is a **futures-only**
//! concept (spot has no mark), so this stream is only meaningful for the
//! `Futures*` environments. The frame also carries the current funding rate
//! and next funding time, which the recorder persists alongside the mark for
//! later funding-from-file replay.
//!
//! ## Frame shape
//! `{"e":"markPriceUpdate","E":<eventTimeMs>,"s":"BTCUSDT","p":"<mark>",
//!   "i":"<index>","r":"<fundingRate>","T":<nextFundingTimeMs>}`

use futures::SinkExt;
use futures::stream::{BoxStream, StreamExt};
use serde::Deserialize;
use std::str::FromStr;
use std::time::Duration;
use tikr_core::{Decimal, Symbol, Timestamp};
use tikr_venue::VenueError;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tracing::warn;

use crate::BinanceEnv;
use crate::depth_stream::binance_symbol;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// One mark-price update from the venue.
#[derive(Debug, Clone)]
pub struct MarkUpdate {
    /// Event time (from the frame's `E` field).
    pub ts: Timestamp,
    /// Mark price (`p`).
    pub mark_price: Decimal,
    /// Current funding rate as a fraction (`r`, e.g. `0.0001` = 1 bp).
    pub funding_rate: Decimal,
    /// Next funding time (`T`).
    pub next_funding_ts: Timestamp,
}

#[derive(Debug, Deserialize)]
struct MarkPriceFrame {
    #[serde(rename = "E")]
    event_time_ms: u64,
    #[serde(rename = "p")]
    mark_price: String,
    #[serde(rename = "r")]
    funding_rate: String,
    #[serde(rename = "T")]
    next_funding_ms: u64,
}

/// Subscribe to the `@markPrice@1s` stream for `symbol`. Futures-only — see
/// module docs. Returns [`VenueError`] for spot environments.
pub async fn subscribe_mark_price(
    env: BinanceEnv,
    symbol: Symbol,
) -> Result<BoxStream<'static, MarkUpdate>, VenueError> {
    let sym_lower = binance_symbol(&symbol).to_lowercase();
    let ws_url = mark_ws_url(env, &sym_lower)?;

    let stream = open_ws(&ws_url).await?;
    let (tx, rx) = mpsc::channel::<MarkUpdate>(256);

    tokio::spawn(mark_pump(stream, tx, ws_url));

    Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))
}

/// Build the mark-price WS URL. Errors for spot envs (no mark price).
pub fn mark_ws_url(env: BinanceEnv, sym_lower: &str) -> Result<String, VenueError> {
    let base = match env {
        BinanceEnv::FuturesTestnet => "wss://stream.binancefuture.com/ws",
        BinanceEnv::FuturesMainnet => "wss://fstream.binance.com/ws",
        BinanceEnv::SpotTestnet | BinanceEnv::SpotMainnet => {
            return Err(VenueError::Rejected {
                reason: "mark price stream is futures-only".to_string(),
            });
        }
    };
    Ok(format!("{base}/{sym_lower}@markPrice@1s"))
}

async fn open_ws(ws_url: &str) -> Result<WsStream, VenueError> {
    let (stream, _) = connect_async(ws_url)
        .await
        .map_err(|e| VenueError::Network(std::io::Error::other(e.to_string())))?;
    Ok(stream)
}

async fn mark_pump(mut stream: WsStream, tx: mpsc::Sender<MarkUpdate>, ws_url: String) {
    let reconnect_min_ms: u64 = 1_000;
    let reconnect_max_ms: u64 = 30_000;
    let mut backoff_ms = reconnect_min_ms;

    loop {
        let frame = stream.next().await;
        match frame {
            None | Some(Err(_)) | Some(Ok(Message::Close(_))) => {
                if !reconnect(
                    &mut stream,
                    &ws_url,
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
                if let Some(update) = parse_mark_price_frame(&txt)
                    && tx.send(update).await.is_err()
                {
                    return;
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = stream.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
        }
    }
}

async fn reconnect(
    stream: &mut WsStream,
    ws_url: &str,
    backoff_ms: &mut u64,
    reconnect_min_ms: u64,
    reconnect_max_ms: u64,
) -> bool {
    loop {
        warn!(
            backoff_ms = *backoff_ms,
            "binance mark-price WS disconnected; reconnecting"
        );
        tokio::time::sleep(Duration::from_millis(*backoff_ms)).await;
        match open_ws(ws_url).await {
            Ok(new_stream) => {
                *stream = new_stream;
                *backoff_ms = reconnect_min_ms;
                return true;
            }
            Err(e) => {
                warn!(error = ?e, "binance mark-price WS reconnect failed");
                *backoff_ms = (*backoff_ms).saturating_mul(2).min(reconnect_max_ms);
            }
        }
    }
}

/// Parse a `markPriceUpdate` frame. Returns `None` on garbage or a
/// non-positive mark price.
pub fn parse_mark_price_frame(txt: &str) -> Option<MarkUpdate> {
    let frame: MarkPriceFrame = serde_json::from_str(txt).ok()?;
    let mark_price = Decimal::from_str(&frame.mark_price).ok()?;
    if mark_price <= Decimal::ZERO {
        return None;
    }
    // Funding rate can legitimately be negative or absent; default to zero.
    let funding_rate = Decimal::from_str(&frame.funding_rate).unwrap_or(Decimal::ZERO);
    Some(MarkUpdate {
        ts: Timestamp(frame.event_time_ms.saturating_mul(1_000_000)),
        mark_price,
        funding_rate,
        next_funding_ts: Timestamp(frame.next_funding_ms.saturating_mul(1_000_000)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mark_url_futures_mainnet() {
        assert_eq!(
            mark_ws_url(BinanceEnv::FuturesMainnet, "btcusdt").unwrap(),
            "wss://fstream.binance.com/ws/btcusdt@markPrice@1s"
        );
    }

    #[test]
    fn mark_url_spot_is_unsupported() {
        assert!(mark_ws_url(BinanceEnv::SpotMainnet, "btcusdt").is_err());
    }

    #[test]
    fn parse_mark_frame_extracts_fields() {
        let f = r#"{"e":"markPriceUpdate","E":1700000000000,"s":"BTCUSDT","p":"76800.50","i":"76799.0","P":"76801.0","r":"0.0001","T":1700028800000}"#;
        let u = parse_mark_price_frame(f).expect("parse");
        assert_eq!(u.mark_price, Decimal::from_str("76800.50").unwrap());
        assert_eq!(u.funding_rate, Decimal::from_str("0.0001").unwrap());
        assert_eq!(u.ts, Timestamp(1_700_000_000_000 * 1_000_000));
        assert_eq!(u.next_funding_ts, Timestamp(1_700_028_800_000 * 1_000_000));
    }

    #[test]
    fn parse_mark_frame_rejects_garbage_and_zero() {
        assert!(parse_mark_price_frame("nope").is_none());
        let zero = r#"{"e":"markPriceUpdate","E":1,"s":"BTCUSDT","p":"0","r":"0","T":1}"#;
        assert!(parse_mark_price_frame(zero).is_none());
    }
}
