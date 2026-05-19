//! Binance public orderbook depth WebSocket subscription.
//!
//! Connects to the `@depth20@100ms` stream (top-20 levels, 100 ms updates).
//! No authentication required.
//!
//! ## Endpoints
//!
//! | Product  | URL |
//! |----------|-----|
//! | Spot testnet | `wss://testnet.binance.vision/ws/<sym>@depth20@100ms` |
//! | Spot mainnet | `wss://stream.binance.com:9443/ws/<sym>@depth20@100ms` |
//! | Futures testnet | `wss://stream.binancefuture.com/ws/<sym>@depth20@100ms` |
//! | Futures mainnet | `wss://fstream.binance.com/ws/<sym>@depth20@100ms` |
//!
//! ## Reconnect
//!
//! Mirrors the reconnect pattern from `tikr-hyperliquid::ws`. Exponential
//! backoff from 1 s to 30 s. No keepalive needed (public stream).

use futures::SinkExt;
use futures::stream::{BoxStream, StreamExt};
use serde::Deserialize;
use std::str::FromStr;
use std::time::Duration;
use tikr_core::{Decimal, Level, MarketEvent, Price, Size, Snapshot, Symbol, Timestamp};
use tikr_venue::VenueError;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tracing::{debug, warn};

use crate::BinanceEnv;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

/// Binance depth update JSON shape (`@depth20@100ms`).
#[derive(Debug, Deserialize)]
pub struct DepthUpdate {
    /// Bid price levels `[price, size]`.
    #[serde(rename = "bids")]
    pub bids: Vec<[String; 2]>,
    /// Ask price levels `[price, size]`.
    #[serde(rename = "asks")]
    pub asks: Vec<[String; 2]>,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Subscribe to the public depth stream for `symbol`.
///
/// Returns a `BoxStream<'static, MarketEvent>` yielding [`MarketEvent::BookUpdate`]
/// frames. Reconnects automatically with exponential backoff.
pub async fn subscribe_depth(
    env: BinanceEnv,
    symbol: Symbol,
) -> Result<BoxStream<'static, MarketEvent>, VenueError> {
    let sym_lower = binance_symbol(&symbol).to_lowercase();
    let ws_url = depth_ws_url(env, &sym_lower);

    let stream = open_depth_ws(&ws_url).await?;

    let (tx, rx) = mpsc::channel::<MarketEvent>(512);

    tokio::spawn(depth_pump(stream, tx, ws_url, symbol));

    let boxed = tokio_stream::wrappers::ReceiverStream::new(rx);
    Ok(Box::pin(boxed))
}

/// Build the depth WS URL for the given env and lowercase symbol.
pub fn depth_ws_url(env: BinanceEnv, sym_lower: &str) -> String {
    let base = match env {
        BinanceEnv::SpotTestnet => "wss://testnet.binance.vision/ws",
        BinanceEnv::SpotMainnet => "wss://stream.binance.com:9443/ws",
        BinanceEnv::FuturesTestnet => "wss://stream.binancefuture.com/ws",
        BinanceEnv::FuturesMainnet => "wss://fstream.binance.com/ws",
    };
    format!("{base}/{sym_lower}@depth20@100ms")
}

/// Build the Binance symbol string (uppercase base+quote).
pub fn binance_symbol(sym: &Symbol) -> String {
    format!("{}{}", sym.base.0.as_ref(), sym.quote.0.as_ref()).to_uppercase()
}

// ---------------------------------------------------------------------------
// Internal
// ---------------------------------------------------------------------------

async fn open_depth_ws(ws_url: &str) -> Result<WsStream, VenueError> {
    let (stream, _) = connect_async(ws_url)
        .await
        .map_err(|e| VenueError::Network(std::io::Error::other(e.to_string())))?;
    Ok(stream)
}

async fn depth_pump(
    mut stream: WsStream,
    tx: mpsc::Sender<MarketEvent>,
    ws_url: String,
    symbol: Symbol,
) {
    let reconnect_min_ms: u64 = 1_000;
    let reconnect_max_ms: u64 = 30_000;
    let mut backoff_ms = reconnect_min_ms;

    loop {
        let frame = stream.next().await;
        match frame {
            None => {
                if !reconnect_depth(
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
            Some(Err(e)) => {
                warn!(error = %e, "binance depth WS read error; reconnecting");
                if !reconnect_depth(
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
                if let Some(event) = parse_depth_frame(&txt, &symbol)
                    && tx.send(event).await.is_err()
                {
                    return; // receiver dropped
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = stream.send(Message::Pong(p)).await;
            }
            Some(Ok(Message::Close(_))) => {
                debug!("binance depth WS server close; reconnecting");
                if !reconnect_depth(
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
            Some(Ok(_)) => {} // Binary / Pong — ignore
        }
    }
}

async fn reconnect_depth(
    stream: &mut WsStream,
    ws_url: &str,
    backoff_ms: &mut u64,
    reconnect_min_ms: u64,
    reconnect_max_ms: u64,
) -> bool {
    loop {
        warn!(
            backoff_ms = *backoff_ms,
            "binance depth WS disconnected; reconnecting"
        );
        tokio::time::sleep(Duration::from_millis(*backoff_ms)).await;
        match open_depth_ws(ws_url).await {
            Ok(new_stream) => {
                *stream = new_stream;
                *backoff_ms = reconnect_min_ms;
                return true;
            }
            Err(e) => {
                warn!(error = ?e, "binance depth WS reconnect failed");
                *backoff_ms = (*backoff_ms).saturating_mul(2).min(reconnect_max_ms);
            }
        }
    }
}

/// Parse a `@depth20@100ms` text frame into a [`MarketEvent::BookUpdate`].
pub fn parse_depth_frame(txt: &str, symbol: &Symbol) -> Option<MarketEvent> {
    let update: DepthUpdate = serde_json::from_str(txt).ok()?;

    let ts = Timestamp(now_ns());

    let bids: Vec<Level> = update.bids.iter().filter_map(parse_level).collect();
    let asks: Vec<Level> = update.asks.iter().filter_map(parse_level).collect();

    Some(MarketEvent::BookUpdate {
        snapshot: Snapshot {
            symbol: symbol.clone(),
            bids,
            asks,
            ts,
        },
    })
}

fn parse_level(entry: &[String; 2]) -> Option<Level> {
    let price = Decimal::from_str(&entry[0]).ok()?;
    let size = Decimal::from_str(&entry[1]).ok()?;
    Some(Level {
        price: Price(price),
        size: Size(size),
    })
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}
