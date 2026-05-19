//! Binance public aggTrade WebSocket subscription.
//!
//! Connects to the `@aggTrade` stream. No authentication required.
//! Mirrors [`crate::depth_stream`] in shape (reconnect, frame pump).
//!
//! ## Frame shape
//!
//! Both spot and futures emit:
//! `{"e":"aggTrade","s":"BTCUSDT","p":"<price>","q":"<qty>","T":<tradeTime>,
//!   "m":<isBuyerMaker>}`
//!
//! `m=true` → taker is the seller (taker_side = Ask).
//! `m=false` → taker is the buyer (taker_side = Bid).

use futures::SinkExt;
use futures::stream::{BoxStream, StreamExt};
use serde::Deserialize;
use std::str::FromStr;
use std::time::Duration;
use tikr_core::{Decimal, MarketEvent, Price, Side, Size, Symbol, Timestamp};
use tikr_venue::VenueError;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tracing::warn;

use crate::BinanceEnv;
use crate::depth_stream::binance_symbol;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, Deserialize)]
struct AggTradeFrame {
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    qty: String,
    #[serde(rename = "T")]
    trade_time_ms: u64,
    #[serde(rename = "m")]
    is_buyer_maker: bool,
}

/// Subscribe to the public aggTrade stream for `symbol`.
pub async fn subscribe_trades(
    env: BinanceEnv,
    symbol: Symbol,
) -> Result<BoxStream<'static, MarketEvent>, VenueError> {
    let sym_lower = binance_symbol(&symbol).to_lowercase();
    let ws_url = trade_ws_url(env, &sym_lower);

    let stream = open_ws(&ws_url).await?;
    let (tx, rx) = mpsc::channel::<MarketEvent>(512);

    tokio::spawn(trade_pump(stream, tx, ws_url, symbol));

    Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))
}

/// Build the trade-stream WS URL.
///
/// Spot uses `@aggTrade` (the canonical aggregated trade stream).
/// **Futures uses `@trade`** — verified 2026-05-19 that `@aggTrade` on
/// `fstream.binance.com` connects but emits nothing for BTC for 13+ minutes
/// while `@trade` fires immediately. Frame field shapes (`p`, `q`, `T`, `m`)
/// match for both, so the same parser handles both.
pub fn trade_ws_url(env: BinanceEnv, sym_lower: &str) -> String {
    let (base, stream) = match env {
        BinanceEnv::SpotTestnet => ("wss://stream.testnet.binance.vision:9443/ws", "aggTrade"),
        BinanceEnv::SpotMainnet => ("wss://stream.binance.com:9443/ws", "aggTrade"),
        BinanceEnv::FuturesTestnet => ("wss://stream.binancefuture.com/ws", "trade"),
        BinanceEnv::FuturesMainnet => ("wss://fstream.binance.com/ws", "trade"),
    };
    format!("{base}/{sym_lower}@{stream}")
}

async fn open_ws(ws_url: &str) -> Result<WsStream, VenueError> {
    let (stream, _) = connect_async(ws_url)
        .await
        .map_err(|e| VenueError::Network(std::io::Error::other(e.to_string())))?;
    Ok(stream)
}

async fn trade_pump(
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
                if let Some(event) = parse_agg_trade_frame(&txt, &symbol)
                    && tx.send(event).await.is_err()
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
            "binance trade WS disconnected; reconnecting"
        );
        tokio::time::sleep(Duration::from_millis(*backoff_ms)).await;
        match open_ws(ws_url).await {
            Ok(new_stream) => {
                *stream = new_stream;
                *backoff_ms = reconnect_min_ms;
                return true;
            }
            Err(e) => {
                warn!(error = ?e, "binance trade WS reconnect failed");
                *backoff_ms = (*backoff_ms).saturating_mul(2).min(reconnect_max_ms);
            }
        }
    }
}

/// Parse an `aggTrade` frame into a [`MarketEvent::Trade`].
pub fn parse_agg_trade_frame(txt: &str, symbol: &Symbol) -> Option<MarketEvent> {
    let frame: AggTradeFrame = serde_json::from_str(txt).ok()?;
    let price = Decimal::from_str(&frame.price).ok()?;
    let size = Decimal::from_str(&frame.qty).ok()?;
    // is_buyer_maker = true → taker is the SELLER → taker_side = Ask.
    let taker_side = if frame.is_buyer_maker {
        Side::Ask
    } else {
        Side::Bid
    };
    Some(MarketEvent::Trade {
        symbol: symbol.clone(),
        price: Price(price),
        size: Size(size),
        side: taker_side,
        ts: Timestamp(frame.trade_time_ms.saturating_mul(1_000_000)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::{Asset, MarketKind, VenueId};

    fn sym() -> Symbol {
        Symbol {
            base: Asset::new("BTC"),
            quote: Asset::new("USDT"),
            venue: VenueId::new("binance"),
            kind: MarketKind::Perp,
        }
    }

    #[test]
    fn trade_url_futures_mainnet_uses_trade_not_aggtrade() {
        assert_eq!(
            trade_ws_url(BinanceEnv::FuturesMainnet, "btcusdt"),
            "wss://fstream.binance.com/ws/btcusdt@trade"
        );
    }

    #[test]
    fn trade_url_spot_mainnet_uses_aggtrade() {
        assert_eq!(
            trade_ws_url(BinanceEnv::SpotMainnet, "btcusdt"),
            "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
        );
    }

    #[test]
    fn parse_agg_trade_buyer_maker_means_taker_sell() {
        let f = r#"{"e":"aggTrade","E":1,"s":"BTCUSDT","a":1,"p":"76800.00","q":"0.001","f":1,"l":1,"T":1700000000000,"m":true}"#;
        let ev = parse_agg_trade_frame(f, &sym()).expect("parse");
        match ev {
            MarketEvent::Trade {
                price, size, side, ..
            } => {
                assert_eq!(price.0, Decimal::from_str("76800.00").unwrap());
                assert_eq!(size.0, Decimal::from_str("0.001").unwrap());
                assert_eq!(side, Side::Ask);
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn parse_agg_trade_taker_buy() {
        let f = r#"{"e":"aggTrade","s":"BTCUSDT","p":"76800.0","q":"0.002","T":1700000000000,"m":false}"#;
        let ev = parse_agg_trade_frame(f, &sym()).expect("parse");
        match ev {
            MarketEvent::Trade { side, .. } => assert_eq!(side, Side::Bid),
            _ => panic!("expected Trade"),
        }
    }

    #[test]
    fn parse_agg_trade_returns_none_on_garbage() {
        assert!(parse_agg_trade_frame("not json", &sym()).is_none());
    }
}
