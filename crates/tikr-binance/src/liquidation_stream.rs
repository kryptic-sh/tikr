//! Binance USD-M Futures forced-liquidation WebSocket stream.
//!
//! Connects to `wss://fstream.binance.com/ws/!forceOrder@arr` (futures-mainnet only).
//! The stream delivers ALL symbols in a single connection; every frame carries
//! one liquidation event with the symbol embedded in the `o.s` field.
//!
//! ## Reconnect
//!
//! Mirrors the reconnect pattern from `tikr-binance::depth_stream`. Exponential
//! backoff from 1 s to 30 s. Ping/Pong handled explicitly.
//!
//! ## Testnet
//!
//! Testnet has effectively zero forced liquidations and the stream is not
//! useful for recording. Calling [`subscribe_liquidations`] with any non-mainnet
//! futures env returns [`VenueError::Rejected`] immediately.

use futures::SinkExt;
use futures::stream::{BoxStream, StreamExt};
use serde::Deserialize;
use std::str::FromStr;
use std::time::Duration;
use tikr_core::{Decimal, LiquidationEvent, Price, Side, Timestamp};
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

/// Raw JSON shape for a `forceOrder` event from `!forceOrder@arr`.
#[derive(Debug, Deserialize)]
struct ForceOrderFrame {
    /// Event type — always `"forceOrder"`.
    #[serde(rename = "e")]
    pub event_type: String,
    /// Inner order object.
    #[serde(rename = "o")]
    pub order: ForceOrderInner,
}

/// The `o` sub-object of a `forceOrder` frame.
#[derive(Debug, Deserialize)]
struct ForceOrderInner {
    /// Symbol string, e.g. `"BTCUSDT"`.
    #[serde(rename = "s")]
    pub symbol: String,
    /// Side: `"SELL"` = long got liquidated, `"BUY"` = short got liquidated.
    #[serde(rename = "S")]
    pub side: String,
    /// Last filled quantity.
    #[serde(rename = "q")]
    pub qty: String,
    /// Average fill price (`ap`) — the actual execution price.
    #[serde(rename = "ap")]
    pub avg_price: String,
    /// Transaction time in milliseconds.
    #[serde(rename = "T")]
    pub transaction_time_ms: u64,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Subscribe to the `!forceOrder@arr` liquidation stream on futures-mainnet.
///
/// Returns a `BoxStream` yielding [`LiquidationEvent`] values. The stream
/// reconnects automatically with exponential backoff (1 s … 30 s) and
/// never silently exits.
///
/// # Errors
///
/// Returns [`VenueError::Rejected`] if `env` is not
/// [`BinanceEnv::FuturesMainnet`]. Testnet has no meaningful liquidation data
/// and the stream endpoint does not exist there.
pub async fn subscribe_liquidations(
    env: BinanceEnv,
) -> Result<BoxStream<'static, LiquidationEvent>, VenueError> {
    if env != BinanceEnv::FuturesMainnet {
        return Err(VenueError::Rejected {
            reason: "subscribe_liquidations only supports FuturesMainnet — \
                     testnet has no meaningful liquidation data. \
                     Pass BinanceEnv::FuturesMainnet."
                .into(),
        });
    }

    let ws_url = liquidation_ws_url(env);
    let stream = open_liquidation_ws(&ws_url).await?;
    let (tx, rx) = mpsc::channel::<LiquidationEvent>(1024);
    tokio::spawn(liquidation_pump(stream, tx, ws_url));
    let boxed = tokio_stream::wrappers::ReceiverStream::new(rx);
    Ok(Box::pin(boxed))
}

/// Subscribe to `!forceOrder@arr` and forward filtered events to an
/// unbounded channel as [`tikr_core::LiqEvent`]s. The filter matches
/// the raw Binance symbol string (e.g. `"BTCUSDT"`) — events for other
/// symbols are dropped silently.
///
/// The returned receiver is the shape `tikr_paper::spawn_bot` expects
/// as `external_liqs`. The spawned forwarder task exits on receiver
/// drop. Failure to subscribe returns the same `VenueError::Rejected`
/// as [`subscribe_liquidations`] (non-mainnet env, etc.).
pub async fn subscribe_liq_fade(
    env: BinanceEnv,
    symbol_filter: String,
) -> Result<tokio::sync::mpsc::UnboundedReceiver<tikr_core::LiqEvent>, VenueError> {
    let mut upstream = subscribe_liquidations(env).await?;
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<tikr_core::LiqEvent>();
    tokio::spawn(async move {
        while let Some(ev) = upstream.next().await {
            if ev.symbol != symbol_filter {
                continue;
            }
            // Repack the venue type into the strategy-side type.
            // `LiquidationEvent` carries `symbol` + bare `Decimal`s;
            // `LiqEvent` is single-symbol and uses typed wrappers.
            let liq = tikr_core::LiqEvent {
                ts: ev.ts,
                side: ev.side,
                qty: tikr_core::Size(ev.qty),
                price: ev.price,
                notional: tikr_core::Notional(ev.notional),
            };
            if tx.send(liq).is_err() {
                // Runner dropped the receiver — quit silently.
                break;
            }
        }
    });
    Ok(rx)
}

/// Build the `!forceOrder@arr` WS URL for the given env.
///
/// Only `FuturesMainnet` is meaningful; other variants are included for
/// completeness but `subscribe_liquidations` will reject them.
pub fn liquidation_ws_url(env: BinanceEnv) -> String {
    let base = match env {
        BinanceEnv::FuturesMainnet => "wss://fstream.binance.com",
        BinanceEnv::FuturesTestnet => "wss://stream.binancefuture.com",
        BinanceEnv::SpotMainnet => "wss://stream.binance.com:9443",
        BinanceEnv::SpotTestnet => "wss://stream.testnet.binance.vision:9443",
    };
    format!("{base}/ws/!forceOrder@arr")
}

// ---------------------------------------------------------------------------
// Internal
// ---------------------------------------------------------------------------

async fn open_liquidation_ws(ws_url: &str) -> Result<WsStream, VenueError> {
    let (stream, _) = connect_async(ws_url)
        .await
        .map_err(|e| VenueError::Network(std::io::Error::other(e.to_string())))?;
    Ok(stream)
}

async fn liquidation_pump(
    mut stream: WsStream,
    tx: mpsc::Sender<LiquidationEvent>,
    ws_url: String,
) {
    let reconnect_min_ms: u64 = 1_000;
    let reconnect_max_ms: u64 = 30_000;
    let mut backoff_ms = reconnect_min_ms;

    loop {
        let frame = stream.next().await;
        match frame {
            None => {
                if !reconnect_liquidation(
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
                warn!(error = %e, "binance liquidation WS read error; reconnecting");
                if !reconnect_liquidation(
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
                if let Some(event) = parse_liquidation_frame(&txt)
                    && tx.send(event).await.is_err()
                {
                    return; // receiver dropped
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = stream.send(Message::Pong(p)).await;
            }
            Some(Ok(Message::Close(_))) => {
                debug!("binance liquidation WS server close; reconnecting");
                if !reconnect_liquidation(
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

async fn reconnect_liquidation(
    stream: &mut WsStream,
    ws_url: &str,
    backoff_ms: &mut u64,
    reconnect_min_ms: u64,
    reconnect_max_ms: u64,
) -> bool {
    loop {
        warn!(
            backoff_ms = *backoff_ms,
            "binance liquidation WS disconnected; reconnecting"
        );
        tokio::time::sleep(Duration::from_millis(*backoff_ms)).await;
        match open_liquidation_ws(ws_url).await {
            Ok(new_stream) => {
                *stream = new_stream;
                *backoff_ms = reconnect_min_ms;
                return true;
            }
            Err(e) => {
                warn!(error = ?e, "binance liquidation WS reconnect failed");
                *backoff_ms = (*backoff_ms).saturating_mul(2).min(reconnect_max_ms);
            }
        }
    }
}

/// Parse a `!forceOrder@arr` text frame into a [`LiquidationEvent`].
///
/// Returns `None` if the frame is not a `forceOrder` event or any required
/// field is missing / unparseable.
pub fn parse_liquidation_frame(txt: &str) -> Option<LiquidationEvent> {
    let frame: ForceOrderFrame = serde_json::from_str(txt).ok()?;
    if frame.event_type != "forceOrder" {
        return None;
    }
    let inner = frame.order;
    let qty = Decimal::from_str(&inner.qty).ok()?;
    let price = Decimal::from_str(&inner.avg_price).ok()?;
    // Drop zero-price or zero-qty frames (malformed / incomplete fills).
    if price.is_zero() || qty.is_zero() {
        return None;
    }
    let notional = qty * price;
    let side = match inner.side.as_str() {
        "SELL" => Side::Ask, // liquidation side SELL → long was force-closed; we map to Ask (sell)
        "BUY" => Side::Bid,  // liquidation side BUY  → short was force-closed; we map to Bid (buy)
        _ => return None,
    };
    // Transaction time is in ms; convert to ns.
    let ts = Timestamp(inner.transaction_time_ms.saturating_mul(1_000_000));

    Some(LiquidationEvent {
        symbol: inner.symbol,
        side,
        qty,
        price: Price(price),
        notional,
        ts,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tikr_core::Side;

    /// Canonical `forceOrder` frame as specified in the Binance docs.
    fn canonical_frame() -> &'static str {
        r#"{
            "e": "forceOrder",
            "E": 1779270000000,
            "o": {
                "s": "BTCUSDT",
                "S": "SELL",
                "o": "LIMIT",
                "f": "IOC",
                "q": "0.014",
                "p": "77000",
                "ap": "77000",
                "X": "FILLED",
                "l": "0.014",
                "z": "0.014",
                "T": 1779270000123
            }
        }"#
    }

    #[test]
    fn liquidation_ws_url_futures_mainnet() {
        assert_eq!(
            liquidation_ws_url(BinanceEnv::FuturesMainnet),
            "wss://fstream.binance.com/ws/!forceOrder@arr"
        );
    }

    #[test]
    fn parse_sell_liquidation_long_got_hit() {
        let event = parse_liquidation_frame(canonical_frame()).expect("should parse");
        assert_eq!(event.symbol, "BTCUSDT");
        assert_eq!(event.side, Side::Ask); // SELL = long liquidated → Ask
        assert_eq!(event.qty, Decimal::from_str("0.014").unwrap());
        assert_eq!(event.price.0, Decimal::from_str("77000").unwrap());
        // notional = 0.014 * 77000 = 1078
        let expected_notional =
            Decimal::from_str("0.014").unwrap() * Decimal::from_str("77000").unwrap();
        assert_eq!(event.notional, expected_notional);
        // ts = 1779270000123 ms → ns
        assert_eq!(event.ts.0, 1779270000123u64 * 1_000_000);
    }

    #[test]
    fn parse_buy_liquidation_short_got_hit() {
        let txt = r#"{
            "e": "forceOrder",
            "E": 1779270001000,
            "o": {
                "s": "ETHUSDT",
                "S": "BUY",
                "o": "LIMIT",
                "f": "IOC",
                "q": "1.5",
                "p": "3000",
                "ap": "3001.50",
                "X": "FILLED",
                "l": "1.5",
                "z": "1.5",
                "T": 1779270001001
            }
        }"#;
        let event = parse_liquidation_frame(txt).expect("should parse");
        assert_eq!(event.symbol, "ETHUSDT");
        assert_eq!(event.side, Side::Bid); // BUY = short liquidated → Bid
        assert_eq!(event.price.0, Decimal::from_str("3001.50").unwrap());
    }

    #[test]
    fn parse_returns_none_on_garbage() {
        assert!(parse_liquidation_frame("not json").is_none());
    }

    #[test]
    fn parse_returns_none_on_wrong_event_type() {
        let txt = r#"{"e": "depthUpdate", "E": 123, "o": {}}"#;
        assert!(parse_liquidation_frame(txt).is_none());
    }

    #[test]
    fn parse_returns_none_on_zero_price() {
        let txt = r#"{
            "e": "forceOrder",
            "E": 1000,
            "o": {
                "s": "BTCUSDT", "S": "SELL", "q": "1.0", "ap": "0", "T": 1000
            }
        }"#;
        assert!(parse_liquidation_frame(txt).is_none());
    }

    #[tokio::test]
    async fn testnet_rejected() {
        let result = subscribe_liquidations(BinanceEnv::FuturesTestnet).await;
        assert!(
            matches!(result, Err(VenueError::Rejected { .. })),
            "testnet must be rejected"
        );
    }

    #[tokio::test]
    async fn spot_envs_rejected() {
        assert!(matches!(
            subscribe_liquidations(BinanceEnv::SpotTestnet).await,
            Err(VenueError::Rejected { .. })
        ));
        assert!(matches!(
            subscribe_liquidations(BinanceEnv::SpotMainnet).await,
            Err(VenueError::Rejected { .. })
        ));
    }
}
