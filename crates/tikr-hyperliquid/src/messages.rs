//! Serde structs mirroring Hyperliquid wire formats.
//!
//! All shapes match the public Info HTTP and WebSocket APIs as documented at
//! <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint>.
//!
//! Decimal-valued fields are kept as `String` and parsed via
//! [`rust_decimal::Decimal::from_str_exact`] in [`crate::mapping`] to avoid
//! float drift.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// WebSocket push: l2Book
// ---------------------------------------------------------------------------

/// `l2Book` channel push payload.
#[derive(Debug, Clone, Deserialize)]
pub struct L2BookPush {
    /// Hyperliquid coin name (base asset, e.g. `"BTC"`).
    pub coin: String,
    /// Snapshot time in milliseconds since UNIX epoch.
    pub time: u64,
    /// `[bids, asks]`. Bids descending by price, asks ascending — matches
    /// the [`tikr_core::Snapshot`] contract.
    pub levels: [Vec<L2Level>; 2],
}

/// A single price level in an `l2Book` push.
#[derive(Debug, Clone, Deserialize)]
pub struct L2Level {
    /// Price (decimal as string).
    pub px: String,
    /// Aggregated size (decimal as string).
    pub sz: String,
    /// Order count at this level. Ignored by our mapping (not in our `Level` type).
    #[allow(dead_code)]
    pub n: u32,
}

// ---------------------------------------------------------------------------
// WebSocket push: trades
// ---------------------------------------------------------------------------

/// A single trade in a `trades` channel push.
#[derive(Debug, Clone, Deserialize)]
pub struct TradePush {
    /// Hyperliquid coin name.
    pub coin: String,
    /// Taker side: `"A"` = ask-taker (sold), `"B"` = bid-taker (bought).
    pub side: String,
    /// Trade price (decimal as string).
    pub px: String,
    /// Trade size (decimal as string).
    pub sz: String,
    /// Trade time in milliseconds since UNIX epoch.
    pub time: u64,
    /// Venue-side trade hash. Unused by mapping; retained for debugging.
    #[allow(dead_code)]
    pub hash: String,
    /// Venue-side trade id. Unused by mapping; retained for debugging.
    #[allow(dead_code)]
    pub tid: u64,
}

// ---------------------------------------------------------------------------
// WebSocket envelope
// ---------------------------------------------------------------------------

/// Top-level WS message envelope.
///
/// Hyperliquid wraps every push in `{ "channel": "...", "data": ... }`. The
/// `subscriptionResponse` ack is informational; `Other` swallows any future
/// channel we don't yet handle.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "channel", content = "data")]
pub enum WsMessage {
    /// `l2Book` channel push.
    #[serde(rename = "l2Book")]
    L2Book(L2BookPush),
    /// `trades` channel push (multiple trades per frame).
    #[serde(rename = "trades")]
    Trades(Vec<TradePush>),
    /// Subscription acknowledgement; carries the original subscription back.
    #[serde(rename = "subscriptionResponse")]
    SubscriptionResponse(serde_json::Value),
    /// Any other channel (e.g. `post`, `pong`); ignored. Captures the raw
    /// data payload so deserialization doesn't fail on unknown channels.
    #[serde(other, deserialize_with = "deserialize_ignore_any")]
    Other,
}

fn deserialize_ignore_any<'de, D: serde::Deserializer<'de>>(d: D) -> Result<(), D::Error> {
    serde::de::IgnoredAny::deserialize(d).map(|_| ())
}

// ---------------------------------------------------------------------------
// WebSocket subscribe outbound
// ---------------------------------------------------------------------------

/// Outbound subscribe frame.
#[derive(Debug, Clone, Serialize)]
pub struct SubscribeMessage<'a> {
    /// Always `"subscribe"`.
    pub method: &'static str,
    /// Subscription parameters.
    pub subscription: Subscription<'a>,
}

/// Subscription parameters for a single channel + coin.
#[derive(Debug, Clone, Serialize)]
pub struct Subscription<'a> {
    /// Channel name: `"l2Book"` or `"trades"`.
    #[serde(rename = "type")]
    pub kind: &'static str,
    /// Hyperliquid coin (base asset).
    pub coin: &'a str,
}

// ---------------------------------------------------------------------------
// HTTP /info: clearinghouseState
// ---------------------------------------------------------------------------

/// `clearinghouseState` response.
///
/// Only the fields we map are deserialized; `marginSummary`, `withdrawable`,
/// etc. are dropped.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClearinghouseStateResp {
    /// Per-asset open positions.
    pub asset_positions: Vec<AssetPositionEntry>,
    /// Server time in milliseconds since UNIX epoch. Unused; retained for debugging.
    #[allow(dead_code)]
    pub time: u64,
}

/// One `assetPositions[]` entry.
#[derive(Debug, Clone, Deserialize)]
pub struct AssetPositionEntry {
    /// The position details.
    pub position: HyperliquidPosition,
}

/// Hyperliquid-side position representation.
///
/// Many fields (`cumFunding`, `unrealizedPnl`, `leverage`, etc.) are dropped
/// for the Phase 3 v0 mapping. They may be re-added when realized-PnL
/// computation is wired up.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HyperliquidPosition {
    /// Hyperliquid coin name (base asset).
    pub coin: String,
    /// Signed size as a decimal string. Negative = short.
    pub szi: String,
    /// Volume-weighted average entry price. `None` when `szi == 0`.
    pub entry_px: Option<String>,
}

// ---------------------------------------------------------------------------
// HTTP /info: userFills
// ---------------------------------------------------------------------------

/// One entry in the `userFills` response array.
///
/// Fields we don't map (`dir`, `closedPnl`, `hash`, `crossed`,
/// `startPosition`) are dropped at deserialization.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserFillEntry {
    /// Hyperliquid coin (base asset of the fill).
    pub coin: String,
    /// Fill price (decimal as string).
    pub px: String,
    /// Fill size (decimal as string).
    pub sz: String,
    /// User side: `"B"` = bought, `"A"` = sold.
    pub side: String,
    /// Fill time in milliseconds since UNIX epoch.
    pub time: u64,
    /// Fee paid (positive decimal string).
    pub fee: String,
    /// Fee currency (typically `"USDC"`).
    pub fee_token: String,
    /// Venue-side trade id. Unused by mapping.
    #[allow(dead_code)]
    pub tid: u64,
    /// Venue-side order id; mapped into [`tikr_core::QuoteId`].
    pub oid: u64,
}

/// One entry in the `openOrders` info response.
///
/// Hyperliquid returns the currently-resting orders for a user. `sz` is the
/// *remaining* size (partials show the unfilled remainder); `limitPx` is the
/// resting price; `side` follows the B/A convention (`"B"` = bid, `"A"` = ask).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenOrderEntry {
    /// Hyperliquid coin (base asset the order rests on).
    pub coin: String,
    /// Resting limit price (decimal as string).
    pub limit_px: String,
    /// Remaining size (decimal as string).
    pub sz: String,
    /// Order side: `"B"` = bid, `"A"` = ask.
    pub side: String,
    /// Venue-side order id; mapped into [`tikr_core::QuoteId`].
    pub oid: u64,
}
