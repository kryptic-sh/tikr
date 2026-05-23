//! Bybit V5 (linear perpetuals) venue adapter.
//!
//! Phase 1 scope — **paper mode only**:
//! - Public WS book stream → [`tikr_core::MarketEvent::BookUpdate`]
//! - Public WS trade stream → [`tikr_core::MarketEvent::Trade`]
//! - REST orderbook snapshot for [`Venue::snapshot`]
//! - All order-placement methods return `VenueError::Unsupported` —
//!   tikr-paper backtest/forward-test paths never call them.
//!
//! Phase 2 will wire HMAC-signed REST + private WS execution stream
//! for live trading; see issue tracker.

#![deny(missing_docs)]

pub mod depth_stream;
pub mod mapping;
pub mod rest;
pub mod sign;
pub mod trade_stream;

use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use std::str::FromStr;

use tikr_core::{Fill, MarketEvent, Position, SignedSize, Symbol, Decimal, Notional, Price};
use tikr_venue::{OpenOrder, QuoteId, QuoteIntent, Venue, VenueError};

/// Bybit V5 environment (mainnet vs testnet, linear product).
///
/// Spot/inverse/options live on separate REST + WS hosts; this crate
/// targets **linear USDT-margined perps** only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BybitEnv {
    /// Linear perps testnet — safe to write keys.
    LinearTestnet,
    /// Linear perps mainnet — REQUIRES `BYBIT_ENABLE_MAINNET=1` for any
    /// signed (live) call. Read-only public streams are always allowed.
    LinearMainnet,
}

impl BybitEnv {
    /// Public REST base URL for orderbook snapshots.
    pub fn rest_base_url(&self) -> &'static str {
        match self {
            BybitEnv::LinearTestnet => "https://api-testnet.bybit.com",
            BybitEnv::LinearMainnet => "https://api.bybit.com",
        }
    }
    /// Public WS host (book + trades, no auth required).
    pub fn public_ws_url(&self) -> &'static str {
        match self {
            BybitEnv::LinearTestnet => "wss://stream-testnet.bybit.com/v5/public/linear",
            BybitEnv::LinearMainnet => "wss://stream.bybit.com/v5/public/linear",
        }
    }
    /// Whether this environment writes against real funds.
    pub fn is_mainnet(&self) -> bool {
        matches!(self, BybitEnv::LinearMainnet)
    }
}

impl FromStr for BybitEnv {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "linear-testnet" | "testnet" => Ok(BybitEnv::LinearTestnet),
            "linear-mainnet" | "mainnet" => Ok(BybitEnv::LinearMainnet),
            other => Err(format!(
                "unknown Bybit env '{other}' (expected linear-testnet | linear-mainnet)"
            )),
        }
    }
}

/// Top-level Bybit client.
///
/// Phase 1 holds only the public surface (REST snapshot + WS subscribe).
/// Phase 2 will add `api_key` + signing-state fields.
#[derive(Debug, Clone)]
pub struct BybitClient {
    env: BybitEnv,
    http: reqwest::Client,
}

impl BybitClient {
    /// Construct a new client. Phase 1: no credentials needed — public
    /// streams + snapshot only.
    pub fn new(env: BybitEnv) -> Self {
        Self {
            env,
            http: reqwest::Client::new(),
        }
    }
    /// The configured environment.
    pub fn env(&self) -> BybitEnv {
        self.env
    }
}

#[async_trait]
impl Venue for BybitClient {
    fn id(&self) -> &str {
        match self.env {
            BybitEnv::LinearTestnet => "bybit-linear-testnet",
            BybitEnv::LinearMainnet => "bybit-linear-mainnet",
        }
    }

    async fn snapshot(&self, symbol: &Symbol) -> Result<tikr_core::Snapshot, VenueError> {
        rest::orderbook_snapshot(&self.http, self.env, symbol).await
    }

    async fn subscribe(
        &self,
        symbol: &Symbol,
    ) -> Result<BoxStream<'_, MarketEvent>, VenueError> {
        // Merge depth + trade streams. Both arrive on independent WS
        // connections so per-stream reconnects don't bring down the
        // sibling — same shape as `tikr-binance::BinanceClient::subscribe`.
        let depth = depth_stream::subscribe_depth(self.env, symbol.clone()).await?;
        let trades = trade_stream::subscribe_trades(self.env, symbol.clone()).await?;
        let merged = futures::stream::select(depth, trades);
        Ok(Box::pin(merged))
    }

    async fn quote(&self, _intent: QuoteIntent) -> Result<QuoteId, VenueError> {
        Err(VenueError::Rejected {
            reason: "bybit: live order placement not wired in Phase 1 (paper mode only)".into(),
        })
    }

    async fn requote(&self, _id: QuoteId, _intent: QuoteIntent) -> Result<(), VenueError> {
        Err(VenueError::Rejected {
            reason: "bybit: live requote not wired in Phase 1".into(),
        })
    }

    async fn cancel(&self, _id: QuoteId) -> Result<(), VenueError> {
        Err(VenueError::Rejected {
            reason: "bybit: live cancel not wired in Phase 1".into(),
        })
    }

    async fn cancel_all(&self, _symbol: &Symbol) -> Result<(), VenueError> {
        Err(VenueError::Rejected {
            reason: "bybit: live cancel_all not wired in Phase 1".into(),
        })
    }

    async fn position(&self, symbol: &Symbol) -> Result<Position, VenueError> {
        // Paper mode: tracker constructs positions from FillSim fills;
        // the runner never calls Venue::position. Return a flat
        // placeholder so paper integration tests aren't blocked.
        Ok(Position {
            symbol: symbol.clone(),
            size: SignedSize(Decimal::ZERO),
            avg_entry: Price(Decimal::ZERO),
            realized_pnl: Notional(Decimal::ZERO),
        })
    }

    async fn fills_since(&self, _since_ts: u64) -> Result<Vec<Fill>, VenueError> {
        Ok(Vec::new())
    }

    async fn open_orders(&self, _symbol: &Symbol) -> Result<Vec<OpenOrder>, VenueError> {
        Ok(Vec::new())
    }
}

/// Pull a stream from `subscribe` into a `Vec` for ergonomic smoke
/// testing. Drops after `max_events` items so the example binary
/// doesn't run forever.
pub async fn collect_first(
    venue: &BybitClient,
    symbol: &Symbol,
    max_events: usize,
) -> Result<Vec<MarketEvent>, VenueError> {
    let mut stream = venue.subscribe(symbol).await?;
    let mut out = Vec::with_capacity(max_events);
    while let Some(ev) = stream.next().await {
        out.push(ev);
        if out.len() >= max_events {
            break;
        }
    }
    Ok(out)
}
