//! MEXC Spot REST adapter.
//!
//! Minimum surface for the **bagboy** accumulator: place a single
//! LIMIT BUY at best_bid, cancel it when the book moves, refill when
//! filled. No WS yet — fills detected via `openOrders` polling.
//!
//! Authentication: HMAC-SHA256 over the query string. Pattern matches
//! Binance Spot exactly. Env vars expected:
//! - `MEXC_API_KEY`
//! - `MEXC_API_SECRET`
//!
//! Mainnet write gate: `TIKR_MEXC_ENABLE_MAINNET=1` (mirrors the
//! Binance convention so accidental real-money trades require an
//! explicit env flip).

pub mod sign;
pub mod spot;

use std::sync::Arc;

use reqwest::Client as HttpClient;
use tikr_core::QuoteId;
use tikr_venue::VenueError;

/// Holds the credentials + reqwest client shared across REST calls.
#[derive(Clone)]
pub struct MexcClient {
    pub http: HttpClient,
    pub base_url: String,
    pub api_key: Arc<String>,
    pub api_secret: Arc<String>,
}

impl MexcClient {
    /// Construct against the standard mainnet endpoint.
    pub fn new(api_key: String, api_secret: String) -> Self {
        Self {
            http: HttpClient::new(),
            base_url: "https://api.mexc.com".to_string(),
            api_key: Arc::new(api_key),
            api_secret: Arc::new(api_secret),
        }
    }

    pub async fn place_limit_buy(
        &self,
        symbol: &str,
        price: &str,
        quantity: &str,
        client_order_id: &str,
    ) -> Result<QuoteId, VenueError> {
        spot::place_limit_order(
            &self.http,
            &self.base_url,
            &self.api_key,
            &self.api_secret,
            symbol,
            tikr_core::Side::Bid,
            price,
            quantity,
            client_order_id,
        )
        .await
    }

    pub async fn cancel_order(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<(), VenueError> {
        spot::cancel_order(
            &self.http,
            &self.base_url,
            &self.api_key,
            &self.api_secret,
            symbol,
            client_order_id,
        )
        .await
    }

    pub async fn cancel_all(&self, symbol: &str) -> Result<(), VenueError> {
        spot::cancel_all_orders(
            &self.http,
            &self.base_url,
            &self.api_key,
            &self.api_secret,
            symbol,
        )
        .await
    }

    pub async fn book_ticker(&self, symbol: &str) -> Result<spot::SpotBookTicker, VenueError> {
        spot::get_book_ticker(&self.http, &self.base_url, symbol).await
    }

    pub async fn balance(&self, asset: &str) -> Result<spot::SpotBalance, VenueError> {
        spot::get_balance(
            &self.http,
            &self.base_url,
            &self.api_key,
            &self.api_secret,
            asset,
        )
        .await
    }

    pub async fn symbol_filters(&self, symbol: &str) -> Result<spot::SymbolFilters, VenueError> {
        spot::get_symbol_filters(&self.http, &self.base_url, symbol).await
    }

    pub async fn open_orders(&self, symbol: &str) -> Result<Vec<spot::OpenOrder>, VenueError> {
        spot::get_open_orders(
            &self.http,
            &self.base_url,
            &self.api_key,
            &self.api_secret,
            symbol,
        )
        .await
    }
}
