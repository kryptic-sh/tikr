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
    mainnet_writes_enabled: bool,
}

impl MexcClient {
    /// Construct against the standard mainnet endpoint.
    ///
    /// Write operations (place, cancel, cancel_all) are gated behind
    /// `TIKR_MEXC_ENABLE_MAINNET=1` — without it every write call returns
    /// [`VenueError::Rejected`] before any network I/O.
    pub fn new(api_key: String, api_secret: String) -> Self {
        let mainnet_writes_enabled =
            std::env::var("TIKR_MEXC_ENABLE_MAINNET").as_deref() == Ok("1");
        Self {
            http: HttpClient::new(),
            base_url: "https://api.mexc.com".to_string(),
            api_key: Arc::new(api_key),
            api_secret: Arc::new(api_secret),
            mainnet_writes_enabled,
        }
    }

    /// Internal constructor with explicit gate flag, for testing.
    #[allow(dead_code)]
    pub(crate) fn new_with_mainnet_gate(
        api_key: String,
        api_secret: String,
        mainnet_writes_enabled: bool,
    ) -> Self {
        Self {
            http: HttpClient::new(),
            base_url: "https://api.mexc.com".to_string(),
            api_key: Arc::new(api_key),
            api_secret: Arc::new(api_secret),
            mainnet_writes_enabled,
        }
    }

    /// Refuse write calls when mainnet writes are disabled.
    fn check_mainnet_gate(&self) -> Result<(), VenueError> {
        if !self.mainnet_writes_enabled {
            return Err(VenueError::Rejected {
                reason: "mainnet writes disabled — set TIKR_MEXC_ENABLE_MAINNET=1".into(),
            });
        }
        Ok(())
    }

    pub async fn place_limit_buy(
        &self,
        symbol: &str,
        price: &str,
        quantity: &str,
        client_order_id: &str,
    ) -> Result<QuoteId, VenueError> {
        self.check_mainnet_gate()?;
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
        self.check_mainnet_gate()?;
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
        self.check_mainnet_gate()?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mainnet_gate_refuses_when_disabled() {
        let client = MexcClient::new_with_mainnet_gate("k".into(), "s".into(), false);
        let result = client.check_mainnet_gate();
        assert!(matches!(result, Err(VenueError::Rejected { .. })));
        assert_eq!(
            result.unwrap_err().to_string(),
            "venue rejected: mainnet writes disabled — set TIKR_MEXC_ENABLE_MAINNET=1"
        );
    }

    #[test]
    fn mainnet_gate_allows_when_enabled() {
        let client = MexcClient::new_with_mainnet_gate("k".into(), "s".into(), true);
        assert!(client.check_mainnet_gate().is_ok());
    }
}
