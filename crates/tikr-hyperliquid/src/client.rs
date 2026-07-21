//! HTTP client for the Hyperliquid `/info` endpoint.
//!
//! All read-side queries (snapshot, position, user fills) are POSTed as JSON
//! to a single endpoint with a `type` discriminator.

use crate::HyperliquidEnv;
use crate::mapping::*;
use crate::messages::*;
use reqwest::Client as HttpClient;
use serde_json::json;
use tikr_core::{Fill, Position, Snapshot, Symbol};
use tikr_venue::VenueError;

pub(crate) struct HyperliquidClient {
    http: HttpClient,
    info_url: String,
}

impl HyperliquidClient {
    pub(crate) fn new(env: HyperliquidEnv) -> Self {
        let info_url = match env {
            HyperliquidEnv::Mainnet => "https://api.hyperliquid.xyz/info".to_string(),
            HyperliquidEnv::Testnet => "https://api.hyperliquid-testnet.xyz/info".to_string(),
        };
        Self {
            http: HttpClient::new(),
            info_url,
        }
    }

    pub(crate) async fn snapshot(&self, symbol: &Symbol) -> Result<Snapshot, VenueError> {
        let body = json!({ "type": "l2Book", "coin": symbol.base.0.as_ref() });
        let resp = self
            .http
            .post(&self.info_url)
            .json(&body)
            .send()
            .await
            .map_err(network_err)?;
        let push: L2BookPush = resp.json().await.map_err(internal_err)?;
        Ok(l2_to_snapshot(symbol, &push))
    }

    pub(crate) async fn position(
        &self,
        symbol: &Symbol,
        user: &str,
    ) -> Result<Position, VenueError> {
        let body = json!({ "type": "clearinghouseState", "user": user });
        let resp = self
            .http
            .post(&self.info_url)
            .json(&body)
            .send()
            .await
            .map_err(network_err)?;
        let state: ClearinghouseStateResp = resp.json().await.map_err(internal_err)?;
        Ok(position_from_clearinghouse(symbol, &state))
    }

    pub(crate) async fn user_fills_since(
        &self,
        user: &str,
        coin: &str,
        since_ts: u64,
    ) -> Result<Vec<Fill>, VenueError> {
        let body = json!({ "type": "userFills", "user": user });
        let resp = self
            .http
            .post(&self.info_url)
            .json(&body)
            .send()
            .await
            .map_err(network_err)?;
        let entries: Vec<UserFillEntry> = resp.json().await.map_err(internal_err)?;
        Ok(entries
            .iter()
            .filter(|f| {
                f.coin.eq_ignore_ascii_case(coin) && f.time.saturating_mul(1_000_000) >= since_ts
            })
            .map(fill_from_user_fill)
            .collect())
    }
}

fn network_err(e: reqwest::Error) -> VenueError {
    VenueError::Network(std::io::Error::other(e.to_string()))
}

fn internal_err(e: reqwest::Error) -> VenueError {
    VenueError::Internal(Box::new(e))
}
