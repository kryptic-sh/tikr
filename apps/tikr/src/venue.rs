//! Per-bot venue construction. Mirrors the relevant chunks of
//! `run_perp::main` so each bot gets its own `BinanceClient` + user-stream
//! subscription. Higher API load than a single shared client but matches
//! the proven run_perp shape, which already filters fills by symbol on
//! each subscriber.

use std::sync::Arc;

use anyhow::Result;
use reqwest::Client as HttpClient;
use tikr_binance::user_stream::subscribe_user_data_stream_cancellable;
use tikr_binance::{
    BinanceClient, BinanceEnv, BinanceKeyMaterial, env_with_product_fallback,
    load_credentials_from_file, load_key_material_from_env, product_var,
};
use tikr_core::{Asset, Fill, MarketKind, Symbol, VenueId};
use tokio::sync::{mpsc, watch};

/// Parse an env string like `"futures-testnet"` / `"futures-mainnet"`.
pub fn parse_env(s: &str) -> Result<BinanceEnv> {
    match s {
        "futures-testnet" => Ok(BinanceEnv::FuturesTestnet),
        "futures-mainnet" => Ok(BinanceEnv::FuturesMainnet),
        other => Err(anyhow::anyhow!(
            "unsupported env '{other}' (try futures-testnet / futures-mainnet)"
        )),
    }
}

/// `BTCUSDT` → `("BTC", "USDT")`. Mirrors `run_perp::split_symbol`.
pub fn split_symbol(sym: &str) -> (&str, &str) {
    for suffix in &["USDT", "BUSD", "USDC", "TUSD"] {
        if let Some(base) = sym.strip_suffix(suffix)
            && !base.is_empty()
        {
            return (base, suffix);
        }
    }
    for suffix in &["BTC", "ETH", "BNB"] {
        if let Some(base) = sym.strip_suffix(suffix)
            && !base.is_empty()
        {
            return (base, suffix);
        }
    }
    (sym, "")
}

/// Build a `tikr_core::Symbol` for a Binance perp.
pub fn perp_symbol(s: &str) -> Symbol {
    let (base, quote) = split_symbol(s);
    Symbol {
        base: Asset::new(base),
        quote: Asset::new(quote),
        venue: VenueId::new("binance"),
        kind: MarketKind::Perp,
    }
}

/// Load API key + key material from env vars / optional key file.
///
/// Returns `(api_key, Arc<key_material>)` so the user-stream subscriber
/// and the venue can share the same material.
pub fn load_credentials(
    env: BinanceEnv,
    key_file: Option<&std::path::Path>,
) -> Result<(String, Arc<BinanceKeyMaterial>)> {
    let api_key = env_with_product_fallback(env, "API_KEY")
        .ok_or_else(|| anyhow::anyhow!("{} (or fallback) not set", product_var(env, "API_KEY")))?;

    let key_material: BinanceKeyMaterial = if let Some(kf) = key_file
        && env_with_product_fallback(env, "KEY_TYPE")
            .unwrap_or_default()
            .to_lowercase()
            != "ed25519"
    {
        let (_k, secret) =
            load_credentials_from_file(kf).map_err(|e| anyhow::anyhow!("key-file: {e}"))?;
        BinanceKeyMaterial::Hmac { secret }
    } else {
        load_key_material_from_env(env, None).map_err(|e| anyhow::anyhow!("credential: {e}"))?
    };

    Ok((api_key, Arc::new(key_material)))
}

/// Build a [`BinanceClient`] for `symbol` with the given env + key material.
/// `leverage` is sent via `POST /fapi/v1/leverage` on futures envs.
pub async fn build_venue(
    env: BinanceEnv,
    api_key: &str,
    key_material: &Arc<BinanceKeyMaterial>,
    symbol: &Symbol,
    leverage: u32,
) -> Result<BinanceClient> {
    let owned = match key_material.as_ref() {
        BinanceKeyMaterial::Hmac { secret } => BinanceKeyMaterial::Hmac {
            secret: secret.clone(),
        },
        BinanceKeyMaterial::Ed25519 { signing_key } => BinanceKeyMaterial::Ed25519 {
            signing_key: signing_key.clone(),
        },
    };
    BinanceClient::with_credentials(env, api_key.to_string(), owned, Some(symbol), leverage)
        .await
        .map_err(|e| anyhow::anyhow!("BinanceClient::with_credentials: {e}"))
}

/// Subscribe to userDataStream for `symbol`, returning the fill receiver.
///
/// `shutdown_rx` is propagated to the internal keepalive + WS pump tasks
/// so they exit cleanly when the bot is being restarted — without this
/// they would leak with every supervisor respawn (each holding an
/// `Arc<HttpClient>` + a listenKey mutex slot).
pub async fn subscribe_fills(
    env: BinanceEnv,
    api_key: &str,
    key_material: Arc<BinanceKeyMaterial>,
    symbol: &Symbol,
    shutdown_rx: watch::Receiver<bool>,
    bnb_price_rx: Option<watch::Receiver<tikr_core::Decimal>>,
) -> Result<mpsc::UnboundedReceiver<Fill>> {
    let http = HttpClient::new();
    let sym_filter =
        format!("{}{}", symbol.base.0.as_ref(), symbol.quote.0.as_ref()).to_uppercase();
    subscribe_user_data_stream_cancellable(
        http,
        env,
        api_key.to_string(),
        key_material,
        MarketKind::Perp,
        sym_filter,
        Some(shutdown_rx),
        bnb_price_rx,
    )
    .await
    .map_err(|e| anyhow::anyhow!("subscribe_user_data_stream: {e}"))
}
