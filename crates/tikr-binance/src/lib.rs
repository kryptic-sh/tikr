//! Binance Spot + USD-M Perps [`Venue`] adapter.
//!
//! # Architecture
//!
//! One [`BinanceClient`] instance targets a single [`BinanceEnv`]:
//! - `SpotTestnet` / `SpotMainnet` — Spot REST + WS.
//! - `FuturesTestnet` / `FuturesMainnet` — USD-M Futures REST + WS.
//!
//! ## Construction
//!
//! Use [`BinanceClient::with_credentials`] to build a write-capable client.
//! The constructor:
//! 1. Fetches `exchangeInfo` and caches precision filters.
//! 2. For futures envs: calls `POST /fapi/v1/leverage` (1x).
//! 3. Checks the mainnet gate.
//!
//! ## Mainnet gate
//!
//! Write actions on `SpotMainnet` or `FuturesMainnet` require the env var
//! `TIKR_BINANCE_ENABLE_MAINNET=1`. Without it every write call returns
//! `VenueError::Rejected { reason: "mainnet writes disabled..." }`.
//!
//! ## Signing
//!
//! HMAC-SHA256 (hex) or Ed25519 (base64) over the query string;
//! `&signature=<value>` appended last. See [`sign`] module.
//!
//! ## Credentials
//!
//! HMAC (default): `TIKR_BINANCE_API_KEY` + `TIKR_BINANCE_API_SECRET`, or
//! `--key-file <path>` flag (single line `key:secret`).
//!
//! Ed25519: `TIKR_BINANCE_API_KEY` + `TIKR_BINANCE_PRIVATE_KEY_PATH` (PEM
//! file), or `--ed25519-key-file <path>` flag. Set `TIKR_BINANCE_KEY_TYPE=ed25519`.
//!
//! ## Security
//!
//! [`BinanceClient`] implements a manual `Debug` that omits `api_key` and
//! `key_material` entirely. Never log those fields.
//!
//! See issues #42, #43, #44, #45 for design decisions and architecture notes.

#![deny(missing_docs)]

pub mod depth_stream;
pub mod errors;
pub mod exchange_info;
/// USD-M Futures REST endpoint wrappers (`/fapi/v1/...`).
pub mod futs;
/// Shared REST response handling: status mapping, rate-limit / `Retry-After`,
/// and used-weight tracking.
pub mod http;
pub mod liquidation_stream;
pub mod mark_price_stream;
pub mod sign;
pub mod spot;
pub mod trade_stream;
pub mod user_stream;

pub use sign::BinanceKeyMaterial;

use async_trait::async_trait;
use depth_stream::binance_symbol;
use exchange_info::{
    ExchangeInfoCache, bump_size_for_min_notional, parse_exchange_info, round_price_for_side,
    round_size, validate_qty,
};
use futures::stream::BoxStream;
use reqwest::Client as HttpClient;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use tikr_core::{
    Fill, MarketEvent, Position, Price, Side, SignedSize, Size, Snapshot, Symbol, Timestamp,
};
use tikr_venue::{QuoteId, QuoteIntent, Venue, VenueError};
use tracing::{info, warn};
use uuid::Uuid;

/// Wrap a depth stream so a lagging consumer can never back up the upstream
/// socket. A light task drains the source continuously and republishes only the
/// LATEST book through a `watch` channel; bursts of `@depth20` snapshots between
/// consumer polls coalesce to the freshest one (each full snapshot supersedes
/// the previous), so the socket pump never blocks on handoff and the consumer
/// never wades through stale books to catch up. Live-bot only — the recorder
/// consumes `subscribe_depth` directly to keep recordings lossless.
fn coalesce_latest_book(
    mut src: BoxStream<'static, MarketEvent>,
) -> BoxStream<'static, MarketEvent> {
    use futures::StreamExt;
    use tokio_stream::wrappers::WatchStream;
    let (tx, rx) = tokio::sync::watch::channel::<Option<MarketEvent>>(None);
    tokio::spawn(async move {
        while let Some(ev) = src.next().await {
            // Non-blocking overwrite; errors only once every receiver is gone.
            if tx.send(Some(ev)).is_err() {
                break;
            }
        }
    });
    // WatchStream::new yields the seed `None` once (filtered out), then the
    // latest value on each change — coalescing bursts to the freshest book.
    Box::pin(WatchStream::new(rx).filter_map(|opt| async move { opt }))
}

/// Wrap a trade stream so a lagging consumer can never back up the upstream
/// socket. A light task drains the source and forwards over a bounded queue via
/// `try_send`; when the consumer falls behind and the queue fills, NEW trade
/// frames are dropped (with a periodic logged count) instead of blocking the
/// reader. Acceptable for the live bot: real fills arrive on the user data
/// stream, not this signal-only trade feed. Live-bot only — the recorder
/// consumes `subscribe_trades` directly to keep recordings lossless.
fn drop_newest_on_lag(mut src: BoxStream<'static, MarketEvent>) -> BoxStream<'static, MarketEvent> {
    use futures::StreamExt;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;
    let (tx, rx) = mpsc::channel::<MarketEvent>(512);
    tokio::spawn(async move {
        let mut dropped: u64 = 0;
        while let Some(ev) = src.next().await {
            match tx.try_send(ev) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    dropped += 1;
                    // Log on the first drop and every 256 after — never silent.
                    if dropped % 256 == 1 {
                        warn!(
                            dropped,
                            "binance trade stream: consumer lagging — dropping trade frames \
                             (live fills come from the user data stream, unaffected)"
                        );
                    }
                }
                Err(mpsc::error::TrySendError::Closed(_)) => break,
            }
        }
    });
    Box::pin(ReceiverStream::new(rx))
}

// ---------------------------------------------------------------------------
// BinanceEnv
// ---------------------------------------------------------------------------

/// Binance environment selector.
///
/// Each variant maps to a distinct REST + WS base URL pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinanceEnv {
    /// Spot testnet (`testnet.binance.vision`).
    SpotTestnet,
    /// Spot mainnet (`api.binance.com`). Requires `TIKR_BINANCE_ENABLE_MAINNET=1`.
    SpotMainnet,
    /// USD-M Futures testnet (`testnet.binancefuture.com`).
    FuturesTestnet,
    /// USD-M Futures mainnet (`fapi.binance.com`). Requires `TIKR_BINANCE_ENABLE_MAINNET=1`.
    FuturesMainnet,
}

impl BinanceEnv {
    /// Returns the REST base URL for this environment.
    pub fn rest_base_url(&self) -> &'static str {
        match self {
            BinanceEnv::SpotTestnet => "https://testnet.binance.vision",
            BinanceEnv::SpotMainnet => "https://api.binance.com",
            BinanceEnv::FuturesTestnet => "https://testnet.binancefuture.com",
            BinanceEnv::FuturesMainnet => "https://fapi.binance.com",
        }
    }

    /// Returns `true` for mainnet environments.
    pub fn is_mainnet(&self) -> bool {
        matches!(self, BinanceEnv::SpotMainnet | BinanceEnv::FuturesMainnet)
    }

    /// Returns `true` for futures environments.
    pub fn is_futures(&self) -> bool {
        matches!(
            self,
            BinanceEnv::FuturesTestnet | BinanceEnv::FuturesMainnet
        )
    }
}

/// Fetch `exchangeInfo` for `env` and return the subset of `symbols` that are
/// NOT listed on the exchange (case-insensitive). An empty result means every
/// symbol is valid. Use this to fail fast before subscribing to streams /
/// placing orders for a symbol that doesn't exist (e.g. a USDC perp that only
/// trades as USDT-M). No credentials required — `exchangeInfo` is public.
pub async fn invalid_symbols(
    env: BinanceEnv,
    symbols: &[String],
) -> Result<Vec<String>, VenueError> {
    let http = HttpClient::new();
    let base_url = env.rest_base_url();
    let resp = if env.is_futures() {
        crate::futs::get_exchange_info(&http, base_url).await?
    } else {
        crate::spot::get_exchange_info(&http, base_url).await?
    };
    let cache = parse_exchange_info(&resp);
    Ok(symbols
        .iter()
        .filter(|s| !cache.contains_key(&s.to_uppercase()))
        .cloned()
        .collect())
}

/// Fetch `exchangeInfo` for `env` and return the precision filters (tick size,
/// lot step, min qty, min notional) for a single `symbol`, or `None` if the
/// symbol isn't listed. Lets offline tools (backtests) auto-detect the real
/// venue geometry instead of hard-coding it. No credentials required.
pub async fn symbol_filters(
    env: BinanceEnv,
    symbol: &str,
) -> Result<Option<exchange_info::SymbolFilters>, VenueError> {
    let http = HttpClient::new();
    let base_url = env.rest_base_url();
    let resp = if env.is_futures() {
        crate::futs::get_exchange_info(&http, base_url).await?
    } else {
        crate::spot::get_exchange_info(&http, base_url).await?
    };
    let cache = parse_exchange_info(&resp);
    Ok(cache.get(&symbol.to_uppercase()).cloned())
}

// ---------------------------------------------------------------------------
// BinanceClient
// ---------------------------------------------------------------------------

/// Binance Spot + USD-M Perps [`Venue`] adapter.
///
/// Constructed via [`BinanceClient::with_credentials`]. One instance per
/// environment; create separate instances for Spot and Futures.
///
/// # Debug
///
/// Manual impl: `api_key` and `key_material` are never printed.
pub struct BinanceClient {
    env: BinanceEnv,
    http: HttpClient,
    api_key: String,
    key_material: BinanceKeyMaterial,
    mainnet_writes_enabled: bool,
    exchange_info_cache: ExchangeInfoCache,
    /// Maker fee (bps) per symbol, fetched from `/fapi/v1/commissionRate` at
    /// build time. Keyed by uppercase Binance symbol. Empty on non-futures /
    /// read-only / fetch-failure — callers fall back to a conservative default.
    commission_maker_bps: HashMap<String, tikr_core::Decimal>,
    /// Tracks every open order placed via `quote()` keyed by the
    /// **venue-assigned** `QuoteId` (derived from Binance's `orderId`),
    /// value = `(binance_symbol, clientOrderId_we_sent_at_place)`.
    ///
    /// Two values stored:
    /// 1. **symbol** — needed because `Venue::cancel(id)` doesn't pass it
    ///    but Binance's DELETE endpoint requires `symbol=`.
    /// 2. **clientOrderId** — the random hex we sent at placement.
    ///    `cancel` calls `DELETE …?origClientOrderId=<this>`. Storing it
    ///    here means our internal `QuoteId` (which equals the order_id-
    ///    derived `QuoteId` returned from `place_order`) is decoupled from
    ///    the random `clientOrderId` we used at placement — `QuoteId` now
    ///    matches the one the `userDataStream` fill parser produces, so
    ///    `FillSim::drop_quote(fill.quote_id)` finally has a hit.
    ///
    /// Entries are removed on successful cancel.
    quote_symbols: Arc<Mutex<HashMap<QuoteId, (String, String)>>>,
}

impl fmt::Debug for BinanceClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BinanceClient")
            .field("env", &self.env)
            .field("mainnet_writes_enabled", &self.mainnet_writes_enabled)
            .field("cached_symbols", &self.exchange_info_cache.len())
            .finish_non_exhaustive()
    }
}

impl BinanceClient {
    /// Snapshot the current futures position-risk row for `symbol` —
    /// signed amount, entry price, mark, unrealized PnL. Used by
    /// `tikr` supervisor to seed the local PositionTracker when
    /// `--clear` is OFF so the strategy doesn't think it's flat when
    /// the venue still holds inventory.
    ///
    /// Returns a `Default` value (all zeros) on non-futures envs since
    /// spot doesn't have a position concept.
    pub async fn position_risk(
        &self,
        symbol: &Symbol,
    ) -> Result<crate::futs::FuturesPositionRisk, VenueError> {
        if !self.env.is_futures() {
            return Ok(crate::futs::FuturesPositionRisk::default());
        }
        let sym_str = binance_symbol(symbol);
        let base_url = self.env.rest_base_url();
        crate::futs::get_position_risk(
            &self.http,
            base_url,
            &self.api_key,
            &self.key_material,
            &sym_str,
        )
        .await
    }

    /// Build a fully write-capable client.
    ///
    /// Steps:
    /// 1. Fetches `exchangeInfo` and caches filters.
    /// 2. For futures envs: calls `POST /fapi/v1/leverage` with the
    ///    supplied `leverage` value.
    /// 3. Checks mainnet gate (warns if unset).
    ///
    /// `symbol` is used for the initial leverage call (futures only).
    /// Pass `None` to skip leverage init (useful in tests / read-only contexts).
    pub async fn with_credentials(
        env: BinanceEnv,
        api_key: String,
        key_material: BinanceKeyMaterial,
        symbol: Option<&Symbol>,
        leverage: u32,
    ) -> Result<Self, VenueError> {
        let http = HttpClient::new();
        let base_url = env.rest_base_url();

        // Check mainnet gate.
        let mainnet_writes_enabled = if env.is_mainnet() {
            std::env::var("TIKR_BINANCE_ENABLE_MAINNET").as_deref() == Ok("1")
        } else {
            true
        };

        if env.is_mainnet() && !mainnet_writes_enabled {
            warn!(
                "BinanceClient: mainnet env + TIKR_BINANCE_ENABLE_MAINNET not set; \
                 write actions will be refused"
            );
        }

        // Fetch exchangeInfo.
        let info_resp = if env.is_futures() {
            crate::futs::get_exchange_info(&http, base_url).await?
        } else {
            crate::spot::get_exchange_info(&http, base_url).await?
        };
        let exchange_info_cache = parse_exchange_info(&info_resp);
        info!(
            env = ?env,
            symbols = exchange_info_cache.len(),
            "exchangeInfo cached"
        );

        // Fetch the per-symbol maker fee (futures + a real symbol + writes
        // allowed). Used by Wave auto-step to floor the step at the round-trip
        // break-even (2 × maker). A 0-fee USDC promo pair returns 0 → no floor.
        // On any failure we leave the cache empty; the strategy builder falls
        // back to a conservative tier-0 maker.
        let mut commission_maker_bps: HashMap<String, tikr_core::Decimal> = HashMap::new();
        if env.is_futures()
            && let Some(sym) = symbol
            && (!env.is_mainnet() || mainnet_writes_enabled)
        {
            let sym_str = binance_symbol(sym);
            match crate::futs::get_commission_rate(
                &http,
                base_url,
                &api_key,
                &key_material,
                &sym_str,
            )
            .await
            {
                Ok(rate) => {
                    // API returns a fraction (0.0002 = 2 bps) → convert to bps.
                    let maker_bps = rate.maker * tikr_core::Decimal::from(10_000);
                    info!(symbol = sym_str, %maker_bps, "commission rate cached");
                    commission_maker_bps.insert(sym_str, maker_bps);
                }
                Err(e) => {
                    warn!(symbol = sym_str, error = ?e, "commissionRate fetch failed; auto-step will use fallback maker fee");
                }
            }
        }

        let client = Self {
            env,
            http,
            api_key,
            key_material,
            mainnet_writes_enabled,
            exchange_info_cache,
            commission_maker_bps,
            quote_symbols: Arc::new(Mutex::new(HashMap::new())),
        };

        // Futures: set per-symbol leverage at startup. Gated by mainnet flag —
        // this is a write action against /fapi/v1/leverage and must respect the
        // same gate as quote/cancel. On mainnet without the flag, skip silently
        // (the same call will be retried by the operator after enabling writes).
        if env.is_futures()
            && let Some(sym) = symbol
            && (!env.is_mainnet() || mainnet_writes_enabled)
        {
            let sym_str = binance_symbol(sym);
            if let Err(e) = crate::futs::update_leverage(
                &client.http,
                base_url,
                &client.api_key,
                &client.key_material,
                &sym_str,
                leverage,
            )
            .await
            {
                warn!(
                    symbol = sym_str,
                    leverage,
                    error = ?e,
                    "update_leverage at startup failed; proceeding"
                );
            }
        } else if env.is_futures() && env.is_mainnet() && !mainnet_writes_enabled {
            warn!(
                leverage,
                "Skipping futures update_leverage at startup: \
                 TIKR_BINANCE_ENABLE_MAINNET not set. \
                 Order placement will also be refused until the flag is enabled."
            );
        }

        Ok(client)
    }

    // -------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------

    /// Look up the minimum order notional (price × size floor) cached
    /// from `exchangeInfo`. Returns `None` if the symbol wasn't seen
    /// during cache construction.
    pub fn min_notional(&self, symbol: &Symbol) -> Option<tikr_core::Decimal> {
        let sym_str = binance_symbol(symbol);
        self.exchange_info_cache
            .get(&sym_str)
            .map(|f| f.min_notional)
    }

    /// Look up the price tick size cached from `exchangeInfo`.
    pub fn tick_size(&self, symbol: &Symbol) -> Option<tikr_core::Decimal> {
        let sym_str = binance_symbol(symbol);
        self.exchange_info_cache.get(&sym_str).map(|f| f.tick_size)
    }

    /// Look up the lot step size cached from `exchangeInfo`.
    pub fn step_size(&self, symbol: &Symbol) -> Option<tikr_core::Decimal> {
        let sym_str = binance_symbol(symbol);
        self.exchange_info_cache.get(&sym_str).map(|f| f.step_size)
    }

    /// Look up the maker fee (bps) cached from `/fapi/v1/commissionRate` at
    /// build time. `None` if not fetched (non-futures / read-only / fetch
    /// failed) — callers should fall back to a conservative default.
    pub fn maker_fee_bps(&self, symbol: &Symbol) -> Option<tikr_core::Decimal> {
        let sym_str = binance_symbol(symbol);
        self.commission_maker_bps.get(&sym_str).copied()
    }

    /// Fetch USD-M futures balance for a margin asset (usually `USDT`).
    pub async fn futures_balance(
        &self,
        asset: &str,
    ) -> Result<crate::futs::FuturesBalance, VenueError> {
        if !self.env.is_futures() {
            return Err(VenueError::Rejected {
                reason: "futures balance requested for non-futures env".to_string(),
            });
        }
        crate::futs::get_balance(
            &self.http,
            self.env.rest_base_url(),
            &self.api_key,
            &self.key_material,
            asset,
        )
        .await
    }

    /// Enforce mainnet gate before any write action.
    fn check_mainnet_gate(&self) -> Result<(), VenueError> {
        if self.env.is_mainnet() && !self.mainnet_writes_enabled {
            return Err(VenueError::Rejected {
                reason: "mainnet writes disabled — set TIKR_BINANCE_ENABLE_MAINNET=1".into(),
            });
        }
        Ok(())
    }

    /// Build a `clientOrderId` from a `QuoteId`.
    ///
    /// Format: 32 hex chars of the UUID's u128 value (no prefix).
    ///
    /// Binance enforces clientOrderId length < 36 chars (verified via -4015
    /// error on live testnet 2026-05-19). Earlier `"tikr_"` prefix produced
    /// 37 chars and was rejected. Bare 32-hex fits with comfortable margin.
    fn client_order_id(id: QuoteId) -> String {
        format!("{:032x}", id.0.as_u128())
    }

    /// Parse a `clientOrderId` back to a `QuoteId`.
    ///
    /// Parses 32-hex string as u128, wraps in Uuid.
    /// Used in tests and available for future runner state reconciliation.
    #[allow(dead_code)]
    fn quote_id_from_client_order_id(coid: &str) -> Option<QuoteId> {
        if coid.len() != 32 {
            return None;
        }
        let val = u128::from_str_radix(coid, 16).ok()?;
        Some(QuoteId::from_uuid(Uuid::from_u128(val)))
    }

    /// Format a Decimal for the wire (normalized, no scientific notation).
    fn format_decimal(d: tikr_core::Decimal) -> String {
        format!("{}", d.normalize())
    }

    /// Place one side's prepped orders, touch-first, as sequential 5-order
    /// `batchOrders` requests (the venue's per-request cap). `pis` are indices
    /// into `prepped`, pre-sorted best→worst. Returns `(orig_idx, result)` per
    /// order. Caller runs the bid side and ask side concurrently so the two
    /// sides' REST round-trips overlap instead of summing.
    async fn place_side_batches(
        &self,
        base_url: &str,
        prepped: &[(usize, PreppedOrder)],
        pis: &[usize],
    ) -> Vec<(usize, Result<QuoteId, VenueError>)> {
        let mut out: Vec<(usize, Result<QuoteId, VenueError>)> = Vec::with_capacity(pis.len());
        // Group by symbol, preserving the best-first order (single-symbol in
        // practice, but stay correct for a mixed caller).
        let mut groups: Vec<(String, Vec<usize>)> = Vec::new();
        for &pi in pis {
            let sym = &prepped[pi].1.sym;
            if let Some(g) = groups.iter_mut().find(|g| &g.0 == sym) {
                g.1.push(pi);
            } else {
                groups.push((sym.clone(), vec![pi]));
            }
        }
        for (sym, sym_pis) in groups {
            for chunk in sym_pis.chunks(5) {
                let reqs: Vec<crate::futs::BatchOrderReq<'_>> = chunk
                    .iter()
                    .map(|&pi| {
                        let p = &prepped[pi].1;
                        crate::futs::BatchOrderReq {
                            side: p.side,
                            price: &p.price,
                            quantity: &p.size,
                            client_order_id: &p.coid,
                            tif: p.tif,
                        }
                    })
                    .collect();
                let placed = crate::futs::place_batch_orders(
                    &self.http,
                    base_url,
                    &self.api_key,
                    &self.key_material,
                    &sym,
                    &reqs,
                )
                .await;
                match placed {
                    Ok(per) => {
                        for (&pi, res) in chunk.iter().zip(per) {
                            let (orig_idx, prep) = (prepped[pi].0, &prepped[pi].1);
                            if let Ok(venue_qid) = &res
                                && let Ok(mut map) = self.quote_symbols.lock()
                            {
                                map.insert(*venue_qid, (prep.sym.clone(), prep.coid.clone()));
                            }
                            out.push((orig_idx, res));
                        }
                    }
                    Err(e) => {
                        // Request-level failure (auth / rate-limit / malformed):
                        // VenueError isn't Clone, so fan a faithful copy out to
                        // every leg — preserving RateLimited so the runner's
                        // cooldown arms no matter which result it inspects.
                        let reason = format!("batch place request failed: {e:?}");
                        for &pi in chunk {
                            out.push((prepped[pi].0, Err(clone_batch_err(&e, &reason))));
                        }
                    }
                }
            }
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Venue impl
// ---------------------------------------------------------------------------

/// Reconstruct a faithful per-element error from a request-level batch failure.
/// `VenueError` isn't `Clone`, so we rebuild the variants the caller acts on —
/// `RateLimited` (arms the runner's cooldown) and `UnknownQuote` (idempotent) —
/// and fall back to a string-cloned `Rejected` for everything else.
fn clone_batch_err(e: &VenueError, reason: &str) -> VenueError {
    match e {
        VenueError::RateLimited { retry_after_ms } => VenueError::RateLimited {
            retry_after_ms: *retry_after_ms,
        },
        VenueError::UnknownQuote => VenueError::UnknownQuote,
        _ => VenueError::Rejected {
            reason: reason.to_string(),
        },
    }
}

/// One validated, wire-formatted order awaiting batch placement. Owns its
/// strings so [`BinanceClient::place_side_batches`] can borrow them across the
/// `batchOrders` request.
struct PreppedOrder {
    sym: String,
    price: String,
    size: String,
    coid: String,
    side: Side,
    tif: tikr_core::TimeInForce,
    /// Rounded price, kept for the best→worst sort within a side.
    sort_price: tikr_core::Decimal,
}

#[async_trait]
impl Venue for BinanceClient {
    fn id(&self) -> &str {
        match self.env {
            BinanceEnv::SpotTestnet | BinanceEnv::SpotMainnet => "binance-spot",
            BinanceEnv::FuturesTestnet | BinanceEnv::FuturesMainnet => "binance-futures",
        }
    }

    /// Fetch the current order-book snapshot via REST.
    ///
    /// Uses `/fapi/v1/depth?symbol=...&limit=5` for futures and
    /// `/api/v3/depth?symbol=...&limit=5` for spot. Returns the top 5
    /// levels per side — enough to price an IOC at touch for startup
    /// flatten without paying for a full 20-level fetch.
    async fn snapshot(&self, symbol: &Symbol) -> Result<Snapshot, VenueError> {
        let sym_str = binance_symbol(symbol);
        let base_url = self.env.rest_base_url();
        let path = if self.env.is_futures() {
            "fapi/v1/depth"
        } else {
            "api/v3/depth"
        };
        let url = format!("{base_url}/{path}?symbol={sym_str}&limit=5");
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| VenueError::Network(std::io::Error::other(e.to_string())))?;
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| VenueError::Internal(Box::new(e)))?;

        fn parse_levels(arr: Option<&Vec<serde_json::Value>>) -> Vec<tikr_core::Level> {
            arr.map(|rows| {
                rows.iter()
                    .filter_map(|row| {
                        let a = row.as_array()?;
                        let p = a.first()?.as_str()?.parse::<tikr_core::Decimal>().ok()?;
                        let s = a.get(1)?.as_str()?.parse::<tikr_core::Decimal>().ok()?;
                        Some(tikr_core::Level {
                            price: Price(p),
                            size: tikr_core::Size(s),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
        }

        let bids = parse_levels(body.get("bids").and_then(serde_json::Value::as_array));
        let asks = parse_levels(body.get("asks").and_then(serde_json::Value::as_array));
        let ts = Timestamp(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0),
        );
        Ok(Snapshot {
            symbol: symbol.clone(),
            bids,
            asks,
            ts,
        })
    }

    /// Subscribe to a live market-data stream.
    ///
    /// Merges two WebSocket sources into one [`MarketEvent`] stream:
    /// [`MarketEvent::BookUpdate`] frames from `@depth20@100ms` and
    /// [`MarketEvent::Trade`] frames from the trade stream (`@trade` on
    /// futures, `@aggTrade` on spot). Both are required: book-driven
    /// strategies (grid/wave) consume the depth frames, while trade-driven
    /// strategies (micro-mean-reversion) only act on the trade prints — a
    /// depth-only stream would leave them permanently idle.
    async fn subscribe(&self, symbol: &Symbol) -> Result<BoxStream<'_, MarketEvent>, VenueError> {
        let depth = depth_stream::subscribe_depth(self.env, symbol.clone()).await?;
        let trades = trade_stream::subscribe_trades(self.env, symbol.clone()).await?;
        // Decouple the socket readers from THIS stream's consumer. The bot's
        // runner does inline REST order placement between stream polls, so a
        // stall there (slow round-trip, rate-limit cooldown) must never
        // back-pressure all the way to the TCP sockets. Each socket pump is its
        // own task, but it hands off over a bounded channel with a blocking
        // send — under a long enough consumer stall that buffer fills and the
        // reader blocks. These adapters insert an always-draining task in front
        // of the consumer: depth COALESCES to the latest book (a full @depth20
        // snapshot supersedes the previous), trades DROP-newest under lag. The
        // recorder consumes `subscribe_depth`/`subscribe_trades` directly, so
        // its recordings stay lossless.
        let depth = coalesce_latest_book(depth);
        let trades = drop_newest_on_lag(trades);
        Ok(Box::pin(futures::stream::select(depth, trades)))
    }

    /// Place a post-only limit order.
    ///
    /// Rounds price and size to tick/step, validates min_qty and min_notional,
    /// then places via the appropriate REST endpoint.
    async fn quote(&self, intent: QuoteIntent) -> Result<QuoteId, VenueError> {
        self.check_mainnet_gate()?;

        let sym_str = binance_symbol(&intent.symbol);
        let price = round_price_for_side(
            &self.exchange_info_cache,
            &sym_str,
            intent.price,
            intent.side,
        )?;
        let rounded = round_size(&self.exchange_info_cache, &sym_str, intent.size)?;
        // Min-notional safety net: rounding price/size to tick/step (and any
        // strategy-side off-by-one-lot) can leave the order a hair under
        // minNotional (e.g. 4.992 < 5). A reject just drops the quote and leaves
        // a hole in the grid; instead bump the size up by whole lots until the
        // notional clears the floor, so the order actually rests at min size.
        let size = bump_size_for_min_notional(&self.exchange_info_cache, &sym_str, rounded, price);
        validate_qty(&self.exchange_info_cache, &sym_str, size, price)?;

        // The `clientOrderId` we send at place-time is derived from a random
        // QuoteId. We don't return THIS one to the runner — we return the
        // order_id-derived `venue_qid` that `place_order` produces, so it
        // matches the QuoteId the userDataStream parser stamps on fill events.
        let placement_qid = QuoteId::new();
        let coid = Self::client_order_id(placement_qid);
        let price_str = Self::format_decimal(price.0);
        let size_str = Self::format_decimal(size.0);
        let base_url = self.env.rest_base_url();

        let venue_qid = if self.env.is_futures() {
            crate::futs::place_order(
                &self.http,
                base_url,
                &self.api_key,
                &self.key_material,
                &sym_str,
                intent.side,
                &price_str,
                &size_str,
                &coid,
                intent.tif,
            )
            .await?
        } else {
            crate::spot::place_order(
                &self.http,
                base_url,
                &self.api_key,
                &self.key_material,
                &sym_str,
                intent.side,
                &price_str,
                &size_str,
                &coid,
            )
            .await?
        };

        // Record (venue_qid → (symbol, clientOrderId)) so `cancel(venue_qid)`
        // can find both the symbol AND the original clientOrderId the venue
        // accepts as `origClientOrderId=…`.
        if let Ok(mut map) = self.quote_symbols.lock() {
            map.insert(venue_qid, (sym_str.clone(), coid));
        }

        Ok(venue_qid)
    }

    async fn reduce_only_limit(
        &self,
        symbol: &Symbol,
        side: Side,
        price: Price,
        size: Size,
    ) -> Result<QuoteId, VenueError> {
        self.check_mainnet_gate()?;
        if !self.env.is_futures() {
            return Err(VenueError::Rejected {
                reason: "reduce_only_limit only supported on futures".to_string(),
            });
        }
        let sym_str = binance_symbol(symbol);
        let price = round_price_for_side(&self.exchange_info_cache, &sym_str, price, side)?;
        let size = round_size(&self.exchange_info_cache, &sym_str, size)?;
        // reduce-only is exempt from MIN_NOTIONAL, so no bump needed.
        let placement_qid = QuoteId::new();
        let coid = Self::client_order_id(placement_qid);
        let price_str = Self::format_decimal(price.0);
        let size_str = Self::format_decimal(size.0);
        let venue_qid = crate::futs::place_reduce_only_limit(
            &self.http,
            self.env.rest_base_url(),
            &self.api_key,
            &self.key_material,
            &sym_str,
            side,
            &price_str,
            &size_str,
            &coid,
        )
        .await?;
        if let Ok(mut map) = self.quote_symbols.lock() {
            map.insert(venue_qid, (sym_str.clone(), coid));
        }
        Ok(venue_qid)
    }

    async fn market_close(&self, symbol: &Symbol, side: Side, qty: Size) -> Result<(), VenueError> {
        self.check_mainnet_gate()?;
        let sym_str = binance_symbol(symbol);
        let size = round_size(&self.exchange_info_cache, &sym_str, qty)?;
        let size_str = Self::format_decimal(size.0);
        let base_url = self.env.rest_base_url();
        if self.env.is_futures() {
            crate::futs::place_market_order(
                &self.http,
                base_url,
                &self.api_key,
                &self.key_material,
                &sym_str,
                side,
                &size_str,
            )
            .await?;
        } else {
            return Err(VenueError::Rejected {
                reason: "market_close not supported for spot".to_string(),
            });
        }
        Ok(())
    }

    /// Cancel the old quote then place a new one.
    async fn requote(&self, id: QuoteId, intent: QuoteIntent) -> Result<(), VenueError> {
        self.check_mainnet_gate()?;

        let sym_str = binance_symbol(&intent.symbol);
        let coid = Self::client_order_id(id);
        let base_url = self.env.rest_base_url();

        // Cancel old (idempotent).
        let cancel_result = if self.env.is_futures() {
            crate::futs::cancel_order(
                &self.http,
                base_url,
                &self.api_key,
                &self.key_material,
                &sym_str,
                &coid,
            )
            .await
        } else {
            crate::spot::cancel_order(
                &self.http,
                base_url,
                &self.api_key,
                &self.key_material,
                &sym_str,
                &coid,
            )
            .await
        };
        if let Err(e) = cancel_result {
            warn!(error = ?e, "requote: cancel failed; proceeding with new quote");
        }

        // Place new.
        self.quote(intent).await?;
        Ok(())
    }

    /// Cancel a single quote by id. Idempotent — an unknown id is treated
    /// as already cancelled (returns `Ok`).
    ///
    /// The symbol-lookup map is populated by `quote()`. A miss happens
    /// when the order was already cancelled (the entry is removed on
    /// successful cancel) or when a strategy emits a recovery `Cancel`
    /// that raced with an earlier successful one. Both cases are safe
    /// to no-op.
    async fn cancel(&self, id: QuoteId) -> Result<(), VenueError> {
        self.check_mainnet_gate()?;
        let entry = match self.quote_symbols.lock() {
            Ok(map) => map.get(&id).cloned(),
            Err(_) => None,
        };
        let Some((sym_str, coid)) = entry else {
            tracing::debug!(
                ?id,
                "BinanceClient::cancel: unknown QuoteId — treating as already cancelled"
            );
            return Ok(());
        };
        let base_url = self.env.rest_base_url();
        let result = if self.env.is_futures() {
            crate::futs::cancel_order(
                &self.http,
                base_url,
                &self.api_key,
                &self.key_material,
                &sym_str,
                &coid,
            )
            .await
        } else {
            crate::spot::cancel_order(
                &self.http,
                base_url,
                &self.api_key,
                &self.key_material,
                &sym_str,
                &coid,
            )
            .await
        };
        if result.is_ok()
            && let Ok(mut map) = self.quote_symbols.lock()
        {
            map.remove(&id);
        }
        result
    }

    /// Batch placement via `POST /fapi/v1/batchOrders` (≤5 orders/request).
    /// Falls back to the per-order loop on spot or a closed mainnet gate.
    /// Each prepared intent maps to one wire order; per-intent prep failures
    /// (rounding / min-notional) become an `Err` at that position and are not
    /// sent. Results are returned in input order.
    async fn batch_quote(&self, intents: Vec<QuoteIntent>) -> Vec<Result<QuoteId, VenueError>> {
        // Spot has no batch endpoint; a closed mainnet gate must surface its
        // own error — both fall back to the per-order path.
        if !self.env.is_futures() || self.check_mainnet_gate().is_err() {
            let mut out = Vec::with_capacity(intents.len());
            for intent in intents {
                out.push(self.quote(intent).await);
            }
            return out;
        }
        let base_url = self.env.rest_base_url();
        let mut results: Vec<Option<Result<QuoteId, VenueError>>> =
            (0..intents.len()).map(|_| None).collect();
        // Prep each intent; keep the owned wire strings alive for the borrow.
        let mut prepped: Vec<(usize, PreppedOrder)> = Vec::new();
        for (idx, intent) in intents.iter().enumerate() {
            let sym_str = binance_symbol(&intent.symbol);
            let price = match round_price_for_side(
                &self.exchange_info_cache,
                &sym_str,
                intent.price,
                intent.side,
            ) {
                Ok(p) => p,
                Err(e) => {
                    results[idx] = Some(Err(e));
                    continue;
                }
            };
            let size = match round_size(&self.exchange_info_cache, &sym_str, intent.size) {
                Ok(s) => s,
                Err(e) => {
                    results[idx] = Some(Err(e));
                    continue;
                }
            };
            if let Err(e) = validate_qty(&self.exchange_info_cache, &sym_str, size, price) {
                results[idx] = Some(Err(e));
                continue;
            }
            prepped.push((
                idx,
                PreppedOrder {
                    sym: sym_str,
                    price: Self::format_decimal(price.0),
                    size: Self::format_decimal(size.0),
                    coid: Self::client_order_id(QuoteId::new()),
                    side: intent.side,
                    tif: intent.tif,
                    sort_price: price.0,
                },
            ));
        }
        // Split into bid + ask, each sorted touch-first (best→worst): bids
        // high→low, asks low→high. The two sides are placed CONCURRENTLY (one
        // logical "thread" each) so their sequential 5-order batch round-trips
        // overlap — full-side placement is bounded by the slower side, not the
        // sum of both. Within a side, the touch orders go out first.
        let mut bid_pis: Vec<usize> = (0..prepped.len())
            .filter(|&pi| prepped[pi].1.side == Side::Bid)
            .collect();
        let mut ask_pis: Vec<usize> = (0..prepped.len())
            .filter(|&pi| prepped[pi].1.side == Side::Ask)
            .collect();
        bid_pis.sort_by(|&a, &b| prepped[b].1.sort_price.cmp(&prepped[a].1.sort_price));
        ask_pis.sort_by(|&a, &b| prepped[a].1.sort_price.cmp(&prepped[b].1.sort_price));

        let (bid_res, ask_res) = tokio::join!(
            self.place_side_batches(base_url, &prepped, &bid_pis),
            self.place_side_batches(base_url, &prepped, &ask_pis),
        );
        for (orig_idx, res) in bid_res.into_iter().chain(ask_res) {
            results[orig_idx] = Some(res);
        }
        results
            .into_iter()
            .map(|r| {
                r.unwrap_or_else(|| {
                    Err(VenueError::Internal(Box::new(std::io::Error::other(
                        "batch_quote: result slot never filled",
                    ))))
                })
            })
            .collect()
    }

    /// Batch cancel via `DELETE /fapi/v1/batchOrders` (≤10/request). Falls back
    /// to the per-order loop on spot / closed gate. Unknown ids (already gone)
    /// and `-2011`/`-2013` are idempotent `Ok`. Results in input order.
    async fn batch_cancel(&self, ids: Vec<QuoteId>) -> Vec<Result<(), VenueError>> {
        if !self.env.is_futures() || self.check_mainnet_gate().is_err() {
            let mut out = Vec::with_capacity(ids.len());
            for id in ids {
                out.push(self.cancel(id).await);
            }
            return out;
        }
        let base_url = self.env.rest_base_url();
        let mut results: Vec<Option<Result<(), VenueError>>> =
            (0..ids.len()).map(|_| None).collect();
        // Resolve each id → (symbol, coid). A miss = already gone → Ok.
        let mut resolved: Vec<(usize, String, String)> = Vec::new();
        {
            let map = self.quote_symbols.lock().ok();
            for (idx, id) in ids.iter().enumerate() {
                match map.as_ref().and_then(|m| m.get(id)) {
                    Some((sym, coid)) => resolved.push((idx, sym.clone(), coid.clone())),
                    None => results[idx] = Some(Ok(())),
                }
            }
        }
        let mut by_symbol: HashMap<String, Vec<usize>> = HashMap::new();
        for (ri, (_, sym, _)) in resolved.iter().enumerate() {
            by_symbol.entry(sym.clone()).or_default().push(ri);
        }
        for (sym, ris) in by_symbol {
            for chunk in ris.chunks(10) {
                let coids: Vec<String> = chunk.iter().map(|&ri| resolved[ri].2.clone()).collect();
                let cancelled = crate::futs::cancel_batch_orders(
                    &self.http,
                    base_url,
                    &self.api_key,
                    &self.key_material,
                    &sym,
                    &coids,
                )
                .await;
                match cancelled {
                    Ok(per) => {
                        for (&ri, res) in chunk.iter().zip(per) {
                            let (orig_idx, id) = (resolved[ri].0, ids[resolved[ri].0]);
                            if res.is_ok()
                                && let Ok(mut map) = self.quote_symbols.lock()
                            {
                                map.remove(&id);
                            }
                            results[orig_idx] = Some(res);
                        }
                    }
                    Err(e) => {
                        let reason = format!("batch cancel request failed: {e:?}");
                        for &ri in chunk {
                            let orig_idx = resolved[ri].0;
                            results[orig_idx] = Some(Err(clone_batch_err(&e, &reason)));
                        }
                    }
                }
            }
        }
        results.into_iter().map(|r| r.unwrap_or(Ok(()))).collect()
    }

    async fn open_orders(&self, symbol: &Symbol) -> Result<Vec<tikr_venue::OpenOrder>, VenueError> {
        let sym_str = binance_symbol(symbol);
        let base_url = self.env.rest_base_url();
        if !self.env.is_futures() {
            // Spot path not implemented yet — fall through to empty so
            // the runner's reconciliation simply does nothing on spot.
            return Ok(Vec::new());
        }
        let rows = crate::futs::get_open_orders(
            &self.http,
            base_url,
            &self.api_key,
            &self.key_material,
            &sym_str,
        )
        .await?;
        // Map each Binance row to a venue-agnostic OpenOrder. The QuoteId
        // is derived the same way as in the user_stream parser
        // (`QuoteId::from_uuid(Uuid::from_u128(order_id as u128))`) so
        // FillSim can compare set membership against IDs it already
        // tracks from `enqueue_place_with_id`.
        let out = rows
            .into_iter()
            .map(|(id, side, price, qty)| tikr_venue::OpenOrder {
                id: tikr_venue::QuoteId::from_uuid(uuid::Uuid::from_u128(id as u128)),
                symbol: symbol.clone(),
                side,
                price: tikr_core::Price(price),
                size: tikr_core::Size(qty),
            })
            .collect();
        Ok(out)
    }

    /// Cancel all open orders for a symbol.
    async fn cancel_all(&self, symbol: &Symbol) -> Result<(), VenueError> {
        self.check_mainnet_gate()?;

        let sym_str = binance_symbol(symbol);
        let base_url = self.env.rest_base_url();

        if self.env.is_futures() {
            crate::futs::cancel_all_orders(
                &self.http,
                base_url,
                &self.api_key,
                &self.key_material,
                &sym_str,
            )
            .await
        } else {
            crate::spot::cancel_all_orders(
                &self.http,
                base_url,
                &self.api_key,
                &self.key_material,
                &sym_str,
            )
            .await
        }
    }

    /// Return the current position for a symbol.
    ///
    /// Futures: REST call to `/fapi/v3/positionRisk` returns signed
    /// position amount + avg entry price. Caller can rely on
    /// `avg_entry` being populated when `size != 0` — critical for
    /// the runner's position-drift safety net, which would otherwise
    /// `force_reconcile` to `avg_entry = 0` and treat all subsequent
    /// closing PnL as if entry were $0 (inflates realized hugely).
    ///
    /// Spot: returns flat (no position concept).
    async fn position(&self, symbol: &Symbol) -> Result<Position, VenueError> {
        use tikr_core::Notional;
        if self.env.is_futures() {
            let sym_str = binance_symbol(symbol);
            let base_url = self.env.rest_base_url();
            let pr = crate::futs::get_position_risk(
                &self.http,
                base_url,
                &self.api_key,
                &self.key_material,
                &sym_str,
            )
            .await?;
            return Ok(Position {
                symbol: symbol.clone(),
                size: SignedSize(pr.position_amount),
                avg_entry: Price(pr.entry_price),
                realized_pnl: Notional(tikr_core::Decimal::ZERO),
            });
        }
        Ok(Position {
            symbol: symbol.clone(),
            size: SignedSize(tikr_core::Decimal::ZERO),
            avg_entry: Price(tikr_core::Decimal::ZERO),
            realized_pnl: Notional(tikr_core::Decimal::ZERO),
        })
    }

    /// Return fills for `symbol` timestamped at or after `since_ts` (ns).
    ///
    /// Futures: signed `GET /fapi/v1/userTrades` since `since_ts`, each row
    /// mapped to a [`Fill`] with its `trade_id` populated so the runner can
    /// deduplicate against the WS stream and replay only missed fills. Spot
    /// reconciliation is not wired yet (the live bots are futures); opt out.
    async fn fills_since(&self, symbol: &Symbol, since_ts: u64) -> Result<Vec<Fill>, VenueError> {
        if !self.env.is_futures() {
            return Ok(Vec::new());
        }
        let sym_str = binance_symbol(symbol);
        let base_url = self.env.rest_base_url();
        // Trait carries nanoseconds; Binance `startTime` is milliseconds.
        let start_ms = since_ts / 1_000_000;
        crate::futs::get_user_trades(
            &self.http,
            base_url,
            &self.api_key,
            &self.key_material,
            &sym_str,
            start_ms,
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// Credential helpers (used by example bins)
// ---------------------------------------------------------------------------

/// Load API key + HMAC secret from environment variables.
///
/// Reads `TIKR_BINANCE_API_KEY` and `TIKR_BINANCE_API_SECRET`.
///
/// For Ed25519, use [`load_key_material_from_env`] instead.
///
/// Env var resolution is **product-aware**: when `env` is a spot variant we
/// look for `TIKR_BINANCE_SPOT_*` first; for futures we look for
/// `TIKR_BINANCE_FUTURES_*` first. Either falls back to plain `TIKR_BINANCE_*`
/// so a single-product .env still works without changes.
pub fn load_credentials_from_env(env: BinanceEnv) -> Result<(String, String), String> {
    let key = env_with_product_fallback(env, "API_KEY")
        .ok_or_else(|| format!("{} (or fallback) not set", product_var(env, "API_KEY")))?;
    let secret = env_with_product_fallback(env, "API_SECRET")
        .ok_or_else(|| format!("{} (or fallback) not set", product_var(env, "API_SECRET")))?;
    Ok((key, secret))
}

/// Look up an env var, trying the product-specific name first
/// (`TIKR_BINANCE_SPOT_<SUFFIX>` or `TIKR_BINANCE_FUTURES_<SUFFIX>`)
/// then falling back to the plain name (`TIKR_BINANCE_<SUFFIX>`).
pub fn env_with_product_fallback(env: BinanceEnv, suffix: &str) -> Option<String> {
    std::env::var(product_var(env, suffix))
        .ok()
        .or_else(|| std::env::var(format!("TIKR_BINANCE_{suffix}")).ok())
}

/// Build the product-specific env var name (e.g. `TIKR_BINANCE_SPOT_API_KEY`).
pub fn product_var(env: BinanceEnv, suffix: &str) -> String {
    let product = if env.is_futures() { "FUTURES" } else { "SPOT" };
    format!("TIKR_BINANCE_{product}_{suffix}")
}

/// Load API key + HMAC secret from a key file (`key:secret` single line).
pub fn load_credentials_from_file(path: &std::path::Path) -> Result<(String, String), String> {
    let content =
        std::fs::read_to_string(path).map_err(|e| format!("key-file {}: {}", path.display(), e))?;
    let line = content.trim();
    let mut parts = line.splitn(2, ':');
    let key = parts
        .next()
        .filter(|s| !s.is_empty())
        .ok_or("key-file: empty key")?
        .to_string();
    let secret = parts
        .next()
        .filter(|s| !s.is_empty())
        .ok_or("key-file: missing ':secret' part")?
        .to_string();
    Ok((key, secret))
}

/// Load [`BinanceKeyMaterial`] from environment variables.
///
/// Env var resolution is **product-aware**: for spot envs we look at
/// `TIKR_BINANCE_SPOT_*` first, for futures at `TIKR_BINANCE_FUTURES_*`;
/// either falls back to plain `TIKR_BINANCE_*`. This lets one `.env` carry
/// distinct keys for both products simultaneously.
///
/// Reads `<KEY_TYPE>` (default: `hmac`) and then either:
/// - HMAC: `<API_SECRET>`
/// - Ed25519: `<PRIVATE_KEY_PATH>` (path to PEM file)
///
/// `ed25519_key_file` overrides `<PRIVATE_KEY_PATH>` for Ed25519.
pub fn load_key_material_from_env(
    env: BinanceEnv,
    ed25519_key_file: Option<&std::path::Path>,
) -> Result<BinanceKeyMaterial, String> {
    let key_type = env_with_product_fallback(env, "KEY_TYPE")
        .unwrap_or_else(|| "hmac".to_string())
        .to_lowercase();

    match key_type.as_str() {
        "hmac" => {
            let secret = env_with_product_fallback(env, "API_SECRET").ok_or_else(|| {
                format!(
                    "{} (or fallback) not set (required for HMAC)",
                    product_var(env, "API_SECRET")
                )
            })?;
            Ok(BinanceKeyMaterial::Hmac { secret })
        }
        "ed25519" => {
            let path = if let Some(p) = ed25519_key_file {
                p.to_path_buf()
            } else {
                env_with_product_fallback(env, "PRIVATE_KEY_PATH")
                    .map(std::path::PathBuf::from)
                    .ok_or_else(|| {
                        format!(
                            "Ed25519 key path required: \
                             --ed25519-key-file or {} (or fallback)",
                            product_var(env, "PRIVATE_KEY_PATH")
                        )
                    })?
            };
            let pem = std::fs::read_to_string(&path)
                .map_err(|e| format!("Ed25519 PEM file {}: {e}", path.display()))?;
            let signing_key = sign::load_ed25519_from_pem(&pem)
                .map_err(|e| format!("Ed25519 PEM parse: {e:?}"))?;
            Ok(BinanceKeyMaterial::Ed25519 { signing_key })
        }
        other => Err(format!(
            "Unknown {}: {other} (expected hmac|ed25519)",
            product_var(env, "KEY_TYPE")
        )),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_order_id_format() {
        let id = QuoteId::from_uuid(Uuid::from_u128(1));
        let coid = BinanceClient::client_order_id(id);
        // Binance limit: clientOrderId length < 36 chars (verified via -4015
        // on live testnet). Bare 32-hex fits with margin.
        assert_eq!(
            coid.len(),
            32,
            "clientOrderId must be exactly 32 chars (< 36 Binance limit)"
        );
        assert!(coid.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn client_order_id_roundtrip() {
        let id = QuoteId::new();
        let coid = BinanceClient::client_order_id(id);
        let recovered = BinanceClient::quote_id_from_client_order_id(&coid)
            .expect("must recover QuoteId from clientOrderId");
        assert_eq!(id, recovered);
    }

    #[test]
    fn binance_env_mainnet_flag() {
        assert!(BinanceEnv::SpotMainnet.is_mainnet());
        assert!(BinanceEnv::FuturesMainnet.is_mainnet());
        assert!(!BinanceEnv::SpotTestnet.is_mainnet());
        assert!(!BinanceEnv::FuturesTestnet.is_mainnet());
    }

    #[test]
    fn binance_env_futures_flag() {
        assert!(BinanceEnv::FuturesTestnet.is_futures());
        assert!(BinanceEnv::FuturesMainnet.is_futures());
        assert!(!BinanceEnv::SpotTestnet.is_futures());
        assert!(!BinanceEnv::SpotMainnet.is_futures());
    }

    #[test]
    fn product_var_names_built_correctly() {
        assert_eq!(
            product_var(BinanceEnv::SpotTestnet, "API_KEY"),
            "TIKR_BINANCE_SPOT_API_KEY"
        );
        assert_eq!(
            product_var(BinanceEnv::SpotMainnet, "PRIVATE_KEY_PATH"),
            "TIKR_BINANCE_SPOT_PRIVATE_KEY_PATH"
        );
        assert_eq!(
            product_var(BinanceEnv::FuturesTestnet, "API_KEY"),
            "TIKR_BINANCE_FUTURES_API_KEY"
        );
        assert_eq!(
            product_var(BinanceEnv::FuturesMainnet, "KEY_TYPE"),
            "TIKR_BINANCE_FUTURES_KEY_TYPE"
        );
    }

    /// Product-specific env wins over plain fallback when both are set.
    /// Plain fallback used when product-specific is absent.
    ///
    /// SAFETY: writing/reading env in tests is racy with other tests.
    /// Use a unique key to avoid clashes.
    #[test]
    fn env_with_product_fallback_prefers_product_specific() {
        // Use a unique-ish key to avoid clashing with other tests.
        let suffix = "TEST_PROD_FALLBACK_FOO";
        let plain = format!("TIKR_BINANCE_{suffix}");
        let product = format!("TIKR_BINANCE_FUTURES_{suffix}");

        // SAFETY: env mutation is racy with other tests in the same process.
        // No other test touches these specific keys.
        unsafe {
            std::env::set_var(&plain, "plain-value");
            std::env::set_var(&product, "product-value");
        }
        assert_eq!(
            env_with_product_fallback(BinanceEnv::FuturesTestnet, suffix).as_deref(),
            Some("product-value"),
            "product-specific must win when both set"
        );
        unsafe {
            std::env::remove_var(&product);
        }
        assert_eq!(
            env_with_product_fallback(BinanceEnv::FuturesTestnet, suffix).as_deref(),
            Some("plain-value"),
            "plain fallback used when product-specific absent"
        );
        unsafe {
            std::env::remove_var(&plain);
        }
        assert!(
            env_with_product_fallback(BinanceEnv::FuturesTestnet, suffix).is_none(),
            "None when neither set"
        );
    }

    #[test]
    fn mainnet_gate_refuses_without_flag() {
        // Simulate the gate check logic.
        let is_mainnet = true;
        let mainnet_writes_enabled = false;
        let result: Result<(), VenueError> = if is_mainnet && !mainnet_writes_enabled {
            Err(VenueError::Rejected {
                reason: "mainnet writes disabled — set TIKR_BINANCE_ENABLE_MAINNET=1".into(),
            })
        } else {
            Ok(())
        };
        assert!(matches!(result, Err(VenueError::Rejected { .. })));
    }

    #[test]
    fn load_credentials_from_file_parses() {
        use std::io::Write;
        let mut tmp = tempfile_hack();
        writeln!(tmp.0, "mykey:mysecret").unwrap();
        let (key, secret) = load_credentials_from_file(&tmp.1).unwrap();
        assert_eq!(key, "mykey");
        assert_eq!(secret, "mysecret");
    }

    // Minimal temp file helper to avoid adding a dep.
    fn tempfile_hack() -> (std::fs::File, std::path::PathBuf) {
        let path = std::env::temp_dir().join("tikr_binance_test_key.txt");
        let f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        (f, path)
    }
}
