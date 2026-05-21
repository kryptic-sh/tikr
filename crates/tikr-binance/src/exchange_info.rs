//! Binance `exchangeInfo` fetch, parse, and precision cache.
//!
//! Fetched once at startup for both spot and futures. Cached as a
//! `HashMap<String, SymbolFilters>` keyed by uppercase symbol name
//! (e.g. `"BTCUSDT"`).
//!
//! ## Filters
//!
//! Each symbol in `exchangeInfo.symbols[].filters` contains:
//! - `PRICE_FILTER` — `tickSize` for price rounding.
//! - `LOT_SIZE` — `stepSize` and `minQty` for size rounding.
//! - `MIN_NOTIONAL` or `NOTIONAL` — `minNotional` / `notional` for value floor.
//!
//! ## Rounding
//!
//! All rounding is **floor** (not round-half-up). This ensures we never
//! accidentally place an order at a price/size that violates tick/step
//! constraints.

use std::collections::HashMap;
use std::str::FromStr;

use rust_decimal::Decimal;
use serde::Deserialize;
use tikr_core::{Price, Side, Size};
use tikr_venue::VenueError;

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

/// Raw `exchangeInfo` response from Binance REST.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExchangeInfoResponse {
    /// Symbol list from the response.
    pub symbols: Vec<SymbolInfo>,
}

/// Per-symbol entry from `exchangeInfo`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SymbolInfo {
    /// Binance symbol name (e.g. `"BTCUSDT"`).
    pub symbol: String,
    /// Raw filter entries from the API.
    pub filters: Vec<FilterEntry>,
}

/// A single precision filter entry from `exchangeInfo`.
#[derive(Debug, Deserialize)]
#[serde(tag = "filterType", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FilterEntry {
    /// Price tick-size filter.
    #[serde(rename = "PRICE_FILTER")]
    PriceFilter {
        /// Minimum price increment.
        #[serde(rename = "tickSize")]
        tick_size: String,
    },
    /// Lot-size (quantity step) filter.
    #[serde(rename = "LOT_SIZE")]
    LotSize {
        /// Minimum size increment.
        #[serde(rename = "stepSize")]
        step_size: String,
        /// Minimum order quantity.
        #[serde(rename = "minQty")]
        min_qty: String,
    },
    /// Minimum notional. Field name differs by product:
    /// - Spot: `minNotional`
    /// - USD-M Futures: `notional` (verified against testnet
    ///   `/fapi/v1/exchangeInfo` 2026-05-19)
    ///
    /// Accept both via serde alias.
    #[serde(rename = "MIN_NOTIONAL")]
    MinNotional {
        /// Minimum notional value (price × size).
        #[serde(rename = "minNotional", alias = "notional")]
        min_notional: String,
    },
    /// Some Futures endpoints emit `NOTIONAL` filter type instead of
    /// `MIN_NOTIONAL`. Field name varies; accept both.
    #[serde(rename = "NOTIONAL")]
    Notional {
        /// Minimum notional value (price × size).
        #[serde(rename = "minNotional", alias = "notional")]
        min_notional: String,
    },
    /// Any other filter type we don't care about.
    #[serde(other)]
    Unknown,
}

// ---------------------------------------------------------------------------
// SymbolFilters — parsed cache entry
// ---------------------------------------------------------------------------

/// Precision constraints for a single Binance symbol.
#[derive(Debug, Clone)]
pub struct SymbolFilters {
    /// Price tick size (floor divisor for price rounding).
    pub tick_size: Decimal,
    /// Lot step size (floor divisor for size rounding).
    pub step_size: Decimal,
    /// Minimum order quantity.
    pub min_qty: Decimal,
    /// Minimum order notional (price × size floor).
    pub min_notional: Decimal,
}

// ---------------------------------------------------------------------------
// Cache
// ---------------------------------------------------------------------------

/// Precision filter cache keyed by uppercase symbol name.
pub type ExchangeInfoCache = HashMap<String, SymbolFilters>;

/// Parse an [`ExchangeInfoResponse`] into a cache.
pub fn parse_exchange_info(resp: &ExchangeInfoResponse) -> ExchangeInfoCache {
    let mut cache = HashMap::new();
    for sym in &resp.symbols {
        let mut tick_size = Decimal::new(1, 8); // sensible default
        let mut step_size = Decimal::new(1, 8);
        let mut min_qty = Decimal::ZERO;
        let mut min_notional = Decimal::ZERO;

        for filter in &sym.filters {
            match filter {
                FilterEntry::PriceFilter { tick_size: ts } => {
                    if let Ok(d) = Decimal::from_str(ts)
                        && d > Decimal::ZERO
                    {
                        tick_size = d;
                    }
                }
                FilterEntry::LotSize {
                    step_size: ss,
                    min_qty: mq,
                } => {
                    if let Ok(d) = Decimal::from_str(ss)
                        && d > Decimal::ZERO
                    {
                        step_size = d;
                    }
                    if let Ok(d) = Decimal::from_str(mq) {
                        min_qty = d;
                    }
                }
                FilterEntry::MinNotional { min_notional: mn }
                | FilterEntry::Notional { min_notional: mn } => {
                    if let Ok(d) = Decimal::from_str(mn) {
                        min_notional = d;
                    }
                }
                FilterEntry::Unknown => {}
            }
        }

        cache.insert(
            sym.symbol.to_uppercase(),
            SymbolFilters {
                tick_size,
                step_size,
                min_qty,
                min_notional,
            },
        );
    }
    cache
}

// ---------------------------------------------------------------------------
// Rounding helpers
// ---------------------------------------------------------------------------

/// Round `price` down to the nearest `tick_size` for `symbol`.
///
/// Returns `VenueError::Rejected` if the symbol is not in the cache.
///
/// **Side-unaware** — see [`round_price_for_side`] for the post-only-
/// safe variant the runner uses.
pub fn round_price(
    cache: &ExchangeInfoCache,
    symbol: &str,
    price: Price,
) -> Result<Price, VenueError> {
    let filters = get_filters(cache, symbol)?;
    Ok(Price(floor_to(price.0, filters.tick_size)))
}

/// Round `price` to the nearest `tick_size`, biased AWAY from the
/// spread so post-only orders don't accidentally land on the
/// opposite side of book and get rejected as `-5022`.
///
/// - `Side::Bid` (buy quote): floor — a tick lower is still post-only.
/// - `Side::Ask` (sell quote): ceil — a tick higher is still post-only.
///
/// Naïve floor rounding on both sides was the source of an infinite
/// reject loop seen on HYPERUSDT: bid 0.1157, ask 0.1158, mid 0.11575.
/// Strategy emits sell @ 0.11580 + ε; floor → 0.1157 which equals the
/// bid → Binance rejects as crossing maker. Ceiling pushes us to
/// 0.1158 which stays strictly inside the ask.
pub fn round_price_for_side(
    cache: &ExchangeInfoCache,
    symbol: &str,
    price: Price,
    side: Side,
) -> Result<Price, VenueError> {
    let filters = get_filters(cache, symbol)?;
    let rounded = match side {
        Side::Bid => floor_to(price.0, filters.tick_size),
        Side::Ask => ceil_to(price.0, filters.tick_size),
    };
    Ok(Price(rounded))
}

/// Round `size` down to the nearest `step_size` for `symbol`.
///
/// Returns `VenueError::Rejected` if the symbol is not in the cache.
pub fn round_size(cache: &ExchangeInfoCache, symbol: &str, size: Size) -> Result<Size, VenueError> {
    let filters = get_filters(cache, symbol)?;
    Ok(Size(floor_to(size.0, filters.step_size)))
}

/// Validate that `size` and `price` satisfy the minimum qty and notional
/// constraints for `symbol`.
///
/// Returns `VenueError::Rejected` on violation or if the symbol is unknown.
pub fn validate_qty(
    cache: &ExchangeInfoCache,
    symbol: &str,
    size: Size,
    price: Price,
) -> Result<(), VenueError> {
    let filters = get_filters(cache, symbol)?;

    if size.0 < filters.min_qty {
        return Err(VenueError::Rejected {
            reason: format!("{}: size {} < minQty {}", symbol, size.0, filters.min_qty),
        });
    }
    let notional = price.0 * size.0;
    if notional < filters.min_notional && filters.min_notional > Decimal::ZERO {
        return Err(VenueError::Rejected {
            reason: format!(
                "{}: notional {} < minNotional {}",
                symbol, notional, filters.min_notional
            ),
        });
    }
    Ok(())
}

fn get_filters<'a>(
    cache: &'a ExchangeInfoCache,
    symbol: &str,
) -> Result<&'a SymbolFilters, VenueError> {
    cache
        .get(&symbol.to_uppercase())
        .ok_or_else(|| VenueError::Rejected {
            reason: format!("unknown symbol '{}'; not in exchangeInfo cache", symbol),
        })
}

/// Floor `value` to the nearest multiple of `tick`.
///
/// If `tick` is zero or negative, returns `value` unchanged (no-op guard).
pub fn floor_to(value: Decimal, tick: Decimal) -> Decimal {
    if tick <= Decimal::ZERO {
        return value;
    }
    // floor(value / tick) * tick
    let quotient = (value / tick).floor();
    quotient * tick
}

/// Ceil `value` to the nearest multiple of `tick`.
///
/// If `tick` is zero or negative, returns `value` unchanged (no-op guard).
pub fn ceil_to(value: Decimal, tick: Decimal) -> Decimal {
    if tick <= Decimal::ZERO {
        return value;
    }
    let quotient = (value / tick).ceil();
    quotient * tick
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_cache() -> ExchangeInfoCache {
        let mut m = HashMap::new();
        m.insert(
            "BTCUSDT".to_string(),
            SymbolFilters {
                tick_size: Decimal::from_str("0.01").unwrap(),
                step_size: Decimal::from_str("0.00001").unwrap(),
                min_qty: Decimal::from_str("0.00001").unwrap(),
                min_notional: Decimal::from_str("10.0").unwrap(),
            },
        );
        m
    }

    #[test]
    fn round_price_to_tick() {
        let cache = make_cache();
        // 30000.005 should floor to 30000.00
        let p = Price(Decimal::from_str("30000.005").unwrap());
        let rounded = round_price(&cache, "BTCUSDT", p).unwrap();
        assert_eq!(rounded.0, Decimal::from_str("30000.00").unwrap());
    }

    #[test]
    fn round_price_to_tick_exact_multiple() {
        let cache = make_cache();
        let p = Price(Decimal::from_str("29999.99").unwrap());
        let rounded = round_price(&cache, "BTCUSDT", p).unwrap();
        assert_eq!(rounded.0, Decimal::from_str("29999.99").unwrap());
    }

    #[test]
    fn round_size_to_step() {
        let cache = make_cache();
        // 0.000019 should floor to 0.00001 (step = 0.00001)
        let s = Size(Decimal::from_str("0.000019").unwrap());
        let rounded = round_size(&cache, "BTCUSDT", s).unwrap();
        assert_eq!(rounded.0, Decimal::from_str("0.00001").unwrap());
    }

    #[test]
    fn reject_below_min_qty() {
        let cache = make_cache();
        // 0.000005 < minQty 0.00001
        let s = Size(Decimal::from_str("0.000005").unwrap());
        let p = Price(Decimal::from_str("30000.0").unwrap());
        let err = validate_qty(&cache, "BTCUSDT", s, p).unwrap_err();
        assert!(matches!(err, VenueError::Rejected { .. }));
    }

    #[test]
    fn reject_below_min_notional() {
        let cache = make_cache();
        // size=0.0001, price=50 → notional=0.005 < minNotional=10
        let s = Size(Decimal::from_str("0.0001").unwrap());
        let p = Price(Decimal::from_str("50.0").unwrap());
        let err = validate_qty(&cache, "BTCUSDT", s, p).unwrap_err();
        assert!(matches!(err, VenueError::Rejected { .. }));
    }

    #[test]
    fn validate_passes_for_valid_order() {
        let cache = make_cache();
        // size=0.001, price=30000 → notional=30 > 10
        let s = Size(Decimal::from_str("0.001").unwrap());
        let p = Price(Decimal::from_str("30000.0").unwrap());
        assert!(validate_qty(&cache, "BTCUSDT", s, p).is_ok());
    }

    /// Regression: USD-M Futures testnet emits `MIN_NOTIONAL` with field
    /// `notional` (not `minNotional`). First-run smoke for issue #45 surfaced
    /// this as a decode failure at column 2031. The MinNotional variant must
    /// accept both field names via serde alias.
    #[test]
    fn futures_min_notional_uses_notional_field() {
        let json = r#"{
            "symbols": [{
                "symbol": "BTCUSDT",
                "filters": [
                    { "filterType": "PRICE_FILTER", "tickSize": "0.10" },
                    { "filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001" },
                    { "filterType": "MIN_NOTIONAL", "notional": "50" }
                ]
            }]
        }"#;
        let resp: ExchangeInfoResponse =
            serde_json::from_str(json).expect("futures-shaped JSON must decode");
        let cache = parse_exchange_info(&resp);
        let filters = cache.get("BTCUSDT").expect("symbol must parse");
        assert_eq!(
            filters.min_notional,
            Decimal::from_str("50").unwrap(),
            "futures `notional` field must populate min_notional"
        );
    }

    #[test]
    fn floor_to_basic() {
        let tick = Decimal::from_str("0.01").unwrap();
        // 1.234 floors to 1.23
        assert_eq!(
            floor_to(Decimal::from_str("1.234").unwrap(), tick),
            Decimal::from_str("1.23").unwrap()
        );
        // 1.230 stays 1.23
        assert_eq!(
            floor_to(Decimal::from_str("1.230").unwrap(), tick),
            Decimal::from_str("1.23").unwrap()
        );
    }

    #[test]
    fn unknown_symbol_returns_rejected() {
        let cache = make_cache();
        let p = Price(Decimal::from_str("1.0").unwrap());
        let err = round_price(&cache, "UNKNOWN", p).unwrap_err();
        assert!(matches!(err, VenueError::Rejected { .. }));
    }
}
