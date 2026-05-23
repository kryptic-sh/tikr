//! Symbol + side mapping between tikr core types and Bybit V5 wire types.

use tikr_core::{Side, Symbol};

/// Build a Bybit V5 symbol string (uppercase base+quote, no separator).
///
/// Bybit linear symbols follow the same convention as Binance perps —
/// `BTCUSDT`, `HYPERUSDT`, etc. The `venue` + `kind` fields on [`Symbol`]
/// are dropped because Bybit's REST/WS only sees the ticker.
pub fn bybit_symbol(sym: &Symbol) -> String {
    format!("{}{}", sym.base.0.as_ref(), sym.quote.0.as_ref()).to_uppercase()
}

/// Bybit side wire token. `"Buy"` / `"Sell"` (CamelCase, per V5 docs).
pub fn side_wire(side: Side) -> &'static str {
    match side {
        Side::Bid => "Buy",
        Side::Ask => "Sell",
    }
}

/// Parse Bybit's `"Buy"` / `"Sell"` taker-side token. Returns `None`
/// for any other string — defensive against API shape drift.
pub fn parse_side_wire(s: &str) -> Option<Side> {
    match s {
        "Buy" => Some(Side::Bid),
        "Sell" => Some(Side::Ask),
        _ => None,
    }
}
