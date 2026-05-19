//! Fixture-driven unit tests for [`tikr_hyperliquid::messages`] +
//! [`tikr_hyperliquid::mapping`]. No network, no async runtime.

use std::fs;
use tikr_core::{Asset, Decimal, MarketEvent, MarketKind, Side, Symbol, VenueId};
use tikr_hyperliquid::mapping::*;
use tikr_hyperliquid::messages::*;

fn btc_symbol() -> Symbol {
    Symbol {
        base: Asset::new("BTC"),
        quote: Asset::new("USDC"),
        venue: VenueId::new("hyperliquid"),
        kind: MarketKind::Perp,
    }
}

fn load(name: &str) -> String {
    fs::read_to_string(format!("tests/fixtures/{name}"))
        .unwrap_or_else(|e| panic!("fixture {name}: {e}"))
}

// ---------------------------------------------------------------------------
// l2Book
// ---------------------------------------------------------------------------

#[test]
fn parses_l2book_push_envelope() {
    let txt = load("l2book_push.json");
    let msg: WsMessage = serde_json::from_str(&txt).expect("parse");
    assert!(matches!(msg, WsMessage::L2Book(_)));
}

#[test]
fn maps_l2book_push_to_snapshot() {
    let txt = load("l2book_push.json");
    let msg: WsMessage = serde_json::from_str(&txt).unwrap();
    let WsMessage::L2Book(push) = msg else {
        panic!("expected L2Book");
    };
    let snap = l2_to_snapshot(&btc_symbol(), &push);

    assert_eq!(snap.symbol.base.0.as_ref(), "BTC");
    assert_eq!(snap.bids.len(), 3);
    assert_eq!(snap.asks.len(), 3);

    // bids descending, asks ascending (matches Snapshot contract)
    assert!(snap.bids[0].price.0 > snap.bids[1].price.0);
    assert!(snap.bids[1].price.0 > snap.bids[2].price.0);
    assert!(snap.asks[0].price.0 < snap.asks[1].price.0);
    assert!(snap.asks[1].price.0 < snap.asks[2].price.0);

    // ts: ms → ns
    assert_eq!(snap.ts.0, 1_730_000_000_000 * 1_000_000);
}

#[test]
fn parses_l2book_snapshot_http_response() {
    let txt = load("l2book_snapshot.json");
    let push: L2BookPush = serde_json::from_str(&txt).expect("parse");
    assert_eq!(push.coin, "BTC");
    assert_eq!(push.levels[0].len(), 2);
    assert_eq!(push.levels[1].len(), 2);
}

// ---------------------------------------------------------------------------
// trades
// ---------------------------------------------------------------------------

#[test]
fn parses_trades_push_envelope() {
    let txt = load("trades_push.json");
    let msg: WsMessage = serde_json::from_str(&txt).expect("parse");
    let WsMessage::Trades(trades) = msg else {
        panic!("expected Trades");
    };
    assert_eq!(trades.len(), 2);
}

#[test]
fn maps_trade_side_a_to_ask() {
    let txt = load("trades_push.json");
    let WsMessage::Trades(trades) = serde_json::from_str(&txt).unwrap() else {
        panic!();
    };
    let ev = trade_to_event(&btc_symbol(), &trades[0]);
    let MarketEvent::Trade {
        side, price, ts, ..
    } = ev
    else {
        panic!("expected Trade variant");
    };
    assert_eq!(side, Side::Ask);
    assert_eq!(price.0, Decimal::from_str_exact("60000.5").unwrap());
    assert_eq!(ts.0, 1_730_000_000_000 * 1_000_000);
}

#[test]
fn maps_trade_side_b_to_bid() {
    let txt = load("trades_push.json");
    let WsMessage::Trades(trades) = serde_json::from_str(&txt).unwrap() else {
        panic!();
    };
    let ev = trade_to_event(&btc_symbol(), &trades[1]);
    let MarketEvent::Trade { side, .. } = ev else {
        panic!()
    };
    assert_eq!(side, Side::Bid);
}

// ---------------------------------------------------------------------------
// clearinghouseState
// ---------------------------------------------------------------------------

#[test]
fn parses_clearinghouse_state_response() {
    let txt = load("clearinghouse_state.json");
    let resp: ClearinghouseStateResp = serde_json::from_str(&txt).expect("parse");
    assert_eq!(resp.asset_positions.len(), 2);
}

#[test]
fn maps_position_finds_matching_coin() {
    let txt = load("clearinghouse_state.json");
    let resp: ClearinghouseStateResp = serde_json::from_str(&txt).unwrap();
    let pos = position_from_clearinghouse(&btc_symbol(), &resp);
    assert_eq!(pos.size.0, Decimal::from_str_exact("0.5").unwrap());
    assert_eq!(pos.avg_entry.0, Decimal::from_str_exact("60000.0").unwrap());
    assert_eq!(pos.realized_pnl.0, Decimal::ZERO);
}

#[test]
fn maps_position_signed_negative_for_short() {
    let txt = load("clearinghouse_state.json");
    let resp: ClearinghouseStateResp = serde_json::from_str(&txt).unwrap();
    let eth = Symbol {
        base: Asset::new("ETH"),
        quote: Asset::new("USDC"),
        venue: VenueId::new("hyperliquid"),
        kind: MarketKind::Perp,
    };
    let pos = position_from_clearinghouse(&eth, &resp);
    assert_eq!(pos.size.0, Decimal::from_str_exact("-2.0").unwrap());
}

#[test]
fn maps_position_returns_flat_when_coin_absent() {
    let txt = load("clearinghouse_state.json");
    let resp: ClearinghouseStateResp = serde_json::from_str(&txt).unwrap();
    let sol = Symbol {
        base: Asset::new("SOL"),
        quote: Asset::new("USDC"),
        venue: VenueId::new("hyperliquid"),
        kind: MarketKind::Perp,
    };
    let pos = position_from_clearinghouse(&sol, &resp);
    assert_eq!(pos.size.0, Decimal::ZERO);
    assert_eq!(pos.avg_entry.0, Decimal::ZERO);
}

// ---------------------------------------------------------------------------
// userFills
// ---------------------------------------------------------------------------

#[test]
fn parses_user_fills_response() {
    let txt = load("user_fills.json");
    let fills: Vec<UserFillEntry> = serde_json::from_str(&txt).expect("parse");
    assert_eq!(fills.len(), 2);
    assert_eq!(fills[0].fee_token, "USDC");
    assert_eq!(fills[0].oid, 12345);
}

#[test]
fn maps_user_fill_side_b_is_bid() {
    let txt = load("user_fills.json");
    let entries: Vec<UserFillEntry> = serde_json::from_str(&txt).unwrap();
    let fill = fill_from_user_fill(&entries[0]);
    assert_eq!(fill.side, Side::Bid);
    assert_eq!(fill.price.0, Decimal::from_str_exact("60000.5").unwrap());
    assert_eq!(fill.size.0, Decimal::from_str_exact("0.1").unwrap());
    assert_eq!(fill.fee_amount, Decimal::from_str_exact("0.012").unwrap());
    assert_eq!(fill.fee_asset.0.as_ref(), "USDC");
    assert_eq!(fill.ts.0, 1_730_000_000_000 * 1_000_000);
}

#[test]
fn maps_user_fill_side_a_is_ask() {
    let txt = load("user_fills.json");
    let entries: Vec<UserFillEntry> = serde_json::from_str(&txt).unwrap();
    let fill = fill_from_user_fill(&entries[1]);
    assert_eq!(fill.side, Side::Ask);
}

#[test]
fn user_fill_quote_id_is_stable_for_oid() {
    let txt = load("user_fills.json");
    let entries: Vec<UserFillEntry> = serde_json::from_str(&txt).unwrap();
    let a = fill_from_user_fill(&entries[0]);
    let b = fill_from_user_fill(&entries[0]);
    assert_eq!(a.quote_id, b.quote_id);
    // Different oid → different quote_id.
    let c = fill_from_user_fill(&entries[1]);
    assert_ne!(a.quote_id, c.quote_id);
}

// ---------------------------------------------------------------------------
// envelope behavior
// ---------------------------------------------------------------------------

#[test]
fn unknown_channel_decodes_to_other() {
    let txt = r#"{ "channel": "post", "data": { "anything": 1 } }"#;
    let msg: WsMessage = serde_json::from_str(txt).expect("parse");
    assert!(matches!(msg, WsMessage::Other));
}

#[test]
fn subscription_response_decodes() {
    let txt = r#"{ "channel": "subscriptionResponse", "data": { "method": "subscribe" } }"#;
    let msg: WsMessage = serde_json::from_str(txt).expect("parse");
    assert!(matches!(msg, WsMessage::SubscriptionResponse(_)));
}
