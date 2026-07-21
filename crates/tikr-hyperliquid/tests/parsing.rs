//! Fixture-driven unit tests for [`tikr_hyperliquid::messages`] +
//! [`tikr_hyperliquid::mapping`]. No network, no async runtime.

use std::fs;
use tikr_core::{Asset, Decimal, MarketEvent, MarketKind, QuoteId, Side, Symbol, Uuid, VenueId};
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

#[test]
fn user_fill_is_full_is_false() {
    let txt = load("user_fills.json");
    let entries: Vec<UserFillEntry> = serde_json::from_str(&txt).unwrap();
    for entry in &entries {
        let fill = fill_from_user_fill(entry);
        assert!(
            !fill.is_full,
            "userFill should not claim is_full (Hyperliquid does not expose remaining size)"
        );
    }
}

#[test]
fn maps_user_fill_mixed_coin_filter() {
    let txt = load("user_fills.json");
    let entries: Vec<UserFillEntry> = serde_json::from_str(&txt).unwrap();
    // Fixture has: entries[0] = BTC, entries[1] = ETH
    let btc_fills: Vec<&UserFillEntry> = entries.iter().filter(|f| f.coin == "BTC").collect();
    assert_eq!(btc_fills.len(), 1);
    assert_eq!(btc_fills[0].oid, 12345);

    let eth_fills: Vec<&UserFillEntry> = entries.iter().filter(|f| f.coin == "ETH").collect();
    assert_eq!(eth_fills.len(), 1);
    assert_eq!(eth_fills[0].oid, 12346);

    // No SOL fills exist
    let sol_fills: Vec<&UserFillEntry> = entries.iter().filter(|f| f.coin == "SOL").collect();
    assert!(sol_fills.is_empty());
}

// ---------------------------------------------------------------------------
// openOrders
// ---------------------------------------------------------------------------

#[test]
fn parses_open_orders_response() {
    let txt = load("open_orders.json");
    let entries: Vec<OpenOrderEntry> = serde_json::from_str(&txt).expect("parse");
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].coin, "BTC");
    assert_eq!(entries[0].oid, 12345);
    assert_eq!(entries[0].limit_px, "59000.0");
}

#[test]
fn maps_open_order_fields() {
    let txt = load("open_orders.json");
    let entries: Vec<OpenOrderEntry> = serde_json::from_str(&txt).unwrap();
    let sym = btc_symbol();

    // entries[0]: BTC bid, remaining 0.05 @ 59000.
    let bid = open_order_from_entry(&sym, &entries[0]);
    assert_eq!(bid.side, Side::Bid);
    assert_eq!(bid.price.0, Decimal::from_str_exact("59000.0").unwrap());
    assert_eq!(bid.size.0, Decimal::from_str_exact("0.05").unwrap());
    assert_eq!(bid.symbol, sym);

    // entries[1]: BTC ask.
    let ask = open_order_from_entry(&sym, &entries[1]);
    assert_eq!(ask.side, Side::Ask);
    assert_eq!(ask.price.0, Decimal::from_str_exact("61000.0").unwrap());
}

#[test]
fn open_order_id_derives_from_oid() {
    let txt = load("open_orders.json");
    let entries: Vec<OpenOrderEntry> = serde_json::from_str(&txt).unwrap();
    let sym = btc_symbol();

    // The id MUST equal quote_id_from_oid(oid) so the runner's reconciliation
    // can match resting orders against locally-tracked quotes.
    let order = open_order_from_entry(&sym, &entries[0]);
    let expected = QuoteId::from_uuid(Uuid::from_u128(12345u128));
    assert_eq!(order.id, expected);

    // Distinct oid → distinct id.
    let other = open_order_from_entry(&sym, &entries[1]);
    assert_ne!(order.id, other.id);
}

#[test]
fn open_order_id_matches_fill_quote_id_for_same_oid() {
    // A resting order and a fill sharing the same oid must map to the same
    // QuoteId — otherwise reconciliation would treat a still-resting order as
    // a ghost and wipe it. user_fills entries[0] and open_orders entries[0]
    // both carry oid 12345.
    let of_txt = load("open_orders.json");
    let of_entries: Vec<OpenOrderEntry> = serde_json::from_str(&of_txt).unwrap();
    let uf_txt = load("user_fills.json");
    let uf_entries: Vec<UserFillEntry> = serde_json::from_str(&uf_txt).unwrap();

    assert_eq!(of_entries[0].oid, uf_entries[0].oid, "fixtures share oid");
    let order = open_order_from_entry(&btc_symbol(), &of_entries[0]);
    let fill = fill_from_user_fill(&uf_entries[0]);
    assert_eq!(order.id, fill.quote_id);
}

#[test]
fn user_fill_maps_trade_id_for_dedup() {
    let txt = load("user_fills.json");
    let entries: Vec<UserFillEntry> = serde_json::from_str(&txt).unwrap();
    let fill = fill_from_user_fill(&entries[0]);
    assert_eq!(fill.trade_id, Some(67890));
    let fill2 = fill_from_user_fill(&entries[1]);
    assert_eq!(fill2.trade_id, Some(67891));
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
