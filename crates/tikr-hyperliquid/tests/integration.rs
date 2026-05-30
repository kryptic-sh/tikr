//! Live integration tests against the Hyperliquid testnet.
//!
//! All `#[ignore]`-gated — run manually with:
//!
//! ```text
//! cargo test -p tikr-hyperliquid --test integration -- --ignored
//! ```

use futures::stream::StreamExt;
use std::time::Duration;
use tikr_core::{Asset, MarketEvent, MarketKind, Symbol, VenueId};
use tikr_hyperliquid::{Hyperliquid, HyperliquidConfig, HyperliquidEnv};
use tikr_venue::{Venue, VenueError};

fn testnet_symbol() -> Symbol {
    Symbol {
        base: Asset::new("BTC"),
        quote: Asset::new("USDC"),
        venue: VenueId::new("hyperliquid"),
        kind: MarketKind::Perp,
    }
}

#[ignore]
#[tokio::test]
async fn live_snapshot_testnet() {
    let venue = Hyperliquid::with_config(HyperliquidConfig {
        env: HyperliquidEnv::Testnet,
        ..Default::default()
    });
    let snap = venue.snapshot(&testnet_symbol()).await.expect("snapshot");
    assert!(!snap.bids.is_empty(), "expected at least one bid level");
    assert!(!snap.asks.is_empty(), "expected at least one ask level");
    // ts in ns and well past UNIX epoch
    assert!(snap.ts.0 > 1_700_000_000_000_000_000);
}

#[ignore]
#[tokio::test]
async fn live_subscribe_receives_first_event_testnet() {
    let venue = Hyperliquid::with_config(HyperliquidConfig {
        env: HyperliquidEnv::Testnet,
        heartbeat_ms: 0,
        ..Default::default()
    });
    let mut stream = venue.subscribe(&testnet_symbol()).await.expect("subscribe");
    let ev = tokio::time::timeout(Duration::from_secs(15), stream.next())
        .await
        .expect("timeout waiting for first event")
        .expect("stream ended unexpectedly");
    match ev {
        MarketEvent::BookUpdate { .. } | MarketEvent::Trade { .. } => {}
        other => panic!("unexpected first event: {other:?}"),
    }
}

#[ignore]
#[tokio::test]
async fn live_position_requires_user_address_testnet() {
    let venue = Hyperliquid::with_config(HyperliquidConfig {
        env: HyperliquidEnv::Testnet,
        user_address: None,
        ..Default::default()
    });
    let res = venue.position(&testnet_symbol()).await;
    assert!(matches!(res, Err(VenueError::Rejected { .. })));
}

#[ignore]
#[tokio::test]
async fn live_fills_since_requires_user_address_testnet() {
    let venue = Hyperliquid::with_config(HyperliquidConfig {
        env: HyperliquidEnv::Testnet,
        user_address: None,
        ..Default::default()
    });
    let res = venue.fills_since(&testnet_symbol(), 0).await;
    assert!(matches!(res, Err(VenueError::Rejected { .. })));
}
