//! Phase 0 smoke test: stub constructs and reports its id.

use tikr_hyperliquid::Hyperliquid;
use tikr_venue::Venue;

#[test]
fn id_is_hyperliquid() {
    let v = Hyperliquid::new();
    assert_eq!(v.id(), "hyperliquid");
}

#[test]
fn default_constructs() {
    let v = Hyperliquid::default();
    assert_eq!(v.id(), "hyperliquid");
}
