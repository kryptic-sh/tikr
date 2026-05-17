# tikr-hyperliquid

> ⚠️ **STUB — Phase 0 only.** All [`Venue`] trait methods are `todo!()`. This
> crate exists to prove the trait abstraction holds for an on-chain orderbook
> venue; real network code lands in Phase 1+.

Hyperliquid on-chain orderbook adapter for the
[tikr](https://github.com/kryptic-sh/tikr) market-making engine.

## Status

| Method                                             | Phase                      |
| -------------------------------------------------- | -------------------------- |
| `id()`                                             | ✅ Phase 0                 |
| `snapshot`, `subscribe`, `position`, `fills_since` | 🔜 Phase 1 — market data   |
| `quote`, `requote`, `cancel`, `cancel_all`         | 🔜 Phase 3 — paper trading |

## References

- [Hyperliquid HTTP API](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint)
- [Hyperliquid WebSocket API](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket)
- [Hyperliquid Rust SDK](https://github.com/hyperliquid-dex/hyperliquid-rust-sdk)
  — possible reference for Phase 1 wiring

## License

MIT
