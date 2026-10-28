# Changelog

All notable changes to this project will be documented in this file. Follows
[Keep a Changelog](https://keepachangelog.com/) and
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed

- Hyperliquid reconciliation now filters account fills by symbol, keeps
  individual fills partial until open-order reconciliation confirms completion,
  aborts requotes on ambiguous cancellation failures, and closes positions with
  reduce-only IOC orders.
- `Venue::market_close` now reports unsupported adapters instead of submitting a
  zero-priced quote.
- Bagboy persists cumulative base and quote acquisition totals under its session
  state directory, restores them across restarts, and disables placements when
  state cannot be read or written.
- The root `run` launcher now starts the supported `tikr` application from
  configuration files and stores logs in an atomically created owner-only
  directory.

### Fixed

- Dependencies bumped for security advisories: `tokio` 1.54.1
  (RUSTSEC-2026-0386), `rustls` 0.23.46 (RUSTSEC-2026-0285, -0346, -0348,
  -0349), `rustls-webpki` 0.103.16 and `webpki-roots` 1.0.11 (RUSTSEC-2026-0381,
  -0382), `anyhow` 1.0.104 (RUSTSEC-2026-0190), `crossbeam-epoch` 0.9.21
  (RUSTSEC-2026-0204), `ruint` 1.21.1 (RUSTSEC-2026-0220) and `zstd-safe` 7.3.0
  / `zstd-sys` 2.1.2. RUSTSEC-2026-0391, -0398 and -0403 (zstd, via polars) and
  RUSTSEC-2026-0387 (crypto-bigint 0.5, via alloy-signer) have no fixed release
  reachable through polars or alloy yet and are ignored in `deny.toml` with the
  reasons.
- MEXC order mutations now require `TIKR_MEXC_ENABLE_MAINNET=1`, and every REST
  endpoint preserves venue rate-limit cooldowns from HTTP 418/429 responses.
- Bybit order-book throttling now returns `VenueError::RateLimited` with
  `Retry-After` cooldowns and bounded non-success response context.
- Post-fill quote failures now record their failure timestamp so side lockouts
  expire after `SIDE_FAILS_RESET_AFTER`.
