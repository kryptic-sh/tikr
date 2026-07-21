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

- MEXC order mutations now require `TIKR_MEXC_ENABLE_MAINNET=1`, and every REST
  endpoint preserves venue rate-limit cooldowns from HTTP 418/429 responses.
- Bybit order-book throttling now returns `VenueError::RateLimited` with
  `Retry-After` cooldowns and bounded non-success response context.
- Post-fill quote failures now record their failure timestamp so side lockouts
  expire after `SIDE_FAILS_RESET_AFTER`.
