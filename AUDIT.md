# Codebase Audit

Audit target: commit `2506fbb980462c9990650669b8ee268f82652737`

Audit date: 2026-07-22

Resolution status: all 11 verified findings were remediated, independently
reviewed, and covered by regression tests. This file is retained as the audit
record; the detailed finding write-ups were pruned once each was fixed. See the
referenced fix commits for the code and tests.

## Scope and method

The repository was divided into seven slices and reviewed by read-only audit
agents running `go://deepseek-v4-flash`:

1. `tikr-core` and `tikr-venue`
2. Binance, Bybit, Hyperliquid, and MEXC adapters
3. Backtest and fill-simulation engine
4. Strategy suite
5. Paper runner and risk gate
6. Application, recorder, scripts, and runtime configuration
7. CI, examples, documentation, and repository plumbing

Every reported candidate was then checked against the cited source and its
callers. This report excludes style-only concerns, intentional documented
trade-offs, impractical numeric limits, and claims that could not be reproduced
from the current code. Severity reflects realistic impact in this repository,
not the audit agent's original rating.

## Findings (all resolved)

| #   | Severity | Finding                                                             | Fix commit           |
| --- | -------- | ------------------------------------------------------------------- | -------------------- |
| 1   | Critical | Hyperliquid reconciliation applied every coin's fills to one symbol | `1cbe3c9`            |
| 2   | Critical | MEXC mainnet write gate was documented but not implemented          | `c5bda93`            |
| 3   | High     | Hyperliquid fill events always claimed the order was fully filled   | `1cbe3c9`, `122d863` |
| 4   | High     | Hyperliquid requote placed a replacement after any cancel failure   | `1cbe3c9`            |
| 5   | High     | Hyperliquid `market_close` could not submit a marketable close      | `1cbe3c9`            |
| 6   | High     | Bagboy hard accumulation caps reset on every process restart        | `90c64af`            |
| 7   | High     | MEXC rate-limit responses lost required backoff information         | `c5bda93`            |
| 8   | Medium   | Post-fill rejection lockouts lacked the configured time-based reset | `053c6fb`            |
| 9   | Medium   | The checked-in launcher referenced a nonexistent example            | `7ad5aca`            |
| 10  | Medium   | Bybit throttling was reported as a generic network failure          | `7ad5aca`            |
| 11  | Low      | Launcher logged trading activity to world-readable files            | `7ad5aca`            |

Finding #3 required both halves of the prescribed fix: `1cbe3c9` stopped the
fill mappings from claiming `is_full`, and `122d863` implemented an
authoritative `Hyperliquid::open_orders` so open-order reconciliation drops only
orders confirmed absent from the venue, rather than wiping local state against
the empty trait default.

## Rejected candidates

The following prominent agent reports were checked and excluded:

- Paper `QuoteId`s are not all nil: `QuoteId::default()` mints a UUID.
- A post-only bid at the best ask is marketable and should be rejected; the
  simulator's inclusive cross check is correct.
- Binance cancel handling already propagates typed HTTP failures through
  `read_json`; the claimed blanket `UnknownQuote` conversion was not present.
- MEXC `baseSizePrecision` and `quoteAmountPrecision` are documented as minimum
  quantity and minimum order amount respectively; those mappings are valid.
- Strategy partial-fill duplication is prevented on Binance and paper paths
  because the runner withholds strategy notification for partial fills. The
  Hyperliquid variant of this defect was tracked as finding #3 and is now
  resolved.
- Batch trait defaults are explicitly per-item result APIs, not atomic APIs.
- Missing serde derives, random identifier defaults, duplicate domain types,
  stale-frame UI hit testing, theoretical Decimal overflow, and future protocol
  changes were not demonstrated as current correctness or security defects.

## Coverage notes

The review covered all tracked Rust sources, manifests, tests, examples, runtime
TOML files, shell scripts, CI workflows, and project documentation. Generated
Git internals and untracked/ignored build output were excluded. No source code
was changed as part of the audit.
