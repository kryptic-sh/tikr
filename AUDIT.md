# Codebase Audit

Audit target: commit `2506fbb980462c9990650669b8ee268f82652737`

Audit date: 2026-07-22

Resolution status: all 11 verified findings were remediated and independently
reviewed on 2026-07-22. Regression coverage was added for each affected code
path; the original findings remain below as the audit record.

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

## Findings

### Critical — Hyperliquid reconciliation applies every coin's fills to one symbol

**Locations:**

- `crates/tikr-hyperliquid/src/lib.rs:324-332`
- `crates/tikr-hyperliquid/src/client.rs:61-79`
- `crates/tikr-paper/src/runner.rs:2738-2769`

`Hyperliquid::fills_since` discards its `symbol` argument. It requests the
account-wide `userFills` list and filters only by timestamp. `UserFillEntry`
contains the coin, but `user_fills_since` maps every entry into a `Fill` without
checking it.

The paper/live runner calls `venue.fills_since(&symbol, ...)` for one symbol and
applies every returned fill to that symbol's single `PositionTracker`. If an
account trades BTC and ETH, a BTC runner can therefore book ETH quantity, price,
fees, and side into its BTC position and P&L. Trade-ID deduplication prevents a
fill from being replayed twice within that runner; it does not correct the
cross-symbol attribution.

**Fix:** pass the requested symbol or coin into `user_fills_since`, filter
`entries` by `f.coin`, and test an account response containing multiple coins.

### Critical — MEXC mainnet write gate is documented but not implemented

**Locations:**

- `crates/tikr-mexc/src/lib.rs:12-14,34-43`
- `apps/tikr/src/bagboy.rs:45-65,323-325`

The MEXC crate says mainnet writes require `TIKR_MEXC_ENABLE_MAINNET=1`, but no
code reads that variable. `MexcClient::new` always selects
`https://api.mexc.com`, and every write method delegates directly to the REST
wrapper. Bagboy creates this client and places live buy orders whenever MEXC
credentials and an enabled config are present.

This defeats the explicit safety convention enforced by the Binance and
Hyperliquid adapters. An operator can reasonably rely on the documented gate and
still submit real-money MEXC orders without enabling it.

**Fix:** store a `mainnet_writes_enabled` flag when constructing `MexcClient`
and reject every write method before network I/O unless the environment variable
is exactly `1`. Add tests covering enabled and disabled states.

### High — Hyperliquid fill events always claim the order was fully filled

**Locations:**

- `crates/tikr-hyperliquid/src/exchange.rs:939-962`
- `crates/tikr-hyperliquid/src/mapping.rs:159-180`
- `crates/tikr-paper/src/runner.rs:2310-2422`

Both Hyperliquid fill mappings set `is_full: true` for every individual fill.
The comments acknowledge that the payload does not expose remaining order size.
Hyperliquid orders can fill partially, so absence of that field is not evidence
that the resting order was consumed.

The live runner trusts `is_full`: it applies the fill, and for a claimed full
fill it removes the quote from `FillSim` and notifies the strategy. Strategies
may then place a replacement while the original remainder still rests on the
venue. Repeated partial fills can create duplicate same-side exposure and make
local open-order state diverge from venue state.

**Fix:** determine completion from an authoritative open-order/order-status
source before setting `is_full`. Until then, map these events as partial and let
open-order reconciliation remove orders confirmed absent. Add a partial-fill
integration test that leaves the original order tracked.

### High — Hyperliquid requote places a replacement after any cancel failure

**Location:** `crates/tikr-hyperliquid/src/lib.rs:268-289`

`Hyperliquid::requote` logs every cancellation error and proceeds to place the
new order. A timeout, 429, server error, or authentication failure does not show
that the old order disappeared. The old order can remain live while the method
successfully creates another one; the method then returns `Ok(())` and the
caller has no identifier for the replacement.

The stated risk-gate backstop limits fills, not the number or notional of
simultaneously resting duplicate orders. Recurring requotes during a cancel
outage can accumulate exposure.

**Fix:** place the replacement only after a successful or explicitly
idempotent/unknown-order cancellation. Propagate ambiguous failures and retain
the old local order state for reconciliation.

### High — Hyperliquid `market_close` cannot submit a marketable close

**Locations:**

- `crates/tikr-venue/src/lib.rs:233-245`
- `crates/tikr-hyperliquid/src/lib.rs:248-265`
- `crates/tikr-hyperliquid/src/exchange.rs:301-337`

Hyperliquid does not override `Venue::market_close`, so it inherits an IOC
intent priced at zero. Its `quote` implementation ignores the intent's
`TimeInForce` and `QuoteKind`; `ExchangeClient::place_order` always submits an
`Alo` post-only limit with `reduce_only` set to false.

A zero-priced bid rests or is rejected rather than covering a short. A
zero-priced ask is invalid or marketable and is rejected by the post-only rule.
Even if accepted, the order is not reduce-only. The trait method can therefore
return an order-placement success without closing the requested position, or
fail outright, violating its contract.

**Fix:** implement `market_close` in the Hyperliquid adapter using a marketable
IOC limit and `reduce_only: true`; do not route it through the post-only quote
path. The trait default should also choose a side-appropriate extreme price or
return `Unsupported` rather than pretending all adapters can emulate a market
close.

### High — Bagboy hard accumulation caps reset on every process restart

**Locations:**

- `apps/tikr/src/config.rs:46-53`
- `apps/tikr/src/bagboy.rs:94-109,127-175`

`max_total_usdt` and `max_total_base` are documented as hard cumulative caps,
but their counters always initialize to zero. Startup reads the existing base
balance only into `last_seen_base`; it does not seed `total_base_acquired`, load
persisted counters, or reconstruct prior spend. Shutdown deliberately leaves
resting orders intact, making restart continuity an expected path.

After every restart Bagboy receives a fresh full cap despite retaining the
previously acquired asset and open orders. Repeating restarts can exceed either
configured limit by an arbitrary multiple. A startup balance-read failure is
worse: the first successful poll treats the complete existing balance as a new
fill while `last_known_bid` is still zero, adding base but recording zero spend.

**Fix:** persist and restore cumulative acquisition/spend state, or define the
base cap against authoritative current holdings and reconstruct quote spend from
trade history. Do not begin fill-delta accounting until both an initial balance
and a valid book price have been established.

### High — MEXC rate-limit responses lose required backoff information

**Locations:**

- `crates/tikr-mexc/src/spot.rs:132-239,282-298,361-368`
- `crates/tikr-mexc/src/spot.rs:99-106`

Only `place_limit_order` checks HTTP 418/429. Cancel, cancel-all, book ticker,
balance, open-orders, and exchange-info calls parse JSON without checking the
status. A JSON 429 becomes a generic `Rejected`; a non-JSON 429 becomes
`Internal`. Neither carries `retry_after_ms`, so callers cannot honor the
venue's cooldown and may continue polling during a ban.

Even the placement path hard-codes one second and ignores `Retry-After`. MEXC's
API documentation states that 418/429 responses carry the required delay and
that repeated violations escalate IP bans from minutes to days.

**Fix:** centralize response handling, inspect status before parsing, map
418/429 to `VenueError::RateLimited`, and parse `Retry-After` for every
endpoint. Map other non-2xx responses with status and bounded body context.

### Medium — Post-fill rejection lockouts lack the configured time-based reset

**Locations:**

- `crates/tikr-paper/src/runner.rs:1725-1737,1819-1825`
- `crates/tikr-paper/src/runner.rs:3322-3356`

The main dispatch path timestamps non-transient quote failures in
`side_fails_last`, allowing the side-failure lockout to clear after
`SIDE_FAILS_RESET_AFTER`. The post-fill batch-quote path increments the same
`side_fails` counters but never updates `side_fails_last`.

If failures accumulate through post-fill refills, the side reaches
`MAX_FAILS_PER_SIDE`; later main-loop quotes are skipped before they can produce
a timestamped failure or success. The intended timer cannot clear an absent
entry, so the side remains blocked until another independent reset such as a
fill or cancel-all occurs.

**Fix:** update `side_fails_last` beside the post-fill counter increment, or
encapsulate failure accounting in one helper used by both dispatch paths.

### Medium — The checked-in launcher references a nonexistent example

**Location:** `run:26-31,55-60`

The launcher builds and runs `tikr-binance` example `run_perp`, but the package
contains only `examples/run_spot.rs`. Running `./run` fails before launching a
bot because Cargo cannot find the requested example. Its arguments are futures
and layered-grid specific, so substituting `run_spot` is not equivalent.

**Fix:** point the script at the supported `tikr` application/config flow, or
restore a maintained futures example with the expected CLI. Add a CI smoke check
that resolves the launch target.

### Medium — Bybit throttling is reported as a generic network failure

**Location:** `crates/tikr-bybit/src/rest.rs:65-79`

The snapshot endpoint maps every non-success status, including HTTP 429, to
`VenueError::Network`. Code that handles `VenueError::RateLimited` cannot apply
a server-directed cooldown and may immediately retry. The current Bybit adapter
is paper/data-only, limiting present impact, but the error contract is already
wrong for consumers.

**Fix:** map 418/429 to `RateLimited`, parse `Retry-After`, and preserve status
and bounded response context for other failures.

### Low — Launcher logs trading activity to normally world-readable files

**Location:** `run:9,53-61`

The launcher redirects each process into a predictable `/tmp/tikr_paper_*.log`
path without setting a restrictive umask. On typical `022` systems, the files
are mode `0644`. Logs include fills, positions, and P&L, exposing trading
activity to other local users; predictable names also permit symlink-based file
clobbering when an attacker can prepare entries in shared `/tmp`.

**Fix:** use a private state/log directory created with mode `0700` and open
logs with owner-only permissions. Avoid predictable shared-temporary paths.

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
  defect remains for Hyperliquid because that adapter incorrectly marks every
  fill full.
- Batch trait defaults are explicitly per-item result APIs, not atomic APIs.
- Missing serde derives, random identifier defaults, duplicate domain types,
  stale-frame UI hit testing, theoretical Decimal overflow, and future protocol
  changes were not demonstrated as current correctness or security defects.

## Coverage notes

The review covered all tracked Rust sources, manifests, tests, examples, runtime
TOML files, shell scripts, CI workflows, and project documentation. Generated
Git internals and untracked/ignored build output were excluded. No source code
was changed as part of the audit.
