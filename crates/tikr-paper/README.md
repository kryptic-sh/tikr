# tikr-paper

> ⚠️ **STUB — Phase 3 scaffold.** Paper trading runner. No real orders sent —
> fills are simulated via
> [FillSim](https://github.com/kryptic-sh/tikr/issues/11) against the real
> venue's market activity. See
> [#23](https://github.com/kryptic-sh/tikr/issues/23) for the architecture.

## Status

| Component                              | Status                                             |
| -------------------------------------- | -------------------------------------------------- |
| Paper runner                           | ✅ Phase 3                                         |
| Hyperliquid WS feed                    | ✅ Phase 3 (read-side; order placement is Phase 5) |
| State snapshots to disk                | ✅ Phase 3                                         |
| Cooperative shutdown                   | ✅ Phase 3                                         |
| Resume from snapshot                   | ❌ Phase 4 risk engine                             |
| Crash recovery / auto-restart          | ❌ Phase 4                                         |
| Multi-symbol coordination              | ❌ Phase 4                                         |
| Alerting (Slack / Discord / PagerDuty) | ❌ Phase 4                                         |
| Real-time TUI                          | ❌ Phase 5 CLI                                     |

## Prerequisites

- Rust toolchain (matches workspace pin)
- Network access to Hyperliquid (mainnet or testnet)
- Optional: `--user-address 0x...` if you want `Venue::position` / `fills_since`
  reads (not strictly needed for read-only book/trade subscribe)

## Quickstart

```bash
cargo run -p tikr-paper --example run_paper -- --symbol BTC --minutes 60
```

Flags:

- `--symbol <BASE>` — base asset, default `BTC`
- `--strategy <naive-grid|avellaneda-stoikov|glft>` — default `naive-grid`
- `--minutes <N>` — duration cap, default `60`. `0` = run until SIGINT
- `--env <mainnet|testnet>` — default `mainnet`
- `--user-address 0x...` — optional, for position / fills queries

Hit Ctrl-C anytime — the runner drains pending FillSim actions, writes a final
state snapshot, and exits cleanly.

## Output interpretation

`PaperReport` fields:

- `realized`: P&L locked in on closed positions (WACC cost basis)
- `unrealized`: mark-to-market against last observed book mid
- `fees`: cumulative fees paid (positive) or rebated (negative)
- `funding`: always 0 in Phase 3 (funding accrual is Phase 4)
- `net`: `realized + unrealized - fees + funding`
- `runtime_secs`: wall-clock duration
- `events_processed`: total `MarketEvent` count
- `fills_emitted`: total simulated `Fill` count from `FillSim`

State snapshots land in `./paper_state/<symbol>_<unix_secs>_<uuid_short>.json`
every 100 events (configurable via `RunnerConfig`).

Tracing logs go to stdout. Set `RUST_LOG=tikr_paper=info` to see fills as they
happen; `tikr_paper=debug` for snapshot writes; `tikr_hyperliquid=debug` for WS
reconnect events.

## Known limitations (v0)

- **No resume from snapshot.** A restart starts fresh; the snapshot file is for
  post-mortem analysis only.
- **No crash recovery.** A panic kills the process — operator restarts manually.
- **No multi-symbol coordination.** Run N processes (one per symbol) and
  aggregate yourself.
- **No alerting.** Stdout logs only.
- **No real orders.** Fills are simulated against real market activity via
  `FillSim` (issue [#11](https://github.com/kryptic-sh/tikr/issues/11)) — see
  the **optimistic fill bias** caveat in
  [`tikr-backtest/README.md`](../tikr-backtest/README.md).
- **`StrategyContext.recent_fills` and `StrategyContext.open_quotes`** are
  always empty in v0 (#26 limitation).
- **5-minute smoke test** lives at `tests/smoke.rs` and is `#[ignore]`-gated
  (run manually: `cargo test -p tikr-paper --test smoke -- --ignored`).

All limitations are addressed in Phase 4 (risk engine + supervisability) and
Phase 5 (live + multi-venue) per the
[roadmap](https://github.com/kryptic-sh/tikr/issues/1).

## License

MIT
