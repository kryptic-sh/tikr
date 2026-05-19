# tikr-paper

> ⚠️ **STUB — Phase 3 scaffold.** Paper trading runner. No real orders sent —
> fills are simulated via
> [FillSim](https://github.com/kryptic-sh/tikr/issues/11) against the real
> venue's market activity. See
> [#23](https://github.com/kryptic-sh/tikr/issues/23) for the architecture.

## Status

| Component                                | Status                                                  |
| ---------------------------------------- | ------------------------------------------------------- |
| Paper runner                             | ✅ Phase 3                                              |
| Hyperliquid WS feed                      | ✅ Phase 3 (read-side; order placement is Phase 5)      |
| State snapshots to disk                  | ✅ Phase 3                                              |
| Cooperative shutdown                     | ✅ Phase 3                                              |
| Resume from snapshot (`run_with_resume`) | ✅ Phase 4 (aggregate-P&L only — see limitation)        |
| Risk-gate integration (`RiskGate`)       | ✅ Phase 4 (check before fill_sim, record_fill after)   |
| Multi-symbol coordination (`run_multi`)  | ✅ Phase 4                                              |
| Supervisor (`supervisor` bin)            | ✅ Phase 4 (subprocess respawn; no `--resume-from` yet) |
| Crash recovery / auto-restart            | ✅ Phase 4 (via supervisor)                             |
| Alerting (Slack / Discord / PagerDuty)   | ❌ #33                                                  |
| Real-time TUI                            | ❌ Phase 5 CLI                                          |

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

## Resume + multi-symbol + supervisor (Phase 4)

`run_with_resume(..., resume, risk_gate)` re-seeds the tracker from a prior
`PaperReport` and optionally layers a `tikr_risk::RiskGate` between the strategy
and the fill simulator. The gate's `check` runs **before** every
`fill_sim.on_action`; `record_fill(ts)` fires after every `tracker.apply` so the
rolling fills-per-minute window stays current.

`multi::run_multi(runs, shutdown)` joins N per-symbol runner futures
concurrently via `join_all` and returns a
`MultiPaperReport { per_symbol, sum }`.

`supervisor` is a binary that spawns
`cargo run -p tikr-paper --example run_paper` and respawns on non-zero exit,
bounded by `--max-restarts-per-hour` over a rolling 1-hour window:

```bash
cargo run -p tikr-paper --bin supervisor -- \
  --symbol BTC --strategy naive-grid --max-restarts-per-hour 5
```

## Known limitations (v0)

- **Resume seeds aggregate P&L only.** `PaperReport` carries `realized`, `fees`,
  `funding`, and counters — not the raw `Position { size, avg_entry }`. On
  resume, position size is reset to zero. **Operators must close all positions
  before restart**; otherwise post-resume unrealized P&L attribution is wrong.
  Position-state persistence is a future enhancement.
- **Supervisor restarts without state continuity.** The `run_paper` example
  doesn't yet accept `--resume-from`, so each respawned child starts cold.
  Snapshots still land in `./paper_state/` for post-mortem.
- **Strategy state is not persisted.** The `tikr_strategy::StrategyResume` trait
  declaration is in place with no-op defaults; no reference strategy opts in yet
  (NaiveGrid is stateless; A-S / GLFT warm back up).
- **Pre-`schema_version` snapshots (from #26) are not resumable.**
  `run_with_resume` hard-fails on `schema_version != 1`.
- **No alerting wiring.** That's #33.
- **No cross-symbol portfolio risk.** Each `run_multi` symbol gets its own
  independent `RiskGate`.
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
