# tikr-paper

> ⚠️ **STUB — Phase 3 scaffold.** Paper trading runner — drives a live `Venue`
> stream through Strategy + FillSim + PositionTracker. No real orders sent
> (paper only); fills simulated via FillSim per
> [#11](https://github.com/kryptic-sh/tikr/issues/11).

> ⚠️ **No supervisability in v0.** No resume from snapshot, no crash recovery,
> no multi-symbol coordination, no alerting. All Phase 4 risk-engine work. See
> [#23](https://github.com/kryptic-sh/tikr/issues/23) for the architecture
> decisions.

## What it does

Mirrors [`tikr_backtest::runner::run`](https://github.com/kryptic-sh/tikr) but
consumes a live [`Venue`] stream (via `tikr_hyperliquid::Hyperliquid` by
default) instead of replaying parquet. Outputs a [`PaperReport`] with
realized/unrealized/fees/funding/net plus runtime stats.

State snapshots written to `./paper_state/<run_id>.json` every 100 events by
default (configurable via `RunnerConfig`).

## Quickstart

End-to-end runnable example ships with
[#27](https://github.com/kryptic-sh/tikr/issues/27):
`cargo run -p tikr-paper --example run_paper -- --symbol BTC --minutes 60`.

## Deps

- `tikr-core`, `tikr-venue`, `tikr-strategy`, `tikr-hyperliquid`,
  `tikr-backtest`
- `tokio` (rt-multi-thread + macros + sync + signal + time)
- `tracing` + `tracing-subscriber` for structured logs
- `serde` + `serde_json` for state snapshots

## License

MIT
