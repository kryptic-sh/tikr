# tikr-backtest

> ⚠️ **STUB — Phase 1 scaffold.** Module skeletons in place; logic is `todo!()`.
> See [issue #13](https://github.com/kryptic-sh/tikr/issues/13).

> ⚠️ **Optimistic fill bias.** The v1 fill model uses **trade-through**: any
> market trade at or through our quote price counts as a fill of our quote. This
> is **optimistic** — real venues have queue position, and only orders ahead of
> ours fill first. P&L numbers from this backtest are an **upper bound**, not
> realistic expectations. Queue-position modeling is Phase 2 work.

## Modules

| Module                                 | Status | Issue |
| -------------------------------------- | ------ | ----- |
| `replay::Replay` + `ParquetReplay`     | stub   | #10   |
| `fill_sim::FillSim`                    | stub   | #11   |
| `pnl::PositionTracker` + `PnLReport`   | stub   | #12   |
| `runner::run`                          | stub   | #15   |
| `bin/record` (Hyperliquid WS recorder) | stub   | #9    |

## Data format

See [`SCHEMA.md`](SCHEMA.md). Parquet, split files per event type, L2 deltas +
trades.

## Phase 1 roadmap

#9 → #10 → #11 → #12 → #15. See
[issue #1](https://github.com/kryptic-sh/tikr/issues/1) for the full Phase 1
list.

## License

MIT
