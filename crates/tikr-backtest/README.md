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

## Golden regression

`cargo test -p tikr-backtest --test golden` runs the full Phase 1 stack against
a synthesized deterministic dataset.

**Phase 1 deviation:** the golden test uses a tiny in-test synthetic dataset (~5
events) rather than the spec's "1 hour of real Hyperliquid data" — we have no
recorder yet. Real data lands when the recorder bin ships.

### Updating the expected value

When you make an INTENTIONAL change that shifts golden P&L (e.g. strategy
parameter, fee model, fill semantics):

1. Run the test; note the new actual value from the assertion failure message.
2. Update the `expected_net` in `tests/golden.rs` AND the
   `// last updated: <today's date>` comment.
3. Re-run; should pass.
4. Mention the intentional shift in the commit message.

Silent drift on this test = silent regression. Never update the expected value
without a commit message line explaining why.

## License

MIT
