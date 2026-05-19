# tikr-backtest data schema

Parquet, split files per event type. See
[issue #9](https://github.com/kryptic-sh/tikr/issues/9) for the locked
decisions.

## File naming

- `book_<SYMBOL>_<YYYY-MM-DD>.parquet` — L2 deltas
- `trades_<SYMBOL>_<YYYY-MM-DD>.parquet` — prints

Symbol is the base asset (e.g. `BTC`). Date is the UTC calendar date of the
first event in the file.

## Book delta schema (L2)

| Column  | Type      | Semantics                                                                                     |
| ------- | --------- | --------------------------------------------------------------------------------------------- |
| `ts_ns` | `u64`     | Exchange-provided event timestamp, nanoseconds since UNIX epoch                               |
| `side`  | `u8`      | `0` = bid, `1` = ask                                                                          |
| `price` | `decimal` | Price level (Decimal preserved; no float drift)                                               |
| `size`  | `decimal` | New size at this level. **`0` = delete level**                                                |
| `seq`   | `u64`     | Per-symbol monotonic sequence number; used by the replay engine for gap detection (issue #10) |

## Trade schema

| Column       | Type      | Semantics                                                           |
| ------------ | --------- | ------------------------------------------------------------------- |
| `ts_ns`      | `u64`     | Exchange-provided event timestamp                                   |
| `price`      | `decimal` | Trade price                                                         |
| `size`       | `decimal` | Trade size                                                          |
| `taker_side` | `u8`      | `0` = bid (buyer was taker), `1` = ask (seller was taker)           |
| `trade_id`   | `u64`     | Venue-assigned trade identifier; uniqueness within `(symbol, date)` |

## Compression

`zstd` by default (parked sub-question — revisit if read perf bottlenecks the
golden test in #15).

## Recording fresh data

```bash
cargo run -p tikr-backtest --bin record -- --symbol BTC --hours 24 --env mainnet --out ./data
```

Outputs per-flush parquet files into `--out` (default `./data`). See the
"Recorder output (v0)" section below for the file naming convention and v0
caveats. `--env testnet` selects the Hyperliquid testnet endpoint. `--hours 0`
runs until SIGINT.

## Example data

Example tiny parquet file checked in at `tests/data/` — will land with issue #15
(golden regression test).

## Recorder output (v0)

The recorder writes per-flush files matching
`book_<SYM>_<DATE>_<FLUSH-COUNTER>.parquet` (and `trades_*` for trades).
`<FLUSH-COUNTER>` is a 6-digit zero-padded monotonic counter within one recorder
process. Flushes happen every 1000 rows or every 60 seconds, whichever comes
first; SIGINT triggers a final flush before exit.

### v0 caveats

- **Within a single recorder run**, all `book_<SYM>_*` files use a globally
  monotonic `seq` column — `ParquetReplay`'s gap detection holds.
- **Mixing files from different recorder runs** in one `ParquetReplay` directory
  will trip the seq check (each run resets `seq` to 1). Clear the directory or
  use distinct dirs per run.
- **Each `BookUpdate` from Hyperliquid produces N rows** (one per bid level +
  one per ask level — the full top-N snapshot serialized as deltas). Wasteful
  but correct; replay reconstructs identically. Phase 4 cleanup target: emit
  only changed levels.
- **Prices + sizes stored as `f64`** for compatibility with the existing
  test-fixture / replay path. Phase 2.5+ migrates to true `Decimal` columns via
  the polars `dtype-decimal` cargo feature.
