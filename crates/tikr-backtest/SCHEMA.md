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
cargo run -p tikr-backtest --bin record -- --symbol BTC --hours 24
```

Outputs the two parquet files for that day into `./data/` by default; override
with `--out`.

> **Note:** the recorder bin is currently a stub (`todo!()` body); real impl
> ships with issue #9 close-out.

## Example data

Example tiny parquet file checked in at `tests/data/` — will land with issue #15
(golden regression test).
