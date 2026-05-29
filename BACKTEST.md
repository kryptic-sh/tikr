# Backtesting

How to record market data, replay it through a strategy, and read the results —
plus what the simulator models and where it still approximates.

## Pipeline

```
record_binance  ──►  *.parquet         ──►  backtest                           ──►  PaperReport (JSON)
(live WS capture)    book_/trades_/mark_     ParquetReplay → FillSim → PnL        + optional equity CSV
```

1. **Record** (`record_binance`) — capture Binance depth + trades (+ mark price
   on futures) to per-symbol parquet shards.
2. **Replay** (`ParquetReplay`) — load + time-sort the shards into one
   deterministic `MarketEvent` stream.
3. **Simulate** (`FillSim` + `PositionTracker`) — match the strategy's orders
   against the stream under a trade-through fill model, accrue P&L.
4. **Report** — emit a `PaperReport` JSON; optionally a per-tick equity CSV.

Backtest is **paper-mode**: write-side venue calls are recorded but never
dispatched; fills come from `FillSim`, not a live exchange.

## 1. Recording data

```bash
cargo run --release -p tikr-backtest --bin record_binance -- \
  --env futures-mainnet \
  --symbols SOLUSDT,BTCUSDT,DOGEUSDT \
  --hours 72 \
  --base-dir ./data
```

| flag         | default           | meaning                                                |
| ------------ | ----------------- | ------------------------------------------------------ |
| `--env`      | `futures-mainnet` | `spot-{testnet,mainnet}` / `futures-{testnet,mainnet}` |
| `--symbols`  | `BTCUSDT`         | comma-separated; each gets its own task + output dir   |
| `--hours`    | `1`               | recording length; `0` = until SIGINT                   |
| `--base-dir` | `./data`          | output root                                            |
| `--label`    | `{hours}h`        | second path segment (e.g. `72h`, `unlimited`)          |

Output layout (one dir per symbol):

```
{base-dir}/{label}/{SYMBOL}/
  book_<BASE>_<stamp>.parquet     # depth snapshots:  ts_ns, side, price, size, seq
  trades_<BASE>_<stamp>.parquet   # public trades:    ts_ns, price, size, taker_side, trade_id
  mark_<BASE>_<stamp>.parquet     # mark + funding:   ts_ns, mark_price, funding_rate, next_funding_ts_ns
```

`mark_` shards are **futures-only** (spot has no mark price). They carry the
mark price _and_ the funding rate, which the replay uses for realistic marking
and funding (see Realism model).

## 2. Running a backtest

`backtest` points at **one symbol's** data dir and runs **one strategy**.
File discovery is by `book_<BASE>_` / `trades_<BASE>_` / `mark_<BASE>_` prefix,
where `<BASE>` is the symbol minus its quote suffix (`SOLUSDT` → `SOL`).

### Quickstart

```bash
# Optimistic (infinite capital, no leverage) — sanity check only
cargo run --release -p tikr-paper --bin backtest -- \
  --data-dir data/72h/SOLUSDT --symbol SOLUSDT \
  --strategy tide --tr-step-bps 20 --tr-grid-levels 1
```

### Realistic account (recommended)

Model an actual wallet: balance-derived order size, a position cap, a
buying-power margin reject, and forced liquidation — mirroring the live config
(`config/wave.usdc.majors.toml`: $600 @ 5×, 1%/order, 100% cap).

```bash
cargo run --release -p tikr-paper --bin backtest -- \
  --data-dir data/72h/SOLUSDT --symbol SOLUSDT \
  --strategy wave --wv-grid-levels 10 --wv-refill-threshold 5 --wv-step-bps 5 \
  --maker-bps 0 --taker-bps 5 \
  --initial-balance 600 --leverage 5 \
  --order-balance-pct 1 --max-position-pct 100 \
  --equity-csv /tmp/wave_equity.csv
```

### Flags

**Data / strategy**

| flag             | default              | meaning                                                                                              |
| ---------------- | -------------------- | ---------------------------------------------------------------------------------------------------- |
| `--data-dir`     | `./data`             | per-symbol parquet dir                                                                               |
| `--symbol`       | `BTCUSDT`            | Binance symbol; base/quote split by 4-char suffix                                                    |
| `--strategy`     | `avellaneda-stoikov` | `tide`/`td`, `wave`/`wv`, `top-of-book`/`tob`, `micro-price`/`mp`, `glft`, `avellaneda-stoikov`/`as` |
| `--heartbeat-ms` | `1000`               | synthetic heartbeat cadence during quiet stretches                                                   |

**Fees / execution realism**

| flag                           | default | meaning                                           |
| ------------------------------ | ------- | ------------------------------------------------- |
| `--maker-bps`                  | `2`     | maker fee bps (negative = rebate)                 |
| `--taker-bps`                  | `5`     | taker fee bps                                     |
| `--submit-latency-ms`          | `0`     | fixed submit/cancel latency                       |
| `--submit-latency-jitter-ms`   | `0`     | mean exponential latency jitter (tail = spikes)   |
| `--silent-cancel-rate-per-min` | `0.0`   | venue silently drops a resting quote at this rate |

**Account / risk** (the realism knobs)

| flag                      | default | meaning                                                                                      |
| ------------------------- | ------- | -------------------------------------------------------------------------------------------- |
| `--initial-balance`       | `0`     | wallet balance (USDT). `0` = infinite capital, no margin check                               |
| `--leverage`              | `0`     | isolated leverage. `>0` enables liquidation + margin cap                                     |
| `--maint-margin-rate`     | `0.005` | maintenance-margin fraction for the liquidation trigger                                      |
| `--order-balance-pct`     | `0`     | per-order notional = balance × this%. `0` = fixed strat notional                             |
| `--max-position-pct`      | `0`     | position cap = balance × this%. `0` = uncapped (or strat cap)                                |
| `--funding-bps`           | `0`     | flat funding rate/interval as a fraction (recorded rate wins if a `mark_` series is present) |
| `--funding-interval-secs` | `28800` | funding interval (Binance USD-M = 8h)                                                        |

**Output**

| flag                        | default            | meaning                                               |
| --------------------------- | ------------------ | ----------------------------------------------------- |
| `--equity-csv`              | _(none)_           | write a running equity curve                          |
| `--snapshot-every-n-events` | `0`                | curve cadence (auto-set to 10000 with `--equity-csv`) |
| `--state-dir`               | `./state/backtest` | snapshot dir (unused for one-shot runs)               |

**Strategy params** — `tide`: `--tr-grid-levels`, `--tr-step-bps`,
`--tr-notional`, `--tr-step-size`, `--tr-min-notional`. `wave`:
`--wv-grid-levels`, `--wv-refill-threshold`, `--wv-step-bps`, `--wv-notional`,
`--wv-step-size`, `--wv-min-notional`, `--wv-max-position-usdt`. (A/S, GLFT,
TOB, MicroPrice have their own `--spread-bps`, `--gamma`, `--tick-size`,
`--max-skew-ticks`, etc.)

When `--initial-balance` + `--order-balance-pct` are set, the per-order notional
is computed from balance and **overrides** the strategy's fixed notional arg;
likewise `--max-position-pct` overrides the strategy cap. Both apply from the
first event.

## 3. Output

A `PaperReport` JSON on stdout. Key fields:

| field                                  | meaning                                                |
| -------------------------------------- | ------------------------------------------------------ | -------- | ---------------------------------------- |
| `net`                                  | `realized + unrealized − fees + funding`               |
| `realized`                             | closed-round-trip P&L (gross of fees)                  |
| `unrealized`                           | open position marked at last mark (or mid)             |
| `fees`                                 | total fees paid (negative = net rebate)                |
| `funding`                              | funding accrued on open inventory                      |
| `fills_emitted`                        | total fills                                            |
| `buy_volume_usdt` / `sell_volume_usdt` | gross traded notional per side                         |
| `peak_position_usdt`                   | \*\*max `                                              | position | × mark` over the run\*\* — the risk tell |
| `liquidations`                         | forced-liquidation count (`0` unless `--leverage > 0`) |
| `sim_duration_secs`                    | data-time span (not wall-clock)                        |

**Equity CSV** (`--equity-csv`) — one row per snapshot tick:
`ts_ns,sim_secs,fills,pos_size,realized,unrealized,fees,funding,net`. This is
how you see the _path_ — drawdowns, inventory build, and liquidation drops —
that the single end-of-run `net` hides.

## 4. Realism model

What `FillSim` + the runner simulate:

- **Trade-through fills with queue priority** — a resting maker order fills only
  when a public trade prints through its price, and only after the queue resting
  ahead of it (snapshotted at placement) drains. Cancels ahead are modelled as
  proportional queue shrink.
- **Post-only / IOC / FOK** — post-only crossing the touch is rejected; IOC
  walks the book level-by-level with a depth-weighted average price + partial
  fills.
- **Fees** — maker (signed; negative = rebate) + taker bps.
- **Latency + jitter** — fixed base latency, plus an optional exponential jitter
  whose tail models spikes; jitter reorders the pending queue, exercising
  cancel/replace races. Seeded → reproducible.
- **Funding** — continuous accrual on open inventory. Uses the **recorded
  per-interval funding rate** from the `mark_` series when present, else the
  flat `--funding-bps`.
- **Mark price** — unrealized P&L, funding, and the liquidation trigger mark
  against the **recorded mark price** (`mark_` series) when present, else the
  book mid.
- **Forced liquidation** — when `--leverage > 0`, an isolated-margin model
  force-closes the position if the mark breaches the liquidation price
  (`entry × (1 − 1/lev + mmr)` for a long). Counts in `liquidations`.
- **Buying-power margin reject** — `balance × leverage` caps total position
  notional (synthetic Binance `-2019`); orders that would breach it are
  rejected. Stops a strategy from holding inventory it can't fund.
- **Position cap** — `--max-position-pct` (or a strategy's own cap) suppresses
  adds once `|position| > cap` (soft cap — see caveats).
- **Silent venue cancels** — optional random drop of resting quotes.

### Why the account model matters (worked example)

tide `step20/lvl1` vs wave `step5/lvl10/rt5` on the same SOL 72h replay, across
three realism levels:

| account model                           | tide net | tide peak | wave net | wave peak |
| --------------------------------------- | -------- | --------- | -------- | --------- |
| infinite capital (no balance)           | +$3,672  | $156,621  | +$2.91   | $262      |
| $600 @ 5×, fixed $10, margin cap only   | +$116    | $3,170    | +$2.91   | $262      |
| $600 @ 5×, full config sizing (1%/100%) | +$18.26  | $715      | +$1.40   | $157      |

tide's headline +$3,672 was ~99.5% **imaginary leverage** — it accumulated a
$156k long from thousands of ~$9 dip-buys with no cap (the lvl-1 grid refills
its bid into a falling market; a rising price later sells it back). Once the
account can only fund $600, the same strategy nets +$18.26 with a bounded,
survivable $715 peak. **Always run with `--initial-balance` + `--leverage`; the
uncapped net is meaningless.**

## 5. Caveats / remaining approximations

- `--max-position-pct` is a **soft** cap (suppresses new adds); a position can
  drift modestly past it via mark appreciation + in-flight fills.
- Funding is **continuous-prorated**, not discrete-at-settlement.
- The buying-power cap is **static** (`balance × leverage` at start); it does
  not shrink as the balance draws down. The liquidation model handles the
  blow-up case separately.
- A maker fill needs a **trade print** through its price; the book gapping
  through a resting order with no print is not filled (small effect with full
  `@trade` data).
- No market impact — the strategy's own orders don't move the replayed book.
- Older snapshots (pre-`mark_`) have no mark/funding data → mark falls back to
  mid, funding to the flat `--funding-bps`.
