# tikr

> ## ⚠️ READ THIS FIRST — FINANCIAL RISK WARNING ⚠️
>
> **This is experimental software for educational and personal research use
> only. It is NOT financial advice and NOT a production trading system.**
>
> - **You can lose ALL of your money.** Market making, perpetual futures, and
>   any leveraged trading carries unlimited downside risk including liquidation
>   and negative balance.
> - **Bugs in this code WILL lose you money** if you run it with real funds.
>   This is pre-alpha, unaudited, written by hobbyists with no fiduciary
>   relationship to you.
> - **You are 100% responsible** for any losses, regulatory consequences, tax
>   obligations, exchange TOS violations, and account actions resulting from
>   running this bot. The authors accept ZERO liability.
> - **Test on testnet only** until you fully understand the code. Even then,
>   start with the smallest possible position sizes if you ever go live.
> - **Manipulation, wash trading, and other prohibited strategies are NOT legal
>   on DEXs either** — see the Mango Markets case. Don't use this for anything
>   that would violate your jurisdiction's market-abuse laws or your exchange's
>   terms of service.
> - **Run only on accounts you can afford to zero out.** Never trade with money
>   you need for rent, debt, dependents, or anything else.
>
> By cloning, building, or running this software you acknowledge that you have
> read this warning and that any financial loss is entirely your own
> responsibility. See [LICENSE](LICENSE) (MIT — "AS IS, WITHOUT WARRANTY OF ANY
> KIND").

Modular, venue-agnostic market-making engine in Rust. Pre-alpha — parquet
backtest engine, a multi-strategy library, a live Binance USD-M / Spot adapter,
and a config-driven multi-bot orchestrator (`tikr`). Unaudited; see the risk
warning above before running it with real funds.

[![CI](https://github.com/kryptic-sh/tikr/actions/workflows/ci.yml/badge.svg)](https://github.com/kryptic-sh/tikr/actions/workflows/ci.yml)
[![release](https://img.shields.io/github/v/release/kryptic-sh/tikr)](https://github.com/kryptic-sh/tikr/releases)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> **Pre-alpha, under active development.** Live trading paths exist and are
> unaudited — treat every release as experimental. Watch
> [Roadmap & exit criteria](https://github.com/kryptic-sh/tikr/issues/1) for
> progress.

## Why?

Most open-source MM bots (including
[mxaddict/mmaker](https://github.com/mxaddict/mmaker), 2017) fail on the same
things: inventory-blind quoting, broken post-only handling, no kill switches, no
backtest-first discipline. `tikr` is the do-over — built backtest-first,
venue-agnostic, with risk limits before live capital.

## Crates

| Crate              | Role                                                                                       |
| ------------------ | ------------------------------------------------------------------------------------------ |
| `tikr-core`        | Vocabulary types: `Price`, `Size`, `Symbol`, `Position`, `Snapshot`, `MarketEvent`, `Fill` |
| `tikr-venue`       | `Venue` trait — abstracts over CEX orderbooks, DEX orderbooks, AMMs via quote-intent model |
| `tikr-strategy`    | `Strategy` trait + impls (Wave, Tide, Tidal, Hydra, Ratchet, GLFT, AvellanedaStoikov, …)   |
| `tikr-hyperliquid` | Hyperliquid perp adapter                                                                   |
| `tikr-binance`     | Binance Spot + USD-M Futures adapter (HMAC + Ed25519 auth, WS-API session.logon)           |
| `tikr-bybit`       | Bybit adapter                                                                              |
| `tikr-mexc`        | MEXC adapter                                                                               |
| `tikr-backtest`    | Parquet replay engine + FillSim (queue-priority + cancel modeling) — library only          |
| `tikr-record`      | Market-data recorder: Binance WS depth + trades → parquet (`record_binance` bin)           |
| `tikr-paper`       | Paper-trading runner (live feed + simulated fills) + `compare` strategy sweep              |
| `tikr-risk`        | Risk engine — pre-trade limits, drawdown gates, alerting                                   |

Crates land as phases ship. See
[roadmap](https://github.com/kryptic-sh/tikr/issues/1).

## Status

Pre-alpha and unaudited. Live adapters and the multi-bot orchestrator work, but
the engine is still being tuned. **Run only with capital you can afford to
lose.**

## Install

```bash
# build from source
git clone https://github.com/kryptic-sh/tikr.git
cd tikr
cargo build --release --workspace
```

Binaries land in `./target/release/`. Key entry points:

| Bin                  | Crate        | Purpose                                                    |
| -------------------- | ------------ | ---------------------------------------------------------- |
| `tikr`               | apps/tikr    | Multi-bot live/paper orchestrator + TUI (main entry point) |
| `record_binance`     | tikr-record  | Record Binance market data (depth + aggTrade) to parquet   |
| `compare`            | tikr-paper   | Backtest a preset sweep on parquet; table + optional JSON  |
| `run_spot` (example) | tikr-binance | Live Binance Spot runner (testnet/mainnet)                 |

## Development

```bash
git clone git@github.com:kryptic-sh/tikr.git
cd tikr
rustup toolchain install stable
cargo test --workspace
```

## Usage

### Record market data to parquet

```bash
# Binance Futures USD-M, 1 hour, ./data
cargo run --release --bin record_binance -- \
  --env futures-mainnet --symbols BTCUSDT --hours 1 --out ./data
```

Recorder writes `book_<BASE>_<DATE>_<NNNNNN>.parquet` +
`trades_<BASE>_<DATE>_<NNNNNN>.parquet` per flush (1000 rows or 60 s).

### Backtest strategies against recorded data

`compare` is the single backtest entry point: it sweeps a set of strategy
presets over the same recorded events (apples-to-apples) and prints a `fills`,
`fills/min`, `realized`, `fees`, `NET`, `$/fill` table. Pass single-element
sweep lists to backtest one configuration, and `--report-json <path>` to dump
the per-preset `PaperReport` for downstream tooling.

```bash
# Full preset sweep
cargo run --release --bin compare -- \
  --data-dir ./data --symbol BTCUSDC \
  --maker-bps 2 --taker-bps 5

# Single Wave config → machine-readable report
cargo run --release --bin compare -- \
  --data-dir ./data --symbol BTCUSDC \
  --wave-step-bps-list 5 --wave-inner-steps-list 2 --wave-grid-levels-list 4 \
  --report-json ./wave.json
```

Venue filters (price tick, lot step, `min_notional`) auto-detect from Binance
`exchangeInfo` by default (`--no-autodetect-filters` +
`--tick-size`/`--step-size` for offline runs; `--venue-env` picks the env).
Account sizing mirrors the live bot: `--sim-initial-balance` ×
`--sim-order-balance-pct` per order, a `--sim-max-position-pct` wallet cap, and
`--leverage` for the exchange margin backstop. Latency is probed from the live
API by default (10 pings → mean latency + stddev jitter; `--no-measure-latency`
falls back to `--sim-submit-latency-ms` / `--sim-cancel-latency-ms` /
`--sim-latency-jitter-ms`).

### Sample config — `config.toml`

The repo's [`config.toml`](./config.toml) ships in **auto-rotation** mode: no
fixed bot list. The `[rampage]` manager scores every liquid Binance USD-M perp,
runs a strategy on the top N, and re-ranks on an interval. One rotator drives
both supported strategies — pick the scoring signal (`[rampage.score]`) and the
strategy + geometry (`[rampage.strategy]`) independently.

**Scoring (`[rampage.score]`).** `mode` selects how candidates are ranked:

- `candle_height` (Wave's signal): rank by the average height of the last
  `candle_count` 1-minute candles, as a percent:
  `mean((high − low) / low × 100)`, wicks included. Big 1m candles mean lots of
  intra-minute oscillation for the frozen lattice to bank — the signature of a
  market that just woke up. A _recent_ signal, so it catches the move as it
  starts rather than diluting it in a 24h aggregate. Floors with
  `min_candle_pct`.
- `tick_bps` (Tide's signal): rank by `tick_size / price × 10000` (no extra API
  calls — read from the discovery snapshot). Floors with `min_tick_bps`.

**Strategy (`[rampage.strategy]`).** `kind` selects what each spawned bot runs:

- `wave` — frozen fixed-step lattice + round-trip refill.
- `tide` — fill-driven sliding lattice (grid re-centers as fills land).

**Each recheck** (`recheck_interval_secs`): list perps + price + 24h volume,
pre-filter by `min_volume_usdt`, score the survivors, and run the chosen
strategy on the top `top_n`.

**Rotation is graceful.** A symbol that drops out of the top N only rotates when
its bot's NET PnL (`realized + unrealized − fees`) is green, OR its NET loss is
within `rotate_loss_pct` of total wallet balance (default `1` = 1%). A bigger
NET loss keeps the bot running (`defer_underwater`, default on) until it
recovers or the loss shrinks within tolerance — rotation never crystallizes more
than the accepted loss. On shutdown, bots cancel their open orders but leave
positions intact; a restart re-adopts the position (orphan-position adoption)
and only re-cancels stale orders.

The `mode`/`kind` tag and its parameters live in separate tables (the params go
under a nested `.params` table):

```toml
[rampage]
enabled = true
min_volume_usdt = "2000000"
recheck_interval_secs = 60
quote_asset = "USDT"
top_n = 10
# defer_underwater defaults true

[rampage.score]
mode = "candle_height"      # "candle_height" (Wave signal) | "tick_bps" (Tide signal)
[rampage.score.params]
candle_count = 60
min_candle_pct = "0"

[rampage.strategy]
kind = "wave"               # "wave" | "tide"
[rampage.strategy.params]
grid_levels = 10
step_bps = 30
inner_steps = 2
refill_threshold = 5
chase_to_avg = false
chase = true
```

Key knobs:

| Section              | Field                                    | What it does                                                                                         |
| -------------------- | ---------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `[account]`          | `env` / `asset`                          | `futures-mainnet`, USDT-margined (USDT perps host the oscillators)                                   |
| `[account]`          | `order_balance_pct` / `max_position_pct` | per-order size + wallet-relative position cap                                                        |
| `[account]`          | `leverage`                               | exchange margin backstop                                                                             |
| `[rampage]`          | `top_n` / `recheck_interval_secs`        | how many bots to run + how often to re-rank                                                          |
| `[rampage]`          | `min_volume_usdt` / `quote_asset`        | liquidity pre-filter + which quote to scan                                                           |
| `[rampage]`          | `defer_underwater` / `rotate_loss_pct`   | defer rotation while NET loss exceeds `rotate_loss_pct` % of wallet (NET = realized+unrealized−fees) |
| `[rampage]`          | `symbols_allowlist`                      | optional explicit symbol set (volume + score filters still apply)                                    |
| `[rampage.score]`    | `mode` + `params`                        | `candle_height` (`candle_count`/`min_candle_pct`) or `tick_bps` (`min_tick_bps`)                     |
| `[rampage.strategy]` | `kind` + `params`                        | `wave` or `tide` lattice params forwarded to every spawned bot                                       |

To run a **fixed bot list** instead, drop `[rampage]` and add `[[bots]]` entries
(one per symbol/strategy). Re-read the warning above before pointing any of this
at real funds.

### Run live (testnet first!)

```bash
# Multi-bot orchestrator (config-driven; testnet env in config.toml first!)
cargo run --release --bin tikr -- --config config.toml

# Single-symbol Binance Spot smoke test (example)
cargo run --release --example run_spot -- \
  --env spot-testnet --symbol BTCUSDT --minutes 5
```

Auth lives in `.env` (gitignored). Use Ed25519 PEM for spot (mandatory) or HMAC
for futures — see `.env.example`.

## Contributing

See the org-wide
[CONTRIBUTING guide](https://github.com/kryptic-sh/.github/blob/main/.github/CONTRIBUTING.md).
Phase 0 is design-heavy — comment on open design issues before opening PRs.

For security issues, see the org-wide
[SECURITY policy](https://github.com/kryptic-sh/.github/blob/main/.github/SECURITY.md).

## License

MIT. See [LICENSE](LICENSE).
