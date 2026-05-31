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

The repo's [`config.toml`](./config.toml) ships in **Wave auto-rotation** mode:
no fixed bot list. The `[wave_auto]` manager scores every liquid Binance USD-M
perp by recent price action and runs the Wave strategy (frozen fixed-step
lattice + round-trip refill) on the top N, re-ranking on an interval.

**How the score works.** Each symbol is ranked by the average height of its last
`candle_count` 1-minute candles, as a percent:
`mean( (high − low) / low × 100 )`, wicks included. Big 1-minute candles mean
lots of intra-minute oscillation for the frozen lattice to bank — the signature
of a market that just woke up. It's a _recent_ signal, so it catches the move as
it starts rather than diluting it in a 24h aggregate.

**Each recheck** (`recheck_interval_secs`): list perps + price + 24h volume,
pre-filter by `min_volume_usdt`, fetch the last `candle_count` 1m klines for
each survivor (concurrent), score, and run Wave on the top `top_n` (avg candle %
≥ `min_candle_pct`).

**Rotation is graceful.** A symbol that drops out of the top N only rotates when
its bot is flat or green. A bot holding an underwater bag keeps running until it
recovers (`defer_underwater`, default on) — rotation never crystallizes a loss.
On shutdown, bots cancel their open orders but leave positions intact; a restart
re-adopts the position and only re-cancels stale orders.

Key knobs:

| Section       | Field                                                           | What it does                                                       |
| ------------- | --------------------------------------------------------------- | ------------------------------------------------------------------ |
| `[account]`   | `env` / `asset`                                                 | `futures-mainnet`, USDT-margined (USDT perps host the oscillators) |
| `[account]`   | `order_balance_pct` / `max_position_pct`                        | per-order size + wallet-relative position cap                      |
| `[account]`   | `leverage`                                                      | exchange margin backstop                                           |
| `[wave_auto]` | `candle_count` / `min_candle_pct` / `top_n`                     | scoring window, qualifying floor, how many bots to run             |
| `[wave_auto]` | `min_volume_usdt` / `quote_asset`                               | liquidity pre-filter + which quote to scan                         |
| `[wave_auto]` | `grid_levels` / `step_bps` / `inner_steps` / `refill_threshold` | Wave lattice params forwarded to every spawned bot                 |
| `[wave_auto]` | `chase_to_avg` / `chase`                                        | reducing-side behavior (chase toward avg entry vs. market)         |

To run a **fixed bot list** instead, drop `[wave_auto]` and add `[[bots]]`
entries (one per symbol/strategy). Re-read the warning above before pointing any
of this at real funds.

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
