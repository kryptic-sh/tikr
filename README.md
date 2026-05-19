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

Modular, venue-agnostic market-making engine in Rust. Pre-alpha — backtest
framework + strategy traits + Hyperliquid stub adapter only. **Not for live
trading yet.**

[![CI](https://github.com/kryptic-sh/tikr/actions/workflows/ci.yml/badge.svg)](https://github.com/kryptic-sh/tikr/actions/workflows/ci.yml)
[![release](https://img.shields.io/github/v/release/kryptic-sh/tikr)](https://github.com/kryptic-sh/tikr/releases)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> **Phase 0** — foundation only. No live trading logic. Watch
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
| `tikr-strategy`    | `Strategy` trait + reference impls (NaiveGrid, AvellanedaStoikov, GLFT, TopOfBook)         |
| `tikr-hyperliquid` | Hyperliquid perp adapter                                                                   |
| `tikr-binance`     | Binance Spot + USD-M Futures adapter (HMAC + Ed25519 auth, WS-API session.logon)           |
| `tikr-dodo`        | DODO LimitOrder adapter (BSC)                                                              |
| `tikr-backtest`    | Parquet replay engine + FillSim (queue-priority + cancel modeling) + recorder bins         |
| `tikr-paper`       | Paper-trading runner (live feed + simulated fills) + backtest runner                       |
| `tikr-risk`        | Risk engine — pre-trade limits, drawdown gates, alerting                                   |

Crates land as phases ship. See
[roadmap](https://github.com/kryptic-sh/tikr/issues/1).

## Status

Pre-alpha. Code is design + skeleton. **Do not run against real capital.**

## Install

```bash
# build from source
git clone https://github.com/kryptic-sh/tikr.git
cd tikr
cargo build --release --workspace
```

Binaries land in `./target/release/`. Key entry points:

| Bin                  | Crate         | Purpose                                                  |
| -------------------- | ------------- | -------------------------------------------------------- |
| `record_binance`     | tikr-backtest | Record Binance market data (depth + aggTrade) to parquet |
| `record`             | tikr-backtest | Record Hyperliquid market data to parquet                |
| `run_backtest`       | tikr-paper    | Run one strategy against parquet data, emit P&L report   |
| `compare_strategies` | tikr-paper    | Run a strategy preset sweep, emit comparison table       |
| `run_perp` (example) | tikr-binance  | Live Binance Futures runner (testnet/mainnet)            |
| `run_spot` (example) | tikr-binance  | Live Binance Spot runner (testnet/mainnet)               |
| `supervisor`         | tikr-paper    | Multi-symbol paper-trading supervisor                    |

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
  --env futures-mainnet --symbol BTCUSDT --hours 1 --out ./data

# Hyperliquid (mainnet)
cargo run --release --bin record -- --symbol BTC --hours 1 --out ./data
```

Recorder writes `book_<BASE>_<DATE>_<NNNNNN>.parquet` +
`trades_<BASE>_<DATE>_<NNNNNN>.parquet` per flush (1000 rows or 60 s).

### Backtest a strategy against recorded data

```bash
cargo run --release --bin run_backtest -- \
  --data-dir ./data --symbol BTCUSDT \
  --strategy top-of-book \
  --size 0.001 --tick-size 0.1 \
  --improve-when-spread-gt-ticks 1 \
  --max-imbalance-ticks 5 \
  --maker-bps 2 --taker-bps 5
```

Strategies: `naive-grid`, `avellaneda-stoikov` (alias `as`), `glft`,
`top-of-book` (alias `tob`).

### Compare strategies on the same data

```bash
cargo run --release --bin compare_strategies -- \
  --data-dir ./data --symbol BTCUSDT \
  --maker-bps 2 --taker-bps 5
```

Runs a fixed sweep (NaiveGrid, A-S, GLFT, TopOfBook with various skew +
imbalance configs, plus BNB-discount variants) and prints a `fills`,
`fills/min`, `realized`, `fees`, `NET`, `$/fill` table.

### Run live (testnet first!)

```bash
# Binance Futures perp testnet
cargo run --release --example run_perp -- \
  --env futures-testnet --symbol BTCUSDT \
  --strategy top-of-book \
  --max-imbalance-ticks 5 \
  --minutes 5

# Binance Spot testnet
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
