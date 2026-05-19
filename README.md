# tikr

> ## ⚠️ READ THIS FIRST — FINANCIAL RISK WARNING ⚠️
>
> **This is experimental software for educational and personal research use
> only. It is NOT financial advice and NOT a production trading system.**
>
> - **You can lose ALL of your money.** Market making, perpetual futures, and
>   any leveraged trading carries unlimited downside risk including
>   liquidation and negative balance.
> - **Bugs in this code WILL lose you money** if you run it with real funds.
>   This is pre-alpha, unaudited, written by hobbyists with no fiduciary
>   relationship to you.
> - **You are 100% responsible** for any losses, regulatory consequences, tax
>   obligations, exchange TOS violations, and account actions resulting from
>   running this bot. The authors accept ZERO liability.
> - **Test on testnet only** until you fully understand the code. Even then,
>   start with the smallest possible position sizes if you ever go live.
> - **Manipulation, wash trading, and other prohibited strategies are NOT
>   legal on DEXs either** — see the Mango Markets case. Don't use this for
>   anything that would violate your jurisdiction's market-abuse laws or your
>   exchange's terms of service.
> - **Run only on accounts you can afford to zero out.** Never trade with
>   money you need for rent, debt, dependents, or anything else.
>
> By cloning, building, or running this software you acknowledge that you
> have read this warning and that any financial loss is entirely your own
> responsibility. See [LICENSE](LICENSE) (MIT — "AS IS, WITHOUT WARRANTY OF
> ANY KIND").

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
| `tikr-strategy`    | `Strategy` trait + reference impls                                                         |
| `tikr-hyperliquid` | First adapter — on-chain orderbook                                                         |
| `tikr-backtest`    | Phase 1 backtest engine + recorder bin                                                     |
| `tikr-paper`       | Phase 3 paper-trading runner (live feed + simulated fills)                                 |

Crates land as phases ship. See
[roadmap](https://github.com/kryptic-sh/tikr/issues/1).

## Status

Pre-alpha. Code is design + skeleton. **Do not run against real capital.**

## Install

```bash
# build from source
git clone https://github.com/kryptic-sh/tikr.git
cd tikr
cargo install --path . --bin tikr
```

## Development

```bash
git clone git@github.com:kryptic-sh/tikr.git
cd tikr
rustup toolchain install stable
cargo test --workspace
```

## Contributing

See the org-wide
[CONTRIBUTING guide](https://github.com/kryptic-sh/.github/blob/main/.github/CONTRIBUTING.md).
Phase 0 is design-heavy — comment on open design issues before opening PRs.

For security issues, see the org-wide
[SECURITY policy](https://github.com/kryptic-sh/.github/blob/main/.github/SECURITY.md).

## License

MIT. See [LICENSE](LICENSE).
