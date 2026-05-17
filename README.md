# tikr

Modular, venue-agnostic market-making engine in Rust. Pre-alpha — backtest
framework + strategy traits + Hyperliquid stub adapter only. **Not for live
trading yet.**

[![CI](https://github.com/kryptic-sh/tikr/actions/workflows/ci.yml/badge.svg)](https://github.com/kryptic-sh/tikr/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> 🚧 **Phase 0** — foundation only. No live trading logic. Watch
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

Crates land as phases ship. See
[roadmap](https://github.com/kryptic-sh/tikr/issues/1).

## Status

Pre-alpha. Code is design + skeleton. **Do not run against real capital.**

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
