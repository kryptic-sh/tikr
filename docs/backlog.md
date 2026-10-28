# Backlog

## Dependencies

- **`deny.toml` ignores RUSTSEC-2026-0387 for now.** `crypto-bigint` 0.5 could
  branch on secret values in subtraction, division and Montgomery
  multiplication, and only 0.7.6+ is fixed. tikr reaches it through
  `alloy-signer-local` (the Hyperliquid signer in `tikr-hyperliquid`) →
  `alloy-signer` → `k256` 0.13 → `elliptic-curve` 0.13, and the latest
  `alloy-signer`, 2.5.0 (checked 2026-10-28), still requires `k256` ^0.13. Not
  analysed: which of the affected operations k256 0.13 signing actually reaches.
  Once an alloy release moves to `k256` 0.14, bump alloy and drop the ignore.

- **`deny.toml` ignores RUSTSEC-2026-0391, -0398 and -0403 (zstd) for now.**
  They are fixed only in `zstd` 0.14.3 and `zstd-safe` 8.2.0, and
  `polars-parquet` requires `zstd` ^0.13 in both the resolved 0.43.1 and the
  latest 0.55.2 (checked 2026-10-28). Read from `polars-parquet` 0.43.1's
  source, none is reachable: it depends on `zstd` with default features off, so
  the `legacy` (0403) and `seekable` (0398) features are not compiled, and it
  calls only `zstd::bulk::compress_to_buffer` and `zstd::Decoder::new`, never a
  `with_context` constructor (0391). Re-check that reading on any polars bump,
  and drop the ignores once polars moves to `zstd` 0.14.3+.
