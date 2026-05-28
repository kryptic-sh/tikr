#!/usr/bin/env bash
# Sweep RSI-MR knobs on 5m klines, USDC promo fees (0/5).
# Usage: rsi_mr_sweep_5m.sh <parquet> <symbol> <tick_size> <step_size>
set -euo pipefail
DATA="${1:?usage: $0 <parquet> <symbol> <tick> <step>}"
SYM="${2:?need symbol}"
TICK="${3:?need tick}"
STEP="${4:?need step}"
BIN="./target/release/backtest_rsi_mr"

echo -e "rsi_buy\tker_max\tsl\ttp\tnet\tentries\ttp/sl/rsi/to"
for RBUY in 20 25 30 35; do
  for KER in 0.3 0.4 0.5 0.6; do
    for SL in 1 2 3; do
      for TP in 2 3 5; do
        OUT=$("$BIN" --parquet "$DATA" --symbol "$SYM" \
              --notional 100 --tick-size "$TICK" --step-size "$STEP" --min-notional 5 \
              --bar-interval-secs 300 \
              --rsi-buy-threshold "$RBUY" --rsi-exit-threshold 50 \
              --ker-max-trending "$KER" --vol-zscore-min 1.5 \
              --atr-sl-mult "$SL" --atr-tp-mult "$TP" \
              --maker-bps 0 --taker-bps 5 2>/dev/null)
        NET=$(echo "$OUT" | awk -F: '/^NET/{gsub(/ /,"",$2); print $2}')
        ENT=$(echo "$OUT" | awk -F: '/^entries/{gsub(/ /,"",$2); print $2}')
        TP_=$(echo "$OUT" | awk -F: '/^tp_exits/{gsub(/ /,"",$2); print $2}')
        SL_=$(echo "$OUT" | awk -F: '/^sl_exits/{gsub(/ /,"",$2); print $2}')
        RX_=$(echo "$OUT" | awk -F: '/^rsi_exits/{gsub(/ /,"",$2); print $2}')
        TO_=$(echo "$OUT" | awk -F: '/^timeout_exits/{gsub(/ \(.*$/,"",$2); gsub(/ /,"",$2); print $2}')
        printf "%d\t%s\t%s\t%s\t%s\t%s\t%s/%s/%s/%s\n" \
          "$RBUY" "$KER" "$SL" "$TP" "$NET" "$ENT" "$TP_" "$SL_" "$RX_" "$TO_"
      done
    done
  done
done
