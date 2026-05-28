#!/usr/bin/env bash
# Sweep RSI-MR knobs over 30d ETHUSDT klines, USDC promo fees (0/5).
set -euo pipefail
DATA="./data/klines/ETHUSDT_1m_30d.parquet"
BIN="./target/release/backtest_rsi_mr"

echo -e "rsi_buy\trsi_exit\tker_max\tvol_min\tsl\ttp\tnet\tentries\ttp/sl/rsi/to"
for RBUY in 20 25 30 35; do
  for KER in 0.3 0.4 0.5 0.6; do
    for SL in 1 2 3; do
      for TP in 2 3 5; do
        OUT=$("$BIN" --parquet "$DATA" --symbol ETHUSDT \
              --notional 100 --tick-size 0.01 --step-size 0.001 --min-notional 5 \
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
        printf "%d\t50\t%s\t1.5\t%s\t%s\t%s\t%s\t%s/%s/%s/%s\n" \
          "$RBUY" "$KER" "$SL" "$TP" "$NET" "$ENT" "$TP_" "$SL_" "$RX_" "$TO_"
      done
    done
  done
done
