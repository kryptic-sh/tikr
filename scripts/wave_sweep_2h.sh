#!/usr/bin/env bash
# Wave parameter sweep on 2h kline-replay data, USDC promo fees (0/5).
# Args: <data-dir> <symbol> <tick> <step>
set -euo pipefail
DATA="${1:-./data/2h/ETHUSDT}"
SYM="${2:-ETHUSDT}"
TICK="${3:-0.01}"
STEP="${4:-0.001}"
BIN="./target/release/backtest"

echo -e "levels\tdrain\tatr_mult\tatr_p\tbar_s\trelat_n\tskew\tnet\tfills\tunrl\tpeak"
for LV in 8 12 16; do
  for DR in 2 4 6; do
    for AM in 0.5 1.0 2.0; do
      for AP in 14; do
        for BS in 60; do
          for RL in 5 10 20; do
            for SK in 0.0 0.25; do
              OUT=$("$BIN" --data-dir "$DATA" --symbol "$SYM" --strategy wave \
                --tick-size "$TICK" --wv-notional 10 --wv-step-size "$STEP" \
                --wv-min-notional 5 --wv-grid-levels "$LV" \
                --wv-recenter-drain-slots "$DR" --wv-step-atr-mult "$AM" \
                --wv-atr-period "$AP" --wv-bar-interval-secs "$BS" \
                --wv-bar-warmup-bars "$AP" \
                --wv-relattice-every-n "$RL" --wv-skew-max-pct "$SK" \
                --maker-bps 0 --taker-bps 5 2>/dev/null)
              NET=$(echo "$OUT" | jq -r .net 2>/dev/null || echo 0)
              FIL=$(echo "$OUT" | jq -r .fills_emitted 2>/dev/null || echo 0)
              UNR=$(echo "$OUT" | jq -r .unrealized 2>/dev/null || echo 0)
              PEAK=$(echo "$OUT" | jq -r .peak_position_usdt 2>/dev/null || echo 0)
              printf "%d\t%d\t%s\t%d\t%d\t%d\t%s\t%s\t%s\t%s\t%s\n" \
                "$LV" "$DR" "$AM" "$AP" "$BS" "$RL" "$SK" "$NET" "$FIL" "$UNR" "$PEAK"
            done
          done
        done
      done
    done
  done
done
