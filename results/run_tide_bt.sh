#!/usr/bin/env bash
set -u
cd /home/mxaddict/Projects/kryptic-sh/tikr
declare -A DIR=( [NEARUSDT]=NEARUSDC [SUIUSDT]=SUIUSDC [WLDUSDT]=WLDUSDC [ZECUSDT]=ZECUSDC )
for SYM in NEARUSDT SUIUSDT WLDUSDT ZECUSDT; do
  echo "=== $SYM @ $(date +%H:%M:%S) ==="
  ./target/release/compare \
    --data-dir "data/72h/${DIR[$SYM]}" --symbol "$SYM" --strategies tide \
    --venue-env futures-mainnet --maker-bps 2 --taker-bps 5 \
    --sim-initial-balance 1500 --sim-order-balance-pct 0.1 --sim-max-position-pct 0 --leverage 10 \
    --tide-step-bps-list 30 --tide-grid-levels-list 10 --tide-inner-steps 1 --tide-inventory-skew-list "0,0.1,0.25,0.5" \
    > "results/tide_${SYM}.txt" 2>&1
  echo "  $SYM done exit=$? @ $(date +%H:%M:%S)"
done
echo "ALL DONE @ $(date +%H:%M:%S)"
