#!/usr/bin/env bash
# Tide parameter sweep over 2h of ETHUSDT snapshot data.
# Output: TSV with knobs + key PnL metrics, sorted by net.
set -euo pipefail
DATA="./data/2h/ETHUSDT"
SYM="ETHUSDT"
BIN="./target/release/backtest"
TICK="0.01"
NOTIONAL="10"
STEP="0.001"
MIN_NOTIONAL="5"

echo -e "spread_bps\tstep_bps\tlevels\tclose_bps\tnet\trealized\tunrealized\tfees\tfills\tpeak_pos_usdt\tbuy_vol\tsell_vol"

for SPREAD in 5 10 20 30; do
  for STEP_BPS in 4 10 20; do
    for LVLS in 3 6 12; do
      for CLOSE in 0 30; do
        OUT=$("$BIN" \
          --data-dir "$DATA" --symbol "$SYM" --strategy tide \
          --tick-size "$TICK" \
          --tr-notional "$NOTIONAL" --tr-step-size "$STEP" --tr-min-notional "$MIN_NOTIONAL" \
          --tr-grid-levels "$LVLS" \
          --tr-min-self-spread-bps "$SPREAD" \
          --tr-grid-step-bps "$STEP_BPS" \
          --tr-close-profit-bps "$CLOSE" \
          --maker-bps 2 --taker-bps 5 2>/dev/null)
        NET=$(echo "$OUT"   | jq -r '.net')
        REAL=$(echo "$OUT"  | jq -r '.realized')
        UNR=$(echo "$OUT"   | jq -r '.unrealized')
        FEES=$(echo "$OUT"  | jq -r '.fees')
        FILLS=$(echo "$OUT" | jq -r '.fills_emitted')
        PEAK=$(echo "$OUT"  | jq -r '.peak_position_usdt')
        BV=$(echo "$OUT"    | jq -r '.buy_volume_usdt')
        SV=$(echo "$OUT"    | jq -r '.sell_volume_usdt')
        printf "%d\t%d\t%d\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
          "$SPREAD" "$STEP_BPS" "$LVLS" "$CLOSE" "$NET" "$REAL" "$UNR" "$FEES" "$FILLS" "$PEAK" "$BV" "$SV"
      done
    done
  done
done
