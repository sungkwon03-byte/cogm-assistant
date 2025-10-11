#!/usr/bin/env bash
set -euo pipefail
ROOT="/workspaces/cogm-assistant"
OUT="$ROOT/output"
mkdir -p "$OUT"

need=( "statcast_ultra_full_clean.parquet" "player_cards_all.parquet" "player_cards_enriched_all_seq.parquet" )
miss=0
for f in "${need[@]}"; do
  if [ ! -f "$OUT/$f" ]; then
    echo "[WARN] missing $OUT/$f"
    miss=$((miss+1))
  fi
done

if [ "$miss" -gt 0 ]; then
  echo "[INFO] If you have a mounted volume or backup, restore files into $OUT/ and rerun."
fi
