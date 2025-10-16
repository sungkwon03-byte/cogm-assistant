#!/usr/bin/env bash
set +e
OUT="output"; SUM="$OUT/summaries"; mkdir -p "$SUM"
miss=()
[ -s "$OUT/player_cards_all.parquet" ] || miss+=("player_cards_all.parquet")
[ -s "$OUT/statcast_ultra_full_clean.parquet" ] || miss+=("statcast_ultra_full_clean.parquet")
[ -s "$OUT/id_map.csv" ] || miss+=("id_map.csv")
if [ ${#miss[@]} -eq 0 ]; then
  echo '{"ok":true,"issues":[]}' | tee "$SUM/data_audit.json" >/dev/null
else
  printf '{"ok":false,"issues":[%s]}\n' "$(printf '"%s",' "${miss[@]}" | sed 's/,$//')" | tee "$SUM/data_audit.json" >/dev/null
fi
