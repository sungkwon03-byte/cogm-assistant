#!/usr/bin/env bash
set +e; ts(){ date -u +%FT%TZ; }; log(){ printf "[%s] %s\n" "$(ts)" "$*"; }
OUT="output"
log "[DOCTOR] start"
for f in "$OUT/player_cards_all.parquet" "$OUT/statcast_ultra_full_clean.parquet" "$OUT/id_map.csv"; do
  [ -s "$f" ] && log "[OK] $f" || log "[MISS] $f"
done
log "[AUDIT] complete"
log "[SMOKE] importing libs"
for m in streamlit pandas pyarrow duckdb numpy; do python -c "import $m" 2>/dev/null && log "[OK] $m"; done
