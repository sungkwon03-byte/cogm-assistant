#!/usr/bin/env bash
set +e
for m in streamlit pandas pyarrow duckdb; do python -c "import $m" >/dev/null 2>&1 && echo "[OK] $m" || echo "[MISS] $m"; done
for f in output/player_cards_all.parquet output/statcast_ultra_full_clean.parquet output/id_map.csv; do [ -s "$f" ] && echo "[OK] $f" || echo "[MISS] $f"; done
