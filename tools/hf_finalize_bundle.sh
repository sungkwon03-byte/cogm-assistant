#!/usr/bin/env bash
set -euo pipefail
ROOT="/workspaces/cogm-assistant"
OUT="$ROOT/output"; REP="$OUT/reports"; SUM="$OUT/summaries"; LOG="$ROOT/logs"
TS="$(date -u +%Y%m%dT%H%M%SZ)"
BUNDLE="$ROOT/handoff_bundle_MLB_HF_${TS}.tar.gz"

include_list=(
  "$OUT/statcast_ultra_full_clean.parquet"
  "$OUT/player_cards_all.parquet"
  "$OUT/player_cards_enriched_all_seq.parquet"
  "$SUM/platoon_split.csv"
  "$REP/platoon_map.png"
  "$SUM/weakness_heatmap_matrix.csv"
  "$REP/weakness_heatmap.png"
  "$REP/trend_cards_3y.pdf"
  "$SUM/euz_umpire_impact.csv"
  "$REP/ump_euz.png"
  "$REP/explainable_attribution_topN.png"
  "$REP/auto_report_v2.pdf"
  "$REP/legacy_report_v2.pdf"
  "$OUT/full_system_validation.json"
  "$SUM/visuals_final_status.json"
  "$LOG/final_fullbuild.log"
)

# 존재 파일만 담기
tmp_list=()
for f in "${include_list[@]}"; do
  [ -f "$f" ] && tmp_list+=("$f")
done

if [ "${#tmp_list[@]}" -eq 0 ]; then
  echo "[ERROR] nothing to bundle"; exit 1
fi

tar -czf "$BUNDLE" -C "/" "${tmp_list[@]#/}" || true
echo "$BUNDLE"
