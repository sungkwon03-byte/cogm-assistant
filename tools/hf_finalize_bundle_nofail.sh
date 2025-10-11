#!/usr/bin/env bash
set +e
ROOT="/workspaces/cogm-assistant"; OUT="$ROOT/output"; REP="$OUT/reports"; SUM="$OUT/summaries"; LOG="$ROOT/logs"
mkdir -p "$OUT" "$REP" "$SUM" "$LOG"
TS="$(date -u +%Y%m%dT%H%M%SZ)"; BUNDLE="$ROOT/handoff_bundle_MLB_HF_${TS}.tar.gz"
inc=( "$OUT/statcast_ultra_full_clean.parquet" "$OUT/player_cards_all.parquet" "$OUT/player_cards_enriched_all_seq.parquet"
      "$SUM/platoon_split.csv" "$REP/platoon_map.png" "$SUM/weakness_heatmap_matrix.csv" "$REP/weakness_heatmap.png"
      "$REP/trend_cards_3y.pdf" "$SUM/euz_umpire_impact.csv" "$REP/ump_euz.png" "$REP/explainable_attribution_topN.png"
      "$REP/auto_report_v2.pdf" "$REP/legacy_report_v2.pdf" "$OUT/full_system_validation.json" "$SUM/visuals_final_status.json"
      "$LOG/visuals_final_hf.log" "$LOG/final_fullbuild.log" "$LOG/never_die.log" )
files=(); for f in "${inc[@]}"; do [ -f "$f" ] && files+=("$f"); done
if [ ${#files[@]} -eq 0 ]; then echo "NO-FAIL bundle placeholder at $TS" > "$OUT/README_NOFAIL.txt"; files+=("$OUT/README_NOFAIL.txt"); fi
tar -czf "$BUNDLE" -C "/" "${files[@]#/}" >/dev/null 2>&1 || true
echo "$BUNDLE"; exit 0
