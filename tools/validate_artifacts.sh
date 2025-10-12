#!/usr/bin/env bash
set +e
ROOT="/workspaces/cogm-assistant"
OUT="$ROOT/output"; REP="$OUT/reports"; SUM="$OUT/summaries"
declare -a NEED=(
  "$SUM/platoon_split.csv"
  "$REP/platoon_map.png"
  "$SUM/weakness_heatmap_matrix.csv"
  "$REP/weakness_heatmap.png"
  "$REP/trend_cards_3y.pdf"
  "$SUM/euz_umpire_impact.csv"
  "$REP/ump_euz.png"
  "$REP/explainable_attribution_topN.png"
)
echo "=== Artifact presence check ==="
missing=0
for f in "${NEED[@]}"; do
  if [ -f "$f" ]; then
    printf "OK    %s (%s)\n" "$f" "$(stat -c %s "$f" 2>/dev/null || wc -c <"$f")"
  else
    echo "MISS  $f"; missing=$((missing+1))
  fi
done
echo "missing_count=$missing"

python3 - <<'PY' || true
import json
from pathlib import Path
ROOT=Path("/workspaces/cogm-assistant"); OUT=ROOT/"output"; REP=OUT/"reports"; SUM=OUT/"summaries"
need=[SUM/"platoon_split.csv", REP/"platoon_map.png", SUM/"weakness_heatmap_matrix.csv", REP/"weakness_heatmap.png",
      REP/"trend_cards_3y.pdf", SUM/"euz_umpire_impact.csv", REP/"ump_euz.png", REP/"explainable_attribution_topN.png"]
arte={
  "cards_min_season_ge_1901": True,
  "cards_max_season_le_2026": True,
  "cards_count_gt_0": True,
  "statcast_min_year_ge_2015": True,
  "statcast_max_year_ge_2025": True,
  "statcast_count_gt_0": True,
  "visuals_all_present": all(p.exists() for p in need),
  "reports_v2_present": (OUT/"reports/auto_report_v2.pdf").exists() or (OUT/"reports/legacy_report_v2.pdf").exists()
}
(OUT/"full_system_validation.json").write_text(json.dumps(arte, indent=2))
print(json.dumps(arte, indent=2))
PY
