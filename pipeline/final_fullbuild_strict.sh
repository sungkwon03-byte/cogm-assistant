#!/usr/bin/env bash
set -euo pipefail

ROOT="/workspaces/cogm-assistant"
OUT="$ROOT/output"; REP="$OUT/reports"; SUM="$OUT/summaries"; LOG="$ROOT/logs"
mkdir -p "$OUT" "$REP" "$SUM" "$LOG"

echo "[final_fullbuild_strict] start $(date -u +%FT%TZ)"

# 1) v2 리포트 재생성(있으면 통과)
if [ -f "$ROOT/pipeline/generate_auto_report_v2.py" ]; then
  (python3 "$ROOT/pipeline/generate_auto_report_v2.py" || echo "[WARN] auto_report_v2 failed") | tee -a "$LOG/final_fullbuild.log"
else
  echo "[INFO] skip auto_report_v2 (script missing)"
fi

if [ -f "$ROOT/pipeline/generate_legacy_report_v2.py" ]; then
  (python3 "$ROOT/pipeline/generate_legacy_report_v2.py" || echo "[WARN] legacy_report_v2 failed") | tee -a "$LOG/final_fullbuild.log"
else
  echo "[INFO] skip legacy_report_v2 (script missing)"
fi

# 2) QC 생성 (파이썬 실패해도 JSON은 반드시 남김)
python3 - <<'PY' || true
import json, pandas as pd
from pathlib import Path
ROOT=Path("/workspaces/cogm-assistant"); OUT=ROOT/"output"; SUM=OUT/"summaries"; REP=OUT/"reports"
arte = {
  "cards_min_season_ge_1901": False, "cards_max_season_le_2026": False, "cards_count_gt_0": False,
  "statcast_min_year_ge_2015": False, "statcast_max_year_ge_2025": False, "statcast_count_gt_0": False,
  "visuals_all_present": False, "reports_v2_present": False
}
def ok(p): return Path(p).exists()
cards = OUT/"player_cards_all.parquet"
statc = OUT/"statcast_ultra_full_clean.parquet"
try:
  if ok(cards):
    dfc = pd.read_parquet(cards, columns=["season"])
    arte["cards_min_season_ge_1901"] = int(dfc["season"].min()) >= 1901
    arte["cards_max_season_le_2026"] = int(dfc["season"].max()) <= 2026
    arte["cards_count_gt_0"] = len(dfc) > 0
  if ok(statc):
    dfs = pd.read_parquet(statc, columns=["year"])
    arte["statcast_min_year_ge_2015"] = int(dfs["year"].min()) >= 2015
    arte["statcast_max_year_ge_2025"] = int(dfs["year"].max()) >= 2025
    arte["statcast_count_gt_0"] = len(dfs) > 0
except Exception:
  pass

visuals = [
  OUT/"summaries/platoon_split.csv",
  OUT/"reports/platoon_map.png",
  OUT/"summaries/weakness_heatmap_matrix.csv",
  OUT/"reports/weakness_heatmap.png",
  OUT/"reports/trend_cards_3y.pdf",
  OUT/"summaries/euz_umpire_impact.csv",
  OUT/"reports/ump_euz.png",
  OUT/"reports/explainable_attribution_topN.png",
]
arte["visuals_all_present"] = all(ok(p) for p in visuals)

reports = [OUT/"reports/auto_report_v2.pdf", OUT/"reports/legacy_report_v2.pdf"]
arte["reports_v2_present"] = any(ok(p) for p in reports)

(OUT/"full_system_validation.json").write_text(json.dumps(arte, indent=2))
print(json.dumps(arte, indent=2))
PY

# 최종 흔적
echo "[final_fullbuild_strict] done $(date -u +%FT%TZ)"
