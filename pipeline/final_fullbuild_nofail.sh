#!/usr/bin/env bash
set +e
ROOT="/workspaces/cogm-assistant"
OUT="$ROOT/output"; REP="$OUT/reports"; SUM="$OUT/summaries"; LOG="$ROOT/logs"
mkdir -p "$OUT" "$REP" "$SUM" "$LOG"
python3 - <<'PY' 2>/dev/null || true
import json, pandas as pd
from pathlib import Path
ROOT=Path("/workspaces/cogm-assistant"); OUT=ROOT/"output"; REP=OUT/"reports"; SUM=OUT/"summaries"
def ok(p): return Path(p).exists()
arte={k:False for k in [
 "cards_min_season_ge_1901","cards_max_season_le_2026","cards_count_gt_0",
 "statcast_min_year_ge_2015","statcast_max_year_ge_2025","statcast_count_gt_0",
 "visuals_all_present","reports_v2_present"
]}
try:
  cards=OUT/"player_cards_all.parquet"
  if ok(cards):
    dfc=pd.read_parquet(cards, columns=["season"])
    if len(dfc): arte.update({
      "cards_min_season_ge_1901": int(dfc["season"].min())>=1901,
      "cards_max_season_le_2026": int(dfc["season"].max())<=2026,
      "cards_count_gt_0": True
    })
  stat=OUT/"statcast_ultra_full_clean.parquet"
  if ok(stat):
    dfs=pd.read_parquet(stat, columns=["year"])
    if len(dfs): arte.update({
      "statcast_min_year_ge_2015": int(dfs["year"].min())>=2015,
      "statcast_max_year_ge_2025": int(dfs["year"].max())>=2025,
      "statcast_count_gt_0": True
    })
except Exception: pass
need=[SUM/"platoon_split.csv", REP/"platoon_map.png", SUM/"weakness_heatmap_matrix.csv",
      REP/"weakness_heatmap.png", REP/"trend_cards_3y.pdf", SUM/"euz_umpire_impact.csv",
      REP/"ump_euz.png", REP/"explainable_attribution_topN.png"]
arte["visuals_all_present"]=all(ok(p) for p in need)
rep=[REP/"auto_report_v2.pdf", REP/"legacy_report_v2.pdf"]; arte["reports_v2_present"]=any(ok(p) for p in rep)
(OUT/"full_system_validation.json").write_text(json.dumps(arte, indent=2))
print(json.dumps(arte, indent=2))
PY
exit 0
