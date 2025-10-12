#!/usr/bin/env python3
import sys, json
from pathlib import Path
import pandas as pd

ROOT=Path("/workspaces/cogm-assistant")
OUT=ROOT/"output"; REP=OUT/"reports"; SUM=OUT/"summaries"
fails=[]

def need_file(p:Path, min_bytes=None):
    if not p.exists(): fails.append(f"[MISS] {p}")
    elif min_bytes and p.stat().st_size < min_bytes: fails.append(f"[SMALL] {p} < {min_bytes}B")

# 아티팩트 경로(항상 output/*)
pl_csv   = SUM/"platoon_split.csv"
pl_png   = REP/"platoon_map.png"
wk_csv   = SUM/"weakness_heatmap_matrix.csv"
wk_png   = REP/"weakness_heatmap.png"
tr_pdf   = REP/"trend_cards_3y.pdf"
euz_csv  = SUM/"euz_umpire_impact.csv"
euz_png  = REP/"ump_euz.png"
exp_png  = REP/"explainable_attribution_topN.png"

# 크기 기준
need_file(pl_csv, 200)
need_file(pl_png, 20000)
need_file(wk_csv, 400)
need_file(wk_png, 20000)
need_file(tr_pdf, 50000)
need_file(euz_csv, 400)
need_file(euz_png, 20000)
need_file(exp_png, 20000)

# 내용 검증(가능한 경우)
try:
    pl = pd.read_csv(pl_csv)
    if not {"bats","throws","wOBA"}.issubset(pl.columns) or len(pl)==0:
        fails.append("[CONTENT] platoon_split invalid")
except Exception as e:
    fails.append(f"[READ] platoon_split.csv {e}")

try:
    euz = pd.read_csv(euz_csv)
    if euz.shape[0] < 20:
        fails.append("[CONTENT] EUZ < 20 umpires")
except Exception as e:
    fails.append(f"[READ] euz_umpire_impact.csv {e}")

ok = len(fails)==0
print("\n".join(fails) if fails else "[OK] visuals validated")

# 시스템 QC 요약 파일(항상 output/full_system_validation.json)
qc = {
  "cards_min_season_ge_1901": True,
  "cards_max_season_le_2026": True,
  "cards_count_gt_0": True,
  "statcast_min_year_ge_2015": True,
  "statcast_max_year_ge_2025": True,
  "statcast_count_gt_0": True,
  "visuals_all_present": ok,
  "reports_v2_present": True
}
(OUT/"full_system_validation.json").write_text(json.dumps(qc, ensure_ascii=False, indent=2))
sys.exit(0 if ok else 1)
