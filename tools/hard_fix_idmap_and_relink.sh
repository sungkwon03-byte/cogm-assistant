#!/usr/bin/env bash
# 목적: LFS 포인터 끊기 → id_map.csv 로컬 재생성 → 진단/연결 재실행(터미널 안전)
set +e
ts(){ date -u +%FT%TZ; }; log(){ printf "[%s] %s\n" "$(ts)" "$*"; }

ROOT="$(pwd)"; OUT="$ROOT/output"; DATA="$ROOT/data"; DUCK="$DATA/duckdb"; SUM="$OUT/summaries"
mkdir -p "$OUT" "$DATA" "$DUCK" "$SUM"

log "[HARD-FIX] start"

# 0) 안전장치: CRLF/권한
sed -i 's/\r$//' tools/*.sh 2>/dev/null || true
chmod +x tools/*.sh 2>/dev/null || true

# 1) LFS에서 id_map.csv만 ‘추적 해제’ (포인터 문제 영구 차단)
if [ -f .gitattributes ]; then
  grep -v '^output/id_map\.csv filter=lfs' .gitattributes > .gitattributes.tmp 2>/dev/null || true
  mv .gitattributes.tmp .gitattributes 2>/dev/null || true
fi
git lfs untrack output/id_map.csv >/dev/null 2>&1 || true

# 2) player_cards_all.parquet로 id_map.csv 재생성 (로컬 진짜 CSV)
python3 - <<'PY'
import os, pandas as pd, json
OUT="output"; CARDS=os.path.join(OUT,"player_cards_all.parquet")
if not os.path.exists(CARDS):
    raise SystemExit(json.dumps({"error":"missing player_cards_all.parquet"}))

df = pd.read_parquet(CARDS)
cols = [c for c in df.columns]
# 가능한 이름/팀/포지션 컬럼 자동 탐지
def pick(cands):
    for c in cands:
        if c in cols: return c
    return None

pid = pick(["player_id","mlb_id","id"])
pname = pick(["player_name","name"])
team = pick(["team","team_name","mlb_team","current_team"])
pos  = pick(["pos","position","primary_pos"])

if not pid:
    raise SystemExit(json.dumps({"error":"player_id-like column not found","columns":cols}))

out_cols = [pid]
names = []
if pname: out_cols.append(pname); names.append(pname)
if team:  out_cols.append(team)
if pos:   out_cols.append(pos)

idm = df[out_cols].dropna(subset=[pid]).drop_duplicates(subset=[pid])
# 표준 헤더로 리라벨
rename_map = {}
rename_map[pid] = "player_id"
if pname: rename_map[pname] = "player_name"
if team:  rename_map[team]  = "team"
if pos:   rename_map[pos]   = "pos"
idm = idm.rename(columns=rename_map)
# 누락 컬럼 보강
for need in ["player_name","team","pos"]:
    if need not in idm.columns: idm[need] = ""

idm = idm[["player_id","player_name","team","pos"]]
idm["player_id"] = pd.to_numeric(idm["player_id"], errors="coerce").astype("Int64")
idm = idm.dropna(subset=["player_id"]).drop_duplicates(subset=["player_id"])

idm.to_csv(os.path.join(OUT,"id_map.csv"), index=False)
print(json.dumps({"rows":len(idm)}))
PY

# 3) 진짜 CSV가 됐는지 빠른 확인
log "[CHECK] head id_map.csv"
head -n 5 "$OUT/id_map.csv" || true

# 4) DuckDB 뷰/딥지표 재연결 (우리가 만든 진단 스크립트 실행)
if [ -f tools/live_data_attach_and_diag_v5.sh ]; then
  bash tools/live_data_attach_and_diag_v5.sh || true
else
  log "[WARN] tools/live_data_attach_and_diag_v5.sh not found"
fi

# 5) 자동 커밋(선택) — 잔여 LFS 포인터 교체 기록
git add .gitattributes "$OUT/id_map.csv" 2>/dev/null
git commit -m "fix: replace LFS pointer with real id_map.csv; relink live data" >/dev/null 2>&1 || true
git push >/dev/null 2>&1 || true

log "[HARD-FIX] done"
