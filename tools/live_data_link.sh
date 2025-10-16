#!/usr/bin/env bash
# 목적: 실데이터 연결 + DuckDB attach + 딥지표 활성화 + 검증 + (옵션)자동 커밋
set -euo pipefail
ts(){ date -u +%FT%TZ; }; log(){ printf "[%s] %s\n" "$(ts)" "$*"; }

ROOT="$(pwd)"
OUT="$ROOT/output"; DATA="$ROOT/data"; DUCK="$DATA/duckdb"; SUM="$OUT/summaries"
mkdir -p "$OUT" "$DUCK" "$SUM"

log "[LIVE-DATA] start"

# 0) 필수 파이썬 패키지 확보
python3 - <<'PY' || true
import importlib, sys, subprocess
need = ["duckdb","pandas","pyarrow"]
ok=True
for m in need:
    try: importlib.import_module(m)
    except Exception:
        ok=False
        subprocess.check_call([sys.executable,"-m","pip","install","--no-cache-dir",m])
PY

# 1) 핵심 파일 존재 확인
miss=()
for f in player_cards_all.parquet statcast_ultra_full_clean.parquet id_map.csv; do
  [ -s "$OUT/$f" ] || miss+=("$f")
done
if [ ${#miss[@]} -gt 0 ]; then
  log "[ERR] missing core files: ${miss[*]}"
  log "    기대 경로: $OUT/{player_cards_all.parquet, statcast_ultra_full_clean.parquet, id_map.csv}"
  exit 2
fi

# 2) DuckDB attach + 뷰/딥지표 생성
python3 - <<'PY'
import os, json, duckdb
root=os.getcwd(); out=f"{root}/output"; db=f"{root}/data/duckdb/main.duckdb"
cards=f"{out}/player_cards_all.parquet"
sc=f"{out}/statcast_ultra_full_clean.parquet"
idm=f"{out}/id_map.csv"

con=duckdb.connect(db)
# 안전하게 VIEW 생성 (BinderException 피하기 위해 직접 리터럴 바인딩 회피)
con.execute(f"CREATE OR REPLACE VIEW player_cards AS SELECT * FROM read_parquet('{cards.replace(\"'\",\"''\")}')")
con.execute(f"CREATE OR REPLACE VIEW statcast      AS SELECT * FROM read_parquet('{sc.replace(\"'\",\"''\")}')")
con.execute(f"CREATE OR REPLACE VIEW id_map        AS SELECT * FROM read_csv_auto('{idm.replace(\"'\",\"''\")}')")

# 딥지표(존재 컬럼 자동 감지)
cols = {r[0].lower() for r in con.execute("DESCRIBE statcast").fetchall()}
has = lambda c: c.lower() in cols
ev = "ev" if has("ev") else "exit_velocity" if has("exit_velocity") else "NULL"
la = "la" if has("la") else "launch_angle" if has("launch_angle") else "NULL"
velo = "pitch_speed" if has("pitch_speed") else "release_speed" if has("release_speed") else "NULL"
spin = "spin_rate" if has("spin_rate") else "release_spin_rate" if has("release_spin_rate") else "NULL"
pt   = "pitch_type" if has("pitch_type") else "pitch_name" if has("pitch_name") else "NULL"
pid  = "player_id" if has("player_id") else "batter" if has("batter") else "NULL"
gdt  = "game_date" if has("game_date") else "date" if has("date") else "NULL"

con.execute(f"""
CREATE OR REPLACE VIEW statcast_deep AS
SELECT
  {pid} AS player_id,
  {gdt} AS game_date,
  {ev}  AS ev,
  {la}  AS la,
  {pt}  AS pitch_type,
  {velo} AS pitch_velo,
  {spin} AS spin_rate,
  CASE WHEN {ev} IS NOT NULL AND {ev}>=95 THEN 1 ELSE 0 END AS hard_hit,
  CASE WHEN {ev} IS NOT NULL AND {ev}>=98 AND {la} BETWEEN 26 AND 30 THEN 1 ELSE 0 END AS barrel_like
FROM statcast
""")

# 간단 집계 뷰 (피처 의존 기능 언블록용)
con.execute("""
CREATE OR REPLACE VIEW exports AS
SELECT player_id, game_date, ev, la FROM statcast_deep
""")

summary = dict(
  rows=dict(
    player_cards=con.execute("SELECT COUNT(*) FROM player_cards").fetchone()[0],
    statcast=con.execute("SELECT COUNT(*) FROM statcast").fetchone()[0],
    deep=con.execute("SELECT COUNT(*) FROM statcast_deep").fetchone()[0],
    id_map=con.execute("SELECT COUNT(*) FROM id_map").fetchone()[0],
  ),
  sample=con.execute("SELECT * FROM statcast_deep LIMIT 3").fetchdf().to_dict(orient="records")
)
print(json.dumps(summary, ensure_ascii=False))
PY

# 3) 결과 저장
python3 - <<'PY'
import os, json, datetime
root=os.getcwd(); fp=f"{root}/output/summaries/live_data_link.json"
payload={"linked":True,"ts":datetime.datetime.utcnow().isoformat()+"Z"}
os.makedirs(os.path.dirname(fp), exist_ok=True)
with open(fp,"w") as f: json.dump(payload,f,ensure_ascii=False,indent=2)
print(json.dumps(payload, ensure_ascii=False))
PY

# 4) (옵션) 자동 커밋/푸시
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  git add output/summaries/live_data_link.json data/duckdb/main.duckdb 2>/dev/null || true
  git commit -m "live-data: attach + deep views + verification" >/dev/null 2>&1 || echo "[Git] nothing to commit"
  git push >/dev/null 2>&1 || echo "[Git] push skipped"
fi

log "[LIVE-DATA] done"
