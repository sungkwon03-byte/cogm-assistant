#!/usr/bin/env bash
# 목적: 실데이터 연결 + DuckDB attach + 딥지표 활성화 + 검증 + (옵션)자동 커밋
# 특징: set +e (실패해도 계속), Python 내부 try/except, DuckDB VIEW/Table 충돌 안전 처리
set +e
ts(){ date -u +%FT%TZ; }
log(){ printf "[%s] %s\n" "$(ts)" "$*"; }

ROOT="$(pwd)"
OUT="$ROOT/output"; DATA="$ROOT/data"; DUCK="$DATA/duckdb"; SUM="$OUT/summaries"
mkdir -p "$OUT" "$DUCK" "$SUM"

log "[LIVE-DATA SAFE] start"

# 0) 필수 파일 확인 (없어도 종료 안 함, 상태만 보고)
for f in player_cards_all.parquet statcast_ultra_full_clean.parquet id_map.csv; do
  if [ ! -s "$OUT/$f" ]; then
    log "[WARN] missing: $OUT/$f"
  else
    log "[OK] found: $OUT/$f"
  fi
done

# 1) 파이썬 의존 확인(없으면 설치 시도, 실패해도 계속)
for m in duckdb pandas pyarrow; do
  python3 - <<PY >/dev/null 2>&1
import importlib; importlib.import_module("$m")
PY
  if [ $? -ne 0 ]; then
    log "[PIP] install $m"
    pip install --no-cache-dir "$m" >/dev/null 2>&1 || log "[WARN] pip install $m failed (ignored)"
  fi
done

# 2) DuckDB 연결/뷰 생성 (TABLE/VIEW 충돌 안전 처리, 예외 무시하고 진행)
python3 - <<'PY'
import os, json, sys
try:
    import duckdb, pandas as pd
    root=os.getcwd(); out=f"{root}/output"; db=f"{root}/data/duckdb/main.duckdb"
    cards=f"{out}/player_cards_all.parquet"
    sc=f"{out}/statcast_ultra_full_clean.parquet"
    idm=f"{out}/id_map.csv"

    def esc(p:str)->str:  # SQL 리터럴용 경로 이스케이프
        return p.replace("'", "''")

    os.makedirs(os.path.dirname(db), exist_ok=True)
    con=duckdb.connect(db)

    # 안전 드롭(타입 불문)
    for name in ("player_cards","statcast","id_map","statcast_deep"):
        for stmt in (f"DROP VIEW IF EXISTS {name}", f"DROP TABLE IF EXISTS {name}"):
            try: con.execute(stmt)
            except Exception: pass

    # 존재하는 파일만 VIEW 생성 (바인드X, 리터럴 인라인)
    if os.path.isfile(cards) and os.path.getsize(cards)>0:
        con.execute(f"CREATE VIEW player_cards AS SELECT * FROM read_parquet('{esc(cards)}')")
    if os.path.isfile(sc) and os.path.getsize(sc)>0:
        con.execute(f"CREATE VIEW statcast AS SELECT * FROM read_parquet('{esc(sc)}')")
    if os.path.isfile(idm) and os.path.getsize(idm)>0:
        con.execute(f"CREATE VIEW id_map AS SELECT * FROM read_csv_auto('{esc(idm)}')")

    # statcast 딥지표 뷰 (컬럼 자동 감지)
    def colset(tbl):
        try:
            return {r[0].lower() for r in con.execute(f"DESCRIBE {tbl}").fetchall()}
        except Exception:
            return set()
    cols = colset("statcast")
    def pick(*cands):
        for c in cands:
            if c and c.lower() in cols: return c
        return None

    ev   = pick("ev","exit_velocity")
    la   = pick("la","launch_angle")
    velo = pick("release_speed","pitch_speed","release_velo")
    spin = pick("release_spin_rate","spin_rate")
    pt   = pick("pitch_name","pitch_type")
    pid  = pick("player_id","batter","pitcher")
    gdt  = pick("game_date","date")

    if cols:
        def sel(c): return c if c else "NULL"
        con.execute(f"""
        CREATE VIEW statcast_deep AS
        SELECT
          {sel(pid)}  AS player_id,
          {sel(gdt)}  AS game_date,
          {sel(ev)}   AS ev,
          {sel(la)}   AS la,
          {sel(pt)}   AS pitch_type,
          {sel(velo)} AS pitch_velo,
          {sel(spin)} AS spin_rate,
          CASE WHEN {sel(ev)} IS NOT NULL AND {sel(ev)}>=95 THEN 1 ELSE 0 END AS hard_hit,
          CASE WHEN {sel(ev)} IS NOT NULL AND {sel(ev)}>=98 AND {sel(la)} BETWEEN 26 AND 30 THEN 1 ELSE 0 END AS barrel_like
        FROM statcast
        """)

    def safe_count(tbl):
        try: return con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        except: return 0
    def safe_df(q):
        try: return con.execute(q).fetchdf()
        except: return pd.DataFrame()

    summary = dict(
      rows={
        "player_cards": safe_count("player_cards"),
        "statcast": safe_count("statcast"),
        "statcast_deep": safe_count("statcast_deep"),
        "id_map": safe_count("id_map"),
      },
      sample=safe_df("SELECT * FROM statcast_deep LIMIT 5").to_dict(orient="records")
    )
    os.makedirs(f"{out}/summaries", exist_ok=True)
    with open(f"{out}/summaries/live_data_link.json","w") as f:
        json.dump(summary,f,ensure_ascii=False,indent=2)
    print(json.dumps(summary,ensure_ascii=False))
except Exception as e:
    print(json.dumps({"error":str(e)}, ensure_ascii=False))
sys.exit(0)
PY

if [ -f "$SUM/live_data_link.json" ]; then
  log "[OK] summary saved → $SUM/live_data_link.json"
else
  log "[WARN] summary file missing (script continued anyway)"
fi

# 3) (옵션) 자동 커밋/푸시
if [ "${GIT_PUSH:-0}" = "1" ]; then
  log "[GIT] committing changes..."
  git add output/summaries/live_data_link.json data/duckdb/main.duckdb 2>/dev/null
  git commit -m "live: link real data + deep metrics (safe run)" >/dev/null 2>&1 || log "[GIT] nothing to commit"
  git push >/dev/null 2>&1 || log "[GIT] push skipped/failure"
fi

log "[LIVE-DATA SAFE] done  ✓"
