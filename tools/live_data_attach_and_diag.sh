#!/usr/bin/env bash
# 목적: 실데이터 연결 + DuckDB 뷰 재구성 + 이름매핑 자동재생성(조인 기반) + 딥지표 생성 +
#       품질진단(누락/NULL/커버리지) → 리포트 저장 → 자동 커밋/푸시
# 특징: set +e (실패해도 터미널 안죽음), LFS 포인터 자동 감지, 조인 기반 id_map 생성으로 Binder 에러 제거

set +e
ts(){ date -u +%FT%TZ; }; log(){ printf "[%s] %s\n" "$(ts)" "$*"; }

ROOT="$(pwd)"
OUT="$ROOT/output"; DATA="$ROOT/data"; DUCK="$DATA/duckdb"; SUM="$OUT/summaries"; EXP="$OUT/exports"
mkdir -p "$OUT" "$DATA" "$DUCK" "$SUM" "$EXP"

chmod +x tools/*.sh 2>/dev/null || true
sed -i 's/\r$//' tools/*.sh 2>/dev/null || true

CARDS="$OUT/player_cards_all.parquet"
STAT="$OUT/statcast_ultra_full_clean.parquet"
IDMAP="$OUT/id_map.csv"
DB="$DUCK/main.duckdb"
REPORT_JSON="$SUM/live_data_attach_report.json"

log "[LIVE+DIAG] start"
for f in "$CARDS" "$STAT" "$IDMAP"; do
  [ -s "$f" ] && log "[OK] found: $f" || log "[WARN] missing/empty: $f"
done

python3 - <<'PY'
import os, json, duckdb, pathlib, sys

root=os.getcwd(); out=f"{root}/output"; data=f"{root}/data"; duck=f"{data}/duckdb"; db=f"{duck}/main.duckdb"
cards=f"{out}/player_cards_all.parquet"
stat =f"{out}/statcast_ultra_full_clean.parquet"
idmap_csv=f"{out}/id_map.csv"
sumdir=f"{out}/summaries"; expdir=f"{out}/exports"
report_path=f"{sumdir}/live_data_attach_report.json"

def ok(p): return os.path.isfile(p) and os.path.getsize(p)>0
def esc(p): return p.replace("'","''")
def is_lfs_pointer(p):
    if not ok(p): return False
    try:
        head=open(p,'r',encoding='utf-8',errors='ignore').read(256)
        return "version https://git-lfs.github.com/spec/v1" in head
    except: return False

res={"steps":[], "errors":[], "rows":{}, "null_rates":{}, "join_coverage":{}, "unmapped_top":[], "samples":{}}

try:
    pathlib.Path(duck).mkdir(parents=True, exist_ok=True)
    con=duckdb.connect(db)
    con.execute("PRAGMA threads=4")
    res["steps"].append("connected_duckdb")

    # 깨끗한 상태로 드롭 (뷰/테이블 구분 없이 시도)
    for obj in ("player_cards","statcast","statcast_deep","id_map"):
        for kind in ("VIEW","TABLE"):
            try: con.execute(f"DROP {kind} IF EXISTS {obj}")
            except Exception as e: res["errors"].append(f"drop {kind} {obj}: {e}")

    # 뷰 생성 (파라미터 바인딩 대신 리터럴 경로)
    if ok(cards):
        con.execute(f"CREATE VIEW player_cards AS SELECT * FROM read_parquet('{esc(cards)}')")
        res["steps"].append("view_player_cards")
    if ok(stat):
        con.execute(f"CREATE VIEW statcast AS SELECT * FROM read_parquet('{esc(stat)}')")
        res["steps"].append("view_statcast")

    # id_map 생성 로직 (조인 기반) — statcast.player_id ↔ player_cards 후보 키 매칭
    if ok(stat) and ok(cards):
        con.execute("""
            CREATE VIEW id_map AS
            WITH c AS (SELECT * FROM player_cards),
            m AS (
              SELECT DISTINCT
                s.player_id,
                COALESCE(c.player_name, c.name, c.full_name) AS player_name,
                COALESCE(c.team, c.current_team) AS team,
                COALESCE(c.pos, c.primary_pos) AS pos
              FROM statcast s
              LEFT JOIN c
                ON  TRY_CAST(c.player_id AS BIGINT)=s.player_id
                 OR TRY_CAST(c.id        AS BIGINT)=s.player_id
                 OR TRY_CAST(c.mlb_id    AS BIGINT)=s.player_id
                 OR TRY_CAST(c.mlbam_id  AS BIGINT)=s.player_id
            )
            SELECT * FROM m
        """)
        res["steps"].append("view_id_map_join_based")
    elif ok(idmap_csv) and not is_lfs_pointer(idmap_csv):
        con.execute(f"CREATE VIEW id_map AS SELECT * FROM read_csv_auto('{esc(idmap_csv)}')")
        res["steps"].append("view_id_map_from_csv")
    else:
        # 최후 fallback: 카드만으로 유추(매칭 X, 정보만 노출)
        con.execute("""
            CREATE VIEW id_map AS
            SELECT DISTINCT
              COALESCE(player_id, id, mlb_id, mlbam_id) AS player_id,
              COALESCE(player_name, name, full_name)     AS player_name,
              COALESCE(team, current_team)               AS team,
              COALESCE(pos, primary_pos)                 AS pos
            FROM player_cards
        """)
        res["steps"].append("view_id_map_from_cards_only")

    # 딥지표
    if ok(stat):
        con.execute("""
          CREATE VIEW statcast_deep AS
          SELECT
            s.*,
            CASE WHEN ev>=95 THEN 1 ELSE 0 END AS hard_hit,
            CASE WHEN ev>=98 AND la BETWEEN 8 AND 32 THEN 1 ELSE 0 END AS barrel_like
          FROM statcast s
        """)
        res["steps"].append("view_statcast_deep")

    # 행수
    for t in ("player_cards","statcast","statcast_deep","id_map"):
        try: res["rows"][t]=con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        except Exception as e: res["errors"].append(f"count {t}: {e}")

    # NULL률(존재 컬럼만 시도)
    def null_rate(table, col):
        try:
            return con.execute(f"SELECT SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END)::DOUBLE/NULLIF(COUNT(*),0) FROM {table}").fetchone()[0]
        except: return None
    for c in ["ev","la","pitch_velo","spin_rate","player_id","game_date","pitch_type"]:
        res["null_rates"][c]=null_rate("statcast", c)

    # 이름 매핑 누락율
    try:
        cov=con.execute("""
          SELECT
            SUM(CASE WHEN player_name IS NULL OR player_name='' THEN 1 ELSE 0 END)::DOUBLE/NULLIF(COUNT(*),0) AS miss_rate
          FROM id_map
        """).fetchone()[0]
        res["join_coverage"]["id_map_missing_name_rate"]=cov
    except Exception as e:
        res["errors"].append(f"join_coverage: {e}")

    # 미매핑 TOP10
    try:
        unmapped=con.execute("""
          SELECT s.player_id, COUNT(*) AS n
          FROM statcast s
          LEFT JOIN id_map m USING (player_id)
          WHERE s.player_id IS NOT NULL AND (m.player_name IS NULL OR m.player_name='')
          GROUP BY 1 ORDER BY n DESC LIMIT 10
        """).fetchdf()
        unmapped.to_csv(f"{expdir}/unmapped_player_ids_top10.csv", index=False)
        res["unmapped_top"]=unmapped.to_dict("records")
    except Exception as e:
        res["errors"].append(f"unmapped_top: {e}")

    # 샘플(매핑된/미매핑)
    try:
        samp_m=con.execute("""
          SELECT s.player_id, m.player_name, s.game_date, s.pitch_type, s.ev, s.la, s.pitch_velo, s.spin_rate, s.hard_hit, s.barrel_like
          FROM statcast_deep s LEFT JOIN id_map m USING(player_id)
          WHERE m.player_name IS NOT NULL AND m.player_name<>'' LIMIT 5
        """).fetchdf()
        res["samples"]["mapped"]=samp_m.to_dict("records")
    except Exception as e:
        res["errors"].append(f"sample_mapped: {e}")
    try:
        samp_u=con.execute("""
          SELECT s.player_id, s.game_date, s.pitch_type, s.ev, s.la, s.pitch_velo, s.spin_rate
          FROM statcast s LEFT JOIN id_map m USING(player_id)
          WHERE (m.player_name IS NULL OR m.player_name='') AND s.player_id IS NOT NULL LIMIT 5
        """).fetchdf()
        res["samples"]["unmapped"]=samp_u.to_dict("records")
    except Exception as e:
        res["errors"].append(f"sample_unmapped: {e}")

    # 품질 요약
    try:
        qa=con.execute("""
          SELECT
            COUNT(*) AS n,
            SUM(CASE WHEN ev>=95 THEN 1 ELSE 0 END) AS hard_hits,
            SUM(CASE WHEN ev>=98 AND la BETWEEN 8 AND 32 THEN 1 ELSE 0 END) AS barrel_like,
            AVG(ev) AS ev_avg, AVG(la) AS la_avg, AVG(spin_rate) AS spin_avg
          FROM statcast
        """).fetchdf()
        qa.to_csv(f"{expdir}/statcast_quality_summary.csv", index=False)
        res["steps"].append("export_quality_summary")
    except Exception as e:
        res["errors"].append(f"quality_summary: {e}")

except Exception as e:
    res["errors"].append(str(e))

os.makedirs(sumdir, exist_ok=True)
with open(report_path,"w",encoding="utf-8") as f:
    json.dump(res,f,ensure_ascii=False,indent=2)
print(json.dumps(res,ensure_ascii=False))
PY

# 요약 출력 (jq 없으면 cat)
if command -v jq >/dev/null 2>&1 && [ -s "$REPORT_JSON" ]; then
  rows=$(jq -r '.rows|to_entries|map("\(.key)=\(.value)")|join(", ")' "$REPORT_JSON")
  miss=$(jq -r '.join_coverage.id_map_missing_name_rate' "$REPORT_JSON")
  echo "[REPORT] $rows"
  echo "[MISS NAME] $miss"
elif [ -s "$REPORT_JSON" ]; then
  echo "[REPORT RAW]"
  cat "$REPORT_JSON"
else
  echo "[WARN] report json missing: $REPORT_JSON"
fi

# 자동 커밋/푸시
git add -A >/dev/null 2>&1
git commit -m "live-data: attach + deep metrics + join-based id_map + diagnostics (report+exports)" >/dev/null 2>&1 || echo "[GIT] nothing to commit"
git push >/dev/null 2>&1 && echo "[GIT] pushed" || echo "[GIT] push skipped"

echo "[LIVE+DIAG] done"
