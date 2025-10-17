#!/usr/bin/env bash
# 목적: 실데이터 연결 + DuckDB 뷰 재구성 + (컬럼 자동 감지) 이름매핑 + 딥지표 + 품질리포트 + 자동 커밋/푸시
# 특징:
# - set +e: 실패해도 터미널 안죽음
# - LFS 포인터 자동 감지
# - player_cards 스키마를 DuckDB로 조회하여 "실제 존재하는 컬럼만"으로 조인/COALESCE 생성

set +e
ts(){ date -u +%FT%TZ; }; log(){ printf "[%s] %s\n" "$(ts)" "$*"; }

ROOT="$(pwd)"
OUT="$ROOT/output"; DATA="$ROOT/data"; DUCK="$DATA/duckdb"; SUM="$OUT/summaries"; EXP="$OUT/exports"
mkdir -p "$OUT" "$DATA" "$DUCK" "$SUM" "$EXP"

CARDS="$OUT/player_cards_all.parquet"
STAT="$OUT/statcast_ultra_full_clean.parquet"
IDMAP="$OUT/id_map.csv"
DB="$DUCK/main.duckdb"
REPORT_JSON="$SUM/live_data_attach_report.json"

log "[LIVE+DIAG v3] start"
for f in "$CARDS" "$STAT" "$IDMAP"; do
  [ -s "$f" ] && log "[OK] found: $f" || log "[WARN] missing/empty: $f"
done

python3 - <<'PY'
import os, json, duckdb, pathlib

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

    # 깨끗하게 드롭
    for obj in ("player_cards","statcast","statcast_deep","id_map"):
        for kind in ("VIEW","TABLE"):
            try: con.execute(f"DROP {kind} IF EXISTS {obj}")
            except Exception as e: res["errors"].append(f"drop {kind} {obj}: {e}")

    # 뷰 생성
    if ok(cards):
        con.execute(f"CREATE VIEW player_cards AS SELECT * FROM read_parquet('{esc(cards)}')")
        res["steps"].append("view_player_cards")
    if ok(stat):
        con.execute(f"CREATE VIEW statcast AS SELECT * FROM read_parquet('{esc(stat)}')")
        res["steps"].append("view_statcast")

    # player_cards 실제 컬럼 목록 조회
    cols_cards=set()
    try:
        df_cols=con.execute("PRAGMA table_info('player_cards')").fetchdf()
        cols_cards=set(df_cols['name'].str.lower().tolist())
    except Exception as e:
        res["errors"].append(f"table_info player_cards: {e}")

    # 후보 키/이름/팀/포지션 컬럼 중 실제 존재하는 것만 고른다
    def pick_first(cands):
        for c in cands:
            if c.lower() in cols_cards:
                return c
        return None

    id_candidates   = ["player_id","id","mlb_id","mlbam_id","fg_id","bbref_id","retro_id"]
    name_candidates = ["player_name","name","full_name","Name"]
    team_candidates = ["team","current_team","mlb_team","Team"]
    pos_candidates  = ["pos","primary_pos","position","Position"]

    # 존재 리스트
    id_cols   = [c for c in id_candidates   if c.lower() in cols_cards]
    name_cols = [c for c in name_candidates if c.lower() in cols_cards]
    team_cols = [c for c in team_candidates if c.lower() in cols_cards]
    pos_cols  = [c for c in pos_candidates  if c.lower() in cols_cards]

    # COALESCE 구성 (없으면 NULL)
    def coalesce_expr(cols):
        if not cols: return "NULL"
        return "COALESCE(" + ", ".join(cols) + ")"

    name_expr = coalesce_expr(name_cols)
    team_expr = coalesce_expr(team_cols)
    pos_expr  = coalesce_expr(pos_cols)

    # 조인식 구성: 존재하는 id 후보들만 OR로 묶어서 statcast.player_id와 매칭
    join_ors=[]
    for c in id_cols:
        join_ors.append(f"TRY_CAST(c.\"{c}\" AS BIGINT)=s.player_id")
    join_clause=" OR ".join(join_ors)

    # id_map 생성 우선순위:
    # 1) statcast & player_cards가 있으면 조인 기반 (존재 컬럼로만)
    # 2) id_map.csv가 실제 파일이면 csv 사용
    # 3) 마지막 fallback: cards만으로 distinct 추출
    if ok(stat) and ok(cards) and join_clause:
        sql=f"""
            CREATE VIEW id_map AS
            WITH c AS (SELECT * FROM player_cards),
            m AS (
              SELECT DISTINCT
                s.player_id,
                {name_expr} AS player_name,
                {team_expr} AS team,
                {pos_expr}  AS pos
              FROM statcast s
              LEFT JOIN c ON {join_clause}
            )
            SELECT * FROM m
        """
        con.execute(sql)
        res["steps"].append("view_id_map_join_based")
    elif ok(idmap_csv) and not is_lfs_pointer(idmap_csv):
        con.execute(f"CREATE VIEW id_map AS SELECT * FROM read_csv_auto('{esc(idmap_csv)}')")
        res["steps"].append("view_id_map_from_csv")
    elif ok(cards):
        # fallback: id 추정 (존재하는 id 후보 중 첫번째를 player_id로 노출)
        pid = pick_first(id_candidates)
        pid_expr = f"\"{pid}\"" if pid else "NULL"
        sql=f"""
            CREATE VIEW id_map AS
            SELECT DISTINCT
              TRY_CAST({pid_expr} AS BIGINT) AS player_id,
              {name_expr} AS player_name,
              {team_expr} AS team,
              {pos_expr}  AS pos
            FROM player_cards
        """
        con.execute(sql)
        res["steps"].append("view_id_map_from_cards_only")
    else:
        # 아무 것도 없으면 빈 뷰
        con.execute("CREATE VIEW id_map AS SELECT CAST(NULL AS BIGINT) AS player_id, CAST(NULL AS VARCHAR) AS player_name, CAST(NULL AS VARCHAR) AS team, CAST(NULL AS VARCHAR) AS pos LIMIT 0")
        res["steps"].append("view_id_map_empty")

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

    # NULL률
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
        pathlib.Path(expdir).mkdir(parents=True, exist_ok=True)
        unmapped.to_csv(f"{expdir}/unmapped_player_ids_top10.csv", index=False)
        res["unmapped_top"]=unmapped.to_dict("records")
    except Exception as e:
        res["errors"].append(f"unmapped_top: {e}")

    # 샘플(매핑/미매핑)
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

except Exception as e:
    res["errors"].append(str(e))

pathlib.Path(sumdir).mkdir(parents=True, exist_ok=True)
with open(report_path,"w",encoding="utf-8") as f:
    json.dump(res,f,ensure_ascii=False,indent=2)
print(json.dumps(res,ensure_ascii=False))
PY

# 요약 출력 (jq 있으면 보기 좋게)
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

# 자동 커밋/푸시 (조용히)
git add -A >/dev/null 2>&1
git commit -m "live-data v3: dynamic-column join for id_map + deep metrics + diagnostics (report+exports)" >/dev/null 2>&1 || echo "[GIT] nothing to commit"
git push >/dev/null 2>&1 && echo "[GIT] pushed" || echo "[GIT] push skipped"

echo "[LIVE+DIAG] done"
