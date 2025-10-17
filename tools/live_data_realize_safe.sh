#!/usr/bin/env bash
# 목적: 실데이터 연결 + DuckDB 뷰 재생성 + 딥지표 활성화 + 상태 JSON 출력 (샘플 금지)
# 특징: set +e, Python try/except, DDL에는 문자열 삽입만(파라미터 바인딩 금지), RC=0 보장
set +e
ts(){ date -u +%FT%TZ; }; log(){ printf "[%s] %s\n" "$(ts)" "$*"; }

ROOT="$(pwd)"
OUT="$ROOT/output"; DATA="$ROOT/data"; DUCK="$DATA/duckdb"; SUM="$OUT/summaries"
mkdir -p "$OUT" "$DUCK" "$SUM"

CARDS="$OUT/player_cards_all.parquet"
STAT="$OUT/statcast_ultra_full_clean.parquet"
IDMAP="$OUT/id_map.csv"
DB="$DUCK/main.duckdb"

log "[LIVE] start"
for f in "$CARDS" "$STAT" "$IDMAP"; do
  if [ -s "$f" ]; then log "[LIVE] OK file: $f"; else log "[LIVE] WARN missing: $f"; fi
done

python3 - <<'PY'
import os, json, duckdb

root=os.getcwd()
out=os.path.join(root,"output")
db =os.path.join(root,"data","duckdb","main.duckdb")
cards=os.path.join(out,"player_cards_all.parquet")
stat =os.path.join(out,"statcast_ultra_full_clean.parquet")
idmap=os.path.join(out,"id_map.csv")
summary=os.path.join(out,"summaries","live_data_status.json")

def safe(p): return os.path.isfile(p) and os.path.getsize(p)>0
def sql_quote_path(p): return "'" + p.replace("'", "''") + "'"

res={"errors":[], "rows":{}, "name_join_missing_rate": None}

try:
    con=duckdb.connect(db)
    con.execute("PRAGMA threads=4")

    # 충돌 정리 (VIEW/TABLE 모두)
    for obj in ("player_cards","statcast","statcast_deep","id_map","pit_mix_by_count"):
        for kind in ("VIEW","TABLE"):
            try: con.execute(f"DROP {kind} IF EXISTS {obj}")
            except: pass

    # 원천 뷰 (DDL에는 절대 바인딩 사용하지 않음)
    if safe(cards):
        con.execute("CREATE VIEW player_cards AS SELECT * FROM read_parquet(" + sql_quote_path(cards) + ")")
    if safe(stat):
        con.execute("CREATE VIEW statcast AS SELECT * FROM read_parquet(" + sql_quote_path(stat) + ")")

    # id_map: 없거나 LFS 포인터면 cards에서 동적 생성
    if safe(idmap):
        try:
            head=open(idmap,'r',encoding='utf-8',errors='ignore').read(256)
        except:
            head=""
        if 'version https://git-lfs.github.com/spec/v1' in head or os.path.getsize(idmap)<10:
            con.execute("""
                CREATE VIEW id_map AS
                SELECT DISTINCT
                  COALESCE(player_id, id) AS player_id,
                  COALESCE(player_name, name) AS player_name,
                  COALESCE(team, current_team, team_name, team_abbrev) AS team,
                  COALESCE(pos, primary_pos, position, def_pos) AS pos
                FROM player_cards
            """)
        else:
            con.execute("CREATE VIEW id_map AS SELECT * FROM read_csv_auto(" + sql_quote_path(idmap) + ", AUTO_DETECT=TRUE, SAMPLE_SIZE=-1)")
    else:
        con.execute("""
            CREATE VIEW id_map AS
            SELECT DISTINCT
              COALESCE(player_id, id) AS player_id,
              COALESCE(player_name, name) AS player_name,
              COALESCE(team, current_team, team_name, team_abbrev) AS team,
              COALESCE(pos, primary_pos, position, def_pos) AS pos
            FROM player_cards
        """)

    # 딥지표(존재 컬럼만 사용)
    cols = set()
    if safe(stat):
        cols = {r[0].lower() for r in con.execute("DESCRIBE statcast").fetchall()}
    ev = "NULL"; la = "NULL"
    if ("ev" in cols) and ("launch_speed" in cols): ev="COALESCE(ev, launch_speed)"
    elif "ev" in cols: ev="ev"
    elif "launch_speed" in cols: ev="launch_speed"
    if ("la" in cols) and ("launch_angle" in cols): la="COALESCE(la, launch_angle)"
    elif "la" in cols: la="la"
    elif "launch_angle" in cols: la="launch_angle"

    if safe(stat):
        con.execute("""
        CREATE VIEW statcast_deep AS
        SELECT s.*,
               CASE WHEN {ev} IS NOT NULL AND {ev}>=95 THEN 1 ELSE 0 END AS hard_hit,
               CASE WHEN {ev} IS NOT NULL AND {la} IS NOT NULL AND {ev}>=98 AND {la} BETWEEN 8 AND 32 THEN 1 ELSE 0 END AS barrel_like
        FROM statcast s
        """.format(ev=ev, la=la))

    # 집계만 기록 (샘플 금지)
    for name in ("player_cards","statcast","statcast_deep","id_map"):
        try:
            res["rows"][name]=con.execute("SELECT COUNT(*) FROM " + name).fetchone()[0]
        except Exception as e:
            res["rows"][name]=0
            res["errors"].append(f"{name}: {e}")

    # 이름 조인 누락률(비율만)
    try:
        res["name_join_missing_rate"]=con.execute("""
            SELECT
              SUM(CASE WHEN m.player_name IS NULL OR m.player_name='' THEN 1 ELSE 0 END)::DOUBLE
              / NULLIF(COUNT(*),0)
            FROM statcast s
            LEFT JOIN id_map m ON m.player_id = s.player_id
        """).fetchone()[0]
    except Exception as e:
        res["errors"].append(f"name_join_missing_rate: {e}")

except Exception as e:
    res["errors"].append(str(e))

os.makedirs(os.path.dirname(summary), exist_ok=True)
with open(summary,'w',encoding='utf-8') as f:
    json.dump(res,f,ensure_ascii=False,indent=2)
print(json.dumps(res,ensure_ascii=False))
PY

# 상태 출력 (요약만)
if [ -s "$SUM/live_data_status.json" ]; then
  log "[LIVE] status:"
  sed -n '1,120p' "$SUM/live_data_status.json"
else
  log "[LIVE] WARN: no status json"
fi

# 항상 0 종료
exit 0
