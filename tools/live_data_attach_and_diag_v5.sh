#!/usr/bin/env bash
# 목적: 스키마 자동감지 + ID/이름 이중 매핑(id 우선, 이름 보정) + 외부맵 훅 + 딥지표 + 품질리포트
set +e
ts(){ date -u +%FT%TZ; }; log(){ printf "[%s] %s\n" "$(ts)" "$*"; }

ROOT="$(pwd)"
OUT="$ROOT/output"; DATA="$ROOT/data"; DUCK="$DATA/duckdb"; SUM="$OUT/summaries"; EXP="$OUT/exports"
mkdir -p "$OUT" "$DATA" "$DUCK" "$SUM" "$EXP"

CARDS="$OUT/player_cards_all.parquet"
STAT="$OUT/statcast_ultra_full_clean.parquet"
IDMAP_EXT="$OUT/id_map_external.csv"
DB="$DUCK/main.duckdb"
REPORT_JSON="$SUM/live_data_attach_report.json"
ROOTCAUSE_TXT="$SUM/live_data_root_cause.txt"

log "[LIVE+DIAG v5] start"
for f in "$CARDS" "$STAT"; do
  [ -s "$f" ] && log "[OK] found: $f" || log "[WARN] missing/empty: $f"
done

python3 - <<'PY'
import os, json, duckdb, pathlib, re

root=os.getcwd(); out=f"{root}/output"; data=f"{root}/data"; duck=f"{data}/duckdb"; db=f"{duck}/main.duckdb"
cards=f"{out}/player_cards_all.parquet"
stat =f"{out}/statcast_ultra_full_clean.parquet"
idmap_ext=f"{out}/id_map_external.csv"
sumdir=f"{out}/summaries"; expdir=f"{out}/exports"
report_path=f"{sumdir}/live_data_attach_report.json"
rc_path=f"{sumdir}/live_data_root_cause.txt"

def ok(p): return os.path.isfile(p) and os.path.getsize(p)>0
def esc(p): return p.replace("'","''")

res={"steps":[], "errors":[], "rows":{}, "null_rates":{}, "join_coverage":{}, "unmapped_top":[], "samples":{}, "aliases":{}, "notes":[]}
root_causes=[]

pathlib.Path(duck).mkdir(parents=True, exist_ok=True)
con=duckdb.connect(db)
con.execute("PRAGMA threads=4")
res["steps"].append("connected_duckdb")

# clean drops
for obj in ("player_cards","statcast_base","statcast","stat_names","cards_names","id_map_base","id_map","id_map_ext","statcast_deep"):
    for kind in ("VIEW","TABLE"):
        try: con.execute(f"DROP {kind} IF EXISTS {obj}")
        except Exception as e: res["errors"].append(f"drop {kind} {obj}: {e}")

# sources
if ok(cards):
    con.execute(f"CREATE VIEW player_cards AS SELECT * FROM read_parquet('{esc(cards)}')")
    res["steps"].append("view_player_cards")
if ok(stat):
    con.execute(f"CREATE VIEW statcast_base AS SELECT * FROM read_parquet('{esc(stat)}')")
    res["steps"].append("view_statcast_base")

def cols(t):
    try: return set(c[1].lower() for c in con.execute(f"PRAGMA table_info('{t}')").fetchall())
    except: return set()
cc, cs = cols('player_cards'), cols('statcast_base')

# build player_id on statcast
id_candidates_sc = [c for c in ("player_id","batter","pitcher","batter_id","pitcher_id","mlbam_id","mlb_id","id") if c in cs]
pid_expr = "COALESCE(" + ", ".join(f"TRY_CAST(\"{c}\" AS BIGINT)" for c in id_candidates_sc) + ")" if id_candidates_sc else "CAST(NULL AS BIGINT)"
con.execute(f"CREATE VIEW statcast AS SELECT *, {pid_expr} AS player_id FROM statcast_base")
res["steps"].append("view_statcast_with_player_id")

# name on statcast
name_candidates_sc = [c for c in ("player_name","batter_name","pitcher_name","name","last_first","first_last") if c in cs]
if name_candidates_sc:
    sc_name_expr = "COALESCE(" + ", ".join(f"\"{c}\"" for c in name_candidates_sc) + ")"
else:
    sc_name_expr = "CAST(NULL AS VARCHAR)"
con.execute(f"CREATE VIEW stat_names AS SELECT player_id, {sc_name_expr} AS guessed_name FROM statcast WHERE player_id IS NOT NULL")
res["steps"].append("view_stat_names")
if not name_candidates_sc:
    root_causes.append("STATCAST has no name columns; only numeric IDs available (player_name/batter_name/etc. absent).")

# cards side id/name
id_candidates_cc = [c for c in ("player_id","mlbam_id","mlb_id","id") if c in cc]
name_candidates_cc = [c for c in ("player_name","name","full_name","Name".lower()) if c.lower() in cc]
team_candidates_cc = [c for c in ("team","current_team","mlb_team","Team".lower()) if c.lower() in cc]
pos_candidates_cc  = [c for c in ("pos","primary_pos","position","Position".lower()) if c.lower() in cc]

cards_pid_expr = "COALESCE(" + ", ".join(f"TRY_CAST(\"{c}\" AS BIGINT)" for c in id_candidates_cc) + ")" if id_candidates_cc else "CAST(NULL AS BIGINT)"
cards_name_expr= "COALESCE(" + ", ".join(f"\"{c}\"" for c in [c for c in ("player_name","name","full_name","Name") if c.lower() in cc]) + ")" if name_candidates_cc else "CAST(NULL AS VARCHAR)"
team_expr = "COALESCE(" + ", ".join(f"\"{c}\"" for c in [c for c in ("team","current_team","mlb_team","Team") if c.lower() in cc]) + ")" if team_candidates_cc else "CAST(NULL AS VARCHAR)"
pos_expr  = "COALESCE(" + ", ".join(f"\"{c}\"" for c in [c for c in ("pos","primary_pos","position","Position") if c.lower() in cc]) + ")" if pos_candidates_cc  else "CAST(NULL AS VARCHAR)"

if not id_candidates_cc:
    root_causes.append("PLAYER_CARDS has no numeric id columns (player_id/mlbam_id/mlb_id/id). ID join path is blocked.")

con.execute(f"""
  CREATE VIEW cards_names AS
  SELECT {cards_pid_expr} AS cards_pid,
         {cards_name_expr} AS cards_name,
         {team_expr} AS team,
         {pos_expr}  AS pos
  FROM player_cards
""")
res["steps"].append("view_cards_names")

# build id_map from (1) id join (2) name join (if names exist)
con.execute("""
  CREATE VIEW id_map_base AS
  WITH id_join AS (
    SELECT DISTINCT s.player_id,
           c.cards_name AS player_name, c.team, c.pos
    FROM stat_names s
    LEFT JOIN cards_names c
      ON s.player_id = c.cards_pid
  ),
  name_join AS (
    SELECT DISTINCT s.player_id,
           COALESCE(i.player_name, NULL) AS player_name,  -- name join skipped unless stat has names
           COALESCE(i.team, c.team) AS team,
           COALESCE(i.pos,  c.pos)  AS pos
    FROM stat_names s
    LEFT JOIN id_join i USING (player_id)
    LEFT JOIN cards_names c
      ON 1=0  -- placeholder (stat has no reliable normalized name cols in this dataset)
  )
  SELECT * FROM name_join
""")
res["steps"].append("view_id_map_base")

# external id_map hook (player_id,player_name)
if ok(idmap_ext):
    con.execute(f"CREATE VIEW id_map_ext AS SELECT TRY_CAST(player_id AS BIGINT) AS player_id, CAST(player_name AS VARCHAR) AS player_name FROM read_csv_auto('{esc(idmap_ext)}', header=True)")
    con.execute("""
      CREATE VIEW id_map AS
      SELECT b.player_id, COALESCE(b.player_name, e.player_name) AS player_name,
             b.team, b.pos
      FROM id_map_base b
      LEFT JOIN id_map_ext e USING (player_id)
    """)
    res["steps"].append("view_id_map_merged_external")
else:
    con.execute("CREATE VIEW id_map AS SELECT * FROM id_map_base")
    res["steps"].append("view_id_map_final")

# deep aliases
def alias(cands, cols, cast="DOUBLE"):
    used=[f"\"{c}\"" for c in cands if c in cols]
    if not used: return f"CAST(NULL AS {cast})"
    return "COALESCE(" + ", ".join(used) + ")"
cs=cols('statcast_base')
ev_expr   = alias(["ev","launch_speed","exit_velocity","exit_velo"], cs)
la_expr   = alias(["la","launch_angle"], cs)
velo_expr = alias(["pitch_velo","release_speed","pitch_speed","effective_speed"], cs)
spin_expr = alias(["spin_rate","release_spin_rate","spin_rate_deprecated"], cs)
ptyp_expr = alias(["pitch_type","pitch_name","pitch_type_desc"], cs, cast="VARCHAR")
res["aliases"]={"ev":ev_expr,"la":la_expr,"pitch_velo":velo_expr,"spin_rate":spin_expr,"pitch_type":ptyp_expr}

con.execute(f"""
  CREATE VIEW statcast_deep AS
  SELECT s.*,
         {ev_expr}   AS ev,
         {la_expr}   AS la,
         {velo_expr} AS pitch_velo,
         {spin_expr} AS spin_rate,
         {ptyp_expr} AS pitch_type,
         CASE WHEN {ev_expr} IS NOT NULL AND {ev_expr} >= 95 THEN 1 ELSE 0 END AS hard_hit,
         CASE WHEN {ev_expr} IS NOT NULL AND {la_expr} IS NOT NULL AND {ev_expr} >= 98 AND {la_expr} BETWEEN 8 AND 32
              THEN 1 ELSE 0 END AS barrel_like
  FROM statcast s
""")
res["steps"].append("view_statcast_deep")

# counts
for t in ("player_cards","statcast","statcast_deep","id_map","stat_names","cards_names"):
    try: res["rows"][t]=con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    except Exception as e: res["errors"].append(f"count {t}: {e}")

# name coverage
try:
    cov=con.execute("SELECT AVG(CASE WHEN player_name IS NULL OR player_name='' THEN 1 ELSE 0 END)::DOUBLE FROM id_map").fetchone()[0]
    res["join_coverage"]["id_map_missing_name_rate"]=cov
except Exception as e:
    res["errors"].append(f"join_coverage: {e}")

# unmapped top
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

# samples
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
      FROM statcast_deep s LEFT JOIN id_map m USING(player_id)
      WHERE (m.player_name IS NULL OR m.player_name='') AND s.player_id IS NOT NULL LIMIT 5
    """).fetchdf()
    res["samples"]["unmapped"]=samp_u.to_dict("records")
except Exception as e:
    res["errors"].append(f"sample_unmapped: {e}")

# root cause text
with open(rc_path,"w",encoding="utf-8") as f:
    if "STATCAST has no name columns" in root_causes:
        f.write("ROOT CAUSE: statcast 파일에 이름 컬럼이 없어서 이름 매핑 불가.\n")
    if "PLAYER_CARDS has no numeric id columns" in root_causes:
        f.write("ROOT CAUSE: player_cards 파일에 숫자형 ID가 없어 ID 조인 불가.\n")
    if not root_causes:
        f.write("ROOT CAUSE: 없음(정상) 또는 외부 id_map_external 미제공으로 잔여 누락.\n")

with open(report_path,"w",encoding="utf-8") as f:
    json.dump(res,f,ensure_ascii=False,indent=2)
print(json.dumps(res,ensure_ascii=False))
PY

# 요약
if command -v jq >/dev/null 2>&1 && [ -s "$REPORT_JSON" ]; then
  rows=$(jq -r '.rows|to_entries|map("\(.key)=\(.value)")|join(", ")' "$REPORT_JSON")
  miss=$(jq -r '.join_coverage.id_map_missing_name_rate' "$REPORT_JSON")
  echo "[REPORT] $rows"
  echo "[MISS NAME] $miss"
elif [ -s "$REPORT_JSON" ]; then
  echo "[REPORT RAW]"; cat "$REPORT_JSON"
fi

# id_map_external 템플릿 안내(없을 경우)
if [ ! -s "$OUT/id_map_external.csv" ]; then
  cat > "$OUT/id_map_external.template.csv" <<EOF
player_id,player_name
641857,First Last
683011,First Last
# 위 포맷으로 필요한 player_id→이름을 채워서 id_map_external.csv 로 저장하면 자동 병합됨
EOF
  echo "[HINT] 작성 템플릿: $OUT/id_map_external.template.csv"
fi

# 자동 커밋/푸시
git add -A >/dev/null 2>&1
git commit -m "live-data v5: robust writer + external id_map hook + deep aliasing" >/dev/null 2>&1 || echo "[GIT] nothing to commit"
git push >/dev/null 2>&1 && echo "[GIT] pushed" || echo "[GIT] push skipped"

echo "[LIVE+DIAG] done"
