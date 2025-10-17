#!/usr/bin/env bash
set +e
ts(){ date -u +%FT%TZ; }; log(){ printf "[%s] %s\n" "$(ts)" "$*"; }

OUT="output"
CARDS="$OUT/player_cards_all.parquet"
STAT="$OUT/statcast_ultra_full_clean.parquet"
IDMAP="$OUT/id_map.csv"
AUDIT="$OUT/idmap_audit.csv"
QC="$OUT/id_map_qc.txt"

log "[CROSSMAP] start"
[ ! -f "$CARDS" ] && log "[ERR] missing $CARDS" && exit 0
[ ! -f "$STAT" ] && log "[ERR] missing $STAT" && exit 0

# 백업
[ -f "$IDMAP" ] && cp -f "$IDMAP" "$OUT/id_map.before_crossmap.csv"

python - <<'PY' || true
import duckdb, json
CARDS="output/player_cards_all.parquet"
STAT ="output/statcast_ultra_full_clean.parquet"
IDMAP="output/id_map.csv"
AUDIT="output/idmap_audit.csv"
QC   ="output/id_map_qc.txt"

con = duckdb.connect(database=":memory:")
def esc(p): return p.replace("'","''")

# 1) 소스 뷰
con.execute(f"CREATE VIEW cards AS SELECT * FROM read_parquet('{esc(CARDS)}')")
con.execute(f"CREATE VIEW stat  AS SELECT * FROM read_parquet('{esc(STAT)}')")

# 2) 이름 정규화 함수 (성, 이름 → 이름 성) + 소문자 + 공백정리 + 비문자 제거
norm = """
lower(
  regexp_replace(
    trim(
      case when position(',' in name)>0
           then concat(trim(split_part(name,',',2)),' ',trim(split_part(name,',',1)))
           else name end
    ),
    '[^a-z ]',''
  )
)
"""

# cards: name_full 기준 (내부 UID는 건드리지 않음)
con.execute(f"""
  CREATE OR REPLACE VIEW cards_names AS
  SELECT
    *, {norm.replace('name','name_full')} AS name_norm
  FROM cards
  WHERE name_full IS NOT NULL AND length(trim(name_full))>0
""")

# stat: batter/pitcher → MLBAM ID + 표준화된 이름
con.execute(f"""
  CREATE OR REPLACE VIEW stat_names_raw AS
  SELECT batter AS mlb_id, player_name AS name FROM stat WHERE batter IS NOT NULL
  UNION ALL
  SELECT pitcher AS mlb_id, player_name AS name FROM stat WHERE pitcher IS NOT NULL
""")
con.execute(f"""
  CREATE OR REPLACE VIEW stat_names AS
  SELECT
    mlb_id::BIGINT AS mlb_id,
    name,
    {norm.replace('name','name')} AS name_norm
  FROM stat_names_raw
  WHERE name IS NOT NULL AND length(trim(name))>0
""")

# 3) MLBID별 대표 이름(최빈값, 동률 시 사전순)
con.execute("""
  CREATE OR REPLACE VIEW stat_rep AS
  SELECT mlb_id, name_norm, count(*) AS cnt
  FROM stat_names
  GROUP BY mlb_id, name_norm
""")
con.execute("""
  CREATE OR REPLACE VIEW stat_pick AS
  SELECT s1.mlb_id, s1.name_norm
  FROM stat_rep s1
  JOIN (
    SELECT mlb_id, max(cnt) AS mxc FROM stat_rep GROUP BY mlb_id
  ) mx ON s1.mlb_id=mx.mlb_id AND s1.cnt=mx.mxc
""")

# 4) 이름 교차매핑
con.execute("""
  CREATE OR REPLACE VIEW name_link AS
  SELECT c.name_full, c.team_id, sp.mlb_id
  FROM cards_names c
  JOIN stat_pick  sp ON c.name_norm = sp.name_norm
""")

# 5) id_map 결과
con.execute("""
  CREATE OR REPLACE VIEW id_map_v AS
  SELECT DISTINCT
    mlb_id        AS player_id,
    name_full     AS player_name,
    team_id       AS team,
    CAST(NULL AS VARCHAR) AS pos
  FROM name_link
  WHERE mlb_id IS NOT NULL
""")

# 저장
con.execute(f"COPY (SELECT * FROM id_map_v ORDER BY player_id) TO '{esc(IDMAP)}' (HEADER, DELIMITER ',')")
con.execute(f"COPY (SELECT player_id, player_name, team FROM id_map_v) TO '{esc(AUDIT)}' (HEADER, DELIMITER ',')")

# 6) 커버리지 측정
total_ids, mapped_ids = con.execute("""
  WITH stat_ids AS (
    SELECT mlb_id FROM (
      SELECT batter AS mlb_id FROM stat WHERE batter IS NOT NULL
      UNION
      SELECT pitcher AS mlb_id FROM stat WHERE pitcher IS NOT NULL
    ) GROUP BY mlb_id
  ),
  hit AS (
    SELECT s.mlb_id
    FROM stat_ids s LEFT JOIN id_map_v m ON s.mlb_id=m.player_id
    WHERE m.player_id IS NOT NULL
  )
  SELECT
    (SELECT count(*) FROM stat_ids),
    (SELECT count(*) FROM hit)
""").fetchone()
rate = 1.0 - (mapped_ids / total_ids if total_ids else 0.0)

with open(QC,"w") as f:
    f.write(f"total_ids={total_ids}\nmapped_ids={mapped_ids}\nmiss_name_rate={rate:.6f}\n")

print(json.dumps({
  "built_id_map_rows": con.execute("SELECT COUNT(*) FROM id_map_v").fetchone()[0],
  "total_stat_unique_ids": total_ids,
  "mapped_ids": mapped_ids,
  "miss_name_rate": round(rate,6)
}, ensure_ascii=False))
PY

log "[CROSSMAP] wrote $IDMAP (and $AUDIT, $QC)"
[ -f "$QC" ] && sed -n '1,20p' "$QC" || true
log "[CROSSMAP] done"
