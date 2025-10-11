#!/usr/bin/env bash
# Safe end-to-end runner (always exit 0)
set +e; set +u; set +o pipefail 2>/dev/null || true
trap '' ERR
ROOT="/workspaces/cogm-assistant"
OUT="$ROOT/output"; LOG="$ROOT/logs/statcast_end2end_v5.log"
mkdir -p "$OUT" "$ROOT/logs" "$OUT/summaries" "$OUT/reports"

echo "[START] $(date -u +%FT%TZ)" > "$LOG"

# 0) 마스터(없으면 재생성, 있으면 재사용)
if [ ! -f "$OUT/statcast_ultra_full.parquet" ]; then
  echo "[RUN] rebuild master" | tee -a "$LOG"
  python3 "$ROOT/pipeline/build_statcast_ultra_v4_3.py" >> "$LOG" 2>&1
else
  echo "[SKIP] master exists" | tee -a "$LOG"
fi

# 1) 카드 합본 없으면 재생성(이미 파이프라인 돌렸으면 스킵)
if [ ! -f "$OUT/player_cards_all.csv" ]; then
  echo "[RUN] build all cards" | tee -a "$LOG"
  FROM=1901 TO=2025 "$ROOT/pipeline/build_all_player_cards.sh" >> "$LOG" 2>&1
else
  echo "[SKIP] player_cards_all.csv exists" | tee -a "$LOG"
fi

# 2) 시퀀스/전환/엔트로피/반복률 계산 + 카드에 머지 (pandas 등록법으로 헤더 이슈 회피)
python3 - <<'PY' >> '"$LOG"' 2>&1
import duckdb, pandas as pd

PARQ = 'output/statcast_ultra_full.parquet'
CARDS= 'output/player_cards_all.csv'
ENR  = 'output/player_cards_enriched_all_seq.csv'

con = duckdb.connect()
con.execute("PRAGMA threads=4")
con.execute("PRAGMA memory_limit='1024MB'")
con.execute(f"CREATE OR REPLACE VIEW sc AS SELECT * FROM read_parquet('{PARQ}')")

# 시즌·이름 단위 분포 → 지배구종/엔트로피
con.execute("""
WITH base AS (
  SELECT CAST(year AS INT) AS season,
         TRIM(player_name) AS name,
         COALESCE(pitch_type,'UNK') AS pitch_type,
         game_pk
  FROM sc
  WHERE player_name IS NOT NULL
),
pt AS (
  SELECT season, name, pitch_type, COUNT(*) AS n
  FROM base GROUP BY 1,2,3
),
tot AS (SELECT season, name, SUM(n) AS tot FROM pt GROUP BY 1,2),
dist AS (
  SELECT p.season, p.name, p.pitch_type, p.n, t.tot, (p.n*1.0)/NULLIF(t.tot,0) AS p
  FROM pt p JOIN tot t USING(season,name)
),
dist_rank AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY season,name ORDER BY p DESC, n DESC) AS rk
  FROM dist
),
dom AS (
  SELECT season, name, pitch_type AS dominant_pitch FROM dist_rank WHERE rk=1
),
ent AS (
  SELECT season, name, -SUM(CASE WHEN p>0 THEN p*ln(p) ELSE 0 END) AS usage_entropy
  FROM dist GROUP BY 1,2
),
w AS (
  SELECT season, name, game_pk, pitch_type,
         LAG(pitch_type) OVER (PARTITION BY season,name ORDER BY season,game_pk) AS prev_pt
  FROM base
),
rep AS (
  SELECT season, name,
         SUM(CASE WHEN pitch_type=prev_pt THEN 1 ELSE 0 END) AS repeat_cnt,
         SUM(CASE WHEN prev_pt IS NOT NULL THEN 1 ELSE 0 END) AS seq_cnt
  FROM w GROUP BY 1,2
),
rr AS (
  SELECT season, name,
         CASE WHEN seq_cnt>0 THEN (repeat_cnt*1.0)/seq_cnt ELSE NULL END AS repeat_rate
  FROM rep
)
SELECT d.season, d.name, d.dominant_pitch, e.usage_entropy, rr.repeat_rate
FROM dom d
LEFT JOIN ent e USING(season,name)
LEFT JOIN rr  USING(season,name)
""")
feat = con.fetch_df()
feat['name_norm'] = feat['name'].astype(str).str.strip()

cards = pd.read_csv(CARDS)
cards['name_norm'] = cards['name'].astype(str).str.strip()
enriched = cards.merge(
    feat[['season','name_norm','dominant_pitch','usage_entropy','repeat_rate']],
    left_on=['season','name_norm'], right_on=['season','name_norm'], how='left'
)
enriched.to_csv(ENR, index=False)
print(f"[OK] enriched -> {ENR} rows={len(enriched)}")

# 스모크: 범위/구종 Top
print(duckdb.query("SELECT COUNT(*) rows, MIN(year) min_y, MAX(year) max_y FROM sc").fetch_df())
print(duckdb.query("SELECT pitch_type, COUNT(*) n FROM sc GROUP BY 1 ORDER BY n DESC LIMIT 10").fetch_df())
con.close()
PY

echo "[DONE] $(date -u +%FT%TZ)" | tee -a "$LOG"
exit 0
