#!/usr/bin/env bash
set -euo pipefail
ts(){ date -u +%FT%TZ; }
log(){ printf "[%s] %s\n" "$(ts)" "$*"; }

ROOT="$(pwd)"
OUT="$ROOT/output"; DATA="$ROOT/data"; DUCK="$DATA/duckdb"; SUM="$OUT/summaries"
mkdir -p "$OUT" "$DUCK" "$SUM"

log "[LIVE-DATA v3] start"

python3 - <<'PY'
import os, json, duckdb, pandas as pd
root=os.getcwd(); out=f"{root}/output"; db=f"{root}/data/duckdb/main.duckdb"
cards=f"{out}/player_cards_all.parquet"
sc=f"{out}/statcast_ultra_full_clean.parquet"
idm=f"{out}/id_map.csv"

con=duckdb.connect(db)
for name in ("player_cards","statcast","id_map","statcast_deep"):
    con.execute(f"DROP VIEW IF EXISTS {name}")
    con.execute(f"DROP TABLE IF EXISTS {name}")

def j(s): return json.dumps(s)
con.execute(f"CREATE VIEW player_cards AS SELECT * FROM read_parquet({j(cards)})")
con.execute(f"CREATE VIEW statcast      AS SELECT * FROM read_parquet({j(sc)})")
con.execute(f"CREATE VIEW id_map        AS SELECT * FROM read_csv_auto({j(idm)})")

cols = {r[0].lower() for r in con.execute("DESCRIBE statcast").fetchall()}
has = lambda c: c.lower() in cols
ev   = "ev" if has("ev") else ("exit_velocity" if has("exit_velocity") else "NULL")
la   = "la" if has("la") else ("launch_angle" if has("launch_angle") else "NULL")
velo = "release_speed" if has("release_speed") else ("pitch_speed" if has("pitch_speed") else "NULL")
spin = "release_spin_rate" if has("release_spin_rate") else ("spin_rate" if has("spin_rate") else "NULL")
pt   = "pitch_name" if has("pitch_name") else ("pitch_type" if has("pitch_type") else "NULL")
pid  = "player_id" if has("player_id") else ("batter" if has("batter") else "NULL")
gdt  = "game_date" if has("game_date") else ("date" if has("date") else "NULL")

con.execute(f"""
CREATE VIEW statcast_deep AS
SELECT
  {pid} AS player_id,
  {gdt} AS game_date,
  {ev} AS ev,
  {la} AS la,
  {pt} AS pitch_type,
  {velo} AS pitch_velo,
  {spin} AS spin_rate,
  CASE WHEN {ev} IS NOT NULL AND {ev}>=95 THEN 1 ELSE 0 END AS hard_hit,
  CASE WHEN {ev} IS NOT NULL AND {ev}>=98 AND {la} BETWEEN 26 AND 30 THEN 1 ELSE 0 END AS barrel_like
FROM statcast
""")

summary = dict(
  rows={
    "player_cards": con.execute("SELECT COUNT(*) FROM player_cards").fetchone()[0],
    "statcast": con.execute("SELECT COUNT(*) FROM statcast").fetchone()[0],
    "deep": con.execute("SELECT COUNT(*) FROM statcast_deep").fetchone()[0],
    "id_map": con.execute("SELECT COUNT(*) FROM id_map").fetchone()[0],
  },
  sample=con.execute("SELECT * FROM statcast_deep LIMIT 5").fetchdf().to_dict(orient="records")
)
os.makedirs(f"{out}/summaries", exist_ok=True)
with open(f"{out}/summaries/live_data_link.json","w") as f:
    json.dump(summary,f,ensure_ascii=False,indent=2)
print(json.dumps(summary,ensure_ascii=False))
PY

log "[LIVE-DATA v3] done ✓  ▶ 앱 실행: bash tools/render_boot.sh"
