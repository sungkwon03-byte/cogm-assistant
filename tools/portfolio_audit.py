#!/usr/bin/env python3
from pathlib import Path
import duckdb as d, json, sys

def stat_parquet(p):
    try:
        con = d.connect()
        return int(con.sql(f"select count(*) from read_parquet('{p.as_posix()}')").fetchone()[0])
    except Exception as e:
        return f"ERR:{e}"

cards=Path("output/player_cards_all.parquet")
sc=Path("output/statcast_ultra_full_clean.parquet")
idmap=Path("output/id_map.csv")

summary={
  "exists": {"cards": cards.exists(), "statcast": sc.exists(), "id_map": idmap.exists()},
  "rows": {
    "cards": stat_parquet(cards) if cards.exists() else 0,
    "statcast": stat_parquet(sc) if sc.exists() else 0
  }
}
print(json.dumps(summary, indent=2))
sys.exit(0)
