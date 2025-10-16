#!/usr/bin/env bash
set +e
python - <<'PY'
import duckdb,os
con=duckdb.connect("data/duckdb/main.duckdb")
con.execute("CREATE OR REPLACE VIEW v_players AS SELECT * FROM read_parquet('output/player_cards_all.parquet');")
con.execute("CREATE OR REPLACE VIEW v_statcast AS SELECT * FROM read_parquet('output/statcast_ultra_full_clean.parquet');")
con.close()
print("[DUCKDB] views refreshed")
PY
