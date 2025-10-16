#!/usr/bin/env bash
set +e
mkdir -p data/duckdb
DB="data/duckdb/main.duckdb"
echo "[DUCKDB] creating or refreshing views..."
python - <<'PY'
import duckdb, os
os.makedirs("data/duckdb",exist_ok=True)
con=duckdb.connect("data/duckdb/main.duckdb")
try:
    con.execute("CREATE OR REPLACE VIEW player_cards AS SELECT * FROM parquet_scan('output/player_cards_all.parquet');")
    con.execute("CREATE OR REPLACE VIEW statcast AS SELECT * FROM parquet_scan('output/statcast_ultra_full_clean.parquet');")
    con.execute("CREATE OR REPLACE VIEW id_map AS SELECT * FROM read_csv_auto('output/id_map.csv');")
    print("[OK] views created")
except Exception as e:
    print("[WARN]",e)
con.close()
PY
