#!/usr/bin/env bash
set +e
ts(){ date -u +%FT%TZ; }; say(){ printf "[%s] %s\n" "$(ts)" "$*"; }
say "[SMOKE] start"

python3 - <<'PY'
import importlib, sys
mods=["streamlit","pandas","duckdb","pyarrow"]
missing=[m for m in mods if importlib.util.find_spec(m) is None]
print({"missing_modules":missing})
PY

python3 - <<'PY'
import duckdb as d, json
res={}
for f in ["output/player_cards_all.parquet","output/statcast_ultra_full_clean.parquet"]:
    try:
        n=d.connect().sql(f"select count(*) from read_parquet('{f}')").fetchone()[0]
        res[f]=int(n)
    except Exception as e:
        res[f]=f"ERR:{e}"
print(json.dumps(res, indent=2))
PY

say "[SMOKE] ok"
exit 0
