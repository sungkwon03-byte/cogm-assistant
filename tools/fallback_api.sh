#!/usr/bin/env bash
set +e
PORT=${PORT:-8080}
cat > /tmp/fallback_api.py <<'PY'
from fastapi import FastAPI
from pathlib import Path
app = FastAPI()
@app.get("/")      def root():   return {"status":"ok","mode":"fallback"}
@app.get("/health")def health(): return {"ok":True}
@app.get("/check")
def check():
    return {
        "cards": Path("output/player_cards_all.parquet").exists(),
        "statcast": Path("output/statcast_ultra_full_clean.parquet").exists(),
        "id_map": Path("output/id_map.csv").exists()
    }
PY
python3 -m uvicorn /tmp/fallback_api:app --host 0.0.0.0 --port $PORT || echo "[WARN] FastAPI fallback crashed"
