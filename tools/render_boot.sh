#!/usr/bin/env bash
set +e
PORT="${PORT:-8080}"
export STREAMLIT_SERVER_PORT=$PORT
echo "[BOOT] installing minimal deps..."
pip install --no-cache-dir streamlit pandas pyarrow duckdb >/dev/null 2>&1 || true
echo "[BOOT] launching Streamlit (port=$PORT)"
if command -v streamlit >/dev/null 2>&1; then
  exec streamlit run streamlit_app.py --server.address 0.0.0.0 --server.port $PORT
else
  echo "[BOOT] fallback FastAPI"
  python - <<'PY'
from fastapi import FastAPI
import uvicorn
app=FastAPI()
@app.get("/") 
def root(): return {"ok":True,"msg":"fallback FastAPI active"}
uvicorn.run(app,host="0.0.0.0",port=int(__import__("os").getenv("PORT","8080")))
PY
fi
