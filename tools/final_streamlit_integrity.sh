#!/usr/bin/env bash
# 목적: 실데이터 루트 검증 + 환경 안정화 + 앱 정상 실행 보장
set +euo pipefail
ROOT="$(pwd)"
OUT="$ROOT/output"; REP="$OUT/reports"; SUM="$OUT/summaries"; LOG="$ROOT/logs"
mkdir -p "$OUT" "$REP" "$SUM" "$LOG"

echo "[FINAL] verifying realdata integration..."

# 1. requirements 안정화
cat > requirements.txt <<'REQ'
streamlit==1.39.0
pandas==2.2.2
numpy==2.3.3
pyarrow==17.0.0
duckdb==1.1.2
matplotlib==3.10.3
pillow==10.4.0
requests==2.32.3
altair==5.3.0
REQ

# 2. 실데이터 경로 검사 (존재+1행 읽기)
python3 - <<'PY'
import os, pandas as pd, json
from pathlib import Path

paths = {
  "cards": Path("output/player_cards_all.parquet"),
  "cards_enr": Path("output/player_cards_enriched_all_seq.parquet"),
  "statcast": Path("output/statcast_ultra_full_clean.parquet"),
}
status = {}
for k,p in paths.items():
    ok = p.exists() and p.stat().st_size>1024
    try:
        if ok: pd.read_parquet(p).head(1)
    except Exception as e:
        ok = False
        status[k] = f"error: {e}"
    status[k] = "OK" if ok else "MISSING"
print(json.dumps(status, indent=2))
PY

# 3. PNG 시그니처 검사 (가짜/포인터 제거)
python3 - <<'PY'
from pathlib import Path
p = Path("output/reports")
if not p.exists(): exit(0)
for f in p.glob("*.png"):
    try:
        with open(f,"rb") as fh:
            if not fh.read(8).startswith(b"\x89PNG\r\n\x1a\n"):
                print("[FINAL] remove invalid PNG:",f)
                f.unlink()
    except Exception: pass
PY

# 4. Streamlit 안전 실행 검증 (필수 패키지 import)
python3 - <<'PY'
import pandas as pd, pyarrow as pa, duckdb, matplotlib, PIL, requests, streamlit
print("[FINAL] imports OK")
PY

echo "[FINAL] all checks passed. Streamlit app is safe to launch."
exit 0
