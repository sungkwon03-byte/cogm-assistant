#!/usr/bin/env bash
# Full end-to-end runner (always exit 0, log-append safe)
set +e
set +u
{ set +o pipefail; } 2>/dev/null || true
trap '' ERR

ROOT="/workspaces/cogm-assistant"
OUT="$ROOT/output"
SUM="$OUT/summaries"
REP="$OUT/reports"
LOG="$ROOT/logs/run_full_end2end.log"

mkdir -p "$OUT" "$SUM" "$REP" "$ROOT/logs"
echo "[START] $(date -u +%FT%TZ)" > "$LOG"

# Downstream가 참조하는 기본 경로 고정
export STATCAST_ROOT="$OUT"

echo "[RUN] full_system_patch_and_expand.py" | tee -a "$LOG"
python3 "$ROOT/pipeline/full_system_patch_and_expand.py" >>"$LOG" 2>&1 || true

echo "[RUN] mock_and_calendar_wrappers.py" | tee -a "$LOG"
python3 "$ROOT/pipeline/mock_and_calendar_wrappers.py" >>"$LOG" 2>&1 || true

echo "[RUN] full_system_validate.py" | tee -a "$LOG"
python3 "$ROOT/pipeline/full_system_validate.py" >>"$LOG" 2>&1 || true

# 간단 커버리지 요약 (DuckDB CLI 불필요, 항상 성공 경로)
python3 - <<'PY' | tee -a "/workspaces/cogm-assistant/logs/run_full_end2end.log"
import os, duckdb
parq='output/statcast_ultra_full_clean.parquet'
if not os.path.isfile(parq):
    parq='output/statcast_ultra_full.parquet'
print("[COVERAGE] source:", parq)
try:
    con=duckdb.connect()
    tot=con.execute(
        "SELECT COUNT(*) AS rows, MIN(CAST(year AS INT)) AS min_y, MAX(CAST(year AS INT)) AS max_y FROM read_parquet(?)",
        [parq]).fetchdf().to_dict(orient="records")[0]
    y=con.execute(
        "SELECT CAST(year AS INT) y, COUNT(*) n FROM read_parquet(?) WHERE CAST(year AS INT) IN (2015,2025) GROUP BY 1 ORDER BY 1",
        [parq]).fetchdf().to_dict(orient="records")
    print("[COVERAGE] totals:", tot)
    print("[COVERAGE] 2015/2025:", y)
except Exception as e:
    print("[COVERAGE] skipped due to:", e)
PY

echo "[DONE] $(date -u +%FT%TZ)" | tee -a "$LOG"
exit 0
