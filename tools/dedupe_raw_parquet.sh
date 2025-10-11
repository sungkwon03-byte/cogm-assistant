#!/usr/bin/env bash
set +e; set +u; set +o pipefail 2>/dev/null || true
trap '' ERR
ROOT="/workspaces/cogm-assistant"
A="$ROOT/raw/statcast_parquet"
B="$ROOT/output/raw/statcast_parquet"
LOG="$ROOT/logs/dedupe_raw_parquet.log"
mkdir -p "$(dirname "$LOG")"
echo "[START] $(date -u +%FT%TZ)" > "$LOG"

echo "[CHECK] sizes" | tee -a "$LOG"
du -sh "$A" 2>/dev/null | tee -a "$LOG"
du -sh "$B" 2>/dev/null | tee -a "$LOG"

if [ "${CONFIRM:-NO}" != "YES" ]; then
  echo "[DRY-RUN] set CONFIRM=YES to actually dedupe" | tee -a "$LOG"
  exit 0
fi

# 기준 폴더는 A로 두고, B를 A로 심볼릭 링크 전환
if [ -d "$A" ] && [ -d "$B" ]; then
  echo "[RUN] remove B and symlink -> A" | tee -a "$LOG"
  rm -rf "$B"
  mkdir -p "$ROOT/output/raw"
  ln -s "$A" "$B"
  echo "[OK] $B -> symlink to $A" | tee -a "$LOG"
fi

echo "[END] $(date -u +%FT%TZ)" | tee -a "$LOG"
df -h | tee -a "$LOG"
