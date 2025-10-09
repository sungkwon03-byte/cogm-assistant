#!/usr/bin/env bash
set -euo pipefail
export PYTHONUNBUFFERED=1
mkdir -p output/logs

START=1901
END=$(date +%Y)
# 워커 수 조절(기본 16). 네트워크가 좋으면 24, 불안정하면 8 추천.
export MLB_WORKERS=${MLB_WORKERS:-16}

# 연도별로 순차 실행(세션 끊겨도 다음에 다시 실행하면 이어서 처리)
for Y in $(seq $START $END); do
  echo "[RUN] YEAR=$Y (workers=$MLB_WORKERS)"
  python3 scripts/mlb_ingest_year.py --year "$Y" | tee -a "output/logs/year_${Y}.log"
  # 파일 시스템 flush/쿨다운
  sleep 1
done

# 샤드 병합(뒤 블록에서 처리)
echo "[OK] per-year ingest completed"
