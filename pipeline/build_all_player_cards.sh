#!/usr/bin/env bash
set -euo pipefail
ROOT="$(pwd)"
OUT="$ROOT/output"; LOG="$ROOT/logs"
mkdir -p "$OUT" "$LOG"

FROM="${FROM:-1901}"
TO="${TO:-2025}"

for Y in $(seq "$FROM" "$TO"); do
  SRC="mart/mlb_${Y}_players.csv"
  if [ -f "$SRC" ]; then
    echo "[RUN] $Y"
    SEASON="$Y" python pipeline/build_player_cards.py
    # 방금 생성된 통합 파일을 연도별로 복사 보관
    cp -f "$OUT/player_cards.csv" "$OUT/player_cards_${Y}.csv"
  else
    echo "[SKIP] $Y (no $SRC)"
  fi
done

# 합본 생성
echo "[MERGE] output/player_cards_all.csv"
{
  head -n1 "$OUT/player_cards_$(seq "$FROM" "$TO" | head -n1).csv"
  for Y in $(seq "$FROM" "$TO"); do
    F="$OUT/player_cards_${Y}.csv"
    [ -f "$F" ] && tail -n +2 "$F"
  done
} > "$OUT/player_cards_all.csv"

# 행수/샘플
wc -l "$OUT"/player_cards_*.csv | sed -n '1,20p'
wc -l "$OUT/player_cards_all.csv"
