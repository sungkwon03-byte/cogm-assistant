#!/usr/bin/env bash
set +e; set +u; set +o pipefail 2>/dev/null || true
trap '' ERR
ROOT="/workspaces/cogm-assistant"
OUT="$ROOT/output"
LOG="$ROOT/logs/free_space_now.log"
mkdir -p "$ROOT/tools" "$ROOT/logs"

echo "[START] $(date -u +%FT%TZ)" > "$LOG"
df -h | tee -a "$LOG"

echo "[STEP] remove plot images / pdf (재생성 가능)" | tee -a "$LOG"
rm -f "$OUT"/reports/*.{png,pdf} 2>/dev/null

echo "[STEP] remove temp/part parquet" | tee -a "$LOG"
rm -f "$OUT"/statcast_*_part.parquet 2>/dev/null

echo "[STEP] truncate logs" | tee -a "$LOG"
find "$ROOT/logs" -type f -name "*.log" -size +1M -exec sh -c '>: "$1"' _ {} \; 2>/dev/null

echo "[STEP] convert huge CSV -> Parquet & delete CSV (보존: *_all.parquet)" | tee -a "$LOG"
python3 - <<'PY'
import os, duckdb, pandas as pd, glob, sys
root="/workspaces/cogm-assistant/output"
targets=[
  "player_cards_all.csv",
  "player_cards_enriched_all_seq.csv"
]
con=duckdb.connect()
for t in targets:
    src=os.path.join(root,t)
    if os.path.exists(src):
        dst=src[:-4]+"parquet"
        try:
            df=pd.read_csv(src)
            con.register("t", df)
            con.execute(f"COPY t TO '{dst}' (FORMAT PARQUET)")
            con.unregister("t")
            print("[OK] wrote", dst, "rows=",len(df))
            os.remove(src)
            print("[DEL]", src)
        except Exception as e:
            print("[SKIP]", src, e)
# 연도별 카드 CSV는 합본이 있으면 삭제
allp=os.path.join(root,"player_cards_all.parquet")
if os.path.exists(allp):
    for f in glob.glob(os.path.join(root,"player_cards_*.csv")):
        try: os.remove(f); print("[DEL]", f)
        except: pass
con.close()
PY

echo "[STEP] delete obvious CSV duplicates at repo root/output" | tee -a "$LOG"
# (초기에 뜬 요약 CSV들 – 재생성 가능)
rm -f "$ROOT"/*statcast*.csv "$OUT"/*statcast*.csv 2>/dev/null

echo "[STEP] git LFS prune (워크트리 외 보관객체 정리)" | tee -a "$LOG"
cd "$ROOT"
git lfs install >/dev/null 2>&1
git lfs prune   >/dev/null 2>&1

echo "[STEP] pip cache prune" | tee -a "$LOG"
pip cache purge >/dev/null 2>&1

echo "[REPORT] biggest 20 files" | tee -a "$LOG"
du -ah "$ROOT" | sort -hr | head -n 20 | tee -a "$LOG" || true

echo "[END] $(date -u +%FT%TZ)" | tee -a "$LOG"
df -h | tee -a "$LOG"
