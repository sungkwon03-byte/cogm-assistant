#!/usr/bin/env python3
import os, glob, pandas as pd, subprocess, sys

RAW_DIR = "raw"
SCRIPT = "scripts/ingest_milb_from_fg.py"
SEASON_DEFAULT = 2024

def detect_hit_pit(files):
    hit, pit = None, None
    for f in files:
        try:
            df = pd.read_csv(f, nrows=1)
        except Exception:
            continue
        cols = set(df.columns)
        if {"PA","wRC+","OBP","SLG"} <= cols: hit = f
        if {"BF","FIP","K/9","BB/9"} <= cols: pit = f
    return hit, pit

def main():
    os.makedirs("mart", exist_ok=True)
    files = sorted(glob.glob(os.path.join(RAW_DIR, "*.csv")))
    if not files:
        sys.exit("[ERROR] raw/*.csv 없음. 업로드 파일을 raw 폴더에 넣어주세요.")

    hit, pit = detect_hit_pit(files)
    if not hit or not pit:
        sys.exit("[ERROR] 히터/피처 CSV 자동 구분 실패.")

    print(f"[INFO] HIT: {hit}\n[INFO] PIT: {pit}")

    subprocess.run([
        "python", SCRIPT,
        "--hitters-csv", hit,
        "--pitchers-csv", pit,
        "--season", str(SEASON_DEFAULT)
    ], check=True)
    print("[OK] ingest + mart 적재 완료")

if __name__ == "__main__":
    main()
