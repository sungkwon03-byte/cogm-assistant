#!/usr/bin/env python3
import sys, re, pandas as pd

def pick_name_col(cols):
    # 최우선 키워드
    pri = ["full_name","name","player","Player","PLAYER"]
    for c in pri:
        if c in cols: return c
    # 후순위: 컬럼명에 name/player 문자열 포함
    for c in cols:
        if re.search(r"name|player", c, re.I): return c
    # 팀 합계 같은 경우(마지막 수단): team 컬럼을 이름으로 사용
    for c in ["team","Team","TEAM","club","Club","CLUB"]:
        if c in cols: return c
    return None

def main(path):
    df = pd.read_csv(path, dtype=str, low_memory=False)
    cols = list(df.columns)
    nm = pick_name_col(cols)
    if not nm:
        print(f"[FATAL] 이름 유사 컬럼을 찾을 수 없음. columns={cols}", file=sys.stderr)
        sys.exit(2)
    if "full_name" not in df.columns:
        df["full_name"] = df[nm]
    # season 보정
    if "season" not in df.columns:
        # 연도 후보 찾기
        year_col = None
        for c in ["year","Year","YEAR","season","Season","SEASON"]:
            if c in cols: year_col = c; break
        if year_col:
            df["season"] = df[year_col].astype(str).str.extract(r"(\d{4})", expand=False)
        else:
            df["season"] = ""
    # 정리 후 덮어쓰기
    df.to_csv(path, index=False)
    print(f"[OK] fixed full_name/season -> {path} (cols={list(df.columns)[:8]} ...)")
if __name__ == "__main__":
    if len(sys.argv)<2: 
        print("usage: python3 scripts/day54_fix_kbo_names.py data/xleague/kbo_batting.csv", file=sys.stderr)
        sys.exit(1)
    main(sys.argv[1])
