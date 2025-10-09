#!/usr/bin/env python3
# 사용 중인 MLB 캐시(mlb_bat_agg.csv, mlb_pit_agg.csv 등)를 자동 인식
# -> 표준 스키마로 정규화(output/tmp/mlb_batting.csv, mlb_pitching.csv)
# -> KBO 파일 full_name/season 보강
# -> Step2(페어) -> Step3(회귀/링크) 실행

import os, re, subprocess, sys
import pandas as pd
from typing import List

MLB_BAT_CAND = [
    "output/cache/mlb_bat_agg.csv",
    "output/cache/mlb_totals_bat.csv",
]
MLB_PIT_CAND = [
    "output/cache/mlb_pit_agg.csv",
    "output/cache/mlb_totals_pit.csv",
]
NAME_MAP_OPT = "output/cache/name2mlbid.csv"  # 있으면 합침(선택)

OUT_BAT = "output/tmp/mlb_batting.csv"
OUT_PIT = "output/tmp/mlb_pitching.csv"

def first_exists(paths: List[str]):
    for p in paths:
        if os.path.exists(p):
            return p
    return None

def pick_col(df, cands):
    low = {c.lower(): c for c in df.columns}
    for c in cands:
        if c.lower() in low: return low[c.lower()]
    # 한국어/변형 후보
    for c in df.columns:
        if re.search(r"name|player|선수|이름", str(c), re.I): return c
    return None

def season_from(df):
    # season/year/Year/yearID/date에서 추출
    for c in ["season","year","Year","YEAR","yearID","yearid","date","Date","DATE"]:
        if c in df.columns:
            s = df[c].astype(str)
            # date면 앞 4자리 연도 추출
            yr = s.str.extract(r"(\d{4})", expand=False)
            if yr.notna().any(): return yr
    # 실패 시 빈 시리즈
    return pd.Series([""]*len(df), index=df.index)

def to_num(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def safe_div(a, b):
    a = pd.to_numeric(a, errors="coerce")
    b = pd.to_numeric(b, errors="coerce")
    b = b.where(b!=0, other=pd.NA)
    return (a/b).astype(float)

def norm_mlb_bat(src):
    df = pd.read_csv(src, dtype=str, low_memory=False)
    # 이름
    nm = pick_col(df, ["full_name","name","player","player_name"])
    if nm is None:
        # id가 있으면 임시 이름으로 사용
        nm = pick_col(df, ["id","bbref_id","retro_id","mlb_id"])
        if nm is None:
            raise SystemExit(f"[FATAL] {src}: 이름/id 컬럼을 찾을 수 없음")
    df["full_name"] = df[nm].astype(str)

    # 시즌
    df["season"] = season_from(df)

    # 지표 매핑(대/소문자 가리지 않음)
    def col(*alts):
        for a in alts:
            if a in df.columns: return a
            low = {c.lower(): c for c in df.columns}
            if a.lower() in low: return low[a.lower()]
        return None

    pa = col("pa","PA")
    ab = col("ab","AB")
    h  = col("h","H","hits")
    hr = col("hr","HR")
    bb = col("bb","BB","walks")
    so = col("so","SO","k","K")
    obp= col("obp","OBP")
    slg= col("slg","SLG")
    ops= col("ops","OPS")
    hbp= col("hbp","HBP")
    sf = col("sf","SF")
    sh = col("sh","SH")
    doubles = col("2b","2B","double","doubles")
    triples = col("3b","3B","triple","triples")

    need = []
    if ab is None: need.append("AB")
    if h  is None: need.append("H")
    if hr is None: need.append("HR")
    if bb is None: need.append("BB")
    if so is None: need.append("SO")
    if need:
        raise SystemExit(f"[FATAL] {src}: 필수 컬럼 누락 -> {need}")

    # 숫자화
    for c in [x for x in [pa,ab,h,hr,bb,so,hbp,sf,sh,doubles,triples] if c:=x]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # PA 계산
    if pa is None:
        pa = "__pa__"
        df[pa] = df[ab] + df.get(bb,0) + df.get(hbp,0) + df.get(sf,0) + df.get(sh,0)

    # SLG/OPS 계산
    if slg is None:
        singles = df[h] - df.get(doubles,0) - df.get(triples,0) - df[hr]
        tb = singles + 2*df.get(doubles,0) + 3*df.get(triples,0) + 4*df[hr]
        slg = "__slg__"
        df[slg] = safe_div(tb, df[ab])
    if obp is None:
        obp = "__obp__"
        df[obp] = safe_div(df[h] + df.get(bb,0) + df.get(hbp,0),
                           df[ab] + df.get(bb,0) + df.get(hbp,0) + df.get(sf,0))
    if ops is None:
        ops = "__ops__"
        df[ops] = df[obp] + df[slg]

    out = df[["full_name","season", pa, ab, h, hr, bb, so, obp, slg, ops]].copy()
    out.columns = ["full_name","season","pa","ab","h","hr","bb","so","obp","slg","ops"]

    # 선택: 이름→ID 매핑 합치기
    if os.path.exists(NAME_MAP_OPT):
        m = pd.read_csv(NAME_MAP_OPT, dtype=str).rename(columns=str.lower)
        # 허용 컬럼: full_name/name/player + [bbref_id/retro_id/mlb_id/id]
        namecol = pick_col(m, ["full_name","name","player"])
        idcols = [c for c in ["bbref_id","retro_id","mlb_id","id"] if c in m.columns]
        if namecol and idcols:
            out = out.merge(m[[namecol]+idcols].drop_duplicates(), left_on="full_name", right_on=namecol, how="left")
            out = out.drop(columns=[namecol], errors="ignore")

    os.makedirs(os.path.dirname(OUT_BAT), exist_ok=True)
    out.to_csv(OUT_BAT, index=False)
    print(f"[OK] MLB batting normalized -> {OUT_BAT}  rows={len(out)}")

def norm_mlb_pit(src):
    df = pd.read_csv(src, dtype=str, low_memory=False)
    nm = pick_col(df, ["full_name","name","player","player_name"])
    if nm is None:
        nm = pick_col(df, ["id","bbref_id","retro_id","mlb_id"])
        if nm is None:
            raise SystemExit(f"[FATAL] {src}: 이름/id 컬럼을 찾을 수 없음")
    df["full_name"] = df[nm].astype(str)
    df["season"] = season_from(df)

    def col(*alts):
        for a in alts:
            if a in df.columns: return a
            low = {c.lower(): c for c in df.columns}
            if a.lower() in low: return low[a.lower()]
        return None

    ip  = col("ip","IP")
    ipouts = col("ipouts","IPouts","p_ipouts")  # retrosheet 계열
    er  = col("er","ER")
    bb  = col("bb","BB")
    so  = col("so","SO","k","K","p_k")
    hr  = col("hr","HR","p_hr")

    need = []
    if er is None: need.append("ER")
    if bb is None: need.append("BB")
    if so is None: need.append("SO/K")
    if hr is None: need.append("HR")
    if (ip is None) and (ipouts is None): need.append("IP or IPouts")
    if need:
        raise SystemExit(f"[FATAL] {src}: 필수 컬럼 누락 -> {need}")

    for c in [x for x in [ip,ipouts,er,bb,so,hr] if c:=x]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    if ip is None:
        ip = "__ip__"
        df[ip] = df[ipouts]/3.0

    out = df[["full_name","season", ip, er, bb, so, hr]].copy()
    out.columns = ["full_name","season","ip","er","bb","so","hr"]

    if os.path.exists(NAME_MAP_OPT):
        m = pd.read_csv(NAME_MAP_OPT, dtype=str).rename(columns=str.lower)
        namecol = pick_col(m, ["full_name","name","player"])
        idcols = [c for c in ["bbref_id","retro_id","mlb_id","id"] if c in m.columns]
        if namecol and idcols:
            out = out.merge(m[[namecol]+idcols].drop_duplicates(), left_on="full_name", right_on=namecol, how="left")
            out = out.drop(columns=[namecol], errors="ignore")

    os.makedirs(os.path.dirname(OUT_PIT), exist_ok=True)
    out.to_csv(OUT_PIT, index=False)
    print(f"[OK] MLB pitching normalized -> {OUT_PIT}  rows={len(out)}")

def fix_kbo_csv(path):
    if not os.path.exists(path):
        raise SystemExit(f"[FATAL] {path} 없음")
    df = pd.read_csv(path, dtype=str, low_memory=False)
    df.columns = [c.strip() for c in df.columns]
    nm = pick_col(df, ["full_name","name","player","선수명","선수","이름"])
    if nm is None:
        raise SystemExit(f"[FATAL] {path}: 이름 컬럼을 찾지 못함. columns={list(df.columns)[:15]}")
    if "full_name" not in df.columns:
        df["full_name"] = df[nm]
    if "season" not in df.columns:
        df["season"] = season_from(df)
    df.to_csv(path, index=False)
    print(f"[OK] KBO fixed -> {path} (full_name/season 보장)")

def sh(cmd):
    r = subprocess.run(cmd, shell=True)
    if r.returncode != 0:
        raise SystemExit(f"[FATAL] failed: {cmd}")

def main():
    bat_src = first_exists(MLB_BAT_CAND)
    pit_src = first_exists(MLB_PIT_CAND)
    if not bat_src or not pit_src:
        raise SystemExit("[FATAL] MLB 후보 파일을 찾지 못함 (output/cache/*mlb*.*)")

    print(f"[INFO] MLB batting src = {bat_src}")
    print(f"[INFO] MLB pitching src = {pit_src}")

    norm_mlb_bat(bat_src)
    norm_mlb_pit(pit_src)

    fix_kbo_csv("data/xleague/kbo_batting.csv")
    fix_kbo_csv("data/xleague/kbo_pitching.csv")

    sh(
        "python3 scripts/day54_build_pairs.py "
        f"--kbo-bat data/xleague/kbo_batting.csv "
        f"--kbo-pit data/xleague/kbo_pitching.csv "
        f"--mlb-bat {OUT_BAT} "
        f"--mlb-pit {OUT_PIT}"
    )
    sh("python3 scripts/day54_bridge_on_demand.py")
    sh("python3 scripts/day54_link_candidates.py")
    print("[DONE] Day54 ②–③ complete.")

if __name__ == "__main__":
    main()
