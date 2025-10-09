#!/usr/bin/env python3
# Day54: Lahman 탐색 → MLB 표준 CSV(청크) → KBO 보정 → Step2/3 실행
import os, sys, re, glob, subprocess
import pandas as pd

ROOT = os.getcwd()
OUT_TMP = os.path.join("output","tmp"); os.makedirs(OUT_TMP, exist_ok=True)

def nkey(s):
    s = "" if pd.isna(s) else str(s)
    s = s.replace("."," ").replace("-"," ")
    return re.sub(r"\s+"," ", s).upper().strip()

def season_series(df):
    for c in ["season","yearid","year","Year","YEAR"]:
        if c in df.columns:
            return df[c].astype(str).str.extract(r"(\d{4})", expand=False)
    return pd.Series([""]*len(df), index=df.index)

def pick_name_col(df):
    for c in ["full_name","name","player","Player","PLAYER","Name","NAME"]:
        if c in df.columns: return c
    for c in df.columns:
        if re.search(r"name|player", c, re.I): return c
    return None

def find_first(patterns, roots):
    for root in roots:
        for pat in patterns:
            hits = glob.glob(os.path.join(root, "**", pat), recursive=True)
            if hits: return sorted(hits, key=lambda p: len(p.split(os.sep)))[0]
    return None

def read_people(people_path):
    import pandas as pd
    p = pd.read_csv(people_path, dtype=str, low_memory=False)
    p.columns = [c.strip().lower() for c in p.columns]

    # 항상 Series 반환하는 안전 접근자
    def col(name):
        return p[name].fillna("") if name in p.columns else pd.Series([""]*len(p), index=p.index)

    # 이름 구성
    first  = col("namefirst")
    last   = col("namelast")
    given  = col("namegiven")
    if (first.str.strip()=="").all() and (given.str.strip()!="").any():
        toks  = given.str.strip().str.split()
        first = toks.map(lambda x: x[0] if isinstance(x,list) and len(x)>0 else "")
        last  = toks.map(lambda x: x[-1] if isinstance(x,list) and len(x)>0 else "")
    full = (first.str.strip()+" "+last.str.strip()).str.replace(r"\s+"," ", regex=True).str.strip()
    for fb in ("namefull","name","player","playername"):
        if fb in p.columns:
            mask = (full=="")
            if mask.any(): full.loc[mask] = p[fb].fillna("").astype(str).str.strip()
            break

    # 식별자 열 자동 감지: playerid -> retroid -> bbrefid -> id
    key = None
    if "playerid" in p.columns: key = "playerid"
    elif "retroid" in p.columns: key = "retroid"
    elif "bbrefid" in p.columns: key = "bbrefid"
    elif "id" in p.columns: key = "id"
    else:
        raise SystemExit(f"[FATAL] People.csv 식별자 컬럼 없음. columns={list(p.columns)[:20]}")

    if key != "playerid": p["playerid"] = p[key]
    p["full_name"] = full
    return p[["playerid","full_name"]]
def fix_kbo_csv(path):
    df = pd.read_csv(path, dtype=str, low_memory=False)
    df.columns = [c.strip() for c in df.columns]
    nmcol = pick_name_col(df)
    if not nmcol:
        if "team" in df.columns: nmcol = "team"
        else: raise SystemExit(f"[FATAL] {path}: 이름 컬럼을 찾을 수 없음 (columns={list(df.columns)[:12]})")
    if "full_name" not in df.columns: df["full_name"] = df[nmcol]
    if "season" not in df.columns:    df["season"] = season_series(df)
    df.to_csv(path, index=False)

def safe_div(num, den):
    num = pd.to_numeric(num, errors="coerce")
    den = pd.to_numeric(den, errors="coerce")
    den = den.where(den!=0, other=pd.NA)
    return (num/den).astype(float)

# ---- locate files
SEARCH_ROOTS = [ROOT, os.path.join(ROOT,"data"), os.path.join(ROOT,"data","mlb"),
                os.path.join(ROOT,"lahman"), os.path.join(ROOT,"data","retrosheet"),
                os.path.join(ROOT,"data","id"), os.path.join(ROOT,"data","id","Lahman")]
bat_path   = find_first(["Batting.csv","batting.csv"], SEARCH_ROOTS)
pit_path   = find_first(["Pitching.csv","pitching.csv"], SEARCH_ROOTS)
people_path= find_first(["People.csv","people.csv"], SEARCH_ROOTS)
if not (bat_path and pit_path and people_path):
    raise SystemExit("[FATAL] Batting.csv / Pitching.csv / People.csv 자동탐색 실패")
print(f"[INFO] Batting: {bat_path}\n[INFO] Pitching: {pit_path}\n[INFO] People: {people_path}")

people = read_people(people_path)

# ---- MLB batting (chunk)
mlb_bat_out = os.path.join(OUT_TMP, "mlb_batting.csv")
if os.path.exists(mlb_bat_out): os.remove(mlb_bat_out)
hdr = pd.read_csv(bat_path, nrows=0)
lower = {c:c.lower() for c in hdr.columns}
need_any = {"playerid","yearid","ab","h","hr","bb","so","hbp","sf","sh","2b","3b"}
usecols = [orig for orig in hdr.columns if lower[orig] in need_any]
first_write = True
for ch in pd.read_csv(bat_path, dtype=str, usecols=usecols if usecols else None,
                      chunksize=200000, low_memory=True):
    ch.columns = [c.lower() for c in ch.columns]
    for c in ["ab","h","hr","bb","so","hbp","sf","sh","2b","3b"]:
        if c not in ch.columns: ch[c] = 0
    ch = ch.merge(people, on="playerid", how="left")
    ch["season"] = ch.get("yearid","").astype(str)

    for c in ["ab","h","hr","bb","so","hbp","sf","sh","2b","3b"]:
        ch[c] = pd.to_numeric(ch[c], errors="coerce").fillna(0)
    ch["pa"] = ch["ab"] + ch["bb"] + ch["hbp"] + ch["sf"] + ch["sh"]
    ch["1b"] = ch["h"] - ch["2b"] - ch["3b"] - ch["hr"]
    ch["tb"] = ch["1b"] + 2*ch["2b"] + 3*ch["3b"] + 4*ch["hr"]
    ch["obp"] = safe_div(ch["h"]+ch["bb"]+ch["hbp"], ch["ab"]+ch["bb"]+ch["hbp"]+ch["sf"])
    ch["slg"] = safe_div(ch["tb"], ch["ab"])
    ch["ops"] = ch["obp"] + ch["slg"]

    out = ch[["full_name","season","pa","ab","h","hr","bb","so","obp","slg","ops"]]
    out.to_csv(mlb_bat_out, mode="a", header=first_write, index=False)
    first_write = False
print(f"[OK] MLB batting -> {mlb_bat_out}")

# ---- MLB pitching (chunk)
mlb_pit_out = os.path.join(OUT_TMP, "mlb_pitching.csv")
if os.path.exists(mlb_pit_out): os.remove(mlb_pit_out)
hdr = pd.read_csv(pit_path, nrows=0)
lower = {c:c.lower() for c in hdr.columns}
need_any = {"playerid","yearid","ipouts","er","bb","so","hr"}
usecols = [orig for orig in hdr.columns if lower[orig] in need_any]
first_write = True
for ch in pd.read_csv(pit_path, dtype=str, usecols=usecols if usecols else None,
                      chunksize=200000, low_memory=True):
    ch.columns = [c.lower() for c in ch.columns]
    for c in ["ipouts","er","bb","so","hr"]:
        if c not in ch.columns: ch[c] = 0
    ch = ch.merge(people, on="playerid", how="left")
    ch["season"] = ch.get("yearid","").astype(str)
    for c in ["ipouts","er","bb","so","hr"]:
        ch[c] = pd.to_numeric(ch[c], errors="coerce").fillna(0)
    ch["ip"] = ch["ipouts"]/3.0
    out = ch[["full_name","season","ip","er","bb","so","hr"]]
    out.to_csv(mlb_pit_out, mode="a", header=first_write, index=False)
    first_write = False
print(f"[OK] MLB pitching -> {mlb_pit_out}")

# ---- ensure KBO CSV
for p in ["data/xleague/kbo_batting.csv","data/xleague/kbo_pitching.csv"]:
    if not os.path.exists(p):
        raise SystemExit(f"[FATAL] {p} 없음.")
    fix_kbo_csv(p)

# ---- run Day54 step2/3
def sh(cmd):
    r = subprocess.run(cmd, shell=True)
    if r.returncode != 0:
        raise SystemExit(f"[FATAL] failed: {cmd}")

sh(
    "python3 scripts/day54_build_pairs.py "
    "--kbo-bat data/xleague/kbo_batting.csv "
    "--kbo-pit data/xleague/kbo_pitching.csv "
    f"--mlb-bat {mlb_bat_out} "
    f"--mlb-pit {mlb_pit_out}"
)
sh("python3 scripts/day54_bridge_on_demand.py")
sh("python3 scripts/day54_link_candidates.py")
print("[DONE] Day54 ②–③ complete.")
