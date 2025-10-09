#!/usr/bin/env python3
import os, re, glob, pandas as pd, numpy as np

OUT_DIR = "data/xleague"
# ✅ KBO 소스만 스캔 (output/cache, data/xleague 제외)
SRC_DIRS = ["data/kbo_sources"]

def ensure_dir(p): os.makedirs(p, exist_ok=True)
def lc(s): return s.lower().strip() if isinstance(s,str) else s
def to_num(x): return pd.to_numeric(x, errors="coerce").fillna(0)

BAT_OUT = ["season","team","full_name","pa","ab","h","doubles","triples","hr","bb","so","hbp","sf","sh","obp","slg","ops"]
PIT_OUT = ["season","team","full_name","ip","er","bb","so","hr","era","k9","bb9"]

def nkey(s):
    s = "" if pd.isna(s) else str(s)
    s = s.replace("."," ").replace("-"," ")
    return re.sub(r"\s+"," ", s).upper().strip()

def pick(df, *alts):
    low = {c.lower(): c for c in df.columns}
    for a in alts:
        if a in df.columns: return a
        if a.lower() in low: return low[a.lower()]
    return None

def season_col(df):
    for c in ["season","Year","year","yearID","yearid","연도"]:
        if c in df.columns: return c
    return None

def name_col(df):
    for c in ["full_name","Name","name","player","선수명","선수","이름"]:
        if c in df.columns: return c
    return None

def team_col(df):
    for c in ["team","Team","tm","구단","팀"]:
        if c in df.columns: return c
    return None

def calc_ops_from_parts(df, ab, h, bb, hbp=None, sf=None, sh=None, d2=None, d3=None, hr=None):
    A = to_num(df[ab]); H = to_num(df[h]); BB = to_num(df[bb])
    HBP = to_num(df[hbp]) if hbp and hbp in df.columns else 0
    SF  = to_num(df[sf])  if sf  and sf  in df.columns else 0
    SH  = to_num(df[sh])  if sh  and sh  in df.columns else 0
    D2  = to_num(df[d2])  if d2  and d2  in df.columns else 0
    D3  = to_num(df[d3])  if d3  and d3  in df.columns else 0
    HR  = to_num(df[hr])  if hr  and hr  in df.columns else 0
    singles = (H - D2 - D3 - HR).clip(lower=0)
    PA = A + BB + HBP + SF + SH
    TB = singles + 2*D2 + 3*D3 + 4*HR
    with np.errstate(invalid="ignore", divide="ignore"):
        obp = (H + BB + HBP) / PA.replace(0, np.nan)
        slg = (TB) / A.replace(0, np.nan)
    ops = obp + slg
    return PA, obp, slg, ops, D2, D3, HR, BB

def adapt_batting(df):
    df = df.copy()
    nm = name_col(df); sc = season_col(df); tm = team_col(df)
    if not nm: return None
    if not sc: df["season"] = ""
    else: df["season"] = df[sc].astype(str).str.extract(r"(\d{4})", expand=False)
    df["full_name"] = df[nm].astype(str).str.strip()
    df["team"] = df[tm].astype(str).str.strip() if tm else ""

    ab = pick(df,"ab","AB"); h=pick(df,"h","H"); bb=pick(df,"bb","BB"); so=pick(df,"so","SO","k","K")
    d2=pick(df,"2b","2B"); d3=pick(df,"3b","3B"); hr=pick(df,"hr","HR")
    hbp=pick(df,"hbp","HBP"); sf=pick(df,"sf","SF"); sh=pick(df,"sh","SH")
    pa = pick(df,"pa","PA")

    if ab and h and bb:
        PA, obp, slg, ops, D2, D3, HR, BB = calc_ops_from_parts(df, ab,h,bb,hbp,sf,sh,d2,d3,hr)
        df["pa"] = to_num(df[pa]) if pa else PA
        df["ab"] = to_num(df[ab]); df["h"]=to_num(df[h]); df["bb"]=BB; df["so"]=to_num(df[so]) if so else 0
        df["doubles"]=D2; df["triples"]=D3; df["hr"]=HR
        df["hbp"]=to_num(df[hbp]) if hbp else 0; df["sf"]=to_num(df[sf]) if sf else 0; df["sh"]=to_num(df[sh]) if sh else 0
        if "obp" not in df.columns: df["obp"]=obp
        if "slg" not in df.columns: df["slg"]=slg
        if "ops" not in df.columns: df["ops"]=ops
    else:
        for c in ["pa","ab","h","bb","so","doubles","triples","hr","hbp","sf","sh","obp","slg","ops"]:
            if c not in df.columns: df[c]=0

    out = df[["season","team","full_name","pa","ab","h","doubles","triples","hr","bb","so","hbp","sf","sh","obp","slg","ops"]].copy()
    return out

def adapt_pitching(df):
    df = df.copy()
    nm = name_col(df); sc = season_col(df); tm = team_col(df)
    if not nm: return None
    if not sc: df["season"] = ""
    else: df["season"] = df[sc].astype(str).str.extract(r"(\d{4})", expand=False)
    df["full_name"] = df[nm].astype(str).str.strip()
    df["team"] = df[tm].astype(str).str.strip() if tm else ""

    ip = pick(df,"ip","IP"); ipouts = pick(df,"ip_outs","IPouts","ipouts")
    er = pick(df,"er","ER"); bb=pick(df,"bb","BB"); so=pick(df,"so","SO","k","K"); hr=pick(df,"hr","HR")

    if not ip and ipouts:
        ipo = to_num(df[ipouts]); df["ip"] = ipo/3.0
    elif ip:
        def ipf(x):
            s=str(x).strip()
            if s=="" or s.lower()=="nan": return 0.0
            if "." in s:
                a,b=s.split(".",1)
                try:
                    a=int(a)
                    if b=="1": return a + 1/3
                    if b=="2": return a + 2/3
                    return float(s)
                except: return float(s)
            return float(s)
        df["ip"] = df[ip].map(ipf)
    else:
        df["ip"]=0.0

    df["er"] = to_num(df[er]) if er else 0
    df["bb"] = to_num(df[bb]) if bb else 0
    df["so"] = to_num(df[so]) if so else 0
    df["hr"] = to_num(df[hr]) if hr else 0

    with np.errstate(invalid="ignore", divide="ignore"):
        df["era"] = (df["er"]*9.0)/df["ip"].replace(0,np.nan)
        df["k9"]  = (df["so"]*9.0)/df["ip"].replace(0,np.nan)
        df["bb9"] = (df["bb"]*9.0)/df["ip"].replace(0,np.nan)

    out = df[["season","team","full_name","ip","er","bb","so","hr","era","k9","bb9"]].copy()
    return out

def scan_sources():
    files=[]
    for d in SRC_DIRS:
        for f in glob.glob(os.path.join(d,"**","*.csv"), recursive=True):
            files.append(f)
    return sorted(set(files))

def classify_and_adapt(f):
    try:
        df = pd.read_csv(f, dtype=str, low_memory=False)
    except Exception:
        return None, None
    df.columns = [c.strip() for c in df.columns]
    cols = set([c.lower() for c in df.columns])
    if len({"pa","ab","h","hr","bb","so"}.intersection(cols))>=3 or "ops" in cols or "obp" in cols or "slg" in cols:
        return "bat", adapt_batting(df)
    if len({"ip","er","bb","so","k9","bb9","ipouts"}.intersection(cols))>=2:
        return "pit", adapt_pitching(df)
    if re.search(r"bat|hitter|타자", f, re.I):
        return "bat", adapt_batting(df)
    if re.search(r"pit|pitch|투수", f, re.I):
        return "pit", adapt_pitching(df)
    return None, None

# ✅ KBO 팀 화이트리스트
KBO_TEAMS = [
    "LG","KIA","KT","NC","SSG","KIWOOM","SAMSUNG","LOTTE","HANWHA","DOOSAN",
    "NEXEN","WOOSOX","WOO","SK","HYUNDAI","MBC","OB","TAEPUNG","SANGMU",
    "PACIFIC","SAMSUNG LIONS","HAITAI","BINGGRAE","SAMMI","KOREA TIGERS",
    "KIA TIGERS","HAITAI TIGERS","HYUNDAI UNICORNS","SSG LANDERS","LOTTE GIANTS",
    "HANWHA EAGLES","LG TWINS","DOOSAN BEARS","KIWOOM HEROES","NEXEN HEROES",
    "NC DINOS","KT WIZ","SK WYVERNS","MBC CHUNGYONG","OB BEARS","PACIFIC DOLPHINS",
    "SSANGBANGWOOL RAIDERS","SAMMI SUPERSTARS","BINGGRAE EAGLES"
]
def is_kbo_team(s: str) -> bool:
    s = ("" if pd.isna(s) else str(s)).upper()
    return any(t in s for t in KBO_TEAMS)

def main():
    ensure_dir(OUT_DIR)
    bats=[]; pits=[]
    for f in scan_sources():
        kind, out = classify_and_adapt(f)
        if out is None: continue
        if kind=="bat": bats.append(out)
        elif kind=="pit": pits.append(out)

    if not bats and not pits:
        raise SystemExit("[FATAL] KBO 소스에서 유효 테이블을 찾지 못함")

    if bats:
        bat_all = pd.concat(bats, ignore_index=True).fillna(0)
        bat_all["season"]=bat_all["season"].astype(str)
        bat_all["full_name"]=bat_all["full_name"].astype(str)
        bat_all["team"]=bat_all["team"].astype(str)
        num_cols = [c for c in BAT_OUT if c not in ["season","team","full_name"]]
        for c in num_cols: bat_all[c]=to_num(bat_all[c])
        bat_all = bat_all.groupby(["season","team","full_name"], as_index=False)[num_cols].max()
        # ✅ 팀 화이트리스트 필터
        bat_all = bat_all[bat_all["team"].map(is_kbo_team)]
        bat_all = bat_all[BAT_OUT]
        bat_all.to_csv(os.path.join(OUT_DIR,"kbo_batting.csv"), index=False)

    if pits:
        pit_all = pd.concat(pits, ignore_index=True).fillna(0)
        pit_all["season"]=pit_all["season"].astype(str)
        pit_all["full_name"]=pit_all["full_name"].astype(str)
        pit_all["team"]=pit_all["team"].astype(str)
        num_cols = [c for c in PIT_OUT if c not in ["season","team","full_name"]]
        for c in num_cols: pit_all[c]=to_num(pit_all[c])
        pit_all = pit_all.groupby(["season","team","full_name"], as_index=False)[num_cols].max()
        # ✅ 팀 화이트리스트 필터
        pit_all = pit_all[pit_all["team"].map(is_kbo_team)]
        pit_all = pit_all[PIT_OUT]
        pit_all.to_csv(os.path.join(OUT_DIR,"kbo_pitching.csv"), index=False)

    print("[OK] wrote:", os.path.join(OUT_DIR,"kbo_batting.csv"), ",", os.path.join(OUT_DIR,"kbo_pitching.csv"))

if __name__ == "__main__":
    main()
