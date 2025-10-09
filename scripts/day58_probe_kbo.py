#!/usr/bin/env python3
import os, json, pandas as pd

ROOTS=[".","data","mart","external","inputs","downloads"]
EXCLUDE=("venv","/env","\\env","/logs","\\logs","/output","\\output","/lahman_extracted","/retrosheet")
KBO_ORGS={"KIA","LOT","DOO","LG","SSG","KIW","HAN","SAM","NC","KT",
          "KIA TIGERS","LOTTE","DOOSAN","LG TWINS","SSG LANDERS","KIWOOM","HANWHA","SAMSUNG","NC DINOS","KT WIZ"}

def should_skip(path:str)->bool:
    low=path.lower()
    return any(x in low for x in EXCLUDE)

cands=[]
for root in ROOTS:
    for r,_,files in os.walk(root):
        if should_skip(r): continue
        for f in files:
            if not f.lower().endswith(".csv"): continue
            p=os.path.join(r,f)
            if "kbo" in p.lower(): cands.append(p)

# fallback: 헤더 기반 추가 후보 (파일명에 kbo가 없어도)
fallback=[]
for root in ROOTS:
    for r,_,files in os.walk(root):
        if should_skip(r): continue
        for f in files:
            if not f.lower().endswith(".csv"): continue
            p=os.path.join(r,f)
            if p in cands: continue
            try:
                df=pd.read_csv(p, nrows=0, low_memory=False)
                cols=[c.lower() for c in df.columns]
                if any(k in cols for k in ("average_batter_age","average_age","age")) and any(k in cols for k in ("team","tm","teamid","org","season","year")):
                    fallback.append(p)
            except Exception:
                pass

cands=list(dict.fromkeys(cands))+fallback  # uniq+append
hits={"KBO_BAT":[], "KBO_PIT":[], "KBO_DETAIL":[]}
for p in cands:
    try:
        df=pd.read_csv(p, nrows=250, low_memory=False)
        cols=[c.lower() for c in df.columns]
        # KBO 여부: 팀코드 or 'kbo' 텍스트 추정
        is_kbo=False
        for tc in ["team","tm","teamid","org"]:
            if tc in cols:
                s=df[df.columns[cols.index(tc)]].astype(str).str.upper().str.strip()
                if s.isin(list(KBO_ORGS)).any(): is_kbo=True; break
        if not is_kbo:
            for lc in ["league","lg","lgid"]:
                if lc in cols:
                    s=df[df.columns[cols.index(lc)]].astype(str).str.upper()
                    if s.str.contains("KBO").any(): is_kbo=True; break
        if not is_kbo and "kbo" not in p.lower():
            continue

        if "average_batter_age" in cols or "avg_batter_age" in cols or "avg_age_bat" in cols:
            hits["KBO_BAT"].append(p); continue
        if "average_age" in cols or "avg_pitcher_age" in cols or "avg_age_pit" in cols:
            hits["KBO_PIT"].append(p); continue
        if "age" in cols:
            hits["KBO_DETAIL"].append(p); continue
    except Exception:
        pass

os.makedirs("logs", exist_ok=True)
open("logs/day58_kbo_paths.json","w",encoding="utf-8").write(json.dumps(hits, ensure_ascii=False, indent=2))
print(json.dumps(hits, ensure_ascii=False, indent=2))
print("[OK] wrote logs/day58_kbo_paths.json")
