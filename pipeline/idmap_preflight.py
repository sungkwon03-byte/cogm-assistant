import sys, json, pandas as pd
from pathlib import Path
ROOT=Path.cwd(); OUT=ROOT/'output'; DATA=ROOT/'data'
res={"ok":False,"errors":[],"warnings":[],"paths":{},"sample_ok":False}

def find_one(pats):
    for pat in pats:
        cc=list((DATA/'lahman_extracted').rglob(pat)) or list(DATA.rglob(pat))
        if cc:
            cc.sort(key=lambda p:p.stat().st_size, reverse=True)
            return cc[0]
    return None

try:
    src=OUT/'mart_star.csv'
    if not src.exists() or src.stat().st_size==0:
        res["errors"].append("E01: mart_star.csv missing/empty"); raise SystemExit
    res["paths"]["mart_star"]=str(src)
    # 빠른 헤더 검증
    head=pd.read_csv(src, nrows=10, dtype=str, low_memory=False)
    for k in ["year","teamID","playerID"]:
        if k not in head.columns:
            res["errors"].append(f"E02: mart_star missing column {k}")
    # People/Master
    ppl=find_one(["People.csv","Master.csv"])
    if ppl is None:
        res["warnings"].append("W01: People/Master not found (bbref<->retro fill reduced)")
    res["paths"]["people"]=str(ppl) if ppl else None
    # Chadwick register
    reg=None
    for pat in ["chadwick*register*.csv","*Chadwick*.csv","*chadwick*.csv","chadwick_register.csv"]:
        cc=list(DATA.rglob(pat))
        if cc:
            cc.sort(key=lambda p:p.stat().st_size, reverse=True)
            reg=cc[0]; break
    if reg is None:
        res["warnings"].append("W02: Chadwick register not found (mlbam/fg fill reduced)")
    res["paths"]["register"]=str(reg) if reg else None

    if res["errors"]:
        print(json.dumps(res,indent=2)); sys.exit(1)

    # 샘플 2k로 타입/로직 건전성 체크
    sample=pd.read_csv(src, nrows=2000, dtype=str, low_memory=False).fillna('')
    for k in ["bbrefID","retroID","mlbam","fgID","player_name"]:
        if k not in sample.columns: sample[k]=''
    # 가벼운 표준화
    for k in ["bbrefID","retroID","mlbam","fgID"]:
        sample[k]=sample[k].astype(str).str.strip().str.replace(r'\.0$','',regex=True).replace({'nan':'','<NA>':''})
    res["sample_ok"]=True
    res["ok"]=True
    print(json.dumps(res,indent=2))
except Exception as e:
    res["errors"].append(f"E99: {e.__class__.__name__}: {e}")
    print(json.dumps(res,indent=2)); sys.exit(2)
