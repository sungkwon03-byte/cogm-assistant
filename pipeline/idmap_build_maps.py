import pandas as pd, json
from pathlib import Path
ROOT=Path.cwd(); DATA=ROOT/'data'; OUT=ROOT/'mappings'
OUT.mkdir(exist_ok=True, parents=True)

def norm(s:pd.Series)->pd.Series:
    return (s.astype(str).str.strip()
            .str.replace(r'\.0$','',regex=True)
            .replace({'nan':'','NaN':'','<NA>':'','None':'','NULL':''}))

def find_one(pats):
    for pat in pats:
        cc=list((DATA/'lahman_extracted').rglob(pat)) or list(DATA.rglob(pat))
        if cc:
            cc.sort(key=lambda p:p.stat().st_size, reverse=True); return cc[0]
    return None

# People/Master → playerID -> bbrefID / retroID
p=find_one(["People.csv","Master.csv"])
if p:
    ppl=pd.read_csv(p, dtype=str, low_memory=False).fillna('')
    for c in ['playerID','bbrefID','retroID']:
        if c not in ppl: ppl[c]=''
    ppl[['playerID','bbrefID','retroID']]=ppl[['playerID','bbrefID','retroID']].apply(norm)
    ppl[['playerID','bbrefID']].drop_duplicates().to_csv(OUT/'pid2bb.csv', index=False)
    ppl[['playerID','retroID']].drop_duplicates().to_csv(OUT/'pid2rt.csv', index=False)

# Chadwick register (컬럼명이 제각각이라 alias 처리)
reg=None
for pat in ["chadwick*register*.csv","*Chadwick*.csv","*chadwick*.csv","chadwick_register.csv"]:
    cc=list(DATA.rglob(pat))
    if cc: cc.sort(key=lambda p:p.stat().st_size, reverse=True); reg=cc[0]; break

if reg:
    r=pd.read_csv(reg, dtype=str, low_memory=False).fillna('')
    alias={'key_bbref':'bbrefID','bbref':'bbrefID','key_retro':'retroID','retro':'retroID',
           'key_mlbam':'mlbam','mlbam':'mlbam','mlb_id':'mlbam',
           'key_fangraphs':'fgID','fg_id':'fgID','fangraphs':'fgID',
           'name_first':'nameFirst','name_last':'nameLast','birth_year':'birthYear'}
    r.columns=[alias.get(c,c) for c in r.columns]
    for c in ['bbrefID','retroID','mlbam','fgID','nameFirst','nameLast']:
        if c in r: r[c]=norm(r[c])
    # bbref/retro 중심 맵
    if set(['bbrefID','mlbam']).issubset(r.columns): r[['bbrefID','mlbam']].drop_duplicates().to_csv(OUT/'bb2m.csv', index=False)
    if set(['bbrefID','fgID']).issubset(r.columns):  r[['bbrefID','fgID']].drop_duplicates().to_csv(OUT/'bb2f.csv', index=False)
    if set(['retroID','mlbam']).issubset(r.columns): r[['retroID','mlbam']].drop_duplicates().to_csv(OUT/'rt2m.csv', index=False)
    if set(['retroID','fgID']).issubset(r.columns):  r[['retroID','fgID']].drop_duplicates().to_csv(OUT/'rt2f.csv', index=False)
    # 이름만 키 fallback
    if set(['nameFirst','nameLast']).issubset(r.columns):
        rk=r.copy(); rk['k']=rk['nameFirst'].str.lower()+'|'+rk['nameLast'].str.lower()
        rk[['k','mlbam']].drop_duplicates().to_csv(OUT/'nm2m.csv', index=False)
        rk[['k','fgID']].drop_duplicates().to_csv(OUT/'nm2f.csv', index=False)

print("[MAPS] built in ./mappings")
