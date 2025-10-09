import sys, pandas as pd
from pathlib import Path
ROOT=Path.cwd(); OUT=ROOT/'output'; DATA=ROOT/'data'
SRC=OUT/'mart_star.csv'
DST=OUT/'mart_star_idfix.csv'
CHUNK=int(sys.argv[1]) if len(sys.argv)>1 else 20000

def norm_ser(s):
    s=s.astype(str).str.strip().str.replace(r'\.0$','',regex=True)
    return s.replace({'nan':'','NaN':'','<NA>':'','None':'','NULL':''})

def find_one(pats):
    for pat in pats:
        cc=list((DATA/'lahman_extracted').rglob(pat)) or list(DATA.rglob(pat))
        if cc:
            cc.sort(key=lambda p:p.stat().st_size, reverse=True)
            return cc[0]
    return None

def load_people_maps():
    p=find_one(["People.csv","Master.csv"])
    maps={'pid2bb':{},'pid2rt':{}}
    if not p: return maps
    usecols=None
    try:
        df=pd.read_csv(p, dtype=str, low_memory=False).fillna('')
    except Exception:
        df=pd.read_csv(p, dtype=str, low_memory=False)
    for c in ['playerID','bbrefID','retroID']:
        if c not in df.columns: df[c]=''
    df[['playerID','bbrefID','retroID']]=df[['playerID','bbrefID','retroID']].apply(norm_ser)
    maps['pid2bb']=dict(df[['playerID','bbrefID']].dropna().values)
    maps['pid2rt']=dict(df[['playerID','retroID']].dropna().values)
    return maps

def load_register_maps():
    reg=None
    for pat in ["chadwick*register*.csv","*Chadwick*.csv","*chadwick*.csv","chadwick_register.csv"]:
        cc=list(DATA.rglob(pat))
        if cc:
            cc.sort(key=lambda p:p.stat().st_size, reverse=True)
            reg=cc[0]; break
    maps={'bb2m':{},'bb2f':{},'rt2m':{},'rt2f':{},'nm2m':{},'nm2f':{}}
    if not reg: return maps
    r=pd.read_csv(reg, dtype=str, low_memory=False).fillna('')
    alias={'key_bbref':'bbrefID','bbref':'bbrefID','key_retro':'retroID','retro':'retroID',
           'key_mlbam':'mlbam','mlbam':'mlbam','mlb_id':'mlbam',
           'key_fangraphs':'fgID','fg_id':'fgID','fangraphs':'fgID',
           'name_first':'nameFirst','name_last':'nameLast','birth_year':'birthYear'}
    r.columns=[alias.get(c,c) for c in r.columns]
    for c in ['bbrefID','retroID','mlbam','fgID','nameFirst','nameLast','birthYear']:
        if c in r: r[c]=norm_ser(r[c])
    # bbref/retro 기반
    if set(['bbrefID','mlbam']).issubset(r.columns):
        maps['bb2m']=dict(r[['bbrefID','mlbam']].dropna().values)
    if set(['bbrefID','fgID']).issubset(r.columns):
        maps['bb2f']=dict(r[['bbrefID','fgID']].dropna().values)
    if set(['retroID','mlbam']).issubset(r.columns):
        maps['rt2m']=dict(r[['retroID','mlbam']].dropna().values)
    if set(['retroID','fgID']).issubset(r.columns):
        maps['rt2f']=dict(r[['retroID','fgID']].dropna().values)
    # 이름만 키(생년 제외) — 안전 fallback
    if set(['nameFirst','nameLast']).issubset(r.columns):
        rk=r.copy()
        rk['k']=rk['nameFirst'].str.lower()+'|'+rk['nameLast'].str.lower()
        maps['nm2m']=dict(rk[['k','mlbam']].dropna().values)
        maps['nm2f']=dict(rk[['k','fgID']].dropna().values)
    return maps

if not SRC.exists() or SRC.stat().st_size==0:
    print("E01: mart_star.csv missing/empty", file=sys.stderr); sys.exit(1)

people=load_people_maps()
reg=load_register_maps()

# header 준비(추가 컬럼 보장)
hdr=pd.read_csv(SRC, nrows=0, dtype=str, low_memory=False)
for k in ['bbrefID','retroID','mlbam','fgID']:
    if k not in hdr.columns: hdr[k]=''
if not DST.exists() or DST.stat().st_size==0:
    hdr.to_csv(DST, index=False)

# 이미 쓴 행 수(헤더 제외)
done=0
if DST.exists():
    with DST.open('r', encoding='utf-8', errors='ignore') as f:
        done=sum(1 for _ in f)-1
total=sum(1 for _ in SRC.open('r', encoding='utf-8', errors='ignore'))-1

rdr=pd.read_csv(SRC, chunksize=CHUNK, dtype=str, low_memory=False)
skipped=done
processed=0

for chunk in rdr:
    if skipped>0:
        if skipped>=len(chunk):
            skipped-=len(chunk); continue
        else:
            chunk=chunk.iloc[skipped:].copy(); skipped=0
    chunk=chunk.fillna('')
    for k in ['bbrefID','retroID','mlbam','fgID','player_name','playerID']:
        if k not in chunk.columns: chunk[k]=''
    for k in ['bbrefID','retroID','mlbam','fgID','playerID']:
        chunk[k]=norm_ser(chunk[k])

    # 이름 파싱 + name-only key
    nm=chunk['player_name'].astype(str).str.split(',', n=1, expand=True)
    chunk['nameLast']=nm[0].str.strip()
    chunk['nameFirst']=nm[1].str.strip() if nm.shape[1]>1 else ''
    namekey=(chunk['nameFirst'].str.lower()+'|'+chunk['nameLast'].str.lower())

    # 1) People: playerID -> bbref/retro
    if people.get('pid2bb'):
        miss=chunk['bbrefID'].eq('')
        chunk.loc[miss,'bbrefID']=chunk.loc[miss,'playerID'].map(people['pid2bb']).fillna('')
    if people.get('pid2rt'):
        miss=chunk['retroID'].eq('')
        chunk.loc[miss,'retroID']=chunk.loc[miss,'playerID'].map(people['pid2rt']).fillna('')

    # 2) Register: bbref/retro -> mlbam/fgID
    for tgt,src_col,mp in [('mlbam','bbrefID','bb2m'),('fgID','bbrefID','bb2f'),
                           ('mlbam','retroID','rt2m'),('fgID','retroID','rt2f')]:
        if reg.get(mp):
            miss=chunk[tgt].eq('')
            chunk.loc[miss,tgt]=chunk.loc[miss,src_col].map(reg[mp]).fillna('')

    # 3) Fallback: name-only -> mlbam/fgID
    if reg.get('nm2m'):
        miss=chunk['mlbam'].eq('')
        chunk.loc[miss,'mlbam']=namekey[miss].map(reg['nm2m']).fillna('')
    if reg.get('nm2f'):
        miss=chunk['fgID'].eq('')
        chunk.loc[miss,'fgID']=namekey[miss].map(reg['nm2f']).fillna('')

    # 저장
    use=['year','teamID','playerID','player_name','bbrefID','mlbam','retroID','fgID']
    chunk[use].to_csv(DST, mode='a', header=False, index=False)
    processed+=len(chunk)
    print(f"[PROG] {processed+done}/{total}", flush=True)

print("[DONE] idmap run complete")
