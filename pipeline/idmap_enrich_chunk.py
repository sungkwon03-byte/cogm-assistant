import sys, pandas as pd
from pathlib import Path
IN=Path(sys.argv[1]); OUT=Path(sys.argv[2]); MAP=Path(sys.argv[3])  # args: in out mappings_dir

def norm(s:pd.Series)->pd.Series:
    return (s.astype(str).str.strip()
            .str.replace(r'\.0$','',regex=True)
            .replace({'nan':'','NaN':'','<NA>':'','None':'','NULL':''}))

# 맵 읽기(작아서 매 샤드마다 로드해도 OK)
def read_map(path, k, v):
    p=path/(path.name and '')
    df=pd.read_csv(path, dtype=str, low_memory=False).fillna('')
    df[[k,v]]=df[[k,v]].apply(norm)
    return dict(df[[k,v]].values)

maps_dir=MAP
pid2bb = read_map(maps_dir/'pid2bb.csv','playerID','bbrefID') if (maps_dir/'pid2bb.csv').exists() else {}
pid2rt = read_map(maps_dir/'pid2rt.csv','playerID','retroID') if (maps_dir/'pid2rt.csv').exists() else {}
bb2m   = read_map(maps_dir/'bb2m.csv','bbrefID','mlbam')       if (maps_dir/'bb2m.csv').exists()   else {}
bb2f   = read_map(maps_dir/'bb2f.csv','bbrefID','fgID')        if (maps_dir/'bb2f.csv').exists()   else {}
rt2m   = read_map(maps_dir/'rt2m.csv','retroID','mlbam')       if (maps_dir/'rt2m.csv').exists()   else {}
rt2f   = read_map(maps_dir/'rt2f.csv','retroID','fgID')        if (maps_dir/'rt2f.csv').exists()   else {}
nm2m   = read_map(maps_dir/'nm2m.csv','k','mlbam')             if (maps_dir/'nm2m.csv').exists()   else {}
nm2f   = read_map(maps_dir/'nm2f.csv','k','fgID')              if (maps_dir/'nm2f.csv').exists()   else {}

df=pd.read_csv(IN, dtype=str, low_memory=False).fillna('')
for c in ['bbrefID','retroID','mlbam','fgID','playerID','player_name']:
    if c not in df: df[c]=''
df[['bbrefID','retroID','mlbam','fgID','playerID']]=df[['bbrefID','retroID','mlbam','fgID','playerID']].apply(norm)

# 이름 키
nm=df['player_name'].astype(str).str.split(',', n=1, expand=True)
df['nameLast']=nm[0].str.strip()
df['nameFirst']=nm[1].str.strip() if nm.shape[1]>1 else ''
namekey=(df['nameFirst'].str.lower()+'|'+df['nameLast'].str.lower())

# 1) playerID -> bbref,retro
miss=df['bbrefID'].eq('')
df.loc[miss,'bbrefID']=df.loc[miss,'playerID'].map(pid2bb).fillna('')
miss=df['retroID'].eq('')
df.loc[miss,'retroID']=df.loc[miss,'playerID'].map(pid2rt).fillna('')

# 2) bbref/retro -> mlbam,fg
for tgt,src,mp in [('mlbam','bbrefID',bb2m),('fgID','bbrefID',bb2f),('mlbam','retroID',rt2m),('fgID','retroID',rt2f)]:
    if mp:
        miss=df[tgt].eq('')
        df.loc[miss,tgt]=df.loc[miss,src].map(mp).fillna('')

# 3) fallback: name-only
if nm2m:
    miss=df['mlbam'].eq('')
    df.loc[miss,'mlbam']=namekey[miss].map(nm2m).fillna('')
if nm2f:
    miss=df['fgID'].eq('')
    df.loc[miss,'fgID']=namekey[miss].map(nm2f).fillna('')

df[['year','teamID','playerID','player_name','bbrefID','mlbam','retroID','fgID']].to_csv(OUT, index=False)
