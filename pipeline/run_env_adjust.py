import pandas as pd, numpy as np
from _util72 import ffind, to_num
from pathlib import Path
OUT=Path.cwd()/'output'; OUT.mkdir(exist_ok=True)

teams=pd.read_csv(ffind('Teams.csv'), low_memory=False)
t=teams.rename(columns={'yearID':'year'})[['year','lgID','G','R','HR','BB','SO']].copy()
for c in ['G','R','HR','BB','SO']: t[c]=to_num(t[c])
g=(t.groupby(['year','lgID'], as_index=False)
     .agg(G=('G','sum'), R=('R','sum'), HR=('HR','sum'), BB=('BB','sum'), SO=('SO','sum')))
g['R_per_G']=g['R']/g['G']; g['HR_per_G']=g['HR']/g['G']
g['BB_per_G']=g['BB']/g['G']; g['SO_per_G']=g['SO']/g['G']
g.to_csv(OUT/'league_runenv.csv', index=False)
print("[72] league_runenv.csv rows=", len(g))
