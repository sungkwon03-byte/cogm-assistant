import pandas as pd, numpy as np, datetime as dt
from pathlib import Path
from _util_safe import zscore
ROOT=Path.cwd(); OUT=ROOT/'output'
df=pd.read_csv(OUT/'statcast_features_player_year.csv', low_memory=False)
for c in ['hardhit_rate','whiff_rate','csw_rate']: 
    if c not in df: df[c]=np.nan
df['hardhit_z']=df.groupby('year')['hardhit_rate'].transform(zscore)
df['whiff_z']  =df.groupby('year')['whiff_rate'].transform(zscore)
df['csw_z']    =df.groupby('year')['csw_rate'].transform(zscore)
flag=((df['hardhit_z']<-1.0).astype(int)+(df['whiff_z']>1.0).astype(int)+(df['csw_z']<-1.0).astype(int))>=2
out=df[['year','mlbam','player_name','role','hardhit_rate','whiff_rate','csw_rate','hardhit_z','whiff_z','csw_z']].copy()
out['injury_risk_flag']=flag.astype(int)
out.to_csv(OUT/'injury_risk_flags.csv', index=False)
print("[OK] injury_risk_flags.csv", len(out))
