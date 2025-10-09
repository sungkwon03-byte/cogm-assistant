import os, pandas as pd, numpy as np
from pathlib import Path
ROOT=Path.cwd(); OUT=ROOT/'output'; OUT.mkdir(exist_ok=True)
base=ROOT/'output'/'cache'/'statcast_clean'; 
if not base.exists(): base = ROOT/'output'/'cache'/'statcast'
files=sorted(base.glob("*.csv"))[:int(os.getenv("STATCAST_MAX_FILES","999999"))]
rows=[]
for fp in files:
    try:
        df=pd.read_csv(fp, usecols=['game_year','game_date','batter','pitcher','description','launch_speed','zone'], low_memory=False)
    except: 
        continue
    if df.empty: continue
    df=df.rename(columns={'game_year':'year'})
    df['game_date']=pd.to_datetime(df['game_date'], errors='coerce')
    df=df.dropna(subset=['game_date'])
    df['half']=np.where(df['game_date'].dt.month<=6,'H1','H2')
    # 배터 whiff/EV
    df['in_zone']=df['zone'].between(1,9)
    whiff=df['description'].fillna('').str.contains('swinging_strike|swinging_strike_blocked', case=False, regex=True)
    bat=df.groupby(['year','batter','half']).agg(whiff_rate=('description',lambda s: (s.str.contains('swing',case=False)&whiff).sum()/max(1,(s.str.contains('swing',case=False)).sum())),
                                                 avg_ev=('launch_speed','mean')).reset_index()
    bat=bat.pivot_table(index=['year','batter'], columns='half', values=['whiff_rate','avg_ev'])
    bat.columns=['_'.join(c) for c in bat.columns]
    bat=bat.reset_index()
    if not bat.empty: rows.append(bat)
out=pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
out.to_csv(OUT/'stickiness_half.csv', index=False)
print(f"[STICK] -> {OUT/'stickiness_half.csv'} rows={len(out)}")
