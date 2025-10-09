import pandas as pd, numpy as np
from pathlib import Path
ROOT=Path.cwd(); OUT=ROOT/'output'; OUT.mkdir(exist_ok=True)
df=pd.read_csv(OUT/'statcast_pitch_mix_detailed_plus_bat.csv', low_memory=False)

def to_num(s): return pd.to_numeric(s, errors='coerce')
need=['pitches','z_contact_rate','z_csw_rate','chase_rate','edge_rate','heart_rate','zone_rate']
for c in need:
    if c not in df.columns: df[c]=0.0
df[need]=df[need].apply(to_num)

# 약점 지표(가중 평균) — apply 제거
agg=(df.groupby(['year','mlbam'], as_index=False)
       .agg(pitches=('pitches','sum'),
            zc=('z_contact_rate','mean'),
            zcsw=('z_csw_rate','mean'),
            chase=('chase_rate','mean'),
            edge=('edge_rate','mean'),
            heart=('heart_rate','mean'),
            zone=('zone_rate','mean')))
agg['weak_zone_edge'] = (1-agg['zc'])*agg['edge']*agg['zone']
agg['heart_chase_idx']= agg['chase']*agg['heart']

agg.rename(columns={'mlbam':'player_mlbam'}, inplace=True)
agg.to_csv(OUT/'weakness_map_player_year.csv', index=False)
print(f"[DAY70] weakness_map_player_year.csv rows={len(agg)}")
