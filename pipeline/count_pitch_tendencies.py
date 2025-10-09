import pandas as pd, numpy as np
from pathlib import Path
ROOT=Path.cwd(); OUT=ROOT/'output'; OUT.mkdir(exist_ok=True)
bat=pd.read_csv(OUT/'statcast_pitch_mix_detailed_plus_bat.csv', low_memory=False)

def to_num(x): return pd.to_numeric(x, errors='coerce').fillna(0)
# 안전 가드
for c in ['pitches','zone_rate','z_swing_rate','o_swing_rate','z_contact_rate','o_contact_rate','z_csw_rate','chase_rate','edge_rate','heart_rate']:
    if c not in bat.columns: bat[c]=0.0

g=(bat
   .assign(pitches=to_num(bat['pitches']))
   .groupby(['year','mlbam','vhb'], as_index=False)
   .agg({'pitches':'sum',
         'zone_rate':'mean','z_swing_rate':'mean','o_swing_rate':'mean',
         'z_contact_rate':'mean','o_contact_rate':'mean',
         'z_csw_rate':'mean','chase_rate':'mean',
         'edge_rate':'mean','heart_rate':'mean'}))

# 간단 안정성(연속 세그간 분산)
stab=(g.groupby(['mlbam'])[['z_contact_rate','z_csw_rate','chase_rate']].std()
        .rename(columns=lambda c: f"{c}_std").reset_index())

res=g.copy()
res.to_csv(OUT/'count_tendencies_bat.csv', index=False)
stab.to_csv(OUT/'bat_stability.csv', index=False)
print(f"[DAY70] count_tendencies_bat.csv rows={len(res)}; bat_stability.csv rows={len(stab)}")
