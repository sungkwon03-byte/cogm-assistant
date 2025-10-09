import json, pandas as pd, numpy as np
from pathlib import Path
ROOT=Path.cwd(); OUT=ROOT/'output'; OUT.mkdir(parents=True, exist_ok=True); LOG=ROOT/'logs'; LOG.mkdir(parents=True, exist_ok=True)

ms = pd.read_csv(OUT/'mart_star.csv', low_memory=False)
for c in ['PA','IPouts','OPS','ERA','WARx']:
    if c not in ms.columns: ms[c]=np.nan
ms['year']=pd.to_numeric(ms['year'], errors='coerce')

# 타자/투수 분리해 리그 평균 근사
b = ms[ms['role'].eq('bat')].copy()
p = ms[ms['role'].eq('pit')].copy()
if 'OPS' not in b.columns: b['OPS']=pd.to_numeric(b.get('OBP',0), errors='coerce').fillna(0)+pd.to_numeric(b.get('SLG',0), errors='coerce').fillna(0)
lg_ops = b.groupby('year')['OPS'].mean()
lg_era = p.groupby('year')['ERA'].mean()

ms['WARx']=pd.to_numeric(ms['WARx'], errors='coerce')
ms['WARx']=ms['WARx'].fillna(0)

# 각 팀/연도/롤 Top3
g = (ms.sort_values('WARx', ascending=False)
       .groupby(['year','teamID','role'], as_index=False)
       .head(3))
req = g[['teamID','year','playerID','role']].copy()
req['days_IL']=30

key=['year','teamID','playerID','role']
sim = req.merge(ms[key+['PA','IPouts','OPS','ERA','WARx']], on=key, how='left')
sim['delta_WAR']  = - (sim['WARx'].fillna(0)/ (162/ sim['days_IL'].clip(lower=1)))  # 간단 축소
sim['delta_wins'] = sim['delta_WAR']  # WAR~wins 근사

sim['note'] = np.where(sim['WARx'].fillna(0).eq(0.0), 'MISS or zero-WARx', '')
out=OUT/'scenario_alt.csv'
sim.to_csv(out, index=False)
print(f"[DAY66] {out} rows=", len(sim))
