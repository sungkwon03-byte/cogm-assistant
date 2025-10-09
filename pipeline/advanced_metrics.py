import pandas as pd, numpy as np
from pathlib import Path
ROOT=Path.cwd(); OUT=ROOT/'output'; OUT.mkdir(exist_ok=True)
def _ff(name):
    c=list((ROOT/'data'/'lahman_extracted').rglob(name)) or list((ROOT/'data').rglob(name))
    assert c, f"{name} not found"; c.sort(key=lambda p:p.stat().st_size, reverse=True); return c[0]
bat=pd.read_csv(_ff('Batting.csv'), low_memory=False)
pit=pd.read_csv(_ff('Pitching.csv'), low_memory=False)
# ---- hitters ----
for c in ['H','AB','BB','SO','SF','HBP','2B','3B','HR','yearID','playerID']: 
    if c not in bat.columns: bat[c]=0
bat['PA']=bat[['AB','BB','HBP','SF']].sum(axis=1, min_count=1)
bat['1B']=bat['H']-bat['2B']-bat['3B']-bat['HR']
bat['SLG']=(bat['1B']+2*bat['2B']+3*bat['3B']+4*bat['HR'])/bat['AB'].replace(0,np.nan)
bat['OBP']=(bat['H']+bat['BB']+bat['HBP'])/(bat['AB']+bat['BB']+bat['HBP']+bat['SF']).replace(0,np.nan)
bat['OPS']=bat['OBP']+bat['SLG']
lgOPS=bat.groupby('yearID')['OPS'].mean().rename('lgOPS').reset_index()
b=bat.merge(lgOPS, on='yearID', how='left')
b['OPS_plus']=100*(b['OPS']/b['lgOPS'])
b['BABIP']=(b['H']-b['HR'])/(b['AB']-b['SO']+b['SF'].fillna(0)).replace(0,np.nan)
b['BB%']=b['BB']/b['PA']; b['K%']=b['SO']/b['PA']
b_out=b[['yearID','playerID','PA','OPS_plus','BABIP','BB%','K%']].rename(columns={'yearID':'year'})
b_out.to_csv(OUT/'advanced_hit.csv', index=False)
# ---- pitchers ----
for c in ['IPouts','SO','BB','HR','H','yearID','playerID']: 
    if c not in pit.columns: pit[c]=0
pit['IP']=pit['IPouts']/3.0
pit['K9']=9*pit['SO']/pit['IP'].replace(0,np.nan)
pit['BB9']=9*pit['BB']/pit['IP'].replace(0,np.nan)
pit['HR9']=9*pit['HR']/pit['IP'].replace(0,np.nan)
# xFIP 근사: HR을 리그 평균 HR/FB로 대체 (FB 데이터 없으므로 HR을 평균 HR/9로 대체하는 간단 근사)
lgHR9=pit.groupby('yearID')['HR9'].mean().rename('lgHR9').reset_index()
p=pit.merge(lgHR9, on='yearID', how='left')
p['xFIP_like']= (13*(p['lgHR9']/9.0) + 3*(p['BB']/p['IP'].replace(0,np.nan)) - 2*(p['SO']/p['IP'].replace(0,np.nan))) + 3.2
p_out=p[['yearID','playerID','IP','K9','BB9','HR9','xFIP_like']].rename(columns={'yearID':'year'})
p_out.to_csv(OUT/'advanced_pit.csv', index=False)
print(f"[ADV] hit_rows={len(b_out)} pit_rows={len(p_out)}")
