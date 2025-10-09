import os, json, pandas as pd, numpy as np
from pathlib import Path
ROOT=Path.cwd(); LOG=ROOT/'logs'; LOG.mkdir(exist_ok=True)
def ffind(name):
    c=list((ROOT/'data'/'lahman_extracted').rglob(name)) or list((ROOT/'data').rglob(name))
    assert c, f'{name} not found'; c.sort(key=lambda p:p.stat().st_size, reverse=True); return c[0]

# 입력
ms  = pd.read_csv(ROOT/'output'/'mart_star.csv', low_memory=False)
bat = pd.read_csv(ffind('Batting.csv'),  low_memory=False)
pit = pd.read_csv(ffind('Pitching.csv'), low_memory=False)
teams = pd.read_csv(ffind('Teams.csv'),   low_memory=False)[['yearID','teamID','lgID']]

# 범위: AL/NL & 1901+
modern = teams[(teams['lgID'].isin(['AL','NL'])) & (teams['yearID']>=1901)][['yearID','teamID']].drop_duplicates()
modern['teamID']=modern['teamID'].astype(str).str.upper()

# mart 재가공
ms['year']=pd.to_numeric(ms['year'], errors='coerce')
ms['teamID']=ms['teamID'].astype(str).str.upper()
msb = ms[ms['role']=='bat'].groupby(['year','teamID'],as_index=False).agg(HR=('HR','sum'),PA=('PA','sum'))
msp = ms[ms['role']=='pit'].groupby(['year','teamID'],as_index=False).agg(ER=('ER','sum'),IPouts=('IPouts','sum'))

# lahman 재가공
for c in ['AB','BB','HBP','SF','SH','HR']:
    if c not in bat.columns: bat[c]=0
bat['PA']=bat[['AB','BB','HBP','SF','SH']].apply(pd.to_numeric, errors='coerce').fillna(0).sum(axis=1)
batg = bat.groupby(['yearID','teamID'],as_index=False).agg(HR=('HR','sum'),PA=('PA','sum')).rename(columns={'yearID':'year'})
pit['IPouts'] = pit.get('IPouts', np.nan)
if pit['IPouts'].isna().any() and 'IP' in pit.columns:
    def ip2outs(x):
        try:
            f=float(x); i=int(f); d=round(f-i,1)
            return i*3 + (1 if abs(d-0.1)<1e-8 else 2 if abs(d-0.2)<1e-8 else 0)
        except: return np.nan
    pit['IPouts']=pit['IP'].map(ip2outs)
pitg = pit.groupby(['yearID','teamID'],as_index=False).agg(ER=('ER','sum'),IPouts=('IPouts','sum')).rename(columns={'yearID':'year'})

# modern 키로 제한
key = modern.rename(columns={'yearID':'year'})
msb = msb.merge(key, on=['year','teamID'], how='inner')
msp = msp.merge(key, on=['year','teamID'], how='inner')
batg= batg.merge(key, on=['year','teamID'], how='inner')
pitg= pitg.merge(key, on=['year','teamID'], how='inner')

# 비교
cmpb = msb.merge(batg, on=['year','teamID'], suffixes=('_mart','_lah'), how='outer')
cmpp = msp.merge(pitg, on=['year','teamID'], suffixes=('_mart','_lah'), how='outer')

def pass_rate(df, a, b, tol_rel=0.01, tol_abs=1.0):
    d=(df[a].fillna(0)-df[b].fillna(0)).abs()
    base=df[b].abs().replace(0, np.nan)
    ok=((d<=tol_abs) | ((d/base)<=tol_rel)).fillna(True)  # 베이스 0이면 절대값 기준
    return float(ok.mean())

summary = {
  "rows_b": int(len(cmpb)), "rows_p": int(len(cmpp)),
  "HR_pass": pass_rate(cmpb,'HR_mart','HR_lah'),
  "PA_pass": pass_rate(cmpb,'PA_mart','PA_lah'),
  "ER_pass": pass_rate(cmpp,'ER_mart','ER_lah'),
  "IPouts_pass": pass_rate(cmpp,'IPouts_mart','IPouts_lah'),
}
summary["pass_rate"] = round(np.mean([summary[k] for k in ['HR_pass','PA_pass','ER_pass','IPouts_pass']]),3)

# 산출
bench = pd.concat([
    cmpb[['year','teamID','HR_mart','HR_lah','PA_mart','PA_lah']],
    cmpp[['year','teamID','ER_mart','ER_lah','IPouts_mart','IPouts_lah']]
], axis=1)
bench.to_csv(ROOT/'tests'/'benchmark_set.csv', index=False)
(Path(LOG/'day63_hotfix_summary.json')).write_text(json.dumps(summary, indent=2), encoding='utf-8')
print("[DAY63-HOTFIX]", json.dumps(summary))
