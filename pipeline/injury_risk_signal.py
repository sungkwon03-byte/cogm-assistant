import pandas as pd, numpy as np
from pathlib import Path
ROOT=Path.cwd(); OUT=ROOT/'output'; OUT.mkdir(exist_ok=True)
feat=pd.read_csv(OUT/'statcast_features_player_year.csv', low_memory=False)
pit=feat[feat['role']=='pit'].copy()
# 연도별 정렬
pit=pit.sort_values(['mlbam','year'])
# 지표: avg_spin, avg_ext, csw_rate
for c in ['avg_spin','avg_ext','csw_rate']:
    if c not in pit.columns: pit[c]=np.nan
# 전년 대비 하락률
for c in ['avg_spin','avg_ext','csw_rate']:
    pit[f'delta_{c}']=pit.groupby('mlbam')[c].diff()
# 간단 threshold: 스핀 -150 rpm↓ or 익스텐션 -0.2 ft↓ or CSW -3%p↓
risk=((pit['delta_avg_spin']<=-150) | (pit['delta_avg_ext']<=-0.2) | (pit['delta_csw_rate']<=-0.03)).astype(int)
pit['injury_risk_flag']=risk
out=OUT/'injury_risk_flags.csv'; pit[['year','mlbam','player_name','delta_avg_spin','delta_avg_ext','delta_csw_rate','injury_risk_flag']].to_csv(out, index=False)
print(f"[RISK] -> {out} rows={len(pit)} pos={risk.sum()}")
