import pandas as pd, numpy as np
from pathlib import Path
ROOT=Path.cwd(); OUT=ROOT/'output'; OUT.mkdir(exist_ok=True)

def _ff(name):
    c=list((ROOT/'data'/'lahman_extracted').rglob(name)) or list((ROOT/'data').rglob(name))
    if not c: raise FileNotFoundError(name)
    c.sort(key=lambda p:p.stat().st_size, reverse=True); return c[0]

bat=pd.read_csv(_ff('Batting.csv'), low_memory=False)
bat=bat.rename(columns={'yearID':'year','playerID':'playerID'})
for c in ['AB','BB','H','HBP','SF','SH','SO','2B','3B','HR']:
    if c not in bat.columns: bat[c]=0
bat['PA']=bat['AB']+bat['BB']+bat.get('HBP',0)+bat.get('SF',0)+bat.get('SH',0)
bat['1B']=bat['H']-bat['2B']-bat['3B']-bat['HR']
# wOBA 근사 → OPS+ 근사 대용
bat['wOBA']=((0.69*bat['BB']+0.72*bat.get('HBP',0)+0.88*bat['1B']+1.27*bat['2B']+1.62*bat['3B']+2.10*bat['HR'])
             / bat['PA'].replace(0,np.nan))
lg=bat.groupby('year').agg(lg_wOBA=('wOBA','mean'), lg_OPS=('wOBA','mean')) # 자리 채움
bat=bat.merge(lg, on='year', how='left')
bat['OPS_plus_approx']=(bat['wOBA']/bat['lg_wOBA']*100).round(1)

# BABIP, BB/K
bat['BABIP']=((bat['H']-bat['HR'])/(bat['AB']-bat['SO']-bat['HR']-bat['SF']).replace(0,np.nan))
bat['BBK']=(bat['BB']/bat['SO'].replace(0,np.nan))

# Statcast features(2015+) — EV/Whiff
sc = OUT/'statcast_features_player_year.csv'
scdf = pd.read_csv(sc, low_memory=False) if sc.exists() else pd.DataFrame(columns=['year','mlbam','avg_ev','whiff_rate'])
scdf = scdf[['year','mlbam','avg_ev','whiff_rate']].drop_duplicates()

# Chadwick ID 매핑(가능한 범위에서)
try:
    ch=list((ROOT/'data').rglob('Chadwick*'))[0]
    idx=pd.read_csv(ch, low_memory=False)
    idx=idx.rename(columns={'bbrefID':'playerID','mlbam':'mlbam'})
    idx['playerID']=idx['playerID'].astype(str)
    scdf=scdf.merge(idx[['playerID','mlbam']].dropna(), on='mlbam', how='left')
except Exception:
    scdf['playerID']=np.nan

# 트렌드(3년 롤링)
bat['playerID']=bat['playerID'].astype(str)
trend=(bat[['year','playerID','BABIP','BBK','OPS_plus_approx']]
          .sort_values(['playerID','year'])
          .groupby('playerID', as_index=False)
          .apply(lambda d: d.assign(BABIP_3yr=d['BABIP'].rolling(3,min_periods=1).mean(),
                                    BBK_3yr=d['BBK'].rolling(3,min_periods=1).mean(),
                                    OPSp_3yr=d['OPS_plus_approx'].rolling(3,min_periods=1).mean()))
          .reset_index(drop=True))

# EV/Whiff 합치기(가능한 범위)
scdf=scdf.dropna(subset=['playerID'])
t=trend.merge(scdf[['year','playerID','avg_ev','whiff_rate']], on=['year','playerID'], how='left')
t.to_csv(OUT/'trend_3yr.csv', index=False)

# 비교용 z-score(최근 시즌 기준): 2–3인 레이더 차트 소스
yr=int(t['year'].max())
latest=t[t['year']==yr][['playerID','BABIP','BBK','OPS_plus_approx','avg_ev','whiff_rate']].copy()
for c in ['BABIP','BBK','OPS_plus_approx','avg_ev','whiff_rate']:
    mu,sd=latest[c].mean(), latest[c].std(ddof=0)
    latest[c+'_z']=(latest[c]-mu)/sd if sd and sd>0 else 0
latest.to_csv(OUT/'player_compare_rows.csv', index=False)
print(f"[DAY70] {OUT/'trend_3yr.csv'} rows={len(t)}; {OUT/'player_compare_rows.csv'} rows={len(latest)}")
