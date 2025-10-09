import pandas as pd, numpy as np
from pathlib import Path
from _util_safe import zscore
ROOT=Path.cwd(); OUT=ROOT/'output'
feat = pd.read_csv(OUT/'statcast_features_player_year.csv', low_memory=False)
yr   = int(pd.to_numeric(feat['year'], errors='coerce').max())
df   = feat[feat['year']==yr].copy()
if 'role' not in df.columns: df['role']='bat'
for c in ['xwOBA','hardhit_rate','barrel_rate','whiff_rate','z_csw_rate','avg_spin','avg_ext','z_whiff_rate','chase_rate']:
    if c not in df: df[c]=np.nan
is_bat=df['role'].eq('bat'); is_pit=df['role'].eq('pit')
df.loc[is_bat, 'perf'] = ( zscore(df.loc[is_bat,'xwOBA'])
                         + 0.5*zscore(df.loc[is_bat,'hardhit_rate'])
                         + 0.3*zscore(df.loc[is_bat,'barrel_rate'])
                         - 0.2*zscore(df.loc[is_bat,'whiff_rate']) )
df.loc[is_pit, 'perf'] = ( zscore(df.loc[is_pit,'z_csw_rate'])
                         + 0.5*zscore(df.loc[is_pit,'z_whiff_rate'])
                         + 0.2*zscore(df.loc[is_pit,'avg_spin'])
                         + 0.1*zscore(df.loc[is_pit,'avg_ext']) )
df['perf']=pd.to_numeric(df['perf'], errors='coerce').fillna(0)

# $/WAR 추정 (trade_value에서 robust 샘플)
perWAR=9.5
tvp=OUT/'trade_value.csv'
if tvp.exists():
    tv=pd.read_csv(tvp, low_memory=False)
    if {'salaryMM','WAR'}.issubset(tv.columns):
        sal=pd.to_numeric(tv['salaryMM'],errors='coerce'); war=pd.to_numeric(tv['WAR'],errors='coerce')
        mask=(sal>0)&(war>0); m=(sal[mask]/war[mask]).median()
        if pd.notna(m): perWAR=float(m)
    elif {'salary','WAR'}.issubset(tv.columns):
        sal=pd.to_numeric(tv['salary'],errors='coerce')/1e6; war=pd.to_numeric(tv['WAR'],errors='coerce')
        mask=(sal>0)&(war>0); m=(sal[mask]/war[mask]).median()
        if pd.notna(m): perWAR=float(m)

pct = df['perf'].rank(pct=True)
war_est = np.select([pct>=0.90,pct>=0.70,pct>=0.40,pct>=0.20],[5.0,3.0,2.0,1.0], default=0.5)
AAV_mid = war_est * perWAR

full = df[['year','role','mlbam','player_name']].copy()
full['perf']=df['perf']; full['pct']=pct; full['war_est']=war_est
full['AAV_mid']=AAV_mid; full['AAV_low']=AAV_mid*0.8; full['AAV_high']=AAV_mid*1.25
full['years_guess']=np.select([pct>=0.90,pct>=0.70,pct>=0.40],[5,3,2], default=1)

# 선택적 FA 후보 필터
for name in ['free_agents.csv', f'free_agents_{yr}.csv']:
    p=Path('data')/name
    if p.exists():
        fa_hint = pd.read_csv(p, low_memory=False)
        if 'mlbam' in fa_hint:
            full = full[full['mlbam'].isin(pd.to_numeric(fa_hint['mlbam'], errors='coerce'))]
        elif 'player_name' in fa_hint:
            full = full[full['player_name'].isin(fa_hint['player_name'])]
        break

# 역할별 Top 200만 요약본 저장, 전체는 _full.csv
top = pd.concat([
    full[full['role']=='bat'].sort_values('AAV_mid', ascending=False).head(200),
    full[full['role']=='pit'].sort_values('AAV_mid', ascending=False).head(200)
]).sort_values(['role','AAV_mid'], ascending=[True,False])

full.to_csv(OUT/'fa_market_mvp_full.csv', index=False)
top.to_csv(OUT/'fa_market_mvp.csv', index=False)
print("[OK] fa_market_mvp.csv", len(top), "| fa_market_mvp_full.csv", len(full))
