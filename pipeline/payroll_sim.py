import pandas as pd, numpy as np
from pathlib import Path
ROOT=Path.cwd(); OUT=ROOT/'output'; OUT.mkdir(exist_ok=True); LOG=ROOT/'logs'; LOG.mkdir(exist_ok=True)

def _ff(name):
    cand=list((ROOT/'data'/'lahman_extracted').rglob(name)) or list((ROOT/'data').rglob(name))
    if not cand: raise FileNotFoundError(name)
    cand.sort(key=lambda p:p.stat().st_size, reverse=True); return cand[0]

sal = pd.read_csv(_ff('Salaries.csv'), low_memory=False)

cbtfile = next((p for p in [ROOT/'data'/'cbt_thresholds.csv', ROOT/'data'/'cbt_thresholds.json'] if p.exists()), None)
if cbtfile is None:
    cbt = pd.DataFrame({'year': list(range(2003,2026)),
                        'threshold':[117e6,120e6,128e6,136e6,148e6,155e6,162e6,170e6,178e6,178e6,189e6,189e6,189e6,189e6,195e6,206e6,208e6,210e6,210e6,230e6,233e6,237e6,241e6]})
else:
    if str(cbtfile).endswith('.csv'):
        cbt=pd.read_csv(cbtfile)
    else:
        import json
        cbt=pd.DataFrame(json.loads(Path(cbtfile).read_text()))
    cbt.rename(columns={'yearID':'year'}, inplace=True)

pay = sal.groupby(['yearID','teamID'], as_index=False)['salary'].sum().rename(columns={'yearID':'year'})
df  = pay.merge(cbt, on='year', how='left')

s = (df['salary'] - df['threshold'])
df['over_amount'] = s.where(df['threshold'].notna(), 0.0).clip(lower=0.0)

def tax_calc(row):
    if pd.isna(row['threshold']) or row['over_amount'] <= 0: return 0.0
    # 단순 고정세율(예시): 20% — 필요 시 실제 규칙으로 교체 가능
    return 0.20 * row['over_amount']

df['cbt_tax']  = df.apply(tax_calc, axis=1)
df['cbt_flag'] = (df['over_amount'] > 0).astype(int)

out = ROOT/'output'/'payroll_sim.csv'
df[['year','teamID','salary','threshold','over_amount','cbt_tax','cbt_flag']].to_csv(out, index=False)
print("[DAY65] output/payroll_sim.csv rows=", len(df))
