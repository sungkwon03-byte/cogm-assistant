import os, pandas as pd, numpy as np
from pathlib import Path
OUT=Path.cwd()/'output'; OUT.mkdir(exist_ok=True)
base=Path('output/cache/statcast')
if not base.exists(): base=Path('output/cache/statcast_clean')
files=sorted(base.glob('*.csv'))
if not files:
    pd.DataFrame(columns=['year','csr_edge','csr_heart','euz_index']).to_csv(OUT/'ump_euz_indices.csv', index=False)
    print("[72][WARN] no statcast cache -> ump_euz_indices.csv (empty)"); raise SystemExit
keep=['game_year','zone','description']
it=[]
for f in files[:int(os.getenv('STATCAST_MAX_FILES','999999'))]:
    try: d=pd.read_csv(f, usecols=[c for c in keep if c], low_memory=False)
    except: continue
    d=d.rename(columns={'game_year':'year'})
    d=d[d['description'].isin(['called_strike','ball'])].copy()
    d['is_edge']=d['zone'].isin([2,3,4,6,7,8])
    d['is_heart']=d['zone'].eq(5)
    it.append(d[['year','description','is_edge','is_heart']])
if not it:
    pd.DataFrame(columns=['year','csr_edge','csr_heart','euz_index']).to_csv(OUT/'ump_euz_indices.csv', index=False); raise SystemExit
df=pd.concat(it, ignore_index=True)
def rate(mask):
    sub=df[mask]
    num=(sub['description']=='called_strike').sum()
    den=(sub['description'].isin(['called_strike','ball'])).sum()
    return (num/den) if den else np.nan
rows=[]
for y in sorted(df['year'].dropna().unique()):
    m=df['year']==y
    csr_e=rate(m & df['is_edge']); csr_h=rate(m & df['is_heart'])
    rows.append({'year':int(y),'csr_edge':csr_e,'csr_heart':csr_h,'euz_index':(csr_e-csr_h) if pd.notna(csr_e) and pd.notna(csr_h) else np.nan})
pd.DataFrame(rows).to_csv(OUT/'ump_euz_indices.csv', index=False)
print("[72] ump_euz_indices.csv rows=", len(rows))
