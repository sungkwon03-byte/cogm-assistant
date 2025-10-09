import pandas as pd, numpy as np, glob
from pathlib import Path
OUT=Path.cwd()/'output'; OUT.mkdir(exist_ok=True)
cand=glob.glob('data/**/*schedule*.csv', recursive=True)+glob.glob('data/**/*mlb_schedule*.csv', recursive=True)
if not cand:
    pd.DataFrame(columns=['date','team','opp','home','note']).to_csv(OUT/'schedule_analysis.csv', index=False)
    print("[72][WARN] no schedule csv -> schedule_analysis.csv (empty)"); raise SystemExit
df=pd.read_csv(sorted(cand)[-1], low_memory=False)
cols=[c for c in ['date','team','opp','home','game_number'] if c in df.columns]
df[cols].to_csv(OUT/'schedule_analysis.csv', index=False)
print("[72] schedule_analysis.csv rows=", len(df))
