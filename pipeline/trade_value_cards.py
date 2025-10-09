import pandas as pd, numpy as np
from pathlib import Path
ROOT=Path.cwd(); OUT=ROOT/'output'
p=OUT/'trade_value.csv'
if not p.exists(): 
    print("[SKIP] trade_value.csv not found"); raise SystemExit
tv=pd.read_csv(p, low_memory=False)
def pick(*names, default=0.0):
    for n in names:
        if n in tv: return pd.to_numeric(tv[n], errors='coerce').fillna(0.0)
    return pd.Series(default, index=tv.index, dtype=float)
WAR   = pick('WAR','WARx','fWAR','bWAR')
salMM = pick('salaryMM','salary_mm','salary', default=0.0)/ (1e6 if tv.get('salary',None) is not None else 1.0)
age   = pick('age', default=27.0)
tv['TV_score']= (WAR/(salMM.replace(0, np.nan).fillna(0.000001))).clip(lower=0, upper=20) * (1.0/(1.0+(age-27.0).abs()).clip(lower=1))
cards = tv.copy()
keep = [c for c in ['year','teamID','playerID','player_name'] if c in cards.columns]
cards = cards[keep+['TV_score']].sort_values('TV_score', ascending=False).head(200)
cards.to_csv(OUT/'trade_value_cards.csv', index=False)
print("[OK] trade_value_cards.csv", len(cards))
