import pandas as pd, numpy as np, matplotlib.pyplot as plt
from pathlib import Path
ROOT=Path.cwd(); OUT=ROOT/'output'
t=pd.read_csv(OUT/'trend_3yr.csv', low_memory=False)
yr=int(pd.to_numeric(t['year'], errors='coerce').max())
pri=['wRCplus','OPS','xwOBA','EV','BABIP']
met = next((c for c in pri if c in t.columns), None)
if met is None:
    print("[SKIP] no suitable metric in trend_3yr.csv"); raise SystemExit
cands=(t[t['year']==yr].groupby('playerID')[met].mean()
       .sort_values(ascending=False).head(4).index.tolist())
pdf=OUT/'trend_3yr_cards.pdf'
plt.rcParams['figure.figsize']=(8,4)
with plt.rc_context({}):
    from matplotlib.backends.backend_pdf import PdfPages
    with PdfPages(pdf) as pp:
        for pid in cands:
            d=t[t['playerID']==pid].sort_values('year')
            plt.figure(); 
            plt.plot(d['year'], d[met], marker='o'); 
            plt.title(f"{pid} — 3yr {met}"); plt.xlabel('year'); plt.ylabel(met)
            pp.savefig(); plt.close()
print("[OK] trend_3yr_cards.pdf")
