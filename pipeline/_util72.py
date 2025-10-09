import pandas as pd, numpy as np
from pathlib import Path
ROOT=Path.cwd(); OUT=ROOT/'output'; OUT.mkdir(exist_ok=True); DATA=ROOT/'data'
def ffind(name):
    c=list((DATA/'lahman_extracted').rglob(name)) or list(DATA.rglob(name))
    if not c: raise FileNotFoundError(name)
    c.sort(key=lambda p:p.stat().st_size, reverse=True); return c[0]
def to_num(x): return pd.to_numeric(x, errors='coerce')
def safe_div(a,b): a=to_num(a); b=to_num(b); r=a/b; return r.replace([np.inf,-np.inf],np.nan)
def wmean(x,w):
    x=to_num(x); w=to_num(w); d=w.fillna(0).sum()
    return float((x.fillna(0)*w.fillna(0)).sum()/(d if d else np.nan))
