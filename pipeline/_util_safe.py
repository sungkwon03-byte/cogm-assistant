import pandas as pd, numpy as np
def to_num(s): return pd.to_numeric(s, errors='coerce')
def wmean(x,w):
    x=to_num(x); w=to_num(w)
    den=float(w.fillna(0).sum())
    return float((x.fillna(0)*w.fillna(0)).sum()/(den if den else 1.0))
def zscore(s):
    s=to_num(s); m=float(s.mean()); sd=float(s.std() or 1.0)
    return (s-m)/sd
