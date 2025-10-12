#!/usr/bin/env python3
import numpy as np, pandas as pd
from pathlib import Path
ROOT=Path("/workspaces/cogm-assistant"); OUT=ROOT/"output"; REP=OUT/"reports"; SUM=OUT/"summaries"
scp=OUT/"statcast_with_cards.parquet"
if not scp.exists(): raise FileNotFoundError(scp)
sc=pd.read_parquet(scp)

def first(cols, cands):
    for c in cands:
        if c in cols: return c
    return None

y=first(sc.columns,["year","game_year","season"])
name=first(sc.columns,["player_name","player_name_1","batter_name"]) or "player_name"
# 준비 지표
m_cols=[]
if "xwOBA" in sc.columns: m_cols.append("xwOBA")
if "avg_ev" in sc.columns: m_cols.append("avg_ev")
if "EV"    in sc.columns:  m_cols.append("EV")
if "BB" in sc.columns and "K" in sc.columns:
    sc["bbk"]=pd.to_numeric(sc["BB"],errors="coerce")/pd.to_numeric(sc["K"],errors="coerce").replace(0,np.nan)
    m_cols.append("bbk")
elif "whiff_rate" in sc.columns:
    sc["bbk"]=1.0/(pd.to_numeric(sc["whiff_rate"],errors="coerce")+1e-6)
    m_cols.append("bbk")

if y is None or not m_cols:
    raise SystemExit(0)  # 만들 조건 없으면 조용히 종료

sc[y]=pd.to_numeric(sc[y], errors="coerce")
maxy=int(sc[y].max())
win=sc[sc[y].between(maxy-2,maxy)].copy()

# 선수-연도 집계
agg=win.groupby([name,y])[m_cols].mean(numeric_only=True).reset_index()
# 3년 표준화 후 합산
def z(s): return (s-s.mean())/s.std(ddof=0) if s.std(ddof=0) not in (0,np.nan,None) else s*0
zcols=[]
for c in m_cols:
    zz=agg.groupby(name)[c].transform(z)
    agg[c+"_z"]=zz
    zcols.append(c+"_z")

agg["score"]=agg[zcols].sum(axis=1)
top=(agg.groupby(name)["score"].sum().sort_values(ascending=False).head(10)).reset_index().rename(columns={name:"player_name","score":"score3"})
top.to_csv(SUM/"explainable_attribution.csv", index=False)

# PNG 생성
import matplotlib
matplotlib.use("Agg"); import matplotlib.pyplot as plt; import numpy as np
fig=plt.figure(figsize=(10,4))
x=np.arange(len(top)); plt.bar(x, top["score3"])
plt.xticks(x, top["player_name"].astype(str).str[:12], rotation=45, ha="right")
plt.title("Explainable attribution (3y composite from Statcast)")
plt.tight_layout(); plt.savefig(REP/"explainable_attribution_topN.png", dpi=240); plt.close(fig)
print("[OK] explainable fallback generated")
