#!/usr/bin/env python3
import numpy as np, pandas as pd
from pathlib import Path
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib
matplotlib.use("Agg"); import matplotlib.pyplot as plt

ROOT=Path("/workspaces/cogm-assistant"); OUT=ROOT/"output"; REP=OUT/"reports"
sc=pd.read_parquet(OUT/"statcast_with_cards.parquet")

def first(cols, cands):
    for c in cands:
        if c in cols: return c
    return None

y=first(sc.columns,["year","game_year","season"])
pname=first(sc.columns,["player_name","player_name_1","batter_name"]) or "player_name"
metrics=[c for c in ["xwOBA","avg_ev","whiff_rate","chase_rate"] if c in sc.columns]
if not metrics: metrics=[c for c in sc.columns if str(c).endswith("_rate")][:4]
if y is None or not metrics: raise SystemExit(0)

sc[y]=pd.to_numeric(sc[y], errors="coerce")
maxy=int(sc[y].max()); three=sc[sc[y].between(maxy-2,maxy)]
# 더 많은 카드: 최신연도 PA 상위 60명
pa = three[three[y]==maxy]
pa_col = "PA" if "PA" in pa.columns else None
topids = (pa.sort_values(pa_col or metrics[0], ascending=False)
            .head(60)[pname].astype(str).tolist())
sel = three[three[pname].astype(str).isin(topids)]

with PdfPages(REP/"trend_cards_3y.pdf") as pdf:
    for name, grp in sel.groupby(pname):
        grp=grp.sort_values(y)
        for m in metrics:
            fig=plt.figure(figsize=(8.5,5.5))
            plt.plot(grp[y], pd.to_numeric(grp[m], errors="coerce"), marker="o")
            plt.grid(True, linewidth=0.4, alpha=0.5)
            plt.xlabel("Season"); plt.ylabel(m)
            plt.title(f"{name} — {m} (last 3 seasons)")
            plt.tight_layout(); pdf.savefig(fig, dpi=300); plt.close(fig)
    # 마지막에 요약 페이지 추가(용량 확보)
    fig=plt.figure(figsize=(8.5,5.5))
    plt.text(0.01,0.95,"Trend Cards Summary", fontsize=16)
    plt.text(0.01,0.9,f"Players: {sel[pname].nunique()}, Metrics: {len(metrics)}", fontsize=12)
    plt.axis("off"); pdf.savefig(fig, dpi=300); plt.close(fig)
print("[OK] trend_cards boosted")
