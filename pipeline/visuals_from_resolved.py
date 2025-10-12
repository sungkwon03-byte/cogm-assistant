#!/usr/bin/env python3
import numpy as np
from pathlib import Path
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

ROOT=Path("/workspaces/cogm-assistant")
OUT, REP, SUM = ROOT/"output", ROOT/"reports", ROOT/"summaries"
for p in (OUT,REP,SUM): p.mkdir(parents=True, exist_ok=True)
SC = OUT/"statcast_with_cards.parquet"; df=pd.read_parquet(SC)

def first(cols, cands):
    for c in cands:
        if c in cols: return c
    return None

ycol = first(df.columns,["year","game_year","season"]) or "year"
if ycol=="game_date":
    df["year"]=pd.to_datetime(df["game_date"], errors="coerce").dt.year; ycol="year"

# wOBA proxy
if {"woba_value","woba_denom"}.issubset(df.columns): wv,wd="woba_value","woba_denom"
elif "xwOBA" in df.columns: df["woba_value"]=pd.to_numeric(df["xwOBA"],errors="coerce").fillna(0); df["woba_denom"]=1.0; wv,wd="woba_value","woba_denom"
elif "events" in df.columns: df["woba_value"]=df["events"].astype(str).isin(["home_run","double","triple","single"]).astype(float); df["woba_denom"]=1.0; wv,wd="woba_value","woba_denom"
else: raise ValueError("no wOBA proxy")

# 1) PLATOON
if not {"bats_resolved","throws_resolved"}.issubset(df.columns): raise ValueError("missing resolved hands")
tmp=df[[ycol,"bats_resolved","throws_resolved",wv,wd]].dropna(subset=["bats_resolved","throws_resolved"]).copy()
tmp["bats"]=tmp["bats_resolved"].astype(str).str.upper().str[0]
tmp["throws"]=tmp["throws_resolved"].astype(str).str.upper().str[0]
pl=(tmp.groupby([ycol,"bats","throws"]).agg(PA=("bats","size"), wv=(wv,"sum"), wd=(wd,"sum")).reset_index())
pl["wOBA"]=pl["wv"]/pl["wd"].replace(0,np.nan)
pl.to_csv(SUM/"platoon_split.csv", index=False)

fig=plt.figure(figsize=(8,5)); agg=pl.groupby(["throws","bats"])["wOBA"].mean(numeric_only=True).reset_index()
x=np.arange(len(agg)); plt.bar(x, agg["wOBA"].fillna(0)); plt.xticks(x, agg["throws"].astype(str)+"/"+agg["bats"].astype(str))
plt.title("Platoon split (avg wOBA)"); plt.tight_layout(); plt.savefig(REP/"platoon_map.png", dpi=220); plt.close(fig)

# 2) WEAKNESS
pt=first(df.columns,["pitch_type","pitchType","pitch_name"]) or "pitch_type"
px=first(df.columns,["plate_x","px","pfx_x"]); pz=first(df.columns,["plate_z","pz","pfx_z"])
if px and pz:
    d=df[[pt,px,pz]].dropna().copy()
    d["xb"]=pd.cut(pd.to_numeric(d[px], errors="coerce"),5,labels=False)
    d["zb"]=pd.cut(pd.to_numeric(d[pz], errors="coerce"),5,labels=False)
    d["zone"]=d["xb"].astype(str)+"x"+d["zb"].astype(str)
    mat=d.groupby([pt,"zone"]).size().reset_index(name="n").rename(columns={pt:"pitch_type"})
    mat.to_csv(SUM/"weakness_heatmap_matrix.csv", index=False)
    top=mat.groupby("pitch_type")["n"].sum().sort_values(ascending=False).index[0]
    sel=mat[mat["pitch_type"]==top].copy()
    def idx(s):
        try: a,b=s.split("x"); return int(a),int(b)
        except: return None
    sel=sel.assign(idx=sel["zone"].apply(idx)).dropna(subset=["idx"])
    grid=np.zeros((5,5))
    for _,r in sel.iterrows():
        a,b=r["idx"]; 
        if 0<=a<5 and 0<=b<5: grid[b,a]=r["n"]
    fig=plt.figure(figsize=(6,5)); plt.imshow(grid, origin="lower"); plt.title(f"Weakness heatmap ({top})"); plt.colorbar()
    plt.tight_layout(); plt.savefig(REP/"weakness_heatmap.png", dpi=220); plt.close(fig)

# 3) TREND CARDS
metrics=[c for c in ["xwOBA","avg_ev","whiff_rate","chase_rate"] if c in df.columns] or [c for c in df.columns if c.endswith("_rate")][:3]
pname=first(df.columns,["player_name","player_name_1","batter_name"]) or "player_name"
scy=df[[ycol,pname,"PA"]+metrics].copy(); scy[ycol]=pd.to_numeric(scy[ycol], errors="coerce")
maxy=int(scy[ycol].max()); three=scy[scy[ycol].between(maxy-2,maxy)]
topn=(three[three[ycol]==maxy].sort_values("PA", ascending=False).head(24)[pname].astype(str).tolist())
sel=three[three[pname].astype(str).isin(topn)]
with PdfPages(REP/"trend_cards_3y.pdf") as pdf:
    for name, grp in sel.groupby(pname):
        grp=grp.sort_values(ycol)
        for m in metrics[:4]:
            fig=plt.figure(figsize=(7.5,4.5)); plt.plot(grp[ycol], pd.to_numeric(grp[m], errors="coerce"))
            plt.title(f"{name} - {m} (3y)"); plt.tight_layout(); pdf.savefig(fig); plt.close(fig)

# 4) EUZ (있을 때만)
ump=first(df.columns,["home_plate_umpire","umpire","umpire_name"])
desc=first(df.columns,["description","call_description"]); typ=first(df.columns,["type","call"])
if ump and (desc or typ):
    take = pd.Series(False, index=df.index)
    if desc: take |= df[desc].astype(str).str.lower().isin(["called_strike","ball"])
    if typ:  take |= df[typ].astype(str).str.lower().isin(["s","b"])
    t=df[take].copy()
    if len(t)>0:
        iscs=pd.Series(False, index=t.index)
        if desc: iscs |= t[desc].astype(str).str.lower().eq("called_strike")
        if typ:  iscs |= t[typ].astype(str).str.lower().eq("s")
        t["is_cs"]=iscs
        by=t.groupby(ump)["is_cs"].mean().reset_index().rename(columns={ump:"home_plate_umpire"})
        by.to_csv(SUM/"euz_umpire_impact.csv", index=False)
        fig=plt.figure(figsize=(8,6)); plt.plot(by["is_cs"].sort_values().values); plt.axhline(float(t["is_cs"].mean()), linewidth=1)
        plt.title("Umpire EUZ Δ called strike%"); plt.tight_layout(); plt.savefig(REP/"ump_euz.png", dpi=220); plt.close(fig)

# 5) Explainable
EXPL=ROOT/"output/summaries/explainable_attribution.csv"
if EXPL.exists():
    dfx=pd.read_csv(EXPL)
    if not dfx.empty:
        scols=[c for c in dfx.columns if c.lower().endswith("3")]
        if scols:
            s=dfx[scols].select_dtypes("number").sum(axis=1)
            top=dfx.assign(_s=s).sort_values("_s", ascending=False).head(10)
            fig=plt.figure(figsize=(10,4)); plt.bar(np.arange(len(top)), top["_s"].fillna(0))
            labels=top.get("player_name", pd.Series(["NA"]*len(top))).astype(str).str[:12]
            plt.xticks(np.arange(len(top)), labels, rotation=45, ha="right"); plt.title("Explainable attribution (Top 10)")
            plt.tight_layout(); plt.savefig(REP/"explainable_attribution_topN.png", dpi=220); plt.close(fig)

print("[OK] visuals_from_resolved done")
