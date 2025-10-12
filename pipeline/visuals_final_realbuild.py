#!/usr/bin/env python3
# Real-data visual build (schema-adaptive, name+season join)
import json, unicodedata
from pathlib import Path
import pandas as pd, numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

ROOT=Path("/workspaces/cogm-assistant")
OUT, REP, SUM, LOG = ROOT/"output", ROOT/"reports", ROOT/"summaries", ROOT/"logs"
for p in (OUT,REP,SUM,LOG): p.mkdir(parents=True, exist_ok=True)

SC    = OUT/"statcast_with_cards.parquet"
CARDS = OUT/"player_cards_enriched_all_seq.parquet"
if not CARDS.exists(): CARDS = OUT/"player_cards_all.parquet"
EXPL  = SUM/"explainable_attribution.csv"
ZONE  = SUM/"zone_repeat_transition.parquet"

def first(cols, cands):
    for c in cands:
        if c in cols: return c
    return None

def need(p:Path, msg:str):
    if not p.exists(): raise FileNotFoundError(f"[MISS] {p} :: {msg}")

def norm(s: pd.Series) -> pd.Series:
    def f(x):
        x = str(x or "").strip()
        x = unicodedata.normalize("NFKD", x)
        x = "".join(ch for ch in x if not unicodedata.combining(ch))
        x = x.replace(".", "").replace("'", "").replace("-", " ")
        x = " ".join(x.replace(",", " ").split()).lower()
        return x
    return s.map(f)

def to_key(s: pd.Series) -> pd.Series:
    # "john michael smith" -> "j smith"
    ss = norm(s)
    def k(x):
        parts = x.split()
        if not parts: return ""
        return (parts[0][0] + " " + parts[-1]) if len(parts)>=2 else parts[0]
    return ss.map(k)

# ---------- load ----------
need(SC, "Statcast 2015–2025")
need(CARDS, "Cards 1901–2014+")
sc = pd.read_parquet(SC)
cards = pd.read_parquet(CARDS)

# ---------- columns ----------
ycol = first(sc.columns, ["year","game_year","season","game_season","gameYear","game_date"])
if ycol is None: raise ValueError(f"[SCHEMA] no year-like column in statcast; have={list(sc.columns)[:40]}")
if ycol == "game_date":
    sc["year"] = pd.to_datetime(sc["game_date"], errors="coerce").dt.year
    ycol = "year"

bat_name_sc = first(sc.columns, ["batter_name","player_name","hitter_name","name"])
pit_name_sc = first(sc.columns, ["pitcher_name","player_name_1","p_name","name_1"])
pt_col      = first(sc.columns, ["pitch_type","pitchType","pitch_name"]) or "pitch_type"
px_col      = first(sc.columns, ["plate_x","px","plateX","pfx_x"])
pz_col      = first(sc.columns, ["plate_z","pz","plateZ","pfx_z"])
woba_cols   = {"woba_value","woba_denom"}
have_woba   = woba_cols.issubset(sc.columns)
have_xwoba  = "xwOBA" in sc.columns
have_events = "events" in sc.columns

if bat_name_sc is None:
    raise ValueError(f"[SCHEMA] cannot find batter name column in statcast; have={list(sc.columns)[:40]}")

# ---------- cards bats/throws maps ----------
name_cards = first(cards.columns, ["name","player_name","name_norm"])
if name_cards is None: raise ValueError("[SCHEMA] cards missing name column")
cards["name_norm"] = norm(cards[name_cards])
bats_c   = first(cards.columns, ["bats","bat_side","stand"])
throws_c = first(cards.columns, ["throws","throw_hand","p_throws"])

def majority_map(df, val):
    g=(df.dropna(subset=["name_norm",val])
         .groupby("name_norm")[val]
         .agg(lambda s: s.value_counts(dropna=True).idxmax())
         .reset_index())
    g["key"]=to_key(g["name_norm"])
    return g[["name_norm","key",val]]

bats_map   = majority_map(cards, bats_c)   if bats_c   else None
throws_map = majority_map(cards, throws_c) if throws_c else None

# ---------- 1) PLATOON ----------
tmp = pd.DataFrame(index=sc.index)
tmp[ycol] = sc[ycol].values
tmp["b_name_norm"] = norm(sc[bat_name_sc]); tmp["b_key"]=to_key(sc[bat_name_sc])

if bats_map is not None:
    tmp = tmp.merge(bats_map[["name_norm","key",bats_c]].rename(columns={bats_c:"bats"}),
                    left_on="b_name_norm", right_on="name_norm", how="left")
    tmp.loc[tmp["bats"].isna(),"bats"] = tmp.loc[tmp["bats"].isna()] \
        .merge(bats_map[["key",bats_c]].rename(columns={bats_c:"bats"}),
               left_on="b_key", right_on="key", how="left")["bats"].values

if pit_name_sc is not None and throws_map is not None:
    tmp["p_name_norm"]=norm(sc[pit_name_sc]); tmp["p_key"]=to_key(sc[pit_name_sc])
    tmp = tmp.merge(throws_map[["name_norm","key",throws_c]].rename(columns={throws_c:"throws"}),
                    left_on="p_name_norm", right_on="name_norm", how="left")
    fill = tmp.loc[tmp["throws"].isna()] \
        .merge(throws_map[["key",throws_c]].rename(columns={throws_c:"throws"}),
               left_on="p_key", right_on="key", how="left")["throws"].values
    tmp.loc[tmp["throws"].isna(),"throws"] = fill

# coverage report
cov = {"bats": float(tmp["bats"].notna().mean()) if "bats" in tmp.columns else 0.0,
       "throws": float(tmp["throws"].notna().mean()) if "throws" in tmp.columns else 0.0}
print(json.dumps({"coverage": cov}, indent=2))

# wOBA proxy
if have_woba:
    wv,wd="woba_value","woba_denom"
    tmp["wv"]=sc[wv].values; tmp["wd"]=sc[wd].values
elif have_xwoba:
    tmp["wv"]=pd.to_numeric(sc["xwOBA"], errors="coerce").fillna(0.0); tmp["wd"]=1.0
elif have_events:
    tmp["wv"]=sc["events"].astype(str).isin(["home_run","double","triple","single"]).astype(float); tmp["wd"]=1.0
else:
    raise ValueError("[SCHEMA] no wOBA proxy (need woba_value/woba_denom or xwOBA or events)")

tmp["bats"]=tmp.get("bats", pd.Series(index=tmp.index)).astype(str).str.upper().str[0]
tmp["throws"]=tmp.get("throws", pd.Series(index=tmp.index)).astype(str).str.upper().str[0]
pl = tmp.dropna(subset=["bats","throws"]).copy()
pl = (pl.groupby([ycol,"bats","throws"], dropna=True)
        .agg(PA=("bats","size"), wv=("wv","sum"), wd=("wd","sum"))
        .reset_index())
pl["wOBA"]=pl["wv"]/pl["wd"].replace(0,np.nan)
pl.to_csv(SUM/"platoon_split.csv", index=False)

fig=plt.figure(figsize=(8,5))
agg = pl.groupby(["throws","bats"])["wOBA"].mean(numeric_only=True).reset_index()
x=np.arange(len(agg)); plt.bar(x, agg["wOBA"].fillna(0))
plt.xticks(x, agg["throws"].astype(str)+"/"+agg["bats"].astype(str))
plt.title("Platoon split (avg wOBA)"); plt.tight_layout()
plt.savefig(REP/"platoon_map.png", dpi=220); plt.close(fig)

# ---------- 2) WEAKNESS ----------
if ZONE.exists():
    mat=pd.read_parquet(ZONE)
    zc = first(mat.columns, ["zone","zone_bin","pitch_zone"])
    pt = first(mat.columns, ["pitch_type","pitchType","pitch_name"]) or "pitch_type"
    if zc is None: raise ValueError("[SCHEMA] zone parquet missing zone*")
    matrix = mat.groupby([pt,zc]).size().reset_index(name="n").rename(columns={pt:"pitch_type",zc:"zone"})
else:
    if px_col is None or pz_col is None: raise ValueError("[SCHEMA] need plate_x/z or pfx_x/z")
    df=sc[[pt_col,px_col,pz_col]].dropna().copy()
    df["xb"]=pd.cut(pd.to_numeric(df[px_col], errors="coerce"),5,labels=False)
    df["zb"]=pd.cut(pd.to_numeric(df[pz_col], errors="coerce"),5,labels=False)
    df["zone"]=df["xb"].astype(str)+"x"+df["zb"].astype(str)
    matrix=df.groupby([pt_col,"zone"]).size().reset_index(name="n").rename(columns={pt_col:"pitch_type"})
matrix.to_csv(SUM/"weakness_heatmap_matrix.csv", index=False)

top = matrix.groupby("pitch_type")["n"].sum().sort_values(ascending=False).index[0]
sel = matrix[matrix["pitch_type"]==top].copy()
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

# ---------- 3) TREND CARDS (실데이터 기반 대체 메트릭: xwOBA, avg_ev, whiff_rate, chase_rate) ----------
metrics=[]
if "xwOBA" in sc.columns: metrics.append("xwOBA")
if "avg_ev" in sc.columns: metrics.append("avg_ev")
if "whiff_rate" in sc.columns: metrics.append("whiff_rate")
if "chase_rate" in sc.columns: metrics.append("chase_rate")
if not metrics: metrics=[c for c in sc.columns if c.endswith("_rate")][:3]

# 최근 3시즌, 상위 타자 24명(=PA 높은 순) 카드
sc_year = sc[[ycol, bat_name_sc, "PA"]+metrics].copy()
sc_year[ycol]=pd.to_numeric(sc_year[ycol], errors="coerce")
max_y = int(sc_year[ycol].max())
three = sc_year[sc_year[ycol].between(max_y-2, max_y)]
top_ids = (three[three[ycol]==max_y]
           .sort_values("PA", ascending=False)
           .head(24)[bat_name_sc].astype(str).tolist())
sel = three[three[bat_name_sc].astype(str).isin(top_ids)].copy()

with PdfPages(REP/"trend_cards_3y.pdf") as pdf:
    for name, grp in sel.groupby(bat_name_sc):
        grp = grp.sort_values(ycol)
        for m in metrics[:4]:
            fig=plt.figure(figsize=(7.5,4.5))
            plt.plot(grp[ycol], pd.to_numeric(grp[m], errors="coerce"))
            plt.title(f"{name} - {m} (3y)"); plt.tight_layout(); pdf.savefig(fig); plt.close(fig)

# ---------- 4) EUZ (심판/판정 없으면 스킵 기록) ----------
ump = first(sc.columns, ["home_plate_umpire","umpire","umpire_name"])
desc= first(sc.columns, ["description","call_description"]); typ=first(sc.columns, ["type","call"])
if ump and (desc or typ):
    take = pd.Series(False, index=sc.index)
    if desc: take |= sc[desc].astype(str).str.lower().isin(["called_strike","ball"])
    if typ:  take |= sc[typ].astype(str).str.lower().isin(["s","b"])
    t=sc[take].copy()
    if len(t)>0:
        iscs=pd.Series(False, index=t.index)
        if desc: iscs |= t[desc].astype(str).str.lower().eq("called_strike")
        if typ:  iscs |= t[typ].astype(str).str.lower().eq("s")
        t["is_cs"]=iscs
        by=t.groupby(ump)["is_cs"].mean().reset_index().rename(columns={ump:"home_plate_umpire"})
        by.to_csv(SUM/"euz_umpire_impact.csv", index=False)
        fig=plt.figure(figsize=(8,6)); plt.plot(by["is_cs"].sort_values().values); plt.axhline(float(t["is_cs"].mean()), linewidth=1)
        plt.title("Umpire EUZ Δ called strike%"); plt.tight_layout(); plt.savefig(REP/"ump_euz.png", dpi=220); plt.close(fig)
else:
    (SUM/"euz_umpire_impact.csv").write_text("reason,no_umpire_or_call_columns\n", encoding="utf-8")

# ---------- 5) Explainable ----------
if EXPL.exists():
    dfx=pd.read_csv(EXPL)
    if not dfx.empty:
        scols=[c for c in dfx.columns if c.lower().endswith("3")]
        if scols:
            s=dfx[scols].select_dtypes("number").sum(axis=1)
            top=dfx.assign(_s=s).sort_values("_s", ascending=False).head(10)
            fig=plt.figure(figsize=(10,4)); plt.bar(np.arange(len(top)), top["_s"].fillna(0))
            labels=top.get("player_name", pd.Series(["NA"]*len(top))).astype(str).str[:12]
            plt.xticks(np.arange(len(top)), labels, rotation=45, ha="right")
            plt.title("Explainable attribution (Top 10)")
            plt.tight_layout(); plt.savefig(REP/"explainable_attribution_topN.png", dpi=220); plt.close(fig)

print("[OK] real build (name+season)")
