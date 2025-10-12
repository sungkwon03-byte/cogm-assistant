#!/usr/bin/env python3
# 실데이터 계산 성공 시에만 기존 산출물 교체(플레이스홀더 덮어쓰기)
import json, math
from pathlib import Path
import pandas as pd, numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

ROOT=Path("/workspaces/cogm-assistant"); OUT=ROOT/"output"; REP=OUT/"reports"; SUM=OUT/"summaries"; LOG=ROOT/"logs"
for p in (OUT,REP,SUM,LOG): p.mkdir(parents=True, exist_ok=True)
SC=OUT/"statcast_ultra_full_clean.parquet"
CARDS=OUT/"player_cards_all.parquet"
CARDS_EN=OUT/"player_cards_enriched_all_seq.parquet"

def ok(p): return Path(p).exists()
def swap(tmp,final,min_bytes=200):
    # 계산물이 충분히 크고(=실데이터 집계) 비어있지 않으면 교체
    if tmp.exists() and tmp.stat().st_size >= min_bytes:
        tmp.replace(final); return True
    tmp.unlink(missing_ok=True); return False

status={"platoon":False,"weakness":False,"trend_cards":False,"euz":False,"explainable":False}

# 1) 플래툰
try:
    if ok(SC):
        df=pd.read_parquet(SC)
        hb="batter_hand" if "batter_hand" in df.columns else ("stand" if "stand" in df.columns else None)
        hp="pitcher_throws" if "pitcher_throws" in df.columns else None
        y="year" if "year" in df.columns else ("game_year" if "game_year" in df.columns else None)
        if hb and hp and (y or "game_date" in df.columns):
            if not y:
                df["year"]=pd.to_datetime(df["game_date"], errors="coerce").dt.year; y="year"
            wv,wd="woba_value","woba_denom"
            if not {wv,wd}.issubset(df.columns):
                if "estimated_woba_using_speedangle" in df.columns:
                    df[wv]=pd.to_numeric(df["estimated_woba_using_speedangle"], errors="coerce").fillna(0); df[wd]=1.0
                elif "launch_speed" in df.columns:
                    ev=pd.to_numeric(df["launch_speed"], errors="coerce").fillna(0)
                    df[wv]=(ev-ev.mean())/(ev.std(ddof=0) or 1.0); df[wd]=1.0
                else:
                    raise RuntimeError("no wOBA proxy")
            grp=df.groupby([y,hb,hp], dropna=True).agg(PA=("batter","count"), wv=(wv,"sum"), wd=(wd,"sum")).reset_index()
            grp["wOBA"]=grp["wv"]/grp["wd"].replace(0,np.nan)
            tmp_csv=SUM/"__tmp_platoon_split.csv"; grp.to_csv(tmp_csv, index=False)
            # 시각화
            agg=grp.groupby([hp,hb])["wOBA"].mean(numeric_only=True).reset_index()
            fig=plt.figure(figsize=(8,5)); x=np.arange(len(agg)); plt.bar(x, agg["wOBA"].fillna(0))
            plt.xticks(x, agg[hp].astype(str)+"/"+agg[hb].astype(str)); plt.title("Platoon split (avg wOBA)"); plt.tight_layout()
            tmp_png=REP/"__tmp_platoon_map.png"; plt.savefig(tmp_png, dpi=180); plt.close(fig)
            a=swap(tmp_csv, SUM/"platoon_split.csv", min_bytes=200)
            b=swap(tmp_png, REP/"platoon_map.png", min_bytes=2000)
            status["platoon"]=a and b
except Exception: pass

# 2) 약점 히트맵
try:
    if ok(SC):
        df=pd.read_parquet(SC)
        need={"pitch_type","plate_x","plate_z"}
        if need.issubset(df.columns):
            m=df[["pitch_type","plate_x","plate_z"]].dropna().copy()
            m["xb"]=pd.cut(pd.to_numeric(m["plate_x"], errors="coerce"),5,labels=False)
            m["zb"]=pd.cut(pd.to_numeric(m["plate_z"], errors="coerce"),5,labels=False)
            m["zone"]=m["xb"].astype(str)+"x"+m["zb"].astype(str)
            mat=m.groupby(["pitch_type","zone"]).size().reset_index(name="n")
            tmp_csv=SUM/"__tmp_weakness_heatmap_matrix.csv"; mat.to_csv(tmp_csv, index=False)
            grid=np.zeros((5,5))
            for _,r in mat.iterrows():
                try:
                    a,b=map(int,str(r["zone"]).split("x")); 
                    if 0<=a<5 and 0<=b<5: grid[b,a]+=r["n"]
                except: pass
            fig=plt.figure(figsize=(6,5)); plt.imshow(grid, origin="lower"); plt.colorbar(); plt.title("Weakness heatmap"); plt.tight_layout()
            tmp_png=REP/"__tmp_weakness_heatmap.png"; plt.savefig(tmp_png, dpi=180); plt.close(fig)
            a=swap(tmp_csv, SUM/"weakness_heatmap_matrix.csv", min_bytes=400)
            b=swap(tmp_png, REP/"weakness_heatmap.png", min_bytes=2000)
            status["weakness"]=a and b
except Exception: pass

# 3) 3년 트렌드 카드
try:
    base=None
    if ok(CARDS_EN): base=pd.read_parquet(CARDS_EN)
    elif ok(CARDS): base=pd.read_parquet(CARDS)
    if base is not None:
        need=["player_id","player_name","season","wRC_plus","BABIP","EV","BB","K","PA"]
        for c in need:
            if c not in base.columns: base[c]=np.nan
        base["season"]=pd.to_numeric(base["season"], errors="coerce")
        max_y=int(base["season"].max())
        tdf=base[base["season"].between(max_y-2, max_y)].copy()
        pa_rank=tdf[tdf["season"]==max_y].sort_values("PA", ascending=False).head(24)["player_id"].tolist()
        sel=tdf[tdf["player_id"].isin(pa_rank)]
        tmp_pdf=REP/"__tmp_trend_cards_3y.pdf"
        with PdfPages(tmp_pdf) as pdf:
            for pid, grp in sel.groupby("player_id"):
                name=grp["player_name"].dropna().astype(str).unique()
                name=name[0] if len(name) else str(pid)
                grp=grp.sort_values("season")
                for metric in ["wRC_plus","BABIP","EV"]:
                    fig=plt.figure(figsize=(7.5,4.5))
                    plt.plot(grp["season"], pd.to_numeric(grp[metric], errors="coerce"))
                    plt.title(f"{name} - {metric} (3y)"); plt.tight_layout(); pdf.savefig(fig); plt.close(fig)
                bb=pd.to_numeric(grp["BB"], errors="coerce"); kk=pd.to_numeric(grp["K"], errors="coerce").replace(0,np.nan)
                fig=plt.figure(figsize=(7.5,4.5)); plt.plot(grp["season"], (bb/kk)); plt.title(f"{name} - BB/K (3y)")
                plt.tight_layout(); pdf.savefig(fig); plt.close(fig)
        status["trend_cards"]=swap(tmp_pdf, REP/"trend_cards_3y.pdf", min_bytes=6000)
except Exception: pass

# 4) EUZ
try:
    if ok(SC):
        df=pd.read_parquet(SC)
        if "home_plate_umpire" in df.columns and (("description" in df.columns) or ("type" in df.columns)):
            take_idx=pd.Series(False,index=df.index)
            if "description" in df.columns: take_idx|=df["description"].astype(str).str.lower().isin(["called_strike","ball"])
            if "type" in df.columns: take_idx|=df["type"].astype(str).str.lower().isin(["s","b"])
            t=df[take_idx].copy()
            if len(t)>0:
                iscs=pd.Series(False,index=t.index)
                if "description" in t.columns: iscs|=t["description"].astype(str).str.lower().eq("called_strike")
                if "type" in t.columns: iscs|=t["type"].astype(str).str.lower().eq("s")
                t["is_cs"]=iscs
                by=t.groupby("home_plate_umpire")["is_cs"].mean().reset_index()
                tmp_csv=SUM/"__tmp_euz_umpire_impact.csv"; by.to_csv(tmp_csv, index=False)
                vals=by["is_cs"].sort_values().values
                fig=plt.figure(figsize=(8,6)); plt.plot(vals); plt.title("Umpire EUZ Δ called strike%"); plt.tight_layout()
                tmp_png=REP/"__tmp_ump_euz.png"; plt.savefig(tmp_png, dpi=180); plt.close(fig)
                a=swap(tmp_csv, SUM/"euz_umpire_impact.csv", min_bytes=400)
                b=swap(tmp_png, REP/"ump_euz.png", min_bytes=2000)
                status["euz"]=a and b
except Exception: pass

# 5) Explainable (있을 때만 교체)
try:
    expl=SUM/"explainable_attribution.csv"
    if ok(expl):
        df=pd.read_csv(expl)
        if not df.empty:
            score_cols=[c for c in df.columns if c.lower().endswith("3")]
            if score_cols:
                s=df[score_cols].select_dtypes("number").sum(axis=1)
                top=df.assign(_s=s).sort_values("_s", ascending=False).head(10)
                fig=plt.figure(figsize=(10,4)); plt.bar(np.arange(len(top)), top["_s"].fillna(0))
                plt.xticks(np.arange(len(top)), top.get("player_name", pd.Series(["NA"]*len(top))).astype(str).str[:12], rotation=45, ha="right")
                plt.tight_layout()
                tmp_png=REP/"__tmp_explainable_attribution_topN.png"; plt.savefig(tmp_png, dpi=180); plt.close(fig)
                status["explainable"]=swap(tmp_png, REP/"explainable_attribution_topN.png", min_bytes=3000)
except Exception: pass

(SUM/"visuals_final_status.json").write_text(json.dumps(status, indent=2))
print(json.dumps(status, indent=2))
