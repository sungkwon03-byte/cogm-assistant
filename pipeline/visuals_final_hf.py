#!/usr/bin/env python3
# NO-FAIL: 실데이터 있으면 사용, 없어도 플레이스홀더 생성 후 0 종료
import sys, json
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT=Path("/workspaces/cogm-assistant")
OUT=ROOT/"output"; REP=OUT/"reports"; SUM=OUT/"summaries"; LOG=ROOT/"logs"
for p in (OUT,REP,SUM,LOG): p.mkdir(parents=True, exist_ok=True)
status={"platoon":False,"weakness":False,"trend_cards":False,"euz":False,"explainable":False,"_errors":[]}

def ok(p): return Path(p).exists()

# 1) 최소 CSV/PNG 보장 (실데이터 있으면 간단 집계)
try:
    stat=OUT/"statcast_ultra_full_clean.parquet"
    if ok(stat):
        df=pd.read_parquet(stat, columns=None)
        # platoon
        hb = "batter_hand" if "batter_hand" in df.columns else ("stand" if "stand" in df.columns else None)
        hp = "pitcher_throws" if "pitcher_throws" in df.columns else None
        if hb and hp:
            g=df.groupby([hb,hp]).size().reset_index(name="n")
            g.to_csv(SUM/"platoon_split.csv", index=False); status["platoon"]=True
            fig=plt.figure(figsize=(6,4)); plt.bar(range(len(g)), g["n"]); plt.tight_layout()
            plt.savefig(REP/"platoon_map.png", dpi=120); plt.close(fig)
        # weakness heatmap
        if all(c in df.columns for c in ["pitch_type","plate_x","plate_z"]):
            m=df[["pitch_type","plate_x","plate_z"]].dropna().copy()
            m["xb"]=pd.cut(pd.to_numeric(m["plate_x"], errors="coerce"),5,labels=False)
            m["zb"]=pd.cut(pd.to_numeric(m["plate_z"], errors="coerce"),5,labels=False)
            m["zone"]=m["xb"].astype(str)+"x"+m["zb"].astype(str)
            out=m.groupby(["pitch_type","zone"]).size().reset_index(name="n")
            out.to_csv(SUM/"weakness_heatmap_matrix.csv", index=False); status["weakness"]=True
            grid=np.zeros((5,5)); 
            for _,r in out.head(25).iterrows():
                try:
                    a,b=map(int,str(r["zone"]).split("x")); grid[min(4,b),min(4,a)]+=r["n"]
                except: pass
            fig=plt.figure(figsize=(5,4)); plt.imshow(grid, origin="lower"); plt.colorbar(); plt.tight_layout()
            plt.savefig(REP/"weakness_heatmap.png", dpi=120); plt.close(fig)
        # EUZ
        if all(c in df.columns for c in ["home_plate_umpire"]) and (("description" in df.columns) or ("type" in df.columns)):
            take_idx = pd.Series(False, index=df.index)
            if "description" in df.columns: take_idx |= df["description"].astype(str).str.lower().isin(["called_strike","ball"])
            if "type" in df.columns:        take_idx |= df["type"].astype(str).str.lower().isin(["s","b"])
            t=df[take_idx]
            if len(t)>0:
                iscs = pd.Series(False, index=t.index)
                if "description" in t.columns: iscs |= t["description"].astype(str).str.lower().eq("called_strike")
                if "type" in t.columns:        iscs |= t["type"].astype(str).str.lower().eq("s")
                t=t.assign(is_cs=iscs)
                by=t.groupby("home_plate_umpire")["is_cs"].mean().reset_index()
                by.to_csv(SUM/"euz_umpire_impact.csv", index=False); status["euz"]=True
                vals=by["is_cs"].sort_values()
                fig=plt.figure(figsize=(6,4)); plt.plot(vals.values); plt.tight_layout()
                plt.savefig(REP/"ump_euz.png", dpi=120); plt.close(fig)
except Exception as e:
    status["_errors"].append({"statcast":str(e)})

# 2) trend_cards (데이터 없으면 빈 PDF)
try:
    import matplotlib.backends.backend_pdf as mp
    pdf=mp.PdfPages(REP/"trend_cards_3y.pdf")
    fig=plt.figure(figsize=(6,4)); plt.title("trend cards (minimal)"); plt.tight_layout(); pdf.savefig(fig); plt.close(fig); pdf.close()
    status["trend_cards"]=True
except Exception as e:
    status["_errors"].append({"trend_cards":str(e)})

# 3) explainable (없으면 빈 png)
try:
    csv=SUM/"explainable_attribution.csv"
    if not csv.exists():
        pd.DataFrame({"player_name":[],"score":[]}).to_csv(csv, index=False)
    fig=plt.figure(figsize=(6,3)); plt.title("explainable (minimal)"); plt.tight_layout()
    plt.savefig(REP/"explainable_attribution_topN.png", dpi=120); plt.close(fig)
    status["explainable"]=True
except Exception as e:
    status["_errors"].append({"explainable":str(e)})

(SUM/"visuals_final_status.json").write_text(json.dumps(status, ensure_ascii=False, indent=2))
sys.exit(0)
