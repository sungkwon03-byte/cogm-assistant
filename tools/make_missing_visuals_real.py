#!/usr/bin/env python3
# Real-only visual builder (fills ONLY missing artefacts). No seaborn; matplotlib only.
import sys, json
from pathlib import Path
import pandas as pd, numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT=Path("/workspaces/cogm-assistant")
OUT=ROOT/"output"; REP=OUT/"reports"; SUM=OUT/"summaries"
SC=OUT/"statcast_ultra_full_clean.parquet"
assert SC.exists(), f"[FATAL] missing {SC} (real statcast required)"

def read_sc():
    try:
        cols = ["player_name","batter","batter_hand","stand","pitcher_throws","pitch_type",
                "woba_value","woba_denom","estimated_woba_using_speedangle",
                "launch_speed","plate_x","plate_z",
                "description","type","home_plate_umpire",
                "year","game_year","game_date","sz_bot","sz_top"]
        try:
            header = pd.read_parquet(SC, rows=0)
            use = [c for c in cols if c in header.columns]
            return pd.read_parquet(SC, columns=use)
        except Exception:
            return pd.read_parquet(SC)
    except Exception as e:
        raise RuntimeError(f"read_parquet failed: {e}")

def ensure_dir(p: Path): p.parent.mkdir(parents=True, exist_ok=True)

def pick_col(df, names, allow_make_year=False):
    for n in names:
        if n in df.columns: return n
    if allow_make_year and "game_date" in df.columns:
        yy = pd.to_datetime(df["game_date"], errors="coerce").dt.year
        df.insert(len(df.columns), "year", yy)
        return "year"
    return None

def build_platoon(df):
    out_csv = SUM/"platoon_split.csv"
    out_png = REP/"platoon_map.png"
    if out_csv.exists() and out_png.exists(): return True

    hb = pick_col(df, ["batter_hand","stand"])
    hp = pick_col(df, ["pitcher_throws"])
    if not hb or not hp: raise RuntimeError("missing batter_hand/stand or pitcher_throws")
    # wOBA proxy
    if {"woba_value","woba_denom"}.issubset(df.columns):
        df["woba_val"], df["woba_den"] = df["woba_value"], df["woba_denom"]
    elif "estimated_woba_using_speedangle" in df.columns:
        df["woba_val"] = pd.to_numeric(df["estimated_woba_using_speedangle"], errors="coerce").fillna(0.0)
        df["woba_den"] = 1.0
    elif "launch_speed" in df.columns:
        ev = pd.to_numeric(df["launch_speed"], errors="coerce")
        z = (ev - ev.mean())/ev.std(ddof=0) if ev.std(ddof=0) else ev*0
        df["woba_val"], df["woba_den"] = z.fillna(0), 1.0
    else:
        raise RuntimeError("no wOBA proxy (need woba_* or estimated_woba_using_speedangle or launch_speed)")
    y = pick_col(df, ["year","game_year"], allow_make_year=True)
    grp = df.groupby([y,hb,hp], dropna=True).agg(PA=("batter","count"),
                                                 woba_val=("woba_val","sum"),
                                                 woba_den=("woba_den","sum")).reset_index()
    grp["wOBA"] = grp["woba_val"] / grp["woba_den"].replace(0,np.nan)
    ensure_dir(out_csv); grp.to_csv(out_csv, index=False)

    agg = grp.groupby([hp,hb], as_index=False)["wOBA"].mean(numeric_only=True)
    x = np.arange(len(agg))
    fig = plt.figure(figsize=(8,5))
    plt.bar(x, agg["wOBA"].fillna(0))
    plt.xticks(x, agg[hp].astype(str)+"/"+agg[hb].astype(str))
    plt.title("Platoon split (avg wOBA, real data)")
    plt.tight_layout(); ensure_dir(out_png); plt.savefig(out_png, dpi=180); plt.close(fig)
    return True

def build_weakness(df):
    out_csv = SUM/"weakness_heatmap_matrix.csv"
    out_png = REP/"weakness_heatmap.png"
    if out_csv.exists() and out_png.exists(): return True

    need = {"pitch_type","plate_x","plate_z"}
    if not need.issubset(df.columns): raise RuntimeError(f"missing columns for heatmap: {need - set(df.columns)}")
    # strike zone guard
    for c,fill in (("sz_bot",1.5),("sz_top",3.5)):
        if c not in df.columns: df[c]=fill
    px = pd.to_numeric(df["plate_x"], errors="coerce").clip(-0.83,0.83)
    pz = pd.to_numeric(df["plate_z"], errors="coerce").clip(1.5,3.5)
    xb = pd.cut(px, bins=5, labels=False)
    zb = pd.cut(pz, bins=5, labels=False)
    mat = pd.DataFrame({"pitch_type":df["pitch_type"].astype(str), "zone":xb.astype(str)+"x"+zb.astype(str)})
    mat = mat.groupby(["pitch_type","zone"]).size().reset_index(name="n")
    ensure_dir(out_csv); mat.to_csv(out_csv, index=False)

    top = mat.groupby("pitch_type")["n"].sum().sort_values(ascending=False)
    if top.empty: raise RuntimeError("no pitch_type counts")
    tname = top.index[0]
    sel = mat[mat["pitch_type"]==tname].copy()
    def idx(z):
        try: a,b=z.split("x"); return int(a),int(b)
        except: return None
    sel=sel.assign(idx=sel["zone"].apply(idx)).dropna(subset=["idx"])
    grid = np.zeros((5,5))
    for _,r in sel.iterrows():
        a,b=r["idx"]
        if 0<=a<5 and 0<=b<5: grid[b,a]=r["n"]
    fig=plt.figure(figsize=(6,5))
    plt.imshow(grid, origin="lower"); plt.title(f"Weakness heatmap ({tname})")
    plt.colorbar(label="count"); plt.tight_layout(); ensure_dir(out_png); plt.savefig(out_png,dpi=180); plt.close(fig)
    return True

def build_euz(df):
    out_csv = SUM/"euz_umpire_impact.csv"
    out_png = REP/"ump_euz.png"
    if out_csv.exists() and out_png.exists(): return True

    ump = pick_col(df, ["home_plate_umpire"]) or "home_plate_umpire"
    if ump not in df.columns: df[ump]="unknown"
    desc = df["description"] if "description" in df.columns else None
    typ  = df["type"] if "type" in df.columns else None
    for c in ["plate_x","plate_z"]:
        if c not in df.columns: raise RuntimeError("missing plate_x/plate_z for EUZ")

    take_mask = pd.Series(False, index=df.index)
    if desc is not None:
        low = desc.astype(str).str.lower()
        take_mask |= low.isin(["called_strike","ball"])
    if typ is not None:
        lowt = typ.astype(str).str.lower()
        take_mask |= lowt.isin(["s","b"])
    take = df[take_mask]
    if take.empty: raise RuntimeError("no taken pitches")

    is_cs = pd.Series(False, index=take.index)
    if desc is not None: is_cs |= take["description"].astype(str).str.lower().eq("called_strike")
    if typ  is not None: is_cs |= take["type"].astype(str).str.lower().eq("s")
    take = take.assign(is_cs=is_cs)

    liga = float(take["is_cs"].mean())
    byu = take.groupby(ump)["is_cs"].mean().reset_index()
    byu["delta_vs_lg"] = byu["is_cs"] - liga
    ensure_dir(out_csv); byu.to_csv(out_csv, index=False)

    s = byu.sort_values("delta_vs_lg", ascending=False)
    head, tail = s.head(10), s.tail(10)
    labels = list(head[ump]) + list(tail[ump])
    vals   = list(head["delta_vs_lg"]) + list(tail["delta_vs_lg"])
    x=np.arange(len(labels))
    fig=plt.figure(figsize=(8,6))
    plt.bar(x, vals); plt.axhline(0, linewidth=1)
    plt.xticks(x, labels, rotation=90)
    plt.ylabel("Δ called strike% vs league")
    plt.title("Umpire EUZ impact (Top/Bottom 10, real data)")
    plt.tight_layout(); ensure_dir(out_png); plt.savefig(out_png, dpi=180); plt.close(fig)
    return True

def main():
    df = read_sc()
    prov = {
        "statcast_file": str(SC),
        "row_count": int(len(df)),
        "min_year": int(pd.to_datetime(df.get("game_date", pd.Series())).dt.year.min()) if "game_date" in df.columns and len(df)>0 else int(pd.to_numeric(df.get("year", pd.Series()), errors="coerce").min() or 0),
        "max_year": int(pd.to_datetime(df.get("game_date", pd.Series())).dt.year.max()) if "game_date" in df.columns and len(df)>0 else int(pd.to_numeric(df.get("year", pd.Series()), errors="coerce").max() or 0),
    }
    (SUM/"_provenance_statcast.json").write_text(json.dumps(prov, indent=2))
    ok = True
    for fn in (build_platoon, build_weakness, build_euz):
        try:
            ok = fn(df) and ok
        except Exception as e:
            print(f"[WARN] {fn.__name__} failed: {e}", flush=True)
            ok = False
    print(json.dumps({"ok": ok}, indent=2))
    return 0

if __name__=="__main__":
    sys.exit(main())
