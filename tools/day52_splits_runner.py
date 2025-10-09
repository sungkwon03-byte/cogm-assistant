#!/usr/bin/env python3
import os, json, glob, pandas as pd
from pathlib import Path
from tools.name_utils import add_norm_name_column

ROOT = Path(".").resolve()
DATA_SPLITS = ROOT/"data/splits"
OUTPUT = ROOT/"output"
TOOLS = ROOT/"tools"

def load_columns_map(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_splits_frames() -> pd.DataFrame:
    rows = []
    for p in sorted(glob.glob(str(DATA_SPLITS / "*.csv"))):
        src = "FG" if os.path.basename(p).startswith("fg_") else ("BR" if os.path.basename(p).startswith("br_") else "UNK")
        try:
            df = pd.read_csv(p)
        except Exception as e:
            print(f"[WARN] Skipping {p}: {e}")
            continue
        base = os.path.basename(p); parts = base.split("_")
        season = None; kind = None
        try:
            season = int(parts[1]); kind = parts[2].split(".")[0]   # bat|pit
        except: pass
        df["season_file"] = season; df["kind"] = kind; df["source_raw"] = src; df["file_path"] = p
        rows.append(df)
    if not rows: return pd.DataFrame()
    return pd.concat(rows, ignore_index=True, sort=False)

def canonicalize(df_all: pd.DataFrame, cmap) -> pd.DataFrame:
    if df_all.empty: return df_all
    frames = []
    for src_key in ["FG","BR","UNK"]:
        sub = df_all[df_all["source_raw"].eq(src_key)].copy()
        if sub.empty: continue
        sub = add_norm_name_column(sub, "Name", "name_norm")
        if "Season" in sub.columns: sub["season"] = sub["Season"]
        elif "season_file" in sub.columns: sub["season"] = sub["season_file"]
        cmap_src = cmap.get(src_key, {})
        for raw, canon in cmap_src.items():
            if raw in sub.columns: sub[canon] = sub[raw]
        if "name" not in sub.columns and "Name" in sub.columns: sub["name"] = sub["Name"]
        if "team" not in sub.columns:
            if "Team" in sub.columns: sub["team"] = sub["Team"]
            elif "Tm" in sub.columns: sub["team"] = sub["Tm"]
            else: sub["team"] = ""
        sub["source"] = src_key
        frames.append(sub)
    if not frames: return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True, sort=False)
    keep_cols = ["season","name","name_norm","team","kind","source",
                 "vsL_wRCplus","vsR_wRCplus","vsL_OPS","vsR_OPS","vsL_wOBA","vsR_wOBA",
                 "PA","AB","H","HR","BB_pct","K_pct","AVG","OBP","SLG","OPS","wOBA","wRCplus","ISO","BABIP",
                 "IP","TBF","ERA","FIP","file_path"]
    final_cols = [c for c in keep_cols if c in out.columns]
    out = out[final_cols].dropna(subset=["season","name_norm"])
    return out

def coalesce_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    value_cols = [c for c in df.columns if c not in ["season","name","name_norm","team","kind","source","file_path"]]
    order = df["source"].map({"FG":0,"BR":1}).fillna(2)
    df = df.assign(_ord=order).sort_values(by=["season","name_norm","kind","_ord"])
    grp = df.groupby(["season","name_norm","kind"], as_index=False)
    agg = {c: "first" for c in value_cols}
    for c in ["name","team","file_path","source","season","name_norm","kind"]:
        if c in df.columns: agg[c] = "first"
    out = grp.agg(agg)
    return out.drop(columns=[c for c in ["_ord"] if c in out.columns])

def load_id_map() -> pd.DataFrame:
    p = OUTPUT/"id_map.csv"
    if p.exists():
        df = pd.read_csv(p)
        if "name_norm" not in df.columns and "full_name" in df.columns:
            from tools.name_utils import normalize_name
            df["name_norm"] = df["full_name"].map(normalize_name)
        return df
    return pd.DataFrame()

def attach_ids(df: pd.DataFrame, iddf: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    for c in ["mlb_id","retro_id","bbref_id"]:
        if c not in df.columns: df[c] = None
    if iddf.empty:
        return df
    keep = [c for c in ["name_norm","mlb_id","retro_id","bbref_id","full_name"] if c in iddf.columns]
    iddf2 = iddf[keep].drop_duplicates()
    merged = df.merge(iddf2, on="name_norm", how="left", validate="m:1")
    for c in ["mlb_id","retro_id","bbref_id"]:
        if c in merged.columns:
            merged[c] = merged[c].where(merged[c].notna(), None)
    return merged

def qc_report(df: pd.DataFrame, outpath: Path):
    lines = [f"rows={len(df)}"]
    if not df.empty:
        nulls = df.isna().mean().sort_values(ascending=False)
        lines.append("null_fraction:\n"+nulls.to_string())
        cols = [c for c in ["vsL_OPS","vsR_OPS","vsL_wRCplus","vsR_wRCplus","FIP","ERA","PA","IP"] if c in df.columns]
        if cols:
            lines.append("describe:\n"+df[cols].describe(include="all").to_string())
    outpath.write_text("\n\n".join(lines), encoding="utf-8")

def main():
    cmap = load_columns_map(TOOLS/"day52_columns_map.json")
    raw = load_splits_frames()
    if raw.empty:
        print("No input CSVs in data/splits/."); return 1
    can = canonicalize(raw, cmap)
    merged = coalesce_duplicates(can)
    idmap = load_id_map()
    merged = attach_ids(merged, idmap)
    OUTPUT.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUTPUT/"splits_merged.csv", index=False)
    if {"mlb_id","retro_id","bbref_id"}.issubset(set(merged.columns)):
        unmatched = merged[merged[["mlb_id","retro_id","bbref_id"]].isna().all(axis=1)].copy()
    else:
        unmatched = merged.copy()
    unmatched.to_csv(OUTPUT/"splits_unmatched.csv", index=False)
    qc_report(merged, OUTPUT/"splits_qc.txt")
    print("WROTE:\n - output/splits_merged.csv\n - output/splits_unmatched.csv\n - output/splits_qc.txt")
    return 0

if __name__=="__main__":
    raise SystemExit(main())
