from app.lib.name_resolver import resolve_names
import pandas as pd
from .data import load_id_map

ID_COLUMNS_CANON = [
    "retro_id","mlbam_id","statcast_id","lahman_id","chadwick_id",
    "first_name","last_name","full_name","debut","final_game"
]

def idmap_table() -> pd.DataFrame:
    df = load_id_map().copy()
    if df.empty:
        return df
    lower = {c.lower(): c for c in df.columns}
    for need in ID_COLUMNS_CANON:
        if need not in df.columns and need in lower:
            df.rename(columns={lower[need]: need}, inplace=True)
    if "full_name" not in df.columns:
        fn = df.get("first_name", pd.Series([""]*len(df)))
        ln = df.get("last_name", pd.Series([""]*len(df)))
        df["full_name"] = (fn.fillna("") + " " + ln.fillna("")).str.strip()
    return df

def link_on_any_id(df: pd.DataFrame, id_col: str, target_cols=None) -> pd.DataFrame:
    im = idmap_table()
    if im.empty or id_col not in df.columns:
        return df
    if target_cols is None:
        target_cols = ["mlbam_id","statcast_id","retro_id","lahman_id","chadwick_id","full_name"]
    out = df.copy()
    joined = None
    for key in ["mlbam_id","statcast_id","retro_id","lahman_id","chadwick_id","full_name"]:
        if key in im.columns:
            cand = im[[key] + [c for c in target_cols if c in im.columns]].drop_duplicates()
            cand[key] = cand[key].astype(str)
            tmp = out.merge(cand, how="left", left_on=id_col, right_on=key, suffixes=("","_im"))
            if joined is None:
                joined = tmp
            else:
                for c in target_cols:
                    if c in joined and c in tmp:
                        joined[c] = joined[c].fillna(tmp[c])
    return joined if joined is not None else out
