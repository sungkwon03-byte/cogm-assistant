#!/usr/bin/env python3
import os, pandas as pd
from tools.name_utils import normalize_name

LAHMAN = "data/id/Lahman/People.csv"
CHAD = "data/id/Chadwick/register.csv"
OUT = "output/id_map.csv"

def load_lahman():
    if not os.path.exists(LAHMAN): return pd.DataFrame()
    df = pd.read_csv(LAHMAN)
    # 표준 컬럼 방어
    fn = None
    ln = None
    for a in ["nameFirst","name_first","given_name","first_name"]:
        if a in df.columns: fn = a; break
    for b in ["nameLast","name_last","surname","last_name","family_name"]:
        if b in df.columns: ln = b; break
    if fn is None and "name" in df.columns:
        df["full_name"] = df["name"].astype(str)
    else:
        df["full_name"] = (df.get(fn,"").astype(str) + " " + df.get(ln,"").astype(str)).str.strip()
    # ID 컬럼 정규화
    rename = {}
    if "bbrefID" in df.columns: rename["bbrefID"] = "bbref_id"
    if "retroID" in df.columns:  rename["retroID"] = "retro_id"
    if "mlbID" in df.columns:    rename["mlbID"] = "mlb_id"
    if "mlbamID" in df.columns:  rename["mlbamID"] = "mlb_id"
    df = df.rename(columns=rename)
    keep = ["full_name","bbref_id","retro_id","mlb_id"]
    for k in keep:
        if k not in df.columns: df[k] = None
    df["name_norm"] = df["full_name"].map(normalize_name)
    return df[["name_norm","full_name","mlb_id","retro_id","bbref_id"]].drop_duplicates()

def load_chadwick():
    if not os.path.exists(CHAD): return pd.DataFrame()
    df = pd.read_csv(CHAD)
    rename = {}
    # Chadwick register 표준
    if "name_first" in df.columns and "name_last" in df.columns:
        df["full_name"] = (df["name_first"].astype(str)+" "+df["name_last"].astype(str)).str.strip()
    elif "name_full" in df.columns:
        df["full_name"] = df["name_full"].astype(str)
    else:
        # best-effort
        df["full_name"] = df.get("name_first","").astype(str)+" "+df.get("name_last","").astype(str)
    if "key_mlbam" in df.columns: rename["key_mlbam"] = "mlb_id"
    if "key_retro" in df.columns: rename["key_retro"] = "retro_id"
    if "key_bbref" in df.columns: rename["key_bbref"] = "bbref_id"
    df = df.rename(columns=rename)
    for k in ["mlb_id","retro_id","bbref_id"]:
        if k not in df.columns: df[k] = None
    df["name_norm"] = df["full_name"].map(normalize_name)
    return df[["name_norm","full_name","mlb_id","retro_id","bbref_id"]].drop_duplicates()

def coalesce(left, right):
    if left.empty and right.empty: return pd.DataFrame(columns=["mlb_id","retro_id","bbref_id","full_name","name_norm"])
    df = pd.concat([left.assign(source="lahman"), right.assign(source="chadwick")], ignore_index=True)
    # 우선순위: mlb_id 존재 > retro_id 존재 > bbref_id 존재
    pri = df[["mlb_id","retro_id","bbref_id"]].notna().dot([3,2,1])
    df = df.assign(_pri=pri)
    df = df.sort_values(by=["name_norm","_pri","source"], ascending=[True, False, True])
    agg = {
        "full_name":"first","mlb_id":"first","retro_id":"first","bbref_id":"first"
    }
    out = df.groupby("name_norm", as_index=False).agg(agg)
    return out[["mlb_id","retro_id","bbref_id","full_name","name_norm"]]

def main():
    os.makedirs("output", exist_ok=True)
    L = load_lahman()
    C = load_chadwick()
    merged = coalesce(L, C)
    merged.to_csv(OUT, index=False)
    print(f"WROTE {OUT} rows={len(merged)}")

if __name__ == "__main__":
    main()
