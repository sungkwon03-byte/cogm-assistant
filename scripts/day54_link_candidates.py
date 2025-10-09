#!/usr/bin/env python3
import os, re, pandas as pd

os.makedirs("output", exist_ok=True)

def nkey(s):
    s = "" if pd.isna(s) else str(s)
    s = s.replace("."," ").replace("-", " ")
    s = re.sub(r"\s+"," ", s).upper().strip()
    return s

def pick_name_col(df):
    for c in ["full_name","name","player","Player","PLAYER"]:
        if c in df.columns:
            return c
    return None

def main():
    kbo = pd.read_csv("data/xleague/kbo_batting.csv", dtype=str, low_memory=False)
    kbo_name_col = pick_name_col(kbo)
    if not kbo_name_col:
        raise SystemExit("[FATAL] data/xleague/kbo_batting.csv에 이름 컬럼(full_name/name/player)이 없습니다.")
    names = kbo[[kbo_name_col]].rename(columns={kbo_name_col:"full_name"}).dropna().drop_duplicates().copy()
    names["nkey"] = names["full_name"].map(nkey)

    idmap_path = "output/id_map.csv"
    if os.path.exists(idmap_path):
        idm = pd.read_csv(idmap_path, dtype=str).fillna("")
        if "nkey" not in idm.columns:
            nmcol = pick_name_col(idm)
            if nmcol:
                idm["nkey"] = idm[nmcol].map(nkey)
            else:
                raise SystemExit("[FATAL] output/id_map.csv에 nkey 또는 이름 컬럼이 필요합니다.")
        for col in ["mlb_id","retro_id","bbref_id"]:
            if col not in idm.columns: idm[col] = ""
        cand = names.merge(idm[["nkey","mlb_id","retro_id","bbref_id"]], on="nkey", how="left")
    else:
        cand = names.copy()
        for col in ["mlb_id","retro_id","bbref_id"]:
            cand[col] = ""

    cand["method"] = "exact_nkey_or_alias"
    cand.to_csv("output/xleague_link_candidates.csv", index=False)

    miss = cand[(cand["mlb_id"].isna()) | (cand["retro_id"].isna()) | (cand["bbref_id"].isna())].copy()
    miss.to_csv("output/xleague_missing_ids.csv", index=False)

    print("[OK] Day54 Step3 complete -> output/xleague_link_candidates.csv, xleague_missing_ids.csv")

if __name__ == "__main__":
    main()
