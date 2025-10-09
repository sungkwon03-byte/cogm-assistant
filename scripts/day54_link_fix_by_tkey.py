#!/usr/bin/env python3
import pandas as pd, re, os

def nkey(s):
    s = "" if pd.isna(s) else str(s)
    s = s.replace("."," ").replace("-"," ")
    return re.sub(r"\s+"," ", s).upper().strip()

def tkey(s):
    toks = re.findall(r"[A-Z0-9]+", nkey(s))
    toks.sort()
    return " ".join(toks)

cand_path = "output/xleague_link_candidates.csv"
miss_path = "output/xleague_missing_ids.csv"
name2_path = "output/cache/name2mlbid.csv"
idm_path   = "output/id_map.csv"

c = pd.read_csv(cand_path, dtype=str).fillna("")
c["_tkey"] = c["full_name"].map(tkey)

changed = 0

# 1) name2mlbid로 mlb_id 채우기 (tkey 기준)
if os.path.exists(name2_path):
    n2 = pd.read_csv(name2_path, dtype=str).fillna("")
    if "full_name" in n2.columns and "mlb_id" in n2.columns:
        n2["_tkey"] = n2["full_name"].map(tkey)
        c = c.merge(n2[["_tkey","mlb_id"]].drop_duplicates(), on="_tkey", how="left", suffixes=("","_n2"))
        # 기존 비어있고 새로 생긴 값이 있으면 대체
        mask = (c["mlb_id"].astype(str).eq("")) & (c["mlb_id_n2"].astype(str).ne(""))
        c.loc[mask, "mlb_id"] = c.loc[mask, "mlb_id_n2"]
        c = c.drop(columns=["mlb_id_n2"])
        changed += int(mask.sum())

# 2) id_map으로 retro/bbref 채우기
if os.path.exists(idm_path):
    idm = pd.read_csv(idm_path, dtype=str).fillna("")
    cols = {k:k for k in ["mlb_id","retro_id","bbref_id","full_name"]}
    idm = idm[[col for col in cols if col in idm.columns]].copy()
    # mlb_id 있으면 아이디로, 없으면 tkey로 이름조인
    if "mlb_id" in idm.columns:
        c = c.merge(idm[["mlb_id","retro_id","bbref_id"]].drop_duplicates(), on="mlb_id", how="left", suffixes=("","_idm"))
        for col in ["retro_id","bbref_id"]:
            mask = (c[col].astype(str).eq("")) & (c[f"{col}_idm"].astype(str).ne(""))
            c.loc[mask, col] = c.loc[mask, f"{col}_idm"]
        c = c.drop(columns=[x for x in ["retro_id_idm","bbref_id_idm"] if x in c.columns], errors="ignore")

    # 이름으로도 보조 매칭
    idm["_tkey"] = idm["full_name"].map(tkey) if "full_name" in idm.columns else ""
    c = c.merge(idm[["_tkey","retro_id","bbref_id"]].drop_duplicates(), on="_tkey", how="left", suffixes=("","_idm2"))
    for col in ["retro_id","bbref_id"]:
        mask = (c[col].astype(str).eq("")) & (c[f"{col}_idm2"].astype(str).ne(""))
        c.loc[mask, col] = c.loc[mask, f"{col}_idm2"]
    c = c.drop(columns=[x for x in ["retro_id_idm2","bbref_id_idm2"] if x in c.columns], errors="ignore")

# 저장
c = c.drop(columns=["_tkey"], errors="ignore")
c.to_csv(cand_path, index=False)

# 누락 파일 업데이트
miss = c[(c["mlb_id"].astype(str)=="") & (c["retro_id"].astype(str)=="") & (c["bbref_id"].astype(str)=="")]
miss.to_csv(miss_path, index=False)

print(f"[OK] link candidates updated. newly_filled_mlb_id={changed}; now_missing={len(miss)}")
