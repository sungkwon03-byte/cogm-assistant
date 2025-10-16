from __future__ import annotations
import re, unicodedata
from dataclasses import dataclass
from functools import lru_cache
from typing import List
import pandas as pd

NAME_COLS=["full_name","player_name","name","name_display_first_last","name_first","name_last"]
ID_CANDS=["player_id","bbref_id","retro_id","retroID","mlbam_id","mlbam","mlb_id","key_mlbam","chadwick_id","lahman_id","fg_id"]

def _norm(s:str)->str:
    if s is None: return ""
    s=unicodedata.normalize("NFKD",str(s)).encode("ascii","ignore").decode("ascii")
    return re.sub(r"\s+"," ",s.strip()).lower()

@lru_cache(maxsize=1)
def load_master()->pd.DataFrame:
    dfs=[]
    for p in ["output/id_map.csv","output/player_cards_all.parquet"]:
        try:
            dfs.append(pd.read_csv(p) if p.endswith(".csv") else pd.read_parquet(p))
        except Exception: pass
    if not dfs: raise FileNotFoundError("no id_map/cards found")
    df=pd.concat(dfs,ignore_index=True)
    df["_full_name"]=df.apply(lambda r: next((r[c] for c in NAME_COLS if c in r and isinstance(r[c],str) and r[c].strip()),None),axis=1)
    df["_name_key"]=df["_full_name"].map(_norm)
    for c in ID_CANDS:
        if c in df.columns: df["_pid"]=df[c]; break
    df=df.dropna(subset=["_full_name"]).drop_duplicates(subset=["_name_key","_pid"],keep="first")
    return df

def resolve_names(names:List[str]):
    df=load_master(); ids=[]; diag=[]
    for n in names:
        k=_norm(n)
        m=df[df["_name_key"].str.contains(k,na=False)]
        if m.empty: diag.append({"input":n,"status":"no_match"}); continue
        pid=str(m.iloc[0].get("_pid","")); ids.append(pid); diag.append({"input":n,"status":"ok","id":pid})
    return ids,diag
