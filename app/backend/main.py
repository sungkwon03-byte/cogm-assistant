import os
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import duckdb, pandas as pd

DATA_ROOT = os.getenv("DATA_ROOT", "/workspaces/cogm-assistant/output")
PARQUET_ROOT = os.getenv("PARQUET_ROOT", f"{DATA_ROOT}/raw/statcast_parquet")
DB = duckdb.connect()
APP = FastAPI(title="PatBot Backend", version="1.0")

APP.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

def q(sql:str, params:tuple=()):
    try:
        return DB.execute(sql, params).df()
    except Exception as e:
        raise HTTPException(400, f"query_error: {e}")

@APP.get("/api/health")
def health():
    return {"ok": True}

@APP.get("/api/player/pitchmix")
def pitchmix(mlbam: int = Query(..., ge=1), year: int = Query(..., ge=1800, le=3000)):
    # 우선 CSV에서 집계(신뢰 지표가 이미 정규화됨)
    p = f"{DATA_ROOT}/statcast_pitch_mix_detailed.csv"
    try:
        df = pd.read_csv(p, low_memory=False)
    except Exception as e:
        raise HTTPException(500, f"load_error: {e}")
    sub = df[(df["mlbam"]==mlbam)&(df["year"]==year)].copy()
    if sub.empty:
        return {"rows": 0, "data": []}
    keep = ["year","mlbam","pitch_type","pitches","usage_rate","zone_rate","whiff_rate","z_whiff_rate","o_whiff_rate","csw_rate","edge_rate","heart_rate","chase_rate"]
    sub = sub[[c for c in keep if c in sub.columns]].fillna(0)
    sub = sub.sort_values("usage_rate", ascending=False)
    return {"rows": len(sub), "data": sub.to_dict(orient="records")}

@APP.get("/api/bat/tendencies")
def bat_tendencies(mlbam: int = Query(..., ge=1), vs_hand: str = Query("vsR"), year: int = Query(...)):
    p = f"{DATA_ROOT}/count_tendencies_bat.csv"
    try:
        df = pd.read_csv(p, low_memory=False)
    except Exception as e:
        raise HTTPException(500, f"load_error: {e}")
    sub = df[(df["mlbam"]==mlbam)&(df["year"]==year)]
    if "vhb" in sub.columns:
        sub = sub[sub["vhb"]==vs_hand]
    keep = ["year","mlbam","vhb","pitches","zone_rate","z_swing_rate","o_swing_rate","z_contact_rate","o_contact_rate","z_csw_rate","chase_rate","edge_rate","heart_rate","swing_rate","whiff_rate","csw_rate"]
    sub = sub[[c for c in keep if c in sub.columns]].fillna(0)
    return {"rows": len(sub), "data": sub.to_dict(orient="records")}

@APP.get("/api/search/player")
def search_player(name: str = Query(..., min_length=2, max_length=40), limit: int = 20):
    # 간단 검색: statcast_features_player_year.csv에서 이름 부분일치
    p = f"{DATA_ROOT}/statcast_features_player_year.csv"
    try:
        df = pd.read_csv(p, usecols=["mlbam","player_name","year"], low_memory=False)
    except Exception as e:
        raise HTTPException(500, f"load_error: {e}")
    mask = df["player_name"].astype(str).str.contains(name, case=False, na=False)
    out = (df[mask].drop_duplicates(["mlbam","player_name"])
                 .sort_values("player_name").head(limit))
    return {"rows": len(out), "data": out.to_dict(orient="records")}
