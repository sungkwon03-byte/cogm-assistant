from __future__ import annotations
import io, json, os
from pathlib import Path
from typing import Optional, Tuple, List, Dict
import pandas as pd
import numpy as np

ROOT = Path(".")
OUT  = ROOT/"output"
REP  = OUT/"reports"
SUM  = OUT/"summaries"

PNG_MAGIC = b"\x89PNG\r\n\x1a\n"
PDF_MAGIC = b"%PDF-"

def _read_parquet_or_csv(p: Path, **kw) -> Optional[pd.DataFrame]:
    try:
        if p.suffix.lower()==".parquet":
            return pd.read_parquet(p, **kw)
        if p.suffix.lower()==".csv":
            return pd.read_csv(p, low_memory=False, **kw)
    except Exception:
        return None
    return None

def load_statcast() -> Optional[pd.DataFrame]:
    cand = [
        OUT/"statcast_ultra_full_clean.parquet",
        OUT/"statcast_ultra_full.parquet",
        OUT/"statcast_master_full.parquet",
    ]
    for p in cand:
        if p.exists():
            df = _read_parquet_or_csv(p)
            if df is not None and len(df)>0:
                return df
    return None

def load_cards() -> Optional[pd.DataFrame]:
    cand = [
        OUT/"player_cards_all.parquet",
        OUT/"player_cards_enriched_all_seq.parquet",
        OUT/"player_cards_ultra.csv",
        OUT/"player_cards_enriched_full.csv",
    ]
    for p in cand:
        if p.exists():
            df = _read_parquet_or_csv(p)
            if df is not None and len(df)>0:
                return df
    return None

def name_columns(df: pd.DataFrame) -> Tuple[str,str]:
    # (player_id, player_name) 후보 탐색
    id_cands = ["player_id","mlb_id","bat_id","pitcher_id","batter","id"]
    nm_cands = ["player_name","name","full_name","mlb_name","Name"]
    pid = next((c for c in id_cands if c in df.columns), None)
    nm  = next((c for c in nm_cands if c in df.columns), None)
    if pid is None:
        df["player_id"] = np.arange(len(df))
        pid = "player_id"
    if nm is None:
        df["player_name"] = df[pid].astype(str)
        nm = "player_name"
    return pid, nm

def season_column(df: pd.DataFrame) -> str:
    for c in ["season","year","game_year"]:
        if c in df.columns: return c
    if "game_date" in df.columns:
        s = pd.to_datetime(df["game_date"], errors="coerce").dt.year
        df["season"] = s
        return "season"
    df["season"] = np.nan
    return "season"

def is_png(p: Path) -> bool:
    try:
        with open(p, "rb") as f:
            return f.read(8)==PNG_MAGIC
    except Exception:
        return False

def is_pdf(p: Path) -> bool:
    try:
        with open(p, "rb") as f:
            return f.read(4)==PDF_MAGIC
    except Exception:
        return False

def list_valid_pngs() -> List[Path]:
    paths = []
    for base in [REP, Path("reports")]:
        if base.exists():
            for p in base.glob("*.png"):
                if is_png(p):
                    paths.append(p)
    return sorted(paths)

def list_pdfs() -> List[Path]:
    outs = []
    for base in [REP, Path("reports")]:
        if base.exists():
            for p in base.glob("*.pdf"):
                if is_pdf(p):
                    outs.append(p)
    return sorted(outs)

def load_qc() -> Dict:
    p = OUT/"full_system_validation.json"
    if p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            pass
    return {}
