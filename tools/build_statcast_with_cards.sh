#!/usr/bin/env bash
set -euo pipefail
ROOT="/workspaces/cogm-assistant"; OUT="$ROOT/output"; mkdir -p "$OUT"

python3 - <<'PY'
import glob, numpy as np, pandas as pd, unicodedata
from pathlib import Path

ROOT=Path("/workspaces/cogm-assistant"); OUT=ROOT/"output"
sc_pq = OUT/"statcast_ultra_full_clean.parquet"
if not sc_pq.exists(): raise FileNotFoundError(sc_pq)

def normalize_name(s: str) -> str:
    if s is None: return ""
    s = str(s).strip()
    # "Last, First" -> "First Last"
    if "," in s:
        last, first = [x.strip() for x in s.split(",", 1)]
        s = f"{first} {last}"
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace(".", "").replace("'", "").replace("-", " ")
    s = " ".join(s.replace(",", " ").split())
    return s.lower()

def clean_hand(x):
    if x is None or (isinstance(x, float) and np.isnan(x)): return np.nan
    s = str(x).strip().upper()
    if s in ("R","L","S","B"): return s
    if s.startswith("R"): return "R"
    if s.startswith("L"): return "L"
    if s.startswith("S"): return "S"
    if s.startswith("B"): return "B"
    return np.nan

# 카드 집계: parquet 우선, 없으면 CSV 전수
cards = None
for p in [OUT/"player_cards_enriched_all_seq.parquet", OUT/"player_cards_all.parquet"]:
    if p.exists():
        df = pd.read_parquet(p)
        name_col = "name" if "name" in df.columns else ("player_name" if "player_name" in df.columns else None)
        if name_col is None: continue
        take = [name_col]
        if "bats" in df.columns:   take.append("bats")
        if "throws" in df.columns: take.append("throws")
        cards = df[take].rename(columns={name_col:"name"}).copy()
        break

if cards is None:
    pool=[]
    for path in glob.glob(str(OUT/"player_cards_*.csv")):
        try: df = pd.read_csv(path, low_memory=False)
        except: continue
        name_col = "player_name" if "player_name" in df.columns else ("name" if "name" in df.columns else None)
        if not name_col: continue
        take=[name_col] + ([ "bats"] if "bats" in df.columns else []) + (["throws"] if "throws" in df.columns else [])
        pool.append(df[take].rename(columns={name_col:"name"}))
    if not pool: raise RuntimeError("no usable cards with name/bats/throws")
    cards = pd.concat(pool, ignore_index=True)

cards["name_norm"] = cards["name"].map(normalize_name)
if "bats"   in cards.columns:   cards["bats"]   = cards["bats"].map(clean_hand)
if "throws" in cards.columns:   cards["throws"] = cards["throws"].map(clean_hand)

def majority(s):
    vc = s.dropna().astype(str).value_counts()
    return vc.index[0] if len(vc) else np.nan

cross = cards.groupby("name_norm").agg(
    bats=("bats", majority) if "bats" in cards.columns else ("name_norm", lambda s: np.nan),
    throws=("throws", majority) if "throws" in cards.columns else ("name_norm", lambda s: np.nan),
).reset_index()

sc = pd.read_parquet(sc_pq)
name_cols = [c for c in ["player_name","player_name_1","batter_name","hitter_name","name"] if c in sc.columns]
if not name_cols: raise RuntimeError("statcast has no name-like column")

sc = sc.copy()
sc["bats_resolved"]   = np.nan
sc["throws_resolved"] = np.nan

for nc in name_cols:
    key = sc[nc].map(normalize_name).rename("name_norm").to_frame()
    m = key.merge(cross, on="name_norm", how="left")
    mask_b = sc["bats_resolved"].isna() & m["bats"].notna()
    sc.loc[mask_b, "bats_resolved"] = m.loc[mask_b, "bats"]
    mask_t = sc["throws_resolved"].isna() & m["throws"].notna()
    sc.loc[mask_t, "throws_resolved"] = m.loc[mask_t, "throws"]

# 보조: vs_hand 있으면 bats 보정
if "vs_hand" in sc.columns:
    vh = sc["vs_hand"].map(clean_hand)
    sc.loc[sc["bats_resolved"].isna() & vh.notna(), "bats_resolved"] = vh[vh.notna()]

cov_b = float(sc["bats_resolved"].notna().mean())
cov_t = float(sc["throws_resolved"].notna().mean())
print({"coverage":{"bats":round(cov_b,3),"throws":round(cov_t,3)}})

out = OUT/"statcast_with_cards.parquet"
sc.to_parquet(out, index=False)
print(f"[DONE] {out}")
PY
