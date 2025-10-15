# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import pandas as pd

# --- robust parquet reader: pandas -> duckdb fallback ---
def read_parquet_robust(path):
    import pandas as _pd
    try:
        return _read_parquet_robust(path)
    except Exception:
        import duckdb as _dd
        # DuckDB reads parquet natively; return pandas DataFrame
        return _dd.query(f"SELECT * FROM read_parquet('{path}')").to_df()

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "output"
CARDS_ENR = OUT / "player_cards_enriched_all_seq.parquet"
CARDS = OUT / "player_cards_all.parquet"

WAR_COL_PREF = ["our_war","fwar","bwar","war","WAR"]
NAME_COL_PREF = ["player_name","mlb_name","name","full_name"]

def _load_cards() -> pd.DataFrame:
    for p in [CARDS_ENR, CARDS]:
        if p.exists():
            try: return read_parquet_robust(p)
            except Exception: pass
    for p in [OUT/"player_cards_enriched_full.csv", OUT/"player_cards_ultra.csv", OUT/"player_cards_all.csv"]:
        if p.exists():
            try: return pd.read_csv(p, low_memory=False)
            except Exception: pass
    raise FileNotFoundError("player cards parquet/csv not found under output/…")

def _pick_name_col(df: pd.DataFrame) -> str:
    for c in NAME_COL_PREF:
        if c in df.columns: return c
    for c in df.columns:
        if "name" in c.lower(): return c
    raise KeyError("No name-like column in cards")

def _pick_war_col(df: pd.DataFrame) -> str:
    for c in WAR_COL_PREF:
        if c in df.columns: return c
    if "wRC_plus" in df.columns:
        df["_war_proxy_"] = (pd.to_numeric(df["wRC_plus"], errors="coerce") - 100.0)/10.0; return "_war_proxy_"
    if "wRC+" in df.columns:
        df["_war_proxy_"] = (pd.to_numeric(df["wRC+"], errors="coerce") - 100.0)/10.0; return "_war_proxy_"
    raise KeyError("No WAR-like column or wRC+/wRC_plus")

def _age_curve(age):
    import math, pandas as pd
    if age is None or (isinstance(age, float) and pd.isna(age)): return 1.0
    age=float(age)
    if age < 25: return 0.94
    if 25 <= age <= 26: return 0.98
    if 27 <= age <= 29: return 1.04
    if 30 <= age <= 32: return 0.98
    if 33 <= age <= 36: return 0.92
    return 0.88

def _pos_curve(pos: str) -> float:
    if not isinstance(pos, str): return 1.0
    up = pos.upper()
    if up.startswith(("C","SS","CF")): return 1.06
    if up.startswith(("2B","3B","RF")): return 1.02
    if up.startswith(("1B","LF","DH")): return 0.95
    return 1.0

def _present_or_none(row, cols):
    for c in cols:
        if c in row and pd.notna(row[c]): return float(row[c])
    return None

def _dollar_per_war(base=9.5e6, years=3):
    return [base*(1.02**i) for i in range(years)]

def _npv(cashflows, r=0.08):
    return sum(cf/((1+r)**t) for t, cf in enumerate(cashflows, start=1))

def _recent3_weighted_mean(vals):
    import pandas as pd
    vals = [v for v in vals if pd.notna(v)]
    if not vals: return 0.0
    vals = vals[-3:]
    weights = list(range(len(vals), 0, -1))
    return float(sum(v*w for v,w in zip(vals, weights))) / float(sum(weights))

def get_trade_value_score(player_name: str, team: str|None=None) -> dict:
    df = _load_cards()
    name_col = _pick_name_col(df)
    war_col = _pick_war_col(df)
    sdf = df[df[name_col].astype(str).str.lower() == str(player_name).lower()].copy()
    if sdf.empty:
        sdf = df[df[name_col].astype(str).str.lower().str.contains(str(player_name).lower())].copy()
    if sdf.empty:
        raise ValueError(f"player not found: {player_name}")

    s_col = "season" if "season" in sdf.columns else ("year" if "year" in sdf.columns else None)
    if s_col is not None: sdf = sdf.sort_values(s_col)

    rec = sdf.tail(3)
    war_vals = pd.to_numeric(rec[war_col], errors="coerce").fillna(0.0).tolist()
    base_war = _recent3_weighted_mean(war_vals)

    age = _present_or_none(rec.iloc[-1], ["age","Age","player_age"])
    pos = rec.iloc[-1].get("pos") or rec.iloc[-1].get("position")
    adj = _age_curve(age) * _pos_curve(pos)
    adj_war = base_war * adj

    proj = [adj_war, adj_war*0.85, adj_war*0.75]
    per = _dollar_per_war()
    market_cashflows = [proj[i]*per[i] for i in range(3)]
    market_value_npv = _npv(market_cashflows)

    last = rec.iloc[-1].to_dict()
    remY = int(_present_or_none(last, ["years_remaining","y_remaining","remaining_years"]) or 3)
    aav = _present_or_none(last, ["aav","AAV","salary","Salary","annual_salary"])
    if aav is None: aav = (sum(per)/3.0)*0.40
    cost_flows = [aav]*min(remY,3)
    contract_cost_npv = _npv(cost_flows) if cost_flows else 0.0

    surplus_npv = market_value_npv - contract_cost_npv
    return {
        "player": str(rec.iloc[-1][name_col]),
        "team": team or "",
        "age": age,
        "pos": pos,
        "base_war_recent3": round(base_war,3),
        "adj_factor": round(adj,3),
        "proj_year_war": [round(v,3) for v in proj],
        "market_cashflows": [round(v,2) for v in market_cashflows],
        "market_value_npv": round(market_value_npv,2),
        "contract_cost_npv": round(contract_cost_npv,2),
        "surplus_value_npv": round(surplus_npv,2),
    }
