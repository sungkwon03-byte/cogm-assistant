#!/usr/bin/env python3
import os, glob, json, pandas as pd

SEARCH_DIRS = [
    "mart","data","output","external","inputs","raw","downloads","."
]

WAR_CANDS = ["WAR","fWAR","war","WAR_total","Bat WAR","Pit WAR","war_total","war_used"]
TEAM_CANDS = ["Team","Tm","team","org","Org","group_id","team_id"]
SEASON_CANDS = ["Season","season","Year","year","season_std"]
LEAGUE_CANDS = ["league","League","src_league"]

def guess_league(path, df):
    cols = set(df.columns)
    # 우선 컬럼 기반
    for c in LEAGUE_CANDS:
        if c in cols:
            v = str(df[c].dropna().astype(str).str.upper().head(1).tolist()[0] if not df[c].dropna().empty else "")
            if v in {"MLB","KBO","MILB","NPB"}: return v
    # 파일경로 힌트
    p = path.lower()
    if "kbo" in p: return "KBO"
    if "milb" in p or "minor" in p: return "MiLB"
    if "npb" in p: return "NPB"
    if "mlb" in p: return "MLB"
    return "UNK"

def pick(df, cands):
    for c in cands:
        if c in df.columns: return c
    return None

rows = []
for root in SEARCH_DIRS:
    if not os.path.isdir(root): continue
    for ext in ("*.csv","*.parquet"):
        for p in glob.glob(os.path.join(root,"**",ext), recursive=True):
            try:
                df = pd.read_parquet(p) if p.endswith(".parquet") else pd.read_csv(p, nrows=5000)
            except Exception:
                continue
            war_col = pick(df, WAR_CANDS)
            season_col = pick(df, SEASON_CANDS)
            team_col = pick(df, TEAM_CANDS)
            league = guess_league(p, df)
            nz = int((pd.to_numeric(df.get(war_col,0), errors="coerce").fillna(0)!=0).sum()) if war_col else 0
            rows.append({
                "path": p, "league": league, "war_col": war_col,
                "season_col": season_col, "team_col": team_col,
                "nonzero_war_rows": nz, "n_rows": len(df)
            })

inv = pd.DataFrame(rows).sort_values(["league","nonzero_war_rows"], ascending=[True,False])
os.makedirs("logs", exist_ok=True)
inv.to_csv("logs/source_inventory.csv", index=False)
with open("logs/source_inventory.json","w") as f:
    json.dump(inv.to_dict(orient="records"), f, indent=2)
print("[OK] inventory -> logs/source_inventory.csv (and .json)")
print(inv.head(25).to_string(index=False))
