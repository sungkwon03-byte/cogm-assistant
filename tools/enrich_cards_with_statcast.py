import os, sys, glob, pandas as pd

STATCAST_DIR = "output/cache/statcast_clean"
if not os.path.isdir(STATCAST_DIR):
    sys.exit("No statcast cache dir; skip")

def find_statcast_csv(season:int):
    pats = [f"*{season}*.csv", f"*{season}*.parquet"]
    files=[]
    for p in pats:
        files += glob.glob(os.path.join(STATCAST_DIR, p))
    return files

def load_statcast_agg(files):
    # 프로젝트 규칙에 맞게 변경: 여기선 player_uid 수준 간단 집계 예시
    dfs=[]
    for f in files:
        try:
            df = pd.read_csv(f) if f.endswith(".csv") else pd.read_parquet(f)
            # 예시: 타자 기준 PA, HR 등 (실제 컬럼명에 맞춰 수정)
            cols=[c for c in df.columns]
            # 안전 최소치: player_uid, season 추정
            if "player_uid" in cols and "season" in cols:
                dfs.append(df[["player_uid","season"]].assign(statcast_seen=1))
        except Exception:
            pass
    if not dfs:
        return None
    out=pd.concat(dfs, ignore_index=True).drop_duplicates()
    return out.groupby(["player_uid","season"], as_index=False).agg({"statcast_seen":"sum"})

def enrich_year(y:int):
    cards=f"output/player_cards_{y}.csv"
    if not os.path.exists(cards):
        return f"[SKIP] {y}: no cards"
    files=find_statcast_csv(y)
    if not files:
        return f"[SKIP] {y}: no statcast files"
    sc=load_statcast_agg(files)
    if sc is None:
        return f"[SKIP] {y}: statcast parse failed"
    df=pd.read_csv(cards)
    if "player_uid" not in df.columns or "season" not in df.columns:
        return f"[ERR] {y}: cards schema missing keys"
    df=df.merge(sc, on=["player_uid","season"], how="left")
    df.to_csv(cards, index=False)
    return f"[OK] {y}: enriched with statcast (rows={len(df)})"

logs=[]
FROM=int(os.environ.get("FROM","2008")); TO=int(os.environ.get("TO","2025"))
for y in range(FROM, TO+1):
    logs.append(enrich_year(y))
print("\n".join(logs))
