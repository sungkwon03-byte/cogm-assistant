import os, glob, polars as pl, duckdb, datetime as dt

LOG = "logs/statcast_merge_full.log"
os.makedirs("logs", exist_ok=True)
os.makedirs("output", exist_ok=True)
with open(LOG, "w") as f: f.write(f"[{dt.datetime.utcnow()}] Statcast full merge start\n")

STATCAST_CAND = [
    "/workspaces/cogm-assistant/output/cache/statcast_clean",
    "/workspaces/cogm-assistant/output"
]
OUT_FULL = "output/statcast_full_merged.parquet"
OUT_CARDS = "output/player_cards_enriched.csv"

def safe_concat(dfs):
    if not dfs: return None
    return pl.concat(dfs, how="diagonal_relaxed")

def find_all_statcast_files():
    exts = (".parquet", ".csv")
    found = []
    for root in STATCAST_CAND:
        for ext in exts:
            found += glob.glob(os.path.join(root, f"**/*statcast*{ext}"), recursive=True)
            found += glob.glob(os.path.join(root, f"{ext.strip('.')}"), recursive=True)
    found = [f for f in found if os.path.isfile(f)]
    with open(LOG, "a") as f: f.write(f"[FILES] {len(found)} found\n")
    return sorted(set(found))

def read_any(path):
    try:
        if path.endswith(".parquet"):
            return pl.read_parquet(path)
        else:
            return pl.read_csv(path, low_memory=True)
    except Exception as e:
        with open(LOG,"a") as f: f.write(f"[SKIP] {path}: {e}\n")
        return None

def merge_statcast():
    files = find_all_statcast_files()
    dfs=[]
    for i,f in enumerate(files,1):
        df = read_any(f)
        if df is not None:
            dfs.append(df)
        if i % 10 == 0:
            with open(LOG,"a") as g: g.write(f"  merged {i}/{len(files)} files\n")
    all_df = safe_concat(dfs)
    if all_df is None:
        raise SystemExit("❌ No valid statcast files found.")
    all_df.write_parquet(OUT_FULL)
    with open(LOG,"a") as f: f.write(f"[OK] wrote {OUT_FULL} rows={all_df.shape[0]} cols={len(all_df.columns)}\n")
    return all_df

def enrich_player_cards(statcast_df: pl.DataFrame):
    cards_path = "output/player_cards.csv"
    if not os.path.exists(cards_path):
        with open(LOG,"a") as f: f.write("[WARN] player_cards.csv not found, skipping enrichment\n")
        return
    cards = pl.read_csv(cards_path)
    join_key = "player_name" if "player_name" in statcast_df.columns else "name"
    if join_key not in cards.columns:
        cards = cards.rename({"name": join_key})
    joined = cards.join(statcast_df, on=join_key, how="left")
    joined.write_csv(OUT_CARDS)
    with open(LOG,"a") as f: f.write(f"[OK] enriched player_cards -> {OUT_CARDS} rows={joined.shape[0]} cols={len(joined.columns)}\n")

def main():
    df = merge_statcast()
    enrich_player_cards(df)
    with open(LOG,"a") as f: f.write(f"[DONE] completed at {dt.datetime.utcnow()}\n")

if __name__=="__main__":
    main()
