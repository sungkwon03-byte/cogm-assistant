import glob, pandas as pd
from pathlib import Path
out=Path("output"); shards=Path("output/shards")
def merge(pattern, out_fp, subset):
    files=sorted(glob.glob(str(shards/"*"/pattern)))
    if not files: print("[WARN] no shards for", pattern); return
    df=pd.concat([pd.read_csv(f, dtype=str, low_memory=True) for f in files], ignore_index=True)
    before=len(df); df=df.drop_duplicates(subset=subset, keep="last"); df.to_csv(out/out_fp, index=False)
    print(f"[MERGE] {pattern} -> {out_fp} {before}->{len(df)}")
merge("games.csv","games.csv",["game_pk"])
merge("team_box.csv","team_box.csv",["game_pk","team"])
merge("player_box.csv","player_box.csv",["game_pk","team","mlb_id","name"])
