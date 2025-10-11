import os, glob
import polars as pl

CANDIDATES = [
    "/workspaces/cogm-assistant/output",
    "/workspaces/cogm-assistant",
    "/workspaces/cogm-assistant/output/cache/statcast_clean"
]

def _glob_files(year:int, ext:str):
    patterns = [
        f"statcast*{year}*.{ext}",
        f"*/statcast*{year}*.{ext}",
        f"statcast_*_player_year.{ext}",
        f"statcast_*_agg.{ext}"
    ]
    files=[]
    for root in CANDIDATES:
        for patt in patterns:
            files += glob.glob(os.path.join(root, patt))
    return sorted(set(files))

def _safe_concat(dfs):
    # 스키마 달라도 자동 병합
    return pl.concat(dfs, how="diagonal_relaxed")

def read_year(year:int):
    pq = _glob_files(year, "parquet")
    if pq:
        print(f"[INFO] Found Parquet {len(pq)} for {year}")
        return _safe_concat([pl.read_parquet(f) for f in pq])
    csv = _glob_files(year, "csv")
    if csv:
        print(f"[INFO] Found CSV {len(csv)} for {year}")
        return _safe_concat([pl.read_csv(f, low_memory=True) for f in csv])
    raise FileNotFoundError(f"no statcast files found for {year}")

def read_range(year_from:int,year_to:int):
    dfs=[]
    for y in range(year_from,year_to+1):
        try:
            dfs.append(read_year(y))
        except FileNotFoundError:
            continue
    if not dfs:
        raise FileNotFoundError("no statcast data found anywhere")
    return _safe_concat(dfs)
