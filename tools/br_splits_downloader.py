#!/usr/bin/env python3
import os, time, random, argparse, pandas as pd, requests
BASE = "https://www.baseball-reference.com/leagues/majors"
OUTDIR = "data/splits"
def fetch_table(url: str, user_agent: str, min_rows: int = 50, tries: int = 4, sleep=(5,12)) -> pd.DataFrame:
    HDRS = {"User-Agent": user_agent}
    for attempt in range(1, tries+1):
        try:
            r = requests.get(url, headers=HDRS, timeout=45)
            r.raise_for_status()
            html = r.text.replace("<!--", "").replace("-->", "")
            tables = pd.read_html(html)
            if not tables:
                raise ValueError("No tables parsed")
            df = max(tables, key=lambda x: x.shape[0])
            if df.shape[0] < min_rows:
                raise ValueError(f"Too few rows: {df.shape[0]}")
            if "Player" in df.columns and "Name" not in df.columns: df = df.rename(columns={"Player":"Name"})
            if "Tm" in df.columns: df = df.rename(columns={"Tm":"Team"})
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [" ".join([str(l) for l in col if str(l)!='nan']).strip() for col in df.columns.values]
            return df
        except Exception as e:
            if attempt == tries: raise
            time.sleep(random.uniform(*sleep))
def save_csv(df: pd.DataFrame, season: int, kind: str) -> str:
    os.makedirs(OUTDIR, exist_ok=True)
    out = os.path.join(OUTDIR, f"br_{season}_{'bat' if kind=='batting' else 'pit'}.csv")
    if "Season" not in df.columns: df.insert(0, "Season", season)
    else: df["Season"] = season
    df.to_csv(out, index=False); return out
def run(year_start: int, year_end: int, user_agent: str, pause=(6,12)):
    sess = [("batting","splits-batting.shtml"), ("pitching","splits-pitching.shtml")]
    rows = []
    for y in range(year_start, year_end+1):
        for kind, suffix in sess:
            url = f"{BASE}/{y}-{suffix}"
            status, path, nrows, ncols, err = "OK", "", 0, 0, ""
            try:
                df = fetch_table(url, user_agent)
                path = save_csv(df, y, kind)
                nrows, ncols = df.shape
            except Exception as e:
                status, err, path = "FAIL", str(e), url
            rows.append({"season":y,"kind":kind,"status":status,"path_or_url":path,"rows":nrows,"cols":ncols,"err":err})
            time.sleep(random.uniform(*pause))
    rep = pd.DataFrame(rows); rep.to_csv(os.path.join(OUTDIR,"br_download_report.csv"), index=False); print(rep.to_string(index=False))
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, required=True)
    ap.add_argument("--end", type=int, required=True)
    ap.add_argument("--ua", type=str, required=True)
    ap.add_argument("--pause-min", type=float, default=6.0)
    ap.add_argument("--pause-max", type=float, default=12.0)
    args = ap.parse_args()
    run(args.start, args.end, args.ua, pause=(args.pause_min, args.pause_max))
