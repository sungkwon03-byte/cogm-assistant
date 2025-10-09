#!/usr/bin/env python3
import os, re, glob, pandas as pd

def head(fn, n=3):
    try:
        df = pd.read_csv(fn, nrows=n, dtype=str, low_memory=False)
        return df
    except Exception as e:
        return f"[READ_FAIL] {e}"

def infer_schema(df):
    if isinstance(df, str):
        return df
    cols = [c.lower() for c in df.columns]
    if cols == ["404: not found".lower()]:
        return "[BAD] 404-not-found file (not CSV)"
    # 아주 대략적인 라벨
    if {"gid","id","date"}.issubset(cols):
        return "retrosheet-game"
    if {"playerid","yearid"}.issubset(cols):
        return "lahman-people-or-stats"
    if {"namefirst","namelast"}.intersection(cols):
        return "people-like"
    return f"unknown ({cols[:8]})"

def scan_people(path):
    df = head(path, n=5)
    print(f"[People] path={path} schema={infer_schema(df)}")
    if isinstance(df, str): return
    print("  columns:", list(df.columns)[:12])
    low = [c.lower() for c in df.columns]
    keys = [k for k in ["playerid","retroid","bbrefid","id"] if k in low]
    print("  id-keys:", keys or "NONE")

def scan_retro(path, kind):
    df = head(path, n=5)
    print(f"[{kind}] path={path} schema={infer_schema(df)}")
    if isinstance(df, str): return
    print("  columns:", list(df.columns)[:16])
    low = [c.lower() for c in df.columns]
    if "id" in low:
        sid = df[df.columns[low.index("id")]].astype(str)
        pat = sid.str.match(r"^[a-z]{5}\d{2}$", case=False, na=False).mean()
        print(f"  id looks like RetroID ratio={pat:.2f}")
    if "date" in low:
        yr = df[df.columns[low.index("date")]].astype(str).str.extract(r"^(\d{4})", expand=False)
        yr = yr.dropna()
        print("  sample years:", list(yr.unique())[:5])

def main():
    # paths from your environment
    people = "data/id/Lahman/People.csv"
    bat = "data/retrosheet/batting.csv"
    pit = "data/retrosheet/pitching.csv"
    for p in [people, bat, pit]:
        print("exists?", p, os.path.exists(p))
    if os.path.exists(people): scan_people(people)
    if os.path.exists(bat):    scan_retro(bat, "Retrosheet-Batting")
    if os.path.exists(pit):    scan_retro(pit, "Retrosheet-Pitching")

if __name__ == "__main__":
    main()
