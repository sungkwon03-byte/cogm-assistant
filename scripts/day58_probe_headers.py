#!/usr/bin/env python3
import json, pandas as pd, os
p="logs/day58_paths.json"
assert os.path.exists(p), "run probe_paths first"
paths=json.load(open(p,encoding="utf-8"))
def head(fp, n=0):
  try:
    df=pd.read_csv(fp, nrows=n, low_memory=False)
    print(f"[OK] {fp} cols={list(df.columns)[:20]}")
  except Exception as e:
    print(f"[ERR] {fp} -> {e}")
for k,v in paths.items():
  for fp in v:
    head(fp, n=0)
