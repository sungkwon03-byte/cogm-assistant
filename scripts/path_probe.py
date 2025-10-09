#!/usr/bin/env python3
import os, json, glob
import pandas as pd
from collections import Counter

CANDIDATE_DIRS = ["data","raw","downloads","input","inputs",".","/workspaces/cogm-assistant"]

def is_fg_csv(path:str)->bool:
    name = os.path.basename(path).lower()
    if not name.endswith(".csv"): return False
    if "fangraph" in name or "fg" in name: return True
    try:
        df = pd.read_csv(path, nrows=5)
        cols = {c.strip().lower() for c in df.columns}
        return (("name" in cols or "player" in cols) and "team" in cols)
    except Exception:
        return False

cands = []
for root in CANDIDATE_DIRS:
    if not os.path.isdir(root): continue
    for p in glob.glob(os.path.join(root,"**","*.csv"), recursive=True):
        if is_fg_csv(p): cands.append(os.path.abspath(p))

if not cands:
    raise SystemExit("[-] Fangraphs CSV 후보를 찾지 못했습니다. CSV를 프로젝트 내부에 넣고 다시 실행하세요.")

dir_counts = Counter(os.path.dirname(p) for p in cands)
fg_dir = max(dir_counts, key=dir_counts.get)

os.makedirs("data", exist_ok=True)
link_path = os.path.abspath(os.path.join("data","fangraphs"))

if os.path.islink(link_path) or os.path.isdir(link_path):
    pass
else:
    try:
        os.symlink(fg_dir, link_path)
    except OSError:
        # 심볼릭 링크 불가 환경이면 폴더만 보장
        os.makedirs(link_path, exist_ok=True)

cache = {"fg_dir": fg_dir, "mart_dir": os.path.abspath("mart")}
with open("pathcache.json","w",encoding="utf-8") as f:
    json.dump(cache, f, ensure_ascii=False, indent=2)

print(f"[OK] fg_dir = {fg_dir}")
print(f"[OK] data/fangraphs -> {fg_dir}")
print(f"[OK] cache written: pathcache.json")
