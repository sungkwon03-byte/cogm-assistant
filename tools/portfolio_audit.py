#!/usr/bin/env python3
"""
목적: 코드가 아니라 '환경·파일' 때문에 다시 터지지 않도록
- 파일 존재/시그니처
- LFS 포인터 여부
- Cards/Statcast 로드 및 컬럼 매핑
- Player Search/Trade 간단 시뮬
- Matplotlib 테스트 렌더
를 한 번에 점검. 항상 0으로 끝남(예외는 리포트에만 기록).
"""
from pathlib import Path
import json, sys
import pandas as pd
import numpy as np

ROOT=Path("."); OUT=ROOT/"output"; REP=OUT/"reports"; SUM=OUT/"summaries"
report={"ok":True,"issues":[]}

def add_issue(msg):
    report["ok"]=False
    report["issues"].append(msg)

def sig_png(p:Path)->bool:
    try:
        with open(p,"rb") as f: return f.read(8)==b"\x89PNG\r\n\x1a\n"
    except: return False

def sig_pdf(p:Path)->bool:
    try:
        with open(p,"rb") as f: return f.read(4)==b"%PDF"
    except: return False

# 1) 시그니처 검사
need = {
    "platoon_csv": SUM/"platoon_split.csv",
    "platoon_png": REP/"platoon_map.png",
    "weak_csv":    SUM/"weakness_heatmap_matrix.csv",
    "weak_png":    REP/"weakness_heatmap.png",
    "trend_pdf":   REP/"trend_cards_3y.pdf",
    "euz_csv":     SUM/"euz_umpire_impact.csv",
    "euz_png":     REP/"ump_euz.png",
    "xai_png":     REP/"explainable_attribution_topN.png",
}
for k,p in need.items():
    if not p.exists() or p.stat().st_size==0:
        add_issue(f"missing artefact: {p}")
    elif p.suffix==".png" and not sig_png(p):
        add_issue(f"invalid PNG signature: {p}")
    elif p.suffix==".pdf" and not sig_pdf(p):
        add_issue(f"invalid PDF signature: {p}")

# 2) QC JSON 확인
qc = OUT/"full_system_validation.json"
if not qc.exists():
    add_issue("QC json missing: output/full_system_validation.json")
else:
    try:
        json.loads(qc.read_text())
    except Exception as e:
        add_issue(f"QC json parse error: {e}")

# 3) Cards / Statcast 로드
def read_any(p:Path):
    try:
        if p.suffix==".parquet": return pd.read_parquet(p)
        if p.suffix==".csv":     return pd.read_csv(p, low_memory=False)
    except: return None

cards=None
for p in [OUT/"player_cards_all.parquet", OUT/"player_cards_enriched_all_seq.parquet", OUT/"player_cards_ultra.csv", OUT/"player_cards_enriched_full.csv"]:
    if p.exists():
        cards=read_any(p)
        if cards is not None and len(cards)>0: break

stat=None
for p in [OUT/"statcast_ultra_full_clean.parquet", OUT/"statcast_ultra_full.parquet", OUT/"statcast_master_full.parquet"]:
    if p.exists():
        stat=read_any(p)
        if stat is not None and len(stat)>0: break

if cards is None: add_issue("cards not found under output/…")
if stat  is None: add_issue("statcast not found under output/…")

# 4) 컬럼 매핑 & 간단 시뮬
def name_columns(df):
    id_cands=["player_id","mlb_id","bat_id","pitcher_id","batter","id"]
    nm_cands=["player_name","name","full_name","mlb_name","Name"]
    pid=next((c for c in id_cands if c in df.columns),None)
    nm =next((c for c in nm_cands if c in df.columns),None)
    if pid is None: df["player_id"]=np.arange(len(df)); pid="player_id"
    if nm  is None: df["player_name"]=df[pid].astype(str); nm ="player_name"
    return pid,nm

if cards is not None:
    pid,nm=name_columns(cards)
    sample=cards.head(1)
    if not sample.empty and nm in sample.columns:
        key=str(sample[nm].iloc[0]).split(" ")[0]
        mask=cards[nm].astype(str).str.contains(key, case=False, na=False)
        _=cards.loc[mask].head(5)

# 5) Matplotlib 헤드리스 렌더 테스트
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig=plt.figure(figsize=(3,2)); plt.plot([1,2,3],[1,4,9]); plt.title("render ok"); fig.savefig(REP/"_render_probe.png"); plt.close(fig)
except Exception as e:
    add_issue(f"matplotlib render error: {e}")

# 6) 리포트 출력
print(json.dumps(report, indent=2))
sys.exit(0)
