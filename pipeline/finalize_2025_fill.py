#!/usr/bin/env python
import pandas as pd
from pathlib import Path

ROOT = Path("/workspaces/cogm-assistant")
OUT  = ROOT/"output"

def ratio(n,d):
    n=pd.to_numeric(n,errors="coerce"); d=pd.to_numeric(d,errors="coerce")
    r=n/d
    return r.clip(lower=0,upper=1)

# ---- plus_bat: batter / vs_hand 채움 ----
pb_path = OUT/"statcast_pitch_mix_detailed_plus_bat.csv"
pb = pd.read_csv(pb_path, low_memory=False)

# 2025 파티션만 가공
pb25 = pb[pb["year"]==2025].copy()

# 필수 카운트 컬럼 보정(없으면 0)
for c in ["Z_Swings","O_Swings","Z_Whiffs","O_Whiffs","pitches"]:
    if c not in pb25.columns: pb25[c]=0

# 결측 0 채움
count_cols = ["Z_Pitches","O_Pitches","Z_Swings","O_Swings","Z_Whiffs","O_Whiffs","CS","edge_cnt","heart_cnt","chase_cnt","group_total","pitches"]
for c in count_cols:
    if c in pb25.columns:
        pb25[c] = pd.to_numeric(pb25[c], errors="coerce").fillna(0)

# 배터/손잡이 텍스트 채우기
pb25["batter"]   = pb25["mlbam"]
pb25["vs_hand"]  = pb25.get("vhb","")

# 숫자 비율 결측 0
rate_cols = ["usage_rate","zone_rate","z_swing_rate","o_swing_rate","z_contact_rate","o_contact_rate","z_csw_rate","csw_rate","edge_rate","heart_rate","chase_rate","z_whiff_rate","o_whiff_rate"]
for c in rate_cols:
    if c in pb25.columns:
        pb25[c] = pd.to_numeric(pb25[c], errors="coerce").fillna(0).clip(0,1)

# 병합 저장
pb_out = pd.concat([pb[pb["year"]!=2025], pb25], ignore_index=True)
pb_out.to_csv(pb_path, index=False)
print(f"[finalize] plus_bat filled: rows_2025={len(pb25)}")

# ---- tendencies: batter / count / swing_rate / whiff_rate 채움 ----
td_path = OUT/"count_tendencies_bat.csv"
td = pd.read_csv(td_path, low_memory=False)

td25 = td[td["year"]==2025].copy()
# plus_bat 2025에서 스윙/휘프 집계 추출
agg = pb25.groupby(["year","mlbam","vhb"], dropna=False).agg(
    Z_Swings=("Z_Swings","sum"),
    O_Swings=("O_Swings","sum"),
    Z_Whiffs=("Z_Whiffs","sum"),
    O_Whiffs=("O_Whiffs","sum"),
    pitches =("pitches","sum"),
).reset_index()

agg["swings"]     = pd.to_numeric(agg["Z_Swings"],errors="coerce").fillna(0) + pd.to_numeric(agg["O_Swings"],errors="coerce").fillna(0)
agg["whiffs_tot"] = pd.to_numeric(agg["Z_Whiffs"],errors="coerce").fillna(0) + pd.to_numeric(agg["O_Whiffs"],errors="coerce").fillna(0)

fill = agg[["year","mlbam","vhb","pitches","swings","whiffs_tot"]]

td25 = td25.merge(fill, on=["year","mlbam","vhb"], how="left")
td25["batter"]     = td25["mlbam"]
td25["count"]      = "all"
td25["swing_rate"] = ratio(td25["swings"], td25["pitches"]).fillna(0)
td25["whiff_rate"] = ratio(td25["whiffs_tot"], td25["swings"]).fillna(0)

# 불필요 중간열 제거
td25 = td25.drop(columns=[c for c in ["swings","whiffs_tot"] if c in td25.columns])

td_out = pd.concat([td[td["year"]!=2025], td25], ignore_index=True)
td_out.to_csv(td_path, index=False)
print(f"[finalize] tendencies filled: rows_2025={len(td25)}")
