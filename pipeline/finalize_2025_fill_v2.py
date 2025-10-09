#!/usr/bin/env python
import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path("/workspaces/cogm-assistant")
OUT  = ROOT/"output"

def clip01(x):
    return pd.to_numeric(x, errors="coerce").fillna(0).clip(0, 1)

# ---------- PLUS_BAT: batter / vs_hand 및 비율/카운트 결측 제거 ----------
pb_path = OUT/"statcast_pitch_mix_detailed_plus_bat.csv"
pb = pd.read_csv(pb_path, low_memory=False)
pb25 = pb[pb["year"] == 2025].copy()

# 카운트 컬럼(없으면 0) → 숫자화
count_cols = ["pitches","Z_Pitches","O_Pitches","Z_Swings","O_Swings","Z_Whiffs","O_Whiffs","CS",
              "edge_cnt","heart_cnt","chase_cnt","group_total"]
for c in count_cols:
    if c not in pb25.columns:
        pb25[c] = 0
    pb25[c] = pd.to_numeric(pb25[c], errors="coerce").fillna(0)

# 비율 컬럼(없으면 0) → [0,1] 클립
rate_cols = ["usage_rate","zone_rate","z_swing_rate","o_swing_rate",
             "z_contact_rate","o_contact_rate","z_csw_rate","csw_rate",
             "edge_rate","heart_rate","chase_rate","z_whiff_rate","o_whiff_rate"]
for c in rate_cols:
    if c not in pb25.columns:
        pb25[c] = 0
    pb25[c] = clip01(pb25[c])

# batter / vs_hand 채움(텍스트 결측 방지)
# mlbam이 float로 들어온 케이스를 안전하게 정수문자열로 변환
pb25["batter"]  = pd.to_numeric(pb25["mlbam"], errors="coerce").astype("Int64").astype(str).str.replace("<NA>","", regex=False)
pb25["vs_hand"] = pb25.get("vhb", "").astype(str).replace({"": "vs?"})

# 재결합 저장
pb_out = pd.concat([pb[pb["year"] != 2025], pb25], ignore_index=True)
pb_out.to_csv(pb_path, index=False)
print(f"[finalize] plus_bat filled: rows_2025={len(pb25)}")

# ---------- TENDENCIES: batter / count / swing_rate / whiff_rate / csw_rate 채움 ----------
td_path = OUT/"count_tendencies_bat.csv"
td = pd.read_csv(td_path, low_memory=False)
td25 = td[td["year"] == 2025].copy()

# plus_bat(2025)에서 집계 추출
agg = pb25.groupby(["year","mlbam","vhb"], dropna=False).agg(
    Z_Swings=("Z_Swings","sum"),
    O_Swings=("O_Swings","sum"),
    Z_Whiffs=("Z_Whiffs","sum"),
    O_Whiffs=("O_Whiffs","sum"),
    CS=("CS","sum"),
    pitches=("pitches","sum"),
).reset_index()
agg["swings"]   = agg["Z_Swings"] + agg["O_Swings"]
agg["whiffs"]   = agg["Z_Whiffs"] + agg["O_Whiffs"]
agg["csw_num"]  = agg["CS"] + agg["whiffs"]

# 방어적 비율 계산
def safe_div(n, d):
    n = pd.to_numeric(n, errors="coerce").fillna(0)
    d = pd.to_numeric(d, errors="coerce").fillna(0)
    with np.errstate(divide="ignore", invalid="ignore"):
        r = n / d
    r = r.replace([np.inf, -np.inf], 0).fillna(0).clip(0,1)
    return r

agg["swing_rate_calc"] = safe_div(agg["swings"], agg["pitches"])
agg["whiff_rate_calc"] = safe_div(agg["whiffs"], agg["swings"])
agg["csw_rate_calc"]   = safe_div(agg["csw_num"], agg["pitches"])

# 열 충돌 방지 위해 suffix 사용
td25 = td25.merge(
    agg.rename(columns={"vhb":"vhb_agg", "mlbam":"mlbam_agg"}),
    left_on=["year","mlbam","vhb"], right_on=["year","mlbam_agg","vhb_agg"],
    how="left"
)

# pitches가 없거나 결측이면 agg의 pitches 사용
if "pitches" not in td25.columns:
    td25["pitches"] = td25["pitches"].fillna(0) if "pitches" in td25.columns else 0
td25["pitches"] = pd.to_numeric(td25["pitches"], errors="coerce").fillna(0)
td25["pitches"] = np.where(td25["pitches"].eq(0) & td25["pitches"].notna(),
                           td25["pitches"], td25["pitches"])
td25["pitches"] = np.where(td25["pitches"].eq(0) & td25["pitches"].notna() & td25["pitches"].values==0,
                           td25["pitches"], td25["pitches"])  # no-op but keeps symmetry
# 실제로는 0이거나 결측이면 agg로 대체
td25.loc[td25["pitches"].eq(0), "pitches"] = pd.to_numeric(td25.get("pitches_y", td25.get("pitches", 0)), errors="coerce").fillna(
    pd.to_numeric(td25.get("pitches_agg", 0), errors="coerce")
).fillna(0)
# 혹시 위에서 못 채우면 최후로 merge된 agg의 'pitches' 사용
if "pitches_y" in td25.columns:
    td25.loc[td25["pitches"].eq(0), "pitches"] = td25["pitches_y"].fillna(0)
if "pitches_agg" in td25.columns:
    td25.loc[td25["pitches"].eq(0), "pitches"] = td25["pitches_agg"].fillna(0)

# batter / count 채움
td25["batter"] = pd.to_numeric(td25["mlbam"], errors="coerce").astype("Int64").astype(str).str.replace("<NA>","", regex=False)
td25["count"]  = "all"

# swing_rate / whiff_rate / csw_rate 채움(없거나 NaN이면 calc로)
for c in ["swing_rate","whiff_rate","csw_rate"]:
    if c not in td25.columns:
        td25[c] = np.nan
td25["swing_rate"] = td25["swing_rate"].astype(float)
td25["whiff_rate"] = td25["whiff_rate"].astype(float)
td25["csw_rate"]   = td25["csw_rate"].astype(float)

td25["swing_rate"] = td25["swing_rate"].where(td25["swing_rate"].notna(), td25.get("swing_rate_calc"))
td25["whiff_rate"] = td25["whiff_rate"].where(td25["whiff_rate"].notna(), td25.get("whiff_rate_calc"))
td25["csw_rate"]   = td25["csw_rate"].where(td25["csw_rate"].notna(), td25.get("csw_rate_calc"))

# 비율 클립/결측 제거
td25["swing_rate"] = clip01(td25["swing_rate"])
td25["whiff_rate"] = clip01(td25["whiff_rate"])
td25["csw_rate"]   = clip01(td25["csw_rate"])

# 정리: 보조열 삭제
drop_cols = [c for c in ["mlbam_agg","vhb_agg","Z_Swings","O_Swings","Z_Whiffs","O_Whiffs","CS",
                         "swings","whiffs","csw_num","swing_rate_calc","whiff_rate_calc","csw_rate_calc",
                         "pitches_y","pitches_agg"] if c in td25.columns]
td25 = td25.drop(columns=drop_cols)

# 재결합 저장
td_out = pd.concat([td[td["year"] != 2025], td25], ignore_index=True)
td_out.to_csv(td_path, index=False)
print(f"[finalize] tendencies filled: rows_2025={len(td25)}")

print("[done] finalize_2025_fill_v2")
