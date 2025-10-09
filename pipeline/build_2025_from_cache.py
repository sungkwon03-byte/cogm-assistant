#!/usr/bin/env python
# 2025 캐시 샤드에서 최소 집계 → 주요 산출물에 2025 파티션 append
import os, glob
from pathlib import Path
import pandas as pd
from pandas.errors import EmptyDataError, ParserError

ROOT = Path("/workspaces/cogm-assistant")
OUT  = ROOT / "output"
CACHE_DIRS = [OUT / "cache" / "statcast", OUT / "cache" / "statcast_clean"]

# ---------- 유틸 ----------
def safe_read_csv(p):
    """빈 파일/헤더 없음/깨진 파일은 None 반환하고 스킵"""
    try:
        if os.path.getsize(p) == 0:
            return None
    except FileNotFoundError:
        return None
    try:
        return pd.read_csv(p, low_memory=False)
    except (EmptyDataError, ParserError, UnicodeDecodeError, OSError):
        return None

def safe_ratio(num, den):
    num = pd.to_numeric(num, errors="coerce")
    den = pd.to_numeric(den, errors="coerce")
    out = num / den
    return out.clip(lower=0, upper=1)

def append_csv(path: Path, df: pd.DataFrame, key_year_col="year"):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        base = pd.read_csv(path, low_memory=False)
        # 2025 이미 있으면 스킵
        if ((key_year_col in base.columns) and (base[key_year_col].astype(str)=="2025").any()) or \
           (("season" in base.columns) and (base["season"].astype(str)=="2025").any()):
            print(f"[skip] {path.name}: already has 2025")
            return
        out = pd.concat([base, df], ignore_index=True)
    else:
        out = df
    out.to_csv(path, index=False)
    print(f"[ok] {path.name}: appended {len(df)} rows")

# ---------- 0) 샤드 모으기 ----------
files = []
for d in CACHE_DIRS:
    if d.exists():
        files += sorted(glob.glob(str(d / "*2025*.csv")))
if not files:
    raise SystemExit("[err] 2025 캐시 CSV가 없습니다. output/cache/statcast/*2025*.csv 확인")

parts = []
for fp in files:
    df = safe_read_csv(fp)
    if df is None or df.empty:
        continue
    # 컬럼 소문자 맵
    low = {c.lower(): c for c in df.columns}
    def pick(name):  # 원하는 컬럼명이 없으면 유사(lower) 매핑
        if name in df.columns: return name
        return low.get(name, None)

    # 표준 컬럼 리네임
    rename = {}
    for k in ["pitch_type","batter","pitcher","stand","p_throws","description","game_date","player_name"]:
        c = pick(k)
        if c and c != k: rename[c] = k
    if rename: df = df.rename(columns=rename)

    # 연도 필터
    if "game_date" in df.columns:
        df["year"] = pd.to_datetime(df["game_date"], errors="coerce").dt.year
    else:
        df["year"] = 2025
    df = df[df["year"] == 2025]
    if df.empty: 
        continue

    # 스윙/미스 플래그(대략)
    if "description" in df.columns:
        d = df["description"].astype(str)
        df["is_swing"] = d.str.contains("swing", case=False, na=False)
        df["is_whiff"] = d.str.contains("swinging_strike|whiff", case=False, na=False)
    else:
        df["is_swing"] = pd.NA
        df["is_whiff"] = pd.NA

    if "batter" not in df.columns:  df["batter"]  = pd.NA
    if "pitcher" not in df.columns: df["pitcher"] = pd.NA
    if "pitch_type" not in df.columns: df["pitch_type"] = "FF"
    if "player_name" not in df.columns: df["player_name"] = ""

    parts.append(df)

if not parts:
    raise SystemExit("[err] 2025 유효 행이 없습니다(모든 샤드가 빈 파일/헤더 없음).")

raw = pd.concat(parts, ignore_index=True)

# ---------- 1) statcast_pitch_mix_detailed.csv (role=pit) ----------
grp = raw.groupby(["year","pitcher","pitch_type"], dropna=False)
pmix = grp.agg(
    pitches=("pitch_type","size"),
    swings=("is_swing","sum"),
    whiffs=("is_whiff","sum"),
).reset_index()
pmix["usage_rate"] = safe_ratio(pmix["pitches"], pmix["pitches"].sum())
pmix["whiff_rate"] = safe_ratio(pmix["whiffs"], pmix["swings"])
pmix_out = pmix.rename(columns={"pitcher":"mlbam"}).assign(
    role="pit", player_name="",
    zone_rate=pd.NA, z_whiff_rate=pd.NA, o_whiff_rate=pd.NA,
    chase_rate=pd.NA, csw_rate=pd.NA, avg_spin=pd.NA, avg_ext=pd.NA,
    h_mov_in=pd.NA, v_mov_in=pd.NA, segment="", edge_rate=pd.NA, heart_rate=pd.NA
)
pmix_out = pmix_out[[
 "year","role","mlbam","player_name","pitch_type","pitches",
 "usage_rate","zone_rate","whiff_rate","z_whiff_rate","o_whiff_rate",
 "chase_rate","csw_rate","avg_spin","avg_ext","h_mov_in","v_mov_in",
 "segment","edge_rate","heart_rate"
]]

# ---------- 2) statcast_pitch_mix_detailed_plus_bat.csv (role=bat) ----------
grp2 = raw.groupby(["year","batter","pitch_type"], dropna=False)
bat = grp2.agg(
    pitches=("pitch_type","size"),
    swings=("is_swing","sum"),
    whiffs=("is_whiff","sum"),
).reset_index()
bat["usage_rate"] = safe_ratio(bat["pitches"], bat["pitches"].sum())
bat_out = bat.rename(columns={"batter":"mlbam"}).assign(
    role="bat", segment="all", vhb="",
    zone_rate=pd.NA, z_swing_rate=pd.NA, o_swing_rate=pd.NA,
    z_contact_rate=pd.NA, o_contact_rate=pd.NA, z_csw_rate=pd.NA, csw_rate=pd.NA,
    edge_rate=pd.NA, heart_rate=pd.NA, chase_rate=pd.NA, z_whiff_rate=pd.NA,
    Z_Pitches=pd.NA, O_Pitches=pd.NA, Z_Swings=pd.NA, O_Swings=pd.NA,
    Z_Whiffs=pd.NA, O_Whiffs=pd.NA, CS=pd.NA, edge_cnt=pd.NA, heart_cnt=pd.NA,
    chase_cnt=pd.NA, group_total=pd.NA
)
bat_out = bat_out[[
 "role","year","mlbam","pitch_type","segment","vhb","pitches","Z_Pitches","O_Pitches",
 "Z_Swings","O_Swings","Z_Whiffs","O_Whiffs","CS","edge_cnt","heart_cnt","chase_cnt",
 "group_total","usage_rate","zone_rate","z_swing_rate","o_swing_rate",
 "z_contact_rate","o_contact_rate","z_csw_rate","csw_rate","edge_rate","heart_rate","chase_rate","z_whiff_rate"
]]

# ---------- 3) count_tendencies_bat.csv ----------
cnt = grp2.agg(pitches=("pitch_type","size")).reset_index()
cnt = cnt.rename(columns={"batter":"mlbam"}).assign(
    vhb="", zone_rate=pd.NA, z_swing_rate=pd.NA, o_swing_rate=pd.NA,
    z_contact_rate=pd.NA, o_contact_rate=pd.NA, z_csw_rate=pd.NA,
    chase_rate=pd.NA, edge_rate=pd.NA, heart_rate=pd.NA
)
cnt_out = cnt[["year","mlbam","vhb","pitches","zone_rate","z_swing_rate","o_swing_rate","z_contact_rate","o_contact_rate","z_csw_rate","chase_rate","edge_rate","heart_rate"]]

# ---------- 4) append ----------
def append(path, df, key_year_col="year"):
    append_csv(path, df, key_year_col)

append(OUT/"statcast_pitch_mix_detailed.csv", pmix_out, "year")
append(OUT/"statcast_pitch_mix_detailed_plus_bat.csv", bat_out, "year")
append(OUT/"count_tendencies_bat.csv", cnt_out, "year")

# 나머지는 기존 스켈레톤/기존 데이터 유지 (필요시 이후 정밀 반영)
print("[done] 2025 aggregation appended from cache.")
