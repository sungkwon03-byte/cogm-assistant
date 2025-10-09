#!/usr/bin/env python
# 2025 정밀 보강: Z/O, Edge/Heart, CSW, vs_hand 등 계산하여 기존 CSV의 2025행을 교체
import os, glob
from pathlib import Path
import pandas as pd
from pandas.errors import EmptyDataError, ParserError

ROOT = Path("/workspaces/cogm-assistant")
OUT  = ROOT / "output"
CACHE_DIRS = [OUT / "cache" / "statcast", OUT / "cache" / "statcast_clean"]

def safe_read(p):
    try:
        if os.path.getsize(p) == 0:
            return None
        return pd.read_csv(p, low_memory=False)
    except (FileNotFoundError, EmptyDataError, ParserError, UnicodeDecodeError, OSError):
        return None

def ratio(num, den):
    n = pd.to_numeric(num, errors="coerce")
    d = pd.to_numeric(den, errors="coerce")
    r = n / d
    return r.clip(lower=0, upper=1)

def load_raw_2025():
    files=[]
    for d in CACHE_DIRS:
        if d.exists(): files += sorted(glob.glob(str(d/"*2025*.csv")))
    parts=[]
    for fp in files:
        df = safe_read(fp)
        if df is None or df.empty: continue

        # 표준화
        low = {c.lower(): c for c in df.columns}
        def pick(name):
            if name in df.columns: return name
            return low.get(name, None)
        rename = {}
        for k in ["pitch_type","batter","pitcher","stand","p_throws","description","game_date",
                  "player_name","plate_x","plate_z","sz_top","sz_bot",
                  "called_strike","swinging_strike","foul","foul_tip","foul_bunt",
                  "hit_into_play"]:
            c = pick(k)
            if c and c != k: rename[c] = k
        if rename: df = df.rename(columns=rename)

        # 연도 필터
        if "game_date" in df.columns:
            df["year"] = pd.to_datetime(df["game_date"], errors="coerce").dt.year
        else:
            df["year"] = 2025
        df = df[df["year"] == 2025]
        if df.empty: continue

        # 이벤트 플래그(명시 열 우선, 없으면 description 패턴)
        desc = df["description"].astype(str) if "description" in df.columns else pd.Series([""]*len(df))
        def coalesce_bool(series, pattern=None):
            try:
                s = series.astype(bool)
            except Exception:
                s = None
            if s is not None:
                return s.fillna(False)
            if pattern:
                return desc.str.contains(pattern, case=False, na=False)
            return pd.Series([False]*len(df))
        is_swing = coalesce_bool(df.get("foul"), "foul") \
                 | coalesce_bool(df.get("foul_tip"), "foul_tip") \
                 | coalesce_bool(df.get("foul_bunt"), "foul_bunt") \
                 | coalesce_bool(df.get("hit_into_play"), "hit_into_play") \
                 | coalesce_bool(df.get("swinging_strike"), "swinging_strike")
        is_whiff = coalesce_bool(df.get("swinging_strike"), "swinging_strike")
        is_cs    = coalesce_bool(df.get("called_strike"), "called_strike")

        # 좌표 기반 존 분류
        px  = pd.to_numeric(df.get("plate_x", pd.NA), errors="coerce")
        pz  = pd.to_numeric(df.get("plate_z", pd.NA), errors="coerce")
        top = pd.to_numeric(df.get("sz_top",  pd.NA), errors="coerce")
        bot = pd.to_numeric(df.get("sz_bot",  pd.NA), errors="coerce")
        half_w = 0.7083  # ft, 17인치의 절반
        in_x = px.abs() <= half_w
        in_z = (pz >= bot) & (pz <= top)
        in_zone = (in_x & in_z).fillna(False)

        mid = (top + bot) / 2.0
        band = (top - bot) * 0.25  # 중앙 50% 높이 → heart
        heart_flag = ((px.abs() <= 0.5) & (pz.between(mid-band, mid+band, inclusive="both"))).fillna(False)
        edge_flag  = (in_zone & (~heart_flag)).fillna(False)

        # vs_hand
        vhb = df.get("stand").map({"R":"vsR","L":"vsL"}) if "stand" in df.columns else pd.Series([""]*len(df))
        vhb = vhb.fillna("")

        # 기본 식별자 보정
        for c in ["batter","pitcher","pitch_type","player_name"]:
            if c not in df.columns:
                df[c] = "" if c=="player_name" else pd.NA

        # 파생 카운터(집계용)
        z_pitch = in_zone.astype(int)
        o_pitch = (~in_zone).astype(int)
        z_swing = (in_zone & is_swing).astype(int)
        o_swing = ((~in_zone) & is_swing).astype(int)
        z_whiff = (in_zone & is_whiff).astype(int)
        o_whiff = ((~in_zone) & is_whiff).astype(int)
        heart   = heart_flag.astype(int)
        edge    = edge_flag.astype(int)
        cs_called = is_cs.astype(int)
        swing_i = is_swing.astype(int)
        whiff_i = is_whiff.astype(int)

        df2 = pd.DataFrame({
            "year": df["year"].values,
            "pitch_type": df["pitch_type"].values,
            "batter": df["batter"].values,
            "pitcher": df["pitcher"].values,
            "player_name": df["player_name"].values if "player_name" in df.columns else [""]*len(df),
            "vhb": vhb.values,
            "z_pitch": z_pitch.values, "o_pitch": o_pitch.values,
            "z_swing": z_swing.values, "o_swing": o_swing.values,
            "z_whiff": z_whiff.values, "o_whiff": o_whiff.values,
            "heart_cnt": heart.values, "edge_cnt": edge.values,
            "cs_called": cs_called.values,
            "swing": swing_i.values, "whiff": whiff_i.values
        })
        parts.append(df2)

    if not parts:
        raise SystemExit("[err] 2025 원천이 비어있습니다.")
    return pd.concat(parts, ignore_index=True)

def replace_2025(path: Path, df_new: pd.DataFrame):
    if path.exists():
        base = pd.read_csv(path, low_memory=False)
        if "year" in base.columns:   base = base[base["year"] != 2025]
        if "season" in base.columns: base = base[base["season"] != 2025]
        out = pd.concat([base, df_new], ignore_index=True)
    else:
        out = df_new
    out.to_csv(path, index=False)
    print(f"[write] {path.name}: 2025 rows={len(df_new)}")

# === Load ===
raw = load_raw_2025()

# === Pitcher view: statcast_pitch_mix_detailed.csv ===
g = raw.groupby(["year","pitcher","pitch_type"], dropna=False)
pm_size = g.size().rename("pitches").reset_index()
pm_sum  = g[["swing","whiff","cs_called","z_pitch","z_swing","z_whiff","o_pitch","o_swing","o_whiff","heart_cnt","edge_cnt"]].sum(min_count=1).reset_index()
pm = pm_size.merge(pm_sum, on=["year","pitcher","pitch_type"], how="left")

pm["usage_rate"]   = pm.groupby(["pitcher","year"])["pitches"].transform(lambda s: (s/s.sum()).clip(0,1)).fillna(0)
pm["whiff_rate"]   = ratio(pm["whiff"], pm["swing"]).fillna(0)
pm["zone_rate"]    = ratio(pm["z_pitch"], pm["pitches"]).fillna(0)
pm["z_whiff_rate"] = ratio(pm["z_whiff"], pm["z_swing"]).fillna(0)
pm["o_whiff_rate"] = ratio(pm["o_whiff"], pm["o_swing"]).fillna(0)
pm["chase_rate"]   = ratio(pm["o_swing"], pm["o_pitch"]).fillna(0)
pm["csw_rate"]     = ratio(pm["cs_called"] + pm["whiff"], pm["pitches"]).fillna(0)
pm["edge_rate"]    = ratio(pm["edge_cnt"], pm["pitches"]).fillna(0)
pm["heart_rate"]   = ratio(pm["heart_cnt"], pm["pitches"]).fillna(0)
pm["segment"]      = ""

pm_out = pm.rename(columns={"pitcher":"mlbam"}).assign(
    role="pit", player_name="", avg_spin=pd.NA, avg_ext=pd.NA, h_mov_in=pd.NA, v_mov_in=pd.NA
)[[
 "year","role","mlbam","player_name","pitch_type","pitches",
 "usage_rate","zone_rate","whiff_rate","z_whiff_rate","o_whiff_rate",
 "chase_rate","csw_rate","avg_spin","avg_ext","h_mov_in","v_mov_in",
 "segment","edge_rate","heart_rate"
]]

# === Batter view: statcast_pitch_mix_detailed_plus_bat.csv ===
gb = raw.groupby(["year","batter","pitch_type","vhb"], dropna=False)
bt_size = gb.size().rename("pitches").reset_index()
bt_sum  = gb[["z_pitch","swing","whiff","z_swing","z_whiff","heart_cnt","edge_cnt"]].sum(min_count=1).reset_index()
bt = bt_size.merge(bt_sum, on=["year","batter","pitch_type","vhb"], how="left")

bt["usage_rate"]     = bt.groupby(["batter","year"])["pitches"].transform(lambda s: (s/s.sum()).clip(0,1)).fillna(0)
bt["zone_rate"]      = ratio(bt["z_pitch"], bt["pitches"]).fillna(0)
bt["z_swing_rate"]   = ratio(bt["z_swing"], bt["z_pitch"]).fillna(0)
bt["o_swing_rate"]   = ratio(bt["swing"] - bt["z_swing"], bt["pitches"] - bt["z_pitch"]).fillna(0)
bt["z_whiff_rate"]   = ratio(bt["z_whiff"], bt["z_swing"]).fillna(0)
bt["z_contact_rate"] = (1 - bt["z_whiff_rate"]).clip(0,1).fillna(0)
bt["o_contact_rate"] = pd.NA
bt["z_csw_rate"]     = pd.NA
bt["csw_rate"]       = pd.NA
bt["edge_rate"]      = ratio(bt["edge_cnt"], bt["pitches"]).fillna(0)
bt["heart_rate"]     = ratio(bt["heart_cnt"], bt["pitches"]).fillna(0)
bt["chase_rate"]     = ratio(bt["swing"] - bt["z_swing"], bt["pitches"] - bt["z_pitch"]).fillna(0)
bt["segment"]        = "all"
bt["role"]           = "bat"  # ★ role 명시

# 헤더 유지용 보조 카운트(필요 시 later 계산)
for c in ["Z_Pitches","O_Pitches","Z_Swings","O_Swings","Z_Whiffs","O_Whiffs","CS","chase_cnt","group_total"]:
    bt[c] = pd.NA
# 기존 스키마와 정확히 같은 필드명으로 두기
bt = bt.rename(columns={"edge_cnt":"edge_cnt", "heart_cnt":"heart_cnt"})

bt_out = bt.rename(columns={"batter":"mlbam"})[[
 "role","year","mlbam","pitch_type","segment","vhb","pitches","Z_Pitches","O_Pitches",
 "Z_Swings","O_Swings","Z_Whiffs","O_Whiffs","CS","edge_cnt","heart_cnt","chase_cnt",
 "group_total","usage_rate","zone_rate","z_swing_rate","o_swing_rate",
 "z_contact_rate","o_contact_rate","z_csw_rate","csw_rate","edge_rate","heart_rate","chase_rate","z_whiff_rate"
]]

# === Tendencies(bat): count_tendencies_bat.csv ===
td = bt_out[["year","mlbam","vhb","pitches","zone_rate","z_swing_rate","o_swing_rate",
             "z_contact_rate","o_contact_rate","z_csw_rate","chase_rate","edge_rate","heart_rate"]]
td = td.groupby(["year","mlbam","vhb"], dropna=False).agg({
    "pitches":"sum",
    "zone_rate":"mean","z_swing_rate":"mean","o_swing_rate":"mean",
    "z_contact_rate":"mean","o_contact_rate":"mean",
    "z_csw_rate":"mean","chase_rate":"mean","edge_rate":"mean","heart_rate":"mean"
}).reset_index()

# === bat_stability(옵션): z_whiff_rate 분산 근사 ===
try:
    bs = bt.groupby(["batter","year"])["z_whiff_rate"].var().reset_index().rename(columns={"batter":"player_id"})
    bs["metric"] = "whiff_rate"
    bs = bs[["player_id","metric","z_whiff_rate"]].rename(columns={"z_whiff_rate":"rolling_var"})
except Exception:
    bs = pd.DataFrame(columns=["player_id","metric","rolling_var"])

# === Replace outputs ===
def replace(path, df): replace_2025(path, df)
replace(OUT/"statcast_pitch_mix_detailed.csv", pm_out)
replace(OUT/"statcast_pitch_mix_detailed_plus_bat.csv", bt_out)
replace(OUT/"count_tendencies_bat.csv", td)

if not bs.empty:
    path = OUT/"bat_stability.csv"
    if path.exists():
        base = pd.read_csv(path, low_memory=False)
        keep = ~((base.get("metric")=="whiff_rate") & (base.get("player_id").astype(str).isin(bs["player_id"].astype(str))))
        base = base[keep]
        out = pd.concat([base, bs], ignore_index=True)
    else:
        out = bs
    out.to_csv(path, index=False)
    print(f"[write] bat_stability.csv: +{len(bs)} rows (2025)")
else:
    print("[info] bat_stability skipped (no variance computed)")

print("[done] enrich_2025_metrics")
