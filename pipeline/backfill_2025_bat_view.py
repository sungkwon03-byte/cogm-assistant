#!/usr/bin/env python
# 2025 bat 뷰: Z/O 카운트 + Heart/Edge + CSW/Chase/컨택 전부 채움 → plus_bat & tendencies 교체
import os, glob
from pathlib import Path
import pandas as pd
from pandas.errors import EmptyDataError, ParserError

ROOT=Path("/workspaces/cogm-assistant"); OUT=ROOT/"output"
CACHE_DIRS=[OUT/"cache"/"statcast", OUT/"cache"/"statcast_clean"]

def safe_read(p):
    try:
        if os.path.getsize(p)==0: return None
        return pd.read_csv(p, low_memory=False)
    except (FileNotFoundError, EmptyDataError, ParserError, UnicodeDecodeError, OSError):
        return None

def ratio(n,d):
    n=pd.to_numeric(n,errors="coerce"); d=pd.to_numeric(d,errors="coerce")
    r=n/d; return r.clip(lower=0,upper=1)

# 1) 2025 원천 로드 + 파생 플래그
files=[]
for d in CACHE_DIRS:
    if d.exists(): files+=sorted(glob.glob(str(d/"*2025*.csv")))
parts=[]
for fp in files:
    df=safe_read(fp)
    if df is None or df.empty: continue

    low={c.lower():c for c in df.columns}
    def pick(n): return n if n in df.columns else low.get(n, None)
    ren={}
    for k in ["pitch_type","batter","pitcher","stand","game_date","description",
              "plate_x","plate_z","sz_top","sz_bot",
              "called_strike","swinging_strike","foul","foul_tip","foul_bunt","hit_into_play"]:
        c=pick(k)
        if c and c!=k: ren[c]=k
    if ren: df=df.rename(columns=ren)

    df["year"]=pd.to_datetime(df.get("game_date"),errors="coerce").dt.year if "game_date" in df.columns else 2025
    df=df[df["year"]==2025]
    if df.empty: continue

    desc=df.get("description","").astype(str)

    def coalesce_bool(s,pat=None):
        try:
            ss=s.astype(bool)
            return ss.fillna(False)
        except Exception:
            if pat: return desc.str.contains(pat,case=False,na=False)
            return pd.Series([False]*len(df))

    is_whiff=coalesce_bool(df.get("swinging_strike"),"swinging_strike")
    is_cs   =coalesce_bool(df.get("called_strike"),"called_strike")
    is_swing=(coalesce_bool(df.get("foul"),"foul") |
              coalesce_bool(df.get("foul_tip"),"foul_tip") |
              coalesce_bool(df.get("foul_bunt"),"foul_bunt") |
              coalesce_bool(df.get("hit_into_play"),"hit_into_play") |
              is_whiff)

    # 좌표 → Z/O + Heart/Edge
    px=pd.to_numeric(df.get("plate_x"),errors="coerce")
    pz=pd.to_numeric(df.get("plate_z"),errors="coerce")
    top=pd.to_numeric(df.get("sz_top"),errors="coerce")
    bot=pd.to_numeric(df.get("sz_bot"),errors="coerce")
    in_zone=(px.abs()<=0.7083) & (pz>=bot) & (pz<=top)
    in_zone=in_zone.fillna(False)

    mid=(top+bot)/2.0
    band=(top-bot)*0.25                     # 중앙 50% 높이 → heart
    heart_flag=((px.abs()<=0.5) & (pz.between(mid-band, mid+band, inclusive="both"))).fillna(False)
    edge_flag=(in_zone & (~heart_flag)).fillna(False)

    # 카운트 파생
    z_cs    =(in_zone & is_cs).astype(int)
    z_pitch = in_zone.astype(int)
    o_pitch =(~in_zone).astype(int)
    z_swing = (in_zone & is_swing).astype(int)
    o_swing = ((~in_zone) & is_swing).astype(int)
    z_whiff = (in_zone & is_whiff).astype(int)
    o_whiff = ((~in_zone) & is_whiff).astype(int)
    heart   = heart_flag.astype(int)
    edge    = edge_flag.astype(int)

    vhb=df.get("stand").map({"R":"vsR","L":"vsL"}) if "stand" in df.columns else ""
    parts.append(pd.DataFrame({
        "year":df["year"], "batter":df.get("batter"), "pitch_type":df.get("pitch_type"), "vhb":vhb.fillna(""),
        "z_pitch":z_pitch, "o_pitch":o_pitch, "z_swing":z_swing, "o_swing":o_swing,
        "z_whiff":z_whiff, "o_whiff":o_whiff, "z_cs":z_cs,
        "heart_cnt":heart, "edge_cnt":edge
    }))

if not parts: raise SystemExit("[err] 2025 원천 없음")
raw=pd.concat(parts, ignore_index=True)

# 2) 집계(batter,pitch_type,vhb)
gb=raw.groupby(["year","batter","pitch_type","vhb"], dropna=False)
size=gb.size().rename("pitches").reset_index()
sumc=gb[["z_pitch","o_pitch","z_swing","o_swing","z_whiff","o_whiff","z_cs","heart_cnt","edge_cnt"]].sum(min_count=1).reset_index()
bt=size.merge(sumc, on=["year","batter","pitch_type","vhb"], how="left")

# 3) 지표 계산(모든 비율 0~1 클립, 결측 0)
bt["usage_rate"]     = bt.groupby(["batter","year"])["pitches"].transform(lambda s: (s/s.sum()).clip(0,1)).fillna(0)
bt["zone_rate"]      = ratio(bt["z_pitch"], bt["pitches"]).fillna(0)
bt["z_swing_rate"]   = ratio(bt["z_swing"], bt["z_pitch"]).fillna(0)
bt["o_swing_rate"]   = ratio(bt["o_swing"], bt["o_pitch"]).fillna(0)
bt["z_whiff_rate"]   = ratio(bt["z_whiff"], bt["z_swing"]).fillna(0)
bt["o_whiff_rate"]   = ratio(bt["o_whiff"], bt["o_swing"]).fillna(0)
bt["z_contact_rate"] = (1 - bt["z_whiff_rate"]).clip(0,1).fillna(0)
bt["o_contact_rate"] = (1 - bt["o_whiff_rate"]).clip(0,1).fillna(0)
bt["z_csw_rate"]     = ratio(bt["z_cs"] + bt["z_whiff"], bt["z_pitch"]).fillna(0)
bt["csw_rate"]       = ratio(bt["z_cs"] + bt["z_whiff"] + (bt["o_whiff"]), bt["pitches"]).fillna(0)
bt["chase_rate"]     = ratio(bt["o_swing"], bt["o_pitch"]).fillna(0)
bt["edge_rate"]      = ratio(bt["edge_cnt"], bt["pitches"]).fillna(0)
bt["heart_rate"]     = ratio(bt["heart_cnt"], bt["pitches"]).fillna(0)
bt["segment"]        = "all"; bt["role"]="bat"

# 4) plus_bat 교체(2025)
cols_out=[
 "role","year","mlbam","pitch_type","segment","vhb","pitches",
 "Z_Pitches","O_Pitches","Z_Swings","O_Swings","Z_Whiffs","O_Whiffs","CS",
 "edge_cnt","heart_cnt","chase_cnt","group_total",
 "usage_rate","zone_rate","z_swing_rate","o_swing_rate",
 "z_contact_rate","o_contact_rate","z_csw_rate","csw_rate","edge_rate","heart_rate","chase_rate","z_whiff_rate"
]
bt_out = bt.rename(columns={
    "batter":"mlbam",
    "z_pitch":"Z_Pitches","o_pitch":"O_Pitches",
    "z_swing":"Z_Swings","o_swing":"O_Swings",
    "z_whiff":"Z_Whiffs","o_whiff":"O_Whiffs",
    "z_cs":"CS"
})
bt_out["chase_cnt"]  = bt["o_swing"]
bt_out["group_total"]= bt["pitches"]

path_pb = OUT/"statcast_pitch_mix_detailed_plus_bat.csv"
if path_pb.exists():
    base=pd.read_csv(path_pb,low_memory=False)
    base=base[base["year"]!=2025]
    out=pd.concat([base, bt_out[cols_out]], ignore_index=True)
else:
    out=bt_out[cols_out]
out.to_csv(path_pb,index=False)
print(f"[write] {path_pb.name}: 2025 rows={len(bt_out)}")

# 5) tendencies 교체(2025)
td = bt_out[["year","mlbam","vhb","pitches","zone_rate","z_swing_rate","o_swing_rate",
             "z_contact_rate","o_contact_rate","z_csw_rate","csw_rate","chase_rate","edge_rate","heart_rate"]]
td = td.groupby(["year","mlbam","vhb"], dropna=False).agg({
    "pitches":"sum",
    "zone_rate":"mean","z_swing_rate":"mean","o_swing_rate":"mean",
    "z_contact_rate":"mean","o_contact_rate":"mean",
    "z_csw_rate":"mean","csw_rate":"mean","chase_rate":"mean","edge_rate":"mean","heart_rate":"mean"
}).reset_index()
path_td = OUT/"count_tendencies_bat.csv"
if path_td.exists():
    base=pd.read_csv(path_td,low_memory=False)
    base=base[base["year"]!=2025]
    out=pd.concat([base, td], ignore_index=True)
else:
    out=td
out.to_csv(path_td,index=False)
print(f"[write] {path_td.name}: 2025 rows={len(td)}")

print("[done] backfill_2025_bat_view (edge/heart/chase_cnt/group_total filled)")
