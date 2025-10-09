
import os, pandas as pd, numpy as np
from pathlib import Path

ROOT=Path.cwd(); OUT=ROOT/'output'; OUT.mkdir(parents=True, exist_ok=True)
base = ROOT/'output'/'cache'/'statcast_clean'
if not base.exists(): base = ROOT/'output'/'cache'/'statcast'
assert base.exists(), "statcast cache dir not found"

limit = int(os.getenv("STATCAST_MAX_FILES", "999999"))
files = sorted(base.glob("*.csv"))[:limit]

NEED = {
    "game_year","pitch_type","description","zone","plate_x","plate_z","sz_top","sz_bot",
    "balls","strikes","batter","pitcher","p_throws","stand",
    "release_spin_rate","release_extension","pfx_x","pfx_z"
}

def safe_div(n, d):
    n = pd.to_numeric(n, errors="coerce"); d = pd.to_numeric(d, errors="coerce")
    out = n / d
    return out.replace([np.inf,-np.inf], np.nan).fillna(0)

def seg_name(balls, strikes):
    try:
        b = int(balls); s = int(strikes)
    except Exception:
        return None
    if s == 2: return "two_strike"
    if b - s >= 1: return "ahead"
    if s - b >= 1: return "behind"
    return None

def band_from_zone(z):
    try:
        zi = int(z)
    except Exception:
        return ("out","chase")
    if 1 <= zi <= 9:
        return ("in", "heart" if zi==5 else "edge")
    return ("out","chase")

keep = []
for fp in files:
    try:
        df = pd.read_csv(fp, low_memory=False)
    except Exception:
        continue
    if df.empty: 
        continue
    # 필요한 컬럼 보정
    for c in NEED:
        if c not in df.columns: df[c] = np.nan
    df = df.rename(columns={"game_year":"year"})
    # 타입/가드
    desc = df["description"].astype(str)
    zone_n = pd.to_numeric(df["zone"], errors="coerce")
    in_zone = zone_n.between(1,9, inclusive="both")
    swing = (desc.str.contains("swing", na=False) |
             desc.str.contains("foul", na=False)  |
             desc.str.contains("hit_into_play", na=False))
    whiff = (desc.str.contains("swinging_strike", na=False) |
             desc.str.contains("swinging_strike_blocked", na=False))
    called = desc.str.contains("called_strike", na=False)
    contact = swing & (~whiff) & (desc.str.contains("foul", na=False) | desc.str.contains("hit_into_play", na=False))

    # 파생
    df["segment"] = [seg_name(b, s) for b,s in zip(df["balls"], df["strikes"])]
    df["vhb"] = np.where(df["stand"].astype(str).str.upper()=="L", "vsL", "vsR")
    inh, band = zip(*[band_from_zone(z) for z in zone_n.fillna(0)])
    df["inout"] = inh; df["band"] = band

    # 카운트
    df["Z_Pitches"] = in_zone.astype(int)
    df["O_Pitches"] = (~in_zone).astype(int)
    df["Swings"]    = swing.astype(int)
    df["Z_Swings"]  = (in_zone & swing).astype(int)
    df["O_Swings"]  = (~in_zone & swing).astype(int)
    df["Whiffs"]    = whiff.astype(int)
    df["Z_Whiffs"]  = (in_zone & whiff).astype(int)
    df["O_Whiffs"]  = (~in_zone & whiff).astype(int)
    df["Contacts"]  = contact.astype(int)
    df["Z_Contacts"]= (in_zone & contact).astype(int)
    df["O_Contacts"]= (~in_zone & contact).astype(int)
    df["Called"]    = called.astype(int)
    df["Z_Called"]  = (in_zone & called).astype(int)
    df["Edge"]      = (df["band"]=="edge").astype(int)
    df["Heart"]     = (df["band"]=="heart").astype(int)
    df["Chase"]     = (df["band"]=="chase").astype(int)

    # 투수-피치믹스 집계 (role='pit')
    grp = ["year","pitcher","pitch_type","segment","vhb"]
    agg = df.groupby(grp, dropna=False).agg(
        pitches=("pitch_type","count"),
        Z_Pitches=("Z_Pitches","sum"), O_Pitches=("O_Pitches","sum"),
        swings=("Swings","sum"), Z_Swings=("Z_Swings","sum"), O_Swings=("O_Swings","sum"),
        whiffs=("Whiffs","sum"), Z_Whiffs=("Z_Whiffs","sum"), O_Whiffs=("O_Whiffs","sum"),
        contacts=("Contacts","sum"), Z_Contacts=("Z_Contacts","sum"), O_Contacts=("O_Contacts","sum"),
        called=("Called","sum"), Z_Called=("Z_Called","sum"),
        edge=("Edge","sum"), heart=("Heart","sum"), chase=("Chase","sum"),
        avg_spin=("release_spin_rate","mean"),
        avg_ext=("release_extension","mean"),
        h_mov_in=("pfx_x","mean"),
        v_mov_in=("pfx_z","mean")
    ).reset_index()

    # 비율
    agg["usage_rate"]    = safe_div(agg["pitches"], agg.groupby(["year","pitcher"])["pitches"].transform("sum"))
    agg["zone_rate"]     = safe_div(agg["Z_Pitches"], agg["pitches"])
    agg["whiff_rate"]    = safe_div(agg["whiffs"], agg["swings"])
    agg["z_whiff_rate"]  = safe_div(agg["Z_Whiffs"], agg["Z_Swings"])
    agg["o_whiff_rate"]  = safe_div(agg["O_Whiffs"], agg["O_Swings"])
    agg["z_swing_rate"]  = safe_div(agg["Z_Swings"], agg["Z_Pitches"])
    agg["o_swing_rate"]  = safe_div(agg["O_Swings"], agg["O_Pitches"])
    agg["z_contact_rate"]= safe_div(agg["Z_Contacts"], agg["Z_Swings"])
    agg["o_contact_rate"]= safe_div(agg["O_Contacts"], agg["O_Swings"])
    agg["csw_rate"]      = safe_div(agg["called"] + agg["whiffs"], agg["pitches"])
    agg["z_csw_rate"]    = safe_div(agg["Z_Called"] + agg["Z_Whiffs"], agg["Z_Pitches"])
    agg["edge_rate"]     = safe_div(agg["edge"], agg["pitches"])
    agg["heart_rate"]    = safe_div(agg["heart"], agg["pitches"])
    agg["chase_rate"]    = safe_div(agg["chase"], agg["pitches"])

    agg = agg.rename(columns={"pitcher":"mlbam"})
    agg.insert(2, "role", "pit")
    keep.append(agg)

mix = pd.concat(keep, ignore_index=True) if keep else pd.DataFrame()
out = OUT/"statcast_pitch_mix_detailed_plus.csv"
mix.to_csv(out, index=False)
print(f"[PLUS] -> {out} rows={len(mix)}")
