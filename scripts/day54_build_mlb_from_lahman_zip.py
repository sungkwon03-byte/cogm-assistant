#!/usr/bin/env python3
import os, re, sys, glob, zipfile
import pandas as pd
import numpy as np

def ensure_dir(p): os.makedirs(p, exist_ok=True)
def to_num(s): return pd.to_numeric(s, errors="coerce").fillna(0)

def safe_div(a, b):
    a = pd.to_numeric(a, errors="coerce")
    b = pd.to_numeric(b, errors="coerce")
    out = np.divide(a, b, out=np.full(a.shape, np.nan, dtype=float), where=(b!=0))
    return pd.Series(out, index=a.index, dtype=float)

def find_lahman_root(arg):
    if arg:
        if os.path.isdir(arg): return arg
        if os.path.isfile(arg) and arg.lower().endswith(".zip"):
            root = "data/lahman_extracted"
            ensure_dir(root)
            with zipfile.ZipFile(arg) as z: z.extractall(root)
            return root
        sys.exit(f"[FATAL] Not a dir/zip: {arg}")
    zips = sorted(
        glob.glob("./**/*lahman*/*.zip", recursive=True) +
        glob.glob("./**/*lahman*.zip", recursive=True) +
        glob.glob("/mnt/data/**/*lahman*/*.zip", recursive=True) +
        glob.glob("/mnt/data/**/*lahman*.zip", recursive=True)
    )
    if zips:
        root = "data/lahman_extracted"
        ensure_dir(root)
        with zipfile.ZipFile(zips[-1]) as z: z.extractall(root)
        return root
    for cand in ["data/lahman_extracted","data/lahman","./lahman","./data/baseballdatabank","./data/Lahman"]:
        if os.path.isdir(cand): return cand
    sys.exit("[FATAL] Lahman ZIP/dir not found.")

def pick(df, *alts):
    low = {c.lower(): c for c in df.columns}
    for a in alts:
        if a in df.columns: return a
        if a.lower() in low: return low[a.lower()]
    return None

def valid_people(df):
    cols = [c.lower() for c in df.columns]
    return not (len(cols)==1 and "404: not found" in cols[0])

def main():
    arg = sys.argv[1] if len(sys.argv)>1 else None
    root = find_lahman_root(arg)

    bat_path = pit_path = ppl_path = None
    for dp,_,fs in os.walk(root):
        fl = [f.lower() for f in fs]
        if "batting.csv" in fl: bat_path = os.path.join(dp, fs[fl.index("batting.csv")])
        if "pitching.csv" in fl: pit_path = os.path.join(dp, fs[fl.index("pitching.csv")])
        if "people.csv"  in fl: ppl_path = os.path.join(dp, fs[fl.index("people.csv")])
    if not bat_path or not pit_path:
        sys.exit(f"[FATAL] Batting.csv/Pitching.csv missing under {root}")

    print("[INFO] Batting:", bat_path)
    print("[INFO] Pitching:", pit_path)
    print("[INFO] People:", ppl_path if ppl_path else "(none)")

    # ---- Batting per-season
    bat = pd.read_csv(bat_path, dtype=str, low_memory=False)
    bat.columns = [c.strip() for c in bat.columns]
    pid = pick(bat,"playerID","playerid")
    yr  = pick(bat,"yearID","yearid","Year","year")
    AB  = pick(bat,"AB","ab"); H = pick(bat,"H","h"); HR = pick(bat,"HR","hr")
    BB  = pick(bat,"BB","bb"); SO = pick(bat,"SO","so","k","K")
    if not all([pid,yr,AB,H,HR,BB,SO]): sys.exit("[FATAL] Batting required cols missing")
    for c in [AB,H,HR,BB,SO]: bat[c] = to_num(bat[c])

    HBP = pick(bat,"HBP","hbp"); SF = pick(bat,"SF","sf"); SH = pick(bat,"SH","sh")
    hbp = to_num(bat[HBP]) if HBP else 0
    sf  = to_num(bat[SF])  if SF  else 0
    sh  = to_num(bat[SH])  if SH  else 0
    bat["PA"] = bat[AB] + bat[BB] + hbp + sf + sh

    _2B = pick(bat,"2B","2b"); _3B = pick(bat,"3B","3b")
    d2 = to_num(bat[_2B]) if _2B else 0
    d3 = to_num(bat[_3B]) if _3B else 0
    singles = bat[H] - d2 - d3 - bat[HR]
    tb = singles + 2*d2 + 3*d3 + 4*bat[HR]
    slg = safe_div(tb, bat[AB])
    obp = safe_div(bat[H] + bat[BB] + hbp, bat["PA"])
    ops = obp + slg

    bat_out = pd.DataFrame({
        "lahman_id": bat[pid].astype(str),
        "season": bat[yr].astype(str),
        "pa": bat["PA"], "ab": bat[AB], "h": bat[H], "hr": bat[HR], "bb": bat[BB], "so": bat[SO],
        "obp": obp, "slg": slg, "ops": ops
    })

    # ---- Pitching per-season
    pit = pd.read_csv(pit_path, dtype=str, low_memory=False)
    pit.columns = [c.strip() for c in pit.columns]
    pidp = pick(pit,"playerID","playerid")
    yrp  = pick(pit,"yearID","yearid","Year","year")
    IPo  = pick(pit,"IPouts","ipouts")
    ER   = pick(pit,"ER","er"); BBp = pick(pit,"BB","bb"); Kp = pick(pit,"SO","so","k","K")
    HRp  = pick(pit,"HR","hr")
    if not all([pidp,yrp,IPo,ER,BBp,Kp]): sys.exit("[FATAL] Pitching required cols missing")
    for c in [IPo,ER,BBp,Kp]: pit[c] = to_num(pit[c])
    pit["ip"] = pit[IPo]/3.0
    pit_out = pd.DataFrame({
        "lahman_id": pit[pidp].astype(str),
        "season": pit[yrp].astype(str),
        "ip": pit["ip"], "er": pit[ER], "bb": pit[BBp], "so": pit[Kp],
        "hr": to_num(pit[HRp]) if HRp else 0
    })

    # ---- People (optional, 깨진 경우 스킵)
    full_name_col = bbref_col = retro_col = None
    if ppl_path:
        ppl = pd.read_csv(ppl_path, dtype=str, low_memory=False)
        cols = [c.lower() for c in ppl.columns]
        if not (len(cols)==1 and "404: not found" in cols[0]):
            ppl.columns = [c.strip() for c in ppl.columns]
            pid2 = pick(ppl,"playerID","playerid")
            first = pick(ppl,"nameFirst","namefirst"); last = pick(ppl,"nameLast","namelast")
            given = pick(ppl,"nameGiven","namegiven")
            if first and last:
                ppl["full_name"] = ppl[first].fillna("").astype(str).str.strip()+" "+ppl[last].fillna("").astype(str).str.strip()
            elif given:
                ppl["full_name"] = ppl[given].fillna("").astype(str).str.strip()
            else:
                ppl["full_name"] = ""
            bbref_col = pick(ppl,"bbrefID","bbrefid")
            retro_col = pick(ppl,"retroID","retroid")
            keep = ["full_name"] + ([bbref_col] if bbref_col else []) + ([retro_col] if retro_col else [])
            ppl_small = ppl[[pid2]+keep].rename(columns={pid2:"lahman_id"})
            bat_out = bat_out.merge(ppl_small, on="lahman_id", how="left")
            pit_out = pit_out.merge(ppl_small, on="lahman_id", how="left")
        else:
            print("[WARN] invalid People.csv (looks like 404); skipping enrichment")

    # ---- attach mlb_id (id_map / name2mlbid)
    def attach_mlb_id(df):
        have = False
        if os.path.exists("output/id_map.csv"):
            idm = pd.read_csv("output/id_map.csv", dtype=str, low_memory=False).rename(columns=str.lower).fillna("")
            for k in ["bbref_id","retro_id","mlb_id","full_name"]:
                if k not in idm.columns: idm[k] = ""
            if bbref_col and bbref_col in df.columns:
                df = df.merge(idm[["bbref_id","mlb_id"]].drop_duplicates(), left_on=bbref_col, right_on="bbref_id", how="left")
                have = True
            if retro_col and "mlb_id" not in df.columns:
                df = df.merge(idm[["retro_id","mlb_id"]].drop_duplicates(), left_on=retro_col, right_on="retro_id", how="left")
                have = True
        if (not have or df.get("mlb_id", pd.Series([],dtype=str)).isna().all()) and os.path.exists("output/cache/name2mlbid.csv"):
            n2 = pd.read_csv("output/cache/name2mlbid.csv", dtype=str, low_memory=False).rename(columns=str.lower)
            if "full_name" in df.columns and {"full_name","mlb_id"}.issubset(n2.columns):
                df = df.merge(n2[["full_name","mlb_id"]].drop_duplicates(), on="full_name", how="left")
        return df

    bat_out = attach_mlb_id(bat_out)
    pit_out = attach_mlb_id(pit_out)

    ensure_dir("output/tmp")
    for c in ["mlb_id","full_name","season","pa","ab","h","hr","bb","so","obp","slg","ops"]:
        if c not in bat_out.columns: bat_out[c] = pd.NA
    for c in ["mlb_id","full_name","season","ip","er","bb","so","hr"]:
        if c not in pit_out.columns: pit_out[c] = pd.NA

    bat_out.to_csv("output/tmp/mlb_batting.csv", index=False)
    pit_out.to_csv("output/tmp/mlb_pitching.csv", index=False)
    print("[OK] wrote: output/tmp/mlb_batting.csv , output/tmp/mlb_pitching.csv (rows: {}, {})".format(len(bat_out), len(pit_out)))

if __name__ == "__main__":
    main()
