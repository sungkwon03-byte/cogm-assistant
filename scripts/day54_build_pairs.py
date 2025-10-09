#!/usr/bin/env python3
import argparse, os, re, sys
import pandas as pd

def nkey(s):
    s = "" if pd.isna(s) else str(s)
    s = s.replace("."," ").replace("-"," ")
    return re.sub(r"\s+"," ", s).upper().strip()

def season_series(df):
    for c in ["season","year","Year","YEAR","yearID","yearid"]:
        if c in df.columns:
            return df[c].astype(str).str.extract(r"(\d{4})", expand=False)
    return pd.Series([""]*len(df), index=df.index)

def name_series(df):
    for c in ["full_name","name","player","Player","PLAYER"]:
        if c in df.columns: return df[c].astype(str)
    return pd.Series([""]*len(df), index=df.index)

def to_num(df, cols):
    for c in cols:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def ops_proxy(obp, slg):
    try: return 1.2*float(obp) + float(slg)
    except: return float('nan')

def era(er, ip):
    ip = float(ip) if pd.notna(ip) else 0.0
    ip = max(ip, 1e-9); er = float(er) if pd.notna(er) else 0.0
    return (er*9.0)/ip

def per9(x, ip):
    ip = float(ip) if pd.notna(ip) else 0.0
    ip = max(ip, 1e-9); x = float(x) if pd.notna(x) else 0.0
    return (9.0*x)/ip

# --- NEW: load id_map for ID-based matching
def load_id_map(path="output/id_map.csv"):
    if not os.path.exists(path):
        return pd.DataFrame(columns=["nkey","bbref_id","retro_id"])
    idm = pd.read_csv(path, dtype=str).fillna("")
    # 보장
    if "nkey" not in idm.columns:
        namecol = None
        for c in ["full_name","name","player"]:
            if c in idm.columns: namecol=c; break
        if namecol is not None: idm["nkey"] = idm[namecol].map(nkey)
        else: idm["nkey"] = ""
    for col in ["bbref_id","retro_id"]:
        if col not in idm.columns: idm[col] = ""
    # 양방향 dict 준비
    bbref2nkey = {r["bbref_id"]: r["nkey"] for _,r in idm.iterrows() if r["bbref_id"]}
    retro2nkey = {r["retro_id"]: r["nkey"] for _,r in idm.iterrows() if r["retro_id"]}
    nkey2bbref = {r["nkey"]: r["bbref_id"] for _,r in idm.iterrows() if r["nkey"] and r["bbref_id"]}
    nkey2retro = {r["nkey"]: r["retro_id"] for _,r in idm.iterrows() if r["nkey"] and r["retro_id"]}
    return idm, bbref2nkey, retro2nkey, nkey2bbref, nkey2retro

def main(args):
    os.makedirs("output", exist_ok=True)
    # KBO
    kbo_bat = pd.read_csv(args.kbo_bat, dtype=str, low_memory=False)
    kbo_pit = pd.read_csv(args.kbo_pit, dtype=str, low_memory=False)
    for df in (kbo_bat, kbo_pit):
        df["full_name"] = name_series(df)
        df["season"] = season_series(df)
        df["nkey"] = df["full_name"].map(nkey)

    # MLB (Retrosheet-seasonized or Lahman-style)
    if not (os.path.exists(args.mlb_bat) and os.path.exists(args.mlb_pit)):
        sys.exit("[FATAL] MLB 소스 파일이 없습니다.")
    mlb_bat = pd.read_csv(args.mlb_bat, dtype=str, low_memory=False)
    mlb_pit = pd.read_csv(args.mlb_pit, dtype=str, low_memory=False)
    for df in (mlb_bat, mlb_pit):
        df["season"] = season_series(df)
        # id 컬럼이 있으면 bbref 후보로 사용(현재 너의 retrosheet 집계 산출물은 full_name=id)
        if "id" in df.columns: df["bbref_id"] = df["id"]
        if "bbref_id" not in df.columns and "full_name" in df.columns:
            # full_name이 id로 채워진 경우
            df["bbref_id"] = df["full_name"]
        if "retro_id" not in df.columns: df["retro_id"] = ""  # 없음 기본

        # 이름 매칭 fallback용
        if "full_name" not in df.columns: df["full_name"] = df.get("name", "")
        df["nkey"] = df["full_name"].map(nkey)

    # id_map 로드
    idm, bbref2nkey, retro2nkey, nkey2bbref, nkey2retro = load_id_map()

    # ----- Batting: 우선순위 매칭 (bbref_id -> retro_id -> nkey)
    bat_k = kbo_bat.copy()
    bat_m = mlb_bat.copy()

    # KBO 측에 id 보강
    bat_k["bbref_id"] = bat_k["nkey"].map(nkey2bbref).fillna("")
    bat_k["retro_id"] = bat_k["nkey"].map(nkey2retro).fillna("")

    # MLB 측에 nkey 보강(아이디→nkey 역매핑)
    bat_m["nkey_from_bbref"] = bat_m["bbref_id"].map(bbref2nkey).fillna("")
    bat_m["nkey_from_retro"] = bat_m["retro_id"].map(retro2nkey).fillna("")
    bat_m["nkey_join"] = bat_m["nkey_from_bbref"]
    mask = bat_m["nkey_join"]==""; bat_m.loc[mask,"nkey_join"] = bat_m.loc[mask,"nkey_from_retro"]
    # 최후 fallback: 이름 nkey 그대로
    mask = bat_m["nkey_join"]==""; bat_m.loc[mask,"nkey_join"] = bat_m.loc[mask,"nkey"]

    # 공통 컬럼 선별
    bat_cols = ["pa","ab","h","hr","bb","so","obp","slg","ops"]
    use_k = ["nkey","season","bbref_id","retro_id"] + [c for c in bat_cols if c in bat_k.columns]
    use_m = ["nkey_join","season","bbref_id","retro_id"] + [c for c in bat_cols if c in bat_m.columns]
    bk = bat_k[use_k].copy()
    bm = bat_m[use_m].copy().rename(columns={"nkey_join":"nkey"})

    # 우선 bbref_id로, 없으면 retro_id로, 마지막 nkey로 병합
    merged = []
    for key in ["bbref_id","retro_id","nkey"]:
        lk = bk[bk[key]!=""]
        rm = bm[bm[key]!=""]
        if not lk.empty and not rm.empty:
            merged.append(lk.merge(rm, on=[key,"season"], suffixes=("_kbo","_mlb"), how="inner"))
    bat = pd.concat(merged, ignore_index=True) if merged else pd.DataFrame(columns=[])
    bat = to_num(bat, [c for c in bat.columns if c not in ("nkey","season","bbref_id","retro_id")])

    bat_rows=[]
    for _,r in bat.iterrows():
        pa_k = max((r.get("pa_kbo") or 0), 1); pa_m = max((r.get("pa_mlb") or 0), 1)
        pa_k = float(pa_k); pa_m = float(pa_m)
        bat_rows += [
            ("hr_pa",  (r.get("hr_kbo") or 0)/pa_k, (r.get("hr_mlb") or 0)/pa_m, r.get("nkey_kbo") or r.get("nkey"), int(r["season"])),
            ("bb_pct", (r.get("bb_kbo") or 0)/pa_k, (r.get("bb_mlb") or 0)/pa_m, r.get("nkey_kbo") or r.get("nkey"), int(r["season"])),
            ("so_pct", (r.get("so_kbo") or 0)/pa_k, (r.get("so_mlb") or 0)/pa_m, r.get("nkey_kbo") or r.get("nkey"), int(r["season"])),
            ("h_pa",   (r.get("h_kbo")  or 0)/pa_k, (r.get("h_mlb")  or 0)/pa_m, r.get("nkey_kbo") or r.get("nkey"), int(r["season"])),
            ("ops_proxy", ops_proxy(r.get("obp_kbo"), r.get("slg_kbo")), ops_proxy(r.get("obp_mlb"), r.get("slg_mlb")), r.get("nkey_kbo") or r.get("nkey"), int(r["season"])),
        ]
    pd.DataFrame(bat_rows, columns=["metric","kbo_val","mlb_val","player_id","season"]).to_csv("output/KBO_bat_pairs.csv", index=False)

    # ----- Pitching: 동일 로직
    pit_k = kbo_pit.copy()
    pit_m = mlb_pit.copy()
    pit_k["bbref_id"] = pit_k["nkey"].map(nkey2bbref).fillna("")
    pit_k["retro_id"] = pit_k["nkey"].map(nkey2retro).fillna("")
    pit_m["nkey_from_bbref"] = pit_m["bbref_id"].map(bbref2nkey).fillna("")
    pit_m["nkey_from_retro"] = pit_m["retro_id"].map(retro2nkey).fillna("")
    pit_m["nkey_join"] = pit_m["nkey_from_bbref"]
    mask = pit_m["nkey_join"]==""; pit_m.loc[mask,"nkey_join"] = pit_m.loc[mask,"nkey_from_retro"]
    mask = pit_m["nkey_join"]==""; pit_m.loc[mask,"nkey_join"] = pit_m.loc[mask,"nkey"]

    # allow 'k' as alias for 'so'
    if "so" in pit_k.columns and "k" not in pit_k.columns: pit_k["k"] = pit_k["so"]
    if "so" in pit_m.columns and "k" not in pit_m.columns: pit_m["k"] = pit_m["so"]

    pit_cols = ["ip","er","bb","k","hr"]
    use_k = ["nkey","season","bbref_id","retro_id"] + [c for c in pit_cols if c in pit_k.columns]
    use_m = ["nkey_join","season","bbref_id","retro_id"] + [c for c in pit_cols if c in pit_m.columns]
    pk = pit_k[use_k].copy()
    pm = pit_m[use_m].copy().rename(columns={"nkey_join":"nkey"})

    merged = []
    for key in ["bbref_id","retro_id","nkey"]:
        lk = pk[pk[key]!=""]
        rm = pm[pm[key]!=""]
        if not lk.empty and not rm.empty:
            merged.append(lk.merge(rm, on=[key,"season"], suffixes=("_kbo","_mlb"), how="inner"))
    pit = pd.concat(merged, ignore_index=True) if merged else pd.DataFrame(columns=[])
    pit = to_num(pit, [c for c in pit.columns if c not in ("nkey","season","bbref_id","retro_id")])

    pit_rows=[]
    for _,r in pit.iterrows():
        pit_rows += [
            ("ERA", era(r.get("er_kbo"), r.get("ip_kbo")), era(r.get("er_mlb"), r.get("ip_mlb")), r.get("nkey_kbo") or r.get("nkey"), int(r["season"])),
            ("K9",  per9(r.get("k_kbo"),  r.get("ip_kbo")), per9(r.get("k_mlb"),  r.get("ip_mlb")), r.get("nkey_kbo") or r.get("nkey"), int(r["season"])),
            ("BB9", per9(r.get("bb_kbo"), r.get("ip_kbo")), per9(r.get("bb_mlb"), r.get("ip_mlb")), r.get("nkey_kbo") or r.get("nkey"), int(r["season"])),
        ]
    pd.DataFrame(pit_rows, columns=["metric","kbo_val","mlb_val","player_id","season"]).to_csv("output/KBO_pit_pairs.csv", index=False)

    print("[OK] wrote output/KBO_bat_pairs.csv , output/KBO_pit_pairs.csv")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--kbo-bat", default="data/xleague/kbo_batting.csv")
    ap.add_argument("--kbo-pit", default="data/xleague/kbo_pitching.csv")
    ap.add_argument("--mlb-bat", required=True)
    ap.add_argument("--mlb-pit", required=True)
    main(ap.parse_args())
