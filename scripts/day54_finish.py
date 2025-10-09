#!/usr/bin/env python3
import os, re, glob, subprocess, sys
import pandas as pd
import numpy as np

def sh(cmd):
    r = subprocess.run(cmd, shell=True)
    if r.returncode != 0:
        raise SystemExit(f"[FATAL] failed: {cmd}")

def nkey(s):
    s = "" if pd.isna(s) else str(s)
    s = s.replace("."," ").replace("-"," ")
    return re.sub(r"\s+"," ", s).upper().strip()

def tkey(s):
    toks = re.findall(r"[A-Z0-9]+", nkey(s))
    toks.sort()
    return " ".join(toks)

def ensure_dir(p): os.makedirs(p, exist_ok=True)

def pick(df, *alts):
    low = {c.lower(): c for c in df.columns}
    for a in alts:
        if a in df.columns: return a
        if a.lower() in low: return low[a.lower()]
    return None

def have_season_cols(fn, need):
    try:
        df = pd.read_csv(fn, nrows=1, dtype=str, low_memory=False)
    except Exception:
        return False
    cols = [c.lower() for c in df.columns]
    if not any(c in cols for c in ["season","year","yearid","date"]): return False
    return all(any(c==x or c==x.lower() for c in cols) for x in need)

def locate_mlb_season():
    cand = ["output/tmp/mlb_batting.csv","output/tmp/mlb_pitching.csv"]
    if all(os.path.exists(c) for c in cand):
        return cand[0], cand[1]
    bat_cands = sorted(glob.glob("./**/*.csv", recursive=True))
    pit_cands = list(bat_cands)
    bat = None; pit = None
    for f in bat_cands:
        if re.search(r"(mlb|lahman|baseball|retrosheet)", f, re.I) and have_season_cols(f, ["ab","h","hr","bb","so"]):
            bat = f; break
    for f in pit_cands:
        if re.search(r"(mlb|lahman|baseball|retrosheet)", f, re.I) and have_season_cols(f, ["ip","er","bb","so"]):
            pit = f; break
    return bat, pit

def fix_kbo(path):
    if not os.path.exists(path):
        raise SystemExit(f"[FATAL] missing: {path}")
    df = pd.read_csv(path, dtype=str, low_memory=False)
    nm = pick(df, "full_name","Name","name","player","선수명","선수","이름")
    if not nm:
        raise SystemExit(f"[FATAL] {path}: no name column. columns={list(df.columns)[:15]}")
    if "full_name" not in df.columns:
        df["full_name"] = df[nm].astype(str)
    sc = pick(df, "season","Year","year","yearID")
    if "season" not in df.columns:
        if sc:
            df["season"] = df[sc].astype(str).str.extract(r"(\d{4})", expand=False)
        else:
            df["season"] = ""
    df.to_csv(path, index=False)

def to_num(s): return pd.to_numeric(s, errors="coerce").fillna(0)

def safe_div(a, b):
    a = pd.to_numeric(a, errors="coerce")
    b = pd.to_numeric(b, errors="coerce")
    out = np.divide(a, b, out=np.full(a.shape, np.nan, dtype=float), where=(b!=0))
    return pd.Series(out, index=a.index, dtype=float)

def season_pairs(kbo_bat_path, kbo_pit_path, mlb_bat_path, mlb_pit_path):
    kb = pd.read_csv(kbo_bat_path, dtype=str, low_memory=False)
    kp = pd.read_csv(kbo_pit_path, dtype=str, low_memory=False)
    mb = pd.read_csv(mlb_bat_path, dtype=str, low_memory=False)
    mp = pd.read_csv(mlb_pit_path, dtype=str, low_memory=False)

    kb["season"] = kb["season"].astype(str).str.extract(r"(\d{4})", expand=False)
    kp["season"] = kp["season"].astype(str).str.extract(r"(\d{4})", expand=False)
    kb["full_name"] = kb["full_name"].astype(str)
    kp["full_name"] = kp["full_name"].astype(str)
    kb["_tkey"] = kb["full_name"].map(tkey)
    kp["_tkey"] = kp["full_name"].map(tkey)

    def seasonify(df):
        sc = None
        for c in ["season","year","yearID","date","Year","YEAR"]:
            if c in df.columns:
                sc = c; break
        if sc is None:
            return None
        df["season"] = df[sc].astype(str).str.extract(r"(\d{4})", expand=False)
        return "season"

    if not seasonify(mb) or not seasonify(mp):
        return 0, 0

    def add_tkey(df):
        nm = pick(df, "full_name","name","player","player_name")
        df["_tkey"] = df[nm].astype(str).map(tkey) if nm else ""

    add_tkey(mb); add_tkey(mp)

    for c in ["pa","ab","h","hr","bb","so"]:
        if c in mb.columns:
            mb[c] = to_num(mb[c])
    if "pa" not in mb.columns:
        HBP = pick(mb,"hbp","HBP"); SF = pick(mb,"sf","SF"); SH = pick(mb,"sh","SH")
        mb["ab"] = to_num(mb.get("ab",0)); mb["bb"]=to_num(mb.get("bb",0))
        mb["pa"] = mb["ab"] + mb["bb"] + to_num(mb.get(HBP,0)) + to_num(mb.get(SF,0)) + to_num(mb.get(SH,0))
    mb["hr_pa"] = safe_div(mb.get("hr",0), mb["pa"])
    mb["bb_pct"]= safe_div(mb.get("bb",0), mb["pa"])
    mb["so_pct"]= safe_div(mb.get("so",0), mb["pa"])
    mb["h_pa"] = safe_div(mb.get("h",0),  mb["pa"])

    for c in ["ip","er","bb","so"]:
        if c in mp.columns: mp[c] = to_num(mp[c])
    if "ip" not in mp.columns:
        IPo = pick(mp,"ip_outs","IPouts","ipouts")
        mp["ip"] = to_num(mp[IPo])/3.0 if IPo else 0.0
    mp["ERA"] = safe_div(to_num(mp.get("er",0))*9.0, mp["ip"])
    mp["K9"]  = safe_div(to_num(mp.get("so",0))*9.0, mp["ip"])
    mp["BB9"] = safe_div(to_num(mp.get("bb",0))*9.0, mp["ip"])

    def join_keys(kdf, mdf):
        if "mlb_id" in kdf.columns and "mlb_id" in mdf.columns:
            j = kdf.merge(mdf, on=["mlb_id","season"], how="inner", suffixes=("_k","_m"))
            if len(j) > 0: return j
        return kdf.merge(mdf, on=["_tkey","season"], how="inner", suffixes=("_k","_m"))

    for c in ["PA","H","HR","BB","SO","pa","h","hr","bb","so"]:
        if c in kb.columns: kb[c] = to_num(kb[c])
    kb["pa"] = kb["PA"] if "PA" in kb.columns else kb.get("pa",0)
    kb["hr_pa"] = safe_div(kb.get("HR",kb.get("hr",0)), kb["pa"])
    kb["bb_pct"]= safe_div(kb.get("BB",kb.get("bb",0)), kb["pa"])
    kb["so_pct"]= safe_div(kb.get("SO",kb.get("so",0)), kb["pa"])
    kb["h_pa"]  = safe_div(kb.get("H", kb.get("h",0)),  kb["pa"])

    bat_j = join_keys(
        kb[["full_name","season","_tkey","pa","hr_pa","bb_pct","so_pct","h_pa","mlb_id"]] if "mlb_id" in kb.columns else kb[["full_name","season","_tkey","pa","hr_pa","bb_pct","so_pct","h_pa"]],
        mb[["full_name","season","_tkey","hr_pa","bb_pct","so_pct","h_pa","mlb_id"]] if "mlb_id" in mb.columns else mb[["full_name","season","_tkey","hr_pa","bb_pct","so_pct","h_pa"]]
    )

    pairs_b=[]
    for _,r in bat_j.iterrows():
        pid = r["mlb_id"] if "mlb_id" in bat_j.columns and pd.notna(r["mlb_id"]) else r["_tkey_k"]
        pairs_b += [
            ("hr_pa",  float(r["hr_pa_k"]) if pd.notna(r["hr_pa_k"]) else np.nan,  float(r["hr_pa_m"]) if pd.notna(r["hr_pa_m"]) else np.nan,  pid, r["season"]),
            ("bb_pct", float(r["bb_pct_k"]) if pd.notna(r["bb_pct_k"]) else np.nan, float(r["bb_pct_m"]) if pd.notna(r["bb_pct_m"]) else np.nan, pid, r["season"]),
            ("so_pct", float(r["so_pct_k"]) if pd.notna(r["so_pct_k"]) else np.nan, float(r["so_pct_m"]) if pd.notna(r["so_pct_m"]) else np.nan, pid, r["season"]),
            ("h_pa",   float(r["h_pa_k"])  if pd.notna(r["h_pa_k"])  else np.nan,  float(r["h_pa_m"])  if pd.notna(r["h_pa_m"])  else np.nan,  pid, r["season"]),
        ]
    dfb = pd.DataFrame(pairs_b, columns=["metric","kbo_val","mlb_val","player_id","season"])

    for c in ["IP","ip","ER","er","BB","bb","SO","so","K","k"]:
        if c in kp.columns: kp[c] = to_num(kp[c])
    if "ip" not in kp.columns and "IP" in kp.columns:
        def ipf(x):
            s=str(x).strip()
            if s=="" or s.lower()=="nan": return 0.0
            if "." in s:
                a,b=s.split(".",1)
                try:
                    a=int(a)
                    if b=="1": return a + 1/3
                    if b=="2": return a + 2/3
                    return float(s)
                except: return float(s)
            return float(s)
        kp["ip"] = kp["IP"].map(ipf)
    er_k = kp.get("ER", kp.get("er",0))
    bb_k = kp.get("BB", kp.get("bb",0))
    so_k = kp.get("SO", kp.get("so", kp.get("K",0)))
    kp["ERA"] = safe_div(to_num(er_k)*9.0, kp["ip"])
    kp["K9"]  = safe_div(to_num(so_k)*9.0, kp["ip"])
    kp["BB9"] = safe_div(to_num(bb_k)*9.0, kp["ip"])
    kp["_tkey"] = kp["full_name"].map(tkey)

    mp_cols = ["full_name","season","_tkey","ERA","K9","BB9","mlb_id"] if "mlb_id" in mp.columns else ["full_name","season","_tkey","ERA","K9","BB9"]
    pit_j = (kp[["full_name","season","_tkey","ERA","K9","BB9","mlb_id"]] if "mlb_id" in kp.columns else kp[["full_name","season","_tkey","ERA","K9","BB9"]]) \
            .merge(mp[mp_cols], on=["_tkey","season"], how="inner", suffixes=("_k","_m"))

    pairs_p=[]
    for _,r in pit_j.iterrows():
        pid = r["mlb_id_k"] if "mlb_id_k" in pit_j.columns and pd.notna(r["mlb_id_k"]) else r["_tkey"]
        pairs_p += [
            ("ERA", float(r["ERA_k"]) if pd.notna(r["ERA_k"]) else np.nan, float(r["ERA_m"]) if pd.notna(r["ERA_m"]) else np.nan, pid, r["season"]),
            ("K9",  float(r["K9_k"])  if pd.notna(r["K9_k"])  else np.nan, float(r["K9_m"])  if pd.notna(r["K9_m"])  else np.nan, pid, r["season"]),
            ("BB9", float(r["BB9_k"]) if pd.notna(r["BB9_k"]) else np.nan, float(r["BB9_m"]) if pd.notna(r["BB9_m"]) else np.nan, pid, r["season"]),
        ]
    dfp = pd.DataFrame(pairs_p, columns=["metric","kbo_val","mlb_val","player_id","season"])
    dfb.to_csv("output/KBO_bat_pairs.csv", index=False)
    dfp.to_csv("output/KBO_pit_pairs.csv", index=False)
    return len(dfb), len(dfp)

def career_pairs(kbo_bat_path, kbo_pit_path, totals_bat, totals_pit):
    kb = pd.read_csv(kbo_bat_path, dtype=str, low_memory=False)
    kp = pd.read_csv(kbo_pit_path, dtype=str, low_memory=False)
    kb["full_name"]=kb["full_name"].astype(str); kp["full_name"]=kp["full_name"].astype(str)
    kb["_tkey"]=kb["full_name"].map(tkey); kp["_tkey"]=kp["full_name"].map(tkey)
    for c in ["PA","H","HR","BB","SO","pa","h","hr","bb","so"]:
        if c in kb.columns: kb[c] = pd.to_numeric(kb[c], errors="coerce").fillna(0)
    kb_g = kb.groupby("_tkey",as_index=False).agg(PA=("PA","sum") if "PA" in kb.columns else ("pa","sum"),
                                                 H=("H","sum")  if "H" in kb.columns else ("h","sum"),
                                                 HR=("HR","sum") if "HR" in kb.columns else ("hr","sum"),
                                                 BB=("BB","sum") if "BB" in kb.columns else ("bb","sum"),
                                                 SO=("SO","sum") if "SO" in kb.columns else ("so","sum"))
    def sdiv(a,b):
        out = np.divide(a, b, out=np.full(a.shape, np.nan, dtype=float), where=(b!=0))
        return out
    kb_g["hr_pa"]=sdiv(kb_g["HR"], kb_g["PA"])
    kb_g["bb_pct"]=sdiv(kb_g["BB"], kb_g["PA"])
    kb_g["so_pct"]=sdiv(kb_g["SO"], kb_g["PA"])
    kb_g["h_pa"]= sdiv(kb_g["H"],  kb_g["PA"])

    mb = pd.read_csv(totals_bat, dtype=str, low_memory=False)
    mb["_tkey"]=mb.get("full_name","").astype(str).map(tkey)
    for c in ["PA","H","HR","BB","SO","hr_pa","bb_pct","so_pct","h_pa"]:
        if c in mb.columns: mb[c]=pd.to_numeric(mb[c], errors="coerce")

    bat = kb_g.merge(mb[["_tkey","hr_pa","bb_pct","so_pct","h_pa","mlb_id"]], on="_tkey", how="inner")
    dfb = pd.DataFrame([
        ("hr_pa",  float(r["hr_pa_x"]), float(r["hr_pa_y"]), r["mlb_id"] if pd.notna(r["mlb_id"]) else r["_tkey"], "career") for _,r in bat.iterrows()
    ]+[
        ("bb_pct", float(r["bb_pct_x"]), float(r["bb_pct_y"]), r["mlb_id"] if pd.notna(r["mlb_id"]) else r["_tkey"], "career") for _,r in bat.iterrows()
    ]+[
        ("so_pct", float(r["so_pct_x"]), float(r["so_pct_y"]), r["mlb_id"] if pd.notna(r["mlb_id"]) else r["_tkey"], "career") for _,r in bat.iterrows()
    ]+[
        ("h_pa",   float(r["h_pa_x"]),  float(r["h_pa_y"]),  r["mlb_id"] if pd.notna(r["mlb_id"]) else r["_tkey"], "career") for _,r in bat.iterrows()
    ], columns=["metric","kbo_val","mlb_val","player_id","season"])

    for c in ["IP","ip","ER","er","BB","bb","SO","so","K","k"]:
        if c in kp.columns: kp[c] = pd.to_numeric(kp[c], errors="coerce").fillna(0)
    if "ip" not in kp.columns and "IP" in kp.columns:
        def ipf(x):
            s=str(x).strip()
            if s=="" or s.lower()=="nan": return 0.0
            if "." in s:
                a,b=s.split(".",1)
                try:
                    a=int(a)
                    if b=="1": return a + 1/3
                    if b=="2": return a + 2/3
                    return float(s)
                except: return float(s)
            return float(s)
        kp["ip"]=kp["IP"].map(ipf)
    kp_g = kp.groupby("_tkey",as_index=False).agg(IP=("ip","sum"), ER=("ER","sum") if "ER" in kp.columns else ("er","sum"),
                                                 BB=("BB","sum") if "BB" in kp.columns else ("bb","sum"),
                                                 SO=("SO","sum") if "SO" in kp.columns else ("so","sum"))
    kp_g["ERA"] = (kp_g["ER"]*9.0)/kp_g["IP"].replace(0, np.nan)
    kp_g["K9"]  = (kp_g["SO"]*9.0)/kp_g["IP"].replace(0, np.nan)
    kp_g["BB9"] = (kp_g["BB"]*9.0)/kp_g["IP"].replace(0, np.nan)

    mp = pd.read_csv(totals_pit, dtype=str, low_memory=False)
    mp["_tkey"]=mp.get("full_name","").astype(str).map(tkey)
    for c in ["ERA","K","BB","K9","BB9"]:
        if c in mp.columns: mp[c]=pd.to_numeric(mp[c], errors="coerce")
    pit = kp_g.merge(mp[["_tkey","ERA","K9","BB9","mlb_id"]], on="_tkey", how="inner")
    dfp = pd.DataFrame([
        ("ERA", float(r["ERA_x"]), float(r["ERA_y"]), r["mlb_id"] if pd.notna(r["mlb_id"]) else r["_tkey"], "career") for _,r in pit.iterrows()
    ]+[
        ("K9",  float(r["K9_x"]),  float(r["K9_y"]),  r["mlb_id"] if pd.notna(r["mlb_id"]) else r["_tkey"], "career") for _,r in pit.iterrows()
    ]+[
        ("BB9", float(r["BB9_x"]), float(r["BB9_y"]), r["mlb_id"] if pd.notna(r["mlb_id"]) else r["_tkey"], "career") for _,r in pit.iterrows()
    ], columns=["metric","kbo_val","mlb_val","player_id","season"])

    dfb.to_csv("output/KBO_bat_pairs.csv", index=False)
    dfp.to_csv("output/KBO_pit_pairs.csv", index=False)
    return len(dfb), len(dfp)

def link_fix_by_tkey():
    import re
    cand_path = "output/xleague_link_candidates.csv"
    miss_path = "output/xleague_missing_ids.csv"
    name2_path = "output/cache/name2mlbid.csv"
    idm_path   = "output/id_map.csv"

    def _nkey(s):
        s = "" if pd.isna(s) else str(s)
        s = s.replace("."," ").replace("-"," ")
        return re.sub(r"\s+"," ", s).upper().strip()
    def _tkey(s):
        toks = re.findall(r"[A-Z0-9]+", _nkey(s))
        toks.sort()
        return " ".join(toks)

    c = pd.read_csv(cand_path, dtype=str).fillna("")
    c["_tkey"] = c["full_name"].map(_tkey)

    changed = 0
    if os.path.exists(name2_path):
        n2 = pd.read_csv(name2_path, dtype=str).fillna("")
        if "full_name" in n2.columns and "mlb_id" in n2.columns:
            n2["_tkey"] = n2["full_name"].map(_tkey)
            c = c.merge(n2[["_tkey","mlb_id"]].drop_duplicates(), on="_tkey", how="left", suffixes=("","_n2"))
            mask = (c["mlb_id"].astype(str).eq("")) & (c["mlb_id_n2"].astype(str).ne(""))
            c.loc[mask, "mlb_id"] = c.loc[mask, "mlb_id_n2"]
            c = c.drop(columns=["mlb_id_n2"])
            changed += int(mask.sum())

    if os.path.exists(idm_path):
        idm = pd.read_csv(idm_path, dtype=str).fillna("")
        if "mlb_id" in idm.columns:
            c = c.merge(idm[["mlb_id","retro_id","bbref_id"]].drop_duplicates(), on="mlb_id", how="left", suffixes=("","_idm"))
            for col in ["retro_id","bbref_id"]:
                mask = (c[col].astype(str).eq("")) & (c[f"{col}_idm"].astype(str).ne(""))
                c.loc[mask, col] = c.loc[mask, f"{col}_idm"]
            c = c.drop(columns=[x for x in ["retro_id_idm","bbref_id_idm"] if x in c.columns], errors="ignore")
        if "full_name" in idm.columns:
            idm["_tkey"] = idm["full_name"].map(_tkey)
            c = c.merge(idm[["_tkey","retro_id","bbref_id"]].drop_duplicates(), on="_tkey", how="left", suffixes=("","_idm2"))
            for col in ["retro_id","bbref_id"]:
                mask = (c[col].astype(str).eq("")) & (c[f"{col}_idm2"].astype(str).ne(""))
                c.loc[mask, col] = c.loc[mask, f"{col}_idm2"]
            c = c.drop(columns=[x for x in ["retro_id_idm2","bbref_id_idm2"] if x in c.columns], errors="ignore")

    c = c.drop(columns=["_tkey"], errors="ignore")
    c.to_csv(cand_path, index=False)
    miss = c[(c["mlb_id"].astype(str)=="") & (c["retro_id"].astype(str)=="") & (c["bbref_id"].astype(str)=="")]
    miss.to_csv(miss_path, index=False)
    print(f"[OK] link candidates updated. newly_filled_mlb_id={changed}; now_missing={len(miss)}")

def verify_links():
    c = pd.read_csv("output/xleague_link_candidates.csv", dtype=str).fillna("")
    idm = pd.read_csv("output/id_map.csv", dtype=str).fillna("") if os.path.exists("output/id_map.csv") else pd.DataFrame()
    linked = c[c["mlb_id"].astype(str)!=""].copy()
    unlinked = c[c["mlb_id"].astype(str)==""].copy()
    linked.to_csv("output/xleague_linked_mlb.csv", index=False)
    unlinked.to_csv("output/xleague_nonmlb.csv", index=False)
    sus = pd.DataFrame()
    if not idm.empty and "mlb_id" in idm.columns:
        sus = linked.merge(idm[["mlb_id"]].drop_duplicates(), on="mlb_id", how="left", indicator=True)
        sus = sus[sus["_merge"]=="left_only"].drop(columns=["_merge"])
        sus.to_csv("output/xleague_link_suspect.csv", index=False)
    print("[CHECK] linked:", len(linked), "| nonmlb:", len(unlinked), "| suspect:", len(sus))

def main():
    ensure_dir("output"); ensure_dir("output/tmp")
    # 1) KBO 표준화
    fix_kbo("data/xleague/kbo_batting.csv")
    fix_kbo("data/xleague/kbo_pitching.csv")

    # 2) MLB 시즌 CSV 확보(없으면 Lahman에서 생성)
    mlb_bat, mlb_pit = locate_mlb_season()
    if not (mlb_bat and mlb_pit):
        print("[INFO] MLB season csv not found. Building from Lahman...")
        sh("python3 scripts/day54_build_mlb_from_lahman_zip.py")
        mlb_bat = "output/tmp/mlb_batting.csv"
        mlb_pit = "output/tmp/mlb_pitching.csv"

    # 3) 시즌 페어 → 없으면 커리어 폴백
    nb, np = season_pairs("data/xleague/kbo_batting.csv","data/xleague/kbo_pitching.csv", mlb_bat, mlb_pit)
    if nb==0 and np==0:
        print("[WARN] season pairs=0. fallback to career totals.")
        totals_bat = "output/cache/mlb_totals_bat.csv"
        totals_pit = "output/cache/mlb_totals_pit.csv"
        if not os.path.exists(totals_bat) or not os.path.exists(totals_pit):
            raise SystemExit("[FATAL] career totals not found for fallback.")
        nb, np = career_pairs("data/xleague/kbo_batting.csv","data/xleague/kbo_pitching.csv", totals_bat, totals_pit)

    # 4) 회귀(②)
    sh("python3 scripts/day54_bridge_on_demand.py")

    # 5) 링크(③) + tkey 보정 + 검증
    if os.path.exists("scripts/day54_link_candidates.py"):
        sh("python3 scripts/day54_link_candidates.py")
        link_fix_by_tkey()
        verify_links()

    # 6) 요약
    print("[SUMMARY]")
    for p in ["output/bridge_report.txt","output/bridge_coef.csv","output/xleague_link_candidates.csv","output/xleague_missing_ids.csv","output/xleague_linked_mlb.csv","output/xleague_nonmlb.csv"]:
        print(" -", p, os.path.getsize(p) if os.path.exists(p) else "MISSING")

if __name__ == "__main__":
    main()
