#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
visuals_force_real_all.py
- 실데이터(Statcast/Lahman 카드)만 사용.
- 칼럼 누락/이름 변형(px/pz, p_throws, bats 등) 자동 매핑.
- 플래툰 / 약점 / EUZ 3종을 '가능한 최대로' 생성.
- 필요한 파일 8개 경로를 항상 채움(불가 구간은 축소판/대체 시각화 + 메타 로그).
"""
import json, warnings
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path("/workspaces/cogm-assistant")
OUT  = ROOT/"output"
REP  = OUT/"reports"
SUM  = OUT/"summaries"
OUT.mkdir(parents=True, exist_ok=True); REP.mkdir(parents=True, exist_ok=True); SUM.mkdir(parents=True, exist_ok=True)

SC   = OUT/"statcast_ultra_full_clean.parquet"
CARDS= OUT/"player_cards_all.parquet"  # (읽지 않음; 확장용 자리)

REQ_PATHS = {
    "platoon_csv" : SUM/"platoon_split.csv",
    "platoon_png" : REP/"platoon_map.png",
    "weak_csv"    : SUM/"weakness_heatmap_matrix.csv",
    "weak_png"    : REP/"weakness_heatmap.png",
    "trend_pdf"   : REP/"trend_cards_3y.pdf",              # 이미 있으면 유지
    "euz_csv"     : SUM/"euz_umpire_impact.csv",
    "euz_png"     : REP/"ump_euz.png",
    "expl_png"    : REP/"explainable_attribution_topN.png" # 이미 있으면 유지
}

LOG_JSON   = SUM/"_visuals_force_real_log.json"
COLMAP_JSON= SUM/"_statcast_column_map.json"

def _exists(p: Path) -> bool:
    try: return p.exists()
    except: return False

def load_statcast():
    assert _exists(SC), f"Missing real Statcast parquet: {SC}"
    try:
        df = pd.read_parquet(SC)
    except Exception:
        df = pd.read_parquet(SC, engine="pyarrow")
    return df

def alias(df, want_names):
    mp = {}
    for key, cands in want_names.items():
        mp[key] = next((c for c in cands if c in df.columns), None)
    return mp

def ensure_png(fig, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)

def ensure_csv(df, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)

def weak_heatmap_from_coords(df, col_px, col_pz, col_pt, out_png):
    xb = pd.cut(pd.to_numeric(df[col_px], errors="coerce").clip(-0.83,0.83), bins=5, labels=False)
    zb = pd.cut(pd.to_numeric(df[col_pz], errors="coerce").clip(1.5,3.5), bins=5, labels=False)
    mat = pd.DataFrame({"pitch_type": df[col_pt].astype(str), "zone": xb.astype(str)+"x"+zb.astype(str)})
    mat = mat.groupby(["pitch_type","zone"]).size().reset_index(name="n")
    totals = mat.groupby("pitch_type")["n"].sum().sort_values(ascending=False)
    if len(totals)==0:
        return mat, False
    top_pt = totals.index[0]
    sel = mat[mat["pitch_type"]==top_pt].copy()

    def to_idx(s):
        try:
            a,b = s.split("x"); return int(a), int(b)
        except:
            return None
    sel = sel.assign(idx=sel["zone"].apply(to_idx)).dropna(subset=["idx"])
    grid = np.zeros((5,5))
    for _,r in sel.iterrows():
        a,b = r["idx"]
        if 0<=a<5 and 0<=b<5:
            grid[b,a] = r["n"]
    fig = plt.figure(figsize=(6,5))
    plt.imshow(grid, origin="lower")
    plt.title(f"Weakness heatmap ({top_pt})")
    plt.colorbar(label="count")
    ensure_png(fig, out_png)
    return mat, True

def weak_bar_from_pitchtype(df, col_pt, out_png):
    vc = df[col_pt].astype(str).value_counts().reset_index()
    vc.columns = ["pitch_type","n"]
    fig = plt.figure(figsize=(8,5))
    x = np.arange(len(vc))
    plt.bar(x, vc["n"])
    plt.xticks(x, vc["pitch_type"], rotation=45, ha="right")
    plt.title("Pitch-type frequency (no plate_x/plate_z present)")
    ensure_png(fig, out_png)
    vc["zone"] = "NA"
    vc = vc[["pitch_type","zone","n"]]
    return vc

def make_platoon(df, amap, paths, log):
    out_csv, out_png = paths["platoon_csv"], paths["platoon_png"]
    hb, hp = amap["batter_hand"], amap["pitcher_throws"]
    wv, wd = amap["woba_value"], amap["woba_denom"]
    w_est  = amap["estimated_woba_using_speedangle"]
    ev     = amap["launch_speed"]

    if hb and hp:
        work = pd.DataFrame({ "hb": df[hb].astype(str), "hp": df[hp].astype(str) })
        if wv and wd:
            woba_val = pd.to_numeric(df[wv], errors="coerce").fillna(0)
            woba_den = pd.to_numeric(df[wd], errors="coerce").replace(0, np.nan)
            work["woba_val"], work["woba_den"] = woba_val, woba_den
            log["platoon_woba_source"] = "woba_value/woba_denom"
        elif w_est:
            work["woba_val"] = pd.to_numeric(df[w_est], errors="coerce").fillna(0)
            work["woba_den"] = 1.0
            log["platoon_woba_source"] = "estimated_woba_using_speedangle"
        elif ev:
            arr = pd.to_numeric(df[ev], errors="coerce")
            std = arr.std(ddof=0)
            z = (arr - arr.mean())/std if std else arr*0
            work["woba_val"], work["woba_den"] = z.fillna(0), 1.0
            log["platoon_woba_source"] = "launch_speed_zscore"
        else:
            gc = work.groupby(["hb","hp"]).size().reset_index(name="PA")
            ensure_csv(gc, out_csv)
            fig = plt.figure(figsize=(8,5))
            x = np.arange(len(gc)); plt.bar(x, gc["PA"]); plt.xticks(x, gc["hp"]+"/"+gc["hb"])
            plt.title("Platoon (PA only) — wOBA proxy unavailable")
            ensure_png(fig, out_png)
            log["platoon_note"] = "wOBA proxy unavailable; PA only"
            return True

        grp = work.groupby(["hb","hp"]).agg(PA=("hb","count"),
                                            woba_val=("woba_val","sum"),
                                            woba_den=("woba_den","sum")).reset_index()
        grp["wOBA"] = grp["woba_val"]/grp["woba_den"]
        ensure_csv(grp.rename(columns={"hb":"batter_hand","hp":"pitcher_throws"}), out_csv)

        fig = plt.figure(figsize=(8,5))
        x = np.arange(len(grp))
        plt.bar(x, grp["wOBA"].fillna(0))
        plt.xticks(x, grp["pitcher_throws"].astype(str)+"/"+grp["batter_hand"].astype(str))
        plt.title("Platoon split (avg wOBA)")
        ensure_png(fig, out_png)
        return True

    # 손잡이 없을 때: pitch_type 기반 대체
    col_pt = amap["pitch_type"]
    if not col_pt:
        raise RuntimeError("no batter/pitcher hand and no pitch_type — cannot build platoon alt")

    src = None
    if wv and wd:
        wv_s = pd.to_numeric(df[wv], errors="coerce").fillna(0)
        wd_s = pd.to_numeric(df[wd], errors="coerce").replace(0, np.nan)
        tmp = pd.DataFrame({"pitch_type":df[col_pt].astype(str), "w": wv_s/wd_s})
        src = "woba_value/woba_denom"
    elif w_est:
        tmp = pd.DataFrame({"pitch_type":df[col_pt].astype(str),
                            "w": pd.to_numeric(df[w_est], errors="coerce").fillna(0)})
        src = "estimated_woba_using_speedangle"
    elif ev:
        arr = pd.to_numeric(df[ev], errors="coerce"); std = arr.std(ddof=0)
        z = (arr - arr.mean())/std if std else arr*0
        tmp = pd.DataFrame({"pitch_type":df[col_pt].astype(str), "w": z.fillna(0)})
        src = "launch_speed_zscore"
    else:
        vc = df[col_pt].astype(str).value_counts().reset_index()
        vc.columns = ["pitch_type","n"]; ensure_csv(vc, out_csv)
        fig = plt.figure(figsize=(8,5))
        x = np.arange(len(vc)); plt.bar(x, vc["n"]); plt.xticks(x, vc["pitch_type"], rotation=45, ha="right")
        plt.title("Platoon alt: pitch-type frequency (no hand & no proxy)")
        ensure_png(fig, out_png)
        log["platoon_note"] = "hand cols missing; saved pitch-type frequency"
        return True

    stat = tmp.groupby("pitch_type")["w"].mean().reset_index().rename(columns={"w":"proxy"})
    ensure_csv(stat, out_csv)
    fig = plt.figure(figsize=(8,5))
    x = np.arange(len(stat)); plt.bar(x, stat["proxy"]); plt.xticks(x, stat["pitch_type"], rotation=45, ha="right")
    plt.title(f"Platoon alt by pitch_type (source={src})")
    ensure_png(fig, out_png)
    log["platoon_note"] = f"hand cols missing; source={src}"
    return True

def make_weakness(df, amap, paths, log):
    out_csv, out_png = paths["weak_csv"], paths["weak_png"]
    col_pt = amap["pitch_type"]; col_px, col_pz = amap["plate_x"], amap["plate_z"]
    if col_pt and col_px and col_pz:
        mat, ok = weak_heatmap_from_coords(df, col_px, col_pz, col_pt, out_png)
        ensure_csv(mat, out_csv); log["weakness_mode"] = "5x5 heatmap"
        return ok
    if col_pt:
        vc = weak_bar_from_pitchtype(df, col_pt, out_png)
        ensure_csv(vc, out_csv); log["weakness_mode"] = "pitch-type bar (no plate coords)"
        return True
    # pitch_type 없으면 EV/LA density fallback
    ev = amap["launch_speed"]; la = amap["launch_angle"]
    if ev:
        X = pd.to_numeric(df[ev], errors="coerce")
        Y = pd.to_numeric(df[la], errors="coerce") if la else pd.Series([0]*len(df))
        H, xe, ye = np.histogram2d(X.fillna(0), Y.fillna(0), bins=20)
        fig = plt.figure(figsize=(6,5))
        plt.imshow(H.T, origin="lower", aspect="auto")
        plt.title("EV/LA density (fallback)"); plt.colorbar()
        ensure_png(fig, out_png)
        ensure_csv(pd.DataFrame({"info":["fallback_ev_la_only"]}), out_csv)
        log["weakness_mode"] = "EV/LA density fallback"
        return True
    raise RuntimeError("no pitch_type and no EV → cannot build weakness")

def make_euz(df, amap, paths, log):
    out_csv, out_png = paths["euz_csv"], paths["euz_png"]
    ump = amap["home_plate_umpire"]; desc = amap["description"]; typ = amap["type"]
    # taken 판정
    take_mask = pd.Series(False, index=df.index)
    if desc: take_mask |= df[desc].astype(str).str.lower().isin(["called_strike","ball"])
    if typ:  take_mask |= df[typ].astype(str).str.lower().isin(["s","b"])
    take = df[take_mask]

    if ump and len(take)>0:
        def cs_rate(x):
            a = pd.Series(False, index=x.index)
            if desc in x: a |= x[desc].astype(str).str.lower().eq("called_strike")
            if typ  in x: a |= x[typ].astype(str).str.lower().eq("s")
            return a.mean()
        byu = take.groupby(ump).apply(cs_rate).reset_index(name="cs_rate")
        liga = float(byu["cs_rate"].mean()) if len(byu) else 0.0
        byu["delta_vs_lg"] = byu["cs_rate"] - liga
        ensure_csv(byu, out_csv)

        s = byu.sort_values("delta_vs_lg", ascending=False)
        head, tail = s.head(10), s.tail(10)
        labels = list(head[ump]) + list(tail[ump])
        vals   = list(head["delta_vs_lg"]) + list(tail["delta_vs_lg"])
        fig = plt.figure(figsize=(8,6))
        x = np.arange(len(labels)); plt.bar(x, vals); plt.axhline(0, linewidth=1)
        plt.xticks(x, labels, rotation=90); plt.ylabel("Δ called strike% vs lg")
        plt.title("Umpire EUZ impact (Top/Bottom 10)")
        ensure_png(fig, out_png)
        log["euz_mode"] = "per-umpire delta"
        return True

    if (desc or typ) and len(take)>0:
        cnt = int(take.shape[0])
        ensure_csv(pd.DataFrame({"taken_rows":[cnt]}), out_csv)
        fig = plt.figure(figsize=(6,4))
        plt.bar([0],[cnt]); plt.xticks([0],["taken_pitches"]); plt.title("Taken pitch sample (no umpire id)")
        ensure_png(fig, out_png)
        log["euz_mode"] = "no-ump overall taken count"
        return True

    # 완전 최소 메타
    ensure_csv(pd.DataFrame({"note":["no umpire & no call columns"]}), out_csv)
    fig = plt.figure(figsize=(6,3)); plt.text(0.5,0.5,"No umpire/call columns", ha="center", va="center"); plt.axis("off")
    ensure_png(fig, out_png)
    log["euz_mode"] = "metadata only"
    return True

def main():
    log = {"steps":[], "notes":[]}
    df = load_statcast()
    amap = alias(df, {
        "player_name": ["player_name","batter_name","batter_full_name","name"],
        "batter_hand": ["batter_hand","stand","bats","batter_bats","batter_stand"],
        "pitcher_throws": ["pitcher_throws","p_throws","throws","pitcher_hand","p_hand"],
        "pitch_type": ["pitch_type","pitch_name","mlbam_pitch_name","ptype"],
        "woba_value": ["woba_value","woba_value_calc"],
        "woba_denom": ["woba_denom","woba_denom_calc"],
        "estimated_woba_using_speedangle": ["estimated_woba_using_speedangle","est_woba"],
        "launch_speed": ["launch_speed","ev","exit_velocity","launch_speed_angle"],
        "launch_angle": ["launch_angle","la","angle"],
        "plate_x": ["plate_x","px","plate_x_adj","plate_x_cor","plate_loc_x"],
        "plate_z": ["plate_z","pz","plate_z_adj","plate_z_cor","plate_loc_z"],
        "description": ["description","des","call","pitch_call"],
        "type": ["type","call_code"],
        "home_plate_umpire": ["home_plate_umpire","umpire","hp_umpire","umpire_name"]
    })
    COLMAP_JSON.write_text(json.dumps(amap, indent=2))

    ok = True
    try:
        ok &= make_platoon(df, amap, REQ_PATHS, log);   log["steps"].append("platoon:done")
    except Exception as e:
        log["steps"].append(f"platoon:fail:{e}"); ok = False
    try:
        ok &= make_weakness(df, amap, REQ_PATHS, log);  log["steps"].append("weakness:done")
    except Exception as e:
        log["steps"].append(f"weakness:fail:{e}"); ok = False
    try:
        ok &= make_euz(df, amap, REQ_PATHS, log);       log["steps"].append("euz:done")
    except Exception as e:
        log["steps"].append(f"euz:fail:{e}"); ok = False

    LOG_JSON.write_text(json.dumps({"ok":bool(ok), "log":log}, ensure_ascii=False, indent=2))
    print(json.dumps({"ok":bool(ok)}, indent=2))

if __name__ == "__main__":
    warnings.filterwarnings("ignore")
    try:
        main()
    except Exception as e:
        meta = {"fatal": str(e), "statcast_exists": _exists(SC), "statcast_path": str(SC)}
        LOG_JSON.write_text(json.dumps(meta, indent=2))
        for _,p in REQ_PATHS.items():
            p.parent.mkdir(parents=True, exist_ok=True)
            if p.suffix.lower()==".csv" and not p.exists():
                pd.DataFrame({"note":[f"failed:{type(e).__name__}"]}).to_csv(p, index=False)
            if p.suffix.lower()==".png" and not p.exists():
                fig=plt.figure(figsize=(6,3)); plt.text(0.5,0.5,"build failed; see _visuals_force_real_log.json", ha="center", va="center"); plt.axis("off")
                fig.savefig(p, dpi=140); plt.close(fig)
        print(json.dumps({"ok":False}, indent=2))
