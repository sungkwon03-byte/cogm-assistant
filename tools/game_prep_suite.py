# === Co-GM append start (Day37-42) ===
# Game Prep Suite: Day37~42 (라인업, 레버리지 stub, EUZ, 파크팩터, 승률예측, 종합리포트)
# 사용법 예시는 맨 아래 참고

import argparse, pandas as pd, numpy as np
from math import radians, sin, cos, asin, sqrt
from datetime import datetime

# ---------- 공통 ----------
def _csv(path): return pd.read_csv(path)
def _save(df, path): df.to_csv(path, index=False); print(f"-> {path} ({len(df)} rows)")

# ---------- Day37: 라인업 최적화 ----------
# roster.csv: name,pos,bats,OPS_plus,vsR_OPS_plus,vsL_OPS_plus
def _adj_bat(row, pit_hand):
    base = row.get(f"vs{'R' if pit_hand=='R' else 'L'}_OPS_plus", np.nan)
    if np.isnan(base): base = row.get("OPS_plus", 100)
    platoon = 5 if ((pit_hand=="R" and row["bats"]=="L") or (pit_hand=="L" and row["bats"]=="R")) else 0
    return float(base) + platoon

def cmd_day37(args):
    df = _csv(args.roster)
    df["score"] = df.apply(lambda r: _adj_bat(r, args.pitcher_hand), axis=1)
    ln = df.sort_values("score", ascending=False).drop_duplicates("name").head(9).copy()
    # 간단 오더링: 1:고출루, 4:최고스코어
    ln = ln.reset_index(drop=True)
    order = [1,0,2,3]+list(range(4,len(ln)))
    order = [i for i in order if i<len(ln)]
    ln = ln.iloc[order].reset_index(drop=True)
    ln.insert(0, "batting_order", ln.index+1)
    _save(ln[["batting_order","name","pos","bats","score"]], args.out)

# ---------- Day38: 인게임 레버리지 STUB ----------
# bullpen.csv: name,role,hand,leverage_score,rested(0/1)
# state: inning,score_diff,base_out_state,opp_top(0/1)
def cmd_day38(args):
    pen = _csv(args.bullpen)
    # 필터: 쉬었는 투수 우선, leverage_score 내림차순
    pen = pen[pen["rested"]==1].sort_values(["leverage_score"], ascending=False)
    # 간단 룰: 7회+ & 접전(|score_diff|<=2) 이면 셋업/클로저 계열 우선
    state = {"inning":args.inning, "score_diff":args.score_diff, "base_out_state":args.base_out_state, "opp_top":args.opp_top}
    close_need = (args.inning>=7 and abs(args.score_diff)<=2)
    if close_need and len(pen):
        rec = pen.head(1).copy()
    else:
        rec = _csv(args.bullpen).sort_values(["rested","leverage_score"], ascending=[False,False]).head(1).copy()
    rec["reason"] = "HI-LEV" if close_need else "NEUTRAL"
    _save(rec, args.out)

# ---------- Day39: 심판 EUZ 모델 ----------
# umpires.csv: umpire, zone_expand_pct, low_strike_bias (0~1)
def cmd_day39(args):
    u = _csv(args.umpires)
    # 간단 점수화: 타자에 유리(+), 투수에 유리(-)
    # EUZ_adj = zone_expand*(-0.5) + low_strike*(-0.5)
    u["euz_adj_batter"] = (1 - u["zone_expand_pct"])*0.5 + (1 - u["low_strike_bias"])*0.5
    u["euz_adj_pitcher"] = -((u["zone_expand_pct"])*0.5 + (u["low_strike_bias"])*0.5)
    _save(u[["umpire","zone_expand_pct","low_strike_bias","euz_adj_batter","euz_adj_pitcher"]], args.out)

# ---------- Day40: 파크팩터(데일리) + 날씨 ----------
# weather.csv: venue,date,temp_c,wind_kph,precip_mm
# park.csv: venue,bat_pf_base,pit_pf_base  (장기 PF)
def cmd_day40(args):
    w = _csv(args.weather); p = _csv(args.park)
    df = w.merge(p, on="venue", how="left")
    # 간단 모델: temp(°C) 20→0, 30→+0.05 / wind 0→0, 30→+0.05 / precip 5mm→-0.05
    temp_adj = (df["temp_c"]-20).clip(lower=0, upper=10)/10*0.05
    wind_adj = df["wind_kph"].clip(0,30)/30*0.05
    precip_adj = -(df["precip_mm"].clip(0,5)/5*0.05)
    df["run_factor_daily"] = (df["bat_pf_base"]+df["pit_pf_base"])/2 + temp_adj + wind_adj + precip_adj
    _save(df[["venue","date","temp_c","wind_kph","precip_mm","run_factor_daily"]], args.out)

# ---------- Day41: 승률 예측 (elo+weather) ----------
# inputs: games.csv: date,home,away,home_elo,away_elo,venue
#         pf_daily.csv(from Day40): venue,date,run_factor_daily
def cmd_day41(args):
    g = _csv(args.games); pf = _csv(args.pf_daily)
    df = g.merge(pf, on=["venue","date"], how="left")
    elo_diff = df["home_elo"] - df["away_elo"]
    # 로지스틱: p = 1/(1+exp(-(k*(elo_diff/100) + b*(run_factor-1))))
    k, b = 1.0, 1.5
    x = k*(elo_diff/100.0) + b*((df["run_factor_daily"].fillna(1.0))-1.0)
    df["home_win_prob"] = 1/(1+np.exp(-x))
    _save(df[["date","home","away","home_win_prob"]], args.out)

# ---------- Day42: Game Prep 종합 리포트 ----------
# inputs: Day37 lineup.csv, Day38 bullpen_rec.csv, Day39 euz.csv, Day40 pf_daily.csv, Day41 winprob.csv
def cmd_day42(args):
    out = []
    def safe(path, need_cols):
        try:
            d = _csv(path)
            miss = [c for c in need_cols if c not in d.columns]
            if miss: raise Exception(f"cols missing {miss}")
            return d
        except Exception as e:
            return pd.DataFrame([{"_error": f"{path}: {e}"}])

    lineup = safe(args.lineup, ["batting_order","name","score"])
    penrec = safe(args.bullpen_rec, ["name","reason"])
    euz    = safe(args.euz, ["umpire","euz_adj_batter","euz_adj_pitcher"])
    pf     = safe(args.pf_daily, ["venue","date","run_factor_daily"])
    wp     = safe(args.winprob, ["date","home","away","home_win_prob"])

    # 요약
    rep = {
        "lineup_top3": ", ".join((lineup.sort_values("batting_order").head(3)["name"]).astype(str)) if "_error" not in lineup.columns else "ERR",
        "bullpen_choice": penrec.iloc[0]["name"] if ("_error" not in penrec.columns and len(penrec)) else "ERR",
        "umpire_bias_batter": float(euz["euz_adj_batter"].mean()) if "_error" not in euz.columns else np.nan,
        "run_factor_daily": float(pf["run_factor_daily"].mean()) if "_error" not in pf.columns else np.nan,
        "home_win_prob_avg": float(wp["home_win_prob"].mean()) if "_error" not in wp.columns else np.nan,
    }
    df = pd.DataFrame([rep])
    _save(df, args.out)

# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser(prog="game_prep_suite")
    sp = ap.add_subparsers(dest="cmd", required=True)

    p37 = sp.add_parser("day37"); p37.add_argument("--roster", required=True); p37.add_argument("--pitcher_hand", choices=["R","L"], required=True); p37.add_argument("--out", required=True); p37.set_defaults(func=cmd_day37)
    p38 = sp.add_parser("day38"); p38.add_argument("--bullpen", required=True); p38.add_argument("--inning", type=int, required=True); p38.add_argument("--score_diff", type=int, required=True); p38.add_argument("--base_out_state", required=True); p38.add_argument("--opp_top", type=int, choices=[0,1], required=True); p38.add_argument("--out", required=True); p38.set_defaults(func=cmd_day38)
    p39 = sp.add_parser("day39"); p39.add_argument("--umpires", required=True); p39.add_argument("--out", required=True); p39.set_defaults(func=cmd_day39)
    p40 = sp.add_parser("day40"); p40.add_argument("--weather", required=True); p40.add_argument("--park", required=True); p40.add_argument("--out", required=True); p40.set_defaults(func=cmd_day40)
    p41 = sp.add_parser("day41"); p41.add_argument("--games", required=True); p41.add_argument("--pf_daily", required=True); p41.add_argument("--out", required=True); p41.set_defaults(func=cmd_day41)
    p42 = sp.add_parser("day42"); p42.add_argument("--lineup", required=True); p42.add_argument("--bullpen_rec", required=True); p42.add_argument("--euz", required=True); p42.add_argument("--pf_daily", required=True); p42.add_argument("--winprob", required=True); p42.add_argument("--out", required=True); p42.set_defaults(func=cmd_day42)

    args = ap.parse_args(); args.func(args)

if __name__ == "__main__":
    main()
# === Co-GM append end (Day37-42) ===
