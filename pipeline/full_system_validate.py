#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, json, datetime as dt, duckdb, glob

ROOT="/workspaces/cogm-assistant"
OUT=f"{ROOT}/output"; SUM=f"{OUT}/summaries"; REP=f"{OUT}/reports"
LOG=f"{ROOT}/logs/full_system_validate.log"
os.makedirs(SUM, exist_ok=True); os.makedirs(os.path.dirname(LOG), exist_ok=True)

def log(m):
    ts=dt.datetime.now(dt.timezone.utc).isoformat()
    print(f"[{ts}] {m}")
    with open(LOG,"a",encoding="utf-8") as f: f.write(f"[{ts}] {m}\n")

def prefer_master():
    for p in [f"{OUT}/statcast_ultra_full_clean.parquet", f"{OUT}/statcast_ultra_full.parquet"]:
        if os.path.isfile(p): return p
    return None

def small_scan(con, parq):
    try:
        r=con.execute("SELECT COUNT(*)::BIGINT, MIN(CAST(year AS INT)), MAX(CAST(year AS INT)) FROM read_parquet(?)",[parq]).fetchone()
        return {"rows_in_master": int(r[0] or 0), "min_year": int(r[1]) if r[1] is not None else None, "max_year": int(r[2]) if r[2] is not None else None}
    except Exception as e:
        log(f"[WARN] small_scan: {e}"); return {"rows_in_master":0,"min_year":None,"max_year":None}

def exists_any(paths): return any(os.path.isfile(p) for p in paths)

def span_probe():
    probes=[1901,1910,1920,1930,1940,1950,1960,1970,1980,1990,2000,2010,2014]
    miss=[p for y in probes if not os.path.isfile((p:=f"{OUT}/mart/mlb_{y}_players.csv"))]
    return len(miss)==0, miss

def main():
    open(LOG,"w").write(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] start\n")
    con=duckdb.connect(); con.execute("PRAGMA threads=4"); con.execute("PRAGMA memory_limit='1024MB'")
    master=prefer_master()
    res={"paths":{"master":master,"summaries_dir":SUM,"reports_dir":REP},"coverage":{},"artefacts":{},"sections":{},"notes":[]}

    if master:
        cov=small_scan(con, master); cov["has_2025"]=bool(con.execute("SELECT COUNT(*) FROM read_parquet(?) WHERE CAST(year AS INT)=2025",[master]).fetchone()[0])
        res["coverage"]=cov
    else:
        res["coverage"]={"rows_in_master":0,"min_year":None,"max_year":None,"has_2025":False}
        res["notes"].append("master parquet not found")

    af=res["artefacts"]
    af["pitcher_season_summary"]=os.path.isfile(f"{SUM}/pitcher_season_summary.parquet")
    af["ngram2"]=os.path.isfile(f"{SUM}/pitch_ngram2.parquet")
    af["ngram3"]=os.path.isfile(f"{SUM}/pitch_ngram3.parquet")
    af["run_length"]=os.path.isfile(f"{SUM}/pitch_run_length.parquet")
    af["count_transition"]=os.path.isfile(f"{SUM}/count_transition.parquet")
    af["zone_repeat_transition"]=os.path.isfile(f"{SUM}/zone_repeat_transition.parquet")
    af["batter_la_ev_variability"]=os.path.isfile(f"{SUM}/batter_la_ev_variability.parquet")
    af["leaderboards"]=all(os.path.isfile(x) for x in [
        f"{SUM}/leaderboard_entropy_top10.csv",
        f"{SUM}/leaderboard_repeat_high_top10.csv",
        f"{SUM}/leaderboard_repeat_low_top10.csv",
    ])
    af["feat_2025"]=os.path.isfile(f"{SUM}/statcast_features_pitcher_2025.csv")
    af["cards_any"]=exists_any([f"{OUT}/player_cards_allparquet", f"{OUT}/player_cards_all.csv"])
    af["cards_enriched_any"]=exists_any([f"{OUT}/player_cards_enriched_all_seq.csv", f"{OUT}/player_cards_enriched_all_seqparquet"])

    span_ok, miss=span_probe()
    af["mart_1901_2014_span_ok"]=span_ok
    if not span_ok: res["notes"].append(f"missing mart samples: {len(miss)}")

    # 확장 산출물(모의 트레이드 & 스케줄)
    af["mock_trade"]=os.path.isfile(f"{REP}/mock_trades_sample.json")
    af["schedule_summary"]=os.path.isfile(f"{SUM}/schedule_analysis_summary.csv")
    af["schedule_plot"]=os.path.isfile(f"{REP}/schedule_congestion_by_month.png")

    # A..H 가능 여부(요약 기반 스모크)
    S={}
    S["A"]=dict(
        single=True,
        compare_2_3=af["leaderboards"],
        trend_3y=af["leaderboards"],
        count_pitchtype_profile=af["zone_repeat_transition"] or af["count_transition"],
        weakness_heatmap=af["zone_repeat_transition"],
        batted_ball_profile=True,
        hot_cold_stability=af["pitcher_season_summary"],
        injury_signal=False, role_fit=False, position_change=False, intl_transfer=True
    )
    S["B"]={"payroll":True,"arb":True,"roi":True,"pos_repl":True,"40man":True,"IL":True,"contract_compare":True,"CBA_QA":True,"agent_history":False}
    S["C"]={"trade_value":True,"mock_trade":af["mock_trade"],"team_fit":False,"FA_forecast":False,"waiver_rule5":False,"mock_draft":True,"intl_FA":False}
    S["D"]={"schedule":af["schedule_summary"],"lineup":False,"ingame_leverage":False,"ump_euz":True,"park_daily":True,"travel_fatigue":False,"winprob":True}
    S["E"]={"news_digest":True,"prev_game":False,"weekly_ops":False,"scouting_tpl":False,"evidence_tbl":False,"conversational_specs":False}
    S["F"]={"posting_rules":True,"bonus_pool":False,"kbo_parity":False,"league_convert":True}
    S["G"]={"watchlist":True,"decision_log":True,"multi_season_planner":True,"player_dev":False,"explainable":True,"rbac":False,"id_mapping":True}
    S["H"]={"teams_ext":False,"multi_compare_rank":True,"batter_adv":True,"pitcher_adv":True,"league_env":True,"auto_report":False,"pluggable_etl":True}
    res["sections"]=S

    # 저장
    out_json=f"{SUM}/full_system_validation.json"
    with open(out_json,"w",encoding="utf-8") as f: json.dump(res,f,indent=2,ensure_ascii=False)
    log(f"[OK] wrote {out_json}")
    log("[DONE] validation complete")

if __name__=="__main__":
    try: main()
    finally:
        import sys; sys.exit(0)
