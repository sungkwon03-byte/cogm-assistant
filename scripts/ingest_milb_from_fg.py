#!/usr/bin/env python3
import argparse, os, sys
import pandas as pd, numpy as np

SEASON_DEFAULT = 2024
HIT_MAP = {"Name":"name_full","Team":"team_id","PA":"pa","AVG":"avg","OBP":"obp","SLG":"slg","wOBA":"woba","wRC+":"wrc_plus","BB%":"bb_pct","K%":"k_pct","ISO":"iso","BABIP":"babip","EV":"ev_avg","Pull%":"pull_pct","Oppo%":"oppo_pct","GB%":"gb_pct","FB%":"fb_pct","LD%":"ld_pct","playerid":"fg_playerid"}
PIT_MAP = {"Name":"name_full","Team":"team_id","BF":"bf","ERA":"era","FIP":"fip","xFIP":"xfip","K/9":"k9","BB/9":"bb9","HR/9":"hr9","CSW%":"csw_pct","SwStr%":"whiff_pct","GB%":"gb_pct","playerid":"fg_playerid"}
def norm_team(x): return str(x).strip()
def build_uid(df):
    if "fg_playerid" in df.columns and df["fg_playerid"].notna().any(): return "fg:"+df["fg_playerid"].astype(str)
    base=df["name_full"].fillna("unknown").str.lower().str.replace(r"\s+","_",regex=True)
    return "milb_tmp:"+base+":"+df["season"].astype(str)
def to_float(df, cols):
    for c in cols:
        if c in df.columns: df[c]=pd.to_numeric(df[c], errors="coerce")
    return df
def main(a):
    os.makedirs("mart", exist_ok=True); os.makedirs("dim", exist_ok=True)
    hit=pd.read_csv(a.hitters_csv).rename(columns=HIT_MAP)
    pit=pd.read_csv(a.pitchers_csv).rename(columns=PIT_MAP)
    for df in (hit,pit): df["league"]="MiLB"; df["season"]=a.season; df["team_id"]=df["team_id"].map(norm_team)
    hit=to_float(hit, ["pa","avg","obp","slg","woba","wrc_plus","bb_pct","k_pct","iso","babip","ev_avg","pull_pct","oppo_pct","gb_pct","fb_pct","ld_pct"])
    pit=to_float(pit, ["bf","era","fip","xfip","k9","bb9","hr9","csw_pct","whiff_pct","gb_pct"])
    hit["player_uid"]=build_uid(hit); pit["player_uid"]=build_uid(pit)
    bat_cols=["player_uid","season","league","team_id","name_full","avg","obp","slg","woba","wrc_plus","bb_pct","k_pct","iso","babip","ev_avg","pull_pct","oppo_pct","gb_pct","fb_pct","ld_pct","pa"]
    pit_cols=["player_uid","season","league","team_id","name_full","era","fip","xfip","k9","bb9","hr9","csw_pct","whiff_pct","gb_pct","bf"]
    hit.reindex(columns=[c for c in bat_cols if c in hit.columns]).drop_duplicates(["player_uid","season","league"]).to_csv("mart/fact_batting.csv", index=False)
    pit.reindex(columns=[c for c in pit_cols if c in pit.columns]).drop_duplicates(["player_uid","season","league"]).to_csv("mart/fact_pitching.csv", index=False)
    fw=pd.DataFrame({"player_uid":hit["player_uid"],"season":a.season,"league":"MiLB","team_id":hit["team_id"],"name_full":hit["name_full"],"war":hit["WAR"] if "WAR" in hit.columns else np.nan,"wraa":hit["wRAA"] if "wRAA" in hit.columns else np.nan,"raa_def":np.nan,"raa_bsr":np.nan,"standard_scale":"milb_std"})
    fw.to_csv("mart/fact_war.csv", index=False)
    pd.DataFrame(columns=["player_uid","season","league","il_days","injury_risk_tier"]).to_csv("mart/fact_health.csv", index=False)
    pd.DataFrame(columns=["player_uid","season","league","trend_war_3yr","trend_form_30d"]).to_csv("mart/fact_trend.csv", index=False)
    print(f"[OK] MiLB {a.season} → mart/*")
if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("--hitters-csv", required=True); ap.add_argument("--pitchers-csv", required=True)
    ap.add_argument("--season", type=int, default=SEASON_DEFAULT)
    sys.exit(main(ap.parse_args()))
