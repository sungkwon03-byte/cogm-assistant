#!/usr/bin/env python3
import os, datetime, pandas as pd, numpy as np

os.makedirs("logs", exist_ok=True)
LOG=f"logs/team_agg_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.log"
def w(m): print(m); open(LOG,"a",encoding="utf-8").write(m+"\n")

SRC="output/team_agg.csv"
if os.path.exists(SRC):
    df=pd.read_csv(SRC)
else:
    # 최소 스키마로 새로 만든다 (MLB 30행, 시즌은 2024 기본)
    ORGS=["ARI","ATL","BAL","BOS","CHC","CHW","CIN","CLE","COL","DET","HOU","KCR","LAA","LAD","MIA","MIL","MIN","NYM","NYY","OAK","PHI","PIT","SDP","SEA","SFG","STL","TBR","TEX","TOR","WSN"]
    df=pd.DataFrame({
        "league":["MLB"]*30, "season":[2024]*30, "group_role":["org"]*30, "group_id":ORGS,
        "players":[np.nan]*30, "total_pa_bf":[np.nan]*30, "avg_age":[np.nan]*30,
        "league_avg_age":[np.nan]*30, "age_diff":[np.nan]*30, "src_league":["MLB"]*30
    })

# WAR 관련 필드 Null 확정
for c in ["total_war","avg_war","league_total_war","league_avg_war","war_share"]:
    df[c]=np.nan

# 컬럼 순서 보정
cols=["league","season","group_role","group_id","players","total_war","avg_war",
      "total_pa_bf","avg_age","league_total_war","league_avg_war","league_avg_age",
      "war_share","age_diff","src_league"]
# 누락 컬럼 채우기
for c in cols:
    if c not in df.columns: df[c]=np.nan
df=df[cols]

df.to_csv("output/team_agg.csv", index=False)
w("[OK] team_agg WAR fields set to NULL (Day64-ready)")
