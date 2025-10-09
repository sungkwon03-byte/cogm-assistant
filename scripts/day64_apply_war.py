#!/usr/bin/env python3
import os, sys, datetime, pandas as pd, numpy as np

if len(sys.argv)<2:
    print("usage: day64_apply_war.py <WAR_CSV_PATH>"); sys.exit(2)

war_path=sys.argv[1]
if not os.path.exists("output/team_agg.csv"):
    print("[-] output/team_agg.csv 없음. 먼저 team_agg_nullify_war.py 실행 필요."); sys.exit(2)

ta=pd.read_csv("output/team_agg.csv")
wdf=pd.read_csv(war_path)

# 유연 매핑
def pick(df,*cands):
    for c in cands:
        if c in df.columns: return c
    return None

# team/season 기준(팀 레벨 WAR)
k_season = pick(wdf,"season","Season","year","Year")
k_team   = pick(wdf,"Team","team","Tm","group_id","org","Org")
k_war    = pick(wdf,"total_war","WAR","war","fWAR","war_used","Bat WAR","Pit WAR")

if not k_season or not k_team or not k_war:
    print(f"[-] WAR CSV 매핑 실패 season={k_season}, team={k_team}, war={k_war}"); sys.exit(2)

wdf=wdf.rename(columns={k_season:"season", k_team:"group_id", k_war:"_WAR_"})
wdf["season"]=pd.to_numeric(wdf["season"],errors="coerce")
wdf=wdf.dropna(subset=["season"]).copy()
wdf["season"]=wdf["season"].astype(int)
wdf["_WAR_"]=pd.to_numeric(wdf["_WAR_"],errors="coerce").fillna(0.0)

# 팀 코드 정규화(간단)
wdf["group_id"]=wdf["group_id"].astype(str).str.upper().str.replace(r"\s+","",regex=True)
alias={"WSH":"WSN","WAS":"WSN","KC":"KCR","TB":"TBR","SD":"SDP","SF":"SFG"}
wdf["group_id"]=wdf["group_id"].replace(alias)

# 병합 후 WAR 필드 계산
ta=ta.drop(columns=["total_war","avg_war","league_total_war","league_avg_war","war_share"], errors="ignore")
ta=ta.merge(wdf.groupby(["season","group_id"],as_index=False)["_WAR_"].sum(), on=["season","group_id"], how="left")
ta["total_war"]=ta["_WAR_"]
ta["avg_war"]=ta["total_war"]/ta["players"].replace({0:np.nan})
league=ta.groupby("season",as_index=False).agg(league_total_war=("total_war","sum"),
                                              league_avg_war=("total_war","mean"))
ta=ta.drop(columns=["_WAR_"], errors="ignore").merge(league, on="season", how="left")
ta["war_share"]=ta["total_war"]/ta["league_total_war"].replace({0:np.nan})

ta.to_csv("output/team_agg.csv", index=False)
print("[OK] team_agg updated with Day64 WAR → output/team_agg.csv")

# Day57 롤링 재계산 호출
os.system("python scripts/day57_rolling.py")
