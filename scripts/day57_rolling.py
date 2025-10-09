#!/usr/bin/env python3
import os, sys, datetime, tempfile, pandas as pd, numpy as np

os.makedirs("logs", exist_ok=True); os.makedirs("output", exist_ok=True)
ts=datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
LOG=f"logs/trend_rolling_{ts}.log"
def w(m): print(m); open(LOG,"a",encoding="utf-8").write(m+"\n")

SRC="output/team_agg.csv"
if not os.path.exists(SRC):
    w(f"[-] {SRC} 없음"); sys.exit(2)

df=pd.read_csv(SRC)
def pick(*cands):
    for c in cands:
        if c in df.columns: return c
    return None

season = pick("season","Season","year","Year")
group  = pick("group_id","team_id","Team","Tm","player_id","player_uid","Name","Player")
war    = pick("total_war","war","WAR","fWAR","war_used")

if not season or not group:
    w(f"[-] 매핑 실패 season={season}, group={group}"); sys.exit(2)

df.rename(columns={season:"season_std", group:"entity_id"}, inplace=True)
df["season_std"]=pd.to_numeric(df["season_std"], errors="coerce")
df=df.dropna(subset=["season_std"]).copy()
df["season_std"]=df["season_std"].astype(int)

# WAR 없거나 전부 0/NaN이면 SKIP: rolling_war_3yr를 NaN으로 채우고 파일 생성
need_calc = False
if war and (pd.to_numeric(df[war], errors="coerce").fillna(0)!=0).any():
    need_calc = True

if need_calc:
    df[war]=pd.to_numeric(df[war], errors="coerce").fillna(0.0)
    df=df.sort_values(["entity_id","season_std"], kind="mergesort")
    def roll(s,w=3): return pd.to_numeric(s,errors="coerce").fillna(0.0).rolling(w, min_periods=1).mean()
    df["rolling_war_3yr"]=df.groupby("entity_id", sort=False)[war].transform(lambda s: roll(s,3))
    w(f"[OK] rolling computed with war_col={war}")
else:
    df["rolling_war_3yr"]=np.nan
    w("[SKIP] WAR unavailable → rolling_war_3yr set to NULL")

# 표준 메타
if "entity_type" not in df.columns:
    df["entity_type"]="team"  # 기본
# 저장(원자적)
tmp_fd,tmp_path=tempfile.mkstemp(prefix="trend_rolling_",suffix=".csv",dir="output"); os.close(tmp_fd)
df.to_csv(tmp_path, index=False)
os.replace(tmp_path, "output/trend_rolling.csv")
w(f"[OK] rows={len(df)} -> output/trend_rolling.csv")
w("[DONE]")
