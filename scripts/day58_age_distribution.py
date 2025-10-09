#!/usr/bin/env python3
import os, glob, datetime, pandas as pd, numpy as np
from dateutil.relativedelta import relativedelta

LOG=f"logs/day58_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.log"
def w(m): print(m); open(LOG,"a",encoding="utf-8").write(m+"\n")

SEARCH_DIRS=["mart","data","external","inputs","raw","downloads","."]

TEAM_CANDS=["Team","Tm","team","org","Org","group_id","team_id"]
SEASON_CANDS=["Season","season","Year","year","season_std"]
AGE_CANDS=["Age","age","player_age"]
DOB_CANDS=["DOB","Birthdate","Birth Date","birth_date","DoB","Date of Birth"]

MLB_ORGS={"ARI","ATL","BAL","BOS","CHC","CHW","CIN","CLE","COL","DET","HOU","KCR","LAA","LAD","MIA","MIL","MIN","NYM","NYY","OAK","PHI","PIT","SDP","SEA","SFG","STL","TBR","TEX","TOR","WSN"}
KBO_ORGS={"KIA","LOT","DOO","LG","SSG","KIW","HAN","SAM","NC","KT"}

def pick(df, cands):
    for c in cands:
        if c in df.columns: return c
    return None

def norm_team(x:str):
    s=str(x).strip().upper().replace(" ","")
    alias={"WSH":"WSN","WAS":"WSN","TB":"TBR","KC":"KCR","SD":"SDP","SF":"SFG"}
    return alias.get(s,s)

def guess_league(path, df):
    for c in ("league","League","src_league"):
        if c in df.columns:
            v = str(df[c].dropna().astype(str).str.upper().head(1).tolist()[0] if not df[c].dropna().empty else "")
            if v in {"MLB","KBO","MILB"}: return v
    p=path.lower()
    if "kbo" in p: return "KBO"
    if "mlb" in p: return "MLB"
    if "milb" in p or "minor" in p: return "MiLB"
    return "UNK"

def season_pivot_date(season:int):
    # 시즌 기준 나이 산정 기준일(6월 30일)
    return pd.Timestamp(season,6,30)

frames=[]
for root in SEARCH_DIRS:
    for p in glob.glob(os.path.join(root,"**","*.csv"), recursive=True):
        try:
            df=pd.read_csv(p)
        except Exception:
            continue
        if df.empty: continue
        team_col=pick(df,TEAM_CANDS)
        season_col=pick(df,SEASON_CANDS)
        if not team_col or not season_col: continue
        age_col=pick(df,AGE_CANDS)
        dob_col=pick(df,DOB_CANDS)

        if not age_col and not dob_col: 
            continue  # 나이 산출 불가

        sub=df[[season_col,team_col] + ([age_col] if age_col else []) + ([dob_col] if dob_col else [])].copy()
        sub.rename(columns={season_col:"season", team_col:"Team"}, inplace=True)
        sub["Team"]=sub["Team"].map(norm_team)
        lg=guess_league(p, df)
        sub["league"]=lg

        # 시즌 숫자화
        sub["season"]=pd.to_numeric(sub["season"], errors="coerce")
        sub=sub.dropna(subset=["season"])
        sub["season"]=sub["season"].astype(int)

        # 팀 필터 (MLB/KBO만, NPB/MiLB 제외)
        if lg=="MLB":
            sub=sub[sub["Team"].isin(MLB_ORGS)]
        elif lg=="KBO":
            sub=sub[sub["Team"].isin(KBO_ORGS)]
        else:
            continue

        # 나이 산출
        if age_col:
            sub["age"]=pd.to_numeric(sub[age_col], errors="coerce")
        else:
            # DOB로 계산
            dob=pd.to_datetime(sub[dob_col], errors="coerce", utc=True).dt.tz_localize(None)
            # 시즌 기준일에서 연령
            base=sub["season"].map(lambda s: season_pivot_date(int(s)))
            base=base.astype("datetime64[ns]")
            # age = (base - dob) in years
            delta=(base - dob)
            sub["age"]= (delta.dt.days / 365.2425)

        sub=sub.dropna(subset=["age"])
        frames.append(sub[["league","season","Team","age"]])

if not frames:
    w("[-] 나이 데이터 소스를 찾지 못했습니다."); raise SystemExit(2)

ages=pd.concat(frames, ignore_index=True, sort=False)
# 팀 평균 나이
team_age=ages.groupby(["league","season","Team"], as_index=False).agg(avg_age=("age","mean"))
# 리그 평균 나이
league_age=team_age.groupby(["league","season"], as_index=False).agg(league_avg_age=("avg_age","mean"))

# team_agg.csv에 병합
if not os.path.exists("output/team_agg.csv"):
    w("[-] output/team_agg.csv 없음. Day57 선행 필요"); raise SystemExit(2)

ta=pd.read_csv("output/team_agg.csv")
# team_agg의 팀 키
key_team="group_id" if "group_id" in ta.columns else "Team"
if "group_id" not in ta.columns and "Team" in ta.columns:
    ta.rename(columns={"Team":"group_id"}, inplace=True)
key_team="group_id"
if "league" not in ta.columns: ta["league"]="MLB"  # 기본

# 팀 코드 정규화
ta["group_id"]=ta["group_id"].map(norm_team)

# 병합
ta=ta.merge(team_age.rename(columns={"Team":"group_id"}), on=["league","season","group_id"], how="left")
ta=ta.merge(league_age, on=["league","season"], how="left")
ta["age_diff"]=ta["avg_age"] - ta["league_avg_age"]

# 저장
ta.to_csv("output/team_agg.csv", index=False)
w("[OK] avg_age/league_avg_age/age_diff 갱신 -> output/team_agg.csv")
