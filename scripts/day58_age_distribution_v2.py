#!/usr/bin/env python3
import os, glob, datetime, pandas as pd, numpy as np

LOG=f"logs/day58_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.log"
def w(m): print(m); open(LOG,"a",encoding="utf-8").write(m+"\n")

SEARCH_DIRS=["mart","data","external"]  # 범위 축소
TEAM_CANDS=["Team","Tm","team","org","Org","group_id","team_id"]
SEASON_CANDS=["Season","season","Year","year","season_std"]
AGE_CANDS=["Age","age","player_age"]
DOB_CANDS=["DOB","Birthdate","Birth Date","birth_date","DoB","Date of Birth"]
MLB_ORGS={"ARI","ATL","BAL","BOS","CHC","CHW","CIN","CLE","COL","DET","HOU","KCR","LAA","LAD","MIA","MIL","MIN","NYM","NYY","OAK","PHI","PIT","SDP","SEA","SFG","STL","TBR","TEX","TOR","WSN"}
KBO_ORGS={"KIA","LOT","DOO","LG","SSG","KIW","HAN","SAM","NC","KT"}

ROW_LIMIT = 2_000_000  # 파일당 최대 읽기 보호

def pick(df, cands):
    for c in cands:
        if c in df.columns: return c
    return None

def norm_team(x:str):
    s=str(x).strip().upper().replace(" ","")
    alias={"WSH":"WSN","WAS":"WSN","TB":"TBR","KC":"KCR","SD":"SDP","SF":"SFG"}
    return alias.get(s,s)

def guess_league_from_path(p):
    p=p.lower()
    if "kbo" in p: return "KBO"
    if "mlb" in p: return "MLB"
    if "milb" in p or "minor" in p: return "MiLB"
    return "UNK"

frames=[]
for root in SEARCH_DIRS:
    if not os.path.isdir(root): continue
    for p in glob.glob(os.path.join(root,"**","*.csv"), recursive=True):
        # MiLB 제외 (요구: MLB/KBO만)
        if "milb" in p.lower() or "minor" in p.lower(): 
            continue
        try:
            df=pd.read_csv(p, dtype=str, low_memory=False, nrows=ROW_LIMIT)
        except Exception:
            continue
        if df.empty: continue
        team_col=pick(df, TEAM_CANDS)
        season_col=pick(df, SEASON_CANDS)
        if not team_col or not season_col: 
            continue
        age_col=pick(df, AGE_CANDS)
        dob_col=pick(df, DOB_CANDS)
        if not age_col and not dob_col:
            continue

        sub=df[[season_col, team_col] + ([age_col] if age_col else []) + ([dob_col] if dob_col else [])].copy()
        sub.rename(columns={season_col:"season", team_col:"Team"}, inplace=True)
        sub["Team"]=sub["Team"].map(norm_team)
        lg=guess_league_from_path(p)
        if lg not in {"MLB","KBO"}: 
            # 컬럼에 league 있으면 활용
            if "league" in df.columns:
                lg=df["league"].dropna().astype(str).str.upper().head(1).tolist() or ["UNK"]
                lg=lg[0]
            else:
                continue
        sub["league"]=lg

        # 시즌 숫자화
        sub["season"]=pd.to_numeric(sub["season"], errors="coerce")
        sub=sub.dropna(subset=["season"])
        sub["season"]=sub["season"].astype(int)

        # 팀 필터
        if lg=="MLB":
            sub=sub[sub["Team"].isin(MLB_ORGS)]
        elif lg=="KBO":
            sub=sub[sub["Team"].isin(KBO_ORGS)]

        if sub.empty: 
            continue

        # 나이 산출
        if age_col:
            sub["age"]=pd.to_numeric(sub[age_col], errors="coerce")
        else:
            dob=pd.to_datetime(sub[dob_col], errors="coerce", utc=True).dt.tz_localize(None)
            base=pd.to_datetime(sub["season"].astype(str)+"-06-30")
            sub["age"]=(base - dob).dt.days / 365.2425

        sub=sub.dropna(subset=["age"])
        if not sub.empty:
            frames.append(sub[["league","season","Team","age"]])

if not frames:
    w("[-] 나이 데이터 소스를 찾지 못했습니다."); raise SystemExit(2)

ages=pd.concat(frames, ignore_index=True, sort=False)

team_age=ages.groupby(["league","season","Team"], as_index=False).agg(avg_age=("age","mean"))
league_age=team_age.groupby(["league","season"], as_index=False).agg(league_avg_age=("avg_age","mean"))

if not os.path.exists("output/team_agg.csv"):
    w("[-] output/team_agg.csv 없음"); raise SystemExit(2)

ta=pd.read_csv("output/team_agg.csv")
# 표준 키 정리
if "group_id" not in ta.columns and "Team" in ta.columns:
    ta=ta.rename(columns={"Team":"group_id"})
ta["group_id"]=ta["group_id"].map(norm_team)

# 병합
ta=ta.merge(team_age.rename(columns={"Team":"group_id"}), on=["league","season","group_id"], how="left")
ta=ta.merge(league_age, on=["league","season"], how="left")
ta["age_diff"]=ta["avg_age"] - ta["league_avg_age"]

# 저장
os.makedirs("output", exist_ok=True)
ta.to_csv("output/team_agg.csv", index=False)
w("[OK] Day58: avg_age/league_avg_age/age_diff 갱신 완료")
