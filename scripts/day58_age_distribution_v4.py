#!/usr/bin/env python3
import os, glob, datetime, pandas as pd, numpy as np, re

LOG=f"logs/day58_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.log"
def w(m): print(m); open(LOG,"a",encoding="utf-8").write(m+"\n")

# 탐색 루트(필요시 추가): output/logs/venv 등은 제외
SEARCH_DIRS=["mart","data","external","inputs","downloads",".","raw"]
EXCLUDE_SUBSTR = ["milb","minor","/output/","\\output\\","/logs/","\\logs\\","/venv","\\venv","/env","\\env"]

MLB_ORGS={"ARI","ATL","BAL","BOS","CHC","CHW","CIN","CLE","COL","DET","HOU","KCR","LAA","LAD","MIA","MIL","MIN","NYM","NYY","OAK","PHI","PIT","SDP","SEA","SFG","STL","TBR","TEX","TOR","WSN"}
KBO_ORGS={"KIA","LOT","DOO","LG","SSG","KIW","HAN","SAM","NC","KT"}

TEAM_CANDS=["Team","Tm","team","org","Org","group_id","team_id"]
SEASON_CANDS=["Season","season","Year","year","season_std"]
AGE_CANDS=["Age","age","player_age"]
DOB_CANDS=["DOB","Birthdate","Birth Date","birth_date","DoB","Date of Birth"]
LEAGUE_CANDS=["league","League","src_league"]

CHUNK=200_000

def norm_team(s):
    s=str(s).strip().upper().replace(" ","")
    alias={"WSH":"WSN","WAS":"WSN","TB":"TBR","KC":"KCR","SD":"SDP","SF":"SFG"}
    return alias.get(s,s)

def list_csvs():
    out=[]
    for root in SEARCH_DIRS:
        if not os.path.isdir(root): continue
        for p in glob.glob(os.path.join(root,"**","*.csv"), recursive=True):
            P=p.replace("\\","/")
            if any(x in P.lower() for x in EXCLUDE_SUBSTR): 
                continue
            if os.path.getsize(p)==0: 
                continue
            out.append(p)
    return sorted(set(out))

def pick(cols, cands):
    for c in cands:
        if c in cols: return c
    return None

def detect_league_sample(path):
    # 샘플 10k로 MLB/KBO 판단
    try:
        df=pd.read_csv(path, dtype=str, nrows=10_000, low_memory=False)
    except Exception:
        return None
    cols=df.columns.tolist()
    lcol=pick(cols, LEAGUE_CANDS)
    tcol=pick(cols, TEAM_CANDS)
    # 1) league 컬럼 우선
    if lcol:
        vs=set(df[lcol].dropna().astype(str).str.upper().str.strip().unique().tolist())
        if "MLB" in vs: return "MLB"
        if "KBO" in vs: return "KBO"
    # 2) 팀 코드로 추정
    if tcol:
        tt=df[tcol].dropna().astype(str).map(norm_team)
        if tt.isin(MLB_ORGS).any(): return "MLB"
        if tt.isin(KBO_ORGS).any(): return "KBO"
    return None

def candidate_files():
    files=list_csvs()
    cands={"MLB":[], "KBO":[]}
    for p in files:
        lg=detect_league_sample(p)
        if lg in ("MLB","KBO"):
            cands[lg].append(p)
    return cands

def iter_chunks(path, usecols):
    for ch in pd.read_csv(path, usecols=lambda c: c in usecols, dtype=str, chunksize=CHUNK, low_memory=False):
        yield ch

def compute_team_age_from_files(files, league):
    if not files: 
        return None
    sum_df={}  # (season, Team) -> [sum_age, cnt]
    used=[]
    for f in files:
        try:
            head=pd.read_csv(f, dtype=str, nrows=0, low_memory=False)
        except Exception:
            continue
        cols=head.columns.tolist()
        tcol=pick(cols, TEAM_CANDS); scol=pick(cols, SEASON_CANDS)
        acol=pick(cols, AGE_CANDS);  dcol=pick(cols, DOB_CANDS)
        if not tcol or not scol or (not acol and not dcol):
            continue
        need={tcol, scol}
        if acol: need.add(acol)
        if dcol: need.add(dcol)

        file_used=False
        for ch in iter_chunks(f, need):
            ch=ch.rename(columns={tcol:"Team", scol:"season"})
            ch["season"]=pd.to_numeric(ch["season"], errors="coerce")
            ch=ch.dropna(subset=["season"])
            if ch.empty: continue
            ch["season"]=ch["season"].astype(int)
            ch["Team"]=ch["Team"].map(norm_team)

            # 리그 팀코드 필터
            if league=="MLB":
                ch=ch[ch["Team"].isin(MLB_ORGS)]
            else:
                ch=ch[ch["Team"].isin(KBO_ORGS)]
            if ch.empty: continue

            if acol and (acol in ch.columns):
                ages=pd.to_numeric(ch[acol], errors="coerce")
            else:
                # DOB 기준 계산
                dob=pd.to_datetime(ch[dcol], errors="coerce", utc=True).dt.tz_localize(None)
                base=pd.to_datetime(ch["season"].astype(str)+"-06-30")
                ages=(base - dob).dt.days / 365.2425
            ch=ch.assign(age=ages).dropna(subset=["age"])
            if ch.empty: continue

            grp=ch.groupby(["season","Team"], as_index=False).agg(sum_age=("age","sum"), cnt=("age","size"))
            for _,r in grp.iterrows():
                key=(int(r["season"]), r["Team"])
                acc=sum_df.get(key, [0.0,0])
                sum_df[key]=[acc[0]+float(r["sum_age"]), acc[1]+int(r["cnt"])]
            file_used=True
        if file_used:
            used.append(f)

    if not sum_df:
        return None

    rows=[]
    for (season, team),(s,c) in sum_df.items():
        rows.append({"league":league,"season":season,"Team":team,"avg_age":(s/c) if c>0 else np.nan})
    team_age=pd.DataFrame(rows)
    league_age=team_age.groupby(["league","season"], as_index=False).agg(league_avg_age=("avg_age","mean"))
    return team_age, league_age, used

def main():
    os.makedirs("logs", exist_ok=True)
    cands=candidate_files()
    if not cands["MLB"] and not cands["KBO"]:
        w("[-] MLB/KBO 후보 파일을 찾지 못했습니다. 탐색 경로/파일 확인 필요.")
        # 힌트 출력
        allcsv=list_csvs()
        for p in allcsv[:30]:
            w(f"  hint: {p}")
        return 2

    w(f"[INFO] MLB files: {len(cands['MLB'])}, KBO files: {len(cands['KBO'])}")
    result_frames=[]; used_map={}
    for lg in ["MLB","KBO"]:
        res=compute_team_age_from_files(cands[lg], lg)
        if res is None:
            w(f"[-] {lg}: 나이 집계 실패(컬럼 부재/데이터 없음)")
            continue
        ta_lg, la_lg, used = res
        result_frames.append(("team", ta_lg))
        result_frames.append(("league", la_lg))
        used_map[lg]=used

    if not result_frames:
        w("[-] 어떤 리그에서도 나이 분포를 만들지 못했습니다.")
        return 2

    team_age=pd.concat([f for t,f in result_frames if t=="team"], ignore_index=True, sort=False)
    league_age=pd.concat([f for t,f in result_frames if t=="league"], ignore_index=True, sort=False)

    # team_agg 병합
    if not os.path.exists("output/team_agg.csv"):
        w("[-] output/team_agg.csv 없음"); return 2
    ta=pd.read_csv("output/team_agg.csv")
    if "group_id" not in ta.columns and "Team" in ta.columns:
        ta=ta.rename(columns={"Team":"group_id"})
    ta["group_id"]=ta["group_id"].map(norm_team)

    ta=ta.merge(team_age.rename(columns={"Team":"group_id"}), on=["league","season","group_id"], how="left")
    ta=ta.merge(league_age, on=["league","season"], how="left")
    ta["age_diff"]=ta["avg_age"] - ta["league_avg_age"]
    ta.to_csv("output/team_agg.csv", index=False)

    # 사용 파일 로그
    with open("logs/day58_used_sources.txt","w",encoding="utf-8") as f:
        for lg,paths in used_map.items():
            f.write(f"[{lg}] used {len(paths)} files\n")
            for p in paths: f.write(p+"\n")

    w("[OK] Day58 v4: avg_age/league_avg_age/age_diff 갱신 완료 -> output/team_agg.csv")
    return 0

if __name__=="__main__":
    import sys; sys.exit(main())
