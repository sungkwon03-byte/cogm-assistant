#!/usr/bin/env python3
import os, glob, datetime, pandas as pd, numpy as np

LOG=f"logs/day58_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.log"
def w(m): print(m); open(LOG,"a",encoding="utf-8").write(m+"\n")

SEARCH_DIRS=["mart","data","external","inputs","downloads","."]
EXCLUDE=("minor","milb","/logs/","\\logs\\","/output/","\\output\\","/venv","\\venv","/env","\\env")

MLB_ORGS={"ARI","ATL","BAL","BOS","CHC","CHW","CIN","CLE","COL","DET","HOU","KCR","LAA","LAD","MIA","MIL","MIN","NYM","NYY","OAK","PHI","PIT","SDP","SEA","SFG","STL","TBR","TEX","TOR","WSN"}
KBO_ORGS={"KIA","LOT","DOO","LG","SSG","KIW","HAN","SAM","NC","KT"}

TEAM_CANDS=["Team","Tm","team","org","Org","group_id","team_id"]
SEASON_CANDS=["Season","season","Year","year","season_std"]
AGE_CANDS=["Season Age","season_age","SeasonAge","Age","age","player_age"]
DOB_CANDS=["DOB","Dob","dob","Birthdate","Birth Date","birth_date","Date of Birth"]
BIRTHY_CANDS=["BirthYear","birth_year","Birth Year","YOB","yob"]
BIRTHM_CANDS=["BirthMonth","birth_month","Birth Month","mob","MoB"]
BIRTHD_CANDS=["BirthDay","birth_day","Birth Day","dob_day","DoB_Day"]

CHUNK=200_000

def pick(cols, cands):
    for c in cands:
        if c in cols: return c
    return None

def norm_team(s):
    s=str(s).strip().upper().replace(" ","")
    alias={"WSH":"WSN","WAS":"WSN","TB":"TBR","KC":"KCR","SD":"SDP","SF":"SFG"}
    return alias.get(s,s)

def list_csvs():
    out=[]
    for root in SEARCH_DIRS:
        if not os.path.isdir(root): continue
        for p in glob.glob(os.path.join(root,"**","*.csv"), recursive=True):
            P=p.replace("\\","/").lower()
            if any(k in P for k in EXCLUDE): 
                continue
            try:
                if os.path.getsize(p)>0: out.append(p)
            except Exception:
                pass
    return sorted(set(out))

def league_of_file(p, sample):
    # league 열 우선
    lcol=pick(sample.columns, ["league","League","src_league"])
    if lcol:
        vals=set(sample[lcol].dropna().astype(str).str.upper().unique().tolist())
        if "MLB" in vals: return "MLB"
        if "KBO" in vals: return "KBO"
    # 파일명 힌트
    L=p.lower()
    if "mlb" in L: return "MLB"
    if "kbo" in L: return "KBO"
    # 팀 코드 힌트
    tcol=pick(sample.columns, TEAM_CANDS)
    if tcol:
        tt=sample[tcol].dropna().astype(str).map(norm_team)
        if tt.isin(MLB_ORGS).any(): return "MLB"
        if tt.isin(KBO_ORGS).any(): return "KBO"
    return None

def iter_chunks(path, usecols):
    for ch in pd.read_csv(path, usecols=lambda c: c in usecols, dtype=str, chunksize=CHUNK, low_memory=False):
        yield ch

def season_pivot(s):
    # 시즌 기준일 6/30
    return pd.Timestamp(int(s),6,30)

def derive_age_frame(path, league):
    used=False
    sum_age={}  # (season, team) -> [sum, cnt]
    # 헤더 조사
    try:
        head=pd.read_csv(path, nrows=0, dtype=str, low_memory=False)
    except Exception:
        return None, used
    cols=head.columns.tolist()
    tcol=pick(cols, TEAM_CANDS); scol=pick(cols, SEASON_CANDS)
    if not tcol or not scol: 
        return None, used

    acol=pick(cols, AGE_CANDS)
    dcol=pick(cols, DOB_CANDS)
    ycol=pick(cols, BIRTHY_CANDS)
    mcol=pick(cols, BIRTHM_CANDS)
    dcol2=pick(cols, BIRTHD_CANDS)

    need={tcol, scol}
    for c in (acol, dcol, ycol, mcol, dcol2):
        if c: need.add(c)

    if len(need)==2 and not (acol or dcol or ycol):
        # 나이 관련 컬럼 아무 것도 없음
        return None, used

    for ch in iter_chunks(path, need):
        ch=ch.rename(columns={tcol:"Team", scol:"season"})
        ch["season"]=pd.to_numeric(ch["season"], errors="coerce")
        ch=ch.dropna(subset=["season"])
        if ch.empty: 
            continue
        ch["season"]=ch["season"].astype(int)
        ch["Team"]=ch["Team"].map(norm_team)

        # 리그 팀 필터
        if league=="MLB": ch=ch[ch["Team"].isin(MLB_ORGS)]
        else:             ch=ch[ch["Team"].isin(KBO_ORGS)]
        if ch.empty: 
            continue

        # 1) Age/SeasonAge 직접 사용
        ages=None
        if acol and acol in ch.columns:
            ages=pd.to_numeric(ch[acol], errors="coerce")

        # 2) DOB로 계산
        if ages is None and dcol and dcol in ch.columns:
            dob=pd.to_datetime(ch[dcol], errors="coerce", utc=True).dt.tz_localize(None)
            base=pd.to_datetime(ch["season"].astype(str) + "-06-30")
            ages=(base - dob).dt.days / 365.2425

        # 3) BirthYear(+Month/+Day) 조합으로 근사
        if ages is None and ycol and ycol in ch.columns:
            by=pd.to_numeric(ch[ycol], errors="coerce")
            # 월/일 없으면 7/1로 가정(보수적으로 시즌 중간 즈음)
            bm = pd.to_numeric(ch[mcol], errors="coerce") if mcol and mcol in ch.columns else 7
            bd = pd.to_numeric(ch[dcol2], errors="coerce") if dcol2 and dcol2 in ch.columns else 1
            # 안전한 날짜 생성
            def safe_date(y, m, d):
                y=int(y); m=int(m) if not pd.isna(m) else 7; d=int(d) if not pd.isna(d) else 1
                m = min(max(m,1),12); d = min(max(d,1),28)
                return pd.Timestamp(y, m, d)
            base=pd.to_datetime(ch["season"].astype(int).astype(str)+"-06-30")
            bdates=[safe_date(y, bm if np.isscalar(bm) else bm.iloc[i], bd if np.isscalar(bd) else bd.iloc[i]) if not pd.isna(y) else pd.NaT
                    for i,y in enumerate(by)]
            bdates=pd.to_datetime(bdates)
            ages=(base - bdates).dt.days / 365.2425

        if ages is None:
            continue

        ch=ch.assign(age=ages)
        ch=ch.dropna(subset=["age"])
        if ch.empty: 
            continue

        grp=ch.groupby(["season","Team"], as_index=False).agg(sum_age=("age","sum"), cnt=("age","size"))
        for _,r in grp.iterrows():
            key=(int(r["season"]), r["Team"])
            prev=sum_age.get(key, [0.0,0])
            sum_age[key]=[prev[0]+float(r["sum_age"]), prev[1]+int(r["cnt"])]
        used=True

    if not sum_age:
        return None, used

    rows=[]
    for (season,team),(s,c) in sum_age.items():
        rows.append({"league":league,"season":season,"Team":team,"avg_age": (s/c) if c>0 else np.nan})
    team_age=pd.DataFrame(rows)
    league_age=team_age.groupby(["league","season"], as_index=False).agg(league_avg_age=("avg_age","mean"))
    return (team_age, league_age), used

def main():
    os.makedirs("logs", exist_ok=True)
    files=list_csvs()
    if not files:
        w("[-] 후보 CSV가 없습니다."); return 2

    mlb_team=None; mlb_league=None; kbo_team=None; kbo_league=None
    used_files={"MLB":[], "KBO":[]}
    # 파일별로 샘플 읽어 리그 판정
    for f in files:
        try: sample=pd.read_csv(f, nrows=2000, dtype=str, low_memory=False)
        except Exception: continue
        lg=league_of_file(f, sample)
        if lg not in ("MLB","KBO"): 
            continue
        res, used = derive_age_frame(f, lg)
        if res is None: 
            continue
        team_age, league_age = res
        if lg=="MLB":
            mlb_team = team_age if mlb_team is None else pd.concat([mlb_team, team_age], ignore_index=True)
            mlb_league = league_age if mlb_league is None else pd.concat([mlb_league, league_age], ignore_index=True)
        else:
            kbo_team = kbo_team if kbo_team is not None else None
            kbo_team = team_age if kbo_team is None else pd.concat([kbo_team, team_age], ignore_index=True)
            kbo_league = league_age if kbo_league is None else pd.concat([kbo_league, league_age], ignore_index=True)
        if used: used_files[lg].append(f)

    frames=[]
    leagues=[]
    if mlb_team is not None:
        mlb_team=mlb_team.groupby(["league","season","Team"], as_index=False).agg(avg_age=("avg_age","mean"))
        mlb_league=mlb_league.groupby(["league","season"], as_index=False).agg(league_avg_age=("league_avg_age","mean"))
        frames.append(("team", mlb_team)); frames.append(("league", mlb_league)); leagues.append("MLB")
    if kbo_team is not None:
        kbo_team=kbo_team.groupby(["league","season","Team"], as_index=False).agg(avg_age=("avg_age","mean"))
        kbo_league=kbo_league.groupby(["league","season"], as_index=False).agg(league_avg_age=("league_avg_age","mean"))
        frames.append(("team", kbo_team)); frames.append(("league", kbo_league)); leagues.append("KBO")

    if not frames:
        w("[-] 나이 산출 가능한 컬럼을 아무 후보에서도 찾지 못했습니다.")
        # 어떤 파일이 왜 제외됐는지 보조 로그
        scan_hits=[]
        for f in files[:50]:
            try: head=pd.read_csv(f, nrows=0, dtype=str)
            except Exception: continue
            cols=head.columns.tolist()
            hits=[c for c in cols if any(h in c.lower() for h in ("age","seasonage","dob","birth","yob"))]
            if hits:
                scan_hits.append((f, hits[:20]))
        if scan_hits:
            for f,h in scan_hits:
                w(f"[HIT] {f} -> {h}")
        else:
            w("[HIT] age/dob/birth 관련 컬럼이 아예 없음")
        return 2

    team_age=pd.concat([f for t,f in frames if t=="team"], ignore_index=True)
    league_age=pd.concat([f for t,f in frames if t=="league"], ignore_index=True)

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

    with open("logs/day58_used_sources.txt","w",encoding="utf-8") as f:
        for lg,paths in used_files.items():
            f.write(f"[{lg}] used {len(paths)} files\n")
            for p in paths: f.write(p+"\n")

    w("[OK] Day58 v5: avg_age/league_avg_age/age_diff 갱신 완료 -> output/team_agg.csv")
    return 0

if __name__=="__main__":
    import sys; sys.exit(main())
