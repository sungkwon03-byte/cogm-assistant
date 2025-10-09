#!/usr/bin/env python3
import os, glob, datetime, pandas as pd, numpy as np

LOG=f"logs/day58_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.log"
def w(m): print(m); open(LOG,"a",encoding="utf-8").write(m+"\n")

MLB_ORGS={"ARI","ATL","BAL","BOS","CHC","CHW","CIN","CLE","COL","DET","HOU","KCR","LAA","LAD","MIA","MIL","MIN","NYM","NYY","OAK","PHI","PIT","SDP","SEA","SFG","STL","TBR","TEX","TOR","WSN"}
KBO_ORGS={"KIA","LOT","DOO","LG","SSG","KIW","HAN","SAM","NC","KT"}

TEAM_CANDS=["Team","Tm","team","org","Org","group_id","team_id"]
SEASON_CANDS=["Season","season","Year","year","season_std"]
AGE_CANDS=["Age","age","player_age"]
DOB_CANDS=["DOB","Birthdate","Birth Date","birth_date","DoB","Date of Birth"]

def pick(cols, cands):
    for c in cands:
        if c in cols: return c
    return None

def norm_team(s):
    s=str(s).strip().upper().replace(" ","")
    alias={"WSH":"WSN","WAS":"WSN","TB":"TBR","KC":"KCR","SD":"SDP","SF":"SFG"}
    return alias.get(s, s)

def files():
    # mart 폴더만 대상, MLB/KBO만
    pats = ["mart/mlb_*_players.csv","mart/mlb_all_players.csv",
            "mart/kbo_*_players.csv","mart/kbo_all_players.csv"]
    out=[]
    for p in pats:
        out.extend(glob.glob(p))
    return sorted(set([f for f in out if os.path.getsize(f)>0]))

def iter_chunks(path, usecols):
    # 청크 처리로 메모리 보호
    for ch in pd.read_csv(path, usecols=lambda c: c in usecols, dtype=str, chunksize=200_000, low_memory=False):
        yield ch

def compute_team_age():
    sum_df = {}  # key=(league,season,Team) -> [sum_age, cnt]
    flist = files()
    if not flist:
        w("[-] mart 내 MLB/KBO 플레이어 파일이 없습니다."); return None
    w(f"[INFO] files: {len(flist)}")
    for f in flist:
        try:
            head = pd.read_csv(f, nrows=0)
        except Exception:
            continue
        cols = head.columns.tolist()
        tcol = pick(cols, TEAM_CANDS); scol = pick(cols, SEASON_CANDS)
        acol = pick(cols, AGE_CANDS);  dcol = pick(cols, DOB_CANDS)
        if not tcol or not scol or (not acol and not dcol):
            continue
        need = {tcol, scol}
        if acol: need.add(acol)
        if dcol: need.add(dcol)

        league = "MLB" if "mlb_" in os.path.basename(f).lower() else ("KBO" if "kbo_" in os.path.basename(f).lower() else "UNK")
        if league not in {"MLB","KBO"}: 
            continue

        for ch in iter_chunks(f, need):
            ch = ch.rename(columns={tcol:"Team", scol:"season"})
            ch["season"]=pd.to_numeric(ch["season"], errors="coerce")
            ch=ch.dropna(subset=["season"])
            ch["season"]=ch["season"].astype(int)
            ch["Team"]=ch["Team"].map(norm_team)

            # 리그별 팀 필터
            if league=="MLB":
                ch = ch[ch["Team"].isin(MLB_ORGS)]
            else:
                ch = ch[ch["Team"].isin(KBO_ORGS)]
            if ch.empty: 
                continue

            if acol and acol in ch.columns:
                ages = pd.to_numeric(ch[acol], errors="coerce")
            else:
                # DOB로 계산 (기준일: 시즌 6/30)
                dob = pd.to_datetime(ch[dcol], errors="coerce", utc=True).dt.tz_localize(None)
                base = pd.to_datetime(ch["season"].astype(str) + "-06-30")
                ages = (base - dob).dt.days / 365.2425

            ch = ch.assign(age=ages).dropna(subset=["age"])
            if ch.empty: 
                continue

            grp = ch.groupby(["season","Team"], as_index=False).agg(sum_age=("age","sum"), cnt=("age","size"))
            for _, r in grp.iterrows():
                key=(league, int(r["season"]), r["Team"])
                prev=sum_df.get(key, [0.0, 0])
                sum_df[key]=[prev[0]+float(r["sum_age"]), prev[1]+int(r["cnt"])]

    if not sum_df:
        return None

    # 집계 → 팀 평균 나이
    rows=[]
    for (lg,season,team),(s,c) in sum_df.items():
        rows.append({"league":lg,"season":season,"Team":team,"avg_age": (s/c) if c>0 else np.nan})
    team_age=pd.DataFrame(rows)
    # 리그 평균(팀 평균의 단순 평균)
    league_age=team_age.groupby(["league","season"], as_index=False).agg(league_avg_age=("avg_age","mean"))
    return team_age, league_age

def main():
    res = compute_team_age()
    if res is None:
        w("[-] 나이 데이터 소스가 없음/파싱 실패"); return 2
    team_age, league_age = res

    if not os.path.exists("output/team_agg.csv"):
        w("[-] output/team_agg.csv 없음"); return 2
    ta=pd.read_csv("output/team_agg.csv")
    if "group_id" not in ta.columns and "Team" in ta.columns:
        ta=ta.rename(columns={"Team":"group_id"})
    ta["group_id"]=ta["group_id"].map(norm_team)

    # 병합
    ta=ta.merge(team_age.rename(columns={"Team":"group_id"}), on=["league","season","group_id"], how="left")
    ta=ta.merge(league_age, on=["league","season"], how="left")
    ta["age_diff"]=ta["avg_age"] - ta["league_avg_age"]

    ta.to_csv("output/team_agg.csv", index=False)
    w("[OK] Day58 v3: avg_age/league_avg_age/age_diff 갱신 완료 -> output/team_agg.csv")
    return 0

if __name__=="__main__":
    import sys; sys.exit(main())
