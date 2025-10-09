#!/usr/bin/env python3
import os, pandas as pd, numpy as np

# --- 공통 ---
alias = {"WSH":"WSN","WAS":"WSN","TB":"TBR","KC":"KCR","SD":"SDP","SF":"SFG"}
def norm_team(x):
    s=str(x).strip().upper().replace(" ","")
    return alias.get(s, s)
def pivot_date(season:int):  # 6/30 기준
    return pd.Timestamp(int(season),6,30)

# --- KBO 처리 ---
def kbo_team_age():
    # 1) 팀 집계 파일 우선
    bat_p = None
    pit_p = None
    for base in ["data","."]:
        if os.path.exists(os.path.join(base,"kbobattingdata.csv")): bat_p=os.path.join(base,"kbobattingdata.csv")
        if os.path.exists(os.path.join(base,"kbopitchingdata.csv")): pit_p=os.path.join(base,"kbopitchingdata.csv")
    frames=[]
    if bat_p:
        df=pd.read_csv(bat_p, low_memory=False)
        # 팀/시즌 추정 컬럼명 후보
        tc=[c for c in df.columns if c.lower() in {"team","tm","teamid","org"}]
        sc=[c for c in df.columns if c.lower() in {"season","year","season_std","yearid"}]
        ac=[c for c in df.columns if c.lower() in {"average_batter_age","avg_batter_age","avg_age_bat"}]
        if tc and sc and ac:
            sub=df[[tc[0],sc[0],ac[0]]].copy()
            sub.columns=["Team","season","avg_bat_age"]
            sub["Team"]=sub["Team"].map(norm_team)
            sub["season"]=pd.to_numeric(sub["season"], errors="coerce").astype("Int64")
            frames.append(sub)
    if pit_p:
        df=pd.read_csv(pit_p, low_memory=False)
        tc=[c for c in df.columns if c.lower() in {"team","tm","teamid","org"}]
        sc=[c for c in df.columns if c.lower() in {"season","year","season_std","yearid"}]
        ac=[c for c in df.columns if c.lower() in {"average_age","avg_pitcher_age","avg_age_pit"}]
        if tc and sc and ac:
            sub=df[[tc[0],sc[0],ac[0]]].copy()
            sub.columns=["Team","season","avg_pit_age"]
            sub["Team"]=sub["Team"].map(norm_team)
            sub["season"]=pd.to_numeric(sub["season"], errors="coerce").astype("Int64")
            frames.append(sub)
    if frames:
        merged=frames[0]
        for f in frames[1:]:
            merged=merged.merge(f, on=["Team","season"], how="outer")
    else:
        merged=None

    # 2) 선수 원천(있을 때)으로 가중 평균 보강
    kbo_detail=None
    for base in ["data","."]:
        p=os.path.join(base,"kbo_dataset_2018_2024.csv")
        if os.path.exists(p):
            df=pd.read_csv(p, low_memory=False)
            # 후보
            tc=[c for c in df.columns if c.lower() in {"team","tm","teamid","org"}]
            sc=[c for c in df.columns if c.lower() in {"season","year","season_std","yearid"}]
            ac=[c for c in df.columns if c.lower()=="age"]
            # 가중치 후보(타자 PA/ 투수 IP)
            pac=[c for c in df.columns if c.lower() in {"pa","plate_app","plate_appearances"}]
            ipc=[c for c in df.columns if c.lower() in {"ip","innings","ip_w","innings_pitched"}]
            if tc and sc and ac:
                sub=df[[tc[0],sc[0],ac[0]] + (pac[:1] if pac else []) + (ipc[:1] if ipc else [])].copy()
                sub.columns=["Team","season","age"] + (["PA"] if pac else []) + (["IP"] if ipc else [])
                sub["Team"]=sub["Team"].map(norm_team)
                sub["season"]=pd.to_numeric(sub["season"], errors="coerce").astype("Int64")
                # 통합 가중치(가능한 컬럼만)
                w = 0
                if "PA" in sub.columns:
                    w = pd.to_numeric(sub["PA"], errors="coerce").fillna(0)
                if "IP" in sub.columns:
                    w = w + pd.to_numeric(sub["IP"], errors="coerce").fillna(0)*4.3  # 타자 상대 추정
                if isinstance(w, int) or isinstance(w, float):
                    w = pd.Series([0]*len(sub))
                sub["w"]=w
                sub["age"]=pd.to_numeric(sub["age"], errors="coerce")
                sub=sub.dropna(subset=["age","season"])
                g=sub.groupby(["Team","season"]).apply(
                    lambda x: np.average(x["age"], weights=(x["w"].replace(0,np.nan) if x["w"].sum()>0 else None)) \
                              if x["w"].sum()>0 else x["age"].mean()
                ).reset_index(name="avg_age_detail")
                kbo_detail=g
            break

    # 3) 최종 결정
    if merged is None and kbo_detail is None:
        return None
    out=None
    if merged is not None:
        # bat/pit 있으면 평균(둘 다 있으면 단순 평균; 세부 가중치 있으면 섞음)
        merged["avg_age"]=merged[["avg_bat_age","avg_pit_age"]].mean(axis=1, skipna=True)
        out=merged[["Team","season","avg_age"]].copy()
    if kbo_detail is not None:
        out = kbo_detail if out is None else out.merge(kbo_detail, on=["Team","season"], how="outer")
        if "avg_age_detail" in out.columns:
            out["avg_age"]=out["avg_age"].combine_first(out["avg_age_detail"])
    out["league"]="KBO"
    out=out.dropna(subset=["season"])
    out["season"]=out["season"].astype(int)
    return out[["league","season","Team","avg_age"]]

# --- MLB (Lahman) 처리 ---
def mlb_team_age():
    root="lahman"
    need = [("People.csv",), ("Batting.csv","Pitching.csv")]
    if not (os.path.exists(os.path.join(root,"People.csv")) and
            (os.path.exists(os.path.join(root,"Batting.csv")) or os.path.exists(os.path.join(root,"Pitching.csv")))):
        return None

    ppl=pd.read_csv(os.path.join(root,"People.csv"), low_memory=False)
    ppl=ppl[["playerID","birthYear","birthMonth","birthDay"]].copy()

    def safe_dob(r):
        y=int(r["birthYear"]) if pd.notna(r["birthYear"]) else np.nan
        if pd.isna(y): return pd.NaT
        m=int(r["birthMonth"]) if pd.notna(r["birthMonth"]) else 7
        d=int(r["birthDay"]) if pd.notna(r["birthDay"]) else 1
        m = min(max(m,1),12); d = min(max(d,1),28)
        try:
            return pd.Timestamp(y,m,d)
        except: # 드물게 이상치
            return pd.Timestamp(y,7,1)
    ppl["dob"]=ppl.apply(safe_dob, axis=1)
    ppl=ppl[["playerID","dob"]]

    rows=[]

    # 타자
    if os.path.exists(os.path.join(root,"Batting.csv")):
        bat=pd.read_csv(os.path.join(root,"Batting.csv"), low_memory=False)
        bat=bat[["playerID","yearID","teamID","AB","BB"] + ([c for c in ["HBP","SF","SH"] if c in bat.columns])].copy()
        for c in ["AB","BB","HBP","SF","SH"]:
            if c in bat.columns: bat[c]=pd.to_numeric(bat[c], errors="coerce").fillna(0)
        bat["PA"]=bat.get("AB",0)+bat.get("BB",0)+bat.get("HBP",0)+bat.get("SF",0)+bat.get("SH",0)
        bat=bat.merge(ppl, on="playerID", how="left")
        bat=bat.dropna(subset=["dob"])
        bat["base"]=bat["yearID"].map(lambda y: pd.Timestamp(int(y),6,30))
        bat["age"]=(bat["base"] - bat["dob"]).dt.days/365.2425
        g=bat.groupby(["yearID","teamID"], as_index=False).apply(
            lambda x: pd.Series({"avg_age": np.average(x["age"], weights=(x["PA"].replace(0,np.nan) if x["PA"].sum()>0 else None)) \
                                           if x["PA"].sum()>0 else x["age"].mean()})
        )
        g.rename(columns={"yearID":"season","teamID":"Team"}, inplace=True)
        g["Team"]=g["Team"].map(norm_team)
        g["league"]="MLB"
        rows.append(g[["league","season","Team","avg_age"]])

    # 투수
    if os.path.exists(os.path.join(root,"Pitching.csv")):
        pit=pd.read_csv(os.path.join(root,"Pitching.csv"), low_memory=False)
        keep=["playerID","yearID","teamID","BFP","IPouts"]
        keep=[c for c in keep if c in pit.columns]
        pit=pit[keep].copy()
        for c in ["BFP","IPouts"]:
            if c in pit.columns: pit[c]=pd.to_numeric(pit[c], errors="coerce").fillna(0)
        if "BFP" in pit.columns:
            pit["W"]=pit["BFP"]
        elif "IPouts" in pit.columns:
            pit["W"]=pit["IPouts"]/3*4.3
        else:
            pit["W"]=0
        pit=pit.merge(ppl, on="playerID", how="left")
        pit=pit.dropna(subset=["dob"])
        pit["base"]=pit["yearID"].map(lambda y: pd.Timestamp(int(y),6,30))
        pit["age"]=(pit["base"] - pit["dob"]).dt.days/365.2425
        g=pit.groupby(["yearID","teamID"], as_index=False).apply(
            lambda x: pd.Series({"avg_age": np.average(x["age"], weights=(x["W"].replace(0,np.nan) if x["W"].sum()>0 else None)) \
                                           if x["W"].sum()>0 else x["age"].mean()})
        )
        g.rename(columns={"yearID":"season","teamID":"Team"}, inplace=True)
        g["Team"]=g["Team"].map(norm_team)
        g["league"]="MLB"
        rows.append(g[["league","season","Team","avg_age"]])

    if not rows:
        return None

    # 타자/투수 결합: 같은 시즌/팀이면 평균(가중결합 여지 있지만 단순평균으로)
    allm=pd.concat(rows, ignore_index=True)
    allm=allm.groupby(["league","season","Team"], as_index=False)["avg_age"].mean()
    return allm

# --- team_agg 병합 ---
def main():
    if not os.path.exists("output/team_agg.csv"):
        print("[-] output/team_agg.csv 없음"); return 2
    ta=pd.read_csv("output/team_agg.csv")
    ta["group_id"]=ta["group_id"].map(norm_team)

    kbo=kbo_team_age()
    mlb=mlb_team_age()

    if kbo is None and mlb is None:
        print("[-] KBO/MLB 나이 데이터 생성 실패"); return 2

    frames=[]
    if mlb is not None: frames.append(mlb)
    if kbo is not None: frames.append(kbo)
    ages=pd.concat(frames, ignore_index=True)

    # 병합
    out=ta.merge(ages.rename(columns={"Team":"group_id"}), on=["league","season","group_id"], how="left")
    # 리그 평균/차이
    league_age = out.groupby(["league","season"], as_index=False)["avg_age"].mean().rename(columns={"avg_age":"league_avg_age"})
    out=out.drop(columns=["league_avg_age"], errors="ignore").merge(league_age, on=["league","season"], how="left")
    out["age_diff"]=out["avg_age"] - out["league_avg_age"]

    out.to_csv("output/team_agg.csv", index=False)
    print("[OK] Day58: avg_age/league_avg_age/age_diff 갱신 완료 -> output/team_agg.csv")
    return 0

if __name__=="__main__":
    import sys; sys.exit(main())
