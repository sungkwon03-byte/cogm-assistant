#!/usr/bin/env python3
import os, sys, pandas as pd, numpy as np

ALIASES={"WSH":"WSN","WAS":"WSN","TB":"TBR","KC":"KCR","SD":"SDP","SF":"SFG"}
def norm_team(x): s=str(x).strip().upper().replace(" ",""); return ALIASES.get(s,s)
def pick(cols, cands):
    lo=[c.lower() for c in cols]
    for k in cands:
        if k in lo: return cols[lo.index(k)]
    return None

def find_files():
    roots=[".","data","mart","external","inputs","downloads","lahman"]
    want={"kbo_bat":["kbobattingdata.csv"],"kbo_pit":["kbopitchingdata.csv"],"kbo_det":["kbo_dataset_2018_2024.csv"],
          "mlb_people":["people.csv"],"mlb_bat":["batting.csv"],"mlb_pit":["pitching.csv"]}
    hits={k:[] for k in want}
    for root in roots:
        for r,_,files in os.walk(root):
            rlow=r.lower()
            if any(s in rlow for s in ("/venv","\\venv","/env","\\env","/logs","\\logs","/output","\\output","/minor","\\minor")): continue
            for f in files:
                fl=f.lower()
                for key,names in want.items():
                    if fl in names: hits[key].append(os.path.join(r,f))
    return hits

def load_kbo_team_age(paths):
    bat=(paths.get("kbo_bat") or [None])[0]; pit=(paths.get("kbo_pit") or [None])[0]; det=(paths.get("kbo_det") or [None])[0]
    merged=None
    if bat and os.path.exists(bat):
        df=pd.read_csv(bat, low_memory=False)
        t=pick(df.columns,["team","tm","teamid","org"]); s=pick(df.columns,["season","year","season_std","yearid"]); a=pick(df.columns,["average_batter_age","avg_batter_age","avg_age_bat"])
        if t and s and a:
            sub=df[[t,s,a]].copy(); sub.columns=["Team","season","avg_bat_age"]
            sub["Team"]=sub["Team"].map(norm_team); sub["season"]=pd.to_numeric(sub["season"],errors="coerce").astype("Int64"); merged=sub
    if pit and os.path.exists(pit):
        df=pd.read_csv(pit, low_memory=False)
        t=pick(df.columns,["team","tm","teamid","org"]); s=pick(df.columns,["season","year","season_std","yearid"]); a=pick(df.columns,["average_age","avg_pitcher_age","avg_age_pit"])
        if t and s and a:
            sub=df[[t,s,a]].copy(); sub.columns=["Team","season","avg_pit_age"]
            sub["Team"]=sub["Team"].map(norm_team); sub["season"]=pd.to_numeric(sub["season"],errors="coerce").astype("Int64")
            merged=sub if merged is None else merged.merge(sub,on=["Team","season"],how="outer")
    if det and os.path.exists(det):
        df=pd.read_csv(det, low_memory=False)
        t=pick(df.columns,["team","tm","teamid","org"]); s=pick(df.columns,["season","year","season_std","yearid"]); a=pick(df.columns,["age"])
        pa=pick(df.columns,["pa","plate_app","plate_appearances"]); ip=pick(df.columns,["ip","innings","ip_w","innings_pitched"])
        if t and s and a:
            cols=[t,s,a]+([pa] if pa else [])+([ip] if ip else [])
            sub=df[cols].copy(); sub.columns=["Team","season","age"]+(["PA"] if pa else [])+(["IP"] if ip else [])
            sub["Team"]=sub["Team"].map(norm_team); sub["season"]=pd.to_numeric(sub["season"],errors="coerce").astype("Int64")
            sub["age"]=pd.to_numeric(sub["age"],errors="coerce"); 
            if "PA" in sub.columns: sub["PA"]=pd.to_numeric(sub["PA"],errors="coerce").fillna(0)
            if "IP" in sub.columns: sub["IP"]=pd.to_numeric(sub["IP"],errors="coerce").fillna(0)
            sub=sub.dropna(subset=["age","season"])
            def agg(g):
                w=(g.get("PA",pd.Series(0,index=g.index))+g.get("IP",pd.Series(0,index=g.index))*4.3).fillna(0)
                return np.average(g["age"],weights=w) if w.sum()>0 else g["age"].mean()
            g=sub.groupby(["Team","season"]).apply(agg).reset_index(name="avg_age_detail")
            merged=g if merged is None else merged.merge(g,on=["Team","season"],how="outer")
    if merged is None: return None
    if "avg_bat_age" in merged.columns or "avg_pit_age" in merged.columns:
        merged["avg_age"]=merged[["avg_bat_age","avg_pit_age"]].mean(axis=1,skipna=True)
    if "avg_age_detail" in merged.columns:
        merged["avg_age"]=merged.get("avg_age").combine_first(merged["avg_age_detail"]) if "avg_age" in merged.columns else merged["avg_age_detail"]
    out=merged.dropna(subset=["season"]).copy(); out["season"]=out["season"].astype(int); out["league"]="KBO"
    return out[["league","season","Team","avg_age"]]

def load_mlb_team_age(paths):
    people=(paths.get("mlb_people") or [None])[0]
    if not people or not os.path.exists(people): return None
    P=pd.read_csv(people, low_memory=False)[["playerID","birthYear","birthMonth","birthDay"]]
    def dob(r):
        y=r["birthYear"]; m=r["birthMonth"]; d=r["birthDay"]
        if pd.isna(y): return pd.NaT
        m=7 if pd.isna(m) else int(m); d=1 if pd.isna(d) else int(d)
        m=min(max(m,1),12); d=min(max(d,1),28)
        try: return pd.Timestamp(int(y),m,d)
        except: return pd.Timestamp(int(y),7,1)
    P["dob"]=P.apply(dob,axis=1); P=P[["playerID","dob"]]
    frames=[]
    bat=(paths.get("mlb_bat") or [None])[0]
    if bat and os.path.exists(bat):
        B=pd.read_csv(bat, low_memory=False)
        keep=["playerID","yearID","teamID","AB","BB"]+[c for c in ["HBP","SF","SH"] if c in B.columns]
        B=B[keep].copy()
        for c in keep[3:]: B[c]=pd.to_numeric(B[c],errors="coerce").fillna(0)
        B["PA"]=B.get("AB",0)+B.get("BB",0)+B.get("HBP",0)+B.get("SF",0)+B.get("SH",0)
        B=B.merge(P,on="playerID",how="left").dropna(subset=["dob"])
        B["base"]=B["yearID"].map(lambda y: pd.Timestamp(int(y),6,30))
        B["age"]=(B["base"]-B["dob"]).dt.days/365.2425
        g=B.groupby(["yearID","teamID"]).apply(lambda x: np.average(x["age"],weights=x["PA"]) if x["PA"].sum()>0 else x["age"].mean()).reset_index(name="avg_age_bat")
        g.rename(columns={"yearID":"season","teamID":"Team"},inplace=True); g["Team"]=g["Team"].map(norm_team); g["league"]="MLB"
        g["w_bat"]=B.groupby(["yearID","teamID"])["PA"].sum().reset_index(drop=True); frames.append(g)
    pit=(paths.get("mlb_pit") or [None])[0]
    if pit and os.path.exists(pit):
        PITCH=pd.read_csv(pit, low_memory=False)
        keep=["playerID","yearID","teamID"]+[c for c in ["BFP","IPouts"] if c in PITCH.columns]
        PITCH=PITCH[keep].copy()
        if "BFP" in PITCH.columns: PITCH["BFP"]=pd.to_numeric(PITCH["BFP"],errors="coerce").fillna(0)
        if "IPouts" in PITCH.columns: PITCH["IPouts"]=pd.to_numeric(PITCH["IPouts"],errors="coerce").fillna(0)
        PITCH["W"]=PITCH["BFP"] if "BFP" in PITCH.columns else (PITCH["IPouts"]/3*4.3 if "IPouts" in PITCH.columns else 0)
        PITCH=PITCH.merge(P,on="playerID",how="left").dropna(subset=["dob"])
        PITCH["base"]=PITCH["yearID"].map(lambda y: pd.Timestamp(int(y),6,30))
        PITCH["age"]=(PITCH["base"]-PITCH["dob"]).dt.days/365.2425
        g=PITCH.groupby(["yearID","teamID"]).apply(lambda x: np.average(x["age"],weights=x["W"]) if x["W"].sum()>0 else x["age"].mean()).reset_index(name="avg_age_pit")
        g.rename(columns={"yearID":"season","teamID":"Team"},inplace=True); g["Team"]=g["Team"].map(norm_team); g["league"]="MLB"
        g["w_pit"]=PITCH.groupby(["yearID","teamID"])["W"].sum().reset_index(drop=True); frames.append(g)
    if not frames: return None
    M=frames[0]
    for f in frames[1:]: M=M.merge(f,on=["league","season","Team"],how="outer")
    def combine(r):
        a=r.get("avg_age_bat"); b=r.get("avg_age_pit"); wb=r.get("w_bat",0); wp=r.get("w_pit",0)
        if pd.notna(a) and pd.notna(b) and (wb+wp)>0: return (a*wb+b*wp)/(wb+wp)
        if pd.notna(a): return a
        if pd.notna(b): return b
        return np.nan
    M["avg_age"]=M.apply(combine,axis=1)
    return M[["league","season","Team","avg_age"]]

def main():
    if not os.path.exists("output/team_agg.csv"): print("[-] output/team_agg.csv not found"); return 2
    ta=pd.read_csv("output/team_agg.csv")
    if "group_id" not in ta.columns: print("[-] team_agg missing group_id"); return 2
    ta["group_id"]=ta["group_id"].map(norm_team)
    paths=find_files()
    mlb=load_mlb_team_age(paths); kbo=load_kbo_team_age(paths)
    if mlb is None and kbo is None: print("[-] MLB/KBO sources not found in workspace"); return 2
    frames=[]; 
    if mlb is not None: frames.append(mlb.rename(columns={"Team":"group_id"}))
    if kbo is not None: frames.append(kbo.rename(columns={"Team":"group_id"}))
    ages=pd.concat(frames, ignore_index=True)
    out=ta.merge(ages,on=["league","season","group_id"],how="left")
    league_age=out.groupby(["league","season"],as_index=False)["avg_age"].mean().rename(columns={"avg_age":"league_avg_age"})
    out=out.drop(columns=["league_avg_age"],errors="ignore").merge(league_age,on=["league","season"],how="left")
    out["age_diff"]=out["avg_age"]-out["league_avg_age"]
    out.to_csv("output/team_agg.csv",index=False)
    print("[OK] Day58 updated -> output/team_agg.csv"); return 0

if __name__=="__main__":
    sys.exit(main())
