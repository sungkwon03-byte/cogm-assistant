#!/usr/bin/env python3
import os, json, pandas as pd, numpy as np

ALIASES={"WSH":"WSN","WAS":"WSN","TB":"TBR","KC":"KCR","SD":"SDP","SF":"SFG"}
def norm_team(x): s=str(x).strip().upper().replace(" ",""); return ALIASES.get(s,s)
def pick(cols, cands):
    lo=[c.lower() for c in cols]
    for k in cands:
        if k in lo: return cols[lo.index(k)]
    return None

assert os.path.exists("output/team_agg.csv"), "output/team_agg.csv not found"
TA=pd.read_csv("output/team_agg.csv"); TA["group_id"]=TA["group_id"].map(norm_team)

# --- KBO from probe ---
paths=json.load(open("logs/day58_kbo_paths.json",encoding="utf-8"))
kbo_bat=(paths.get("KBO_BAT") or [None])[0]
kbo_pit=(paths.get("KBO_PIT") or [None])[0]
kbo_det=(paths.get("KBO_DETAIL") or [None])[0]

def kbo_frame():
    frames=[]
    if kbo_bat and os.path.exists(kbo_bat):
        df=pd.read_csv(kbo_bat, low_memory=False)
        t=pick(df.columns,["team","tm","teamid","org"]); s=pick(df.columns,["season","year","season_std","yearid"])
        a=pick(df.columns,["average_batter_age","avg_batter_age","avg_age_bat"])
        if t and s and a:
            sub=df[[t,s,a]].copy(); sub.columns=["Team","season","avg_bat_age"]
            sub["Team"]=sub["Team"].map(norm_team); sub["season"]=pd.to_numeric(sub["season"],errors="coerce").astype("Int64")
            frames.append(sub)
    if kbo_pit and os.path.exists(kbo_pit):
        df=pd.read_csv(kbo_pit, low_memory=False)
        t=pick(df.columns,["team","tm","teamid","org"]); s=pick(df.columns,["season","year","season_std","yearid"])
        a=pick(df.columns,["average_age","avg_pitcher_age","avg_age_pit"])
        if t and s and a:
            sub=df[[t,s,a]].copy(); sub.columns=["Team","season","avg_pit_age"]
            sub["Team"]=sub["Team"].map(norm_team); sub["season"]=pd.to_numeric(sub["season"],errors="coerce").astype("Int64")
            frames.append(sub)
    out=None
    if frames:
        out=frames[0]
        for f in frames[1:]: out=out.merge(f,on=["Team","season"],how="outer")
        out["avg_age"]=out[["avg_bat_age","avg_pit_age"]].mean(axis=1,skipna=True)
        out["league"]="KBO"; out=out.dropna(subset=["season"]); out["season"]=out["season"].astype(int)
        out=out[["league","season","Team","avg_age"]]
    if out is None and kbo_det and os.path.exists(kbo_det):
        df=pd.read_csv(kbo_det, low_memory=False)
        t=pick(df.columns,["team","tm","teamid","org"]); s=pick(df.columns,["season","year","season_std","yearid"]); a=pick(df.columns,["age"])
        if t and s and a:
            sub=df[[t,s,a]].copy(); sub.columns=["Team","season","age"]
            sub["Team"]=sub["Team"].map(norm_team); sub["season"]=pd.to_numeric(sub["season"],errors="coerce").astype("Int64")
            sub["age"]=pd.to_numeric(sub["age"],errors="coerce"); sub=sub.dropna(subset=["age","season"])
            g=sub.groupby(["Team","season"])["age"].mean().reset_index().rename(columns={"age":"avg_age"})
            g["league"]="KBO"; g["season"]=g["season"].astype(int)
            out=g[["league","season","Team","avg_age"]]
    return out

# --- MLB (Lahman) ---
def find_one(filename):
    hits=[]
    for root,_,files in os.walk("data"):
        rl=root.lower()
        if any(x in rl for x in ("/venv","\\venv","/env","\\env","/logs","\\logs","/output","\\output")): continue
        for f in files:
            if f.lower()==filename.lower():
                p=os.path.join(root,f)
                try:
                    if os.path.getsize(p)>0: hits.append(p)
                except: pass
    if not hits: return None
    hits.sort(key=lambda p: (0 if "lahman_1871-2024_csv" in p.lower() else 1, len(p)))
    return hits[0]

def load_people(path):
    df=pd.read_csv(path, low_memory=False)
    if "playerID" not in df.columns: raise RuntimeError("People.csv malformed")
    keep=[c for c in ["playerID","birthYear","birthMonth","birthDay"] if c in df.columns]
    df=df[keep].copy()
    def dob(r):
        y=r.get("birthYear"); 
        if pd.isna(y): return pd.NaT
        m=r.get("birthMonth",np.nan); d=r.get("birthDay",np.nan)
        m=7 if pd.isna(m) else int(m); d=1 if pd.isna(d) else int(d)
        m=min(max(m,1),12); d=min(max(d,1),28)
        try: return pd.Timestamp(int(y),m,d)
        except: return pd.Timestamp(int(y),7,1)
    df["dob"]=df.apply(dob,axis=1)
    return df[["playerID","dob"]]

def mlb_frame():
    ppl=find_one("People.csv"); bat=find_one("Batting.csv"); pit=find_one("Pitching.csv")
    if not ppl: return None
    P=load_people(ppl)
    frames=[]
    if bat:
        B=pd.read_csv(bat, low_memory=False)
        need=["playerID","yearID","teamID","AB","BB"]
        for c in need:
            if c not in B.columns: break
        keep=["playerID","yearID","teamID","AB","BB"]+[c for c in ["HBP","SF","SH"] if c in B.columns]
        B=B[keep].copy()
        for c in keep[3:]: B[c]=pd.to_numeric(B[c],errors="coerce").fillna(0)
        B["PA"]=B.get("AB",0)+B.get("BB",0)+B.get("HBP",0)+B.get("SF",0)+B.get("SH",0)
        B=B.merge(P,on="playerID",how="left").dropna(subset=["dob"])
        B["base"]=B["yearID"].map(lambda y: pd.Timestamp(int(y),6,30))
        B["age"]=(B["base"]-B["dob"]).dt.days/365.2425
        gb=B.groupby(["yearID","teamID"])
        g=gb.apply(lambda x: np.average(x["age"],weights=x["PA"]) if x["PA"].sum()>0 else x["age"].mean()).reset_index(name="avg_age_bat")
        w=gb["PA"].sum().reset_index().rename(columns={"yearID":"season","teamID":"Team","PA":"w_bat"})
        g.rename(columns={"yearID":"season","teamID":"Team"}, inplace=True); g["Team"]=g["Team"].map(norm_team); g["league"]="MLB"
        g=g.merge(w, on=["season","Team"], how="left")
        frames.append(g)
    if pit:
        PITCH=pd.read_csv(pit, low_memory=False)
        keep=["playerID","yearID","teamID"]+[c for c in ["BFP","IPouts"] if c in PITCH.columns]
        PITCH=PITCH[keep].copy()
        if "BFP" in PITCH.columns: PITCH["BFP"]=pd.to_numeric(PITCH["BFP"],errors="coerce").fillna(0)
        if "IPouts" in PITCH.columns: PITCH["IPouts"]=pd.to_numeric(PITCH["IPouts"],errors="coerce").fillna(0)
        PITCH["W"]=PITCH["BFP"] if "BFP" in PITCH.columns else (PITCH["IPouts"]/3*4.3 if "IPouts" in PITCH.columns else 0)
        PITCH=PITCH.merge(P,on="playerID",how="left").dropna(subset=["dob"])
        PITCH["base"]=PITCH["yearID"].map(lambda y: pd.Timestamp(int(y),6,30))
        PITCH["age"]=(PITCH["base"]-PITCH["dob"]).dt.days/365.2425
        gp=PITCH.groupby(["yearID","teamID"])
        g=gp.apply(lambda x: np.average(x["age"],weights=x["W"]) if x["W"].sum()>0 else x["age"].mean()).reset_index(name="avg_age_pit")
        w=gp["W"].sum().reset_index().rename(columns={"yearID":"season","teamID":"Team","W":"w_pit"})
        g.rename(columns={"yearID":"season","teamID":"Team"}, inplace=True); g["Team"]=g["Team"].map(norm_team); g["league"]="MLB"
        g=g.merge(w, on=["season","Team"], how="left")
        frames.append(g)
    if not frames: return None
    M=frames[0]
    for f in frames[1:]: M=M.merge(f, on=["league","season","Team"], how="outer")
    def combine(r):
        a=r.get("avg_age_bat"); b=r.get("avg_age_pit"); wb=r.get("w_bat",0); wp=r.get("w_pit",0)
        if pd.notna(a) and pd.notna(b) and (wb+wp)>0: return (a*wb+b*wp)/(wb+wp)
        if pd.notna(a): return a
        if pd.notna(b): return b
        return np.nan
    M["avg_age"]=M.apply(combine,axis=1)
    return M[["league","season","Team","avg_age"]]

KBO=kbo_frame()
MLB=mlb_frame()
frames=[]
if MLB is not None: frames.append(MLB.rename(columns={"Team":"group_id"}))
if KBO is not None: frames.append(KBO.rename(columns={"Team":"group_id"}))
if not frames: raise SystemExit("no MLB/KBO sources")

AGES=pd.concat(frames, ignore_index=True)
OUT=TA.merge(AGES, on=["league","season","group_id"], how="left")
league_age=OUT.groupby(["league","season"], as_index=False)["avg_age"].mean().rename(columns={"avg_age":"league_avg_age"})
OUT=OUT.drop(columns=["league_avg_age"], errors="ignore").merge(league_age, on=["league","season"], how="left")
OUT["age_diff"]=OUT["avg_age"]-OUT["league_avg_age"]
OUT.to_csv("output/team_agg.csv", index=False)
print("[OK] Day58 updated -> output/team_agg.csv")
