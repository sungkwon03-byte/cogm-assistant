import json, pandas as pd, sys
from pathlib import Path

def load_sched(path):
    js=json.load(open(path))
    m={}
    for d in js.get("dates",[]):
        for g in d.get("games",[]):
            pk=g.get("gamePk")
            date=g.get("gameDate","") or g.get("officialDate","")
            venue=(g.get("venue") or {}).get("name","")
            home=(g.get("teams",{}).get("home",{}).get("team",{}) or {}).get("abbreviation","")
            away=(g.get("teams",{}).get("away",{}).get("team",{}) or {}).get("abbreviation","")
            m[pk]={"date":date,"venue":venue,"home":home,"away":away}
    return m

def main(date):
    sched=f"data/schedule_{date}.json"
    games_csv="output/games.csv"
    df=pd.read_csv(games_csv)
    m=load_sched(sched)

    # 백필: 빈 값만 채움
    for i,row in df.iterrows():
        pk=int(row["game_pk"])
        info=m.get(pk,{})
        if not str(row.get("date","")).strip():
            df.at[i,"date"]=info.get("date","")
        if not str(row.get("venue","")).strip():
            df.at[i,"venue"]=info.get("venue","")
        if not str(row.get("home","")).strip():
            df.at[i,"home"]=info.get("home","")
        if not str(row.get("away","")).strip():
            df.at[i,"away"]=info.get("away","")

    df.to_csv(games_csv, index=False)
    print(f"[backfill] updated {games_csv}: {len(df)} rows")

if __name__=="__main__":
    date=sys.argv[1] if len(sys.argv)>1 else "2025-04-01"
    main(date)
