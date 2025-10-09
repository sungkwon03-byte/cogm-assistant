import json, sys, os, requests, pandas as pd
from pathlib import Path

SCHED_URL = "https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date}"
FEED_URL  = "https://statsapi.mlb.com/api/v1.1/game/{pk}/feed/live"

def load_schedule_map(date:str):
    path = f"data/schedule_{date}.json"
    if not os.path.exists(path):
        js = requests.get(SCHED_URL.format(date=date), timeout=20).json()
        Path("data").mkdir(parents=True, exist_ok=True)
        json.dump(js, open(path,"w"))
    else:
        js = json.load(open(path))
    m={}
    for d in js.get("dates",[]):
        for g in d.get("games",[]):
            pk = g.get("gamePk")
            venue = (g.get("venue") or {}).get("name","")
            home = ((g.get("teams",{}).get("home",{}).get("team",{}) or {}).get("abbreviation",""))
            away = ((g.get("teams",{}).get("away",{}).get("team",{}) or {}).get("abbreviation",""))
            date_iso = g.get("officialDate") or g.get("gameDate") or ""
            m[pk] = {"date": date_iso, "venue": venue, "home": home, "away": away}
    return m

def feed_live(pk:int):
    try:
        js = requests.get(FEED_URL.format(pk=pk), timeout=20).json()
        venue = ((js.get("gameData",{}) or {}).get("venue",{}) or {}).get("name","")
        date  = ((js.get("gameData",{}) or {}).get("datetime",{}) or {}).get("originalDate","")
        return {"venue": venue, "date": date}
    except Exception:
        return {"venue":"", "date":""}

def backfill(date:str, games_csv:str, out_csv:str):
    df = pd.read_csv(games_csv)
    sched = load_schedule_map(date)

    # team_box로 runs 보강(혹시 비었으면)
    try:
        tb = pd.read_csv("output/team_box.csv")
    except Exception:
        tb = None

    for i, row in df.iterrows():
        pk = int(row["game_pk"])
        info = sched.get(pk, {})
        # 날짜/구장/홈/원정 백필
        if not str(row.get("date","")).strip():
            df.at[i,"date"] = info.get("date","")
        if not str(row.get("venue","")).strip():
            df.at[i,"venue"] = info.get("venue","")
        if not str(row.get("home","")).strip():
            df.at[i,"home"] = info.get("home","")
        if not str(row.get("away","")).strip():
            df.at[i,"away"] = info.get("away","")

        # 여전히 비면 feed/live 최종 보강
        if not str(df.at[i,"venue"]).strip() or not str(df.at[i,"date"]).strip():
            live = feed_live(pk)
            if not str(df.at[i,"venue"]).strip():
                df.at[i,"venue"] = live.get("venue","")
            if not str(df.at[i,"date"]).strip():
                df.at[i,"date"] = live.get("date","")

        # runs 보강
        if tb is not None:
            try:
                if pd.isna(row.get("home_runs")) or str(row.get("home_runs"))=="":
                    home = df.at[i,"home"]
                    df.at[i,"home_runs"] = int(tb[(tb.game_pk==pk)&(tb.team==home)]["runs"].iloc[0])
                if pd.isna(row.get("away_runs")) or str(row.get("away_runs"))=="":
                    away = df.at[i,"away"]
                    df.at[i,"away_runs"] = int(tb[(tb.game_pk==pk)&(tb.team==away)]["runs"].iloc[0])
            except Exception:
                pass

    # 정렬/형식 간단 정리
    cols = ["game_pk","date","venue","home","away","home_runs","away_runs"]
    df = df[cols]
    df.to_csv(out_csv, index=False)
    print(f"[backfill_v2] wrote {out_csv} ({len(df)} rows)")
