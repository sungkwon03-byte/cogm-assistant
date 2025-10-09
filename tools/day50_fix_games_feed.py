import pandas as pd, requests, sys, time

FEED = "https://statsapi.mlb.com/api/v1.1/game/{pk}/feed/live"

def fix(path_in, path_out):
    df = pd.read_csv(path_in)
    for i,row in df.iterrows():
        need = any(not str(row.get(k,"")).strip() for k in ["date","venue","home","away"])
        if not need: continue
        pk = int(row["game_pk"])
        for _ in range(3):
            try:
                js = requests.get(FEED.format(pk=pk), timeout=20).json()
                gd = js.get("gameData",{}) or {}
                teams = (gd.get("teams") or {})
                home = (teams.get("home") or {}).get("abbreviation","")
                away = (teams.get("away") or {}).get("abbreviation","")
                venue = (gd.get("venue") or {}).get("name","")
                date  = (gd.get("datetime") or {}).get("originalDate","")
                if not str(row.get("home","")).strip():  df.at[i,"home"]=home
                if not str(row.get("away","")).strip():  df.at[i,"away"]=away
                if not str(row.get("venue","")).strip(): df.at[i,"venue"]=venue
                if not str(row.get("date","")).strip():  df.at[i,"date"]=date
                break
            except Exception:
                time.sleep(0.6)
                continue
    df = df[["game_pk","date","venue","home","away","home_runs","away_runs"]]
    df.to_csv(path_out, index=False)
    print(f"[fix_feed] wrote {path_out} ({len(df)} rows)")
if __name__=="__main__":
    inp = sys.argv[1] if len(sys.argv)>1 else "output/games.csv"
    out = sys.argv[2] if len(sys.argv)>2 else inp
    fix(inp,out)
