import sys, time, requests, pandas as pd

FEED = "https://statsapi.mlb.com/api/v1.1/game/{pk}/feed/live"

def G(d,*ks,default=""):
    for k in ks:
        if not isinstance(d, dict): return default
        d = d.get(k, None)
    return d if d not in (None,"") else default

def norm_date(js):
    gd = js.get("gameData",{}) or {}
    # 우선순위: originalDate > officialDate > dateTime(YYYY-MM-DD)
    for ks in [("datetime","originalDate"), ("datetime","officialDate")]:
        v = G(gd,*ks)
        if v: return v[:10]
    dt = G(gd,"datetime","dateTime")
    return (dt[:10] if dt else "")

def norm_venue(js):
    gd = js.get("gameData",{}) or {}
    v  = G(gd,"venue","name")
    if v: return v
    # fallback: liveData linescore venue (드물게 존재)
    v2 = G(js,"liveData","linescore","venue","name")
    return v2 or ""

def norm_team(js, side):
    gd = js.get("gameData",{}) or {}
    # abbreviation > teamCode > nameCode
    for key in ["abbreviation","teamCode","nameCode"]:
        v = G(gd,"teams",side,key)
        if v: return v.upper()
    return ""

def main(inp="output/games.csv", outp=None):
    outp = outp or inp
    df = pd.read_csv(inp)
    changed = 0
    for i,row in df.iterrows():
        need = any(not str(row.get(k,"")).strip() for k in ["date","venue"])
        if not need: continue
        pk = int(row["game_pk"])
        for _ in range(3):
            try:
                js = requests.get(FEED.format(pk=pk), timeout=20).json()
                # 채움
                if not str(row.get("date","")).strip():
                    df.at[i,"date"] = norm_date(js)
                if not str(row.get("venue","")).strip():
                    df.at[i,"venue"] = norm_venue(js)
                # 홈/원정도 비어있으면 보강
                if not str(row.get("home","")).strip():
                    df.at[i,"home"] = norm_team(js,"home")
                if not str(row.get("away","")).strip():
                    df.at[i,"away"] = norm_team(js,"away")
                changed += 1
                break
            except Exception:
                time.sleep(0.5)
                continue
    df = df[["game_pk","date","venue","home","away","home_runs","away_runs"]]
    df.to_csv(outp, index=False)
    print(f"[fill_date_venue] wrote {outp} (changed {changed} rows of {len(df)})")

if __name__=="__main__":
    args = sys.argv[1:]
    if len(args)==0: main()
    elif len(args)==1: main(args[0], args[0])
    else: main(args[0], args[1])
