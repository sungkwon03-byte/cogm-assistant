import json, glob, re, pandas as pd, sys, os
from pathlib import Path

def pk_from_name(p): 
    m=re.search(r'(\d{6,})', os.path.basename(p)); return int(m.group(1)) if m else None

def safe(d,*ks,default=None):
    cur=d
    for k in ks:
        if not isinstance(cur, dict): return default
        cur = cur.get(k)
    return cur if cur not in (None,"") else default

def team_abbr(box, side): 
    return safe(box, "teams", side, "team", "abbreviation", default="")

def team_bat(box, side, key): 
    return safe(box, "teams", side, "teamStats", "batting", key)

def team_pit(box, side, key): 
    return safe(box, "teams", side, "teamStats", "pitching", key)

def extract_players(box, pk, side):
    team = team_abbr(box, side)
    players = safe(box, "teams", side, "players", default={}) or {}
    rows=[]
    for _, pdata in players.items():
        info = pdata.get("person",{}) or {}
        stats = pdata.get("stats",{}) or {}
        bat = stats.get("batting",{}) or {}
        pit = stats.get("pitching",{}) or {}
        pa = sum(bat.get(k,0) or 0 for k in ["atBats","baseOnBalls","hitByPitch","sacBunts","sacFlies"])
        rows.append({
            "game_pk": pk, "team": team,
            "mlb_id": info.get("id"), "name": info.get("fullName"),
            "PA": pa, "H": bat.get("hits",0), "HR": bat.get("homeRuns",0),
            "BB": bat.get("baseOnBalls",0), "SO": bat.get("strikeOuts",0),
            "IP_outs": pit.get("outs",0), "K_p": pit.get("strikeOuts",0),
            "BB_p": pit.get("baseOnBalls",0), "ER": pit.get("earnedRuns",0),
        })
    return rows

def main():
    Path("output").mkdir(parents=True, exist_ok=True)
    games_rows=[]; team_rows=[]; player_rows=[]
    line_files = sorted(glob.glob("data/line_*.json"))
    for lf in line_files:
        pk = pk_from_name(lf)
        with open(lf,"r") as f: line=json.load(f)
        bf = f"data/box_{pk}.json"
        if not os.path.exists(bf): 
            print(f"[warn] missing box for {pk}, skip", file=sys.stderr); 
            continue
        with open(bf,"r") as f: box=json.load(f)

        date  = safe(line,"gameDate", default="")
        venue = safe(line,"venue","name", default="")
        home  = team_abbr(box,"home"); away = team_abbr(box,"away")
        home_r= team_bat(box,"home","runs"); away_r= team_bat(box,"away","runs")

        games_rows.append({"game_pk":pk,"date":date,"venue":venue,"home":home,"away":away,"home_runs":home_r,"away_runs":away_r})
        for side in ["home","away"]:
            team_rows.append({
                "game_pk": pk, "team": team_abbr(box, side),
                "hits": team_bat(box, side, "hits"),
                "runs": team_bat(box, side, "runs"),
                "hr":   team_bat(box, side, "homeRuns"),
                "so_p": team_pit(box, side, "strikeOuts"),
                "bb_p": team_pit(box, side, "baseOnBalls"),
            })
            player_rows += extract_players(box, pk, side)

    games   = pd.DataFrame(games_rows, columns=["game_pk","date","venue","home","away","home_runs","away_runs"])
    teambox = pd.DataFrame(team_rows,  columns=["game_pk","team","hits","runs","hr","so_p","bb_p"])
    player  = pd.DataFrame(player_rows, columns=["game_pk","team","mlb_id","name","PA","H","HR","BB","SO","IP_outs","K_p","BB_p","ER"])

    games.to_csv("output/games.csv", index=False)
    teambox.to_csv("output/team_box.csv", index=False)
    player.to_csv("output/player_box.csv", index=False)
    print(f"[rebuild] games:{len(games)} team_box:{len(teambox)} player_box:{len(player)}")

if __name__=="__main__":
    main()
