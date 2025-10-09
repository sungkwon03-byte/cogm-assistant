#!/usr/bin/env bash
set -euo pipefail
export PYTHONUNBUFFERED=1

python3 - <<'PY'
import os, csv, time, requests
from pathlib import Path
from datetime import date, timedelta

SPORT_ID   = 1
START_YEAR = 1901                # ← 고정
END_DATE   = date.today()
REQ_TIMEOUT= 30
RETRIES    = 3
SLEEP      = 0.4
YEAR_SLEEP = 1.0

out = Path("output"); cache = Path("output/cache")
out.mkdir(parents=True, exist_ok=True); cache.mkdir(parents=True, exist_ok=True)

games_fp      = out/"games.csv"
team_box_fp   = out/"team_box.csv"
player_box_fp = out/"player_box.csv"
seen_fp       = cache/"seen_games.txt"

def ensure_header(fp, header):
    if not fp.exists():
        with fp.open("w", newline="") as f: csv.writer(f).writerow(header)

ensure_header(games_fp,      ["game_pk","date","venue","home","away","home_runs","away_runs"])
ensure_header(team_box_fp,   ["game_pk","team","hits","runs","hr","so_p","bb_p"])
ensure_header(player_box_fp, ["game_pk","team","mlb_id","name","PA","H","HR","BB","SO","IP_outs","K_p","BB_p","ER"])

seen=set()
if seen_fp.exists():
    seen.update(x.strip() for x in seen_fp.read_text().splitlines() if x.strip())

def get_json(url, retries=RETRIES):
    last=None
    for i in range(retries):
        try:
            r=requests.get(url, timeout=REQ_TIMEOUT)
            if r.ok: return r.json()
            last=f"HTTP {r.status_code}"
        except Exception as e:
            last=repr(e)
        time.sleep(SLEEP*(i+1))
    raise RuntimeError(f"GET fail: {url} :: {last}")

def safe(d,*ks,default=None):
    for k in ks:
        if d is None: return default
        d=d.get(k)
    return default if d is None else d

def days(year):
    d=date(year,1,1); end=date(year,12,31)
    if end>END_DATE: end=END_DATE
    while d<=end:
        yield d
        d+=timedelta(days=1)

with seen_fp.open("a") as seen_out, \
     games_fp.open("a", newline="") as g_f, \
     team_box_fp.open("a", newline="") as tb_f, \
     player_box_fp.open("a", newline="") as pb_f:

    g_w, tb_w, pb_w = csv.writer(g_f), csv.writer(tb_f), csv.writer(pb_f)
    total_new=0
    for y in range(START_YEAR, END_DATE.year+1):
        year_new=0
        for d in days(y):
            ds=d.isoformat()
            try:
                sched=get_json(f"https://statsapi.mlb.com/api/v1/schedule?sportId={SPORT_ID}&date={ds}")
            except Exception:
                time.sleep(SLEEP); continue
            dates=sched.get("dates", [])
            if not dates: continue

            for game in dates[0].get("games", []):
                pk=str(game.get("gamePk"))
                if not pk or pk in seen: continue

                # linescore
                try: line=get_json(f"https://statsapi.mlb.com/api/v1/game/{pk}/linescore")
                except Exception: line={}
                home_runs=safe(line,"teams","home","runs", default=0)
                away_runs=safe(line,"teams","away","runs", default=0)

                game_date=safe(game,"gameDate", default=ds)[:10]
                venue    =safe(game,"venue","name","default")
                home_team=safe(game,"teams","home","team","name","default")
                away_team=safe(game,"teams","away","team","name","default")

                g_w.writerow([pk,game_date,venue,home_team,away_team,home_runs,away_runs])

                # boxscore
                try: box=get_json(f"https://statsapi.mlb.com/api/v1/game/{pk}/boxscore")
                except Exception: box={}

                # team summary
                for side in ("home","away"):
                    team_name=safe(box,"teams",side,"team","name","default")
                    tstats=safe(box,"teams",side,"teamStats","batting", default={}) or {}
                    pstats=safe(box,"teams",side,"teamStats","pitching", default={}) or {}
                    tb_w.writerow([pk,team_name,
                                   tstats.get("hits",0), tstats.get("runs",0), tstats.get("homeRuns",0),
                                   pstats.get("strikeOuts",0), pstats.get("baseOnBalls",0)])

                # players
                for side in ("home","away"):
                    players=safe(box,"teams",side,"players", default={}) or {}
                    team_name=safe(box,"teams",side,"team","name","default")
                    for _, pdata in players.items():
                        pid = safe(pdata,"person","id")
                        name= safe(pdata,"person","fullName")
                        bat = safe(pdata,"stats","batting", default={}) or {}
                        pit = safe(pdata,"stats","pitching", default={}) or {}
                        PA=bat.get("plateAppearances"); H=bat.get("hits"); HR=bat.get("homeRuns")
                        BB=bat.get("baseOnBalls"); SO=bat.get("strikeOuts")
                        IP_outs=pit.get("outs"); K_p=pit.get("strikeOuts")
                        BB_p=pit.get("baseOnBalls"); ER=pit.get("earnedRuns")

                        if not any(v not in (None,0) for v in [PA,H,HR,BB,SO,IP_outs,K_p,BB_p,ER]):
                            continue
                        pb_w.writerow([pk,team_name,pid,name,PA,H,HR,BB,SO,IP_outs,K_p,BB_p,ER])

                seen.add(pk); print(pk, file=seen_out, flush=True)
                total_new+=1; year_new+=1
                time.sleep(SLEEP)

        print(f"[YEAR {y}] new_games={year_new}")
        time.sleep(YEAR_SLEEP)

    print(f"[DONE] total_new_games={total_new}")
PY
