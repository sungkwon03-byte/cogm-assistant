#!/usr/bin/env bash
set -euo pipefail
export PYTHONUNBUFFERED=1
mkdir -p output output/cache

python3 - <<'PY'
import os, csv, time, math, requests
from pathlib import Path
from datetime import date, datetime, timedelta

# -------- 설정 --------
SPORT_ID = 1               # MLB
START_YEAR = 1871          # 프로야구 초창기
END_DATE = date.today()    # 오늘(서버 기준)
REQ_TIMEOUT = 30
RETRIES = 3
BASE_SLEEP = 0.5           # 기본 대기 (레이트리밋 보호)
YEAR_SLEEP = 1.5           # 연도 경계 대기

out_dir = Path("output")
cache_dir = Path("output/cache")
out_dir.mkdir(parents=True, exist_ok=True)
cache_dir.mkdir(parents=True, exist_ok=True)

games_fp      = out_dir/"games.csv"
team_box_fp   = out_dir/"team_box.csv"
player_box_fp = out_dir/"player_box.csv"
seen_fp       = cache_dir/"seen_games.txt"

# 머리글 생성(없을 때만)
def ensure_header(fp, header):
    if not fp.exists():
        with fp.open("w", newline="") as f:
            csv.writer(f).writerow(header)

ensure_header(games_fp,      ["game_pk","date","venue","home","away","home_runs","away_runs"])
ensure_header(team_box_fp,   ["game_pk","team","hits","runs","hr","so_p","bb_p"])
ensure_header(player_box_fp, ["game_pk","team","mlb_id","name","PA","H","HR","BB","SO","IP_outs","K_p","BB_p","ER"])

# 이미 처리한 gamePk 집합(재시작/이어받기용)
seen = set()
if seen_fp.exists():
    seen.update(x.strip() for x in seen_fp.read_text().splitlines() if x.strip())

def get_json(url, retries=RETRIES, sleep=BASE_SLEEP):
    last = None
    for i in range(retries):
        try:
            r = requests.get(url, timeout=REQ_TIMEOUT)
            if r.ok:
                return r.json()
            last = f"HTTP {r.status_code}"
        except Exception as e:
            last = repr(e)
        time.sleep(sleep * (i+1))
    raise RuntimeError(f"GET fail: {url} :: {last}")

def safe(d, *keys, default=None):
    for k in keys:
        if d is None: return default
        d = d.get(k)
    return default if d is None else d

# 날짜 이터레이터(연도 단위로 쪼개서 API 부하 분산)
def days_in_year(y):
    start = date(y, 1, 1)
    end   = date(y, 12, 31)
    if end > END_DATE: end = END_DATE
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)

# 메인 루프
with seen_fp.open("a") as seen_out, \
     games_fp.open("a", newline="") as g_f, \
     team_box_fp.open("a", newline="") as tb_f, \
     player_box_fp.open("a", newline="") as pb_f:

    g_w  = csv.writer(g_f)
    tb_w = csv.writer(tb_f)
    pb_w = csv.writer(pb_f)

    total_new = 0
    for y in range(START_YEAR, END_DATE.year + 1):
        year_new = 0
        for d in days_in_year(y):
            dstr = d.isoformat()
            sched_url = f"https://statsapi.mlb.com/api/v1/schedule?sportId={SPORT_ID}&date={dstr}"
            try:
                sched = get_json(sched_url)
            except Exception:
                # 날짜 단위 에러는 건너뛰고 다음날 진행
                time.sleep(BASE_SLEEP)
                continue

            dates = sched.get("dates", [])
            if not dates:
                continue

            for game in dates[0].get("games", []):
                pk = str(game.get("gamePk"))
                if not pk or pk in seen:
                    continue

                # linescore
                try:
                    line = get_json(f"https://statsapi.mlb.com/api/v1/game/{pk}/linescore")
                except Exception:
                    # 라인스코어 실패 시 점수는 0으로 두고 진행
                    line = {}

                home_runs = safe(line,"teams","home","runs", default=0)
                away_runs = safe(line,"teams","away","runs", default=0)

                game_date = safe(game,"gameDate", default=dstr)[:10]
                venue     = safe(game,"venue","name","default")
                home_team = safe(game,"teams","home","team","name","default")
                away_team = safe(game,"teams","away","team","name","default")

                g_w.writerow([pk, game_date, venue, home_team, away_team, home_runs, away_runs])

                # boxscore
                try:
                    box = get_json(f"https://statsapi.mlb.com/api/v1/game/{pk}/boxscore")
                except Exception:
                    box = {}

                # 팀 요약
                for side in ("home","away"):
                    team_name = safe(box,"teams",side,"team","name","default")
                    tstats = safe(box,"teams",side,"teamStats","batting", default={}) or {}
                    pstats = safe(box,"teams",side,"teamStats","pitching", default={}) or {}
                    tb_w.writerow([
                        pk, team_name,
                        tstats.get("hits",0),
                        tstats.get("runs",0),
                        tstats.get("homeRuns",0),
                        pstats.get("strikeOuts",0),
                        pstats.get("baseOnBalls",0)
                    ])

                # 선수(타/투 통합)
                for side in ("home","away"):
                    players = safe(box,"teams",side,"players", default={}) or {}
                    team_name = safe(box,"teams",side,"team","name","default")
                    for pid, pdata in players.items():
                        person = safe(pdata,"person", default={}) or {}
                        pid_num = person.get("id")
                        name = person.get("fullName")
                        bat = safe(pdata,"stats","batting", default={}) or {}
                        pit = safe(pdata,"stats","pitching", default={}) or {}

                        PA = bat.get("plateAppearances"); H  = bat.get("hits")
                        HR = bat.get("homeRuns");        BB = bat.get("baseOnBalls")
                        SO = bat.get("strikeOuts")
                        IP_outs = pit.get("outs");       K_p = pit.get("strikeOuts")
                        BB_p    = pit.get("baseOnBalls");ER  = pit.get("earnedRuns")

                        if not any(v not in (None,0) for v in [PA,H,HR,BB,SO,IP_outs, K_p,BB_p,ER]):
                            continue

                        pb_w.writerow([pk, team_name, pid_num, name, PA, H, HR, BB, SO, IP_outs, K_p, BB_p, ER])

                seen.add(pk)
                print(pk, file=seen_out, flush=True)
                total_new += 1
                year_new  += 1
                time.sleep(BASE_SLEEP)  # 레이트리밋 보호

        # 연도 경계 휴식
        time.sleep(YEAR_SLEEP)
        print(f"[YEAR {y}] new_games={year_new}")

    print(f"[DONE] total_new_games={total_new}")
PY
