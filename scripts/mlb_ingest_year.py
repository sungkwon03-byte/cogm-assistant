import os, csv, time, requests, argparse
from pathlib import Path

SPORT_ID=1; TIMEOUT=30; RETRIES=3; BACKOFF=0.6
WORKERS=int(os.environ.get("MLB_WORKERS","16"))
SLEEP_BETWEEN=0.05

def get_json(url, retries=RETRIES):
    last=None
    for i in range(retries):
        try:
            r=requests.get(url, timeout=TIMEOUT)
            if r.ok: return r.json()
            last=f"HTTP {r.status_code}"
        except Exception as e:
            last=repr(e)
        time.sleep(BACKOFF*(2**i))
    raise RuntimeError(f"GET fail: {url} :: {last}")

def safe(d,*ks,default=None):
    for k in ks:
        if d is None: return default
        d=d.get(k)
    return default if d is None else d

def ensure_header(fp, header):
    if not fp.exists():
        with fp.open("w", newline="") as f: csv.writer(f).writerow(header)

def main(year:int):
    from concurrent.futures import ThreadPoolExecutor, as_completed
    out = Path(f"output/shards/{year}"); out.mkdir(parents=True, exist_ok=True)
    games_fp=out/"games.csv"; team_box_fp=out/"team_box.csv"; player_box_fp=out/"player_box.csv"; seen_fp=out/"seen_games.txt"
    ensure_header(games_fp,["game_pk","date","venue","home","away","home_runs","away_runs"])
    ensure_header(team_box_fp,["game_pk","team","hits","runs","hr","so_p","bb_p"])
    ensure_header(player_box_fp,["game_pk","team","mlb_id","name","PA","H","HR","BB","SO","IP_outs","K_p","BB_p","ER"])

    seen=set()
    if seen_fp.exists(): seen.update(x.strip() for x in seen_fp.read_text().splitlines() if x.strip())

    # 스케줄 일괄
    routes = __import__("json").load(open("tools/stats_routes.json"))
    url = routes["schedule"].format(start=f"{year}-01-01", end=f"{year}-12-31")
    data = get_json(url)
    games=[]
    for dd in data.get("dates", []):
        ds=dd.get("date","")[:10]
        for g in dd.get("games", []):
            pk=str(g.get("gamePk"))
            if not pk or pk in seen: continue
            games.append({"pk":pk,"g":g,"d":ds})
    if not games:
        print(f"[{year}] no new games"); return

    def fetch_one(item):
        pk=item["pk"]; g=item["g"]; dstr=item["d"]
        routes = __import__("json").load(open("tools/stats_routes.json"))
        try: line=get_json(routes["linescore"].format(pk=pk))
        except Exception: line={}
        try: box=get_json(routes["boxscore"].format(pk=pk))
        except Exception: box={}
        home_runs=safe(line,"teams","home","runs", default=0)
        away_runs=safe(line,"teams","away","runs", default=0)
        game_date=safe(g,"gameDate", default=dstr)[:10]
        venue=safe(g,"venue","name","default")
        home_team=safe(g,"teams","home","team","name","default")
        away_team=safe(g,"teams","away","team","name","default")

        team_rows=[]
        for side in ("home","away"):
            team_name=safe(box,"teams",side,"team","name","default")
            tstats=safe(box,"teams",side,"teamStats","batting", default={}) or {}
            pstats=safe(box,"teams",side,"teamStats","pitching", default={}) or {}
            team_rows.append([pk,team_name,tstats.get("hits",0),tstats.get("runs",0),tstats.get("homeRuns",0),pstats.get("strikeOuts",0),pstats.get("baseOnBalls",0)])

        player_rows=[]
        for side in ("home","away"):
            players=safe(box,"teams",side,"players", default={}) or {}
            team_name=safe(box,"teams",side,"team","name","default")
            for _, pdata in players.items():
                person=safe(pdata,"person", default={}) or {}
                pid=person.get("id"); name=person.get("fullName")
                bat=safe(pdata,"stats","batting", default={}) or {}
                pit=safe(pdata,"stats","pitching", default={}) or {}
                PA=bat.get("plateAppearances"); H=bat.get("hits"); HR=bat.get("homeRuns"); BB=bat.get("baseOnBalls"); SO=bat.get("strikeOuts")
                IP_outs=pit.get("outs"); K_p=pit.get("strikeOuts"); BB_p=pit.get("baseOnBalls"); ER=pit.get("earnedRuns")
                if not any(v not in (None,0) for v in [PA,H,HR,BB,SO,IP_outs,K_p,BB_p,ER]): continue
                player_rows.append([pk,team_name,pid,name,PA,H,HR,BB,SO,IP_outs,K_p,BB_p,ER])
        return pk,[pk,game_date,venue,home_team,away_team,home_runs,away_runs],team_rows,player_rows

    new_cnt=0
    with games_fp.open("a", newline="") as g_f, team_box_fp.open("a", newline="") as tb_f, player_box_fp.open("a", newline="") as pb_f, seen_fp.open("a") as seen_out, ThreadPoolExecutor(max_workers=WORKERS) as ex:
        g_w=csv.writer(g_f); tb_w=csv.writer(tb_f); pb_w=csv.writer(pb_f)
        futs={ex.submit(fetch_one, it): it["pk"] for it in games}
        for fut in __import__("concurrent.futures").as_completed(futs):
            pk=futs[fut]
            try:
                pk_out, g_row, t_rows, p_rows = fut.result()
            except Exception:
                continue
            g_w.writerow(g_row)
            for r in t_rows: tb_w.writerow(r)
            for r in p_rows: pb_w.writerow(r)
            print(pk, file=seen_out, flush=True)
            new_cnt += 1
            time.sleep(SLEEP_BETWEEN)
    print(f"[{year}] new_games={new_cnt}")
if __name__=="__main__":
    ap=argparse.ArgumentParser(); ap.add_argument("--year", type=int, required=True); args=ap.parse_args(); main(args.year)
