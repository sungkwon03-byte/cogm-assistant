import os, time, json, sqlite3, urllib.request, ssl

DB = "data/lahman.sqlite"
SEASON = int(os.getenv("SEASON", "2025"))
QPS = float(os.getenv("STATSAPI_QPS", "3.0"))  # 초당 호출 제한
SLEEP = 1.0 / max(QPS, 0.1)

def get_json(url):
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={"User-Agent":"curl/8 statsapi"})
    with urllib.request.urlopen(req, context=ctx, timeout=60) as r:
        return json.load(r)

def upsert_batting(cur, playerID, stat):
    AB = int(stat.get("atBats",0)); H=int(stat.get("hits",0))
    _2=int(stat.get("doubles",0)); _3=int(stat.get("triples",0)); HR=int(stat.get("homeRuns",0))
    BB=int(stat.get("baseOnBalls",0)); HBP=int(stat.get("hitByPitch",0))
    SF=int(stat.get("sacFlies",0)); SH=int(stat.get("sacBunts",0)); SO=int(stat.get("strikeOuts",0))
    cur.execute("""
      INSERT INTO Batting(playerID, yearID, stint, AB, H, "2B", "3B", HR, BB, HBP, SF, SH, SO)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
      ON CONFLICT(playerID,yearID,stint) DO UPDATE SET
        AB=excluded.AB, H=excluded.H, "2B"=excluded."2B", "3B"=excluded."3B", HR=excluded.HR,
        BB=excluded.BB, HBP=excluded.HBP, SF=excluded.SF, SH=excluded.SH, SO=excluded.SO
    """, (playerID, SEASON, 1, AB,H,_2,_3,HR,BB,HBP,SF,SH,SO))

def upsert_pitching(cur, playerID, stat):
    W=int(stat.get("wins",0)); L=int(stat.get("losses",0))
    G=int(stat.get("gamesPlayed",0)); GS=int(stat.get("gamesStarted",0)); SV=int(stat.get("saves",0))
    IPouts=int(stat.get("inningsPitched", "0").replace(".",""))
    # StatsAPI는 이닝을 "12.1" 형식으로 주기도 해서 간단 변환( .1=1/3, .2=2/3 )
    if "." in str(stat.get("inningsPitched","0")):
        ip=str(stat["inningsPitched"])
        whole, frac = ip.split("."); outs=int(whole)*3 + (1 if frac=="1" else 2 if frac=="2" else 0)
        IPouts = outs
    SO=int(stat.get("strikeOuts",0)); BB=int(stat.get("baseOnBalls",0)); H=int(stat.get("hits",0))
    HBP=int(stat.get("hitByPitch",0)); ER=int(stat.get("earnedRuns",0))
    cur.execute("""
      INSERT INTO Pitching(playerID, yearID, stint, W, L, G, GS, SV, IPouts, SO, BB, H, HBP, ER)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
      ON CONFLICT(playerID,yearID,stint) DO UPDATE SET
        W=excluded.W, L=excluded.L, G=excluded.G, GS=excluded.GS, SV=excluded.SV, IPouts=excluded.IPouts,
        SO=excluded.SO, BB=excluded.BB, H=excluded.H, HBP=excluded.HBP, ER=excluded.ER
    """, (playerID, SEASON, 1, W,L,G,GS,SV,IPouts,SO,BB,H,HBP,ER))

def main():
    if not os.path.exists(DB): raise SystemExit("missing DB; run lahman_sync first")
    conn = sqlite3.connect(DB)
    conn.execute("PRAGMA journal_mode=WAL")
    cur = conn.cursor()

    # id_map에 mlbam_id가 있는 선수만 대상
    cur.execute("SELECT playerID, mlbam_id FROM id_map WHERE mlbam_id IS NOT NULL")
    rows = cur.fetchall()
    total = len(rows); print("players to update:", total)
    for i,(playerID, mlbam) in enumerate(rows,1):
        # Hitting
        u = f"https://statsapi.mlb.com/api/v1/people/{mlbam}/stats?stats=season&group=hitting&gameType=R&season={SEASON}"
        try:
            j = get_json(u); time.sleep(SLEEP)
            splits = (j.get("stats") or [{}])[0].get("splits") or []
            if splits:
                upsert_batting(cur, playerID, splits[0].get("stat",{}))
        except Exception as e:
            print("hit err", mlbam, e)
        # Pitching
        u = f"https://statsapi.mlb.com/api/v1/people/{mlbam}/stats?stats=season&group=pitching&gameType=R&season={SEASON}"
        try:
            j = get_json(u); time.sleep(SLEEP)
            splits = (j.get("stats") or [{}])[0].get("splits") or []
            if splits:
                upsert_pitching(cur, playerID, splits[0].get("stat",{}))
        except Exception as e:
            print("pit err", mlbam, e)

        if i % 50 == 0: conn.commit()
    conn.commit(); conn.close(); print("done 2025 ingest")
if __name__=="__main__":
    main()
