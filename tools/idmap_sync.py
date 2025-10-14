import os, io, csv, sqlite3, urllib.request, ssl

DB = "data/lahman.sqlite"
REG_URL = "https://raw.githubusercontent.com/chadwickbureau/register/master/data/people.csv"

def fetch(url):
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={"User-Agent":"curl/8 idmap"})
    with urllib.request.urlopen(req, context=ctx, timeout=90) as r:
        return r.read().decode("utf-8", errors="ignore")

def main():
    if not os.path.exists(DB):
        raise SystemExit("missing DB; run lahman_sync first")
    txt = fetch(REG_URL)
    rdr = csv.DictReader(io.StringIO(txt))
    # register 헤더: key_bbref, key_mlbam, key_retro, name_first, name_last, ...
    conn = sqlite3.connect(DB); cur = conn.cursor()
    cur.execute("DELETE FROM id_map")
    # Lahman People에 있는 bbrefID/retroID와 매칭
    for r in rdr:
        bb = r.get("key_bbref") or ""
        rt = r.get("key_retro") or ""
        ml = int(r.get("key_mlbam") or 0) or None
        if not (bb or rt or ml): 
            continue
        # 우선 bbref → 그다음 retro
        row=None
        if bb:
            cur.execute("SELECT playerID, bbrefID, retroID FROM People WHERE bbrefID=?", (bb,))
            row = cur.fetchone()
        if (row is None) and rt:
            cur.execute("SELECT playerID, bbrefID, retroID FROM People WHERE retroID=?", (rt,))
            row = cur.fetchone()
        if row:
            pid, bbrefID, retroID = row
            cur.execute("INSERT OR REPLACE INTO id_map(playerID, mlbam_id, bbrefID, retroID) VALUES(?,?,?,?)",
                        (pid, ml, bb or bbrefID, rt or retroID))
    conn.commit(); conn.close()
    print("id_map updated")

if __name__=="__main__":
    main()
