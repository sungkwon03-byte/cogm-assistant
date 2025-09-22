import os, io, csv, zipfile, sqlite3, urllib.request, ssl, hashlib, json, time, sys
from contextlib import closing

OUT_SQLITE = "data/lahman.sqlite"
STATE_JSON  = "data/lahman_state.json"
# 환경변수로 특정 릴리스 강제 가능 (예: LAHMAN_RELEASE=2024.1)
VER = os.getenv("LAHMAN_RELEASE", "latest")

# 1) 다운로드 후보(우선순위: SABR → GitHub release → GitHub heads)
CANDIDATES = []
# SABR(페이지 구조 변동 가능, 직접 버전링크를 넣을 수도 있음)
# 아래는 빈 슬롯(오프라인 환경 회피용). 필요시 정확 링크를 넣어도 됨.
SABR_CSV_DIRECT = os.getenv("SABR_LAHMAN_ZIP", "").strip()
if SABR_CSV_DIRECT:
    CANDIDATES.append(("sabr", SABR_CSV_DIRECT))
# GitHub 릴리스(버전 지정 시)
if VER != "latest":
    CANDIDATES.append(("gh_release", f"https://github.com/chadwickbureau/baseballdatabank/releases/download/{VER}/baseballdatabank-{VER}.zip"))
# GitHub heads (master/main)
CANDIDATES += [
    ("gh_master", "https://github.com/chadwickbureau/baseballdatabank/archive/refs/heads/master.zip"),
    ("gh_main",   "https://github.com/chadwickbureau/baseballdatabank/archive/refs/heads/main.zip"),
]

def log(*a): print("[lahman_sync]", *a)

def http_get(url, timeout=120):
    log("downloading:", url)
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={"User-Agent": "curl/8 lahman-sync"})
    with closing(urllib.request.urlopen(req, context=ctx, timeout=timeout)) as r:
        return r.read()

def detect_core_names(z):
    names = z.namelist()
    def pick(target):
        # core/ 또는 임의 폴더 하위에서 말단 파일명 매칭
        cands = [n for n in names if n.endswith("/core/"+target) or n.endswith("/"+target)]
        if not cands: raise FileNotFoundError(target)
        # 가장 짧은 경로 우선
        return sorted(cands, key=len)[0]
    return {
        "People":  pick("People.csv"),
        "Batting": pick("Batting.csv"),
        "Pitching":pick("Pitching.csv"),
        "Fielding":pick("Fielding.csv"),
    }

def to_int(x): 
    try: return int(x)
    except: return 0

def build_sqlite_from_zip(buf: bytes):
    os.makedirs("data", exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(buf)) as z:
        core = detect_core_names(z)
        conn = sqlite3.connect(OUT_SQLITE)
        cur  = conn.cursor()
        # People: 필수 컬럼 + 식별자( retroID, bbrefID ) 포함
        cur.executescript("""
        PRAGMA journal_mode=WAL;
        DROP TABLE IF EXISTS People;
        CREATE TABLE People(
            playerID TEXT PRIMARY KEY,
            birthYear INT, birthMonth INT, birthDay INT,
            nameFirst TEXT, nameLast TEXT,
            bbrefID TEXT, retroID TEXT
        );
        DROP TABLE IF EXISTS Batting;
        CREATE TABLE Batting(
            playerID TEXT, yearID INT, stint INT DEFAULT 1,
            AB INT, H INT, "2B" INT, "3B" INT, HR INT,
            BB INT, HBP INT, SF INT, SH INT, SO INT
        );
        DROP TABLE IF EXISTS Pitching;
        CREATE TABLE Pitching(
            playerID TEXT, yearID INT, stint INT DEFAULT 1,
            W INT, L INT, G INT, GS INT, SV INT, IPouts INT,
            SO INT, BB INT, H INT, HBP INT, ER INT
        );
        DROP TABLE IF EXISTS Fielding;
        CREATE TABLE Fielding(
            playerID TEXT, yearID INT, stint INT DEFAULT 1,
            POS TEXT, G INT, GS INT, InnOuts INT, PO INT, A INT, E INT, DP INT
        );
        -- ID 매핑(후속 idmap_sync에서 채움)
        DROP TABLE IF EXISTS id_map;
        CREATE TABLE id_map(
            playerID TEXT PRIMARY KEY,
            mlbam_id INT, bbrefID TEXT, retroID TEXT
        );
        """)
        # load People
        with z.open(core["People"]) as f:
            rdr = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
            rows = []
            for r in rdr:
                rows.append((r["playerID"], to_int(r.get("birthYear")), to_int(r.get("birthMonth")), to_int(r.get("birthDay")),
                             r.get("nameFirst","") or "", r.get("nameLast","") or "",
                             r.get("bbrefID","") or "", r.get("retroID","") or ""))
            cur.executemany("INSERT INTO People VALUES(?,?,?,?,?,?,?,?)", rows)
        # Batting
        with z.open(core["Batting"]) as f:
            rdr = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
            rows=[]
            for r in rdr:
                I=lambda k: to_int(r.get(k))
                rows.append((r["playerID"], I("yearID"), to_int(r.get("stint",1)),
                             I("AB"), I("H"), I("2B"), I("3B"), I("HR"),
                             I("BB"), I("HBP"), I("SF"), I("SH"), I("SO")))
            cur.executemany("INSERT INTO Batting VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
        # Pitching
        with z.open(core["Pitching"]) as f:
            rdr = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
            rows=[]
            for r in rdr:
                I=lambda k: to_int(r.get(k))
                rows.append((r["playerID"], I("yearID"), to_int(r.get("stint",1)),
                             I("W"), I("L"), I("G"), I("GS"), I("SV"), I("IPouts"),
                             I("SO"), I("BB"), I("H"), I("HBP"), I("ER")))
            cur.executemany("INSERT INTO Pitching VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
        # Fielding(핵심만)
        with z.open(core["Fielding"]) as f:
            rdr = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
            rows=[]
            for r in rdr:
                I=lambda k: to_int(r.get(k))
                rows.append((r["playerID"], I("yearID"), to_int(r.get("stint",1)),
                             r.get("POS","") or "", I("G"), I("GS"), I("InnOuts"), I("PO"), I("A"), I("E"), I("DP")))
            cur.executemany("INSERT INTO Fielding VALUES(?,?,?,?,?,?,?,?,?,?,?)", rows)

        for idx in [
            "CREATE INDEX IF NOT EXISTS idx_batting_pid_year ON Batting(playerID,yearID)",
            "CREATE INDEX IF NOT EXISTS idx_pitching_pid_year ON Pitching(playerID,yearID)",
            "CREATE INDEX IF NOT EXISTS idx_fielding_pid_year ON Fielding(playerID,yearID)",
        ]: cur.execute(idx)
        conn.commit(); conn.close()

def sha256(b:bytes)->str: return hashlib.sha256(b).hexdigest()

def main():
    os.makedirs("data", exist_ok=True)
    last = {}
    if os.path.exists(STATE_JSON):
        try: last = json.load(open(STATE_JSON,"r",encoding="utf-8"))
        except: last = {}

    err = None
    buf = None
    used = None
    for tag,url in CANDIDATES:
        try:
            buf = http_get(url)
            used = {"source": tag, "url": url}
            break
        except Exception as e:
            err = e; log("try failed:", url, e)

    if buf is None:
        log("ERROR: all download attempts failed"); 
        if err: raise err
        raise SystemExit(1)

    h = sha256(buf)
    if last.get("sha256")==h and os.path.exists(OUT_SQLITE):
        log("no change; keep existing", OUT_SQLITE)
        return

    build_sqlite_from_zip(buf)
    state = {"sha256": h, "ts": int(time.time()), **used}
    json.dump(state, open(STATE_JSON,"w",encoding="utf-8"), ensure_ascii=False, indent=2)
    log("updated:", OUT_SQLITE, state)

if __name__=="__main__":
    main()
