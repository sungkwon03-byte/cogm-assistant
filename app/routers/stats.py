# app/main.py
import os, sqlite3, time, functools, threading
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

app = FastAPI(title="Co-GM Assistant")

DB_PATH = os.getenv("DB_PATH", "data/lahman.sqlite")
REDIS_URL = os.getenv("REDIS_URL", "")

# ----------------------------
# DB & 헬퍼
# ----------------------------
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _rval(row: sqlite3.Row, col: str, default=None):
    try:
        return row[col]
    except Exception:
        return default

def row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {k: row[k] for k in row.keys()}

# ----------------------------
# 간단 캐시 (Redis 있으면 Redis, 없으면 메모리)
# ----------------------------
try:
    if REDIS_URL:
        import redis  # type: ignore
        _rc = redis.from_url(REDIS_URL, decode_responses=True)
    else:
        _rc = None
except Exception:
    _rc = None

_mem_cache = {}
_mem_lock = threading.Lock()

def cache_get(k: str) -> Optional[str]:
    if _rc:
        return _rc.get(k)
    with _mem_lock:
        v = _mem_cache.get(k)
        if not v:
            return None
        # v = (expire_ts, data)
        if v[0] and v[0] < time.time():
            _mem_cache.pop(k, None)
            return None
        return v[1]

def cache_set(k: str, v: str, ttl: int = 300):
    if _rc:
        _rc.setex(k, ttl, v)
        return
    with _mem_lock:
        expire_ts = time.time() + ttl if ttl else None
        _mem_cache[k] = (expire_ts, v)

# ----------------------------
# 통계 계산 헬퍼 (OPS+/ERA+)
# ----------------------------
def safe_div(n, d):
    try:
        return n / d if d else 0.0
    except ZeroDivisionError:
        return 0.0

def compute_obp(h, bb, hbp, ab, sf):
    return safe_div(h + bb + hbp, ab + bb + hbp + sf)

def compute_slg(h, doubles, triples, hr, ab):
    singles = max(h - doubles - triples - hr, 0)
    tb = singles + 2*doubles + 3*triples + 4*hr
    return safe_div(tb, ab)

def league_ops(conn, year: int) -> float:
    q = """
    SELECT SUM(H) as H, SUM(BB) as BB, SUM(HBP) as HBP, SUM(AB) as AB, SUM(SF) as SF,
           SUM("2B") as D2, SUM("3B") as D3, SUM(HR) as HR
    FROM Batting WHERE yearID = ?
    """
    r = conn.execute(q, (year,)).fetchone()
    if not r or _rval(r, "AB", 0) == 0:
        return 0.0
    obp = compute_obp(_rval(r,"H",0), _rval(r,"BB",0), _rval(r,"HBP",0), _rval(r,"AB",0), _rval(r,"SF",0))
    slg = compute_slg(_rval(r,"H",0), _rval(r,"D2",0), _rval(r,"D3",0), _rval(r,"HR",0), _rval(r,"AB",0))
    return obp + slg

def league_era(conn, year: int) -> float:
    q = """
    SELECT SUM(ER) as ER, SUM(IPouts) as IPouts
    FROM Pitching WHERE yearID = ?
    """
    r = conn.execute(q, (year,)).fetchone()
    ip = safe_div(_rval(r,"IPouts",0), 3.0)
    return 9.0 * safe_div(_rval(r,"ER",0), ip) if ip > 0 else 0.0

def player_ops_plus(conn, playerID: str, year: int) -> float:
    # 선수 누적
    q = """
    SELECT SUM(H) H, SUM(BB) BB, SUM(HBP) HBP, SUM(AB) AB, SUM(SF) SF,
           SUM("2B") D2, SUM("3B") D3, SUM(HR) HR
    FROM Batting WHERE playerID = ? AND yearID = ?
    """
    r = conn.execute(q, (playerID, year)).fetchone()
    if not r or _rval(r, "AB", 0) == 0:
        return 0.0
    obp = compute_obp(_rval(r,"H",0), _rval(r,"BB",0), _rval(r,"HBP",0), _rval(r,"AB",0), _rval(r,"SF",0))
    slg = compute_slg(_rval(r,"H",0), _rval(r,"D2",0), _rval(r,"D3",0), _rval(r,"HR",0), _rval(r,"AB",0))
    ops = obp + slg
    lg_ops = league_ops(conn, year)
    return 100.0 * safe_div(ops, lg_ops) if lg_ops > 0 else 0.0

def pitcher_era_plus(conn, playerID: str, year: int) -> float:
    q = """
    SELECT SUM(ER) ER, SUM(IPouts) IPouts
    FROM Pitching WHERE playerID = ? AND yearID = ?
    """
    r = conn.execute(q, (playerID, year)).fetchone()
    ip = safe_div(_rval(r,"IPouts",0), 3.0)
    era = 9.0 * safe_div(_rval(r,"ER",0), ip) if ip > 0 else 0.0
    lg_era = league_era(conn, year)
    return 100.0 * safe_div(lg_era, era) if era > 0 else 0.0

def find_player_ids_by_name(conn, name: str) -> List[Dict[str, str]]:
    name = name.strip()
    # "first last" 분해 탐색 + 부분일치
    parts = [p for p in name.split() if p]
    if len(parts) >= 2:
        fn, ln = parts[0], parts[-1]
        q = """
        SELECT playerID, nameFirst, nameLast
        FROM People
        WHERE (nameFirst || ' ' || nameLast) LIKE ? COLLATE NOCASE
        ORDER BY debut IS NULL, debut
        """
        like = f"%{fn}% {ln}%"
        rows = conn.execute(q, (like,)).fetchall()
    else:
        q = """
        SELECT playerID, nameFirst, nameLast
        FROM People
        WHERE nameFirst LIKE ? OR nameLast LIKE ?
        COLLATE NOCASE
        ORDER BY debut IS NULL, debut
        """
        like = f"%{name}%"
        rows = conn.execute(q, (like, like)).fetchall()
    return [{"playerID": _rval(r,"playerID",""),
             "name": f"{_rval(r,'nameFirst','')} {_rval(r,'nameLast','')}".strip()} for r in rows]

# ----------------------------
# 1) Player Power Rankings (이름 join 반영)
# ----------------------------
@app.get("/player_power_rankings")
def player_power_rankings(season: int = Query(..., ge=1871, le=2100),
                          limit: int = Query(25, ge=1, le=200)) -> Dict[str, Any]:
    """
    시즌별 타자 OPS+ Top N, 투수 ERA+ Top N (합쳐서 정렬하지 않고 role 단위 제공)
    이름은 People join으로 정상 표기.
    """
    import json
    cache_key = f"ppr:{season}:{limit}"
    cached = cache_get(cache_key)
    if cached:
        return json.loads(cached)

    conn = get_db()
    # 타자 후보: 시즌 타석/AB 필터(노이즈 제거를 위해 AB>=200)
    bat_q = """
    SELECT b.playerID
    FROM Batting b
    WHERE b.yearID = ? 
    GROUP BY b.playerID
    HAVING SUM(AB) >= 200
    """
    bat_ids = [r["playerID"] for r in conn.execute(bat_q, (season,)).fetchall()]

    bat_list = []
    for pid in bat_ids:
        opsp = player_ops_plus(conn, pid, season)
        if opsp <= 0:
            continue
        p = conn.execute("SELECT nameFirst, nameLast FROM People WHERE playerID = ?", (pid,)).fetchone()
        name = f"{_rval(p,'nameFirst','')} {_rval(p,'nameLast','')}".strip() if p else pid
        bat_list.append({"playerID": pid, "name": name, "season": season, "OPS_plus": round(opsp,1)})

    bat_list.sort(key=lambda x: x["OPS_plus"], reverse=True)
    bat_list = bat_list[:limit]

    # 투수 후보: IP>=80
    pit_q = """
    SELECT p.playerID
    FROM Pitching p
    WHERE p.yearID = ?
    GROUP BY p.playerID
    HAVING SUM(IPouts) >= 80*3
    """
    pit_ids = [r["playerID"] for r in conn.execute(pit_q, (season,)).fetchall()]

    pit_list = []
    for pid in pit_ids:
        erap = pitcher_era_plus(conn, pid, season)
        if erap <= 0:
            continue
        p = conn.execute("SELECT nameFirst, nameLast FROM People WHERE playerID = ?", (pid,)).fetchone()
        name = f"{_rval(p,'nameFirst','')} {_rval(p,'nameLast','')}".strip() if p else pid
        pit_list.append({"playerID": pid, "name": name, "season": season, "ERA_plus": round(erap,1)})

    pit_list.sort(key=lambda x: x["ERA_plus"], reverse=True)
    pit_list = pit_list[:limit]

    out = {"season": season, "batting_top": bat_list, "pitching_top": pit_list}
    cache_set(cache_key, json.dumps(out), ttl=300)
    return out

# ----------------------------
# 2) Compare Players API (OPS+/ERA+ 비교)
# ----------------------------
class PlayerCompareResponse(BaseModel):
    season: int
    resolved: Dict[str, Any]
    hitters: List[Dict[str, Any]]
    pitchers: List[Dict[str, Any]]

@app.get("/compare_players", response_model=PlayerCompareResponse)
def compare_players(name1: str = Query(..., description="예: Bryce Harper"),
                    name2: str = Query(..., description="예: Mookie Betts"),
                    season: int = Query(..., ge=1871, le=2100)):
    conn = get_db()
    cands1 = find_player_ids_by_name(conn, name1)
    cands2 = find_player_ids_by_name(conn, name2)
    if not cands1 or not cands2:
        raise HTTPException(status_code=404, detail="플레이어 이름 해석 실패")

    pid1, disp1 = cands1[0]["playerID"], cands1[0]["name"]
    pid2, disp2 = cands2[0]["playerID"], cands2[0]["name"]

    # 타자 OPS+
    ops1 = player_ops_plus(conn, pid1, season)
    ops2 = player_ops_plus(conn, pid2, season)

    # 투수 ERA+
    era1 = pitcher_era_plus(conn, pid1, season)
    era2 = pitcher_era_plus(conn, pid2, season)

    hitters = []
    if ops1 > 0: hitters.append({"player": disp1, "playerID": pid1, "season": season, "OPS_plus": round(ops1,1)})
    if ops2 > 0: hitters.append({"player": disp2, "playerID": pid2, "season": season, "OPS_plus": round(ops2,1)})

    pitchers = []
    if era1 > 0: pitchers.append({"player": disp1, "playerID": pid1, "season": season, "ERA_plus": round(era1,1)})
    if era2 > 0: pitchers.append({"player": disp2, "playerID": pid2, "season": season, "ERA_plus": round(era2,1)})

    return {
        "season": season,
        "resolved": {"name1": disp1, "playerID1": pid1, "name2": disp2, "playerID2": pid2},
        "hitters": sorted(hitters, key=lambda x: x["OPS_plus"], reverse=True),
        "pitchers": sorted(pitchers, key=lambda x: x["ERA_plus"], reverse=True)
    }

# ----------------------------
# 3) 진행률 API (/progress)
# ----------------------------
TOTAL_FEATURES = 49
COMPLETED = 11   # 현재까지 구현 개수
PENDING = TOTAL_FEATURES - COMPLETED

@app.get("/progress")
def progress():
    return {
        "total_features": TOTAL_FEATURES,
        "completed": COMPLETED,
        "pending": PENDING,
        "completion_rate": round(100.0 * COMPLETED / TOTAL_FEATURES, 2),
        "buckets": {
            "Player_Intelligence": {"done": 3, "total": 6},
            "Roster_Payroll": {"done": 0, "total": 5},
            "Transactions_Draft": {"done": 0, "total": 5},
            "Game_Prep_Forecast": {"done": 0, "total": 4},
            "Intel_Reporting": {"done": 6, "total": 7},
            "Ops_International_XAI": {"done": 0, "total": 9},
            "P_extensions": {"done": 0, "total": 7},
            "C_quality": {"done": 0, "total": 6}
        }
    }

# ----------------------------
# 헬스체크
# ----------------------------
@app.get("/_ping")
def ping():
    return {"ok": True}

