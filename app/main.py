# app/main.py  — stable rebuild (OPS+/ERA+, caching, trends)

from __future__ import annotations
import os
import time
import json
import sqlite3
from typing import Any, Callable, Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Query

# -----------------------------
# App & constants
# -----------------------------
app = FastAPI(title="cogm-assistant API (stable)")
DB_PATH = os.environ.get("LAHMAN_DB", "data/lahman.sqlite")

# -----------------------------
# Safe DB
# -----------------------------
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# -----------------------------
# Caching layer (Redis if available, else in-memory)
# -----------------------------
_CACHE_METRICS = {"hits": 0, "miss": 0, "set": 0}
_MEM_CACHE: Dict[str, Tuple[float, Any]] = {}
_REDIS = None          # redis client or None
_REDIS_LAST_ERROR: Optional[str] = None

def _redis_try_init() -> None:
    """Init redis client if REDIS_URL is provided; otherwise keep None."""
    global _REDIS, _REDIS_LAST_ERROR
    url = os.environ.get("REDIS_URL")
    if not url:
        _REDIS = None
        _REDIS_LAST_ERROR = None
        return
    try:
        import redis  # type: ignore
        r = redis.from_url(url, decode_responses=True)  # Upstash works w/o explicit ssl kw
        # health check
        r.ping()
        _REDIS = r
        _REDIS_LAST_ERROR = None
    except Exception as e:  # pragma: no cover
        _REDIS = None
        _REDIS_LAST_ERROR = f"{type(e).__name__}: {e}"

# call once at import
_redis_try_init()

def _cache_get(key: str) -> Optional[Any]:
    now = time.time()
    # redis first
    if _REDIS:
        try:
            raw = _REDIS.get(key)
            if raw is not None:
                _CACHE_METRICS["hits"] += 1
                return json.loads(raw)
            _CACHE_METRICS["miss"] += 1
            return None
        except Exception as e:  # fallback to memory
            global _REDIS_LAST_ERROR
            _REDIS_LAST_ERROR = f"{type(e).__name__}: {e}"
    # memory
    item = _MEM_CACHE.get(key)
    if not item:
        _CACHE_METRICS["miss"] += 1
        return None
    exp, val = item
    if exp >= now:
        _CACHE_METRICS["hits"] += 1
        return val
    # expired
    _CACHE_METRICS["miss"] += 1
    _MEM_CACHE.pop(key, None)
    return None

def _cache_set(key: str, value: Any, ttl: int) -> None:
    exp = time.time() + max(ttl, 1)
    # redis
    if _REDIS:
        try:
            _REDIS.setex(key, ttl, json.dumps(value, separators=(",", ":")))
            _CACHE_METRICS["set"] += 1
            return
        except Exception as e:
            global _REDIS_LAST_ERROR
            _REDIS_LAST_ERROR = f"{type(e).__name__}: {e}"
    # memory
    _MEM_CACHE[key] = (exp, value)
    _CACHE_METRICS["set"] += 1

# decorator
def cached(key_func: Callable[..., str], ttl: int = 600):
    def _decorator(fn: Callable[..., Any]):
        def _wrapped(*args, **kwargs):
            try:
                key = key_func(**kwargs)
            except TypeError:
                # allow lambdas that don't name all kwargs
                key = key_func(*args, **kwargs)  # type: ignore
            hit = _cache_get(key)
            if hit is not None:
                return hit
            val = fn(*args, **kwargs)
            try:
                # ensure JSON-serializable
                json.dumps(val)
                _cache_set(key, val, ttl)
            except TypeError:
                # if not json-serializable, just skip caching
                pass
            return val
        return _wrapped
    return _decorator

# -----------------------------
# Utilities: math & lookups
# -----------------------------
def _safe_div(n: float, d: float) -> float:
    try:
        n = float(n); d = float(d)
        return n / d if d else 0.0
    except Exception:
        return 0.0

def _round(x: float, digits: int = 1) -> float:
    return float(round(x, digits))

def resolve_player_id(conn: sqlite3.Connection, name: str) -> Optional[str]:
    """Find first People.playerID by case-insensitive full name."""
    q = """
    SELECT playerID, nameFirst, nameLast
    FROM People
    WHERE UPPER(nameFirst || ' ' || nameLast) = UPPER(?)
       OR UPPER(nameLast || ', ' || nameFirst) = UPPER(?)
    LIMIT 1
    """
    cur = conn.execute(q, (name, name))
    row = cur.fetchone()
    return row["playerID"] if row else None

# -----------------------------
# League baselines (OPS / ERA)
# -----------------------------
def league_ops(conn: sqlite3.Connection, season: int) -> float:
    q = """
    SELECT
      SUM(b.H)  as H,
      SUM(b.BB) as BB,
      SUM(COALESCE(b.HBP,0)) as HBP,
      SUM(b.AB) as AB,
      SUM(COALESCE(b.SF,0)) as SF,
      SUM(b."2B") as _2B,
      SUM(b."3B") as _3B,
      SUM(b.HR) as HR
    FROM Batting b
    WHERE b.yearID = ?
    """
    row = conn.execute(q, (season,)).fetchone()
    if not row or row["AB"] in (None, 0):
        return 0.0
    H = row["H"] or 0
    BB = row["BB"] or 0
    HBP = row["HBP"] or 0
    AB = row["AB"] or 0
    SF = row["SF"] or 0
    _2B = row["_2B"] or 0
    _3B = row["_3B"] or 0
    HR = row["HR"] or 0
    _1B = H - _2B - _3B - HR
    TB = _1B + 2*_2B + 3*_3B + 4*HR
    OBP = _safe_div(H + BB + HBP, AB + BB + HBP + SF)
    SLG = _safe_div(TB, AB)
    return OBP + SLG

def league_era(conn: sqlite3.Connection, season: int) -> float:
    q = """
    SELECT SUM(p.ER) as ER, SUM(COALESCE(p.IPouts,0)) as IPouts
    FROM Pitching p
    WHERE p.yearID = ?
    """
    row = conn.execute(q, (season,)).fetchone()
    ER = row["ER"] or 0
    IPouts = row["IPouts"] or 0
    IP = _safe_div(IPouts, 3.0)
    return 9.0 * _safe_div(ER, IP)

# -----------------------------
# Basic endpoints
# -----------------------------
@app.get("/_ping")
def ping():
    return {"ok": True}

@app.get("/_metrics")
def metrics():
    return {
        "cache": _CACHE_METRICS,
        "db_path": DB_PATH,
        "redis_enabled": bool(_REDIS),
        "last_error": _REDIS_LAST_ERROR,
    }

@app.post("/_cache_reload")
def cache_reload():
    _redis_try_init()
    return {"redis_enabled": bool(_REDIS), "last_error": _REDIS_LAST_ERROR}

# -----------------------------
# Team endpoints
# -----------------------------
@cached(lambda season, limit=30, **kw: f"tl:{season}:{limit}", ttl=600)
@app.get("/team_leaderboard")
def team_leaderboard(season: int = Query(...), limit: int = Query(30, ge=1, le=60)):
    conn = get_conn()
    lg_ops = league_ops(conn, season)
    lg_era = league_era(conn, season)

    # Batting만으로 팀 OPS 집계 (조인 제거)
    q_bat = """
    SELECT b.teamID AS team, b.yearID AS season,
           SUM(b.H) AS H, SUM(b.BB) AS BB,
           SUM(COALESCE(b.HBP,0)) AS HBP,
           SUM(b.AB) AS AB, SUM(COALESCE(b.SF,0)) AS SF,
           SUM(b."2B") AS _2B, SUM(b."3B") AS _3B, SUM(b.HR) AS HR
    FROM Batting b
    WHERE b.yearID = ?
    GROUP BY b.teamID, b.yearID
    """
    bat = []
    for r in conn.execute(q_bat, (season,)):
        H = r["H"] or 0; BB = r["BB"] or 0; HBP = r["HBP"] or 0
        AB = r["AB"] or 0; SF = r["SF"] or 0
        _2B = r["_2B"] or 0; _3B = r["_3B"] or 0; HR = r["HR"] or 0
        _1B = H - _2B - _3B - HR
        TB = _1B + 2*_2B + 3*_3B + 4*HR
        OBP = _safe_div(H + BB + HBP, AB + BB + HBP + SF)
        SLG = _safe_div(TB, AB)
        OPS = OBP + SLG
        OPS_plus = 100.0 * _safe_div(OPS, lg_ops) if lg_ops else 0.0
        bat.append({"team": r["team"], "season": r["season"], "OPS_plus": _round(OPS_plus, 1)})
    bat.sort(key=lambda x: x["OPS_plus"], reverse=True)
    top_bat = bat[:limit]

    # Pitching만으로 팀 ERA 집계 (조인 제거)
    q_pit = """
    SELECT p.teamID AS team, p.yearID AS season,
           SUM(p.ER) AS ER, SUM(COALESCE(p.IPouts,0)) AS IPouts
    FROM Pitching p
    WHERE p.yearID = ?
    GROUP BY p.teamID, p.yearID
    """
    pit = []
    for r in conn.execute(q_pit, (season,)):
        ER = r["ER"] or 0
        IP = _safe_div(r["IPouts"] or 0, 3.0)
        era = 9.0 * _safe_div(ER, IP)
        ERA_plus = 100.0 * _safe_div(lg_era, era) if era else 0.0
        pit.append({"team": r["team"], "season": r["season"], "ERA_plus": _round(ERA_plus, 1)})
    pit.sort(key=lambda x: x["ERA_plus"], reverse=True)
    top_pit = pit[:limit]

    return {
        "season": season,
        "baselines": {"league_ops": _round(lg_ops, 6), "league_era": _round(lg_era, 6)},
        "top_bat": top_bat,
        "top_pit": top_pit,
    }

@cached(lambda season, limit=30, bat_w=0.5, pit_w=0.5, **kw: f"tpr:{season}:{limit}:{bat_w}:{pit_w}", ttl=600)
@app.get("/team_power_rankings")
def team_power_rankings(
    season: int = Query(...),
    limit: int = Query(30, ge=1, le=60),
    bat_w: float = Query(0.5, ge=0.0, le=1.0),
    pit_w: float = Query(0.5, ge=0.0, le=1.0),
):
    lb = team_leaderboard(season=season, limit=60)
    ops_map = {x["team"]: x["OPS_plus"] for x in lb["top_bat"]}
    era_map = {x["team"]: x["ERA_plus"] for x in lb["top_pit"]}
    teams = sorted(set(ops_map) | set(era_map))
    out = []
    for t in teams:
        opsp = ops_map.get(t, 0.0)
        erap = era_map.get(t, 0.0)
        score = bat_w * opsp + pit_w * erap
        out.append({"team": t, "season": season, "OPS_plus": opsp, "ERA_plus": erap, "score": _round(score, 2)})
    out.sort(key=lambda x: x["score"], reverse=True)
    return out[:limit]

# -----------------------------
# Player endpoints
# -----------------------------
@app.get("/get_player_stats")
def get_player_stats(name: str, season: int):
    conn = get_conn()
    pid = resolve_player_id(conn, name)
    if not pid:
        raise HTTPException(status_code=404, detail=f"Player '{name}' not found")

    q = """
    SELECT
      SUM(b.H) as H, SUM(b.AB) as AB, SUM(b.BB) as BB,
      SUM(COALESCE(b.HBP,0)) as HBP, SUM(COALESCE(b.SF,0)) as SF,
      SUM(b."2B") as _2B, SUM(b."3B") as _3B, SUM(b.HR) as HR
    FROM Batting b
    WHERE b.playerID = ? AND b.yearID = ?
    """
    r = conn.execute(q, (pid, season)).fetchone()
    if not r or (r["AB"] or 0) == 0:
        return {"player": name, "playerID": pid, "season": season, "ops": 0.0}

    H = r["H"] or 0; AB = r["AB"] or 0; BB = r["BB"] or 0
    HBP = r["HBP"] or 0; SF = r["SF"] or 0
    _2B = r["_2B"] or 0; _3B = r["_3B"] or 0; HR = r["HR"] or 0
    _1B = H - _2B - _3B - HR
    TB = _1B + 2*_2B + 3*_3B + 4*HR
    OBP = _safe_div(H + BB + HBP, AB + BB + HBP + SF)
    SLG = _safe_div(TB, AB)
    OPS = OBP + SLG
    return {"player": name, "playerID": pid, "season": season, "obp": _round(OBP, 3), "slg": _round(SLG, 3), "ops": _round(OPS, 3),
            "H": H, "AB": AB, "BB": BB, "HR": HR}

@app.get("/get_pitching_stats")
def get_pitching_stats(name: str, season: int):
    conn = get_conn()
    pid = resolve_player_id(conn, name)
    if not pid:
        raise HTTPException(status_code=404, detail=f"Player '{name}' not found")
    q = """
    SELECT SUM(p.ER) as ER, SUM(COALESCE(p.IPouts,0)) as IPouts
    FROM Pitching p WHERE p.playerID = ? AND p.yearID = ?
    """
    r = conn.execute(q, (pid, season)).fetchone()
    ER = r["ER"] or 0
    IP = _safe_div(r["IPouts"] or 0, 3.0)
    ERA = 9.0 * _safe_div(ER, IP)
    return {"player": name, "playerID": pid, "season": season, "era": _round(ERA, 2), "ER": ER, "IP": _round(IP, 1)}

@app.get("/compare_players")
def compare_players(name1: str, name2: str, season: int):
    return {
        "season": season,
        "player1": get_player_stats(name1, season),
        "player2": get_player_stats(name2, season),
    }

@app.get("/get_player_trend")
def get_player_trend(name: str, season: int, years: int = 3):
    conn = get_conn()
    pid = resolve_player_id(conn, name)
    if not pid:
        return {"player": name, "playerID": "", "trend": []}

    start = max(season - years + 1, 1871)
    q = """
    SELECT yearID as season,
           SUM(b.H) as H, SUM(b.AB) as AB, SUM(b.BB) as BB,
           SUM(COALESCE(b.HBP,0)) as HBP, SUM(COALESCE(b.SF,0)) as SF,
           SUM(b."2B") as _2B, SUM(b."3B") as _3B, SUM(b.HR) as HR
    FROM Batting b
    WHERE b.playerID = ? AND b.yearID BETWEEN ? AND ?
    GROUP BY yearID
    ORDER BY yearID
    """
    rows = conn.execute(q, (pid, start, season)).fetchall()
    trend: List[Dict[str, Any]] = []
    for r in rows:
        H = r["H"] or 0; AB = r["AB"] or 0; BB = r["BB"] or 0
        HBP = r["HBP"] or 0; SF = r["SF"] or 0
        _2B = r["_2B"] or 0; _3B = r["_3B"] or 0; HR = r["HR"] or 0
        _1B = H - _2B - _3B - HR
        TB = _1B + 2*_2B + 3*_3B + 4*HR
        OBP = _safe_div(H + BB + HBP, AB + BB + HBP + SF)
        SLG = _safe_div(TB, AB)
        OPS = OBP + SLG
        trend.append({"season": r["season"], "ops": _round(OPS, 3)})
    return {"player": name, "playerID": pid, "trend": trend}

# === Co-GM attach (append-only, do not move) ===
try:
    from player_intel_core import attach_player_intel, _unhandled_exc_handler
    try:
        app.add_exception_handler(Exception, _unhandled_exc_handler)
    except Exception:
        pass
    attach_player_intel(app)
    _COGM_ATTACHED = True
except Exception as e:
    _COGM_ATTACHED = False
    import traceback as _tb
    _COGM_ATTACH_ERR = {
        "error": f"{type(e).__name__}: {e}",
        "stack_tail": "".join(_tb.format_exception(type(e), e, e.__traceback__))[-1200:]
    }

# Diagnostics
try:
    @app.get("/_routes")
    def _routes():
        return [getattr(r, "path", str(r)) for r in app.router.routes]
except Exception:
    pass

try:
    @app.get("/_startup_errors")
    def _startup_errors():
        return {"attached": _COGM_ATTACHED, "error": (None if _COGM_ATTACHED else _COGM_ATTACH_ERR)}
except Exception:
    pass
