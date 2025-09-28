# player_intel_core.py
# Co-GM Assistant — Player Intelligence Core (Day 1)
# Endpoints:
#   POST /player/get_player_stats
#   POST /player/get_batted_ball_profile
#   GET  /player/_selfcheck
#
# 기능:
#   - 타자: OBP/SLG/OPS, OPS+ (리그 기준치 반영, park 미반영 단순형)
#   - 투수: ERA, ERA+ (IPouts/innings 토글 지원), 기본 안전장치
#   - 타구질 프로파일: EV/LA/Hard%, GB/FB, 샘플 분포
#   - 캐시: 인메모리 기본, REDIS_URL(rediss://) 설정 시 Upstash Redis 사용
#   - 에러 포맷: 요청 헤더 핵심 + 스택 트레이스 요약 반환
#
# 인수인계서 반영:
#   - "OPS+/ERA+ 0.0 문제" → 재계산 로직 + 나눗셈 안전장치
#   - "ERA=ER/IPouts*3" 표기 호환(기본 ipouts 모드, env 토글)
#   - "리그 기준치 복구" → 시즌별 기본값 + 요청시 오버라이드 허용
#   - "캐시 계층" → 메모리/Upstash 토글
#   - "uvicorn 로그 기반 진단" → 예외 핸들러에서 헤더+스택 요약

import os
import time
import json
import traceback
from typing import Optional, List, Dict, Any, Tuple

from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

ERA_FORMULA = os.getenv("ERA_FORMULA", "ipouts").lower()  # "ipouts" | "innings"
REDIS_URL = os.getenv("REDIS_URL", "").strip()            # "rediss://..." 권장
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "900"))  # 15분

DEFAULT_LEAGUE_BASELINES: Dict[int, Dict[str, float]] = {
    2023: {"lg_OBP": 0.320, "lg_SLG": 0.410, "lg_OPS": 0.730, "lg_ERA": 4.30},
    2024: {"lg_OBP": 0.317, "lg_SLG": 0.400, "lg_OPS": 0.717, "lg_ERA": 4.25},
    2025: {"lg_OBP": 0.318, "lg_SLG": 0.402, "lg_OPS": 0.720, "lg_ERA": 4.22},
}

# -------------------
# 캐시 계층
# -------------------
class _MemoryCache:
    def __init__(self):
        self._store: Dict[str, Tuple[float, Any]] = {}
    def get(self, key: str):
        rec = self._store.get(key)
        if not rec:
            return None
        exp, val = rec
        if time.time() > exp:
            self._store.pop(key, None)
            return None
        return val
    def set(self, key: str, val: Any, ttl: int):
        self._store[key] = (time.time() + ttl, val)

_mem_cache = _MemoryCache()

class _RedisCache:
    def __init__(self, url: str):
        import redis  # noqa: F401
        self._cli = redis.from_url(url)
    def get(self, key: str):
        val = self._cli.get(key)
        if val is None:
            return None
        try:
            return json.loads(val)
        except Exception:
            return None
    def set(self, key: str, val: Any, ttl: int):
        self._cli.setex(key, ttl, json.dumps(val))

if REDIS_URL:
    try:
        cache = _RedisCache(REDIS_URL)
    except Exception:
        cache = _MemoryCache()
else:
    cache = _MemoryCache()

def _cache_key(name: str, payload: Dict[str, Any]) -> str:
    return f"cgma:{name}:{json.dumps(payload, sort_keys=True, ensure_ascii=False)}"

# -------------------
# 유틸(계산)
# -------------------
def safe_div(n: float, d: float, default: float = 0.0) -> float:
    return n / d if d not in (0, 0.0, None) else default

def calc_obp(H: int, BB: int, HBP: int, AB: int, SF: int) -> float:
    return round(safe_div(H + BB + HBP, AB + BB + HBP + SF), 3)

def calc_slg(_1B: int, _2B: int, _3B: int, HR: int, AB: int) -> float:
    TB = _1B + 2*_2B + 3*_3B + 4*HR
    return round(safe_div(TB, AB), 3)

def calc_ops(obp: float, slg: float) -> float:
    return round(obp + slg, 3)

def calc_ops_plus(obp: float, slg: float, lg_obp: float, lg_slg: float) -> float:
    if lg_obp <= 0 or lg_slg <= 0:
        return 0.0
    val = 100.0 * ((safe_div(obp, lg_obp) + safe_div(slg, lg_slg)) - 1.0)
    return round(val, 1)

def calc_era(er: float, ip_outs: Optional[int], innings: Optional[float]) -> float:
    if ERA_FORMULA == "ipouts":
        if ip_outs and ip_outs > 0:
            return round(er * 27.0 / ip_outs, 3)
        if innings and innings > 0:
            return round(er * 9.0 / innings, 3)
        return 0.0
    else:
        if innings and innings > 0:
            return round(er * 9.0 / innings, 3)
        if ip_outs and ip_outs > 0:
            return round(er * 27.0 / ip_outs, 3)
        return 0.0

def calc_era_plus(era: float, lg_era: float) -> float:
    if era <= 0:
        return 999.0
    if lg_era <= 0:
        return 0.0
    return round(100.0 * (lg_era / era), 1)

def calc_batted_ball_profile(bbe: List[Dict[str, float]]) -> Dict[str, Any]:
    if not bbe:
        return {"ev_avg": 0.0, "la_avg": 0.0, "hard_pct": 0.0, "gb_pct": 0.0, "fb_pct": 0.0, "ld_pct": 0.0, "samples": 0}
    n = len(bbe)
    ev_avg = sum(x.get("ev", 0.0) for x in bbe) / n
    la_avg = sum(x.get("la", 0.0) for x in bbe) / n
    hard_cnt = sum(1 for x in bbe if x.get("ev", 0.0) >= 95.0)
    tcnt = {"gb": 0, "fb": 0, "ld": 0}
    for x in bbe:
        t = x.get("type", "")
        if t in tcnt:
            tcnt[t] += 1
    return {
        "ev_avg": round(ev_avg, 1),
        "la_avg": round(la_avg, 1),
        "hard_pct": round(100.0 * hard_cnt / n, 1),
        "gb_pct": round(100.0 * tcnt["gb"] / n, 1),
        "fb_pct": round(100.0 * tcnt["fb"] / n, 1),
        "ld_pct": round(100.0 * tcnt["ld"] / n, 1),
        "samples": n
    }

# -------------------
# I/O 모델
# -------------------
class LeagueBaselines(BaseModel):
    lg_OBP: float = Field(..., ge=0.0)
    lg_SLG: float = Field(..., ge=0.0)
    lg_OPS: float = Field(..., ge=0.0)
    lg_ERA: float = Field(..., ge=0.0)

class PlayerQuery(BaseModel):
    player_id: str
    season: int
    league_baselines: Optional[LeagueBaselines] = None

class PlayerStatsResponse(BaseModel):
    player_id: str
    season: int
    batting: Dict[str, float]
    pitching: Dict[str, float]
    advanced: Dict[str, float]
    batted_ball_profile: Optional[Dict[str, Any]] = None

class BattedBallQuery(BaseModel):
    player_id: str
    season: int
    last_n: Optional[int] = Field(default=100, ge=1, le=2000)

class BattedBallResponse(BaseModel):
    player_id: str
    season: int
    profile: Dict[str, Any]

# -------------------
# 데이터 소스 훅(스텁)
# -------------------
def fetch_batting_row(player_id: str, season: int) -> Dict[str, int]:
    return {"AB": 510, "H": 150, "_2B": 32, "_3B": 3, "HR": 24, "BB": 55, "HBP": 5, "SF": 6}

def fetch_pitching_row(player_id: str, season: int) -> Dict[str, float]:
    return {"ER": 68.0, "IPouts": 540, "innings": 180.0}

def fetch_bbe(player_id: str, season: int, last_n: int) -> List[Dict[str, float]]:
    base = [
        {"ev": 102.1, "la": 18.5, "type": "fb"},
        {"ev": 88.3,  "la": 4.2,  "type": "gb"},
        {"ev": 96.7,  "la": 12.0, "type": "ld"},
        {"ev": 92.4,  "la": 7.5,  "type": "gb"},
        {"ev": 98.9,  "la": 23.0, "type": "fb"},
        {"ev": 85.2,  "la": -2.0, "type": "gb"},
        {"ev": 100.3, "la": 15.2, "type": "ld"},
        {"ev": 94.1,  "la": 10.7, "type": "ld"},
    ]
    return base[: min(last_n, len(base))]

def league_baseline_for(season: int, override: Optional[LeagueBaselines]) -> LeagueBaselines:
    if override:
        return override
    base = DEFAULT_LEAGUE_BASELINES.get(season) or list(DEFAULT_LEAGUE_BASELINES.values())[-1]
    return LeagueBaselines(**base)

# -------------------
# 코어 계산 파이프라인
# -------------------
def build_player_stats(player_id: str, season: int, baselines: LeagueBaselines) -> PlayerStatsResponse:
    bat = fetch_batting_row(player_id, season)
    pit = fetch_pitching_row(player_id, season)

    _1B = max(0, bat["H"] - (bat["_2B"] + bat["_3B"] + bat["HR"]))
    obp = calc_obp(bat["H"], bat["BB"], bat["HBP"], bat["AB"], bat["SF"])
    slg = calc_slg(_1B, bat["_2B"], bat["_3B"], bat["HR"], bat["AB"])
    ops = calc_ops(obp, slg)
    ops_plus = calc_ops_plus(obp, slg, baselines.lg_OBP, baselines.lg_SLG)

    era = calc_era(pit["ER"], int(pit.get("IPouts") or 0), float(pit.get("innings") or 0.0))
    era_plus = calc_era_plus(era, baselines.lg_ERA)

    batting_out = {
        "AB": float(bat["AB"]), "H": float(bat["H"]),
        "1B": float(_1B), "2B": float(bat["_2B"]), "3B": float(bat["_3B"]), "HR": float(bat["HR"]),
        "BB": float(bat["BB"]), "HBP": float(bat["HBP"]), "SF": float(bat["SF"]),
        "OBP": obp, "SLG": slg, "OPS": ops
    }
    pitching_out = {
        "ER": float(pit["ER"]),
        "IPouts": float(pit.get("IPouts") or 0.0),
        "innings": float(pit.get("innings") or 0.0),
        "ERA": float(era)
    }
    advanced_out = {"OPS_plus": float(ops_plus), "ERA_plus": float(era_plus)}

    return PlayerStatsResponse(
        player_id=player_id,
        season=season,
        batting=batting_out,
        pitching=pitching_out,
        advanced=advanced_out,
    )

# -------------------
# 예외 핸들러 (app 레벨에서 add_exception_handler로 등록)
# -------------------
async def _unhandled_exc_handler(request: Request, exc: Exception):
    headers = {
        "host": request.headers.get("host"),
        "ua": request.headers.get("user-agent"),
        "trace": request.headers.get("x-request-id") or request.headers.get("x-trace-id")
    }
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))[-1200:]
    return JSONResponse(status_code=500, content={"error": "internal_error", "hint": headers, "stack_tail": tb})

# -------------------
# FastAPI 라우터
# -------------------
router = APIRouter()

@router.post("/player/get_player_stats", response_model=PlayerStatsResponse)
async def get_player_stats(q: PlayerQuery):
    payload = q.dict()
    key = _cache_key("get_player_stats", payload)
    cached = cache.get(key)
    if cached:
        return cached
    baselines = league_baseline_for(q.season, q.league_baselines)
    res = build_player_stats(q.player_id, q.season, baselines)
    # 경량 bbe 포함
    bbe = fetch_bbe(q.player_id, q.season, last_n=8)
    res.batted_ball_profile = calc_batted_ball_profile(bbe)
    out = res.dict()
    cache.set(key, out, CACHE_TTL_SECONDS)
    return out

@router.post("/player/get_batted_ball_profile", response_model=BattedBallResponse)
async def get_batted_ball_profile(q: BattedBallQuery):
    payload = q.dict()
    key = _cache_key("get_bbe", payload)
    cached = cache.get(key)
    if cached:
        return cached
    bbe = fetch_bbe(q.player_id, q.season, q.last_n or 100)
    prof = calc_batted_ball_profile(bbe)
    out = {"player_id": q.player_id, "season": q.season, "profile": prof}
    cache.set(key, out, CACHE_TTL_SECONDS)
    return out

@router.get("/player/_selfcheck")
async def _selfcheck():
    q = PlayerQuery(player_id="demo123", season=2025)
    b = league_baseline_for(q.season, q.league_baselines)
    res = build_player_stats(q.player_id, q.season, b)
    assert res.batting["OPS"] > 0.0, "OPS가 0.0입니다"
    assert res.pitching["ERA"] >= 0.0, "ERA 계산 실패"
    assert res.advanced["OPS_plus"] != 0.0, "OPS+가 0.0입니다"
    assert res.advanced["ERA_plus"] != 0.0, "ERA+가 0.0입니다"
    bb = calc_batted_ball_profile(fetch_bbe("demo123", 2025, 8))
    assert bb["samples"] > 0 and bb["ev_avg"] > 0.0, "bbe 프로파일 계산 실패"
    return {
        "ok": True,
        "ops": res.batting["OPS"],
        "ops_plus": res.advanced["OPS_plus"],
        "era": res.pitching["ERA"],
        "era_plus": res.advanced["ERA_plus"],
        "bbe_samples": bb["samples"]
    }

def attach_player_intel(app: FastAPI):
    app.include_router(router)
