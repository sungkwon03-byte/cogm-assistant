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
from typing import Tuple, Optional, List, Dict, Any, Tuple

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

# ========= Day2: compare(2–3), 3-year trend, count tendencies =========
from pydantic import BaseModel
from statistics import mean

# ----- 공용 유틸 -----
def _norm_0_100(val: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.0
    x = (val - lo) / (hi - lo) * 100.0
    if x < 0: x = 0.0
    if x > 100: x = 100.0
    return round(x, 1)

# ----- #2 2–3인 비교 (스파이더/버터플라이) -----
class ComparePlayersQuery(BaseModel):
    player_ids: List[str]  # 2~3명
    season: int
    league_baselines: Optional[LeagueBaselines] = None

class ComparePlayersResponse(BaseModel):
    season: int
    axes: List[str]                 # 스파이더 축 이름
    players: List[Dict[str, Any]]   # [{player_id, raw:{...}, scaled:{...}}]

def _build_spider_axes() -> List[str]:
    # 간단형 축: OBP, SLG, OPS, OPS+, ERA, ERA+
    return ["OBP", "SLG", "OPS", "OPS_plus", "ERA", "ERA_plus"]

@router.post("/player/compare_players2", response_model=ComparePlayersResponse)
async def compare_players2(q: ComparePlayersQuery):
    assert 2 <= len(q.player_ids) <= 3, "player_ids는 2~3명이어야 합니다."
    baselines = league_baseline_for(q.season, q.league_baselines)
    axes = _build_spider_axes()

    rows = []
    for pid in q.player_ids:
        st = build_player_stats(pid, q.season, baselines)
        raw = {
            "OBP": st.batting["OBP"],
            "SLG": st.batting["SLG"],
            "OPS": st.batting["OPS"],
            "OPS_plus": st.advanced["OPS_plus"],
            "ERA": st.pitching["ERA"],
            "ERA_plus": st.advanced["ERA_plus"],
        }
        rows.append({"player_id": pid, "raw": raw})

    # 스케일: 타자축은 높을수록 좋음, ERA는 낮을수록 좋음
    # 범위는 리그 기준 + 샘플 분포를 섞어서 완만하게
    obp_lo, obp_hi = baselines.lg_OBP*0.8, baselines.lg_OBP*1.25
    slg_lo, slg_hi = baselines.lg_SLG*0.8, baselines.lg_SLG*1.25
    ops_lo, ops_hi = baselines.lg_OPS*0.8, baselines.lg_OPS*1.25
    opsp_lo, opsp_hi = 50.0, 175.0
    era_lo, era_hi = max(1.5, baselines.lg_ERA*0.5), baselines.lg_ERA*1.8
    erap_lo, erap_hi = 50.0, 200.0

    for r in rows:
        raw = r["raw"]
        scaled = {
            "OBP": _norm_0_100(raw["OBP"], obp_lo, obp_hi),
            "SLG": _norm_0_100(raw["SLG"], slg_lo, slg_hi),
            "OPS": _norm_0_100(raw["OPS"], ops_lo, ops_hi),
            "OPS_plus": _norm_0_100(raw["OPS_plus"], opsp_lo, opsp_hi),
            # ERA는 낮을수록 좋음 → 역스케일
            "ERA": 100.0 - _norm_0_100(raw["ERA"], era_lo, era_hi),
            "ERA_plus": _norm_0_100(raw["ERA_plus"], erap_lo, erap_hi),
        }
        r["scaled"] = {k: round(v,1) for k,v in scaled.items()}

    return ComparePlayersResponse(season=q.season, axes=axes, players=rows)

# ----- #3 3년 트렌드 (wRC+, BABIP, EV, BB/K) -----
class Trend3YQuery(BaseModel):
    player_id: str
    season_end: int   # 이 해를 끝으로 직전 2시즌 포함 (예: 2025 → 2023,2024,2025)
    league_baselines: Optional[LeagueBaselines] = None

class Trend3YResponse(BaseModel):
    player_id: str
    seasons: List[int]
    series: Dict[str, List[float]]  # {"wRC_plus":[..], "BABIP":[..], "EV":[..], "BBK":[..]}

# 간단 추정치(스텁): 실제 ETL 구간 연결 전까지 사용
def _fake_wrc_plus(st: PlayerStatsResponse, base: LeagueBaselines) -> float:
    # 단순형: OPS+에 소량 보정
    return round(st.advanced["OPS_plus"] * 0.95 + 5, 1)

def _fake_babip(st: PlayerStatsResponse) -> float:
    # 극단치 방지용 단순 추정(안타-홈런)/타수가 최소식과 다르지만 임시
    h = st.batting["H"]; hr = st.batting["HR"]; ab = st.batting["AB"]
    sf = st.batting["SF"]; bb = st.batting["BB"]; hbp = st.batting["HBP"]
    # 최소 안전장치
    balls_in_play = max(1.0, ab - hr - (bb + hbp + sf)*0.0)
    return round((max(0.0, h-hr)) / balls_in_play, 3)

def _fake_ev(st: PlayerStatsResponse) -> float:
    # Day1 batted_ball_profile의 평균 EV 사용(없을 경우 90.0)
    prof = st.batted_ball_profile or {}
    return float(prof.get("ev_avg", 90.0))

def _fake_bbk(st: PlayerStatsResponse) -> float:
    bb = st.batting["BB"]; k = max(1.0, 120.0)  # K 스텁(임시): 추후 실제 값 연결
    return round(bb / k, 3)

@router.post("/player/three_year_trend", response_model=Trend3YResponse)
async def three_year_trend(q: Trend3YQuery):
    years = [q.season_end-2, q.season_end-1, q.season_end]
    base = league_baseline_for(q.season_end, q.league_baselines)
    wr, bab, evv, bbk = [], [], [], []
    for y in years:
        st = build_player_stats(q.player_id, y, base)
        # (스텁) 시즌별 차이를 내기 위해 약한 변형 적용
        # 실제 연결 시 fetch_ 계열에서 연도별 데이터 반환으로 교체
        st.batted_ball_profile = st.batted_ball_profile or {"ev_avg": 92.0 + (y-years[0])*1.2}
        wr.append(_fake_wrc_plus(st, base))
        bab.append(_fake_babip(st))
        evv.append(_fake_ev(st))
        bbk.append(_fake_bbk(st))
    return Trend3YResponse(player_id=q.player_id, seasons=years,
                           series={"wRC_plus": wr, "BABIP": bab, "EV": evv, "BBK": bbk})

# ----- #4 카운트/투수유형별 성향(기초 분포) -----
class CountTendencyQuery(BaseModel):
    player_id: str
    season: int
    sample: Optional[int] = 200

class CountTendencyResponse(BaseModel):
    player_id: str
    season: int
    counts: Dict[str, Dict[str, float]]  # {"0-0":{"swing%":..,"whiff%":..,"inplay%":..}, ...}
    pitch_types: Dict[str, Dict[str, float]]  # {"FF":{"swing%":..,"whiff%":..}, ...}

@router.post("/player/count_tendencies", response_model=CountTendencyResponse)
async def count_tendencies(q: CountTendencyQuery):
    # 스텁 분포(균형 감각만): 실제 연결 시 pitch-by-pitch DB로 대체
    counts = {
        "0-0": {"swing%": 30.0, "whiff%": 8.0, "inplay%": 18.0},
        "1-0": {"swing%": 28.0, "whiff%": 7.0, "inplay%": 16.0},
        "0-1": {"swing%": 45.0, "whiff%": 14.0, "inplay%": 20.0},
        "1-1": {"swing%": 40.0, "whiff%": 12.0, "inplay%": 19.0},
        "2-1": {"swing%": 48.0, "whiff%": 13.0, "inplay%": 22.0},
        "1-2": {"swing%": 56.0, "whiff%": 24.0, "inplay%": 17.0},
        "3-2": {"swing%": 62.0, "whiff%": 26.0, "inplay%": 21.0},
    }
    pitch_types = {
        "FF": {"swing%": 44.0, "whiff%": 10.0},
        "SL": {"swing%": 39.0, "whiff%": 16.0},
        "CH": {"swing%": 36.0, "whiff%": 15.0},
        "CB": {"swing%": 33.0, "whiff%": 14.0},
        "SI": {"swing%": 41.0, "whiff%": 11.0},
    }
    return CountTendencyResponse(player_id=q.player_id, season=q.season,
                                 counts=counts, pitch_types=pitch_types)


# ========= Day3: #5 약점맵(구종×코스) / #8 핫·콜드 스틱 / #9 부상 리스크 =========
from pydantic import BaseModel

# ----- 공용 -----
def _clamp(v: float, lo: float, hi: float) -> float:
    return lo if v < lo else hi if v > hi else v

def _zone_grid_keys():
    # 3x3 구역
    return [
        "Up-In","Up-Mid","Up-Out",
        "Mid-In","Mid","Mid-Out",
        "Low-In","Low-Mid","Low-Out"
    ]

# ===== #5 약점 탐색(구종×코스) =====
class WeaknessMapQuery(BaseModel):
    player_id: str
    season: int
    pitch_types: Optional[List[str]] = None  # None이면 기본 ["FF","SL","CH","CB"]

class WeaknessMapResponse(BaseModel):
    player_id: str
    season: int
    metric: str                       # "xwOBA_like" (0.200~0.450)
    grid: Dict[str, Dict[str, float]] # {pitch_type: {zone: value}}

def _seed_from(pid: str, season: int) -> int:
    return sum(ord(c) for c in f"{pid}{season}")

def _xwoba_like(seed: int, bias: float) -> float:
    # 0.200~0.450 사이 값. bias(0~1)를 조금 반영.
    base = 0.200 + ((seed % 251) / 250.0) * 0.25
    val = base * (0.85 + 0.3 * bias)
    return round(_clamp(val, 0.200, 0.450), 3)

@router.post("/player/weakness_map", response_model=WeaknessMapResponse)
async def weakness_map(q: WeaknessMapQuery):
    ptypes = q.pitch_types or ["FF","SL","CH","CB"]
    zones = _zone_grid_keys()
    # 플레이어 현재 성능을 약간 반영 (OPS 높으면 전체가 약간 낮아짐=약점 덜함)
    base = league_baseline_for(q.season, None)
    st = build_player_stats(q.player_id, q.season, base)
    ops_bias = _clamp(1.0 - (st.batting["OPS"] - base.lg_OPS) / 0.3, 0.0, 1.0)
    seed = _seed_from(q.player_id, q.season)
    grid: Dict[str, Dict[str, float]] = {}
    for i,pt in enumerate(ptypes):
        pt_seed = seed + (i+1)*17
        row: Dict[str, float] = {}
        for j,zone in enumerate(zones):
            z_bias = (j+1)/len(zones) * 0.6 + 0.4*ops_bias
            row[zone] = _xwoba_like(pt_seed + j*13, z_bias)
        grid[pt] = row
    return WeaknessMapResponse(player_id=q.player_id, season=q.season, metric="xwOBA_like", grid=grid)

# ===== #8 핫/콜드 스틱 검증(안정화) =====
class HotColdQuery(BaseModel):
    player_id: str
    season: int

class HotColdResponse(BaseModel):
    player_id: str
    season: int
    status: str            # "hot" | "neutral" | "cold"
    z_ops: float           # 리그대비 OPS z-score 근사치
    stability_score: float # 0~100 (표본 대충)
    notes: List[str]

@router.post("/player/hot_cold_stick", response_model=HotColdResponse)
async def hot_cold_stick(q: HotColdQuery):
    base = league_baseline_for(q.season, None)
    st = build_player_stats(q.player_id, q.season, base)
    # 분산 근사: 리그 OPS 표준편차 ~0.060 가정
    z_ops = _clamp((st.batting["OPS"] - base.lg_OPS) / 0.06, -3.0, 3.0)
    status = "hot" if z_ops >= 0.8 else "cold" if z_ops <= -0.8 else "neutral"
    # 안정화: 타석 수 대용으로 AB 사용 (AB 300 → 60점, 500→100점 근사)
    stability = _clamp((st.batting.get("AB", 0.0) / 500.0) * 100.0, 10.0, 100.0)
    notes = []
    prof = st.batted_ball_profile or {}
    if prof.get("hard_pct", 0.0) >= 45.0: notes.append("hard% high")
    if prof.get("ev_avg", 0.0) >= 92.0: notes.append("EV solid")
    return HotColdResponse(player_id=q.player_id, season=q.season, status=status,
                           z_ops=round(float(z_ops),2), stability_score=round(float(stability),1), notes=notes)

# ===== #9 부상 리스크 시그널(기초) =====
class InjuryRiskQuery(BaseModel):
    player_id: str
    season: int

class InjuryRiskResponse(BaseModel):
    player_id: str
    season: int
    risk_score: float      # 0~100
    factors: Dict[str, float]
    flags: List[str]

@router.post("/player/injury_risk_signal", response_model=InjuryRiskResponse)
async def injury_risk_signal(q: InjuryRiskQuery):
    base = league_baseline_for(q.season, None)
    st = build_player_stats(q.player_id, q.season, base)
    innings = st.pitching.get("innings", 0.0) or (st.pitching.get("IPouts", 0.0)/3.0)
    era = st.pitching.get("ERA", 0.0)
    # 간단 근사: workload + (저ERA 하드사용) + hard%
    workload = _clamp((innings/200.0)*60.0, 0.0, 70.0)
    hard = float((st.batted_ball_profile or {}).get("hard_pct", 0.0))
    hard_load = _clamp(hard/100.0*20.0, 0.0, 20.0)
    low_era_bonus = 10.0 if era <= 3.3 and innings >= 150 else 0.0  # 강부하 로테이션 가정
    risk = _clamp(workload + hard_load + low_era_bonus, 0.0, 100.0)
    flags = []
    if innings >= 170: flags.append("workload_high")
    if hard >= 50: flags.append("hard_contact_high")
    if era <= 3.3 and innings >= 150: flags.append("low_ERA_heavy_use")
    return InjuryRiskResponse(player_id=q.player_id, season=q.season, risk_score=round(float(risk),1),
                              factors={"workload": round(float(workload),1),
                                       "hard_load": round(float(hard_load),1),
                                       "low_era_bonus": round(float(low_era_bonus),1)},
                              flags=flags)


# ========= Day4: Roster & Payroll v1 — #13 멀티-이어 페이롤 / #14 ARB 예상 =========
from pydantic import BaseModel
from math import pow

# ----- 공용 -----
def _npv(cashflows: List[float], discount_rate: float = 0.08) -> float:
    # 연 단위 NPV (연 8% 기본)
    return round(sum(cf / pow(1.0 + discount_rate, i) for i, cf in enumerate(cashflows, start=1)), 2)

def _salary_growth(base: float, rate: float, years: int) -> List[float]:
    # 단순 연복리 성장
    out = []
    sal = base
    for _ in range(years):
        out.append(round(sal, 2))
        sal *= (1.0 + rate)
    return out

# ===== #13 멀티-이어 페이롤 시뮬 =====
class MultiYearPayrollItem(BaseModel):
    player_id: str
    base_year_salary: float    # 시작 연도 연봉(USD)
    growth_rate: float = 0.05  # 연 성장률(기본 5%)
    years: int = 3             # 시뮬 연수
    arb_eligible: bool = False # ARB 대상이면, 별도 ARB 시나리오 적용 가능

class MultiYearPayrollQuery(BaseModel):
    season_start: int
    items: List[MultiYearPayrollItem]
    discount_rate: float = 0.08

class MultiYearPayrollResponse(BaseModel):
    season_years: List[int]
    table: List[Dict[str, Any]]   # [{player_id, yearly:[...], total, npv}]
    totals_by_year: List[float]
    grand_total: float
    grand_npv: float

@router.post("/roster/multi_year_payroll", response_model=MultiYearPayrollResponse)
async def multi_year_payroll(q: MultiYearPayrollQuery):
    max_years = max((it.years for it in q.items), default=0)
    years = [q.season_start + i for i in range(max_years)]
    table = []
    totals = [0.0 for _ in years]

    for it in q.items:
        yearly = _salary_growth(it.base_year_salary, it.growth_rate, it.years)
        # 자리채움(표 길이 정렬)
        yearly += [0.0] * (max_years - len(yearly))
        for i, v in enumerate(yearly):
            totals[i] += v
        total = round(sum(yearly), 2)
        npv = _npv([v for v in yearly if v > 0], q.discount_rate)
        table.append({"player_id": it.player_id, "yearly": yearly, "total": total, "npv": npv})

    grand_total = round(sum(totals), 2)
    grand_npv = _npv([v for v in totals if v > 0], q.discount_rate)
    return MultiYearPayrollResponse(season_years=years, table=table, totals_by_year=[round(t,2) for t in totals],
                                    grand_total=grand_total, grand_npv=grand_npv)

# ===== #14 ARB 예상(기초) =====
# 매우 단순한 기초 모델: 서비스 타임/직전 성과(OPS+/ERA+)에 비례한 가중치를 사용
class ArbInput(BaseModel):
    player_id: str
    role: str                 # "batter" | "pitcher"
    last_season: int
    service_years: float      # 예: 3.1 (3년 1개월) → 3.08년 등으로 받기
    baseline_salary: float    # 직전 시즌 보장/합의 급여(USD)

class ArbQuery(BaseModel):
    entries: List[ArbInput]

class ArbEstRow(BaseModel):
    player_id: str
    role: str
    last_season: int
    est_raise_pct: float
    est_salary: float
    drivers: Dict[str, float]

class ArbResponse(BaseModel):
    table: List[ArbEstRow]

@router.post("/roster/arb_estimate", response_model=ArbResponse)
async def arb_estimate(q: ArbQuery):
    out = []
    for e in q.entries:
        base = league_baseline_for(e.last_season, None)
        st = build_player_stats(e.player_id, e.last_season, base)

        if e.role == "batter":
            perf = st.advanced.get("OPS_plus", 100.0)
            driver_perf = (perf - 100.0) / 100.0   # 100 기준 ±
        else:
            perf = st.advanced.get("ERA_plus", 100.0)
            driver_perf = (perf - 100.0) / 120.0  # 피처는 민감도 완화

        # 서비스타임 가중치: 3~6년 구간에서 점증(최대치 1.0)
        svc = _clamp((e.service_years - 3.0) / 3.0, 0.0, 1.0)

        # 기본 인상률: 10% + 성과 기여(최대 ±20%) + 서비스 가중(최대 +15%)
        raise_pct = 0.10 + _clamp(driver_perf, -0.20, 0.20) + 0.15 * svc
        raise_pct = _clamp(raise_pct, -0.10, 0.50)  # 하한 -10%, 상한 +50%

        est_salary = round(e.baseline_salary * (1.0 + raise_pct), 2)
        out.append(ArbEstRow(
            player_id=e.player_id, role=e.role, last_season=e.last_season,
            est_raise_pct=round(raise_pct*100.0, 1),
            est_salary=est_salary,
            drivers={"perf_metric": float(perf), "svc_years": float(e.service_years)}
        ))
    return ArbResponse(table=out)


# ========= Day5: #15 계약 ROI/서플러스, #16 포지션 대체 자원 추천 =========
from pydantic import BaseModel

def _npv_series(values: List[float], r: float) -> float:
    return round(sum(v / ((1.0 + r) ** i) for i, v in enumerate(values, start=1)), 2)

# ----- #15 계약 ROI/서플러스 ($/WAR·NPV) -----
class ContractYear(BaseModel):
    year: int
    salary: float      # 연봉(USD)
    proj_war: float    # 예상 WAR

class ContractROIQuery(BaseModel):
    contract: List[ContractYear]
    dollar_per_war: float = 9000000.0   # $/WAR 기본 9M
    discount_rate: float = 0.08

class ContractROIResponse(BaseModel):
    table: List[Dict[str, float]]        # [{year, salary, proj_war, value, surplus}]
    totals: Dict[str, float]             # {salary, value, surplus, npv_salary, npv_value, npv_surplus}
    roi: float                           # value / salary (총합 기준)

@router.post("/roster/contract_roi", response_model=ContractROIResponse)
async def contract_roi(q: ContractROIQuery):
    rows = []
    v_series, s_series = [], []
    for cy in q.contract:
        value = cy.proj_war * q.dollar_per_war
        surplus = value - cy.salary
        rows.append({"year": cy.year, "salary": round(cy.salary,2), "proj_war": round(cy.proj_war,2),
                     "value": round(value,2), "surplus": round(surplus,2)})
        v_series.append(value); s_series.append(cy.salary)
    tot_salary = round(sum(s_series), 2)
    tot_value = round(sum(v_series), 2)
    tot_surplus = round(tot_value - tot_salary, 2)
    npv_salary = _npv_series(s_series, q.discount_rate)
    npv_value  = _npv_series(v_series, q.discount_rate)
    npv_surplus = round(npv_value - npv_salary, 2)
    roi = round((tot_value / tot_salary) if tot_salary > 0 else 0.0, 3)
    return ContractROIResponse(table=rows,
                               totals={"salary": tot_salary, "value": tot_value, "surplus": tot_surplus,
                                       "npv_salary": npv_salary, "npv_value": npv_value, "npv_surplus": npv_surplus},
                               roi=roi)

# ----- #16 포지션 대체 자원 추천 (간단형) -----
class ReplacementCand(BaseModel):
    player_id: str
    pos: str
    proj_war: float
    expected_cost: float

class ReplacementQuery(BaseModel):
    need_pos: str
    candidates: List[ReplacementCand]
    min_war: float = 0.5
    top_n: int = 5

from pydantic import Field
from pydantic import ConfigDict
class ReplacementRankRow(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    player_id: str
    proj_war: float
    expected_cost: float
    war_per_dollar: float = Field(alias="war_per_$")
    surplus: float

class ReplacementResponse(BaseModel):
    pos: str
    ranked: List[ReplacementRankRow]

@router.post("/roster/replacement_suggestions", response_model=ReplacementResponse)
async def replacement_suggestions(q: ReplacementQuery):
    out: List[Dict[str, float]] = []
    for c in q.candidates:
        if c.pos != q.need_pos:
            continue
        if c.proj_war < q.min_war:
            continue
        war_per_dollar = (c.proj_war / c.expected_cost) if c.expected_cost > 0 else 0.0
        surplus = c.proj_war * 9_000_000.0 - c.expected_cost  # 9M/WAR 기본
        out.append({
            "player_id": c.player_id,
            "proj_war": round(c.proj_war, 2),
            "expected_cost": round(c.expected_cost, 2),
            "war_per_$": round(war_per_dollar, 8),
            "surplus": round(surplus, 2),
        })
    out.sort(key=lambda x: (x["surplus"], x["war_per_$"]), reverse=True)
    ranked = [ReplacementRankRow(**row) for row in out[: q.top_n]]
    return ReplacementResponse(pos=q.need_pos, ranked=ranked)

# ========= Day6: #22 트레이드 밸류 / #23 모의 트레이드 =========
from pydantic import BaseModel

def _player_trade_value(years: List[Dict[str, float]], dollar_per_war: float, r: float) -> float:
    # 가치 = Σ(WAR_i * $/WAR / (1+r)^i) - Σ(Salary_i/(1+r)^i)
    v = 0.0
    for i, yr in enumerate(years, start=1):
        v += (float(yr.get("war", 0.0)) * dollar_per_war) / ((1.0 + r) ** i)
        v -= (float(yr.get("salary", 0.0))) / ((1.0 + r) ** i)
    return round(v, 2)

class TradeValueInput(BaseModel):
    player_id: str
    years: List[Dict[str, float]]   # [{"year":2025,"war":2.5,"salary":6000000}, ...]
    risk_pct: float = 0.1           # 0.0~0.9

class TradeValueQuery(BaseModel):
    entries: List[TradeValueInput]
    dollar_per_war: float = 9_000_000.0
    discount_rate: float = 0.08

class TradeValueRow(BaseModel):
    player_id: str
    raw_value: float
    adj_value: float
    drivers: Dict[str, float]

class TradeValueResponse(BaseModel):
    table: List[TradeValueRow]

@router.post("/transactions/trade_value", response_model=TradeValueResponse)
async def trade_value(q: TradeValueQuery):
    out: List[TradeValueRow] = []
    for e in q.entries:
        raw = _player_trade_value(e.years, q.dollar_per_war, q.discount_rate)
        adj = round(raw * (1.0 - _clamp(e.risk_pct, 0.0, 0.9)), 2)
        out.append(TradeValueRow(
            player_id=e.player_id, raw_value=raw, adj_value=adj,
            drivers={"risk_pct": float(e.risk_pct), "dpw": float(q.dollar_per_war)}
        ))
    out.sort(key=lambda x: x.adj_value, reverse=True)
    return TradeValueResponse(table=out)

# ----- #23 모의 트레이드 -----
class TradeSide(BaseModel):
    team: str
    players: List[str]

class MockTradeQuery(BaseModel):
    sideA: TradeSide
    sideB: TradeSide
    values: Dict[str, float]    # {player_id: adj_value}
    tolerance: float = 3_000_000.0

class MockTradeResponse(BaseModel):
    ok: bool
    delta: float
    sideA_total: float
    sideB_total: float
    winner: str
    note: str

@router.post("/transactions/mock_trade", response_model=MockTradeResponse)
async def mock_trade(q: MockTradeQuery):
    a_total = round(sum(q.values.get(pid, 0.0) for pid in q.sideA.players), 2)
    b_total = round(sum(q.values.get(pid, 0.0) for pid in q.sideB.players), 2)
    delta = round(abs(a_total - b_total), 2)
    ok = delta <= q.tolerance
    winner = q.sideA.team if a_total > b_total else q.sideB.team if b_total > a_total else "even"
    note = "balanced" if ok else "needs sweetener"
    return MockTradeResponse(ok=ok, delta=delta, sideA_total=a_total, sideB_total=b_total, winner=winner, note=note)

# ========= Day7: 주간 통합 스모크 =========
@router.get("/_regression_smoke")
async def _regression_smoke():
    errs = []
    try:
        await _selfcheck()
    except Exception as e:
        errs.append(f"selfcheck:{type(e).__name__}")
    try:
        await compare_players2(ComparePlayersQuery(player_ids=["a","b"], season=2025))
    except Exception as e:
        errs.append(f"compare:{type(e).__name__}")
    try:
        await three_year_trend(Trend3YQuery(player_id="demo123", season_end=2025))
    except Exception as e:
        errs.append(f"trend:{type(e).__name__}")
    try:
        await count_tendencies(CountTendencyQuery(player_id="demo123", season=2025))
    except Exception as e:
        errs.append(f"count:{type(e).__name__}")
    try:
        await replacement_suggestions(ReplacementQuery(
            need_pos="1B",
            candidates=[ReplacementCand(player_id="A",pos="1B",proj_war=1.2,expected_cost=2_000_000)],
            top_n=1
        ))
    except Exception as e:
        errs.append(f"repl:{type(e).__name__}")
    ok = len(errs) == 0
    return {"ok": ok, "errors": errs}

# ========= Day8: #29 일정 분석 / #35 승률 예측 =========
from pydantic import BaseModel
from typing import Tuple, List, Dict, Optional
from datetime import datetime, timedelta, date
import math

# ----- 유틸 -----
def _parse_ymd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()

def _stable_rng01(seed: int) -> float:
    # 시드 기반 0~1 난수(안정적)
    x = (seed * 9301 + 49297) % 233280
    return x / 233280.0

# ===== #29 일정 분석 =====
class ScheduleAnalyzeResponse(BaseModel):
    team: str
    date_from: str
    date_to: str
    days: int
    games: int
    back_to_backs: int
    rest_days: int
    travel_km_stub: int
    opp_strength_stub: float
    fatigue_index: float  # 0~100

@router.get("/schedule/analyze", response_model=ScheduleAnalyzeResponse)
async def schedule_analyze(team: str, from_: str, to: str):
    d0 = _parse_ymd(from_)
    d1 = _parse_ymd(to)
    if d1 < d0:
        d0, d1 = d1, d0
    days = (d1 - d0).days + 1

    # 시드: 팀+기간
    seed = _seed_from(team, d0.year) + days * 37
    r1 = _stable_rng01(seed + 11)
    r2 = _stable_rng01(seed + 23)
    r3 = _stable_rng01(seed + 31)
    r4 = _stable_rng01(seed + 47)

    # 간단 스텁 로직: 기간 중 경기수, 백투백, 휴식일, 이동거리, 상대강도
    games = max(0, int(round(days * (0.6 + 0.25 * r1))))
    back_to_backs = max(0, int(round(games * (0.10 + 0.10 * r2))))
    rest_days = max(0, days - math.ceil(games * 1.05))
    travel_km_stub = int( (300 + 1700 * r3) * max(1, days/10) )  # 대략적 스케일
    opp_strength = round(0.45 + 0.15 * r4, 3)  # 0.45~0.60

    # 피로 지수: 경기밀도/백투백/이동거리/상대강도 종합
    density = games / max(1, days)
    fatigue = (
        45.0 * density +
        25.0 * (back_to_backs / max(1, games)) +
        20.0 * min(1.0, travel_km_stub / 5000.0) +
        10.0 * (opp_strength - 0.5 + 0.5)
    )
    fatigue = round(_clamp(fatigue, 0.0, 100.0), 1)

    return ScheduleAnalyzeResponse(
        team=team, date_from=d0.isoformat(), date_to=d1.isoformat(), days=days,
        games=games, back_to_backs=back_to_backs, rest_days=rest_days,
        travel_km_stub=travel_km_stub, opp_strength_stub=opp_strength,
        fatigue_index=fatigue
    )

# ===== #35 승률/WP 예측 =====
class WinProbQuery(BaseModel):
    home: str
    away: str
    elo_home: float
    elo_away: float
    park: float = 1.00            # 1.00=중립, 1.02=타자친화 등
    sp_adj: Optional[float] = 0.0 # 선발 매치업 보정(+면 홈 유리)
    home_field_pts: float = 20.0  # 엘로 점수로 15~25 권장
    pyth_rs_home: Optional[float] = None
    pyth_ra_home: Optional[float] = None
    pyth_exp: float = 1.83        # 피타고라스 지수

class WinProbResponse(BaseModel):
    home: str
    away: str
    wp_home: float
    wp_away: float
    components: Dict[str, float]

@router.post("/forecast/win_prob", response_model=WinProbResponse)
async def forecast_win_prob(q: WinProbQuery):
    # 엘로 차이 + 홈 보정 + 구장/선발 보정
    diff = (q.elo_home - q.elo_away) + q.home_field_pts + 100.0 * (q.park - 1.0) + (q.sp_adj or 0.0)
    # 로지스틱 변환 (엘로 전통식)
    p_home = 1.0 / (1.0 + math.pow(10.0, -diff / 400.0))
    # 피타고라스 보정(선택)
    pyth = None
    if q.pyth_rs_home is not None and q.pyth_ra_home is not None and q.pyth_rs_home >= 0 and q.pyth_ra_home >= 0:
        rs = q.pyth_rs_home; ra = q.pyth_ra_home
        denom = math.pow(rs, q.pyth_exp) + math.pow(ra, q.pyth_exp)
        if denom > 0:
            pyth = float(math.pow(rs, q.pyth_exp) / denom)
            # 간단히 평균(50% 반영)으로 섞기
            p_home = 0.5 * p_home + 0.5 * pyth

    p_home = float(round(_clamp(p_home, 0.0, 1.0), 4))
    p_away = float(round(1.0 - p_home, 4))
    return WinProbResponse(
        home=q.home, away=q.away, wp_home=p_home, wp_away=p_away,
        components={
            "elo_diff_effect": round(diff, 2),
            "park": q.park,
            "sp_adj": float(q.sp_adj or 0.0),
            "home_field_pts": q.home_field_pts,
            **({"pyth_home": round(pyth,4)} if pyth is not None else {})
        }
    )

# ========= Day9: #24 팀 컬러-핏 매칭 / #33 데일리 파크팩터 =========
from pydantic import BaseModel
from typing import Tuple, List, Dict, Optional
from datetime import datetime

# ===== #24 팀 컬러-핏 매칭 =====
# 간단 규칙:
# - needs와 후보의 tools 교집합 크기 가중(70%)
# - LHH/RHH 선호 보정(20%)
# - 포지션/유틸리티 보정(10%) — pos가 일치하거나 "UTL" 포함 시 가점
_NEED_SYNONYM = {
    "LHH": {"LHH","L","LH","Left"},
    "RHH": {"RHH","R","RH","Right"},
    "POW": {"POW","PWR","POWER"},
    "DEF": {"DEF","D","FIELD","GLOVE"},
    "SPD": {"SPD","RUN","SPEED"},
    "OBP": {"OBP","DISC","BB"},
}

def _norm_need(x: str) -> str:
    x = (x or "").upper()
    for k, al in _NEED_SYNONYM.items():
        if x in al:
            return k
    return x

class TeamFitCandidate(BaseModel):
    player_id: str
    bats: Optional[str] = None   # "L"|"R"|"S"
    tools: List[str] = []        # ["POW","DEF","SPD",...]
    pos: Optional[str] = None    # "1B","RF","UTL" 등

class TeamFitQuery(BaseModel):
    team: str
    needs: List[str]
    candidates: List[TeamFitCandidate]
    top_n: int = 5

class TeamFitRow(BaseModel):
    player_id: str
    score: float
    reasons: List[str]

class TeamFitResponse(BaseModel):
    team: str
    ranked: List[TeamFitRow]

@router.post("/scouting/team_fit", response_model=TeamFitResponse)
async def team_fit(q: TeamFitQuery):
    needs = [_norm_need(n) for n in q.needs]
    out: List[TeamFitRow] = []
    for c in q.candidates:
        ctools = { _norm_need(t) for t in (c.tools or []) }
        # 1) needs-툴 매칭
        inter = ctools.intersection(needs)
        base = 0.7 * (len(inter) / max(1, len(needs)))
        reasons = []
        if inter:
            reasons.append(f"needs_matched:{','.join(sorted(inter))}")
        # 2) 좌우타 보정
        bats = (c.bats or "").upper()
        lh_bonus = 0.2 if ("LHH" in needs and bats == "L") else 0.0
        rh_bonus = 0.2 if ("RHH" in needs and bats == "R") else 0.0
        if lh_bonus: reasons.append("bats:LHH_fit")
        if rh_bonus: reasons.append("bats:RHH_fit")
        # 3) 포지션/유틸 보정
        pos_bonus = 0.1 if (c.pos in needs or (c.pos == "UTL") or any(p in (c.pos or "") for p in ("UTL","OF","IF"))) else 0.0
        if pos_bonus: reasons.append("pos_flex")
        score = round(min(1.0, base + lh_bonus + rh_bonus + pos_bonus), 4)
        out.append(TeamFitRow(player_id=c.player_id, score=score, reasons=reasons or ["baseline"]))
    out.sort(key=lambda r: r.score, reverse=True)
    return TeamFitResponse(team=q.team, ranked=out[: q.top_n])

# ===== #33 구장 파크팩터(데일리) =====
class ParkFactorsResponse(BaseModel):
    park: str
    date: str
    run_factor: float    # R
    hr_factor: float     # HR
    xbh_factor: float    # 2B/3B 가중

def _seed_from_str(s: str) -> int:
    return sum(ord(ch) for ch in (s or "")) % 10_000_019

@router.get("/parks/daily_factors", response_model=ParkFactorsResponse)
async def parks_daily_factors(park: str, date: str):
    # 날짜 파싱 + 안정 난수로 일별 변동 (±3% 내외)
    try:
        d = datetime.strptime(date, "%Y-%m-%d").date()
    except Exception:
        # 잘못된 날짜는 오늘로 처리(스텁)
        d = datetime.utcnow().date()
    seed = (_seed_from_str(park) * 97 + d.toordinal() * 131) % 1_000_003
    # 안정 난수 0~1 세 개
    def rng(k: int) -> float:
        x = (seed + k*7919) % 233280
        return x / 233280.0
    base_run = 1.00 + (rng(1) - 0.5) * 0.06   # ±3%
    base_hr  = 1.00 + (rng(2) - 0.5) * 0.08   # ±4%
    base_xbh = 1.00 + (rng(3) - 0.5) * 0.04   # ±2%
    return ParkFactorsResponse(
        park=park,
        date=d.isoformat(),
        run_factor=round(float(base_run), 3),
        hr_factor=round(float(base_hr), 3),
        xbh_factor=round(float(base_xbh), 3),
    )

# ========= Day10: #31 인-게임 레버리지 어시스트 / #32 심판 EUZ 편향(스텁) =========
from pydantic import BaseModel
from typing import Tuple, List, Dict, Optional

# ===== #31 인-게임 레버리지 어시스트 =====
class LeverageQuery(BaseModel):
    inning: int          # 1~9(+)
    score_diff: int      # 홈 관점: 홈-원정
    outs: int            # 0/1/2
    base: str            # "0", "1B", "2B", "3B", "1B2B", "1B3B", "2B3B", "123"
    batter: Optional[str] = None  # "L"|"R"
    pitcher: Optional[str] = None # "L"|"R"

class LeverageSuggestion(BaseModel):
    move: str
    rationale: str
    impact_hint: float   # -1.0~+1.0 (추정)

class LeverageResponse(BaseModel):
    ctx: Dict[str, str]
    suggestions: List[LeverageSuggestion]

def _base_state_factor(b: str) -> float:
    # 주자 상황 가치 가중(스텁)
    mapping = {
        "0": 0.00, "1B": 0.08, "2B": 0.15, "3B": 0.18,
        "1B2B": 0.22, "1B3B": 0.26, "2B3B": 0.28, "123": 0.32
    }
    return mapping.get(b.upper(), 0.0)

@router.post("/game/leverage_assist", response_model=LeverageResponse)
async def leverage_assist(q: LeverageQuery):
    hi_leverage = (q.inning >= 7 and abs(q.score_diff) <= 2)
    handed_bonus = 0.05 if (q.batter and q.pitcher and q.batter != q.pitcher) else 0.0
    base_val = _base_state_factor(q.base)
    close_game = max(0.0, 0.2 - 0.05*abs(q.score_diff))
    leverage_idx = round(min(1.0, base_val + close_game + (0.1 if hi_leverage else 0.0)), 3)

    suggestions: List[LeverageSuggestion] = []
    # 1) 불펜 매치업
    if hi_leverage and (q.pitcher == "R" and q.batter == "L"):
        suggestions.append(LeverageSuggestion(
            move="Bring LHP (matchup)",
            rationale="Late-inning, platoon edge vs L batter",
            impact_hint=round(0.12 + handed_bonus + leverage_idx*0.2, 3)
        ))
    elif hi_leverage and (q.pitcher == "L" and q.batter == "R"):
        suggestions.append(LeverageSuggestion(
            move="Bring RHP (matchup)",
            rationale="Late-inning, platoon edge vs R batter",
            impact_hint=round(0.12 + handed_bonus + leverage_idx*0.2, 3)
        ))
    # 2) 대주자/도루
    if q.base in ("1B","1B2B") and q.outs in (0,1) and abs(q.score_diff) <= 1:
        suggestions.append(LeverageSuggestion(
            move="PR/Steal attempt",
            rationale="Runner on 1B, leverage for SB/PR can raise run expectancy",
            impact_hint=round(0.06 + leverage_idx*0.15, 3)
        ))
    # 3) 수비 시프트/번트 방지
    if q.base in ("2B","2B3B","1B3B","123") and q.outs == 0 and q.score_diff < 0:
        suggestions.append(LeverageSuggestion(
            move="No-bunt defense",
            rationale="Tie/behind with RISP and 0 out — reduce sac bunt value",
            impact_hint=round(0.05 + leverage_idx*0.1, 3)
        ))
    # 4) 대타 카드
    if q.outs == 2 and abs(q.score_diff) <= 2:
        suggestions.append(LeverageSuggestion(
            move="Pinch hitter (power)",
            rationale="Two outs, marginal leverage — maximize XBH/HR odds",
            impact_hint=round(0.04 + leverage_idx*0.12, 3)
        ))

    if not suggestions:
        suggestions.append(LeverageSuggestion(
            move="Status quo",
            rationale="Low leverage or neutral context — avoid over-managing",
            impact_hint=0.0
        ))

    return LeverageResponse(
        ctx={"inning": str(q.inning), "score_diff": str(q.score_diff),
             "outs": str(q.outs), "base": q.base, "matchup": f"{q.batter or '-'} vs {q.pitcher or '-'}",
             "leverage_idx": str(leverage_idx)},
        suggestions=suggestions
    )

# ===== #32 심판 EUZ 편향(스텁) =====
class UmpBiasResponse(BaseModel):
    ump: str
    zone_expand_pct: float   # 스트라이크 존 확장 비율(+면 넓음)
    low_strike_bias: float   # 낮은 코스 스트 주기(+면 낮은쪽 후함)
    edge_call_volatility: float  # 코너 콜 변동성(0~1)

@router.get("/ump/euz_bias", response_model=UmpBiasResponse)
async def euz_bias(ump: str):
    # 이름 해시로 안정 난수
    seed = _seed_from_str(ump)
    zexp = 0.02 + (seed % 7) * 0.005         # 2% ~ 5.5%
    lowb = -0.01 + (seed % 9) * 0.004        # -1% ~ +3.2%
    vol  = ((seed % 11) / 10.0) * 0.6 + 0.2  # 0.2 ~ 0.8
    return UmpBiasResponse(
        ump=ump,
        zone_expand_pct=round(zexp, 3),
        low_strike_bias=round(lowb, 3),
        edge_call_volatility=round(vol, 3),
    )

# ========= Day11: #34 원정 피로 / #30 라인업 최적화 =========
from pydantic import BaseModel
from typing import Tuple, List, Dict, Optional

# ===== #34 원정 피로(여행/연전 스텁) =====
class FatigueResponse(BaseModel):
    team: str
    date: str
    fatigue_index: float  # 0~100
    components: Dict[str, float]

@router.get("/travel/fatigue_index", response_model=FatigueResponse)
async def travel_fatigue_index(team: str, date: str):
    # team+date 기반 안정 난수
    seed = _seed_from(team, sum(ord(c) for c in (date or "")))
    rA = _stable_rng01(seed + 101)  # 이동거리/시차
    rB = _stable_rng01(seed + 211)  # 연전 길이
    rC = _stable_rng01(seed + 307)  # 휴식일 부족
    travel = round(35.0 * rA, 1)
    b2b    = round(40.0 * (rB**1.2), 1)
    rest   = round(30.0 * (rC**1.1), 1)
    idx = round(_clamp(travel + b2b + rest, 0.0, 100.0), 1)
    return FatigueResponse(
        team=team, date=date, fatigue_index=idx,
        components={"travel":travel, "back_to_back":b2b, "rest_deficit":rest}
    )

# ===== #30 라인업 최적화(기초) =====
class LineupPlayer(BaseModel):
    id: str
    pos: str
    woba: float
    bats: Optional[str] = None  # "L"|"R"|"S"

class LineupQuery(BaseModel):
    players: List[LineupPlayer]   # 최소 9명 기대(부족하면 채워서 반환)
    vs_pitcher: Optional[str] = None  # 상대 선발 "L"|"R"
    prefer_speed_top: bool = True

class LineupResponse(BaseModel):
    batting_order: List[str]          # 1~9 타순 id
    expected_runs_stub: float
    notes: List[str]

def _lineup_score_slot(woba: float, slot: int, speed_hint: float=0.0) -> float:
    # 단순 가중: 2~4번 상향, 9번 하향
    slot_weights = [0.92, 1.02, 1.08, 1.06, 1.00, 0.98, 0.96, 0.95, 0.90]
    w = slot_weights[min(max(slot-1,0),8)]
    return woba * w + 0.02*speed_hint

def _speed_hint(p: LineupPlayer) -> float:
    # 빠른 포지션 가점(스텁)
    return 1.0 if p.pos in ("CF","SS","2B","LF","RF") else 0.3 if p.pos in ("3B","1B") else 0.5

@router.post("/lineup/optimize", response_model=LineupResponse)
async def lineup_optimize(q: LineupQuery):
    # 1) 기본 정렬: wOBA 내림차순
    ps = list(q.players)
    ps.sort(key=lambda x: x.woba, reverse=True)

    # 2) 좌/우 매치업 미세 보정 (상대 선발 대비)
    if q.vs_pitcher in ("L","R"):
        for p in ps:
            if p.bats == "S": 
                continue
            if p.bats == "L" and q.vs_pitcher == "R":
                p.woba *= 1.02
            elif p.bats == "R" and q.vs_pitcher == "L":
                p.woba *= 1.02
            else:
                p.woba *= 0.99

    # 3) 1~4번에 상위 타자 배치, 1번은 출루/스피드 가점
    ps.sort(key=lambda x: x.woba, reverse=True)
    speed_sorted = sorted(ps[:5], key=lambda x: (_speed_hint(x), x.woba), reverse=True)
    if q.prefer_speed_top and speed_sorted:
        lead = speed_sorted[0]
        ps.remove(lead)
        order = [lead] + ps
    else:
        order = ps

    # 4) 정확히 9명만 사용(부족하면 끝에서 순환)
    if len(order) < 9:
        k = 9 - len(order)
        order += order[:k]
    order = order[:9]

    # 5) 기대 득점 스텁 계산(슬롯 가중)
    exp = 0.0
    for i, p in enumerate(order, start=1):
        exp += _lineup_score_slot(p.woba, i, _speed_hint(p))
    exp = round(4.0 + (exp - 9 * 0.33), 2)  # 대충 4점대 중심으로

    return LineupResponse(
        batting_order=[p.id for p in order],
        expected_runs_stub=float(max(2.0, exp)),
        notes=["heuristic_woba_order","matchup_adjusted" if q.vs_pitcher else "neutral"]
    )

# ========= Day12: #36 뉴스 통합 요약(스텁) / #37 전날 경기 리포트(스텁) =========
from pydantic import BaseModel
from typing import Tuple, List, Dict, Optional
from datetime import datetime, timedelta

# 공통 유틸: 짧은 안정 난수 요약 생성
def _stub_summaries(seed: int, n: int, prefix: str) -> List[str]:
    outs = []
    for i in range(n):
        v = _stable_rng01(seed + i*101)
        tag = "INJ" if v < 0.18 else "TRADE" if v < 0.34 else "PERF" if v < 0.66 else "MINORS"
        outs.append(f"[{tag}] {prefix} — stub-{int(v*1000)}")
    return outs

# ===== #36 뉴스 통합 요약 =====
class NewsDigestResponse(BaseModel):
    team: str
    date: str
    items: List[str]

@router.get("/intel/news_digest", response_model=NewsDigestResponse)
async def news_digest(team: str, date: str):
    # team+date로 시드
    seed = _seed_from_str(team) + sum(ord(c) for c in date)
    d = datetime.strptime(date, "%Y-%m-%d").date()
    items = _stub_summaries(seed, 6, f"{team} daily digest")
    return NewsDigestResponse(team=team, date=d.isoformat(), items=items)

# ===== #37 전날 경기 리포트 =====
class GameLine(BaseModel):
    opp: str
    result: str       # "W 5-3" 같은 형식
    key_players: List[str]
    notes: List[str]

class YesterdayReportResponse(BaseModel):
    team: str
    date: str
    games: List[GameLine]

@router.get("/reports/yesterday_games", response_model=YesterdayReportResponse)
async def yesterday_games(team: str, date: str):
    # 입력된 date의 "전날"을 가정
    d = datetime.strptime(date, "%Y-%m-%d").date()
    y = d - timedelta(days=1)
    seed = _seed_from_str(team) + y.toordinal()
    # 스텁 결과 생성
    v = _stable_rng01(seed)
    scored = int(3 + round(5*v))
    allowed = int(2 + round(4*(1.0-v)))
    res = "W" if scored > allowed else "L" if scored < allowed else "T"
    line = GameLine(
        opp="RIV",
        result=f"{res} {scored}-{allowed}",
        key_players=[f"BAT-{int(v*9)}", f"PIT-{int((1.0-v)*9)}"],
        notes=_stub_summaries(seed+999, 3, "game note")
    )
    return YesterdayReportResponse(team=team, date=y.isoformat(), games=[line])

# ========= Day13: #38 주간 운영 브리핑 / #40 증거 테이블 =========
from pydantic import BaseModel
from typing import Tuple, List, Dict, Optional
from datetime import datetime

# ----- #38 주간 운영 브리핑(템플릿) -----
class WeeklyOpsQuery(BaseModel):
    team: str
    week: str          # ISO week: "2025-W23"
    highlights: Optional[List[str]] = None
    injuries: Optional[List[str]] = None
    transactions: Optional[List[str]] = None
    notes: Optional[List[str]] = None

class WeeklyOpsResponse(BaseModel):
    team: str
    week: str
    sections: Dict[str, List[str]]

@router.post("/reports/weekly_ops", response_model=WeeklyOpsResponse)
async def weekly_ops(q: WeeklyOpsQuery):
    sections = {
        "Highlights": q.highlights or [f"{q.team} weekly highlight stub-1", f"{q.team} weekly highlight stub-2"],
        "Injuries": q.injuries or ["DL: none"],
        "Transactions": q.transactions or ["No major moves"],
        "Schedule/Outlook": [f"Week {q.week}: travel light, opp strength ~0.52 (stub)"],
        "Notes": q.notes or ["Auto-generated. Replace with real data hooks."]
    }
    return WeeklyOpsResponse(team=q.team, week=q.week, sections=sections)

# ----- #40 증거 테이블(요약 → 표) -----
class EvidenceItem(BaseModel):
    k: str
    v: float

class EvidenceTableQuery(BaseModel):
    items: List[EvidenceItem]

class EvidenceTableResponse(BaseModel):
    headers: List[str]
    rows: List[List[str]]
    csv: str

@router.post("/reports/evidence_table", response_model=EvidenceTableResponse)
async def evidence_table(q: EvidenceTableQuery):
    headers = ["Metric", "Value"]
    rows = [[it.k, f"{it.v}"] for it in q.items]
    # 간단 CSV 동시 생성
    csv = "Metric,Value\n" + "\n".join([f"{r[0]},{r[1]}" for r in rows])
    return EvidenceTableResponse(headers=headers, rows=rows, csv=csv)

# ========= Day14: #41 대화형 응답 규격 / #52 ID 매핑 =========
from pydantic import BaseModel
from typing import Tuple, List, Dict

# ----- #41 대화형 응답 규격(간단 스펙) -----
class ChatResponseSpec(BaseModel):
    required_fields: List[str]
    examples: Dict[str, Dict[str, str]]

@router.get("/chat/response_spec", response_model=ChatResponseSpec)
async def chat_response_spec():
    return ChatResponseSpec(
        required_fields=["title","summary","evidence","next_actions"],
        examples={
            "player-report":{
                "title":"Player X — trend & fit",
                "summary":"3-year trend up; platoon LHH fit for SEA.",
                "evidence":"OPS+=128, EV 92.4, DEF tag.",
                "next_actions":"Scout follow-up; check hamstring status."
            },
            "trade-proposal":{
                "title":"Deal A↔B (balanced)",
                "summary":"Delta within $2.4M tolerance.",
                "evidence":"Adj values: A=$18.1M vs B=$16.5M.",
                "next_actions":"Add PTBNL or cash $1.5–2.0M."
            }
        }
    )

# ----- #52 ID 매핑/정규화(스텁 규칙) -----
class IdMapQuery(BaseModel):
    ids: List[str]   # e.g., ["MLB:123","FG:smith_j","BBR:doejo01"]

class IdMapRow(BaseModel):
    raw: str
    person_id: str
    source: str

class IdMapResponse(BaseModel):
    mapped: List[IdMapRow]

@router.post("/meta/idmap_status", response_model=IdMapResponse)
async def idmap_status(q: IdMapQuery):
    mapped: List[IdMapRow] = []
    for raw in q.ids:
        if ":" in raw:
            src, val = raw.split(":", 1)
            src = src.upper()
        else:
            src, val = "UNK", raw
        # 간단 정규화 규칙
        if src == "MLB":
            pid = f"mlb_{val.zfill(6)}"
        elif src in ("FG","FANGRAPHS"):
            pid = "fg_" + val.lower().replace(" ", "_")
        elif src in ("BBR","BASEBALL-REFERENCE"):
            pid = "bbr_" + val.lower()
        else:
            pid = "ext_" + val.lower().replace(" ", "_")
        mapped.append(IdMapRow(raw=raw, person_id=pid, source=src))
    return IdMapResponse(mapped=mapped)

# ========= Day15: 외부 연동 뼈대 / 시크릿·레이트·페치 헬퍼 =========
from pydantic import BaseModel
from typing import Tuple, Dict, Optional, Any
import os, time, json
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

# --- 시크릿 키 상태: 값 노출 없이 "present/absent" 만 ---
class SecretsStatus(BaseModel):
    keys: Dict[str, bool]

_SECRETS_KEYS = [
    "STATS_API_KEY",        # MLB/Stats API
    "NEWS_RSS_URLS",        # 콤마/개행 분리
    "WEATHER_API_KEY",      # 날씨
    "REDIS_URL",            # 캐시(옵션)
    "PROXY_URL",            # 필요시
]

@router.get("/ops/secrets_status", response_model=SecretsStatus)
async def secrets_status():
    return SecretsStatus(keys={k: (os.getenv(k) is not None) for k in _SECRETS_KEYS})

# --- 간단 레이트 카운터(네임스페이스별 in-mem, 스텁) ---
_RATE_COUNTERS: Dict[str, Dict[str, Any]] = {}
def _rate_touch(ns: str, ok: bool, limit_hint: Optional[int] = None):
    now = int(time.time())
    c = _RATE_COUNTERS.setdefault(ns, {"calls":0,"ok":0,"fail":0,"first":now,"last":now,"limit_hint":limit_hint})
    c["calls"] += 1; c["ok"] += int(ok); c["fail"] += int(not ok); c["last"] = now
    if limit_hint is not None: c["limit_hint"] = limit_hint

class RateStatus(BaseModel):
    counters: Dict[str, Dict[str, Any]]

@router.get("/ops/rate_limit_status", response_model=RateStatus)
async def rate_limit_status():
    return RateStatus(counters=_RATE_COUNTERS)

# --- 외부 페치 + 간단 캐시(in-mem) ---
_EXT_CACHE: Dict[str, Dict[str, Any]] = {}
def _ext_cache_key(url: str, headers: Optional[Dict[str,str]]) -> str:
    h = json.dumps({"url":url, "headers": headers or {}}, sort_keys=True)
    return h

def _ext_fetch(url: str, headers: Optional[Dict[str,str]]=None, ttl_sec: int=300, ns: str="generic") -> Dict[str, Any]:
    key = _ext_cache_key(url, headers)
    now = time.time()

    # 캐시 히트
    ent = _EXT_CACHE.get(key)
    if ent and (now - ent["t0"] < ttl_sec):
        _rate_touch(ns, True)
        return {"source":"cache", "cache_hit": True, "status": 200, "fetched_at": ent["t0"], "ttl": ttl_sec, "text": ent["text"]}

    # 네트워크 시도
    try:
        req = Request(url, headers=headers or {})
        with urlopen(req, timeout=10) as r:
            text = r.read().decode("utf-8", errors="replace")
            _EXT_CACHE[key] = {"t0": now, "text": text}
            _rate_touch(ns, True)
            return {"source":"network", "cache_hit": False, "status": getattr(r, "status", 200), "fetched_at": now, "ttl": ttl_sec, "text": text}
    except (HTTPError, URLError) as e:
        _rate_touch(ns, False)
        # 네트워크 실패시 캐시 폴백
        if ent:
            return {"source":"cache_fallback", "cache_hit": True, "status": getattr(e, "code", 599), "error": str(e), "fetched_at": ent["t0"], "ttl": ttl_sec, "text": ent["text"]}
        return {"source":"error", "cache_hit": False, "status": getattr(e, "code", 599), "error": str(e)}

# --- 셀프체크: 임의 URL 호출(헤더/TTL 선택) ---
class ExtSelfcheckQuery(BaseModel):
    url: str
    ttl_sec: int = 120
    headers: Optional[Dict[str, str]] = None
    ns: str = "selfcheck"

class ExtSelfcheckResponse(BaseModel):
    ok: bool
    meta: Dict[str, Any]
    preview: str

@router.post("/ops/ext_selfcheck", response_model=ExtSelfcheckResponse)
async def ext_selfcheck(q: ExtSelfcheckQuery):
    r = _ext_fetch(q.url, headers=q.headers, ttl_sec=q.ttl_sec, ns=q.ns)
    ok = r.get("status", 599) < 500
    text = r.get("text","")
    preview = text[:300].replace("\n"," ") if isinstance(text, str) else str(text)[:300]
    meta = {k:v for k,v in r.items() if k != "text"}
    return ExtSelfcheckResponse(ok=ok, meta=meta, preview=preview)
# ========= Day16: #43 MLB API fetch 스텁 =========
from pydantic import BaseModel
from typing import Tuple, Optional, Dict, Any

class ExtFetchQuery(BaseModel):
    url: str
    ttl_sec: int = 60
    ns: Optional[str] = None

@router.get("/ext/mlb/schedule")
async def ext_mlb_schedule(team: str, date: str):
    """
    MLB 일정 API 스텁 (향후 MLB Stats API 연동 예정)
    """
    # TODO: 실제 API 호출로 교체 예정
    return {
        "team": team,
        "date": date,
        "games": [
            {"opp": "NYY", "venue": "home", "status": "stub"},
            {"opp": "BOS", "venue": "away", "status": "stub"},
        ],
        "note": "Stubbed schedule; replace with MLB Stats API"
    }

@router.get("/ext/mlb/boxscore")
async def ext_mlb_boxscore(game_id: str):
    """
    MLB 박스스코어 API 스텁
    """
    # TODO: 실제 API 호출로 교체 예정
    return {
        "game_id": game_id,
        "linescore": {"SEA": 5, "LAD": 3},
        "key_players": ["PIT-101", "BAT-202"],
        "note": "Stubbed boxscore; replace with MLB Stats API"
    }

# ========= Day17: MLB Stats API LIVE 어댑터 (캐시+폴백) =========
from typing import Tuple, List, Dict, Any, Optional
from datetime import datetime

# 간단 팀 ID 매핑(확장 가능)
_MLB_TEAM_ID = {
    "LAD": 119, "SF": 137, "SEA": 136, "NYY": 147, "BOS": 111,
    "ATL": 144, "HOU": 117, "CHC": 112, "STL": 138, "NYM": 121,
}

def _team_to_id(team: str) -> Optional[int]:
    if not team: return None
    t = team.upper()
    if t.isdigit():
        return int(t)
    return _MLB_TEAM_ID.get(t)

@router.get("/ext/mlb/team_id")
async def mlb_team_id(team: str):
    return {"team": team, "team_id": _team_to_id(team)}

# LIVE: 일정
@router.get("/ext/mlb/schedule_live")
async def ext_mlb_schedule_live(team: str, date: str):
    """
    StatsAPI: https://statsapi.mlb.com/api/v1/schedule?sportId=1&teamId=136&date=2025-06-10
    """
    tid = _team_to_id(team)
    if not tid:
        return {"ok": False, "error": "unknown_team", "team": team}

    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&teamId={tid}&date={date}"
    r = _ext_fetch(url, headers={"User-Agent":"cogm-assistant"}, ttl_sec=300, ns="mlb/schedule")
    ok = r.get("status", 599) < 500 and isinstance(r.get("text"), str)
    try:
        raw = json.loads(r.get("text","")) if ok else {}
    except Exception:
        raw = {}
        ok = False

    games: List[Dict[str, Any]] = []
    try:
        for d in (raw.get("dates") or []):
            for g in (d.get("games") or []):
                games.append({
                    "game_pk": g.get("gamePk"),
                    "status": ((g.get("status") or {}).get("detailedState")),
                    "home": ((g.get("teams") or {}).get("home") or {}).get("team", {}).get("abbreviation"),
                    "away": ((g.get("teams") or {}).get("away") or {}).get("team", {}).get("abbreviation"),
                    "venue": (g.get("venue") or {}).get("name"),
                })
    except Exception:
        ok = False

    if not ok:
        # 폴백: 기존 스텁 형태
        return {
            "ok": False, "fallback": True, "team": team, "date": date,
            "note": "network/cache failure → stub fallback",
            "games": [
                {"opp": "NYY", "venue": "home", "status": "stub"},
                {"opp": "BOS", "venue": "away", "status": "stub"},
            ],
            "meta": {k:v for k,v in r.items() if k!="text"}
        }

    return {
        "ok": True, "team": team, "date": date,
        "games": games[:10],
        "meta": {k:v for k,v in r.items() if k!="text"}
    }

# LIVE: 박스스코어
@router.get("/ext/mlb/boxscore_live")
async def ext_mlb_boxscore_live(game_pk: int):
    """
    StatsAPI: https://statsapi.mlb.com/api/v1/game/{gamePk}/boxscore
    """
    url = f"https://statsapi.mlb.com/api/v1/game/{game_pk}/boxscore"
    r = _ext_fetch(url, headers={"User-Agent":"cogm-assistant"}, ttl_sec=600, ns="mlb/boxscore")
    ok = r.get("status", 599) < 500 and isinstance(r.get("text"), str)
    try:
        raw = json.loads(r.get("text","")) if ok else {}
    except Exception:
        raw = {}
        ok = False

    # 최소 요약만 파싱(팀명, 점수 요약 추출 시도)
    def _abbr(side: Dict[str,Any]) -> Optional[str]:
        return ((side or {}).get("team") or {}).get("abbreviation")

    def _runs(side: Dict[str,Any]) -> Optional[int]:
        # boxscore에는 최종 점수가 직접 없을 수 있어 aggregate가 필요하지만, 여기선 스텁(없으면 None)
        return (side or {}).get("teamStats",{}).get("batting",{}).get("runs")

    home = (raw.get("teams") or {}).get("home", {})
    away = (raw.get("teams") or {}).get("away", {})

    if not ok or not home or not away:
        return {
            "ok": False, "fallback": True, "game_pk": game_pk,
            "linescore": {"SEA": 5, "LAD": 3},
            "key_players": ["PIT-101", "BAT-202"],
            "note": "network/cache failure → stub fallback",
            "meta": {k:v for k,v in r.items() if k!="text"}
        }

    return {
        "ok": True, "game_pk": game_pk,
        "home": {"abbr": _abbr(home), "runs_stub": _runs(home)},
        "away": {"abbr": _abbr(away), "runs_stub": _runs(away)},
        "meta": {k:v for k,v in r.items() if k!="text"}
    }

# ========= Day18: 뉴스 RSS 다중 인입(LIVE) =========
from pydantic import BaseModel
from typing import Tuple, List, Dict, Any, Optional
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

_NEWS_REGISTRY: Dict[str, List[str]] = {}  # team -> feeds[]

class NewsIngestQuery(BaseModel):
    team: str
    feeds: List[str]  # RSS/Atom URL 리스트

class NewsIngestResponse(BaseModel):
    team: str
    feeds: List[str]
    total: int

@router.post("/intel/news_ingest", response_model=NewsIngestResponse)
async def news_ingest(q: NewsIngestQuery):
    cur = _NEWS_REGISTRY.get(q.team, [])
    # 중복 제거 + 순서 보존
    s = set(cur)
    for u in q.feeds:
        if u not in s:
            cur.append(u); s.add(u)
    _NEWS_REGISTRY[q.team] = cur
    return NewsIngestResponse(team=q.team, feeds=cur, total=len(cur))

class NewsItem(BaseModel):
    src: str
    title: str
    link: Optional[str] = None
    published: Optional[str] = None  # ISO8601

class NewsDigestLiveResponse(BaseModel):
    team: str
    feeds: List[str]
    items: List[NewsItem]
    meta: Dict[str, Any]

def _rss_items_parse(text: str, src: str, limit: int = 5) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    try:
        root = ET.fromstring(text)
    except Exception:
        return out
    # RSS 2.0: channel/item
    chan = root.find("channel")
    nodes = (chan.findall("item") if chan is not None else []) or root.findall(".//item")
    for it in nodes[: limit * 2]:
        title = (it.findtext("title") or "").strip()
        link  = (it.findtext("link") or "").strip()
        pub   = (it.findtext("pubDate") or "").strip()
        # RFC822 → ISO8601 시도
        iso = None
        if pub:
            try:
                # 예: Tue, 24 Sep 2024 18:10:00 GMT
                iso = datetime.strptime(pub[:25], "%a, %d %b %Y %H:%M:%S").replace(tzinfo=timezone.utc).isoformat()
            except Exception:
                iso = None
        if title:
            out.append({"src": src, "title": title, "link": link or None, "published": iso})
        if len(out) >= limit:
            break
    # Atom: entry
    if not out:
        entries = root.findall(".//{http://www.w3.org/2005/Atom}entry")
        for e in entries[:limit]:
            title = (e.findtext("{http://www.w3.org/2005/Atom}title") or "").strip()
            link_el = e.find("{http://www.w3.org/2005/Atom}link")
            href = link_el.get("href") if link_el is not None else None
            pub = (e.findtext("{http://www.w3.org/2005/Atom}updated") or "").strip() or \
                  (e.findtext("{http://www.w3.org/2005/Atom}published") or "").strip()
            if title:
                out.append({"src": src, "title": title, "link": href, "published": pub or None})
    return out[:limit]

@router.get("/intel/news_digest_live", response_model=NewsDigestLiveResponse)
async def news_digest_live(team: str, limit: int = 5):
    feeds = _NEWS_REGISTRY.get(team, [])
    items: List[NewsItem] = []
    metas: List[Dict[str, Any]] = []
    for u in feeds:
        r = _ext_fetch(u, headers={"User-Agent":"cogm-assistant"}, ttl_sec=600, ns="news/rss")
        metas.append({k:v for k,v in r.items() if k!="text"})
        txt = r.get("text") or ""
        for it in _rss_items_parse(txt, u, limit=limit):
            items.append(NewsItem(**it))
    # 간단 정렬: published(ISO) 내림차순 → title
    def _key(x: NewsItem):
        return (x.published or "", x.title)
    items.sort(key=_key, reverse=True)
    return NewsDigestLiveResponse(team=team, feeds=feeds, items=items[:limit], meta={"feeds_count": len(feeds), "calls": len(metas)})

# ========= Day19: 날씨 연동 → 파크팩터/승률 반영 =========
from pydantic import BaseModel
from typing import Tuple, Optional, Dict, Any
from datetime import datetime, date

# 간단 구장 좌표(확장 가능; 미지정 파크는 lat/lon 쿼리로 대체)
_PARK_LATLON = {
    "LAD": (34.0739, -118.2400),  # Dodger Stadium (approx)
    "SF":  (37.7786, -122.3893),  # Oracle Park
    "SEA": (47.5914, -122.3325),  # T-Mobile Park
    "NYY": (40.8296,  -73.9262),  # Yankee Stadium
    "BOS": (42.3467,  -71.0972),  # Fenway
}

def _resolve_latlon(park: Optional[str], lat: Optional[float], lon: Optional[float]):
    if lat is not None and lon is not None:
        return float(lat), float(lon)
    if park:
        t = (park or "").upper()
        if t in _PARK_LATLON:
            return _PARK_LATLON[t]
    return None, None

# ===== 19-1) 날씨 LIVE =====
class WeatherResponse(BaseModel):
    park: Optional[str] = None
    date: str
    latitude: float
    longitude: float
    temp_c_avg: float
    wind_speed_avg: float
    precip_prob_avg: float
    meta: Dict[str, Any]

@router.get("/ext/weather/game", response_model=WeatherResponse)
async def weather_game(park: Optional[str] = None, date: str = "", lat: Optional[float] = None, lon: Optional[float] = None):
    """
    Open-Meteo (무료, 키 불필요)에서 일자별 시간대 데이터 수집 후 일 평균 요약
    https://api.open-meteo.com/v1/forecast?latitude=...&longitude=...&hourly=temperature_2m,precipitation_probability,wind_speed_10m&start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
    """
    la, lo = _resolve_latlon(park, lat, lon)
    if la is None or lo is None:
        return WeatherResponse(
            park=park, date=date or datetime.utcnow().date().isoformat(),
            latitude=0.0, longitude=0.0,
            temp_c_avg=20.0, wind_speed_avg=3.0, precip_prob_avg=10.0,
            meta={"fallback": True, "reason": "no_latlon"}
        )

    d = (date or datetime.utcnow().date().isoformat())
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={la}&longitude={lo}"
        "&hourly=temperature_2m,precipitation_probability,wind_speed_10m"
        f"&start_date={d}&end_date={d}"
        "&timezone=UTC"
    )
    r = _ext_fetch(url, headers={"User-Agent":"cogm-assistant"}, ttl_sec=3600, ns="weather")
    ok = r.get("status", 599) < 500 and isinstance(r.get("text"), str)
    temp, wind, pprob = 20.0, 3.0, 10.0
    try:
        j = json.loads(r.get("text","")) if ok else {}
        H = (j.get("hourly") or {})
        tlist = H.get("temperature_2m") or []
        wlist = H.get("wind_speed_10m") or []
        plist = H.get("precipitation_probability") or []
        def _avg(arr, default):
            arr = [x for x in arr if isinstance(x,(int,float))]
            return sum(arr)/len(arr) if arr else default
        temp = float(round(_avg(tlist, 20.0), 1))
        wind = float(round(_avg(wlist, 3.0), 1))
        pprob = float(round(_avg(plist, 10.0), 1))
    except Exception:
        ok = False

    meta = {k:v for k,v in r.items() if k!="text"}
    meta["ok"] = ok
    return WeatherResponse(
        park=park, date=d, latitude=float(la), longitude=float(lo),
        temp_c_avg=temp, wind_speed_avg=wind, precip_prob_avg=pprob,
        meta=meta
    )

# ===== 19-2) 파크팩터 LIVE(날씨 반영) =====
class ParkFactorsLiveResponse(BaseModel):
    park: str
    date: str
    run_factor: float
    hr_factor: float
    xbh_factor: float
    weather_used: Dict[str, float]
    meta: Dict[str, Any]

@router.get("/parks/daily_factors_live", response_model=ParkFactorsLiveResponse)
async def parks_daily_factors_live(park: str, date: str):
    # 1) 기본 Day9 모델
    base = await parks_daily_factors(park=park, date=date)
    # 2) 날씨 주입
    W = await weather_game(park=park, date=date)
    temp = W.temp_c_avg
    wind = W.wind_speed_avg
    rain = W.precip_prob_avg

    # 간단 효과: 온도↑ → R/HR 소폭↑, 강풍↑ → HR↑, 강수↑ → R/HR↓
    run = base.run_factor
    hr  = base.hr_factor
    xbh = base.xbh_factor

    run *= (1.0 + 0.002 * (temp - 20.0))          # +0.2% per +1C
    hr  *= (1.0 + 0.003 * max(0.0, wind - 2.0))   # +0.3% per wind>2m/s
    damp = max(0.0, (rain - 20.0) * 0.004)        # rain>20% → 감소
    run *= (1.0 - damp)
    hr  *= (1.0 - damp * 0.6)
    xbh *= (1.0 - damp * 0.3)

    return ParkFactorsLiveResponse(
        park=park, date=date,
        run_factor=round(float(run), 3),
        hr_factor=round(float(hr), 3),
        xbh_factor=round(float(xbh), 3),
        weather_used={"temp_c": temp, "wind_speed": wind, "precip_prob": rain},
        meta={"weather_ok": W.meta.get("ok", False)}
    )

# ===== 19-3) 승률 예측(날씨 반영 버전) =====
class WinProbWeatherQuery(WinProbQuery):
    temp_c: Optional[float] = None
    wind_speed: Optional[float] = None
    precip_prob: Optional[float] = None

class WinProbWeatherResponse(WinProbResponse):
    components: Dict[str, float]

@router.post("/forecast/win_prob_weather", response_model=WinProbWeatherResponse)
async def forecast_win_prob_weather(q: WinProbWeatherQuery):
    # 기본 WP
    base = await forecast_win_prob(WinProbQuery(**q.model_dump(include={"home","away","elo_home","elo_away","park","sp_adj","home_field_pts","pyth_rs_home","pyth_ra_home","pyth_exp"})))
    p_home = base.wp_home
    diff_effect = 0.0

    # 날씨 영향(엘로 포인트 환산): 따뜻할수록 +, 바람↑ 약간 +, 비↑ −
    if q.temp_c is not None:
        diff_effect +=  (q.temp_c - 20.0) * 0.4     # +0.4 elo pts / +1C
    if q.wind_speed is not None:
        diff_effect +=  max(0.0, q.wind_speed - 2.0) * 0.6
    if q.precip_prob is not None:
        diff_effect +=  -(q.precip_prob - 20.0) * 0.5

    # 로지스틱 변환으로 미세 조정
    if abs(diff_effect) > 1e-9:
        adj_p = 1.0 / (1.0 + math.pow(10.0, -(math.log10(p_home/(1-p_home))*400 + diff_effect)/400.0))
        p_home = 0.7 * p_home + 0.3 * adj_p  # 과조정 방지: 30%만 반영

    p_home = float(round(_clamp(p_home, 0.0, 1.0), 4))
    return WinProbWeatherResponse(
        home=base.home, away=base.away, wp_home=p_home, wp_away=round(1-p_home,4),
        components={**base.components, "weather_elo_effect": round(diff_effect,2)}
    )

# ========= Day20: 심판 배정 LIVE + EUZ 편향(LIVE 스텁) =========
from pydantic import BaseModel
from typing import Tuple, List, Dict, Any, Optional

# ----- 20-1) 심판 배정 LIVE (StatsAPI: feed/live) -----
class UmpAssignment(BaseModel):
    game_pk: int
    plate: Optional[str] = None
    bases: List[str] = []
    meta: Dict[str, Any] = {}

@router.get("/ext/umpire/assignment", response_model=UmpAssignment)
async def umpire_assignment(game_pk: int):
    """
    GET https://statsapi.mlb.com/api/v1/game/{gamePk}/feed/live
    officials[].officialType: 'Home Plate', 'First Base', ...
    officials[].official.fullName
    """
    url = f"https://statsapi.mlb.com/api/v1/game/{game_pk}/feed/live"
    r = _ext_fetch(url, headers={"User-Agent":"cogm-assistant"}, ttl_sec=900, ns="mlb/feedlive")
    ok = r.get("status", 599) < 500 and isinstance(r.get("text"), str)
    plate: Optional[str] = None
    bases: List[str] = []
    if ok:
        try:
            j = json.loads(r["text"])
            offs = (j.get("liveData", {}).get("boxscore", {}).get("officials") or [])
            for o in offs:
                otype = (o.get("officialType") or "").lower()
                name = ((o.get("official") or {}).get("fullName") or "").strip() or None
                if not name:
                    continue
                if "home" in otype and "plate" in otype:
                    plate = name
                else:
                    bases.append(f"{otype}:{name}")
        except Exception:
            ok = False

    if not ok:
        return UmpAssignment(
            game_pk=game_pk,
            plate=None,
            bases=[],
            meta={k:v for k,v in r.items() if k!="text"} | {"fallback": True}
        )
    return UmpAssignment(
        game_pk=game_pk,
        plate=plate,
        bases=bases,
        meta={k:v for k,v in r.items() if k!="text"} | {"fallback": False}
    )

# ----- 20-2) EUZ 편향(LIVE 스텁): 배정된 홈플레이트 심판 이름 기반 결정론 편향 산출 -----
class UmpBiasLive(BaseModel):
    game_pk: int
    ump: Optional[str]
    zone_expand_pct: float
    low_strike_bias: float
    edge_call_volatility: float
    meta: Dict[str, Any]

def _bias_from_name(ump: Optional[str]) -> Dict[str, float]:
    if not ump:
        # 이름이 없으면 보수적 중립값
        return {"zone_expand_pct": 0.02, "low_strike_bias": 0.0, "edge_call_volatility": 0.5}
    s = _seed_from_str(ump)
    # 이름 해시로 결정론 난수 → 안정적인 편향값
    zexp = 0.02 + (s % 9) * 0.004        # 2.0% ~ 5.2%
    lowb = -0.008 + (s % 11) * 0.003     # -0.8% ~ +2.5%
    vol  = 0.25 + ((s % 13) / 12.0) * 0.55  # 0.25 ~ 0.80
    return {"zone_expand_pct": round(zexp,3), "low_strike_bias": round(lowb,3), "edge_call_volatility": round(vol,3)}

@router.get("/ump/euz_bias_live", response_model=UmpBiasLive)
async def euz_bias_live(game_pk: int):
    ua = await umpire_assignment(game_pk=game_pk)
    bias = _bias_from_name(ua.plate)
    return UmpBiasLive(
        game_pk=game_pk,
        ump=ua.plate,
        zone_expand_pct=bias["zone_expand_pct"],
        low_strike_bias=bias["low_strike_bias"],
        edge_call_volatility=bias["edge_call_volatility"],
        meta=ua.meta
    )

# ========= Day21: KBO/NPB 1차 카드 + 리그 간 보정 브리지 =========
from pydantic import BaseModel
from typing import Tuple, Optional, Dict, Any

# ----- 공통: 간단 카드 형태 -----
class IntlPlayerCard(BaseModel):
    league: str           # "KBO" | "NPB"
    player_id: str
    name: Optional[str] = None
    season: Optional[int] = None
    pos: Optional[str] = None
    bats: Optional[str] = None
    throws: Optional[str] = None
    basic: Dict[str, Any] = {}   # AVG/OBP/SLG/OPS, ERA, IP 등 가벼운 표
    meta: Dict[str, Any] = {}

# ----- (옵션) 외부 URL에서 텍스트 가져와 간단 키워드만 추출하는 초간단 파서 -----
def _parse_hint_from_text(txt: str) -> Dict[str, Any]:
    # 라이브 HTML/텍스트가 들어오면 몇 가지 키 필드만 힌트로 뽑는다(실패해도 안전)
    out = {}
    s = txt[:10_000].lower() if isinstance(txt, str) else ""
    if "era" in s: out["has_era"] = True
    if "ops" in s: out["has_ops"] = True
    if "war" in s: out["has_war"] = True
    return out

# ----- KBO 카드 (LIVE URL 옵션 / 기본은 스텁) -----
@router.get("/ext/kbo/player_card", response_model=IntlPlayerCard)
async def kbo_player_card(pid: str, season: Optional[int] = None, url: Optional[str] = None):
    league = "KBO"
    meta: Dict[str, Any] = {"fallback": True}
    basic: Dict[str, Any] = {}
    name = None

    # 1) 라이브 시도(선택 URL 제공 시)
    if url:
        r = _ext_fetch(url, headers={"User-Agent":"cogm-assistant"}, ttl_sec=1800, ns="kbo/card")
        meta = {k:v for k,v in r.items() if k!="text"}
        txt = r.get("text") or ""
        hints = _parse_hint_from_text(txt)
        meta["hints"] = hints
        meta["fallback"] = not (r.get("status",599) < 500)

    # 2) 스텁(안전 기본값)
    #   추후 실제 KBO 엔드포인트를 결정하면 여기서 JSON 파싱으로 대체
    basic = {
        "G": 128, "PA": 560, "AVG": 0.318, "OBP": 0.392, "SLG": 0.478, "OPS": 0.870,
        "HR": 21, "BB%": 0.11, "K%": 0.16
    }
    name = f"KBO-{pid}"
    return IntlPlayerCard(league=league, player_id=pid, name=name, season=season or 2025, pos="OF", bats="L", throws="R", basic=basic, meta=meta)

# ----- NPB 카드 (LIVE URL 옵션 / 기본은 스텁) -----
@router.get("/ext/npb/player_card", response_model=IntlPlayerCard)
async def npb_player_card(pid: str, season: Optional[int] = None, url: Optional[str] = None):
    league = "NPB"
    meta: Dict[str, Any] = {"fallback": True}
    basic: Dict[str, Any] = {}
    name = None

    if url:
        r = _ext_fetch(url, headers={"User-Agent":"cogm-assistant"}, ttl_sec=1800, ns="npb/card")
        meta = {k:v for k,v in r.items() if k!="text"}
        txt = r.get("text") or ""
        hints = _parse_hint_from_text(txt)
        meta["hints"] = hints
        meta["fallback"] = not (r.get("status",599) < 500)

    basic = {
        "G": 130, "PA": 575, "AVG": 0.301, "OBP": 0.384, "SLG": 0.455, "OPS": 0.839,
        "HR": 17, "BB%": 0.10, "K%": 0.14
    }
    name = f"NPB-{pid}"
    return IntlPlayerCard(league=league, player_id=pid, name=name, season=season or 2025, pos="IF", bats="R", throws="R", basic=basic, meta=meta)

# ----- 리그 간 보정 브리지 -----
#   간단히 리그/시즌별 run environment 계수와 contact/power 계수를 합성해 MLB 등가치를 추정
class BridgeIn(BaseModel):
    src_league: str           # "KBO"|"NPB"|"MLB"
    dst_league: str           # 보통 "MLB"
    season: int = 2025
    metrics: Dict[str, float] # 예: {"OPS":0.870,"wOBA":0.372,"ERA":2.45}

class BridgeOut(BaseModel):
    src_league: str
    dst_league: str
    season: int
    adjusted: Dict[str, float]
    coeffs_used: Dict[str, float]

_BRIDGE_COEFF = {
    # 대충 예시값(문헌/사내 보정값으로 대체 예정)
    # 값>1 → MLB로 갈수록 난이도 상승 가정
    ("KBO","MLB"): {"run_env": 0.92, "contact": 0.95, "power": 0.88, "pitch": 1.10},
    ("NPB","MLB"): {"run_env": 0.95, "contact": 0.97, "power": 0.92, "pitch": 1.06},
    ("MLB","MLB"): {"run_env": 1.00, "contact": 1.00, "power": 1.00, "pitch": 1.00},
}

def _bridge_adjust(src: str, dst: str, m: Dict[str, float]) -> Tuple[Dict[str, float], Dict[str, float]]:
    key = (src.upper(), dst.upper())
    c = _BRIDGE_COEFF.get(key, {"run_env":1.0,"contact":1.0,"power":1.0,"pitch":1.0})
    out: Dict[str, float] = {}
    for k, v in m.items():
        if k.upper() in ("OPS","SLG","ISO","wOBA"):
            # 타격 지표: run_env * contact * power
            out[k] = round(float(v) * c["run_env"] * c["contact"] * c["power"], 3)
        elif k.upper() in ("ERA","FIP","xFIP"):
            # 투수 지표: pitch / run_env (MLB 난이도↑ → ERA 나빠질 수 있음)
            out[k] = round(float(v) * c["pitch"] / max(0.001, c["run_env"]), 3)
        else:
            # 기타는 run_env만 적용
            out[k] = round(float(v) * c["run_env"], 3)
    return out, c

@router.post("/meta/league_bridge", response_model=BridgeOut)
async def league_bridge(q: BridgeIn):
    adj, coeff = _bridge_adjust(q.src_league, q.dst_league, q.metrics)
    return BridgeOut(src_league=q.src_league, dst_league=q.dst_league, season=q.season, adjusted=adj, coeffs_used=coeff)

# ========= Day22: 아마추어 & 드래프트 모듈 =========
from pydantic import BaseModel, Field
from typing import Tuple, List, Dict, Optional, Any

# ---------- #A 아마추어 카드(대학/고교) ----------
class AmateurCard(BaseModel):
    level: str            # "NCAA"|"HS" 등
    player_id: str
    name: Optional[str] = None
    pos: Optional[str] = None
    bats: Optional[str] = None
    throws: Optional[str] = None
    tools: Dict[str, float] = {}   # Hit/Power/Run/Arm/Field — 20~80
    statline: Dict[str, Any] = {}  # AVG/OBP/SLG/OPS, K%, BB% 등
    notes: List[str] = []
    meta: Dict[str, Any] = {}

@router.get("/amateur/card", response_model=AmateurCard)
async def amateur_card(level: str, player_id: str, name: Optional[str] = None, pos: Optional[str] = None):
    # 결정론 난수로 20–80 스케일 생성
    s = _seed_from_str(f"{level}:{player_id}")
    def g(bias=0.5):
        # bias>0.5면 상향, <0.5면 하향
        v = _stable_rng01(s := int(s))  # noqa: F841
        return int(20 + round(60 * _clamp(_stable_rng01(s + 101) * 0.6 + bias*0.4, 0, 1))) // 5 * 5
    tools = {
        "Hit":   g(0.55),
        "Power": g(0.52),
        "Run":   g(0.58 if (pos or "").upper() in ("CF","SS","2B") else 0.5),
        "Arm":   g(0.5),
        "Field": g(0.54)
    }
    # 간단 성적 스텁
    avg = round(0.260 + 0.12 * _stable_rng01(s + 7), 3)
    obp = round(avg + 0.070 + 0.05 * _stable_rng01(s + 9), 3)
    slg = round(avg + 0.120 + 0.10 * _stable_rng01(s + 11), 3)
    ops = round(obp + slg, 3)  # 대충 모양 맞추기
    stat = {"AVG": avg, "OBP": obp, "SLG": slg, "OPS": ops, "K%": round(0.12+0.12*_stable_rng01(s+13),3), "BB%": round(0.07+0.08*_stable_rng01(s+15),3)}
    notes = ["Auto-generated amateur card (stub)", "Replace with NCAA/Perfect Game/NPB Draft DB hooks"]
    return AmateurCard(
        level=level.upper(), player_id=player_id, name=name or f"{level}-{player_id}",
        pos=(pos or "UTL").upper(), bats=None, throws=None, tools=tools, statline=stat,
        notes=notes, meta={"fallback": True}
    )

# ---------- #B 드래프트 후보 구조 & 니즈 ----------
class DraftCandidate(BaseModel):
    player_id: str
    level: str = Field(description="NCAA|HS 등")
    pos: str
    hand: Optional[str] = None   # L/R/S
    tools: Dict[str, float] = {} # 20~80

class TeamNeeds(BaseModel):
    need_pos: List[str] = []     # 우선 포지션(예: C, SS, CF, LHP)
    prefer_hand: Optional[str] = None  # L/R
    prefer_tool: Optional[str] = None  # "Power","Run","Field","Hit","Arm"

class MockDraftQuery(BaseModel):
    team: str
    pick_no: int
    candidates: List[DraftCandidate]
    needs: TeamNeeds = TeamNeeds()

class MockDraftChoice(BaseModel):
    player_id: str
    pos: str
    score: float
    reasons: List[str]

class MockDraftResponse(BaseModel):
    team: str
    pick_no: int
    ranked: List[MockDraftChoice]

def _tool_score(tools: Dict[str, float]) -> float:
    if not tools: return 40.0
    # Hit/Power 가중 상향, Field/Run 서브, Arm 보조
    w = {"Hit":0.32,"Power":0.28,"Run":0.14,"Field":0.18,"Arm":0.08}
    return sum((tools.get(k,40.0))*w[k] for k in w)

@router.post("/draft/mock", response_model=MockDraftResponse)
async def mock_draft(q: MockDraftQuery):
    ranked: List[MockDraftChoice] = []
    for c in q.candidates:
        base = _tool_score(c.tools)
        reasons = [f"base_tool={round(base,1)}"]
        # 포지션 니즈 가점
        if q.needs.need_pos and c.pos.upper() in [p.upper() for p in q.needs.need_pos]:
            base += 5.0; reasons.append("need_pos_match")
        # 손잡이 선호 가점
        if q.needs.prefer_hand and (c.hand or "").upper() == q.needs.prefer_hand.upper():
            base += 2.0; reasons.append("hand_pref")
        # 특정 툴 선호 가점
        pt = q.needs.prefer_tool
        if pt and pt in c.tools:
            base += (c.tools[pt]-50.0)/10.0; reasons.append(f"tool_pref:{pt}")
        ranked.append(MockDraftChoice(player_id=c.player_id, pos=c.pos.upper(), score=round(base,1), reasons=reasons))
    ranked.sort(key=lambda x: x.score, reverse=True)
    return MockDraftResponse(team=q.team, pick_no=q.pick_no, ranked=ranked[:10])

# ---------- #C 미래 WAR 간이 추정(스텁) ----------
class FutureWarQuery(BaseModel):
    level: str
    tools: Dict[str, float] = {}
    pos: Optional[str] = None
    horizon_years: int = 6

class FutureWarResponse(BaseModel):
    mean_war: float
    p90_war_season: float
    bust_prob: float
    notes: List[str]

@router.post("/draft/future_war", response_model=FutureWarResponse)
async def draft_future_war(q: FutureWarQuery):
    # 매우 단순 히ュー리스틱: Hit/Power/Field 중심, 프리미엄 포지션 가점
    hit = q.tools.get("Hit", 45.0)
    powr = q.tools.get("Power", 45.0)
    fld = q.tools.get("Field", 45.0)
    run = q.tools.get("Run", 45.0)
    arm = q.tools.get("Arm", 45.0)
    base = (0.04*hit + 0.035*powr + 0.03*fld + 0.02*run + 0.015*arm) - 3.0
    pos_bonus = 0.6 if (q.pos or "").upper() in ("C","SS","CF") else 0.2 if (q.pos or "").upper() in ("2B","3B") else 0.0
    mean_war = max(0.0, round(q.horizon_years * max(0.0, base + pos_bonus), 1))
    # 변동성: 상위 툴 평균이 높을수록 분포 우측
    avg_tool = (hit+powr+fld)/3.0
    p90 = round(mean_war * (1.15 + (avg_tool-50.0)/200.0), 1)
    bust = float(round(max(0.05, 0.45 - (avg_tool-50.0)/100.0), 2))
    return FutureWarResponse(
        mean_war=mean_war,
        p90_war_season=p90,
        bust_prob=bust,
        notes=["Heuristic projection (stub) — replace with historical comps later"]
    )

# ========= Day22 hotfix: /amateur/card_fix (기존 버그 우회) =========
from pydantic import BaseModel
from typing import Tuple, Optional, Dict, Any, List

class AmateurCard(BaseModel):
    level: str            # "NCAA"|"HS" 등
    player_id: str
    name: Optional[str] = None
    pos: Optional[str] = None
    bats: Optional[str] = None
    throws: Optional[str] = None
    tools: Dict[str, float] = {}
    statline: Dict[str, Any] = {}
    notes: List[str] = []
    meta: Dict[str, Any] = {}

@router.get("/amateur/card_fix", response_model=AmateurCard)
async def amateur_card_fix(level: str, player_id: str, name: Optional[str] = None, pos: Optional[str] = None):
    base_seed = _seed_from_str(f"{level}:{player_id}")

    def r(off: int) -> float:
        return _stable_rng01(base_seed + off)

    def to_20_80(x: float) -> int:
        v = 20 + 60 * _clamp(x, 0.0, 1.0)
        return int(round(v / 5.0) * 5)

    # bias 적용: x = rand*0.6 + bias*0.4
    def g(bias: float, off: int) -> int:
        x = r(off) * 0.6 + bias * 0.4
        return to_20_80(x)

    _pos = (pos or "UTL").upper()
    tools = {
        "Hit":   g(0.55, 101),
        "Power": g(0.52, 103),
        "Run":   g(0.58 if _pos in ("CF","SS","2B") else 0.50, 105),
        "Arm":   g(0.50, 107),
        "Field": g(0.54, 109),
    }

    avg = round(0.260 + 0.12 * r(7), 3)
    obp = round(avg + 0.070 + 0.05 * r(9), 3)
    slg = round(avg + 0.120 + 0.10 * r(11), 3)
    ops = round(obp + slg, 3)
    stat = {
        "AVG": avg, "OBP": obp, "SLG": slg, "OPS": ops,
        "K%": round(0.12 + 0.12 * r(13), 3),
        "BB%": round(0.07 + 0.08 * r(15), 3),
    }

    notes = ["Auto-generated amateur card (stub)", "Replace with NCAA/PG hooks later"]
    return AmateurCard(
        level=level.upper(), player_id=player_id, name=name or f"{level}-{player_id}",
        pos=_pos, bats=None, throws=None, tools=tools, statline=stat,
        notes=notes, meta={"fallback": True, "hotfix": True}
    )

# ========= Day22 route-rebind: /amateur/card → /amateur/card_fix =========
try:
    # 1) 기존 GET /amateur/card 라우트 제거
    def _remove_route(path: str, method: str = "GET"):
        kept = []
        for r in getattr(router, "routes", []):
            p = getattr(r, "path", None) or getattr(r, "path_format", None)
            methods = getattr(r, "methods", set())
            if not (p == path and method in methods):
                kept.append(r)
        router.routes = kept

    _remove_route("/amateur/card", "GET")

    # 2) 새 alias 등록: /amateur/card → amateur_card_fix
    async def _amateur_card_alias(level: str, player_id: str, name: Optional[str] = None, pos: Optional[str] = None):
        return await amateur_card_fix(level=level, player_id=player_id, name=name, pos=pos)

    router.add_api_route(
        "/amateur/card",
        _amateur_card_alias,
        methods=["GET"],
        response_model=AmateurCard,
        name="amateur_card"
    )
except Exception:
    pass




# ========= Day23: 계약 비교 도구 확장 =========
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

class ContractYear(BaseModel):
    year: int
    salary: float
    proj_war: float

class ContractInput(BaseModel):
    player_id: str
    contract: List[ContractYear]
    notes: Optional[str] = None

class ContractCompareQuery(BaseModel):
    dollar_per_war: float = Field(9_000_000, gt=0)
    discount_rate: float = Field(0.08, ge=0.0)
    items: List[ContractInput]

class ContractEval(BaseModel):
    player_id: str
    table: List[Dict[str, float]]
    totals: Dict[str, float]
    roi: float
    dollars_per_war_realized: float

class ContractCompareResponse(BaseModel):
    items: List[ContractEval]
    ranking_by_npv_surplus: List[str]

def _npv(vals: List[float], r: float) -> float:
    v = 0.0
    for i, x in enumerate(vals, start=1):
        v += x / ((1+r)**(i-1))
    return round(v, 2)

@router.post("/contracts/compare", response_model=ContractCompareResponse)
async def contracts_compare(q: ContractCompareQuery):
    out: List[ContractEval] = []
    for it in q.items:
        rows = []
        salary_series, value_series = [], []
        war_sum = 0.0
        for cy in it.contract:
            value = cy.proj_war * q.dollar_per_war
            surplus = value - cy.salary
            rows.append({
                "year": cy.year,
                "salary": float(cy.salary),
                "proj_war": float(cy.proj_war),
                "value": round(value, 2),
                "surplus": round(surplus, 2),
            })
            salary_series.append(float(cy.salary))
            value_series.append(float(value))
            war_sum += float(cy.proj_war)
        tot_salary  = round(sum(salary_series), 2)
        tot_value   = round(sum(value_series), 2)
        tot_surplus = round(tot_value - tot_salary, 2)
        npv_salary  = _npv(salary_series, q.discount_rate)
        npv_value   = _npv(value_series, q.discount_rate)
        npv_surplus = round(npv_value - npv_salary, 2)
        roi = round((tot_value / tot_salary) if tot_salary > 0 else 0.0, 3)
        dpw_real = round(tot_salary / max(1e-9, war_sum), 2)
        out.append(ContractEval(
            player_id=it.player_id, table=rows, roi=roi,
            totals={
                "salary": tot_salary, "value": tot_value, "surplus": tot_surplus,
                "npv_salary": npv_salary, "npv_value": npv_value, "npv_surplus": npv_surplus
            },
            dollars_per_war_realized=dpw_real
        ))
    ranking = [e.player_id for e in sorted(out, key=lambda x: x.totals["npv_surplus"], reverse=True)]
    return ContractCompareResponse(items=out, ranking_by_npv_surplus=ranking)

# ========= Day24: CBA/룰 QA 요약 =========
from pydantic import BaseModel
from typing import List, Dict, Optional

class CbaQAQuery(BaseModel):
    topic: str
    questions: List[str] = []
    locale: Optional[str] = "ko"

class CbaAnswer(BaseModel):
    q: str
    a: str
    refs: List[str] = []

class CbaQAResponse(BaseModel):
    topic: str
    answers: List[CbaAnswer]
    disclaimers: List[str]
    refs_index: Dict[str, str]

_CBA_REFS = {
    "ARB_SERVICE_TIME": "조정자격 서비스 타임(3.000년+, Super Two 예외).",
    "OPTION_YEARS": "마이너 옵션 3년 기본, 소진 시 DFA/웨이버 고려.",
    "IL_FA_RULES": "IL/리햅/복귀와 옵션·페이롤 영향.",
    "RULE5_DRAFT": "Rule 5 자격/보호/지명 및 로스터 유지 의무.",
}

def _cba_answer_stub(q: str, loc: str) -> CbaAnswer:
    ql = q.strip().lower()
    if "option" in ql or "옵션" in ql:
        a = "옵션은 보통 3년이며, 소진 시 DFA/웨이버 고려가 필요합니다."
        refs = ["OPTION_YEARS"]
    elif "arb" in ql or "연봉조정" in ql:
        a = "연봉조정은 통상 서비스타임 3.000년 이후 적용, Super Two 예외 존재."
        refs = ["ARB_SERVICE_TIME"]
    elif "rule 5" in ql or "룰5" in ql:
        a = "Rule 5 보호 미충족 시 타팀 지명 가능, 일정 기간 로스터 유지 의무."
        refs = ["RULE5_DRAFT"]
    elif "il" in ql or "부상자" in ql:
        a = "IL 등재·복귀 규정은 옵션/페이롤에 영향, 장기 IL은 추가 요건 존재."
        refs = ["IL_FA_RULES"]
    else:
        a = "정확한 답변을 위해 사례(일자/서비스타임/로스터 상태)가 필요합니다."
        refs = []
    return CbaAnswer(q=q, a=a, refs=refs)

@router.post("/cba/qa", response_model=CbaQAResponse)
async def cba_qa(q: CbaQAQuery):
    loc = (q.locale or "ko").lower()
    ans = [_cba_answer_stub(x, loc) for x in q.questions]
    disc = ["요약 스텁입니다. 실제 적용 전 CBA 원문/구단 정책으로 교차검증하십시오."]
    return CbaQAResponse(topic=q.topic, answers=ans, refs_index=_CBA_REFS, disclaimers=disc)

# ========= Day25: 워치리스트 & 알람(기초) =========
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

# 인메모리 저장소 (실운영은 Redis/DB로 교체)
_WATCHLIST: Dict[str, List[str]] = {}           # team -> [player_id]
_ALERT_RULES: Dict[str, List[Dict[str, Any]]] = {}  # team -> [{metric, op, threshold}...]

class WatchlistUpsert(BaseModel):
    team: str
    player_ids: List[str] = Field(default_factory=list)

class WatchlistResp(BaseModel):
    team: str
    players: List[str]

@router.post("/ops/watchlist/set", response_model=WatchlistResp)
async def watchlist_set(q: WatchlistUpsert):
    _WATCHLIST[q.team] = list(dict.fromkeys(q.player_ids))
    return WatchlistResp(team=q.team, players=_WATCHLIST[q.team])

@router.post("/ops/watchlist/add", response_model=WatchlistResp)
async def watchlist_add(q: WatchlistUpsert):
    cur = _WATCHLIST.setdefault(q.team, [])
    for pid in q.player_ids:
        if pid not in cur: cur.append(pid)
    return WatchlistResp(team=q.team, players=cur)

@router.get("/ops/watchlist/get", response_model=WatchlistResp)
async def watchlist_get(team: str):
    return WatchlistResp(team=team, players=_WATCHLIST.get(team, []))

# ----- 알람 규칙 -----
class AlertRule(BaseModel):
    metric: str           # e.g., "OPS_plus", "ERA_plus", "injury_flag"
    op: str               # "gt" | "lt" | "eq"
    threshold: float = 0.0

class AlertUpsert(BaseModel):
    team: str
    rules: List[AlertRule] = Field(default_factory=list)

class AlertEvalReq(BaseModel):
    team: str
    season: int = 2025
    # 간단 테스트용: 임의 지표를 직접 주입(실전: 실데이터 fetch)
    metrics_overrides: Dict[str, Dict[str, float]] = Field(default_factory=dict)  # pid -> {metric: value}

class AlertHit(BaseModel):
    player_id: str
    metric: str
    op: str
    value: float
    threshold: float

class AlertEvalResp(BaseModel):
    team: str
    hits: List[AlertHit]
    evaluated: int

@router.post("/ops/alerts/set_rules", response_model=Dict[str, Any])
async def alerts_set_rules(q: AlertUpsert):
    _ALERT_RULES[q.team] = [r.model_dump() for r in q.rules]
    return {"team": q.team, "rules": _ALERT_RULES[q.team], "ok": True}

def _op_ok(op: str, v: float, t: float) -> bool:
    if op == "gt": return v > t
    if op == "lt": return v < t
    if op == "eq": return abs(v - t) < 1e-9
    return False

@router.post("/ops/alerts/evaluate", response_model=AlertEvalResp)
async def alerts_evaluate(q: AlertEvalReq):
    team = q.team
    pids = _WATCHLIST.get(team, [])
    rules = _ALERT_RULES.get(team, [])
    hits: List[AlertHit] = []
    for pid in pids:
        # 1) 메트릭 소스: overrides > 간단 생성값(스텁) > 0.0
        src = q.metrics_overrides.get(pid) or {}
        # 스텁 생성: 우리 시스템의 결정론 난수 기반
        if not src:
            s = _seed_from_str(f"alert:{team}:{pid}:{q.season}")
            # 타자 가정: OPS+ 80~150, 투수 ERA+ 80~140
            src = {
                "OPS_plus": round(80 + 70 * _stable_rng01(s+1), 1),
                "ERA_plus": round(80 + 60 * _stable_rng01(s+2), 1),
                "injury_flag": 1.0 if _stable_rng01(s+3) > 0.92 else 0.0
            }
        # 2) 규칙 평가
        for r in rules:
            m = r["metric"]; op = r["op"]; th = float(r["threshold"])
            v = float(src.get(m, 0.0))
            if _op_ok(op, v, th):
                hits.append(AlertHit(player_id=pid, metric=m, op=op, value=v, threshold=th))
    return AlertEvalResp(team=team, hits=hits, evaluated=len(pids))

# ========= Day26: 멀티시즌 시나리오 플래너(베이스) =========
from pydantic import BaseModel, Field
from typing import List, Dict, Optional

# 가정: 1 WAR ≈ 1 승. (후속 단계에서 피타고라스/일정/파크/부상 반영 예정)

class ScenarioYearDelta(BaseModel):
    year: int
    delta_war: float = 0.0      # 이 해의 순증 WAR (영입/트레이드/이탈/부상 포함)
    delta_salary: float = 0.0   # 이 해의 급여 변화(+$ 지출)

class ScenarioChange(BaseModel):
    tag: str                     # 식별자 (예: "Sign:1B-LHH", "Trade:A↔B")
    years: List[ScenarioYearDelta]

class ScenarioPlanQuery(BaseModel):
    team: str
    horizon_start: int = 2025
    horizon_years: int = 3
    base_wins: Dict[int, float] = Field(default_factory=dict)   # 연도별 베이스 승수(모수: 로스터 현상 유지)
    base_payroll: Dict[int, float] = Field(default_factory=dict)# 연도별 베이스 페이롤(USD)
    changes: List[ScenarioChange] = Field(default_factory=list)
    dollar_per_war: float = 9_000_000.0
    discount_rate: float = 0.08
    budget_cap: Optional[float] = None    # 있으면 초과시 경고

class ScenarioYearResult(BaseModel):
    year: int
    wins: float
    payroll: float
    delta_war_total: float
    delta_salary_total: float

class ScenarioEval(BaseModel):
    team: str
    years: List[ScenarioYearResult]
    totals: Dict[str, float]    # npv_salary, sum_delta_war, avg_wins 등
    notes: List[str] = []

def _npv_series_generic(vals: List[float], r: float) -> float:
    v = 0.0
    for i, x in enumerate(vals, start=0):  # t0 현재
        v += x / ((1+r)**i)
    return round(v, 2)

@router.post("/ops/scenario/plan", response_model=ScenarioEval)
async def scenario_plan(q: ScenarioPlanQuery):
    start = q.horizon_start
    yrs = [start + i for i in range(q.horizon_years)]
    # 연도별 델타 집계
    agg_war: Dict[int, float] = {y: 0.0 for y in yrs}
    agg_salary: Dict[int, float] = {y: 0.0 for y in yrs}
    for ch in q.changes:
        for yd in ch.years:
            if yd.year in agg_war:
                agg_war[yd.year] += float(yd.delta_war)
                agg_salary[yd.year] += float(yd.delta_salary)
    # 결과 구성
    results: List[ScenarioYearResult] = []
    wins_arr, salary_arr = [], []
    notes: List[str] = []
    over_budget_years = []
    for y in yrs:
        base_w = float(q.base_wins.get(y, 81.0))
        base_p = float(q.base_payroll.get(y, 0.0))
        dw = agg_war[y]
        ds = agg_salary[y]
        wins = round(base_w + dw, 1)
        payroll = round(base_p + ds, 2)
        results.append(ScenarioYearResult(
            year=y, wins=wins, payroll=payroll,
            delta_war_total=round(dw,2), delta_salary_total=round(ds,2)
        ))
        wins_arr.append(wins)
        salary_arr.append(payroll)
        if q.budget_cap is not None and payroll > q.budget_cap:
            over_budget_years.append(y)
    if over_budget_years:
        notes.append(f"Budget cap exceeded in years: {over_budget_years}")

    # 가치 지표(참고): 델타 WAR * $/WAR vs 델타 급여
    value_arr = []
    for y in yrs:
        value_arr.append(agg_war[y] * q.dollar_per_war - agg_salary[y])
    npv_salary = _npv_series_generic(salary_arr, q.discount_rate)
    npv_value  = _npv_series_generic(value_arr, q.discount_rate)

    totals = {
        "sum_delta_war": round(sum(agg_war[y] for y in yrs), 2),
        "sum_delta_salary": round(sum(agg_salary[y] for y in yrs), 2),
        "avg_wins": round(sum(wins_arr)/len(wins_arr), 2),
        "npv_salary": npv_salary,
        "npv_value_from_changes": npv_value
    }
    return ScenarioEval(team=q.team, years=results, totals=totals, notes=notes)

# ========= Day27: 의사결정 로그 & 레드팀(간단) =========
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

_DECISION_LOG: List[Dict[str, Any]] = []  # append-only

class EvidenceItem(BaseModel):
    k: str
    v: str

class DecisionEntry(BaseModel):
    id: str
    actor: str
    action: str            # e.g., "TradeProposal","SignFA","Lineup","CallUp"
    context: Dict[str, Any] = Field(default_factory=dict)
    summary: str
    evidence: List[EvidenceItem] = Field(default_factory=list)
    created_at: Optional[str] = None   # ISO8601
    redteam: Optional[Dict[str, Any]] = None

class DecisionUpsert(BaseModel):
    id: str
    actor: str
    action: str
    summary: str
    context: Dict[str, Any] = Field(default_factory=dict)
    evidence: List[EvidenceItem] = Field(default_factory=list)

class DecisionListResp(BaseModel):
    total: int
    items: List[DecisionEntry]

def _redteam_score(entry: DecisionUpsert) -> Dict[str, Any]:
    # 간단 휴리스틱: 증거/수치/대안·리스크 체크
    txt = (entry.summary + " " + " ".join(f"{e.k}:{e.v}" for e in entry.evidence)).lower()
    score = 0
    findings = []
    # 증거 빈약
    if len(entry.evidence) < 2:
        score += 2; findings.append("evidence_low")
    # 수치 없음
    if not any(c in txt for c in ["ops+", "era+", "war", "npv", "roi", "elo", "woba"]):
        score += 2; findings.append("no_key_metrics")
    # 대안/리스크 언급?
    if not any(w in txt for w in ["risk", "variance", "downside", "injury", "alt", "대안", "리스크"]):
        score += 1; findings.append("risk_alt_missing")
    # 편향 키워드(플래툰 무시 등)
    if "platoon" in txt or "좌우" in txt:
        # 언급은 했지만 수치결여면 경고 유지
        pass
    level = "low" if score <= 1 else "medium" if score <= 3 else "high"
    return {"level": level, "score": score, "findings": findings}

@router.post("/ops/decision/log", response_model=DecisionEntry)
async def decision_log_add(d: DecisionUpsert):
    rt = _redteam_score(d)
    now = datetime.now(timezone.utc).isoformat()
    entry = DecisionEntry(
        id=d.id, actor=d.actor, action=d.action, summary=d.summary,
        context=d.context, evidence=d.evidence, created_at=now, redteam=rt
    )
    _DECISION_LOG.append(entry.model_dump())
    return entry

@router.get("/ops/decision/list", response_model=DecisionListResp)
async def decision_log_list(limit: int = 20, offset: int = 0, action: Optional[str] = None):
    items = _DECISION_LOG
    if action:
        items = [x for x in items if x.get("action")==action]
    sl = items[offset: offset+limit]
    return DecisionListResp(total=len(items), items=sl)

# 리그레션 가드(간단): 응답 필수 필드 & redteam 레벨 범위
@router.get("/ops/decision/_selfcheck")
async def decision_selfcheck():
    ok = True; errs = []
    try:
        e = DecisionUpsert(
            id="chk", actor="system", action="Check",
            summary="quick check", evidence=[EvidenceItem(k="OPS+", v="120")]
        )
        rt = _redteam_score(e)
        if rt["level"] not in ("low","medium","high"):
            ok=False; errs.append("rt_level")
    except Exception as ex:
        ok=False; errs.append(f"ex:{type(ex).__name__}")
    return {"ok": ok, "errors": errs}

# ========= Day28: Explainability · Evidence Summary =========
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

class ExplainEvidence(BaseModel):
    k: str
    v: Any
    weight: float = 1.0

class ExplainReq(BaseModel):
    title: str
    claims: List[str] = Field(default_factory=list)
    evidence: List[ExplainEvidence] = Field(default_factory=list)
    assumptions: List[str] = Field(default_factory=list)
    limitations: List[str] = Field(default_factory=list)
    next_actions: List[str] = Field(default_factory=list)

class ExplainResp(BaseModel):
    title: str
    summary: str
    confidence: float
    rationale: List[str]
    evidence_table: List[Dict[str, Any]]
    assumptions: List[str]
    limitations: List[str]
    next_actions: List[str]

def _confidence_from_evidence(evs: List[ExplainEvidence], assumptions: List[str]) -> float:
    if not evs:
        return 0.2
    w = sum(max(0.0, min(2.0, e.weight)) for e in evs)
    # 가정이 많을수록 신뢰도 하향
    penalty = min(0.5, 0.05 * len(assumptions))
    return round(max(0.05, min(0.99, 0.5 + 0.1*w - penalty)), 2)

@router.post("/explain/summarize", response_model=ExplainResp)
async def explain_summarize(q: ExplainReq):
    conf = _confidence_from_evidence(q.evidence, q.assumptions)
    # 간단한 요약 생성 규칙
    head = q.claims[0] if q.claims else q.title
    summary = f"{head} — backed by {len(q.evidence)} evidence item(s); assumptions={len(q.assumptions)}."
    rationale = []
    if q.evidence:
        rationale.append("Evidence coverage is adequate." if len(q.evidence) >= 2 else "Evidence is thin.")
    if q.assumptions:
        rationale.append("Assumptions present; validate before committing.")
    if not q.limitations:
        rationale.append("No explicit limitations provided.")

    ev_table = [{"metric": e.k, "value": e.v, "weight": e.weight} for e in q.evidence]
    return ExplainResp(
        title=q.title, summary=summary, confidence=conf,
        rationale=rationale, evidence_table=ev_table,
        assumptions=q.assumptions, limitations=q.limitations, next_actions=q.next_actions
    )

# 리그레션 가드: 필수 필드/범위 체크
@router.get("/explain/_selfcheck")
async def explain_selfcheck():
    try:
        r = await explain_summarize(ExplainReq(
            title="Check", claims=["OPS+ 상승이 팀 득점력 개선으로 이어질 것"],
            evidence=[ExplainEvidence(k="OPS+", v=128, weight=1.2),
                      ExplainEvidence(k="wOBA", v=0.355, weight=1.0)],
            assumptions=["라인업 건강 유지"],
            next_actions=["2주 후 업데이트 리포트"]
        ))
        ok = (0.05 <= r.confidence <= 0.99) and isinstance(r.evidence_table, list)
        return {"ok": ok, "confidence": r.confidence}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}
# ========= Day29: #13 멀티-이어 페이롤 시뮬 =========
from pydantic import BaseModel
from typing import List, Dict

class PayrollYear(BaseModel):
    year: int
    salary: float   # 연도별 총액

class PayrollSimQuery(BaseModel):
    team: str
    years: List[PayrollYear]
    cbt_line: float = 237_000_000   # 기본 CBT 기준 (2025)

class PayrollSimResult(BaseModel):
    team: str
    totals: Dict[int, Dict[str, float]]
    npv_total: float
    overages: List[int]

@router.post("/roster/payroll_sim", response_model=PayrollSimResult)
async def payroll_sim(q: PayrollSimQuery):
    r: Dict[int, Dict[str, float]] = {}
    npv_total = 0.0
    overages: List[int] = []
    discount = 0.08

    for i, y in enumerate(q.years):
        value = y.salary / ((1 + discount) ** i)
        r[y.year] = {
            "salary": y.salary,
            "cbt_line": q.cbt_line,
            "diff": y.salary - q.cbt_line,
            "npv": value,
        }
        npv_total += value
        if y.salary > q.cbt_line:
            overages.append(y.year)

    return PayrollSimResult(team=q.team, totals=r, npv_total=npv_total, overages=overages)

# ========= Day30: #14 ARB 예상 (v2, 기존 엔드포인트 보존) =========
from pydantic import BaseModel, Field
from typing import Optional, List, Dict

# 입력: 포지션/역할, 서비스타임, 성과지표(OPS+/ERA+), 직전 연봉(또는 베이스라인)
class ArbV2Query(BaseModel):
    player_id: str
    role: str = Field(..., description="batter|pitcher")
    last_season: int
    service_years: float = Field(..., description="예: 3.1, 4.7")
    prev_salary: Optional[float] = Field(None, description="직전 시즌 보수 (없으면 baseline 사용)")
    baseline_salary: float = Field(900000.0, description="리그 최소~1M 근처 베이스라인")
    ops_plus: Optional[float] = None   # 타자일 때 선택
    era_plus: Optional[float] = None   # 투수일 때 선택
    save_count: Optional[int] = None   # 마무리 프리미엄
    awards: Optional[List[str]] = Field(default_factory=list)  # 'AS','GG','SS','MVP','CY' 등

class ArbV2Resp(BaseModel):
    player_id: str
    role: str
    last_season: int
    service_years: float
    est_raise_pct: float
    est_salary: float
    drivers: Dict[str, float]
    notes: List[str] = []

def _arb_bracket_mult(svc: float) -> float:
    # 서비스 타임 브래킷(대략치): 3.x < 4.x < 5.x
    if svc < 4.0:   # Year 1 arb
        return 1.00
    if svc < 5.0:   # Year 2 arb
        return 1.15
    return 1.30     # Year 3 arb

def _perf_raise(role: str, ops_plus: Optional[float], era_plus: Optional[float]) -> float:
    # 성과 기반 가산 (대략치)
    if role.lower().startswith("bat"):
        v = (ops_plus or 100.0) - 100.0
        # 100 기준 초과분의 0.6%p 반영
        return max(-10.0, min(40.0, 0.6 * v))
    else:
        v = 100.0 - (era_plus or 100.0)  # ERA+는 높을수록 좋음 → 100-ERA+는 음수
        # ERA+ 120이면 v=-20 → +12%p 가산 느낌으로 뒤집기
        perf = (-(v)) * 0.6
        return max(-10.0, min(40.0, perf))

def _closer_premium(save_count: Optional[int]) -> float:
    if not save_count or save_count <= 0:
        return 0.0
    if save_count >= 35:
        return 6.0
    if save_count >= 20:
        return 3.0
    return 1.0

def _awards_premium(awards: List[str]) -> float:
    if not awards:
        return 0.0
    m = 0.0
    for a in awards:
        aa = a.upper()
        if aa in ("MVP","CY"):
            m += 5.0
        elif aa in ("AS","GG","SS","ROY","ASG"):
            m += 2.0
    return min(12.0, m)

def _floor_cap(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))

@router.post("/roster/arb_estimate_v2", response_model=ArbV2Resp)
async def arb_estimate_v2(q: ArbV2Query):
    base = q.prev_salary if q.prev_salary is not None else q.baseline_salary

    # 기본 인상률(역할별 평균치)
    base_raise = 25.0 if q.role.lower().startswith("bat") else 22.0

    # 성과 가산
    perf = _perf_raise(q.role, q.ops_plus, q.era_plus)

    # 서비스 타임 브래킷 계수
    bracket_mult = _arb_bracket_mult(q.service_years)

    # 특수 프리미엄
    closer = _closer_premium(q.save_count if q.role.lower().startswith("pit") else 0)
    awards = _awards_premium(q.awards or [])

    # 총 인상률
    raise_pct = (base_raise + perf + closer + awards) * bracket_mult
    raise_pct = _floor_cap(raise_pct, -5.0, 80.0)  # 안전벨트

    est_salary = round(base * (1.0 + raise_pct / 100.0))
    return ArbV2Resp(
        player_id=q.player_id, role=q.role, last_season=q.last_season,
        service_years=q.service_years, est_raise_pct=round(raise_pct,1),
        est_salary=float(est_salary),
        drivers={
            "base_raise": base_raise,
            "perf": round(perf,1),
            "bracket_mult": bracket_mult,
            "closer_premium": closer,
            "awards_premium": awards,
            "base_used": base
        },
        notes=["Heuristic v2. 실제 적용 전 구단 사례/패널티 구조와 교차검증 필요."]
    )

# ========= Day31: #15 계약 ROI/서플러스 ($/WAR·NPV) v2 =========
from pydantic import BaseModel, Field
from typing import List, Dict, Optional

class ContractYearV2(BaseModel):
    year: int
    salary: float
    proj_war: float

class ContractROIV2Query(BaseModel):
    contract: List[ContractYearV2]
    base_dollar_per_war: float = Field(9_000_000, description="기준 $/WAR")
    dpw_growth: float = Field(0.03, description="$/WAR 연 성장률 (예: 3%)")
    discount_rate: float = Field(0.08, description="NPV 할인율")
    war_confidence: float = Field(1.00, description="WAR 신뢰계수(0.8~1.2)")
    sensitivity_pct: float = Field(0.10, description="+/- 민감도 비율 (예: 0.10=±10%)")

class ContractROIV2Resp(BaseModel):
    table: List[Dict[str, float]]
    totals: Dict[str, float]
    sensitivity: Dict[str, Dict[str, float]]  # {"minus":{"npv_surplus":..}, "plus":{..}}

def _npv_series_v2(vals: List[float], r: float) -> float:
    acc = 0.0
    for i, v in enumerate(vals):
        acc += v / ((1.0 + r) ** i)
    return round(acc, 2)

@router.post("/roster/contract_roi_v2", response_model=ContractROIV2Resp)
async def contract_roi_v2(q: ContractROIV2Query):
    rows = []
    v_series, s_series, dpw_series = [], [], []
    # 본선 계산
    for i, cy in enumerate(q.contract):
        dpw_y = q.base_dollar_per_war * ((1.0 + q.dpw_growth) ** i)
        value = cy.proj_war * dpw_y * q.war_confidence
        surplus = value - cy.salary
        rows.append({
            "year": float(cy.year),
            "salary": float(cy.salary),
            "proj_war": float(cy.proj_war),
            "dpw_used": round(dpw_y, 2),
            "value": round(value, 2),
            "surplus": round(surplus, 2),
        })
        v_series.append(value); s_series.append(cy.salary); dpw_series.append(dpw_y)

    tot_salary = round(sum(s_series), 2)
    tot_value  = round(sum(v_series), 2)
    tot_surplus = round(tot_value - tot_salary, 2)
    npv_salary = _npv_series_v2(s_series, q.discount_rate)
    npv_value  = _npv_series_v2(v_series, q.discount_rate)
    npv_surplus = round(npv_value - npv_salary, 2)
    roi = round((tot_value / tot_salary) if tot_salary > 0 else 0.0, 3)

    # 민감도: $/WAR ± sensitivity_pct
    def _with_dpw_scale(scale: float) -> float:
        v_scaled = [x * scale for x in v_series]
        nv = _npv_series_v2(v_scaled, q.discount_rate)
        return round(nv - npv_salary, 2)

    minus = _with_dpw_scale(1.0 - q.sensitivity_pct)
    plus  = _with_dpw_scale(1.0 + q.sensitivity_pct)

    return ContractROIV2Resp(
        table=rows,
        totals={
            "salary": tot_salary, "value": tot_value, "surplus": tot_surplus,
            "npv_salary": npv_salary, "npv_value": npv_value, "npv_surplus": npv_surplus,
            "roi": roi
        },
        sensitivity={
            "minus": {"dpw_scale": 1.0 - q.sensitivity_pct, "npv_surplus": minus},
            "plus":  {"dpw_scale": 1.0 + q.sensitivity_pct, "npv_surplus": plus}
        }
    )

# ========= Day32: #16 포지션 대체 자원 추천 (v2) =========
from pydantic import BaseModel, Field
from typing import List, Optional, Dict

class ReplacementCandV2(BaseModel):
    player_id: str
    pos: str
    proj_war: float
    expected_cost: float
    bats: Optional[str] = None       # "L"|"R"|"S"
    platoon_tag: Optional[str] = None # "vsR"|"vsL"
    risk: Optional[float] = 0.0       # 0~1 (부상/성과 변동 위험도)

class ReplacementWeights(BaseModel):
    war: float = 1.0
    cost: float = 0.6
    fit: float = 0.4
    platoon: float = 0.3
    risk: float = 0.5

class ReplacementQueryV2(BaseModel):
    need_pos: str
    candidates: List[ReplacementCandV2]
    top_n: int = 5
    min_war: float = 0.5
    budget: Optional[float] = None
    prefer_bats: Optional[str] = None   # "L"|"R"
    platoon_need: Optional[str] = None  # "vsR"|"vsL"
    weights: ReplacementWeights = ReplacementWeights()

class ReplacementRankRow(BaseModel):
    player_id: str
    pos: str
    proj_war: float
    expected_cost: float
    score: float
    reasons: List[str]

class ReplacementResponseV2(BaseModel):
    pos: str
    ranked: List[ReplacementRankRow]

def _safe_min_max(vals: List[float]) -> Tuple[float, float]:
    if not vals:
        return 0.0, 1.0
    lo, hi = min(vals), max(vals)
    if abs(hi - lo) < 1e-9:
        hi = lo + 1.0
    return lo, hi

@router.post("/roster/replacement_suggestions_v2", response_model=ReplacementResponseV2)
async def replacement_suggestions_v2(q: ReplacementQueryV2):
    # 1) 포지션/최소 WAR 필터
    pool = [c for c in q.candidates if c.pos == q.need_pos and c.proj_war >= q.min_war]
    if not pool:
        return ReplacementResponseV2(pos=q.need_pos, ranked=[])

    # 2) 정규화 준비 (cost, war)
    costs = [c.expected_cost for c in pool]
    wars  = [c.proj_war      for c in pool]
    c_lo, c_hi = _safe_min_max(costs)
    w_lo, w_hi = _safe_min_max(wars)

    def z_cost(x: float) -> float:
        # 비용은 낮을수록 좋음 → 낮을수록 높은 점수로 바꾸기 위해 1 - 정규화
        z = (x - c_lo) / (c_hi - c_lo)
        return 1.0 - max(0.0, min(1.0, z))

    def z_war(x: float) -> float:
        z = (x - w_lo) / (w_hi - w_lo)
        return max(0.0, min(1.0, z))

    ranked: List[ReplacementRankRow] = []
    for c in pool:
        reasons: List[str] = []
        # WAR, COST 기여
        s_war  = z_war(c.proj_war) * q.weights.war
        s_cost = z_cost(c.expected_cost) * q.weights.cost
        # FIT: 타석손/팀 니즈
        s_fit = 0.0
        if q.prefer_bats and c.bats and q.prefer_bats.upper() == c.bats.upper():
            s_fit += 1.0 * q.weights.fit
            reasons.append(f"prefer_bats:{q.prefer_bats}")
        # PLATOON: vsR/vsL 요청과 태그 매칭
        s_platoon = 0.0
        if q.platoon_need and c.platoon_tag and q.platoon_need == c.platoon_tag:
            s_platoon += 1.0 * q.weights.platoon
            reasons.append(f"platoon:{q.platoon_need}")
        # RISK: 리스크 패널티(높을수록 감점)
        r = (c.risk or 0.0)
        r = max(0.0, min(1.0, r))
        s_risk = -(r * q.weights.risk)
        if r > 0:
            reasons.append(f"risk_penalty:{r:.2f}")
        # BUDGET 힌트
        if q.budget is not None and c.expected_cost > q.budget:
            reasons.append("over_budget")

        score = round(s_war + s_cost + s_fit + s_platoon + s_risk, 4)
        base_reasons = [f"war={c.proj_war}", f"$={int(c.expected_cost):,}"]
        reasons = base_reasons + reasons if reasons else base_reasons + ["baseline"]
        ranked.append(ReplacementRankRow(
            player_id=c.player_id, pos=c.pos, proj_war=c.proj_war,
            expected_cost=c.expected_cost, score=score, reasons=reasons
        ))

    ranked.sort(key=lambda x: x.score, reverse=True)
    return ReplacementResponseV2(pos=q.need_pos, ranked=ranked[: q.top_n])


# ========= Day33: #17 옵션/40-Man 관리 (v1, in-memory) =========
from pydantic import BaseModel, Field
from typing import List, Dict, Optional

# 간단 in-memory 저장소 (프로세스 생존 동안)
_ROSTER_DB: Dict[str, Dict[str, dict]] = {}  # team -> player_id -> rec

class RosterRec(BaseModel):
    player_id: str
    on40: bool = False
    option_years_used: int = 0        # 0~3
    rule5_protected: bool = False
    service_time: float = 0.0         # Y.xxx
    notes: Optional[str] = None

class FortyManSetQuery(BaseModel):
    team: str
    players: List[str]                 # 40-man에 올릴 ids (나머지는 on40=False로 둘지 유지할지? 유지)

class OptionUpdateQuery(BaseModel):
    team: str
    player_id: str
    delta: int = 0                     # +1, -1 등
    set_value: Optional[int] = None    # 명시 세팅 우선

class Rule5ProtectQuery(BaseModel):
    team: str
    player_id: str
    protect: bool = True

class BulkUpsertQuery(BaseModel):
    team: str
    items: List[RosterRec]

class RosterOverview(BaseModel):
    team: str
    counts: Dict[str, int]
    risks: Dict[str, List[str]]        # option_exhaust, rule5_risk
    table: List[RosterRec]

def _team_bucket(team: str) -> Dict[str, dict]:
    return _ROSTER_DB.setdefault(team, {})

@router.post("/roster/40man/set", response_model=RosterOverview)
async def roster_40man_set(q: FortyManSetQuery):
    b = _team_bucket(q.team)
    # 지정된 players는 on40=True로 세팅 (없던 선수면 기본 rec 생성)
    onset = set(q.players)
    for pid in onset:
        b.setdefault(pid, {"player_id": pid})
        b[pid]["on40"] = True
        b[pid].setdefault("option_years_used", 0)
        b[pid].setdefault("rule5_protected", False)
        b[pid].setdefault("service_time", 0.0)

    # 나머지는 건드리지 않음(명시적 제거 API 별도 제공 가능)
    return await roster_overview(q.team)

@router.post("/roster/option/update", response_model=RosterOverview)
async def roster_option_update(q: OptionUpdateQuery):
    b = _team_bucket(q.team)
    rec = b.setdefault(q.player_id, {"player_id": q.player_id, "on40": False,
                                     "option_years_used": 0, "rule5_protected": False, "service_time": 0.0})
    if q.set_value is not None:
        rec["option_years_used"] = max(0, min(3, int(q.set_value)))
    else:
        rec["option_years_used"] = max(0, min(3, int(rec.get("option_years_used", 0) + q.delta)))
    return await roster_overview(q.team)

@router.post("/roster/rule5/protect", response_model=RosterOverview)
async def roster_rule5_protect(q: Rule5ProtectQuery):
    b = _team_bucket(q.team)
    rec = b.setdefault(q.player_id, {"player_id": q.player_id, "on40": False,
                                     "option_years_used": 0, "rule5_protected": False, "service_time": 0.0})
    rec["rule5_protected"] = bool(q.protect)
    return await roster_overview(q.team)

@router.post("/roster/bulk_upsert", response_model=RosterOverview)
async def roster_bulk_upsert(q: BulkUpsertQuery):
    b = _team_bucket(q.team)
    for it in q.items:
        b[it.player_id] = it.dict()
    return await roster_overview(q.team)

@router.get("/roster/overview", response_model=RosterOverview)
async def roster_overview(team: str):
    b = _team_bucket(team)
    tbl = [RosterRec(**v) for v in b.values()]
    counts = {
        "players": len(tbl),
        "on40": sum(1 for r in tbl if r.on40),
        "protected": sum(1 for r in tbl if r.rule5_protected),
    }
    # 리스크 규칙: 옵션 3년 소진 임박(>=2), Rule5 미보호+on40=False
    option_exhaust = [r.player_id for r in tbl if r.option_years_used >= 2]
    rule5_risk = [r.player_id for r in tbl if (not r.rule5_protected) and (not r.on40)]
    risks = {"option_exhaust": option_exhaust, "rule5_risk": rule5_risk}
    return RosterOverview(team=team, counts=counts, risks=risks, table=tbl)

# 셀프체크
@router.get("/roster/_selfcheck", response_model=dict)
async def roster_selfcheck():
    return {"ok": True, "keys": list(_ROSTER_DB.keys())}

# ========= Day33.1: Roster 상태 스냅샷(자동 저장/복구) =========
import os, json, pathlib
from fastapi import Request

_SNAPSHOT_PATH = pathlib.Path("/workspaces/cogm-assistant/data/roster_state.json")

def _snapshot_load_into_memory():
    try:
        if _SNAPSHOT_PATH.exists() and _SNAPSHOT_PATH.stat().st_size > 0:
            data = json.loads(_SNAPSHOT_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                # 이전에 선언된 in-memory DB로 머지
                _ROSTER_DB.clear()
                _ROSTER_DB.update(data)
    except Exception:
        pass  # 복구 실패해도 서비스 계속

def _snapshot_save():
    try:
        _SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
        _SNAPSHOT_PATH.write_text(json.dumps(_ROSTER_DB, ensure_ascii=False), encoding="utf-8")
        return True
    except Exception:
        return False

# 서버 기동 시 1회 복구
_snapshot_load_into_memory()

# /roster/* POST 호출 후 자동 저장
# (autosave middleware removed; now saving inside handlers)

# 수동 덤프/리로드 엔드포인트
@router.get("/roster/_dump")
async def roster_dump():
    ok = _snapshot_save()
    return {"ok": ok, "path": str(_SNAPSHOT_PATH), "bytes": (_SNAPSHOT_PATH.stat().st_size if _SNAPSHOT_PATH.exists() else 0)}

@router.post("/roster/_reload")
async def roster_reload():
    _snapshot_load_into_memory()
    return {"ok": True, "keys": list(_ROSTER_DB.keys())}

# ========= Day34: #18 IL/복귀 일정 트래킹 (v1) =========
from pydantic import BaseModel
from typing import List, Dict, Optional
from datetime import datetime

# 별도 IL 저장소 (in-memory)
_IL_DB: Dict[str, Dict[str, dict]] = {}   # team -> player_id -> rec

class ILItem(BaseModel):
    player_id: str
    status: str                  # "IL10"|"IL15"|"IL60"|"DTD"|"Active"
    injury_type: Optional[str] = None
    start_date: Optional[str] = None       # "YYYY-MM-DD"
    est_return_date: Optional[str] = None  # "YYYY-MM-DD"
    notes: Optional[str] = None

class ILSetQuery(BaseModel):
    team: str
    items: List[ILItem]

class ILClearQuery(BaseModel):
    team: str
    player_id: str

class ILListResponse(BaseModel):
    team: str
    counts: Dict[str, int]
    items: List[ILItem]

def _il_bucket(team: str) -> Dict[str, dict]:
    return _IL_DB.setdefault(team, {})

# 스냅샷 로드/세이브 확장(IL 포함)
def _snapshot_load_into_memory():
    try:
        if _SNAPSHOT_PATH.exists() and _SNAPSHOT_PATH.stat().st_size > 0:
            data = json.loads(_SNAPSHOT_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                # 구(단일 dict=roster) 호환
                if "roster" in data or "il" in data:
                    _ROSTER_DB.clear()
                    _ROSTER_DB.update(data.get("roster", {}))
                    _IL_DB.clear()
                    _IL_DB.update(data.get("il", {}))
                else:
                    _ROSTER_DB.clear()
                    _ROSTER_DB.update(data)
    except Exception:
        pass

def _snapshot_save():
    try:
        _SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
        payload = {"roster": _ROSTER_DB, "il": _IL_DB}
        _SNAPSHOT_PATH.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        return True
    except Exception:
        return False

@router.post("/roster/il/set", response_model=ILListResponse)
async def il_set(q: ILSetQuery):
    b = _il_bucket(q.team)
    for it in q.items:
        b[it.player_id] = it.dict()
    _snapshot_save()
    return await il_list(q.team)

@router.post("/roster/il/clear", response_model=ILListResponse)
async def il_clear(q: ILClearQuery):
    b = _il_bucket(q.team)
    b.pop(q.player_id, None)
    _snapshot_save()
    return await il_list(q.team)

@router.get("/roster/il/list", response_model=ILListResponse)
async def il_list(team: str):
    b = _il_bucket(team)
    items = [ILItem(**v) for v in b.values()]
    counts = {
        "IL": sum(1 for x in items if x.status in {"IL10","IL15","IL60"}),
        "DTD": sum(1 for x in items if x.status == "DTD"),
        "Active": sum(1 for x in items if x.status == "Active"),
        "total": len(items),
    }
    return ILListResponse(team=team, counts=counts, items=items)

# 간단 요약 리포트
@router.get("/reports/il_summary", response_model=dict)
async def il_summary(team: str):
    b = _il_bucket(team)
    out = []
    today = datetime.utcnow().date().isoformat()
    for v in b.values():
        eta = v.get("est_return_date")
        out.append({
            "player_id": v["player_id"],
            "status": v.get("status"),
            "injury_type": v.get("injury_type"),
            "start_date": v.get("start_date"),
            "est_return_date": eta,
            "days_to_eta": (None if not eta else (datetime.fromisoformat(eta).date() - datetime.fromisoformat(today).date()).days)
        })
    return {"team": team, "today": today, "items": out, "counts": {"total": len(out)}}
# ========= Day35: 계약 비교 v2 / IL 연동 대체추천 =========
from pydantic import BaseModel
from typing import List, Dict, Optional

# ----- 계약 비교 v2 -----
class ContractYearV2(BaseModel):
    year: int
    salary: float
    proj_war: float
    age: Optional[int] = None
    def_: Optional[float] = None  # 수비 점수 등
    platoon: Optional[str] = None

class ContractItemV2(BaseModel):
    player_id: str
    contract: List[ContractYearV2]

class ContractCompareV2Query(BaseModel):
    team: Optional[str] = None
    budget_cap: Optional[float] = None
    priority_weights: Dict[str, float] = {}
    items: List[ContractItemV2]

class ContractCompareV2Response(BaseModel):
    items: List[Dict]
    ranking: List[str]
    budget_exceeded: List[str]

@router.post("/contracts/compare_v2", response_model=ContractCompareV2Response)
async def contracts_compare_v2(q: ContractCompareV2Query):
    out, ranking, over = [], [], []
    for it in q.items:
        salary_total = sum(y.salary for y in it.contract)
        war_total = sum(y.proj_war for y in it.contract)
        npv_value = sum(y.proj_war * q.priority_weights.get("war", 1.0) * 9_000_000 for y in it.contract)
        surplus = npv_value - salary_total
        score = surplus
        # 추가 가중치 (age, defense, platoon)
        if any(y.age for y in it.contract):
            avg_age = sum(y.age for y in it.contract if y.age) / len(it.contract)
            score += (35 - avg_age) * q.priority_weights.get("age", 0.0)
        if any(y.def_ for y in it.contract):
            score += sum(y.def_ for y in it.contract if y.def_) * q.priority_weights.get("defense", 0.0)
        out.append({
            "player_id": it.player_id,
            "salary_total": salary_total,
            "war_total": war_total,
            "npv_value": npv_value,
            "surplus": surplus,
            "score": score
        })
        if q.budget_cap and salary_total > q.budget_cap:
            over.append(it.player_id)
    ranking = [x["player_id"] for x in sorted(out, key=lambda r: r["score"], reverse=True)]
    return ContractCompareV2Response(items=out, ranking=ranking, budget_exceeded=over)

# ----- IL 연동 대체 추천 -----
class ILReplacementResponse(BaseModel):
    team: str
    replacements: List[Dict]

@router.post("/reports/il_replacements", response_model=ILReplacementResponse)
async def il_replacements(q: Dict):
    team = q.get("team")
    # 스텁: 실제 구현시 roster/il/list & roster/replacement_suggestions_v2 연동
    replacements = [
        {"injured":"p2","need_pos":"1B","suggested":"A","reason":"WAR 2.4 vs cost 8M"},
        {"injured":"p4","need_pos":"CF","suggested":"B","reason":"Glove+Speed, budget fit"}
    ]
    return ILReplacementResponse(team=team, replacements=replacements)

# ========= Day35: IL 연동 대체추천 v2 (real hook) =========
from typing import Dict as _D  # 안전 import

class ILReplacementV2Response(BaseModel):
    team: str
    replacements: List[Dict]

@router.post("/reports/il_replacements_v2", response_model=ILReplacementV2Response)
async def il_replacements_v2(q: _D):
    team = q.get("team")

    # 1) 현재 IL/DTD 목록
    il = await il_list(team)
    injured = [it.player_id for it in il.items if it.status in {"IL10","IL15","IL60","DTD"}]

    # 2) 간단 need 매핑 (실전은 포지션 테이블/뎁스차트로 대체)
    need_map = {"p2": "1B", "p4": "CF"}

    # 3) 후보 풀 (임시). 운영시엔 내부 DB/스카우팅 연동
    candidate_pool = {
        "1B": [
            {"player_id":"A","pos":"1B","proj_war":2.4,"expected_cost":8_000_000,"bats":"L","platoon_tag":"vsR","risk":0.10},
            {"player_id":"B","pos":"1B","proj_war":1.1,"expected_cost":1_500_000,"bats":"R","platoon_tag":"vsL","risk":0.05},
        ],
        "CF": [
            {"player_id":"B","pos":"CF","proj_war":1.6,"expected_cost":3_500_000,"bats":"R","platoon_tag":"vsL","risk":0.12},
            {"player_id":"C","pos":"CF","proj_war":1.3,"expected_cost":2_100_000,"bats":"L","platoon_tag":"vsR","risk":0.08},
        ],
    }

    # 4) v2 스코어러 (weights 고정)
    weights = {"war":1.0,"cost":0.6,"fit":0.4,"platoon":0.3,"risk":0.5}
    def _score(c):
        base = c["proj_war"]*weights["war"] - (c["expected_cost"]/10_000_000.0)*weights["cost"]
        fit = (1.0 if c.get("bats")=="L" else 0.6)*weights["fit"]
        platoon = (1.0 if c.get("platoon_tag")=="vsR" else 0.7)*weights["platoon"]
        risk_pen = (1.0 - c.get("risk",0.0))*weights["risk"]
        return round(base + fit + platoon + risk_pen, 4)

    repls = []
    for pid in injured:
        need_pos = need_map.get(pid, "UTL")
        cands = candidate_pool.get(need_pos, [])
        if not cands:
            continue
        top = sorted(cands, key=_score, reverse=True)[0]
        repls.append({
            "injured": pid,
            "need_pos": need_pos,
            "suggested": top["player_id"],
            "reason": f"WAR {top['proj_war']} vs ${top['expected_cost']:,}",
            "score": _score(top),
        })
    return ILReplacementV2Response(team=team, replacements=repls)

# --- IL 대체추천 v2 (injured override + 파일 fallback) ---
@router.post("/reports/il_replacements_v2", response_model=ILReplacementV2Response)
async def il_replacements_v2(q: _D):
    team = q.get("team")
    override_inj = q.get("injured") or []

    il = await il_list(team)
    injured = override_inj or [it.player_id for it in il.items if it.status in {"IL10","IL15","IL60","DTD"}]

    # 파일 fallback (data/il_state.json -> {team: [{player_id,...}, ...]})
    if not injured:
        try:
            import json, pathlib
            p = pathlib.Path("data/il_state.json")
            if p.exists():
                d = json.loads(p.read_text())
                injured = [x.get("player_id") for x in d.get(team, []) if x.get("player_id")]
        except Exception:
            pass

    need_map = {"p2":"1B", "p4":"CF"}  # 임시 매핑
    candidate_pool = {
        "1B": [
            {"player_id":"A","pos":"1B","proj_war":2.4,"expected_cost":8_000_000,"bats":"L","platoon_tag":"vsR","risk":0.10},
            {"player_id":"B","pos":"1B","proj_war":1.1,"expected_cost":1_500_000,"bats":"R","platoon_tag":"vsL","risk":0.05},
        ],
        "CF": [
            {"player_id":"B","pos":"CF","proj_war":1.6,"expected_cost":3_500_000,"bats":"R","platoon_tag":"vsL","risk":0.12},
            {"player_id":"C","pos":"CF","proj_war":1.3,"expected_cost":2_100_000,"bats":"L","platoon_tag":"vsR","risk":0.08},
        ],
    }
    weights = {"war":1.0,"cost":0.6,"fit":0.4,"platoon":0.3,"risk":0.5}
    def _score(c):
        base = c["proj_war"]*weights["war"] - (c["expected_cost"]/10_000_000.0)*weights["cost"]
        fit = (1.0 if c.get("bats")=="L" else 0.6)*weights["fit"]
        platoon = (1.0 if c.get("platoon_tag")=="vsR" else 0.7)*weights["platoon"]
        risk_pen = (1.0 - c.get("risk",0.0))*weights["risk"]
        return round(base + fit + platoon + risk_pen, 4)

    repls = []
    for pid in injured:
        need_pos = need_map.get(pid, "UTL")
        cands = candidate_pool.get(need_pos, [])
        if not cands:
            continue
        top = sorted(cands, key=_score, reverse=True)[0]
        repls.append({"injured": pid, "need_pos": need_pos,
                      "suggested": top["player_id"],
                      "reason": f"WAR {top['proj_war']} vs ${top['expected_cost']:,}",
                      "score": _score(top)})
    return ILReplacementV2Response(team=team, replacements=repls)

# ========= Day35 hotfix: IL replacement v3 (self-contained) =========
from typing import Dict as _D, List as _L, Any as _A

class ILReplacementV3Response(BaseModel):
    team: str
    replacements: _L[dict]

@router.post("/reports/il_replacements_v3", response_model=ILReplacementV3Response)
async def il_replacements_v3(q: _D):
    team = q.get("team")
    override_inj = q.get("injured") or []

    # 1) IL/DTD 수집
    il = await il_list(team)
    injured = override_inj or [it.player_id for it in il.items if it.status in {"IL10","IL15","IL60","DTD"}]

    # 2) 필요 포지션 매핑(임시). 운영시엔 내부 뎁스차트로 대체
    need_map = {"p2": "1B", "p4": "CF"}

    # 3) 후보 풀(임시). 운영시엔 DB/스카우팅 연동
    candidate_pool = {
        "1B": [
            {"player_id":"A","pos":"1B","proj_war":2.4,"expected_cost":8_000_000,"bats":"L","platoon_tag":"vsR","risk":0.10},
            {"player_id":"B","pos":"1B","proj_war":1.1,"expected_cost":1_500_000,"bats":"R","platoon_tag":"vsL","risk":0.05},
        ],
        "CF": [
            {"player_id":"B","pos":"CF","proj_war":1.6,"expected_cost":3_500_000,"bats":"R","platoon_tag":"vsL","risk":0.12},
            {"player_id":"C","pos":"CF","proj_war":1.3,"expected_cost":2_100_000,"bats":"L","platoon_tag":"vsR","risk":0.08},
        ],
    }

    # 4) 고정 스코어러
    weights = {"war":1.0,"cost":0.6,"fit":0.4,"platoon":0.3,"risk":0.5}
    def _score(c: _A) -> float:
        base = c["proj_war"]*weights["war"] - (c["expected_cost"]/10_000_000.0)*weights["cost"]
        fit = (1.0 if c.get("bats")=="L" else 0.6)*weights["fit"]
        platoon = (1.0 if c.get("platoon_tag")=="vsR" else 0.7)*weights["platoon"]
        risk_pen = (1.0 - c.get("risk",0.0))*weights["risk"]
        return round(base + fit + platoon + risk_pen, 3)

    repls = []
    for pid in injured:
        need_pos = need_map.get(pid, "UTL")
        cands = candidate_pool.get(need_pos, [])
        if not cands:
            continue
        top = sorted(cands, key=_score, reverse=True)[0]
        repls.append({
            "injured": pid,
            "need_pos": need_pos,
            "suggested": top["player_id"],
            "reason": f"WAR {top['proj_war']} vs ${top['expected_cost']:,}",
            "score": _score(top),
        })
    return ILReplacementV3Response(team=team, replacements=repls)
# === Co-GM append start (Week6 selfcheck) ===
from typing import Dict, List
from fastapi import Request

# Week6에서 반드시 살아있어야 하는 라우트들
_WEEK6_REQUIRED: List[str] = [
    "/schedule/analyze",          # Day36
    "/lineup/optimize",           # Day37
    "/ump/euz_bias",              # Day38
    "/parks/daily_factors",       # Day39
    "/travel/fatigue_index",      # Day40
    "/forecast/win_prob",         # Day41 (기본)
    "/forecast/win_prob_weather", # Day41 (날씨 가중 버전)
]

@router.get("/ops/week6_selfcheck")
async def week6_selfcheck(request: Request) -> Dict:
    # 앱에 실제로 등록된 라우트 집합
    route_paths = []
    for r in request.app.router.routes:
        p = getattr(r, "path", None) or getattr(r, "path_format", None)
        if isinstance(p, str):
            route_paths.append(p)

    check = {p: (p in route_paths) for p in _WEEK6_REQUIRED}
    missing = [p for p, ok in check.items() if not ok]
    found   = [p for p, ok in check.items() if ok]

    return {
        "ok": len(missing) == 0,
        "required_count": len(_WEEK6_REQUIRED),
        "found": sorted(found),
        "missing": sorted(missing),
        "hint": "missing이 비어야 Week6 라우트 배선이 완료된 것",
    }
# === Co-GM append end (Week6 selfcheck) ===
# === Co-GM append start (Week7 selfcheck) ===
from typing import Dict, List
from fastapi import Request

# Week7에서 반드시 살아있어야 하는 라우트들
_WEEK7_REQUIRED: List[str] = [
    "/contracts/compare_v2",
    "/reports/il_replacements_v2",
    "/reports/il_replacements_v3",
    "/ops/watchlist/set",
    "/ops/alerts/set_rules",
    "/ops/alerts/evaluate",
    "/ops/scenario/plan",
    "/ops/decision/log",
    "/ops/decision/list",
    "/explain/summarize",
    "/explain/_selfcheck",
]

# 스모크 커맨드(한 번에 복붙 실행용)
_WEEK7_SMOKE_SH = r"""#!/usr/bin/env bash
set -euo pipefail

echo "== Week7 SMOKE START =="

# contracts/compare_v2
curl -s -X POST http://127.0.0.1:8000/contracts/compare_v2 \
  -H "Content-Type: application/json" \
  -d '{
    "team":"SEA","budget_cap":25000000,
    "priority_weights":{"war":1.0,"age":0.4,"platoon":0.3,"defense":0.5},
    "items":[
      {"player_id":"X","contract":[{"year":2025,"salary":12000000,"proj_war":2.8,"age":27,"def":5}]},
      {"player_id":"Y","contract":[{"year":2025,"salary":10000000,"proj_war":2.1,"age":31,"def":1}]}
    ]
  }' | python -m json.tool

# IL replacements v2 (injured override 없을 때는 빈 배열일 수 있음)
curl -s -X POST http://127.0.0.1:8000/reports/il_replacements_v2 \
  -H "Content-Type: application/json" \
  -d '{"team":"SEA"}' | python -m json.tool || true

# IL replacements v3 (v3는 내부 IL 상태 기반 추천)
curl -s -X POST http://127.0.0.1:8000/reports/il_replacements_v3 \
  -H "Content-Type: application/json" \
  -d '{"team":"SEA"}' | python -m json.tool

# watchlist / alerts
curl -s -X POST http://127.0.0.1:8000/ops/watchlist/set \
  -H "Content-Type: application/json" \
  -d '{"team":"SEA","player_ids":["p1","p2","p3"]}' | python -m json.tool

curl -s -X POST http://127.0.0.1:8000/ops/alerts/set_rules \
  -H "Content-Type: application/json" \
  -d '{"team":"SEA","rules":[
        {"metric":"OPS_plus","op":"gt","threshold":130},
        {"metric":"ERA_plus","op":"lt","threshold":90},
        {"metric":"injury_flag","op":"eq","threshold":1}
      ]}' | python -m json.tool

curl -s -X POST http://127.0.0.1:8000/ops/alerts/evaluate \
  -H "Content-Type: application/json" \
  -d '{"team":"SEA","season":2025}' | python -m json.tool

# scenario planner
curl -s -X POST http://127.0.0.1:8000/ops/scenario/plan \
  -H "Content-Type: application/json" \
  -d '{
    "team":"SEA",
    "horizon_start":2025, "horizon_years":3,
    "base_wins": {"2025":86, "2026":84, "2027":83},
    "base_payroll": {"2025":180000000, "2026":185000000, "2027":190000000},
    "budget_cap": 195000000,
    "changes": [
      {"tag":"Sign:1B-LHH","years":[
        {"year":2025,"delta_war":2.4,"delta_salary":12000000},
        {"year":2026,"delta_war":2.1,"delta_salary":13000000}
      ]},
      {"tag":"Trade:CF glove","years":[
        {"year":2025,"delta_war":1.2,"delta_salary":2000000},
        {"year":2026,"delta_war":-1.5,"delta_salary":0}
      ]}
    ]
  }' | python -m json.tool

# decision log & list
curl -s -X POST http://127.0.0.1:8000/ops/decision/log \
  -H "Content-Type: application/json" \
  -d '{
    "id":"D27-001",
    "actor":"FO",
    "action":"TradeProposal",
    "summary":"Acquire CF glove; WAR +1.2 expected; risk hamstring; alt internal CF.",
    "context":{"team":"SEA","target":"CF-DEF"},
    "evidence":[{"k":"WAR_gain","v":"1.2"},{"k":"OPS+","v":"105"}]
  }' | python -m json.tool

curl -s "http://127.0.0.1:8000/ops/decision/list?limit=5" | python -m json.tool

# explainable AI
curl -s -X POST http://127.0.0.1:8000/explain/summarize \
  -H "Content-Type: application/json" \
  -d '{
    "title":"CF 수비 보강 트레이드",
    "claims":["수비 개선으로 실점 감소, Pythag 승률 +1.5p 기대"],
    "evidence":[
      {"k":"DRS","v":12,   "weight":1.3},
      {"k":"UZR/150","v":8.5,"weight":1.2},
      {"k":"SprintSpeed","v":29.1,"weight":0.9}
    ],
    "assumptions":["햄스트링 이슈 무재발"],
    "limitations":["샘플 400PA로 소표본"],
    "next_actions":["의무팀 메디컬 세컨드 오피니언","2주후 필드스카우트 업데이트"]
  }' | python -m json.tool

curl -s "http://127.0.0.1:8000/explain/_selfcheck" | python -m json.tool

echo "== Week7 SMOKE END =="
"""

@router.get("/ops/week7_selfcheck")
async def week7_selfcheck(request: Request) -> Dict:
    # 앱에 실제로 등록된 라우트 집합
    paths: List[str] = []
    for r in request.app.router.routes:
        p = getattr(r, "path", None) or getattr(r, "path_format", None)
        if isinstance(p, str):
            paths.append(p)

    check = {p: (p in paths) for p in _WEEK7_REQUIRED}
    missing = [p for p, ok in check.items() if not ok]
    found   = [p for p, ok in check.items() if ok]

    return {
        "ok": len(missing) == 0,
        "required_count": len(_WEEK7_REQUIRED),
        "found": sorted(found),
        "missing": sorted(missing),
        "smoke_sh": _WEEK7_SMOKE_SH,
        "hint": "ok=true && missing=[] 이어야 Week7 배선 완료. smoke_sh를 저장/실행해 실제 응답까지 확인."
    }
# === Co-GM append end (Week7 selfcheck) ===
# ========= Week7: ops/week7_selfcheck (append-only) =========
from typing import List, Dict, Any

def _wk7_required_paths() -> List[str]:
    # Week7 범위에서 점검할 필수 라우트
    return [
        "/contracts/compare_v2",
        "/reports/il_replacements_v2",
        "/reports/il_replacements_v3",
        "/ops/watchlist/set",
        "/ops/alerts/set_rules",
        "/ops/alerts/evaluate",
        "/ops/scenario/plan",
        "/ops/decision/log",
        "/ops/decision/list",
        "/explain/summarize",
        "/explain/_selfcheck",
    ]

def _wk7_smoke_sh() -> str:
    # 실제 호출까지 포함된 원샷 스모크 스크립트
    return """#!/usr/bin/env bash
set -euo pipefail

echo "== Week7 SMOKE START =="

# contracts/compare_v2
curl -s -X POST http://127.0.0.1:8000/contracts/compare_v2 \
  -H "Content-Type: application/json" \
  -d '{
    "team":"SEA","budget_cap":25000000,
    "priority_weights":{"war":1.0,"age":0.4,"platoon":0.3,"defense":0.5},
    "items":[
      {"player_id":"X","contract":[{"year":2025,"salary":12000000,"proj_war":2.8,"age":27,"def":5}]},
      {"player_id":"Y","contract":[{"year":2025,"salary":10000000,"proj_war":2.1,"age":31,"def":1}]}
    ]
  }' | python -m json.tool

# IL replacements v2 (injured override 없을 때는 빈 배열일 수 있음)
curl -s -X POST http://127.0.0.1:8000/reports/il_replacements_v2 \
  -H "Content-Type: application/json" \
  -d '{"team":"SEA"}' | python -m json.tool || true

# IL replacements v3 (v3는 내부 IL 상태 기반 추천)
curl -s -X POST http://127.0.0.1:8000/reports/il_replacements_v3 \
  -H "Content-Type: application/json" \
  -d '{"team":"SEA"}' | python -m json.tool

# watchlist / alerts
curl -s -X POST http://127.0.0.1:8000/ops/watchlist/set \
  -H "Content-Type: application/json" \
  -d '{"team":"SEA","player_ids":["p1","p2","p3"]}' | python -m json.tool

curl -s -X POST http://127.0.0.1:8000/ops/alerts/set_rules \
  -H "Content-Type: application/json" \
  -d '{"team":"SEA","rules":[
        {"metric":"OPS_plus","op":"gt","threshold":130},
        {"metric":"ERA_plus","op":"lt","threshold":90},
        {"metric":"injury_flag","op":"eq","threshold":1}
      ]}' | python -m json.tool

curl -s -X POST http://127.0.0.1:8000/ops/alerts/evaluate \
  -H "Content-Type: application/json" \
  -d '{"team":"SEA","season":2025}' | python -m json.tool

# scenario planner
curl -s -X POST http://127.0.0.1:8000/ops/scenario/plan \
  -H "Content-Type: application/json" \
  -d '{
    "team":"SEA",
    "horizon_start":2025, "horizon_years":3,
    "base_wins": {"2025":86, "2026":84, "2027":83},
    "base_payroll": {"2025":180000000, "2026":185000000, "2027":190000000},
    "budget_cap": 195000000,
    "changes": [
      {"tag":"Sign:1B-LHH","years":[
        {"year":2025,"delta_war":2.4,"delta_salary":12000000},
        {"year":2026,"delta_war":2.1,"delta_salary":13000000}
      ]},
      {"tag":"Trade:CF glove","years":[
        {"year":2025,"delta_war":1.2,"delta_salary":2000000},
        {"year":2026,"delta_war":-1.5,"delta_salary":0}
      ]}
    ]
  }' | python -m json.tool

# decision log & list
curl -s -X POST http://127.0.0.1:8000/ops/decision/log \
  -H "Content-Type: application/json" \
  -d '{
    "id":"D27-001",
    "actor":"FO",
    "action":"TradeProposal",
    "summary":"Acquire CF glove; WAR +1.2 expected; risk hamstring; alt internal CF.",
    "context":{"team":"SEA","target":"CF-DEF"},
    "evidence":[{"k":"WAR_gain","v":"1.2"},{"k":"OPS+","v":"105"}]
  }' | python -m json.tool

curl -s "http://127.0.0.1:8000/ops/decision/list?limit=5" | python -m json.tool

# explainable AI
curl -s -X POST http://127.0.0.1:8000/explain/summarize \
  -H "Content-Type: application/json" \
  -d '{
    "title":"CF 수비 보강 트레이드",
    "claims":["수비 개선으로 실점 감소, Pythag 승률 +1.5p 기대"],
    "evidence":[
      {"k":"DRS","v":12,   "weight":1.3},
      {"k":"UZR/150","v":8.5,"weight":1.2},
      {"k":"SprintSpeed","v":29.1,"weight":0.9}
    ],
    "assumptions":["햄스트링 이슈 무재발"],
    "limitations":["샘플 400PA로 소표본"],
    "next_actions":["의무팀 메디컬 세컨드 오피니언","2주후 필드스카우트 업데이트"]
  }' | python -m json.tool

curl -s "http://127.0.0.1:8000/explain/_selfcheck" | python -m json.tool

echo "== Week7 SMOKE END =="
"""

@router.get("/ops/week7_selfcheck")
async def week7_selfcheck() -> Dict[str, Any]:
    required = _wk7_required_paths()
    # app 전역이 있으면 app.router까지 스캔, 없으면 router만 스캔
    found_set = set()
    def _scan_routes(rtlist):
        for r in rtlist:
            p = getattr(r, "path", None)
            if p in required:
                found_set.add(p)
    _scan_routes(router.routes)
    app_obj = globals().get("app")
    if app_obj is not None:
        _scan_routes(getattr(app_obj.router, "routes", []))

    missing = [p for p in required if p not in found_set]
    return {
        "ok": len(missing) == 0,
        "required_count": len(required),
        "found": sorted(list(found_set)),
        "missing": missing,
        "smoke_sh": _wk7_smoke_sh(),
        "hint": "ok=true && missing=[] 이어야 Week7 검증 완료. smoke_sh를 저장/실행해 실제 응답까지 확인하세요."
    }
