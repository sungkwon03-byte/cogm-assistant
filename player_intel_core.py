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
from typing import List, Dict, Optional
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
from typing import List, Dict, Optional
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
from typing import List, Dict, Optional

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
from typing import List, Dict, Optional

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
from typing import List, Dict, Optional
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
from typing import List, Dict, Optional
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
from typing import List, Dict

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
