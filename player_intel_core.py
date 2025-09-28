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

