# -*- coding: utf-8 -*-
# 공용 팀코드 표준화 유틸
from typing import Iterable
import pandas as pd

TEAM_ALIAS = {
    # Retrosheet/Chadwick 계열 3문자 약칭 -> MLB 표준(또는 우리 내부 표준)
    "LAN":"LAD", "NYA":"NYY", "NYN":"NYM", "SFN":"SFG", "SDN":"SDP", "SLN":"STL",
    "CHN":"CHC", "CHA":"CWS", "TBA":"TBR", "FLA":"MIA", "ANA":"LAA",
    # 역사적/이전 코드
    "CAL":"LAA", "MLN":"MIL", "MON":"WSN", "KCA":"KCR", "WSH":"WSN",
}

def norm_team(value: str) -> str:
    if value is None: return None
    t = str(value).upper()
    return TEAM_ALIAS.get(t, t)

def norm_team_series(s: Iterable) -> pd.Series:
    ser = pd.Series(s, copy=False).astype(str).str.upper()
    return ser.map(lambda x: TEAM_ALIAS.get(x, x))
