from app.lib.name_resolver import resolve_names
from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
import streamlit as st
from app.core.data import load_cards, load_statcast
from app.core.paths import summary, report  # reserved for future

def _load_any(paths: list[str]) -> pd.DataFrame | None:
    for p in paths:
        if "*" in p:
            base = Path(".")
            for match in base.rglob(Path(p).name):
                df = _load_file(match)
                if df is not None and not df.empty:
                    return df
        else:
            f = Path(p)
            if not f.is_absolute():
                f = Path(".") / p
            df = _load_file(f)
            if df is not None and not df.empty:
                return df
    return None

def _load_file(fpath: Path) -> pd.DataFrame | None:
    try:
        if not fpath.exists() or fpath.stat().st_size == 0:
            return None
        if fpath.suffix == ".csv":
            return pd.read_csv(fpath)
        if fpath.suffix == ".parquet":
            return pd.read_parquet(fpath)
        if fpath.suffix == ".json":
            with open(fpath, "r", encoding="utf-8") as f:
                return pd.json_normalize(json.load(f))
        if fpath.suffix == ".txt":
            txt = fpath.read_text(encoding="utf-8", errors="ignore")
            return pd.DataFrame({"content": [txt]})
    except Exception:
        return None
    return None

def render_generic_table(title: str, candidates: list[str], note: str = ""):
    st.subheader(title)
    if not candidates:
        st.info("소스 후보 경로가 비어 있습니다.")
        return
    df = _load_any(candidates)
    if df is None or df.empty:
        st.warning("데이터가 비어있습니다. (후보 소스: {})".format(", ".join(candidates)))
        if note:
            st.caption(note)
        return
    st.dataframe(df, use_container_width=True)
    if note:
        st.caption(note)

def render_player_single():
    st.subheader("단일 선수 분석")
    cards = load_cards()
    if cards.empty:
        st.error("player_cards_all.parquet이 비어있습니다.")
        return
    name = st.text_input("선수 이름(부분 검색):", "")
    df = cards
    if name:
        _n = name.lower()
        cols = [c for c in cards.columns if "name" in c.lower()] or list(cards.columns[:1])
        mask = False
        for c in cols:
            mask = mask | cards[c].astype(str).str.lower().str.contains(_n, na=False)
        df = cards[mask]
    st.write(f"Rows: {len(df)}")
    st.dataframe(df.head(500), use_container_width=True)

def render_compare():
    render_generic_table(
        "2–3인 비교(스파이더/버터플라이)",
        ["output/summaries/player_compare_rows.csv", "output/player_compare_rows.csv"],
        note="‘player_compare_rows’가 존재하면 표/차트로 확장 가능",
    )

def render_trend_3y():
    st.subheader("3년 트렌드: wRC+, BABIP, EV, BB/K")
    st.caption("cards/statcast에서 핵심 컬럼 미리보기")
    cards = load_cards()
    stat = load_statcast()
    if cards.empty and stat.empty:
        st.warning("cards/statcast 모두 비어있습니다.")
        return
    if not cards.empty:
        keep = [c for c in cards.columns if any(k in c.lower() for k in ["wrc","babip","bb","k","ev","season","player","name","id"])]
        st.dataframe(cards[keep].head(1000), use_container_width=True)
    if not stat.empty:
        keep = [c for c in stat.columns if any(k in c.lower() for k in ["launch","ev","bb","k","season","player","name","id"])]
        st.dataframe(stat[keep].head(1000), use_container_width=True)
