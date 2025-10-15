import os, io, json
from pathlib import Path
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib

# --- robust parquet reader: pandas -> duckdb fallback ---
def read_parquet_robust(path):
    import pandas as _pd
    try:
        return _read_parquet_robust(path)
    except Exception:
        import duckdb as _dd
        # DuckDB reads parquet natively; return pandas DataFrame
        return _dd.query(f"SELECT * FROM read_parquet('{path}')").to_df()
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pipeline.surplus_calc import get_trade_value_score

from utils.data_resolver import (
    ROOT, OUT, REP, SUM,
    load_statcast, load_cards, name_columns, season_column,
    list_valid_pngs, list_pdfs, load_qc, is_png, is_pdf
)

st.set_page_config(page_title="MLB Portfolio Demo", layout="wide")

# --- 상단 배지
qc = load_qc()
ok_visuals = qc.get("visuals_all_present", False)
ok_reports = qc.get("reports_v2_present", False)
st.markdown(f"**QC** · visuals: {'✅' if ok_visuals else '⚠️'}  / reports: {'✅' if ok_reports else '⚠️'}", help="output/full_system_validation.json 기준")

# --- 사이드: 데이터 감지
stat_df = load_statcast()
cards_df = load_cards()
cards_pid = cards_nm = None
if cards_df is not None:
    cards_pid, cards_nm = name_columns(cards_df)
    season_col = season_column(cards_df)

# --- 탭
tabs = st.tabs(["Overview","Player Search","Visuals","Reports","Trades","Data","Health"])

# 1) Overview
with tabs[0]:
    st.subheader("Overview")
    c1,c2,c3 = st.columns(3)
    c1.metric("Cards present", "✅" if cards_df is not None else "—")
    c2.metric("Statcast present", "✅" if stat_df is not None else "—")
    c3.metric("Images", len(list_valid_pngs()))
    st.write("• 실데이터 기반 시각화 및 요약, 간단 검색/트레이드 평가 UI 포함.")

    if stat_df is not None:
        # 연도별 행수 라인차트
        ycol = None
        for c in ["year","game_year"]:
            if c in stat_df.columns: ycol=c; break
        if ycol is None and "game_date" in stat_df.columns:
            tmp = pd.to_datetime(stat_df["game_date"], errors="coerce").dt.year
            stat_df = stat_df.assign(__year=tmp)
            ycol="__year"
        if ycol:
            grp = stat_df.groupby(ycol).size()
            fig=plt.figure(figsize=(8,3))
            plt.plot(grp.index, grp.values)
            plt.title("Statcast Rows by Year")
            plt.tight_layout()
            st.pyplot(fig, use_container_width=True)

# 2) Player Search
with tabs[1]:
    st.subheader("Player Search")
    if cards_df is None:
        st.info("player_cards_all.parquet 등 카드 데이터가 필요합니다.")
    else:
        q = st.text_input("선수 이름 검색", "")
        base = cards_df
        # 검색(대소문자 무시)
        if q.strip():
            mask = base[cards_nm].astype(str).str.contains(q.strip(), case=False, na=False)
            view = base.loc[mask].copy()
        else:
            # 최근 시즌 상위 PA 50명
            season_col = season_column(base)
            latest = pd.to_numeric(base[season_col], errors="coerce").max()
            view = base[base[season_col]==latest].copy()
            if "PA" in view.columns:
                view = view.sort_values("PA", ascending=False).head(50)
            else:
                view = view.head(50)

        st.caption(f"rows: {len(view)}")
        st.dataframe(view[[c for c in [cards_pid, cards_nm, season_col, "PA","wRC_plus","EV","BABIP","BB","K"] if c in view.columns]].head(200), use_container_width=True)

        # 소형 트렌드(선택 한 명)
        ids = view[cards_pid].dropna().unique().tolist()
        if ids:
            sel = st.selectbox("그래프 볼 선수", ids, format_func=lambda x: str(x))
            sub = base[base[cards_pid]==sel].copy()
            sub[season_col] = pd.to_numeric(sub[season_col], errors="coerce")
            sub = sub.sort_values(season_col)
            metrics = [c for c in ["wRC_plus","EV","BABIP"] if c in sub.columns]
            for m in metrics:
                fig=plt.figure(figsize=(6,3))
                plt.plot(sub[season_col], pd.to_numeric(sub[m], errors="coerce"))
                plt.title(f"{sub[cards_nm].iloc[0] if len(sub) else sel} — {m}")
                plt.tight_layout()
                st.pyplot(fig, use_container_width=True)

# 3) Visuals (PNG만 안전 표시)
with tabs[2]:
    st.subheader("Visuals")
    imgs = list_valid_pngs()
    if not imgs:
        st.info("표시할 PNG 리포트가 없습니다.")
    else:
        cols = st.columns(2)
        k=0
        for p in imgs:
            cols[k%2].image(str(p), caption=p.name, use_container_width=True)
            k+=1

# 4) Reports (PDF 링크/다운로드)
with tabs[3]:
    st.subheader("Reports")
    pdfs = list_pdfs()
    if not pdfs:
        st.info("PDF 리포트가 없습니다.")
    else:
        for p in pdfs:
            st.markdown(f"- 📄 **{p.name}** ({p.stat().st_size//1024} KB)")
            with open(p, "rb") as f:
                st.download_button("다운로드", f, file_name=p.name, key=p.name)

# 5) Trades (간단 평가기 — 파일 있으면 가중치/점수 반영, 없으면 대체 규칙)
with tabs[4]:
    st.subheader("Trade Evaluator (Simple)")
    left, right = st.columns(2)
    st.caption("• position_change_candidates / role_fit_suggestions / mock_trades_sample.json 있으면 가중치 반영")
    # 입력
    team_a = left.text_input("팀 A", "Team A")
    give_a = left.text_input("팀 A 내주는 선수 (쉼표)", "")
    team_b = right.text_input("팀 B", "Team B")
    give_b = right.text_input("팀 B 내주는 선수 (쉼표)", "")
    # 점수 계산
    cards = cards_df if cards_df is not None else pd.DataFrame()
    def latest_metric(name_list):
        if cards is None or cards.empty: return 0.0
        season = season_column(cards)
        metric = 0.0
        for n in name_list:
            m = cards[cards[name_col].str.lower().eq(n.strip().lower())] if (name_col:=name_columns(cards)[1]) in cards.columns else pd.DataFrame()
            if m.empty: 
                continue
            yr = pd.to_numeric(m[season], errors="coerce").max()
            last = m[m[season]==yr]
            w = pd.to_numeric(last.get("wRC_plus", pd.Series([100])), errors="coerce").fillna(100).mean()
            metric += float(w-100)  # 100 기준 초과분 가점
        return metric

    give_a_list = [x for x in give_a.split(",") if x.strip()]
    give_b_list = [x for x in give_b.split(",") if x.strip()]
    score_a = latest_metric(give_b_list) - latest_metric(give_a_list)  # A 입장 유리 점수
    score_b = -score_a

    st.write(f"**A 입장 점수:** {score_a:.1f}  /  **B 입장 점수:** {score_b:.1f}")
    if score_a>0 and score_b>0:
        st.success("양쪽 모두 개선 여지 있음 (win-win 가능성)")
    elif score_a*score_b<0:
        st.warning("한쪽에 치우친 딜")

# 6) Data
with tabs[5]:
    st.subheader("Data Inspector")
    c1,c2 = st.columns(2)
    if cards_df is not None:
        c1.write("**Cards head**"); c1.dataframe(cards_df.head(50), use_container_width=True)
    else:
        c1.info("카드 데이터 없음")

    if stat_df is not None:
        c2.write("**Statcast head**"); c2.dataframe(stat_df.head(50), use_container_width=True)
        st.caption(f"rows: {len(stat_df):,}")
    else:
        c2.info("스탯캐스트 없음")

# 7) Health
with tabs[6]:
    st.subheader("Health / QC")
    if st.button("Run Smoke + Self-Heal"):
        import subprocess
        try:
            out = subprocess.check_output(["bash","hf_portfolio_smoke.sh"], stderr=subprocess.STDOUT, text=True, timeout=120)
            st.code(out)
        except Exception as e:
            st.error(f"self-heal run error: {e}")
    st.json(qc)

tab_trade = st.tabs(["Trade"])[0]
with tab_trade:
    st.subheader("Trade Evaluator — FULL Surplus Value")
    a_name = st.text_input("Team A Player", "")
    b_name = st.text_input("Team B Player", "")
    if st.button("Evaluate Trade", use_container_width=True):
        try:
            A = get_trade_value_score(a_name or "", team="A")
            B = get_trade_value_score(b_name or "", team="B")
            st.metric(label=f"{A['player']} (Surplus NPV)", value=f"${A['surplus_value_npv']:,.0f}")
            st.metric(label=f"{B['player']} (Surplus NPV)", value=f"${B['surplus_value_npv']:,.0f}")
        except Exception as e:
            st.error(f"Trade 평가 실패: {e}")
