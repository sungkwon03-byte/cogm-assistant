import streamlit as st
import pandas as pd
from pathlib import Path

st.set_page_config(page_title="Co-GM Assistant", layout="wide")
st.title("⚾ Co-GM Assistant — Streamlit Portfolio Version")
st.caption("실데이터 기반 리포트 · 시각화 · 트레이드 점수 엔진")

import os, urllib.request, tarfile, io
OUT=Path("output")
BUNDLE_URL=os.environ.get("BUNDLE_URL","")
if (not OUT.exists() or not any(OUT.glob("*"))) and BUNDLE_URL:
    try:
        print("[bootstrap] fetching:", BUNDLE_URL)
        data=urllib.request.urlopen(BUNDLE_URL, timeout=60).read()
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tf:
            tf.extractall(path=OUT)
        print("[bootstrap] extracted to output/")
    except Exception as e:
        print("[bootstrap] failed:", e)
DATA_FILES = list(OUT.glob("*.csv"))

if not OUT.exists() or len(DATA_FILES) == 0:
    st.error("❌ output/ 폴더가 비어 있습니다. 실데이터를 넣어주세요.")
    st.stop()

tab1, tab2, tab3 = st.tabs(["📊 Player Data", "📈 Visuals", "🔁 Trade Simulator"])

# -------------------
# Tab 1: Player Data
# -------------------
with tab1:
    st.header("Player Cards & Stats")
    csvs = [f.name for f in DATA_FILES if "player" in f.name.lower()]
    if csvs:
        file_sel = st.selectbox("파일 선택", sorted(csvs))
        df = pd.read_csv(OUT / file_sel)
        st.dataframe(df.head(100), use_container_width=True)
    else:
        st.warning("player 관련 CSV 파일을 찾을 수 없습니다. (예: player_cards.csv)")

# -------------------
# Tab 2: Visuals
# -------------------
with tab2:
    st.header("Performance Visualizations")
    pngs = sorted(list(OUT.glob("*.png")))
    pdfs = sorted(list(OUT.glob("*.pdf")))
    cols = st.columns(2)
    shown = 0
    for img in pngs:
        cols[shown % 2].image(str(img), caption=img.name, use_container_width=True)
        shown += 1
    if shown == 0 and pdfs:
        st.info("PNG가 없으면 PDF를 다운로드로 제공합니다.")
        for pdf in pdfs[:4]:
            st.download_button(label=f"📄 {pdf.name}", file_name=pdf.name, data=open(pdf, "rb"))

# -------------------
# Tab 3: Trade Simulator
# -------------------
with tab3:
    st.header("Trade Value Comparison")
    trade_file = next((f for f in DATA_FILES if "trade" in f.name.lower()), None)
    if trade_file:
        trade_df = pd.read_csv(trade_file)
        # 기대 컬럼: player_name, trade_value
        if not {"player_name","trade_value"}.issubset(trade_df.columns):
            st.error("trade_value.csv에 'player_name', 'trade_value' 컬럼이 필요합니다.")
        else:
            player_names = trade_df["player_name"].dropna().astype(str).unique().tolist()
            col1, col2 = st.columns(2)
            with col1:
                p1 = st.selectbox("선수 1", player_names, index=0 if player_names else None, key="p1")
            with col2:
                p2 = st.selectbox("선수 2", player_names, index=1 if len(player_names)>1 else 0, key="p2")
            if st.button("🔁 Compare Trade Value"):
                v1 = trade_df.loc[trade_df["player_name"] == p1, "trade_value"].astype(float).mean()
                v2 = trade_df.loc[trade_df["player_name"] == p2, "trade_value"].astype(float).mean()
                if pd.notna(v1) and pd.notna(v2):
                    diff = v1 - v2
                    st.metric(label=f"{p1} vs {p2}", value=f"{diff:+.2f}")
                else:
                    st.warning("선택한 선수의 트레이드 값이 없습니다.")
    else:
        st.info("trade_value.csv 파일을 output 폴더에 추가하면 시뮬레이터가 활성화됩니다.")

st.success("✅ 모든 모듈 로드 완료. Streamlit Cloud에서 바로 실행 가능합니다.")
