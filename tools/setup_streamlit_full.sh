#!/usr/bin/env bash
set -euo pipefail

echo "[INIT] 🧩 Streamlit Cloud 전용 완전형 구성 시작"

# repo 루트로 이동
ROOT="$(git rev-parse --show-toplevel 2>/dev/null || echo "$PWD")"
cd "$ROOT"

# git 작성자 정보 없을 때 기본값 설정 (Codespaces 대비)
git config user.email >/dev/null 2>&1 || git config user.email "you@example.com"
git config user.name  >/dev/null 2>&1 || git config user.name "Your Name"

# 0) 필수 파일/폴더
mkdir -p output
touch requirements.txt

# 1) requirements.txt 정리 (Streamlit/Pillow 고정)
echo "[STEP 1] requirements.txt 업데이트..."
# streamlit==1.39.0 보장
if grep -Eqi '^streamlit(==|>=|<=)' requirements.txt; then
  sed -i 's/^streamlit.*/streamlit==1.39.0/' requirements.txt
else
  echo 'streamlit==1.39.0' >> requirements.txt
fi
# pillow==10.3.0 보장
if grep -Eqi '^pillow(==|>=|<=)' requirements.txt; then
  sed -i 's/^pillow.*/pillow==10.3.0/' requirements.txt
else
  echo 'pillow==10.3.0' >> requirements.txt
fi
# pandas 없는 경우 기본 추가
grep -Eqi '^pandas(==|>=|<=)' requirements.txt || echo 'pandas==2.2.2' >> requirements.txt
# matplotlib 없는 경우 기본 추가
grep -Eqi '^matplotlib(==|>=|<=)' requirements.txt || echo 'matplotlib==3.8.4' >> requirements.txt
echo "✅ requirements.txt 완료"

# 2) Streamlit 전용 앱 작성 (streamlit_app.py)
echo "[STEP 2] Streamlit 앱 생성..."
cat > streamlit_app.py <<'PY'
import streamlit as st
import pandas as pd
from pathlib import Path

st.set_page_config(page_title="Co-GM Assistant", layout="wide")
st.title("⚾ Co-GM Assistant — Streamlit Portfolio Version")
st.caption("실데이터 기반 리포트 · 시각화 · 트레이드 점수 엔진")

OUT = Path("output")
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
PY
echo "✅ streamlit_app.py 생성 완료"

# 3) 커밋 & 브랜치 생성/푸시
echo "[STEP 3] git 커밋/브랜치/푸시..."
git add -A
git commit --allow-empty -m "build: Streamlit Cloud full portfolio (app + reqs + autodetect output/)"
# streamlit-demo 브랜치 강제 세팅
if git show-ref --verify --quiet refs/heads/streamlit-demo; then
  git branch -M streamlit-demo
else
  git checkout -b streamlit-demo
fi
git push -u origin streamlit-demo -f
echo "✅ streamlit-demo 브랜치 푸시 완료"

# 4) 최종 안내
cat <<'MSG'

🎉 준비 완료!

📦 Streamlit Cloud 설정 값
  • Repository: <your repo> (예: sungkwon03-byte/cogm-assistant)
  • Branch: streamlit-demo
  • Main file path: streamlit_app.py
  • Build: pip install -r requirements.txt

📁 데이터 경로
  • 실데이터는 repo의 output/ 아래에 두면 자동 인식
  • 예시 파일명:
      - player_cards.csv, player_cards_all.csv
      - trend_3yr_cards.pdf, weakness_heatmap.png 등
      - trade_value.csv (컬럼: player_name, trade_value)

✅ 이 상태로 바로 Deploy 하면 동작합니다.
MSG
