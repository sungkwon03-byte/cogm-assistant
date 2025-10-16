import os, streamlit as st
from app.core.data import ok
st.set_page_config(page_title="Co-GM Suite (MAX)", layout="wide")
st.title("Co-GM Suite (MAX)")
st.write({
  "player_cards_all.parquet": ok("output/player_cards_all.parquet"),
  "statcast_ultra_full_clean.parquet": ok("output/statcast_ultra_full_clean.parquet"),
  "id_map.csv": ok("output/id_map.csv"),
})
st.success("모든 기능 페이지를 좌측에서 확인. 데이터/컬럼 유무에 따라 자동 축소 없이 ‘최대치’ 계산.")
