from app.lib.name_resolver import resolve_names
import streamlit as st
from app.core.data import ensure_views, table_exists, df
st.title("H) League / Teams")
ensure_views()
if not table_exists("player_season_metrics"):
    st.info("요약 뷰(player_season_metrics) 생성 시 더 풍부한 차트 제공.")
else:
    st.dataframe(df("SELECT * FROM player_season_metrics ORDER BY season DESC LIMIT 200"))
