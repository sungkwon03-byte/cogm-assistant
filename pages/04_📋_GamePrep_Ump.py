from app.lib.name_resolver import resolve_names
import streamlit as st
from app.core.data import ensure_views, table_exists, df
st.title("D) Game Prep / Umpire EUZ")
ensure_views()
if not table_exists("statcast"):
    st.warning("statcast not available"); st.stop()
st.write("샘플: 최근 시즌 상위 타구속 평균")
st.dataframe(df("""
SELECT season, AVG(ev) AS avg_ev, COUNT(*) AS n
FROM statcast
GROUP BY 1 ORDER BY 1 DESC LIMIT 20
"""))
