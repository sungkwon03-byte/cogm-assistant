from app.lib.name_resolver import resolve_names
import streamlit as st
from app.core.data import ensure_views, table_exists, df
st.title("B) Roster / Payroll / Surplus")
ensure_views()
if not table_exists("cards"):
    st.warning("cards not available")
    st.stop()
st.write("샘플: 시즌별 핵심지표 요약")
st.dataframe(df("""
SELECT season, COUNT(DISTINCT player_id) AS players, AVG(woba) AS avg_woba
FROM cards
GROUP BY 1
ORDER BY 1 DESC
LIMIT 50
"""))
