from app.lib.name_resolver import resolve_names
import streamlit as st
from app.core.data import ensure_views, table_exists, df
st.title("League / Teams Aggregates")
ensure_views()
target = "v_cards" if table_exists("v_cards") else ("cards" if table_exists("cards") else None)
if not target:
    st.warning("No cards"); st.stop()
st.dataframe(df(f"""
SELECT season, team, COUNT(DISTINCT player_id) players, AVG(COALESCE(woba,0)) avg_woba
FROM {target} GROUP BY 1,2 ORDER BY season DESC, avg_woba DESC NULLS LAST LIMIT 1500
"""))
