from app.lib.name_resolver import resolve_names
import streamlit as st
from app.core.data import ensure_views, table_exists, df
st.title("Season Dashboard")
ensure_views()
if table_exists("player_season"):
    st.dataframe(df("SELECT * FROM player_season ORDER BY season DESC, n DESC LIMIT 1000"))
elif table_exists("v_cards"):
    st.dataframe(df("""
    SELECT season, COUNT(DISTINCT player_id) AS players, AVG(COALESCE(woba,0)) AS avg_woba
    FROM v_cards GROUP BY 1 ORDER BY 1 DESC LIMIT 100
    """))
else:
    st.warning("No season data.")
