from app.lib.name_resolver import resolve_names
import streamlit as st
from app.core.data import ensure_views, table_exists, df
st.title("Value (wOBA baseline)")
ensure_views()
if table_exists("value_simple"):
    st.dataframe(df("SELECT * FROM value_simple ORDER BY season DESC LIMIT 2000"))
elif table_exists("v_cards"):
    st.dataframe(df("""
    SELECT player_id, season, AVG(COALESCE(woba,0)) AS value
    FROM v_cards GROUP BY 1,2 ORDER BY season DESC LIMIT 2000
    """))
else:
    st.warning("No cards/wOBA.")
