from app.lib.name_resolver import resolve_names
import streamlit as st
from app.core.data import ensure_views, table_exists, df
st.title("Count Tendencies")
ensure_views()
if table_exists("count_tendencies"):
    st.dataframe(df("SELECT * FROM count_tendencies ORDER BY season DESC, pitch_ct DESC LIMIT 1000"))
elif table_exists("v_stat"):
    st.dataframe(df("""
    SELECT season, player_id, balls, strikes, COUNT(*) pitch_ct
    FROM v_stat GROUP BY 1,2,3,4 ORDER BY season DESC, pitch_ct DESC LIMIT 1000
    """))
else:
    st.warning("No count data.")
