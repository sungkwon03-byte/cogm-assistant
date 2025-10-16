from app.lib.name_resolver import resolve_names
import streamlit as st
from app.core.data import ensure_views, table_exists, df
st.title("Zone Heat / Repeat")
ensure_views()
if table_exists("zone_heat"):
    st.dataframe(df("SELECT * FROM zone_heat ORDER BY season DESC, n DESC LIMIT 2000"))
elif table_exists("v_stat"):
    st.dataframe(df("""
    SELECT season, player_id, zone, COUNT(*) n
    FROM v_stat WHERE zone IS NOT NULL
    GROUP BY 1,2,3 ORDER BY season DESC, n DESC LIMIT 2000
    """))
else:
    st.warning("No zone data.")
