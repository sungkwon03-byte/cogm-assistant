from app.lib.name_resolver import resolve_names
import streamlit as st
from app.core.data import ensure_views, table_exists, df, cols
st.title("Platoon & Splits")
ensure_views()
if table_exists("platoon_split"):
    st.dataframe(df("SELECT * FROM platoon_split ORDER BY season DESC, n DESC LIMIT 1000"))
elif table_exists("v_stat") and "stand" in [c.lower() for c in cols("v_stat")]:
    st.dataframe(df("""
    SELECT season, player_id, stand, AVG(COALESCE(ev,0)) avg_ev, AVG(COALESCE(la,0)) avg_la, COUNT(*) n
    FROM v_stat GROUP BY 1,2,3 ORDER BY season DESC, n DESC LIMIT 1000
    """))
else:
    st.warning("No platoon columns.")
