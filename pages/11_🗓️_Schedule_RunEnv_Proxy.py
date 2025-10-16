from app.lib.name_resolver import resolve_names
import streamlit as st
from app.core.data import ensure_views, table_exists, df
st.title("Schedule / Run Environment (Proxy by EV)")
ensure_views()
if not table_exists("v_stat"):
    st.warning("No statcast."); st.stop()
st.dataframe(df("""
SELECT season, AVG(COALESCE(ev,0)) AS avg_ev, COUNT(*) n
FROM v_stat GROUP BY 1 ORDER BY season DESC
"""))
