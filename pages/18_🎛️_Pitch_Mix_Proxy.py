from app.lib.name_resolver import resolve_names
import streamlit as st
from app.core.data import ensure_views, table_exists, df, cols
st.title("Pitch Mix (proxy)")
ensure_views()
if not table_exists("v_stat"):
    st.warning("No statcast"); st.stop()
if "pitch_type" not in [c.lower() for c in cols("v_stat")]:
    st.warning("pitch_type column not present."); st.stop()
st.dataframe(df("""
SELECT season, pitch_type, COUNT(*) n, AVG(COALESCE(ev,0)) avg_ev
FROM v_stat GROUP BY 1,2 ORDER BY season DESC, n DESC LIMIT 1500
"""))
