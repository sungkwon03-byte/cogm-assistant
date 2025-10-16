from app.lib.name_resolver import resolve_names
import streamlit as st
from app.core.data import ensure_views, table_exists, df, cols
st.title("Count → Outcome Matrix")
ensure_views()
if not table_exists("v_stat"):
    st.warning("No statcast"); st.stop()
have_event = "event" in [c.lower() for c in cols("v_stat")]
st.dataframe(df(f"""
SELECT season, balls, strikes{", event" if have_event else ""}, COUNT(*) n
FROM v_stat GROUP BY 1,2,3{",4" if have_event else ""} ORDER BY season DESC, n DESC LIMIT 2000
"""))
