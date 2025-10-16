from app.lib.name_resolver import resolve_names
import streamlit as st
from app.core.data import ensure_views, table_exists, df
st.title("Umpire EUZ Proxy (zone freq)")
ensure_views()
if not table_exists("v_stat"):
    st.warning("No statcast."); st.stop()
st.dataframe(df("""
SELECT season, zone, COUNT(*) n
FROM v_stat WHERE zone IS NOT NULL
GROUP BY 1,2 ORDER BY season DESC, n DESC LIMIT 2000
"""))
