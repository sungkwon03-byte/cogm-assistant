from app.lib.name_resolver import resolve_names
import streamlit as st
from app.core.data import ensure_views, table_exists, df
st.title("EV Trends")
ensure_views()
if not table_exists("v_stat"):
    st.warning("No statcast"); st.stop()
pid = st.text_input("player_id contains", "")
q = "SELECT season, AVG(COALESCE(ev,0)) avg_ev FROM v_stat"
if pid: q += f" WHERE CAST(player_id AS VARCHAR) ILIKE '%%{pid}%%'"
q += " GROUP BY 1 ORDER BY 1"
st.dataframe(df(q))
