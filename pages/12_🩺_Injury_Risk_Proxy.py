from app.lib.name_resolver import resolve_names
import streamlit as st
from app.core.data import ensure_views, table_exists, df
st.title("Injury Risk (EV volatility proxy)")
ensure_views()
if not table_exists("v_stat"):
    st.warning("No statcast."); st.stop()
st.dataframe(df("""
WITH s AS (
  SELECT player_id, season, STDDEV_POP(COALESCE(ev,0)) AS ev_sd, COUNT(*) n
  FROM v_stat GROUP BY 1,2
)
SELECT * FROM s ORDER BY ev_sd DESC NULLS LAST LIMIT 1000
"""))
