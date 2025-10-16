from app.lib.name_resolver import resolve_names
import streamlit as st
from app.core.data import ensure_views, table_exists, df, cols
st.title("Role Fit (pos/team wOBA)")
ensure_views()
if not table_exists("v_cards"):
    st.warning("No cards"); st.stop()
st.dataframe(df("""
SELECT season, team, pos, AVG(COALESCE(woba,0)) avg_woba, COUNT(*) n
FROM v_cards GROUP BY 1,2,3 ORDER BY season DESC, avg_woba DESC NULLS LAST LIMIT 1500
"""))
