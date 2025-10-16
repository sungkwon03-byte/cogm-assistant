from app.lib.name_resolver import resolve_names
import streamlit as st
from app.core.data import ensure_views, table_exists, df
st.title("Explainability (simple ranks)")
ensure_views()
if not table_exists("v_cards"):
    st.warning("No cards"); st.stop()
st.dataframe(df("""
SELECT season, player_id, name, COALESCE(woba,0) AS woba, ROW_NUMBER() OVER(PARTITION BY season ORDER BY COALESCE(woba,0) DESC) AS rk
FROM v_cards ORDER BY season DESC, rk ASC LIMIT 1000
"""))
