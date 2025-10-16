from app.lib.name_resolver import resolve_names
import streamlit as st
from app.core.data import ensure_views, table_exists, df
st.title("Lineup Helper (Best-9 by wOBA)")
ensure_views()
if not table_exists("v_cards"):
    st.warning("No cards"); st.stop()
season=st.number_input("Season", 1901,2100,2024)
st.dataframe(df(f"""
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER(PARTITION BY COALESCE(pos,'?') ORDER BY COALESCE(woba,0) DESC) AS rk
  FROM v_cards WHERE season={int(season)}
)
SELECT * FROM ranked WHERE rk<=1 ORDER BY COALESCE(woba,0) DESC
"""))
