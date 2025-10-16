from app.lib.name_resolver import resolve_names
import streamlit as st
from app.core.data import ensure_views, table_exists, df
st.title("EV/LA Grid")
ensure_views()
if table_exists("ev_la_grid"):
    st.dataframe(df("SELECT * FROM ev_la_grid ORDER BY season DESC, n DESC LIMIT 2000"))
elif table_exists("v_stat"):
    st.dataframe(df("""
    SELECT season, ROUND(ev)::INT ev_bin, ROUND(la)::INT la_bin, COUNT(*) n
    FROM v_stat WHERE ev IS NOT NULL AND la IS NOT NULL
    GROUP BY 1,2,3 ORDER BY season DESC, n DESC LIMIT 2000
    """))
else:
    st.warning("No EV/LA columns.")
