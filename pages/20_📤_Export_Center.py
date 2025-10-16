from app.lib.name_resolver import resolve_names
import streamlit as st
from app.core.data import ensure_views, table_exists, df
st.title("Export Center")
ensure_views()
opt = st.selectbox("Table", ["player_season","v_cards","v_stat","platoon_split","zone_heat","ev_la_grid","value_simple","id_map"])
tbl = opt if table_exists(opt) else None
if not tbl:
    st.warning("Selected table not available.")
else:
    d = df(f"SELECT * FROM {tbl} LIMIT 5000")
    st.dataframe(d)
    st.download_button("Download CSV", d.to_csv(index=False).encode("utf-8"), file_name=f"{opt}.csv", mime="text/csv")
