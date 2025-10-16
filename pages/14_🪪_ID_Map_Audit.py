from app.lib.name_resolver import resolve_names
import streamlit as st
from app.core.data import ensure_views, table_exists, df
st.title("ID Map / Linkage Audit")
ensure_views()
if not table_exists("id_map"):
    st.warning("No id_map.csv"); st.stop()
st.dataframe(df("SELECT * FROM id_map LIMIT 1000"))
if table_exists("idmap_audit"):
    st.subheader("Duplicates")
    st.dataframe(df("SELECT * FROM idmap_audit ORDER BY cnt DESC LIMIT 500"))
