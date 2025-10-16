from app.lib.name_resolver import resolve_names
import streamlit as st
from app.core.data import ensure_views, table_exists, df
st.title("F) International / X-League")
ensure_views()
st.write("보정 전/후 비교(데이터 가용 범위 내).")
