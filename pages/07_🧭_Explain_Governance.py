from app.lib.name_resolver import resolve_names
import streamlit as st
from app.core.data import ensure_views, table_exists, df
st.title("G) Explainability / Governance")
ensure_views()
st.write("특성 중요도/데이터 품질 게이지는 데이터 가용 시 자동 확장.")
