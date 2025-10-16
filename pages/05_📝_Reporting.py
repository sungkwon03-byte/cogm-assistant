from app.lib.name_resolver import resolve_names
import streamlit as st
from app.core.data import ensure_views, table_exists, df
st.title("E) Reporting / Export")
ensure_views()
st.write("표/차트 우측 상단 메뉴로 CSV 다운로드 가능. PDF/PNG는 브라우저 인쇄 기능 이용.")
