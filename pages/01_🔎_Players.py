from app.lib.name_resolver import resolve_names
import streamlit as st
from app.core.data import ensure_views, table_exists, df

st.title("A) Players / Compare / Heatmaps")
ensure_views()

if not table_exists("cards") and not table_exists("statcast"):
    st.warning("No player data.")
    st.stop()

pid = st.text_input("Player ID or Name contains", "")
season = st.number_input("Season", min_value=1901, max_value=2100, value=2024)

if pid:
    try:
        q = f"""
        SELECT *
        FROM cards
        WHERE (CAST(player_id AS VARCHAR) ILIKE '%%{pid}%%' OR name ILIKE '%%{pid}%%')
          AND season = {int(season)}
        LIMIT 200
        """
        st.dataframe(df(q))
    except Exception as e:
        st.error(str(e))

st.caption("Tip: ID/이름 파편만 입력해도 매칭. 대용량은 LIMIT로 안전.")
