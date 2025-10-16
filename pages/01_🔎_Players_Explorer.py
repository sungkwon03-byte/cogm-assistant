from app.lib.name_resolver import resolve_names
import streamlit as st
from app.core.data import ensure_views, table_exists, df, cols
st.title("Players Explorer / Search")
ensure_views()
if not table_exists("cards") and not table_exists("statcast"):
    st.warning("No data."); st.stop()
q = st.text_input("Search (player_id / name contains)", "")
season = st.number_input("Season", 1901, 2100, 2024)
if q:
    where = f"(CAST(player_id AS VARCHAR) ILIKE '%%{q}%%' OR name ILIKE '%%{q}%%')" 
    if table_exists("v_cards"):
        st.subheader("cards")
        st.dataframe(df(f"SELECT * FROM v_cards WHERE {where} AND season={int(season)} LIMIT 200"))
    elif table_exists("cards"):
        st.subheader("cards(raw)")
        st.dataframe(df(f"SELECT * FROM cards WHERE {where} AND season={int(season)} LIMIT 200"))
    if table_exists("v_stat"):
        st.subheader("statcast (sample)")
        st.dataframe(df(f"SELECT * FROM v_stat WHERE {where} AND season={int(season)} LIMIT 200"))
