from app.lib.name_resolver import resolve_names
import streamlit as st
from app.core.data import ensure_views, table_exists, df
st.title("Trade Builder (MAX)")
ensure_views()
if not table_exists("v_cards") and not table_exists("value_simple"):
    st.warning("No value table."); st.stop()
season = st.number_input("Season", 1901, 2100, 2024)
left = st.text_area("Package A ids (comma)", "")
right= st.text_area("Package B ids (comma)", "")
def pack(ids):
    ids=[x.strip() for x in ids.split(",") if x.strip()]
    if not ids: return 0.0
    q=f"SELECT AVG(COALESCE(woba,0)) v FROM v_cards WHERE season={int(season)} AND CAST(player_id AS VARCHAR) IN ({','.join("'" + i + "'" for i in ids)})"
    try: return float(df(q)['v'].iloc[0])
    except: return 0.0
col1,col2=st.columns(2)
with col1: st.metric("Package A", f"{pack(left):.3f}")
with col2: st.metric("Package B", f"{pack(right):.3f}")
