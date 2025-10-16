from app.lib.name_resolver import resolve_names
import streamlit as st
from app.core.data import ensure_views, table_exists, df
st.title("C) Trade Builder / Mock Draft")
ensure_views()
st.write("패키지 빌더(경량 예시). 좌/우 후보를 입력해 합산 가치 시뮬레이션.")

left = st.text_area("Package A (player ids, comma)", "")
right= st.text_area("Package B (player ids, comma)", "")
season = st.number_input("Season", 1901, 2100, 2024)

def pack_value(ids):
    ids = [i.strip() for i in ids.split(",") if i.strip()]
    if not ids: return 0.0
    try:
        q = f"""
        SELECT AVG(COALESCE(woba,0)) AS v
        FROM cards
        WHERE season={int(season)} AND CAST(player_id AS VARCHAR) IN ({",".join("'" + x + "'" for x in ids)})
        """
        d = df(q)
        return float(d['v'].iloc[0]) if len(d) else 0.0
    except:
        return 0.0

colA, colB = st.columns(2)
with colA: st.metric("Package A value", f"{pack_value(left):.3f}")
with colB: st.metric("Package B value", f"{pack_value(right):.3f}")
