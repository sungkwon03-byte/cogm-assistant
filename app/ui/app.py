import os, requests, streamlit as st
import pandas as pd

API_PORT = os.getenv("PORT_BACKEND","8000")
API = f"http://localhost:{API_PORT}/api"

st.set_page_config(page_title="PatBot Front Office Suite", layout="wide")
st.title("⚾ PatBot – Front Office Suite")

with st.sidebar:
    st.subheader("Search Player")
    name = st.text_input("Name contains", value="Kim")
    if st.button("Search"):
        r = requests.get(f"{API}/search/player", params={"name": name})
        st.session_state["search"] = r.json()
    st.divider()
    st.subheader("Player Intel")
    mlbam = st.text_input("MLBAM ID", value="455117")
    year  = st.number_input("Year", min_value=2000, max_value=2030, value=2025, step=1)
    vs    = st.selectbox("vs Hand", ["vsR","vsL"], index=0)

tab1, tab2 = st.tabs(["Pitch Mix (CSV)", "Bat Tendencies (CSV)"])

with tab1:
    if st.button("Load Pitch Mix", key="pm"):
        r = requests.get(f"{API}/player/pitchmix", params={"mlbam": mlbam, "year": year})
        js = r.json()
        st.caption(f"rows={js.get('rows',0)}")
        if js.get("rows",0)>0:
            df = pd.DataFrame(js["data"])
            st.dataframe(df, use_container_width=True)
        else:
            st.info("No rows.")

with tab2:
    if st.button("Load Tendencies", key="td"):
        r = requests.get(f"{API}/bat/tendencies", params={"mlbam": mlbam, "year": year, "vs_hand": vs})
        js = r.json()
        st.caption(f"rows={js.get('rows',0)}")
        if js.get("rows",0)>0:
            df = pd.DataFrame(js["data"])
            st.dataframe(df, use_container_width=True)
        else:
            st.info("No rows.")

st.divider()
st.subheader("Search results")
if "search" in st.session_state:
    s = st.session_state["search"]
    st.caption(f"matches={s.get('rows',0)}")
    if s.get("rows",0)>0:
        st.dataframe(pd.DataFrame(s["data"]), use_container_width=True)
