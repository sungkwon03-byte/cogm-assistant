import streamlit as st, json, pathlib, datetime as dt
p = pathlib.Path("output/summaries/final_frontier_progress.json")
if p.exists():
    j = json.loads(p.read_text())
    st.info(f"**Final Frontier 100** | ✅ {j['status_counts']['done']} · �� {j['status_counts']['partial']} · ⏳ {j['status_counts']['design']} · ⏸ {j['status_counts']['hold']}  "
            f"· updated: {dt.datetime.utcnow().isoformat(timespec='seconds')}Z")
else:
    st.info("**Final Frontier 100** | 진행도 파일 없음")
