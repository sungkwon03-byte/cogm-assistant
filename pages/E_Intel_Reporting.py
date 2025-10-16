from app.lib.name_resolver import resolve_names
import streamlit as st
from app.features.registry import FEATURES_BY_GROUP
from app.features import renderers as R
from app.core.paths import report
from pathlib import Path

st.title("E. Intel & Reporting")
features = FEATURES_BY_GROUP["E"]
labels = {f"#{f.order:02d} {f.label}": f.key for f in features}
choice = st.selectbox("기능 선택", list(labels.keys()))
key = labels[choice]

if key in ("E38","E39"):
    st.subheader(choice)
    f = report("weekly_briefing.pdf") if key=="E38" else report("scouting_report.pdf")
    p = Path(f)
    if p.exists() and p.stat().st_size>0:
        st.download_button("PDF 다운로드", data=p.read_bytes(), file_name=p.name)
    else:
        st.warning("리포트 PDF가 없습니다.")
else:
    MAP = {
      "E36": ["output/summaries/news_digest.json"],
      "E37": ["output/summaries/gameprep_report_day42.csv"],
      "E40": ["output/summaries/visuals_final_status.json"],
      "E41": ["output/summaries/duckdb_query_templates.txt"],
    }
    R.render_generic_table(choice, MAP.get(key, []))
