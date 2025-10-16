from app.lib.name_resolver import resolve_names
import streamlit as st
from app.features.registry import FEATURES_BY_GROUP
from app.features import renderers as R

st.title("D. Game Prep & Forecast")
features = FEATURES_BY_GROUP["D"]
labels = {f"#{f.order:02d} {f.label}": f.key for f in features}
choice = st.selectbox("기능 선택", list(labels.keys()))
key = labels[choice]

MAP = {
 "D29": ["output/summaries/schedule_analysis.csv","output/summaries/schedule_analysis_summary.csv"],
 "D30": ["output/summaries/lineup_day37.csv","output/lineup_day37.csv"],
 "D31": ["output/summaries/winprob_day41.csv","output/winprob_day41.csv"],
 "D32": ["output/summaries/ump_euz_indices.csv","output/ump_euz_indices.csv"],
 "D33": ["output/summaries/pf_daily_day40.csv","output/pf_daily_day40.csv"],
 "D34": ["output/summaries/schedule_analysis_summary.csv"],
 "D35": ["output/summaries/winprob_day41.csv"],
}
R.render_generic_table(choice, MAP.get(key, []))
