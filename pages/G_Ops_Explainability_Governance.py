from app.lib.name_resolver import resolve_names
import streamlit as st
from app.features.registry import FEATURES_BY_GROUP
from app.features import renderers as R

st.title("G. Ops · Explainability · Governance")
features = FEATURES_BY_GROUP["G"]
labels = {f"#{f.order:02d} {f.label}": f.key for f in features}
choice = st.selectbox("기능 선택", list(labels.keys()))
key = labels[choice]

MAP = {
 "G46": ["output/summaries/watchlist.csv"],
 "G47": ["output/summaries/decision_log.json"],
 "G48": ["output/summaries/scenario_alt.csv","output/scenario_alt.csv"],
 "G49": ["output/summaries/dev_tracker.csv"],
 "G50": ["output/summaries/explainable_feature_attrib.csv","output/explainable_feature_attrib.csv"],
 "G51": ["output/summaries/mart_span_validation.json"],
 "G52": ["output/id_map.csv"],
}
R.render_generic_table(choice, MAP.get(key, []))
