from app.lib.name_resolver import resolve_names
import streamlit as st
from app.features.registry import FEATURES_BY_GROUP
from app.features import renderers as R

st.title("C. Transactions & Draft")
features = FEATURES_BY_GROUP["C"]
labels = {f"#{f.order:02d} {f.label}": f.key for f in features}
choice = st.selectbox("기능 선택", list(labels.keys()))
key = labels[choice]

MAP = {
 "C22": ["output/summaries/trade_value.csv","output/trade_value.csv"],
 "C23": ["output/summaries/trade_proposals_v2.csv","output/summaries/trade_proposals.csv"],
 "C24": ["output/summaries/team_fit.csv"],
 "C25": ["output/summaries/fa_forecast.csv"],
 "C26": ["output/summaries/waivers_shortlist.csv","output/waivers_shortlist.csv"],
 "C27": ["output/summaries/mock_draft.csv","output/mock_draft.csv"],
 "C28": ["output/summaries/intl_fa_targets.csv","output/intl_fa_targets.csv"],
}
R.render_generic_table(choice, MAP.get(key, []))
