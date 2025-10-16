from app.lib.name_resolver import resolve_names
import streamlit as st
from app.features.registry import FEATURES_BY_GROUP
from app.features import renderers as R

st.title("F. International & Amateur")
features = FEATURES_BY_GROUP["F"]
labels = {f"#{f.order:02d} {f.label}": f.key for f in features}
choice = st.selectbox("기능 선택", list(labels.keys()))
key = labels[choice]

MAP = {
 "F42": ["output/summaries/posting_rules.json"],
 "F43": ["output/summaries/bonus_pool.csv"],
 "F44": ["output/summaries/player_card_xleague.csv","output/player_card_xleague.csv"],
 "F45": ["output/summaries/xleague_coeffs.json"],
}
R.render_generic_table(choice, MAP.get(key, []))
