from app.lib.name_resolver import resolve_names
import streamlit as st
from app.features.registry import FEATURES_BY_GROUP
from app.features import renderers as R

st.title("B. Roster & Payroll")
features = FEATURES_BY_GROUP["B"]
labels = {f"#{f.order:02d} {f.label}": f.key for f in features}
choice = st.selectbox("기능 선택", list(labels.keys()))
key = labels[choice]

MAP = {
 "B13": ["output/summaries/payroll_sim.csv","output/payroll_sim.csv"],
 "B14": ["output/summaries/fa_market_mvp.csv","output/summaries/fa_market_mvp_full.csv"],
 "B15": ["output/summaries/package_surplus.csv"],
 "B16": ["output/summaries/team_fit.csv","output/team_fit.csv"],
 "B17": ["output/summaries/options_40man.csv"],
 "B18": ["output/summaries/gameprep_report_day42.csv","output/gameprep_report_day42.csv"],
 "B19": ["output/summaries/contract_compare.csv"],
 "B20": ["output/summaries/cba_qa.json"],
 "B21": ["output/summaries/agent_history.csv"],
}
R.render_generic_table(choice, MAP.get(key, []), note="데이터가 없으면 Doctor로 상태 확인.")
