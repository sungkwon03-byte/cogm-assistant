from app.lib.name_resolver import resolve_names
import streamlit as st
from app.features import renderers as R

st.title("H. Extensions (보강·후속)")
st.caption("팀 분석 확장, 다자 비교/랭킹, 리그 평균/런환경 보정, 자동 리포트·증거표, ETL/캐시 플러그블 등")

MAP = {
 "팀 분석 확장": ["output/summaries/team_agg.csv","output/team_agg.csv"],
 "다자 비교/랭킹": ["output/summaries/leaderboard_season_change.csv","output/summaries/leaderboard_entropy_top10.csv"],
 "타자 고급지표": ["output/summaries/advanced_metrics.csv","output/advanced_metrics.csv"],
 "투수 고급지표": ["output/summaries/pitcher_season_feature_base.csv"],
 "리그 평균/런환경": ["output/summaries/league_runenv.csv","output/league_runenv.csv"],
 "자동 리포트/증거표": ["output/summaries/visuals_final_status.json"],
 "ETL/캐시 확장": ["output/summaries/full_system_validation.json"],
}
choice = st.selectbox("확장 기능", list(MAP.keys()))
R.render_generic_table(choice, MAP[choice])
