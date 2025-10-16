from app.lib.name_resolver import resolve_names
import streamlit as st
from app.features.registry import FEATURES_BY_GROUP
from app.features import renderers as R

st.title("A. Player Intelligence")

features = FEATURES_BY_GROUP["A"]
labels = {f"#{f.order:02d} {f.label}": f.key for f in features}
choice = st.selectbox("기능 선택", list(labels.keys()))
key = labels[choice]

if key == "A01":
    R.render_player_single()
elif key == "A02":
    R.render_compare()
elif key == "A03":
    R.render_trend_3y()
elif key == "A04":
    R.render_generic_table("카운트/투수유형별 성향", ["output/summaries/count_tendencies_bat.csv"])
elif key == "A05":
    R.render_generic_table("약점 탐색(구종×코스)", ["output/summaries/weakness_map_player_year.csv","output/weakness_map_player_year.csv"])
elif key == "A06":
    R.render_generic_table("플래툰 성향 맵", ["output/summaries/platoon_map_player_year.csv","output/platoon_map_player_year.csv"])
elif key == "A07":
    R.render_generic_table("타구질 프로파일", ["output/summaries/batter_la_ev_variability.parquet","output/summaries/batter_la_ev_variability.csv"])
elif key == "A08":
    R.render_generic_table("핫/콜드 스틱 안정성", ["output/summaries/bat_stability.csv"])
elif key == "A09":
    R.render_generic_table("부상 리스크 시그널", ["output/summaries/injury_risk_flags.csv"])
elif key == "A10":
    R.render_generic_table("롤 적합도 제안", ["output/summaries/role_fit_suggestions.csv"])
elif key == "A11":
    R.render_generic_table("포지션 전환 가능성", ["output/summaries/position_change_candidates.csv"])
elif key == "A12":
    R.render_generic_table("해외 전환 분석(KBO/NPB↔MLB 보정)", ["output/summaries/player_card_xleague.csv","output/player_card_xleague.csv"])
else:
    st.info("아직 연결되지 않은 키입니다.")
