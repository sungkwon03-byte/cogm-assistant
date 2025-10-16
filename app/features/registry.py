from app.lib.name_resolver import resolve_names
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict

@dataclass(frozen=True)
class Feature:
    key: str
    group: str
    order: int
    label: str
    source_hint: str | None = None

FEATURES: List[Feature] = [
    # A (1~12)
    Feature("A01","A",1,"단일 선수 분석","cards/parquet, id_map"),
    Feature("A02","A",2,"2–3인 비교(스파이더/버터플라이)","cards, summaries/player_compare_rows.csv"),
    Feature("A03","A",3,"3년 트렌드(wRC+, BABIP, EV, BB/K)","cards, statcast"),
    Feature("A04","A",4,"카운트/투수유형별 성향","summaries/count_tendencies_bat.csv"),
    Feature("A05","A",5,"약점 탐색(구종×코스)","summaries/weakness_map_player_year.csv"),
    Feature("A06","A",6,"플래툰 성향 맵","summaries/platoon_map_player_year.csv"),
    Feature("A07","A",7,"타구질 프로파일(EV/LA/Hard%)","summaries/batter_la_ev_variability.*"),
    Feature("A08","A",8,"핫/콜드 스틱 안정성","summaries/bat_stability.csv"),
    Feature("A09","A",9,"부상 리스크 시그널","summaries/injury_risk_flags.csv"),
    Feature("A10","A",10,"롤 적합도 제안","summaries/role_fit_suggestions.csv"),
    Feature("A11","A",11,"포지션 전환 가능성","summaries/position_change_candidates.csv"),
    Feature("A12","A",12,"해외 전환 분석(KBO/NPB↔MLB 보정)","summaries/player_card_xleague.csv"),
    # B (13~21)
    Feature("B13","B",13,"멀티-이어 페이롤 시뮬","summaries/payroll_sim.csv"),
    Feature("B14","B",14,"ARB 예상","summaries/fa_market_mvp*.csv"),
    Feature("B15","B",15,"계약 ROI/서플러스($/WAR·NPV)","summaries/package_surplus.csv"),
    Feature("B16","B",16,"포지션 대체 자원 추천","summaries/team_fit.csv"),
    Feature("B17","B",17,"옵션/40-Man 관리","(내부테이블)"),
    Feature("B18","B",18,"IL/복귀 일정 트래킹","summaries/gameprep_report_day42.csv"),
    Feature("B19","B",19,"계약 비교 도구","summaries/contract_compare.csv"),
    Feature("B20","B",20,"CBA/룰 QA 요약","summaries/cba_qa.json"),
    Feature("B21","B",21,"에이전트 히스토리 분석","summaries/agent_history.csv"),
    # C (22~28)
    Feature("C22","C",22,"트레이드 밸류 판단","summaries/trade_value.csv"),
    Feature("C23","C",23,"모의 트레이드 생성","summaries/trade_proposals*.csv"),
    Feature("C24","C",24,"팀 컬러-핏 매칭","summaries/team_fit.csv"),
    Feature("C25","C",25,"FA 시장 예측","summaries/fa_forecast.csv"),
    Feature("C26","C",26,"웨이버/Rule 5 숏리스트","summaries/waivers_shortlist.csv"),
    Feature("C27","C",27,"모의 드래프트","summaries/mock_draft.csv"),
    Feature("C28","C",28,"국제 FA 타깃 추천","summaries/intl_fa_targets.csv"),
    # D (29~35)
    Feature("D29","D",29,"일정 분석","summaries/schedule_analysis*.csv"),
    Feature("D30","D",30,"라인업 최적화","summaries/lineup_day*.csv"),
    Feature("D31","D",31,"인-게임 레버리지 어시스트","summaries/winprob_day*.csv"),
    Feature("D32","D",32,"심판 영향 모델(EUZ)","summaries/ump_euz_indices.csv"),
    Feature("D33","D",33,"구장 파크팩터(데일리)","summaries/pf_daily_day*.csv"),
    Feature("D34","D",34,"원정 피로 모델","summaries/schedule_analysis_summary.csv"),
    Feature("D35","D",35,"승률/WP 예측","summaries/winprob_day*.csv"),
    # E (36~41)
    Feature("E36","E",36,"뉴스 통합 요약","summaries/news_digest.json"),
    Feature("E37","E",37,"전날 경기 리포트","summaries/gameprep_report_day42.csv"),
    Feature("E38","E",38,"주간 운영 브리핑(PDF/노션)","reports/weekly_briefing.pdf"),
    Feature("E39","E",39,"스카우팅 리포트 템플릿","reports/scouting_report.pdf"),
    Feature("E40","E",40,"증거 테이블 자동 첨부","summaries/visuals_final_status.json"),
    Feature("E41","E",41,"대화형 응답 규격","summaries/duckdb_query_templates.txt"),
    # F (42~45)
    Feature("F42","F",42,"KBO/NPB 포스팅 규정 카드","summaries/posting_rules.json"),
    Feature("F43","F",43,"드래프트/국제 보너스 풀 트래커","summaries/bonus_pool.csv"),
    Feature("F44","F",44,"KBO 동급 지원(리더보드·트렌드)","summaries/player_card_xleague.csv"),
    Feature("F45","F",45,"리그 간 보정 계수(KBO↔MLB 변환)","summaries/xleague_coeffs.json"),
    # G (46~52)
    Feature("G46","G",46,"워치리스트/알람","summaries/watchlist.csv"),
    Feature("G47","G",47,"의사결정 로그 & 레드팀","summaries/decision_log.json"),
    Feature("G48","G",48,"멀티시즌 시나리오 플래너","summaries/scenario_alt.csv"),
    Feature("G49","G",49,"선수 개발 트래커","summaries/dev_tracker.csv"),
    Feature("G50","G",50,"Explainable AI","summaries/explainable_feature_attrib.csv"),
    Feature("G51","G",51,"데이터 거버넌스(RBAC 등)","summaries/mart_span_validation.json"),
    Feature("G52","G",52,"ID 매핑/정규화(Chadwick/Lahman/FG)","output/id_map.csv"),
]
FEATURES_BY_GROUP: Dict[str, list[Feature]] = {}
for f in FEATURES:
    FEATURES_BY_GROUP.setdefault(f.group, []).append(f)
for group in FEATURES_BY_GROUP.values():
    group.sort(key=lambda x: x.order)
