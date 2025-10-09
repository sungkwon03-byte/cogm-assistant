#!/usr/bin/env bash
set -euo pipefail

echo "== Week7 SMOKE START =="

# contracts/compare_v2
curl -s -X POST http://127.0.0.1:8000/contracts/compare_v2 \
  -H "Content-Type: application/json" \
  -d '{
    "team":"SEA","budget_cap":25000000,
    "priority_weights":{"war":1.0,"age":0.4,"platoon":0.3,"defense":0.5},
    "items":[
      {"player_id":"X","contract":[{"year":2025,"salary":12000000,"proj_war":2.8,"age":27,"def":5}]},
      {"player_id":"Y","contract":[{"year":2025,"salary":10000000,"proj_war":2.1,"age":31,"def":1}]}
    ]
  }' | python -m json.tool

# IL replacements v2 (injured override 없을 때는 빈 배열일 수 있음)
curl -s -X POST http://127.0.0.1:8000/reports/il_replacements_v2 \
  -H "Content-Type: application/json" \
  -d '{"team":"SEA"}' | python -m json.tool || true

# IL replacements v3 (v3는 내부 IL 상태 기반 추천)
curl -s -X POST http://127.0.0.1:8000/reports/il_replacements_v3 \
  -H "Content-Type: application/json" \
  -d '{"team":"SEA"}' | python -m json.tool

# watchlist / alerts
curl -s -X POST http://127.0.0.1:8000/ops/watchlist/set \
  -H "Content-Type: application/json" \
  -d '{"team":"SEA","player_ids":["p1","p2","p3"]}' | python -m json.tool

curl -s -X POST http://127.0.0.1:8000/ops/alerts/set_rules \
  -H "Content-Type: application/json" \
  -d '{"team":"SEA","rules":[
        {"metric":"OPS_plus","op":"gt","threshold":130},
        {"metric":"ERA_plus","op":"lt","threshold":90},
        {"metric":"injury_flag","op":"eq","threshold":1}
      ]}' | python -m json.tool

curl -s -X POST http://127.0.0.1:8000/ops/alerts/evaluate \
  -H "Content-Type: application/json" \
  -d '{"team":"SEA","season":2025}' | python -m json.tool

# scenario planner
curl -s -X POST http://127.0.0.1:8000/ops/scenario/plan \
  -H "Content-Type: application/json" \
  -d '{
    "team":"SEA",
    "horizon_start":2025, "horizon_years":3,
    "base_wins": {"2025":86, "2026":84, "2027":83},
    "base_payroll": {"2025":180000000, "2026":185000000, "2027":190000000},
    "budget_cap": 195000000,
    "changes": [
      {"tag":"Sign:1B-LHH","years":[
        {"year":2025,"delta_war":2.4,"delta_salary":12000000},
        {"year":2026,"delta_war":2.1,"delta_salary":13000000}
      ]},
      {"tag":"Trade:CF glove","years":[
        {"year":2025,"delta_war":1.2,"delta_salary":2000000},
        {"year":2026,"delta_war":-1.5,"delta_salary":0}
      ]}
    ]
  }' | python -m json.tool

# decision log & list
curl -s -X POST http://127.0.0.1:8000/ops/decision/log \
  -H "Content-Type: application/json" \
  -d '{
    "id":"D27-001",
    "actor":"FO",
    "action":"TradeProposal",
    "summary":"Acquire CF glove; WAR +1.2 expected; risk hamstring; alt internal CF.",
    "context":{"team":"SEA","target":"CF-DEF"},
    "evidence":[{"k":"WAR_gain","v":"1.2"},{"k":"OPS+","v":"105"}]
  }' | python -m json.tool

curl -s "http://127.0.0.1:8000/ops/decision/list?limit=5" | python -m json.tool

# explainable AI
curl -s -X POST http://127.0.0.1:8000/explain/summarize \
  -H "Content-Type: application/json" \
  -d '{
    "title":"CF 수비 보강 트레이드",
    "claims":["수비 개선으로 실점 감소, Pythag 승률 +1.5p 기대"],
    "evidence":[
      {"k":"DRS","v":12,   "weight":1.3},
      {"k":"UZR/150","v":8.5,"weight":1.2},
      {"k":"SprintSpeed","v":29.1,"weight":0.9}
    ],
    "assumptions":["햄스트링 이슈 무재발"],
    "limitations":["샘플 400PA로 소표본"],
    "next_actions":["의무팀 메디컬 세컨드 오피니언","2주후 필드스카우트 업데이트"]
  }' | python -m json.tool

curl -s "http://127.0.0.1:8000/explain/_selfcheck" | python -m json.tool

echo "== Week7 SMOKE END =="
