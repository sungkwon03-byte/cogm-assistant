#!/usr/bin/env bash
set -euo pipefail

BASE="http://127.0.0.1:8000"
echo "== smoke: _ping"; curl -s "$BASE/_ping" | jq .

echo "== smoke: progress"; curl -s "$BASE/progress" | jq .

echo "== smoke: player_search"; curl -s --get "$BASE/player_search" --data-urlencode "q=harper" | jq '.results[:5]'

echo "== smoke: get_player_stats"; curl -s --get "$BASE/get_player_stats" \
  --data-urlencode "name=Bryce Harper" --data-urlencode "season=2021" | jq .

echo "== smoke: get_pitching_stats"; curl -s --get "$BASE/get_pitching_stats" \
  --data-urlencode "name=Max Scherzer" --data-urlencode "season=2021" | jq .

echo "== smoke: get_player_trend"; curl -s --get "$BASE/get_player_trend" \
  --data-urlencode "name=Bryce Harper" --data-urlencode "season=2021" --data-urlencode "years=3" | jq .

echo "== smoke: player_power_rankings"; curl -s "$BASE/player_power_rankings?season=2021&limit=5&minAB=300&minIP=100&include_baselines=true" \
  | jq '{season, baselines, bat:.batting_top, pit:.pitching_top}'

echo "== smoke: team_leaderboard"; curl -s "$BASE/team_leaderboard?season=2021&limit=10&modern_codes=true&include_baselines=true" \
  | jq '{season, baselines, top_bat:.batting_top[:5], top_pit:.pitching_top[:5]}'

echo "== smoke: compare_teams"; curl -s --get "$BASE/compare_teams" \
  --data-urlencode "season=2021" --data-urlencode "teamA=LAD" --data-urlencode "teamB=SFG" | jq .

echo "== smoke: team_power_rankings"; curl -s "$BASE/team_power_rankings?season=2021&limit=10&bat_w=0.5&pit_w=0.5" | jq '.power[:5]'

echo "== smoke: trade_value"; curl -s --get "$BASE/trade_value" \
  --data-urlencode "name=Sample" --data-urlencode "war=5,4.5,3" \
  --data-urlencode "salary=1,20,25" --data-urlencode "dpw=9" --data-urlencode "dr=0.08" | jq .

echo "== smoke: trade_package_value"; curl -s -X POST "$BASE/trade_package_value" \
  -H "Content-Type: application/json" -d '{
    "discount_rate": 0.08,
    "teamA": [
      {"name":"Prospect A","playerID":"prosA","war":[2.0,2.5,3.0],"salary":[0.7,1.0,1.5],"dpw":9.0},
      {"name":"MLB SP","playerID":"sp01","war":[3.5,3.0],"salary":[12,14],"dpw":9.0}
    ],
    "teamB": [
      {"name":"Star Hitter","playerID":"hit01","war":[5.0,4.0,3.0],"salary":[20,22,24],"dpw":9.0}
    ]
  }' | jq .

echo "== smoke: contract_compare"; curl -s --get "$BASE/contract_compare" \
  --data-urlencode "name1=Deal A" --data-urlencode "war1=5,4,3" --data-urlencode "salary1=1,20,25" \
  --data-urlencode "name2=Deal B" --data-urlencode "war2=4,3,2" --data-urlencode "salary2=10,15,18" \
  --data-urlencode "dpw=9" --data-urlencode "dr=0.08" | jq .
