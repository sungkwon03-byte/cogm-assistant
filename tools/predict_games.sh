#!/usr/bin/env bash
set +e
python - <<'PY'
import pandas as pd,numpy as np,duckdb,json
cards=pd.read_parquet("output/player_cards_all.parquet")
idmap=pd.read_csv("output/id_map.csv")
def simulate_match(lineup_a,lineup_b):
    def score(lu): return np.sum(lu['xwOBA']*lu['PA']/lu['PA'].sum())*9
    sA,sB=score(lineup_a),score(lineup_b)
    win=float(1/(1+10**((sB-sA)/1.5)))
    return {"teamA_expRuns":round(sA,2),"teamB_expRuns":round(sB,2),"teamA_winProb":round(win,3)}
print(json.dumps(simulate_match(cards.sample(9),cards.sample(9)),indent=2))
PY
