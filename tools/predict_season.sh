#!/usr/bin/env bash
set +e
python - <<'PY'
import pandas as pd,numpy as np,json
cards=pd.read_parquet("output/player_cards_all.parquet")
teams=cards['team'].dropna().unique()
res=[]
for t in teams:
    power=cards.query("team==@t")['WAR'].mean()
    winpct=1/(1+np.exp(-power/2))
    res.append({"team":t,"power":round(float(power),2),"projWinPct":round(float(winpct),3)})
print(json.dumps(res,indent=2))
PY
