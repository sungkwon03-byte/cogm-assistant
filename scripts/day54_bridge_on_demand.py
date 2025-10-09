#!/usr/bin/env python3
import os, json
import pandas as pd
from collections import defaultdict

os.makedirs("output", exist_ok=True)

SPEC = {
    "KBO_bat": ["hr_pa","bb_pct","so_pct","h_pa","ops_proxy"],
    "KBO_pit": ["ERA","K9","BB9"]
}
PAIR_FILE = {
    "KBO_bat": "output/KBO_bat_pairs.csv",
    "KBO_pit": "output/KBO_pit_pairs.csv"
}

def fit_xy(x, y):
    import numpy as np
    x = np.asarray(x, dtype=float); y = np.asarray(y, dtype=float)
    if len(x) < 2: return 0.0, 0.0, 0   # a, b, n
    xm, ym = x.mean(), y.mean()
    denom = ((x-xm)**2).sum()
    if denom == 0: return 0.0, 0.0, len(x)
    a = ((x-xm)*(y-ym)).sum()/denom
    b = ym - a*xm
    return float(a), float(b), len(x)

def main():
    rows = []
    j = defaultdict(dict)
    rpt = []

    for group, metrics in SPEC.items():
        path = PAIR_FILE[group]
        if not os.path.exists(path):
            rpt.append(f"{group}: pairs=0 (missing {path})")
            for m in metrics:
                rows.append({"group":group,"metric":m,"n":0,"scale":0.0,"offset":0.0})
                j[group][m] = {"n":0,"scale":0.0,"offset":0.0}
            continue
        df = pd.read_csv(path)
        # 기대 컬럼: metric,kbo_val,mlb_val,player_id,season
        if df.empty:
            rpt.append(f"{group}: pairs=0 (empty)")
        else:
            rpt.append(f"{group}: pairs={df['player_id'].nunique() if 'player_id' in df.columns else len(df)}")

        for m in metrics:
            sub = df[df["metric"]==m].dropna(subset=["kbo_val","mlb_val"])
            a,b,n = fit_xy(sub["kbo_val"], sub["mlb_val"])
            rows.append({"group":group,"metric":m,"n":int(n),"scale":round(a,6),"offset":round(b,6)})
            j[group][m] = {"n":int(n),"scale":a,"offset":b}

    coef = pd.DataFrame(rows)
    coef.to_csv("output/bridge_coef.csv", index=False)
    with open("output/xleague_coeffs.json","w") as f:
        json.dump(j, f, indent=2)

    with open("output/bridge_report.txt","w") as f:
        for line in rpt: f.write(line+"\n")

    print("[OK] Day54 Step2 complete -> output/bridge_coef.csv, xleague_coeffs.json, bridge_report.txt")

if __name__ == "__main__":
    main()
