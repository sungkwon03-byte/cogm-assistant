#!/usr/bin/env bash
set -euo pipefail
ROOT="/workspaces/cogm-assistant"
OUT="$ROOT/output"; LOG="$ROOT/logs"
mkdir -p "$LOG"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RLOG="$LOG/smoke_$STAMP.log"
echo "[smoke] start $STAMP" | tee "$RLOG"

py() { python - "$@" 2>&1 | tee -a "$RLOG"; }

py <<'PY'
import os, sys, csv, math
from pathlib import Path

ROOT = "/workspaces/cogm-assistant"
OUT  = f"{ROOT}/output"

FILES = [
  ("statcast_features_player_year.csv",
    dict(required=["xwOBA","hardhit_rate","barrel_rate","whiff_rate"],
         rates=["hardhit_rate","barrel_rate","whiff_rate"])),
  ("statcast_pitch_mix_detailed.csv",
    dict(required=["pitch_type","usage_rate","zone_rate","edge_rate","heart_rate","chase_rate","z_whiff_rate"],
         rates=["usage_rate","zone_rate","edge_rate","heart_rate","chase_rate","z_whiff_rate"])),
  ("statcast_pitch_mix_detailed_plus_bat.csv",
    dict(required=["batter","vs_hand","usage_rate","z_whiff_rate"], rates=["usage_rate","z_whiff_rate"])),
  ("count_tendencies_bat.csv",
    dict(required=["batter","count","swing_rate","whiff_rate"], rates=["swing_rate","whiff_rate"])),
  ("bat_stability.csv",
    dict(required=["player_id","metric","rolling_var"], rates=[])),
  ("weakness_map_player_year.csv",
    dict(required=["player_id","pitch_type","zone","edge_weight","heart_weight"], rates=[])),
  ("trend_3yr.csv",
    dict(required=["player_id","season","OPS","xwOBA","EV","BABIP"], rates=[])),
  ("trade_value.csv",
    dict(required=["player_id","name","value","surplus"], rates=[])),
  ("mock_trades_mvp.csv",
    dict(required=["trade_id","team_from","team_to","players_out","players_in"], rates=[])),
  ("fa_market_mvp.csv",
    dict(required=["player_id","name","years","aav"], rates=[])),
  ("advanced_metrics.csv", dict(required=["player_id"], rates=[])),
  ("league_runenv.csv",   dict(required=["season","run_env"], rates=[])),
  ("ump_euz_indices.csv", dict(required=["season","umpire_id","euz_index"], rates=[])),
]

def pct_in_01(vals):
    ok=bad=0
    for v in vals:
        if v is None or v=="":
            continue
        try:
            x=float(v)
            if math.isnan(x):
                continue
            if -1e-9 <= x <= 1+1e-9:
                ok+=1
            else:
                bad+=1
        except: bad+=1
    total = ok+bad
    return 1.0 if total==0 else ok/max(1,total)

report=[]
for fname, rules in FILES:
    path = Path(OUT, fname)
    status = {"file": fname, "exists": path.exists(), "rows": 0,
              "req_columns": "SKIP", "rate_sanity": "SKIP", "notes": ""}

    if not path.exists():
        status["notes"]="missing"
        report.append(status); continue

    req = rules.get("required",[])
    rates = rules.get("rates",[])
    head = []
    sample = []
    try:
        with open(path, newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            head = rdr.fieldnames or []
            for i,row in enumerate(rdr):
                sample.append(row)
                if i>=4999: break
        status["rows"] = len(sample)
    except Exception as e:
        status["notes"]=f"read_error:{e}"
        report.append(status); continue

    miss = [c for c in req if c not in head]
    status["req_columns"] = "PASS" if not miss else f"FAIL(missing:{','.join(miss)})"

    if rates:
        vals = {c:[] for c in rates if c in head}
        for r in sample:
            for c in vals:
                vals[c].append(r.get(c))
        bad_cols = []
        for c, arr in vals.items():
            ok_pct = pct_in_01(arr)
            if ok_pct < 0.95:
                bad_cols.append(f"{c}:{ok_pct:.2f}")
        status["rate_sanity"] = "PASS" if not bad_cols else f"FAIL({';'.join(bad_cols)})"

    report.append(status)

MAP = Path(OUT, "mart_star_idfix.csv")
if MAP.exists():
    import csv
    n=0; filled=0
    with open(MAP, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        cols = rdr.fieldnames or []
        key_cols = [c for c in ["player_id","mlb_id","bbref_id","fg_id"] if c in cols]
        for i,row in enumerate(rdr):
            n += 1
            if any(row.get(k) for k in key_cols): filled+=1
            if i>=9999: break
    cov = 0 if n==0 else filled/n
    report.append({"file":"mart_star_idfix.csv","exists":True,"rows":n,
                   "req_columns":f"INFO(keys:{','.join(key_cols)})","rate_sanity":f"COVERAGE≈{cov:.2f}","notes":"light_audit"})

print("FILE\tEXISTS\tROWS\tREQ_COLUMNS\tRATE_SANITY\tNOTES")
for r in report:
    print(f"{r['file']}\t{r['exists']}\t{r['rows']}\t{r['req_columns']}\t{r['rate_sanity']}\t{r['notes']}")
PY

echo "[smoke] done $STAMP" | tee -a "$RLOG"
