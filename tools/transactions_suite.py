# === Co-GM Week7 Suite (Day43–49) ===
# CLI: trade value, mock trades, team fit, FA forecast, waivers, mock draft, intl FA
# ex)
# python tools/transactions_suite.py day43_players --players data/players.csv --dollar_per_war 9e6 --discount_rate 0.08 --out output/trade_values.csv
# python tools/transactions_suite.py day44_v2 --players output/trade_values.csv --teams data/teams.csv --min_surplus 1e6 --out output/trade_proposals_v2.csv

import argparse
import numpy as np
import pandas as pd
from typing import List

def npv(cashflows: List[float], rate: float) -> float:
    return sum(cf / ((1.0 + rate) ** t) for t, cf in enumerate(cashflows, start=1))

# ---------- Day43: Trade Value ----------
def cmd_day43_players(args):
    df = pd.read_csv(args.players)
    vals, costs = [], []
    for _, r in df.iterrows():
        yrs = int(r.get("years", 0) or 0)
        w = [float(r.get(f"war_y{i}", 0) or 0) for i in range(1, yrs + 1)]
        s = [float(r.get(f"salary_y{i}", 0) or 0) for i in range(1, yrs + 1)]
        v = [wi * float(args.dollar_per_war) for wi in w]
        vals.append(npv(v, float(args.discount_rate)))
        costs.append(npv(s, float(args.discount_rate)))
    df["NPV_value"] = vals
    df["NPV_cost"] = costs
    df["Surplus"] = df["NPV_value"] - df["NPV_cost"]
    df.to_csv(args.out, index=False)
    print(f"[Day43] -> {args.out} ({len(df)} rows)")

def cmd_day43_package(args):
    p = pd.read_csv(args.players)
    pkg = pd.read_csv(args.package)
    m = pkg.merge(p, on="player", how="left")
    m["Surplus_total"] = m["qty"] * m["Surplus"]
    out = pd.DataFrame([{"package_surplus": float(m["Surplus_total"].sum())}])
    out.to_csv(args.out, index=False)
    print(f"[Day43] package -> {args.out}")

# ---------- Day44 (baseline) ----------
def cmd_day44(args):
    players = pd.read_csv(args.players)
    teams = pd.read_csv(args.teams)
    deals = []
    for _, row in players.iterrows():
        for _, t in teams.iterrows():
            if abs(float(row.get("Surplus", 0))) <= float(args.tolerance) * max(1.0, abs(float(row.get("NPV_value", 0)))):
                deals.append({"player": row.get("player"),
                              "team": t.get("team"),
                              "Surplus": float(row.get("Surplus", 0))})
    pd.DataFrame(deals).to_csv(args.out, index=False)
    print(f"[Day44] -> {args.out} ({len(deals)} deals)")

# ---------- Day44_v2 (practical proposals) ----------
def cmd_day44_v2(args):
    players = pd.read_csv(args.players)
    teams = pd.read_csv(args.teams)
    players = players.copy()
    players = players[players.get("Surplus", 0) >= float(args.min_surplus)].fillna({"team": "NA"})
    cols = [c for c in ["player", "team", "pos", "age", "hand", "Surplus"] if c in players.columns]
    players = players[cols]

    deals = []
    for _, p in players.iterrows():
        for _, t in teams.iterrows():
            if str(t.get("team")) == str(p.get("team")):
                continue
            deals.append({
                "player": p.get("player"),
                "from": p.get("team"),
                "to": t.get("team"),
                "pos": p.get("pos", ""),
                "age": p.get("age", ""),
                "hand": p.get("hand", ""),
                "surplus_$": round(float(p.get("Surplus", 0.0)), 2)
            })
    out = pd.DataFrame(deals)
    out.to_csv(args.out, index=False)
    print(f"[Day44_v2] -> {args.out} ({len(out)} deals)")

# ---------- Day45: Team Fit ----------
def cmd_day45(args):
    players = pd.read_csv(args.players)
    teams = pd.read_csv(args.teams)
    fits = []
    rng = np.random.default_rng(43)
    for _, p in players.iterrows():
        for _, t in teams.iterrows():
            surplus = float(p.get("Surplus", 0))
            age = float(p.get("age", 27)) if "age" in p else 27.0
            base = 0.5 + 5e-9 * surplus
            if str(t.get("state")) == "rebuild":
                base += 0.05 * (27 - min(age, 27))
            score = float(np.clip(base + rng.normal(0, 0.05), 0, 1))
            fits.append({"player": p.get("player"), "team": t.get("team"), "fit": score})
    pd.DataFrame(fits).to_csv(args.out, index=False)
    print(f"[Day45] -> {args.out}")

# ---------- Day46: FA Forecast ----------
def cmd_day46(args):
    m = pd.read_csv(args.market)
    m["exp_salary"] = m["proj_war"] * float(args.dollar_per_war) * np.exp(-0.05 * (m["age"] - 29))
    m.to_csv(args.out, index=False)
    print(f"[Day46] -> {args.out}")

# ---------- Day47: Waivers/Rule5 ----------
def cmd_day47(args):
    w = pd.read_csv(args.waivers)
    shortlist = w[(w["options"] <= 0) | (w["40man"] == 0) | (w["perf30d"] > 1.2)].copy()
    shortlist.to_csv(args.out, index=False)
    print(f"[Day47] -> {args.out} ({len(shortlist)})")

# ---------- Day48: Mock Draft ----------
def cmd_day48(args):
    p = pd.read_csv(args.prospects)
    tool_cols = [c for c in ["tools_hit", "pwr", "run", "arm", "glove"] if c in p.columns]
    p["score"] = p[tool_cols].mean(axis=1) * p.get("sign_prob", 1.0)
    picks = p.sort_values("score", ascending=False).reset_index(drop=True)
    picks.to_csv(args.out, index=False)
    print(f"[Day48] -> {args.out} ({len(picks)})")

# ---------- Day49: Intl FA ----------
def cmd_day49(args):
    pool = pd.read_csv(args.intl_pool)  # reserved for future pool checks
    prospects = pd.read_csv(args.prospects)
    prospects = prospects[prospects["age"] <= 18].copy()
    top = prospects.sort_values("tools_hit", ascending=False)
    top.to_csv(args.out, index=False)
    print(f"[Day49] -> {args.out} ({len(top)})")

# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser(prog="transactions_suite")
    sp = ap.add_subparsers(dest="cmd", required=True)

    p43p = sp.add_parser("day43_players")
    p43p.add_argument("--players", required=True)
    p43p.add_argument("--dollar_per_war", required=True, type=float)
    p43p.add_argument("--discount_rate", required=True, type=float)
    p43p.add_argument("--out", required=True)
    p43p.set_defaults(func=cmd_day43_players)

    p43pkg = sp.add_parser("day43_package")
    p43pkg.add_argument("--players", required=True)
    p43pkg.add_argument("--package", required=True)
    p43pkg.add_argument("--out", required=True)
    p43pkg.set_defaults(func=cmd_day43_package)

    p44 = sp.add_parser("day44")
    p44.add_argument("--players", required=True)
    p44.add_argument("--teams", required=True)
    p44.add_argument("--tolerance", required=True, type=float)
    p44.add_argument("--out", required=True)
    p44.set_defaults(func=cmd_day44)

    p44v2 = sp.add_parser("day44_v2")
    p44v2.add_argument("--players", required=True)
    p44v2.add_argument("--teams", required=True)
    p44v2.add_argument("--min_surplus", type=float, default=1e6)
    p44v2.add_argument("--out", required=True)
    p44v2.set_defaults(func=cmd_day44_v2)

    p45 = sp.add_parser("day45")
    p45.add_argument("--players", required=True)
    p45.add_argument("--teams", required=True)
    p45.add_argument("--out", required=True)
    p45.set_defaults(func=cmd_day45)

    p46 = sp.add_parser("day46")
    p46.add_argument("--market", required=True)
    p46.add_argument("--dollar_per_war", required=True, type=float)
    p46.add_argument("--out", required=True)
    p46.set_defaults(func=cmd_day46)

    p47 = sp.add_parser("day47")
    p47.add_argument("--waivers", required=True)
    p47.add_argument("--out", required=True)
    p47.set_defaults(func=cmd_day47)

    p48 = sp.add_parser("day48")
    p48.add_argument("--prospects", required=True)
    p48.add_argument("--out", required=True)
    p48.set_defaults(func=cmd_day48)

    p49 = sp.add_parser("day49")
    p49.add_argument("--intl_pool", required=True)
    p49.add_argument("--prospects", required=True)
    p49.add_argument("--out", required=True)
    p49.set_defaults(func=cmd_day49)

    args = ap.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
# === End Week7 Suite ===
