# -*- coding: utf-8 -*-
"""
Day59 Matchup Simulator v0 (MLB only)
- Data source: Lahman Teams.csv (AL/NL)
- Model: Poisson scoring with offense/defense blending
- HFA: +0.14 runs to home mean
- Outputs:
  * output/matchup_samples/{year}_{A}_vs_{B}.csv  (score samples; optionally downsampled)
  * output/matchup_summary_{year}_{A}_vs_{B}.json (summary)
  * logs/matchup_sim_{timestamp}.log             (run log)
"""

import argparse, math, os, sys, re, json, time
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path.cwd()
OUT = ROOT / "output"
LOGS = ROOT / "logs"
OUT.mkdir(parents=True, exist_ok=True)
LOGS.mkdir(parents=True, exist_ok=True)

TS = time.strftime("%Y%m%d-%H%M%S")
LOGF = LOGS / f"matchup_sim_{TS}.log"

def log(msg: str):
    print(msg)
    with open(LOGF, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

# ---------- utils ----------
def find_lahman_csv(name_like: str) -> Path:
    cands = []
    for base in [ROOT/"data"/"lahman_extracted", ROOT/"data"/"lahman", ROOT/"lahman", ROOT/"data"]:
        if not base.exists(): continue
        for p in base.rglob("*.csv"):
            if re.search(name_like, p.name, re.IGNORECASE):
                cands.append(p)
    if not cands:
        raise FileNotFoundError(f"CSV not found for /{name_like}/ under data/…")
    # prefer deeper path (extracted set)
    cands = sorted(cands, key=lambda p: (-len(str(p)), p.name))
    return cands[0]

def normalize_key(s: str) -> str:
    s = s.upper()
    s = re.sub(r"[^A-Z0-9]", "", s)
    return s

# ---------- data ----------
def load_teams_df():
    teams_csv = find_lahman_csv(r"^Teams\.csv$")
    df = pd.read_csv(teams_csv)
    # keep MLB (AL/NL) only
    if "lgID" in df.columns:
        df = df[df["lgID"].isin(["AL","NL"])].copy()
    # keys for matching
    df["__key_teamid__"] = df["teamID"].astype(str).str.upper().map(normalize_key)
    name_col = "name" if "name" in df.columns else "nameOrig" if "nameOrig" in df.columns else None
    if name_col is None:
        df["__key_name__"] = ""
    else:
        df["__key_name__"] = df[name_col].astype(str).map(normalize_key)
    # basic rates
    df["G"] = df["G"].fillna(0).replace(0, np.nan)
    df["RS_per_g"] = (df["R"] / df["G"]).replace([np.inf, -np.inf], np.nan)
    df["RA_per_g"] = (df["RA"] / df["G"]).replace([np.inf, -np.inf], np.nan)
    return df

def pick_team_row(df, year, team_query: str):
    tq = normalize_key(team_query)
    cand = df[(df["yearID"] == year) & ((df["__key_teamid__"] == tq) | (df["__key_name__"] == tq))]
    if cand.empty:
        # try startswith (e.g., "YANKEES" for "NEWYORKYANKEES")
        cand = df[(df["yearID"] == year) & (df["__key_name__"].str.startswith(tq))]
    if cand.empty:
        # try franchiseID if available
        if "franchID" in df.columns:
            df["__key_fr__"] = df["franchID"].astype(str).map(normalize_key)
            cand = df[(df["yearID"] == year) & (df["__key_fr__"] == tq)]
    if cand.empty:
        raise ValueError(f"Team not found for year={year}, query={team_query}. "
                         f"Try Lahman teamID (e.g., NYA, BOS, LAD, ATL) or exact team name.")
    # if multiple (name changes mid-season), pick max G
    cand = cand.sort_values("G", ascending=False).iloc[0:1]
    return cand.iloc[0].to_dict()

# ---------- model ----------
def expected_means(rowA, rowB, league_rs_per_g: float, home: str, hfa_runs: float = 0.14):
    # Simple blend:
    # mu_A ≈ sqrt( A_off (RS/G) * B_def (RA/G) )
    # mu_B ≈ sqrt( B_off * A_def )
    mu_A = float(np.sqrt(max(rowA["RS_per_g"], 0) * max(rowB["RA_per_g"], 0)))
    mu_B = float(np.sqrt(max(rowB["RS_per_g"], 0) * max(rowA["RA_per_g"], 0)))

    # sanity fallback: if NaN, revert to league mean
    if not np.isfinite(mu_A): mu_A = league_rs_per_g
    if not np.isfinite(mu_B): mu_B = league_rs_per_g

    # home-field advantage
    if home.upper() == "A":
        mu_A += hfa_runs
    elif home.upper() == "B":
        mu_B += hfa_runs

    # clamp to reasonable MLB range (2–8 runs)
    mu_A = float(np.clip(mu_A, 2.0, 8.0))
    mu_B = float(np.clip(mu_B, 2.0, 8.0))
    return mu_A, mu_B

def simulate_poisson(mu_A, mu_B, nsims=10000, seed=42, extra_home_edge=0.54, home="A"):
    rng = np.random.default_rng(seed)
    a = rng.poisson(mu_A, size=nsims)
    b = rng.poisson(mu_B, size=nsims)

    # 동점 완전 해소: 홈팀이 p로 승, 아니면 원정 승
    ties = (a == b)
    if ties.any():
        r = rng.random(size=nsims)
        if home.upper() == "A":
            a = a + (ties & (r < extra_home_edge)).astype(int)
            b = b + (ties & (r >= extra_home_edge)).astype(int)
        else:
            b = b + (ties & (r < extra_home_edge)).astype(int)
            a = a + (ties & (r >= extra_home_edge)).astype(int)

    winA = (a > b).mean()
    winB = (b > a).mean()           # ties 없음 → 합계 1.0
    expA = a.mean()
    expB = b.mean()

    return {
        "winA": float(winA),
        "winB": float(winB),
        "exp_runs_A": float(expA),
        "exp_runs_B": float(expB),
        "qA": { "p10": float(np.quantile(a, 0.10)), "p50": float(np.quantile(a, 0.50)),
                "p90": float(np.quantile(a, 0.90)) },
        "qB": { "p10": float(np.quantile(b, 0.10)), "p50": float(np.quantile(b, 0.50)),
                "p90": float(np.quantile(b, 0.90)) },
        "nsims": int(nsims)
    }, a, b


def league_rs_per_g(df, year):
    t = df[df["yearID"] == year]
    if t.empty: raise ValueError(f"No teams for year={year}")
    rs = t["R"].sum()
    g  = t["G"].sum()
    return float(rs / g) if g else 4.5

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--teamA", type=str, required=True, help="Lahman teamID or name e.g. NYA, BOS, LAD, ATL")
    ap.add_argument("--teamB", type=str, required=True)
    ap.add_argument("--home",  type=str, default="A", choices=["A","B"])
    ap.add_argument("--nsims", type=int, default=20000)
    ap.add_argument("--seed",  type=int, default=59)
    ap.add_argument("--save_samples", action="store_true", help="save samples CSV (downsampled up to 100k rows)")
    ap.add_argument("--selfcheck", action="store_true", help="run cross check A↔B and A vs A")
    args = ap.parse_args()

    log(f"=== Day59 Matchup v0 ===")
    log(f"Params: year={args.year} A={args.teamA} B={args.teamB} home={args.home} nsims={args.nsims}")

    teams = load_teams_df()
    LRG = league_rs_per_g(teams, args.year)
    log(f"[INFO] league RS/G ~ {LRG:.3f}")

    A = pick_team_row(teams, args.year, args.teamA)
    B = pick_team_row(teams, args.year, args.teamB)
    log(f"[A] {A.get('name','')} ({A['teamID']}) RS/G={A['RS_per_g']:.3f} RA/G={A['RA_per_g']:.3f}")
    log(f"[B] {B.get('name','')} ({B['teamID']}) RS/G={B['RS_per_g']:.3f} RA/G={B['RA_per_g']:.3f}")

    muA, muB = expected_means(A, B, LRG, args.home)
    log(f"[MU] exp runs A={muA:.3f}, B={muB:.3f}")

    summary, a, b = simulate_poisson(muA, muB, nsims=args.nsims, seed=args.seed, home=args.home)
    log(f"[SIM] winA={summary['winA']:.3f} winB={summary['winB']:.3f} "
        f"expA={summary['exp_runs_A']:.2f} expB={summary['exp_runs_B']:.2f}")

    # sanity
    assert abs(summary["winA"] + summary["winB"] - 1.0) < 0.02, "win probs should sum ≈1"
    assert 2.0 <= summary["exp_runs_A"] <= 8.5 and 2.0 <= summary["exp_runs_B"] <= 8.5, "run means out of MLB range"

    # save outputs
    tag = f"{args.year}_{A['teamID']}_vs_{B['teamID']}"
    out_json = OUT / f"matchup_summary_{tag}.json"
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump({
            "year": args.year,
            "teamA": {"teamID": A["teamID"], "name": A.get("name","")},
            "teamB": {"teamID": B["teamID"], "name": B.get("name","")},
            "home": args.home,
            "league_rs_per_g": LRG,
            "mu": {"A": muA, "B": muB},
            **summary
        }, f, ensure_ascii=False, indent=2)

    log(f"[OUT] summary -> {out_json}")

    if args.save_samples:
        # downsample up to 100k rows to keep size manageable
        n = min(len(a), 100_000)
        idx = np.random.default_rng(args.seed+1).choice(len(a), size=n, replace=False)
        df_samp = pd.DataFrame({
            "year": args.year,
            "teamA": A["teamID"], "teamB": B["teamID"], "home": args.home,
            "runsA": a[idx], "runsB": b[idx]
        })
        out_csv = OUT / "matchup_samples" / f"{tag}.csv"
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        df_samp.to_csv(out_csv, index=False)
        log(f"[OUT] samples ({n} rows) -> {out_csv}")

    if args.selfcheck:
        # A↔B symmetry check
        muB2, muA2 = expected_means(B, A, LRG, "B" if args.home=="A" else "A")
        log(f"[XCHK] swap μ: A'={muA2:.3f}, B'={muB2:.3f} (should swap aside HFA)")
        # A vs A check ~ 50%
        mu_same, _ = expected_means(A, A, LRG, args.home)
        s2, _, _ = simulate_poisson(mu_same, mu_same, nsims=20000, seed=args.seed+7, home=args.home)
        log(f"[XCHK] {A['teamID']} vs {A['teamID']} winA≈{s2['winA']:.3f} (≈0.50 expected)")

    log("[DONE] Day59 Matchup v0")

if __name__ == "__main__":
    main()
