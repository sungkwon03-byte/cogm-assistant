#!/usr/bin/env python3
import os, glob, pandas as pd, numpy as np, argparse, re, json

MLB_ORGS = {
 "ARI","ATL","BAL","BOS","CHC","CHW","CIN","CLE","COL","DET",
 "HOU","KCR","LAA","LAD","MIA","MIL","MIN","NYM","NYY","OAK",
 "PHI","PIT","SDP","SEA","SFG","STL","TBR","TEX","TOR","WSN"
}
KBO_CODES = {
 "KIA","LOT","DOO","LG","SSG","KIW","HAN","SAM","NC","KT",
 "KIA TIGERS","LOTTE","DOOSAN","LG TWINS","SSG LANDERS",
 "KIWOOM","HANWHA","SAMSUNG","NC DINOS","KT WIZ"
}

def _series_or_default(df: pd.DataFrame, candidates, default="unknown"):
    for c in candidates:
        if c in df.columns:
            return df[c]
    return pd.Series([default] * len(df), index=df.index)

def _norm_str(s: pd.Series) -> pd.Series:
    return (s.astype(str)
              .str.normalize("NFKD")
              .str.encode("ascii", "ignore").str.decode("ascii")
              .str.strip().str.lower()
              .str.replace(r"\s+", "_", regex=True))

def build_uid(df: pd.DataFrame, season: int, league: str) -> pd.Series:
    base = _norm_str(_series_or_default(df, ["Name","Player","player_name"], "unknown"))
    team = _series_or_default(df, ["Team","team","org"], "NA").astype(str).str.upper().replace({"": "NA"})
    pos  = _series_or_default(df, ["Pos","position"], "XX").astype(str).str.upper().replace({"": "XX"})
    lg = (league or "UNK").upper()
    return base + "_" + pd.Series([season]*len(df), index=df.index).astype(str) + "_" + team + "_" + pos + "_" + lg

def detect_league(df: pd.DataFrame, fpath:str) -> str:
    path = fpath.lower()
    if "kbo" in path: return "KBO"
    if "milb" in path or "minor" in path: return "MiLB"
    if "mlb" in path: return "MLB"
    teams = set(str(t).upper() for t in _series_or_default(df, ["Team","team","org"], "").dropna().unique())
    if any(t in MLB_ORGS for t in teams): return "MLB"
    if any(t in KBO_CODES for t in teams): return "KBO"
    return "MLB"

def discover_src_dir(cli_src:str|None) -> str:
    if cli_src and os.path.isdir(cli_src): return cli_src
    if os.path.isdir("data/fangraphs"): return "data/fangraphs"
    if os.path.exists("pathcache.json"):
        try:
            j = json.load(open("pathcache.json","r",encoding="utf-8"))
            if os.path.isdir(j.get("fg_dir","")): return j["fg_dir"]
        except Exception: pass
    # fallback: project-wide search
    for root in ["data","raw","downloads","input","inputs",".","/workspaces/cogm-assistant"]:
        if os.path.isdir(root):
            return root
    raise FileNotFoundError("Fangraphs CSV 경로를 찾지 못했습니다.")

def main(args):
    src = discover_src_dir(args.src)
    files = glob.glob(os.path.join(src, "**", "*.csv"), recursive=True)
    files = [f for f in files if os.path.getsize(f) > 0]
    if not files:
        raise FileNotFoundError(f"{src} 내 CSV가 없습니다. CSV를 넣고 다시 실행하세요.")

    by_league: dict[str, list[pd.DataFrame]] = {"MLB":[], "KBO":[], "MiLB":[]}

    for f in files:
        try:
            df = pd.read_csv(f)
        except Exception:
            continue
        if df.empty: continue
        league = detect_league(df, f)
        season = args.season or 2024
        df["player_uid"] = build_uid(df, season, league)
        by_league.setdefault(league, []).append(df)

    os.makedirs("mart", exist_ok=True)
    written = 0
    for lg, parts in by_league.items():
        if not parts: continue
        merged = pd.concat(parts, ignore_index=True, sort=False)
        # 기본 중복 제거 키
        dedup_keys = [c for c in ["player_uid","Name","Player","player_name","Team","Pos"] if c in merged.columns]
        merged = merged.drop_duplicates(subset=dedup_keys, keep="first")
        out = f"mart/{lg.lower()}_{args.season}_players.csv"
        merged.to_csv(out, index=False)
        print(f"[OK] {lg}: {out} ({len(merged)} rows)")
        written += 1

    if written == 0:
        raise SystemExit("[-] 유효한 인제스트 결과가 없습니다.")

    return 0

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--season", type=int, default=2024, help="시즌 연도")
    p.add_argument("--src", type=str, default=None, help="FG CSV 루트(미지정 시 자동 탐색)")
    args = p.parse_args()
    raise SystemExit(main(args))
