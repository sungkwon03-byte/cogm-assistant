#!/usr/bin/env python3
import os, json, re, datetime, pandas as pd, numpy as np

LOG=f"logs/team_agg_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.log"
def w(m): print(m); open(LOG,"a",encoding="utf-8").write(m+"\n")

WAR_CANDS = ["WAR","fWAR","war","WAR_total","Bat WAR","Pit WAR","war_total","war_used"]
TEAM_CANDS = ["Team","Tm","team","org","Org","group_id","team_id"]
SEASON_CANDS = ["Season","season","Year","year","season_std"]
MLB_ORGS={"ARI","ATL","BAL","BOS","CHC","CHW","CIN","CLE","COL","DET","HOU","KCR","LAA","LAD","MIA","MIL","MIN","NYM","NYY","OAK","PHI","PIT","SDP","SEA","SFG","STL","TBR","TEX","TOR","WSN"}
KBO_ORGS={"KIA","LOT","DOO","LG","SSG","KIW","HAN","SAM","NC","KT"}

def pick(df, cands):
    for c in cands:
        if c in df.columns: return c
    return None

def normalize_team(x):
    s=str(x).strip().upper()
    alias={"WSH":"WSN","WAS":"WSN","KC":"KCR","TB":"TBR","SD":"SDP","SF":"SFG"}
    return alias.get(s, s)

def load_best_sources():
    inv = json.load(open("logs/source_inventory.json"))
    chosen = {"MLB":None, "KBO":None}
    for league in ["MLB","KBO"]:
        # 1) 해당 리그이면서 WAR 비제로 행 존재
        candidates = [r for r in inv if r["league"]==league and r["war_col"] and r["nonzero_war_rows"]>0]
        # 2) 없으면 해당 리그이면서 war_col만 존재(0이라도)
        if not candidates:
            candidates = [r for r in inv if r["league"]==league and r["war_col"]]
        # 3) 그래도 없으면 리그=UNK에서 차선책
        if not candidates:
            candidates = [r for r in inv if r["league"]=="UNK" and r["war_col"]]
        if candidates:
            chosen[league] = candidates[0]  # nonzero 많은 순으로 이미 정렬되어 있음
    return chosen

def make_team_agg(league, src):
    if src is None:
        w(f"[-] {league}: WAR 소스를 찾지 못했습니다. 스킵.")
        return None
    path = src["path"]
    war_col = src["war_col"]
    season_col = src["season_col"]
    team_col = src["team_col"]

    # 로드
    df = pd.read_parquet(path) if path.endswith(".parquet") else pd.read_csv(path)
    # 컬럼 보정
    war = pick(df, WAR_CANDS) or war_col
    season = pick(df, SEASON_CANDS) or season_col
    team = pick(df, TEAM_CANDS) or team_col
    if war is None or season is None or team is None:
        w(f"[-] {league}: 필수 컬럼 부족(war/season/team) in {path}"); return None

    # 정리
    df = df.rename(columns={season:"season", team:"Team", war:"WAR"})
    df["season"] = pd.to_numeric(df["season"], errors="coerce")
    df = df.dropna(subset=["season"]).copy()
    df["season"] = df["season"].astype(int)

    df["Team"] = df["Team"].map(normalize_team)
    if league=="MLB":
        df = df[df["Team"].isin(MLB_ORGS)]
    elif league=="KBO":
        df = df[df["Team"].isin(KBO_ORGS)]

    # WAR 보강: Bat/Pit 분리형이면 합산
    if ("Bat WAR" in df.columns) or ("Pit WAR" in df.columns):
        df["WAR"] = pd.to_numeric(df.get("Bat WAR",0), errors="coerce").fillna(0) + \
                    pd.to_numeric(df.get("Pit WAR",0), errors="coerce").fillna(0)
    else:
        df["WAR"] = pd.to_numeric(df["WAR"], errors="coerce").fillna(0.0)

    if df.empty:
        w(f"[-] {league}: 필터 후 데이터가 비었습니다. ({path})")
        return None

    # 팀 집계
    agg = df.groupby(["season","Team"], as_index=False).agg(
        players = ("Team","size"),
        total_war = ("WAR","sum")
    )
    league_g = agg.groupby("season", as_index=False).agg(
        league_total_war=("total_war","sum"),
        league_avg_war=("total_war","mean")
    )
    out = agg.merge(league_g, on="season", how="left")
    out["avg_war"]   = out["total_war"] / out["players"].replace({0:np.nan})
    out["avg_age"]   = np.nan
    out["league_avg_age"] = np.nan
    out["age_diff"]  = np.nan
    out["war_share"] = out["total_war"] / out["league_total_war"].replace({0:np.nan})
    out["league"]    = league
    out["group_role"]= "org"
    out["group_id"]  = out["Team"]
    out["src_league"]= league
    out["total_pa_bf"]= np.nan

    cols = ["league","season","group_role","group_id","players","total_war","avg_war",
            "total_pa_bf","avg_age","league_total_war","league_avg_war","league_avg_age",
            "war_share","age_diff","src_league"]
    return out[cols]

def main():
    chosen = load_best_sources()
    frames = []
    for lg in ["MLB","KBO"]:
        f = make_team_agg(lg, chosen[lg])
        if f is not None: frames.append(f)
    if not frames:
        w("[-] 어떤 리그도 재생성하지 못했습니다."); return
    out = pd.concat(frames, ignore_index=True, sort=False)
    # NPB 금지: 혹시 포함됐으면 제거
    out = out[out["league"]!="NPB"]
    os.makedirs("output", exist_ok=True)
    out.to_csv("output/team_agg.csv", index=False)
    w(f"[OK] team_agg rebuilt(auto) -> output/team_agg.csv (rows={len(out)})")
    w("[PASS] 라벨 교정 완료 (MLB/KBO, NPB 제외)")
if __name__=="__main__": main()
