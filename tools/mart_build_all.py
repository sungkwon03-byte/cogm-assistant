import os, sys, json
import pandas as pd

SEASON_FROM = int(os.environ.get("FROM","1901"))
SEASON_TO   = int(os.environ.get("TO","2025"))
STATCAST_MIN = int(os.environ.get("STATCAST_MIN","2008"))  # 정책상 Statcast 시작 가정치
USE_CHAD = os.path.exists("data/chadwick/people.csv")

def must(p):
    if not os.path.exists(p):
        sys.exit(f"ERROR: missing {p}")
    return p

# 필수 Lahman
must("data/lahman/People.csv")
must("data/lahman/Appearances.csv")
must("data/lahman/Teams.csv")

people = pd.read_csv("data/lahman/People.csv")
apps   = pd.read_csv("data/lahman/Appearances.csv")
teams  = pd.read_csv("data/lahman/Teams.csv")

if USE_CHAD:
    chad = pd.read_csv("data/chadwick/people.csv")
    # 간단 이름 키 (정확 매핑은 id_map 권장)
    if {"name_first","name_last"}.issubset(chad.columns):
        chad["name"] = (chad["name_first"].fillna("")+" "+chad["name_last"].fillna("")).str.strip()
    else:
        USE_CHAD = False

os.makedirs("mart", exist_ok=True)

def build_year(y:int):
    # MLB 시즌 판별: Teams 테이블에서 AL/NL 존재 기준
    teams_y = teams[(teams["yearID"]==y) & (teams["lgID"].isin(["AL","NL"]))][["yearID","teamID","name"]].drop_duplicates()
    if teams_y.empty:
        return f"[SKIP] {y}: no AL/NL teams"
    teams_y = teams_y.rename(columns={"name":"teamName"})

    # 해당 시즌 출전자
    a = apps[apps["yearID"]==y][["playerID","yearID","teamID"]].drop_duplicates()
    if a.empty:
        return f"[SKIP] {y}: no appearances"

    cols = ["playerID","nameFirst","nameLast"]
    if "bats" in people.columns: cols.append("bats")
    if "throws" in people.columns: cols.append("throws")
    ppl = people[cols].copy()

    df = (a.merge(ppl, on="playerID", how="left")
            .merge(teams_y, on=["yearID","teamID"], how="left"))

    df["player_uid"] = df["playerID"]
    df["season"]     = df["yearID"]
    df["league"]     = "MLB"
    df["team"]       = df["teamID"]
    df["name"]       = (df["nameFirst"].fillna("")+" "+df["nameLast"].fillna("")).str.strip()
    if "bats"   not in df.columns: df["bats"] = None
    if "throws" not in df.columns: df["throws"] = None

    # Chadwick로 bats/throws/이름 보강(가능할 때만)
    if USE_CHAD:
        take = ["name"]
        if "bats" in chad.columns:   take.append("bats")
        if "throws" in chad.columns: take.append("throws")
        df = df.merge(chad[take].drop_duplicates(), on="name", how="left", suffixes=("","_ch"))
        if "bats_ch" in df.columns:
            df["bats"] = df["bats"].fillna(df["bats_ch"])
        if "throws_ch" in df.columns:
            df["throws"] = df["throws"].fillna(df["throws_ch"])

    out = df[["player_uid","season","league","team","teamName","name","bats","throws"]].drop_duplicates()

    # 파일명 정책: statcast 있는 해는 동일하게 mlb_<y> 형식(다운스트림 호환)
    out_path = f"mart/mlb_{y}_players.csv"
    out.to_csv(out_path, index=False)
    return f"[OK] {y}: rows={len(out)} -> {out_path}"

logs=[]
for y in range(SEASON_FROM, SEASON_TO+1):
    try:
        logs.append(build_year(y))
    except Exception as e:
        logs.append(f"[ERR] {y}: {e}")

print("\n".join(logs))
