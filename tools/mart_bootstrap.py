import os, pandas as pd

LAH_PEOPLE = "data/lahman/People.csv"
LAH_APP    = "data/lahman/Appearances.csv"
LAH_TEAMS  = "data/lahman/Teams.csv"
OUT_DIR    = "mart"
OUT_FILE   = os.path.join(OUT_DIR, "players.csv")

os.makedirs(OUT_DIR, exist_ok=True)

def load_csv(p):
    if not os.path.exists(p):
        raise FileNotFoundError(p)
    return pd.read_csv(p)

people = load_csv(LAH_PEOPLE)
apps   = load_csv(LAH_APP)
teams  = load_csv(LAH_TEAMS)

# 기본 컬럼 정규화
for c in ["playerID","yearID","teamID"]:
    if c not in apps.columns: raise ValueError(f"Missing {c} in Appearances.csv")
if "nameFirst" not in people.columns or "nameLast" not in people.columns:
    raise ValueError("Missing nameFirst/nameLast in People.csv")

# MLB 시즌만 추정(Teams lgID in AL/NL)
mlb_years = set(teams.loc[teams["lgID"].isin(["AL","NL"]), "yearID"].unique().tolist())
apps = apps[apps["yearID"].isin(mlb_years)].copy()

# 2010~2025 범위로 좁혀 노이즈 제거 (원하면 수정)
apps = apps[(apps["yearID"]>=2010) & (apps["yearID"]<=2025)].copy()

# 이름/핸드 정보 결합(없으면 기본치)
sub_people = people[["playerID","nameFirst","nameLast"] + [c for c in ["bats","throws"] if c in people.columns]].copy()
apps = apps.merge(sub_people, on="playerID", how="left")

# 팀코드 → 팀명 보강(선택)
tmap = teams[["yearID","teamID","name"]].drop_duplicates()
tmap.rename(columns={"name":"teamName"}, inplace=True)
apps = apps.merge(tmap, on=["yearID","teamID"], how="left")

# 출력 스키마(최소)
apps["player_uid"] = apps["playerID"]
apps["season"]     = apps["yearID"]
apps["league"]     = "MLB"
apps["team"]       = apps["teamID"]
apps["name"]       = (apps["nameFirst"].fillna("") + " " + apps["nameLast"].fillna("")).str.strip()
if "bats" not in apps.columns: apps["bats"] = None
if "throws" not in apps.columns: apps["throws"] = None

out_cols = ["player_uid","season","league","team","teamName","name","bats","throws"]
apps[out_cols].drop_duplicates().to_csv(OUT_FILE, index=False)
print(f"[OK] wrote {OUT_FILE} rows={len(apps)}")
