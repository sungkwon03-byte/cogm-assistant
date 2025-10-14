import sys, requests, pandas as pd
API="https://statsapi.mlb.com/api/v1/teams?sportId=1&season={y}&activeStatus=all"

def build_venue_map(year:int):
    js = requests.get(API.format(y=year), timeout=20).json()
    m = {}
    for t in js.get("teams", []):
        abbr = t.get("abbreviation","") or t.get("teamCode","")
        venue = (t.get("venue") or {}).get("name","")
        if abbr: m[abbr] = venue
    return m

def fill(date_str, csv_path):
    year = int(date_str[:4])
    vm = build_venue_map(year)
    df = pd.read_csv(csv_path)

    # date 비었으면 모두 조회일로
    df.loc[df["date"].astype(str).str.strip().eq(""), "date"] = date_str
    # venue 비었으면 home 팀 기준으로 채움
    def fill_venue(row):
        v = str(row.get("venue","")).strip()
        if v: return v
        home = str(row.get("home","")).strip()
        return vm.get(home, v)
    df["venue"] = df.apply(fill_venue, axis=1)

    df = df[["game_pk","date","venue","home","away","home_runs","away_runs"]]
    df.to_csv(csv_path, index=False)
    print(f"[fill_by_teams] wrote {csv_path} ({len(df)} rows)")
if __name__=="__main__":
    date = sys.argv[1] if len(sys.argv)>1 else "2025-04-01"
    path = sys.argv[2] if len(sys.argv)>2 else "output/games.csv"
    fill(date, path)
