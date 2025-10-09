import sys, pandas as pd, requests
API="https://statsapi.mlb.com/api/v1/teams?sportId=1&season={y}&activeStatus=all"

def venue_map(year:int):
    js = requests.get(API.format(y=year), timeout=20).json()
    m={}
    for t in js.get("teams",[]):
        ab = (t.get("abbreviation") or t.get("teamCode") or "").upper()
        vn = ((t.get("venue") or {}).get("name") or "").strip()
        if ab: m[ab]=vn
    return m

def is_missing(x):
    return pd.isna(x) or str(x).strip()=="" or str(x).strip().lower()=="nan"

def main(date_str="2025-04-01", path="output/games.csv"):
    year=int(date_str[:4])
    vm=venue_map(year)
    df=pd.read_csv(path, dtype={"game_pk":"int64","home_runs":"Int64","away_runs":"Int64"})
    # 문자열로 강제 캐스팅
    for col in ["date","venue","home","away"]:
        if col not in df.columns: df[col]=""
        df[col]=df[col].astype("string")  # pandas NA-aware string

    # date: 결측/빈값 → 모두 date_str
    df.loc[df["date"].map(is_missing), "date"] = date_str

    # venue: 결측/빈값 → home 약칭으로 매핑
    def fill_venue(row):
        v = row["venue"]
        if not is_missing(v): return v
        home = (row["home"] or "").upper()
        return vm.get(home, v)
    df["venue"] = df.apply(fill_venue, axis=1).astype("string")

    # 남은 결측을 빈문자로 정리
    df["venue"] = df["venue"].fillna("")

    # 컬럼 순서 보존, 모든 문자열 컬럼은 기본 string→object로 저장
    out = df[["game_pk","date","venue","home","away","home_runs","away_runs"]].astype({
        "date":"object","venue":"object","home":"object","away":"object"
    })
    out.to_csv(path, index=False)
    print(f"[fill_strict] wrote {path} ({len(out)} rows)")
if __name__=="__main__":
    args=sys.argv[1:]
    if len(args)==0: main()
    elif len(args)==1: main(args[0])
    else: main(args[0], args[1])
