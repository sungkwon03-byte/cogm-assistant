import pandas as pd, unicodedata as U, re, sys

def norm(s):
    if pd.isna(s): return ""
    s = str(s)
    s = U.normalize("NFKD", s)
    s = "".join(ch for ch in s if not U.combining(ch))
    s = re.sub(r"[.\-']", " ", s).upper()
    s = " ".join(s.split())
    return s

def split_name(s):
    s = norm(s)
    parts = s.split()
    if not parts: return "", ""
    first = parts[0]
    last  = parts[-1]
    return first, last

def main():
    spl = pd.read_csv("output/splits_merged.csv")
    pb  = pd.read_csv("output/player_box.csv", usecols=["mlb_id","name"]).dropna(subset=["mlb_id"]).drop_duplicates()
    # 인덱스용 컬럼
    pb["_FIRST"], pb["_LAST"] = zip(*pb["name"].map(split_name))
    # 매칭 대상만
    need = spl[spl["mlb_id"].isna()].copy()
    if need.empty:
        print("[fuzzy] nothing to link; no missing mlb_id")
        return
    fills = {}
    for i,row in need.iterrows():
        nm = row.get("name","")
        f,l = split_name(nm)
        cand = pb[pb["_LAST"]==l]
        if f:
            cand = cand[(cand["_FIRST"].str[0:1] == f[0:1])]
        cand = cand.drop_duplicates(subset=["mlb_id"])
        if len(cand)==1:
            fills[i] = int(cand["mlb_id"].iloc[0])
    if not fills:
        print("[fuzzy] no unique candidates; nothing filled")
    # 반영
    for i,mid in fills.items():
        spl.at[i,"mlb_id"] = mid
    spl.to_csv("output/splits_merged.csv", index=False)
    # 리포트
    print(f"[fuzzy] filled {len(fills)} rows; total {len(need)} missing before")
    # 남은 미매칭 목록
    remain = spl[spl["mlb_id"].isna()][["name"]]
    remain.to_csv("output/splits_unmatched.csv", index=False)
    print(f"[fuzzy] unmatched -> output/splits_unmatched.csv ({len(remain)} rows)")
if __name__=="__main__":
    main()
