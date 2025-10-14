import pandas as pd, unicodedata as U, re, sys
from difflib import get_close_matches

def norm(s):
    if pd.isna(s): return ""
    s = str(s)
    s = U.normalize("NFKD", s)
    s = "".join(ch for ch in s if not U.combining(ch))
    s = re.sub(r"[.\-']", " ", s).upper()
    s = " ".join(s.split())
    return s

def top_matches(name, pool, n=3):
    cand = get_close_matches(norm(name), [norm(x) for x in pool], n=n, cutoff=0.6)
    # 원본 이름 반환
    rev = {norm(x):x for x in pool}
    return [rev.get(c,c) for c in cand]

def main():
    s = pd.read_csv("output/splits_merged.csv")
    m = pd.read_csv("output/id_map.csv")
    pb = pd.read_csv("output/player_box.csv", usecols=["mlb_id","name"]).dropna(subset=["mlb_id"]).drop_duplicates()

    # 1) splits 내부 결측 통계
    total = len(s)
    miss_id = s["mlb_id"].isna().sum()
    miss_name = s["name"].isna().sum()
    print(f"[diag] splits rows={total}, missing mlb_id={miss_id}, missing name={miss_name}")

    # 2) player_box와 이름 정규화로 직접 매칭 시도(가능성 확인)
    s["_name_norm"] = s["name"].map(norm)
    pb["_name_norm"] = pb["name"].map(norm)
    s2 = s.merge(pb[["_name_norm","mlb_id"]].drop_duplicates(), on="_name_norm", how="left", suffixes=("","_pb"))
    filled = s2["mlb_id"].fillna(s2["mlb_id_pb"])
    fillable = filled.notna().sum()
    print(f"[diag] name-normalized join could fill mlb_id for {fillable}/{total}")

    # 3) 실제 미매칭 목록과 근접 후보 제시
    s_un = s2[filled.isna()].copy()
    s_un = s_un[["name"]].drop_duplicates()
    if not s_un.empty:
        pb_names = pb["name"].dropna().unique().tolist()
        rows=[]
        for nm in s_un["name"]:
            rows.append({
                "splits_name": nm,
                "suggest_candidates": "; ".join(top_matches(nm, pb_names, n=3)) or "(none)"
            })
        rep = pd.DataFrame(rows)
        rep.to_csv("output/splits_unmatched_diag.csv", index=False)
        print(f"[diag] unmatched: {len(rep)} → output/splits_unmatched_diag.csv")
    else:
        print("[diag] unmatched: 0")

    # 4) id_map 키 정합
    s["key"] = s["mlb_id"].fillna("").astype(str).where(lambda x: x!="", s["name"])
    m["key"] = m.get("mlb_id", pd.Series(dtype="object")).fillna("").astype(str).where(lambda x: x!="", m.get("full_name", ""))
    joined = s.merge(m[["key"]].drop_duplicates(), on="key", how="left", indicator=True)
    hit = (joined["_merge"]=="both").sum()
    print(f"[diag] id_map joinable keys: {hit}/{total}")
if __name__=="__main__":
    main()
