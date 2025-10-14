import pandas as pd, unicodedata as U

def norm(s):
    if pd.isna(s): return ""
    s = str(s)
    s = U.normalize("NFKD", s)
    s = "".join(ch for ch in s if not U.combining(ch))   # 악센트 제거
    s = s.replace(".", " ").replace("-", " ").strip().upper()
    s = " ".join(s.split())                              # 다중 공백 정리
    return s

spl = pd.read_csv("output/splits_merged.csv")
pb  = pd.read_csv("output/player_box.csv", usecols=["mlb_id","name"]).dropna(subset=["mlb_id"]).drop_duplicates()

spl["_name_norm"] = spl["name"].map(norm)
pb["_name_norm"]  = pb["name"].map(norm)

# 1) 정확(정규화) 일치 조인
m1 = spl.merge(pb[["mlb_id","_name_norm"]].drop_duplicates(), on="_name_norm", how="left", suffixes=("","_pb"))
# 2) splits에 mlb_id가 이미 있으면 유지, 없으면 pb에서 채움
m1["mlb_id"] = m1["mlb_id"].combine_first(m1["mlb_id_pb"])

m1.drop(columns=["_name_norm","mlb_id_pb"], inplace=True)
m1.to_csv("output/splits_merged.csv", index=False)
print(f"[Day52 attach] output/splits_merged.csv rows={len(m1)} with mlb_id_filled={(m1['mlb_id'].notna()).sum()}")
