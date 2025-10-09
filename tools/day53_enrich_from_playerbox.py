import pandas as pd, sys
pb = pd.read_csv("output/player_box.csv", usecols=["mlb_id","name"])
pb = pb.dropna(subset=["mlb_id"]).drop_duplicates().astype({"mlb_id":"Int64"})
pb["retro_id"]=pd.NA; pb["bbref_id"]=pd.NA
# full_name
pb["full_name"]=pb["name"]
pb = pb[["mlb_id","retro_id","bbref_id","full_name"]]

# 기존 id_map 있으면 병합
try:
    m = pd.read_csv("output/id_map.csv")
except Exception:
    m = pd.DataFrame(columns=["mlb_id","retro_id","bbref_id","full_name"])
    # 일부 스켈레톤 파일 컬럼을 호환
    if "name_first_lah" in m.columns and "name_last_lah" in m.columns:
        m["full_name"] = (m["name_first_lah"].fillna("")+" "+m["name_last_lah"].fillna("")).str.strip()

# 통일된 스키마만 유지
keep_cols=["mlb_id","retro_id","bbref_id","full_name"]
for c in keep_cols:
    if c not in m.columns: m[c]=pd.NA
m = m[keep_cols].drop_duplicates()

# 우선순위: 기존 맵 우선, 없으면 player_box로 채움
merged = pd.concat([m, pb], ignore_index=True)\
          .drop_duplicates(subset=["mlb_id"], keep="first")\
          .reset_index(drop=True)

merged.to_csv("output/id_map.csv", index=False)
print(f"[enrich] output/id_map.csv rows={len(merged)}")
