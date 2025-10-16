import json,os
def ok(p): return os.path.exists(p) and os.path.getsize(p)>0
res={"ok":all(map(ok,["output/player_cards_all.parquet","output/statcast_ultra_full_clean.parquet","output/id_map.csv"])),"issues":[]}
print(json.dumps(res))
