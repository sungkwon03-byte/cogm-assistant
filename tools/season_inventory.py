import os, re, json
BASE = "output/cache/statcast_clean"
seasons = set()
if os.path.isdir(BASE):
    for r,_,fs in os.walk(BASE):
        for f in fs:
            m = re.search(r'(\d{4})', f)
            if m:
                y = int(m.group(1))
                if 1900 <= y <= 2100:
                    seasons.add(y)
print(json.dumps({"detected": sorted(seasons), "assumed_min_year": 2008}, ensure_ascii=False, indent=2))
