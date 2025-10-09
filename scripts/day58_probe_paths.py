#!/usr/bin/env python3
import os, json
roots=[".","data","mart","external","inputs","downloads","lahman"]
names={
 "mlb_people":["people.csv"],
 "mlb_bat":["batting.csv"],
 "mlb_pit":["pitching.csv"],
 "kbo_bat":["kbobattingdata.csv"],
 "kbo_pit":["kbopitchingdata.csv"],
 "kbo_det":["kbo_dataset_2018_2024.csv"],
}
res={k:[] for k in names}
for root in roots:
  for r,_,files in os.walk(root):
    rl=r.lower()
    if any(x in rl for x in ("/venv","\\venv","/env","\\env","/logs","\\logs","/output","\\output","/minor","\\minor")): 
      continue
    for f in files:
      fl=f.lower()
      for k,v in names.items():
        if fl in v:
          res[k].append(os.path.join(r,f))
print(json.dumps(res,indent=2,ensure_ascii=False))
os.makedirs("logs",exist_ok=True)
open("logs/day58_paths.json","w",encoding="utf-8").write(json.dumps(res,ensure_ascii=False,indent=2))
print("[OK] wrote logs/day58_paths.json")
