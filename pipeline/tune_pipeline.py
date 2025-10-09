import json, time
from pathlib import Path
ROOT=Path.cwd(); LOG=ROOT/'logs'; LOG.mkdir(exist_ok=True); OUT=ROOT/'output'
t0=time.time(); stats={}
for p in [OUT/'statcast_agg_player_year.csv', OUT/'mart_star.csv', OUT/'trade_value.csv']:
    if p.exists(): stats[str(p)]={'bytes':p.stat().st_size,'mtime':p.stat().st_mtime}
logf=LOG/'day60_64_mlb_multi.log'
hits=None
if logf.exists(): txt=logf.read_text(errors='ignore'); hits=txt.count("No columns to parse from file")
stats['statcast_cache_hits']=hits; stats['ran_at']=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
(LOG/'tune_profile.json').write_text(json.dumps(stats, indent=2), encoding='utf-8')
print("[DAY68] logs/tune_profile.json written")
