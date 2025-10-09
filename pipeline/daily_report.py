import pandas as pd, numpy as np, datetime as dt
from pathlib import Path
ROOT=Path.cwd(); OUT=ROOT/'output'
f=OUT/'statcast_features_player_year.csv'
df=pd.read_csv(f, low_memory=False)
yr=int(df['year'].max())
b=df[(df.get('role','bat')=='bat')&(df['year']==yr)].copy()
p=df[(df.get('role','pit')=='pit')&(df['year']==yr)].copy()
top_b=b.sort_values('xwOBA', ascending=False).head(10)[['player_name','xwOBA','avg_ev','barrel_rate','chase_rate']]
top_p=p.sort_values('csw_rate', ascending=False).head(10)[['player_name','csw_rate','avg_spin','avg_ext','z_whiff_rate']]
lines=["# Daily Report (latest available cache)\n\n",
       f"- Generated: {dt.datetime.utcnow().isoformat()}Z\n\n",
       f"## Hitters — Top xwOBA ({yr})\n", top_b.to_markdown(index=False), "\n\n",
       f"## Pitchers — Top CSW% ({yr})\n", top_p.to_markdown(index=False), "\n"]
(Path(OUT/'daily_report_latest.md')).write_text("".join(lines), encoding='utf-8'); print("[OK] daily_report_latest.md")
