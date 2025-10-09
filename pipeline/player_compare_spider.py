import pandas as pd, numpy as np, matplotlib.pyplot as plt
from pathlib import Path
from _util_safe import to_num
ROOT=Path.cwd(); OUT=ROOT/'output'
df=pd.read_csv(OUT/'statcast_features_player_year.csv', low_memory=False)
yr=int(df['year'].max()); pool=df[df['year']==yr].copy()
pool['label']=pool['player_name'].fillna(pool['mlbam'].astype(str))
metrics=['xwOBA','avg_ev','hardhit_rate','barrel_rate','whiff_rate','z_csw_rate','chase_rate']
for c in metrics: pool[c]=to_num(pool[c])
top=pool.sort_values('xwOBA', ascending=False).head(3)
# z-norm
for c in metrics:
    m=pool[c].mean(); s=pool[c].std() or 1
    top[c]=(top[c]-m)/s
# radar
ang=np.linspace(0,2*np.pi,len(metrics),endpoint=False); ang=np.r_[ang,ang[:1]]
fig=plt.figure(figsize=(8,8)); ax=plt.subplot(111, polar=True)
for _,r in top.iterrows():
    vals=[r[c] for c in metrics]; vals+=vals[:1]
    ax.plot(ang, vals, lw=2, label=r['label']); ax.fill(ang, vals, alpha=0.1)
ax.set_thetagrids(np.degrees(ang[:-1]), metrics); ax.legend(loc='upper right', bbox_to_anchor=(1.3,1.1))
fig.savefig(OUT/f"player_compare_spider_{yr}.png", bbox_inches='tight'); plt.close(fig)
print("[OK] player_compare_spider_", yr)
