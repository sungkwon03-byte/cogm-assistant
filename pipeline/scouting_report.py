import matplotlib.pyplot as plt, pandas as pd, numpy as np
from pathlib import Path
ROOT=Path.cwd(); OUT=ROOT/'output'; OUT.mkdir(exist_ok=True)
src=OUT/'statcast_features_player_year.csv'
out=OUT/'scouting_report.pdf'

def placeholder(msg):
    fig=plt.figure(figsize=(8.5,11)); plt.axis('off')
    plt.text(0.08,0.92,"Scouting Report", fontsize=20, weight='bold')
    plt.text(0.08,0.86,msg, fontsize=12)
    fig.savefig(out); plt.close(fig)
    print("[DAY67] scouting_report.pdf (placeholder)")

if not src.exists() or src.stat().st_size==0:
    placeholder("No statcast_features_player_year.csv"); raise SystemExit

df=pd.read_csv(src, low_memory=False)
if df.empty: placeholder("Empty statcast_features_player_year.csv"); raise SystemExit

yr=int(pd.to_numeric(df['year'], errors='coerce').max())
bat=df[(df['role']=='bat')&(df['year']==yr)].copy()
pit=df[(df['role']=='pit')&(df['year']==yr)].copy()
for c in ['xwOBA','avg_ev','hardhit_rate','barrel_rate','whiff_rate','o_swing_rate','z_contact_rate',
          'csw_rate','avg_spin','avg_ext','h_mov_in','v_mov_in','chase_rate']:
    if c not in df.columns: df[c]=np.nan
bat_top=bat.sort_values('xwOBA', ascending=False).head(12)
pit_top=pit.sort_values('csw_rate', ascending=False).head(12)

fig=plt.figure(figsize=(8.5,11)); plt.axis('off')
plt.text(0.08,0.94,f"Scouting Report — {yr}", fontsize=20, weight='bold')
plt.text(0.08,0.90,"Batters (xwOBA)", fontsize=14, weight='bold')
y=0.88
for _,r in bat_top.iterrows():
    line=f"{r.get('player_name','N/A'):<22s} xwOBA {r.get('xwOBA',np.nan):.3f} | EV {r.get('avg_ev',np.nan):.1f} | HH {r.get('hardhit_rate',np.nan):.3f} | Barrel {r.get('barrel_rate',np.nan):.3f} | Z-CT {r.get('z_contact_rate',np.nan):.3f} | Whiff {r.get('whiff_rate',np.nan):.3f}"
    plt.text(0.08,y,line, fontsize=9); y-=0.018
plt.text(0.08,y-0.02,"Pitchers (CSW / Spin / Ext / Movement)", fontsize=14, weight='bold'); y-=0.04
for _,r in pit_top.iterrows():
    line=f"{r.get('player_name','N/A'):<22s} CSW {r.get('csw_rate',np.nan):.3f} | Whiff {r.get('whiff_rate',np.nan):.3f} | Chase {r.get('chase_rate',np.nan):.3f} | Spin {r.get('avg_spin',np.nan):.0f} rpm | Ext {r.get('avg_ext',np.nan):.2f} ft | H {r.get('h_mov_in',np.nan):.1f}\" V {r.get('v_mov_in',np.nan):.1f}\""
    plt.text(0.08,y,line, fontsize=9); y-=0.018
fig.savefig(out); plt.close(fig)
print("[DAY67] scouting_report.pdf written")
