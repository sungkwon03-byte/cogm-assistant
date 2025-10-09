import pandas as pd, numpy as np, matplotlib.pyplot as plt
from pathlib import Path
ROOT=Path.cwd(); OUT=ROOT/'output'
f=OUT/'weakness_map_player_year.csv'
if not f.exists(): 
    print("[SKIP] weakness_map_player_year.csv not found"); raise SystemExit
df=pd.read_csv(f, low_memory=False)
# 즉석 보강
for c in ['z_contact_rate','edge_rate','zone_rate','heart_rate','chase_rate']:
    if c not in df: df[c]=np.nan
if 'weak_zone_edge' not in df:
    df['weak_zone_edge']=(1-df['z_contact_rate'])*df['edge_rate']*df['zone_rate']
if 'heart_chase_idx' not in df:
    df['heart_chase_idx']=df['chase_rate']*df['heart_rate']
if 'zone_edge_balance' not in df:
    df['zone_edge_balance']=df['zone_rate']*df['edge_rate']
yr=int(df['year'].max())
top=df[df['year']==yr].copy()
if 'weak_zone_edge' in top:
    top=top.sort_values('weak_zone_edge', ascending=False).head(8)
pdf=OUT/'weakness_cards.pdf'
for _,r in top.iterrows():
    fig=plt.figure(figsize=(8.5,5)); plt.axis('off')
    def g(name): 
        try: return float(r.get(name, float('nan')))
        except: return float('nan')
    txt=(f"{int(r.get('mlbam',0))} — {yr}\n"
         f"• Zone-Edge miss idx: {g('weak_zone_edge'):.3f}\n"
         f"• Heart chase idx   : {g('heart_chase_idx'):.3f}\n"
         f"• Zone/Edge balance : {g('zone_edge_balance'):.3f}")
    plt.text(0.06,0.9,"Weakness Profile", fontsize=16, weight='bold')
    plt.text(0.06,0.8,txt, fontsize=12)
    fig.savefig(pdf) if pdf.exists() else fig.savefig(pdf); plt.close(fig)
print("[OK] weakness_cards.pdf")
