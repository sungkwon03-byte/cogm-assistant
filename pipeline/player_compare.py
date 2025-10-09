import argparse, math, pandas as pd, numpy as np, matplotlib.pyplot as plt
from pathlib import Path
ROOT=Path.cwd(); OUT=ROOT/'output'; OUT.mkdir(exist_ok=True)
f=OUT/'statcast_features_player_year.csv'
M=['xwOBA','avg_ev','hardhit_rate','barrel_rate','whiff_rate','o_swing_rate','z_contact_rate','csw_rate']
def load():
    df=pd.read_csv(f,low_memory=False)
    for c in M:
        if c not in df.columns: df[c]=np.nan
    return df
def pick(df, who, year=None, role=None):
    d=df.copy()
    if role: d=d[d['role']==role]
    if year: d=d[d['year']==year]
    mask=False
    for w in who:
        if w.isdigit():
            mask = mask | (d['mlbam'].astype(str)==w)
        else:
            mask = mask | d['player_name'].str.contains(w, case=False, na=False)
    d=d[mask]
    return d.sort_values(['player_name','year'])
def radar(rows, title):
    if rows.empty: return
    labels=M; K=len(labels)
    ang=np.linspace(0,2*np.pi,K,endpoint=False).tolist(); ang+=ang[:1]
    fig=plt.figure(figsize=(6,6)); ax=plt.subplot(111, polar=True)
    for _,r in rows.iterrows():
        vals=[r.get(k,np.nan) for k in labels]
        # 0-1 스케일 근사
        arr=pd.Series(vals).astype(float)
        v=((arr-arr.min())/(arr.max()-arr.min()+1e-9)).fillna(0).tolist(); v+=v[:1]
        ax.plot(ang,v,label=f"{r.get('player_name','?')} {int(r.get('year',0))}")
        ax.fill(ang,v, alpha=0.1)
    ax.set_xticks(ang[:-1]); ax.set_xticklabels(labels); ax.set_title(title); ax.legend(loc='upper right', fontsize=8)
    out=OUT/f"player_compare_{title.replace(' ','_')}.png"; fig.savefig(out, dpi=130, bbox_inches='tight'); plt.close(fig)
    print(f"[COMPARE] figure -> {out}")
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--players', nargs='+', required=True, help='이름 일부 또는 MLBAM ID')
    ap.add_argument('--year', type=int, default=None)
    ap.add_argument('--role', choices=['bat','pit'], default=None)
    args=ap.parse_args()
    df=load(); rows=pick(df, args.players, args.year, args.role)
    out=OUT/'player_compare_rows.csv'; rows.to_csv(out, index=False); print(f"[COMPARE] rows -> {out} ({len(rows)})")
    radar(rows.head(3), f"{'&'.join(args.players)}_{args.year or 'ALL'}_{args.role or 'ALL'}")
if __name__=='__main__': main()
