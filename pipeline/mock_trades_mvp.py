import itertools, pandas as pd, numpy as np
from pathlib import Path
ROOT=Path.cwd(); OUT=ROOT/'output'
p=OUT/'trade_value.csv'
if not p.exists(): print("[SKIP] trade_value.csv not found"); raise SystemExit
tv=pd.read_csv(p, low_memory=False)

def pick(s,*names, default=0.0):
    for n in names:
        if n in s: return pd.to_numeric(s[n], errors='coerce').fillna(0.0)
    return pd.Series(default, index=s.index, dtype=float)

# TV score 로버스트 산정
WAR   = pick(tv,'WAR','WARx','fWAR','bWAR')
salMM = pick(tv,'salaryMM','salary_mm', default=0.0)
if 'salary' in tv and (salMM==0).all():
    salMM = pick(tv,'salary',default=0.0)/1e6
base_score = WAR.copy()
# WAR/$ 있으면 가중, 아니면 WAR 자체를 사용
eff = (WAR / salMM.replace(0,np.nan)).replace([np.inf,-np.inf],np.nan)
TV_score = np.where(pd.notna(eff), eff.clip(0,20), base_score.clip(lower=0))

team_col = 'teamID' if 'teamID' in tv.columns else ('Tm' if 'Tm' in tv.columns else None)
if team_col is None: print("[SKIP] team column missing"); raise SystemExit

need = [c for c in [team_col,'playerID','player_name'] if c in tv.columns]
base = tv[need].copy().assign(TV_score=TV_score).fillna({'TV_score':0})
# 최소 기준
base = base[base['TV_score']>0].copy()
if base.empty and 'WAR' in tv.columns:
    base = tv[need+['WAR']].rename(columns={'WAR':'TV_score'})
    base = base[pd.to_numeric(base['TV_score'],errors='coerce')>0]

if base.empty:
    pd.DataFrame(columns=['teamA','teamB','playersA','playersB','tvA','tvB','delta']) \
      .to_csv(OUT/'mock_trades_mvp.csv', index=False)
    print("[OK] mock_trades_mvp.csv 0"); raise SystemExit

top_by_team=(base.sort_values([team_col,'TV_score'], ascending=[True, False])
                 .groupby(team_col).head(20))
teams=sorted(top_by_team[team_col].unique().tolist())

rows=[]
for i,a in enumerate(teams):
    A=top_by_team[top_by_team[team_col]==a].copy()
    for b in teams[i+1:]:
        B=top_by_team[top_by_team[team_col]==b].copy()
        for kA,kB in [(1,1),(1,2),(2,1)]:
            if A.empty or B.empty: continue
            for As in itertools.combinations(A.index, min(kA, len(A))):
                tvA=float(A.loc[list(As),'TV_score'].sum())
                B2=B.iloc[:12]
                for Bs in itertools.combinations(B2.index, min(kB, len(B2))):
                    tvB=float(B2.loc[list(Bs),'TV_score'].sum())
                    tot=max(tvA,tvB) or 1.0
                    delta=abs(tvA-tvB)/tot
                    if delta<=0.10:
                        rows.append({ 'teamA':a,'teamB':b,
                                      'playersA':",".join(A.loc[list(As),'playerID'].astype(str).tolist()) if 'playerID' in A else "",
                                      'playersB':",".join(B2.loc[list(Bs),'playerID'].astype(str).tolist()) if 'playerID' in B2 else "",
                                      'tvA':tvA,'tvB':tvB,'delta':delta})

# 완전 0건이면 1:1 근사 매칭으로 반드시 delta 포함
if not rows:
    tmp=[]
    for i,a in enumerate(teams):
        A=top_by_team[top_by_team[team_col]==a].head(1)
        for b in teams[i+1:]:
            B=top_by_team[top_by_team[team_col]==b].head(1)
            tvA=float(A['TV_score'].sum()); tvB=float(B['TV_score'].sum())
            tot=max(tvA,tvB) or 1.0
            tmp.append({'teamA':a,'teamB':b,
                        'playersA':",".join(A.get('playerID',pd.Series([],dtype=object)).astype(str)) if 'playerID' in A else "",
                        'playersB':",".join(B.get('playerID',pd.Series([],dtype=object)).astype(str)) if 'playerID' in B else "",
                        'tvA':tvA,'tvB':tvB,'delta':abs(tvA-tvB)/tot})
    rows=tmp

mvp=pd.DataFrame(rows)
if mvp.empty:
    mvp=pd.DataFrame(columns=['teamA','teamB','playersA','playersB','tvA','tvB','delta'])
mvp=mvp.sort_values('delta', ascending=True, na_position='last').head(200)
mvp.to_csv(OUT/'mock_trades_mvp.csv', index=False)
print("[OK] mock_trades_mvp.csv", len(mvp))
