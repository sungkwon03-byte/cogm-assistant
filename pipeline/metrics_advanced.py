import pandas as pd, numpy as np
from _util72 import ffind, to_num, safe_div
from pathlib import Path
OUT=Path.cwd()/'output'; OUT.mkdir(exist_ok=True)

bat=pd.read_csv(ffind('Batting.csv'), low_memory=False)
pit=pd.read_csv(ffind('Pitching.csv'), low_memory=False)

# ---- Batters
b=bat.copy()
b.rename(columns={'yearID':'year'}, inplace=True)
for c in ['AB','BB','HBP','SF','SH','SO','H','2B','3B','HR']: 
    if c not in b.columns: b[c]=0
b['1B']=to_num(b['H'])-to_num(b['2B'])-to_num(b['3B'])-to_num(b['HR'])
b['PA']=to_num(b['AB'])+to_num(b['BB'])+to_num(b['HBP'])+to_num(b['SF'])+to_num(b['SH'])
b['TB']=to_num(b['1B'])+2*to_num(b['2B'])+3*to_num(b['3B'])+4*to_num(b['HR'])
g_b=(b.groupby(['year','playerID'], as_index=False)
       .agg({'AB':'sum','BB':'sum','HBP':'sum','SF':'sum','SH':'sum','SO':'sum',
             'H':'sum','1B':'sum','2B':'sum','3B':'sum','HR':'sum','PA':'sum','TB':'sum'}))
g_b['AVG']=safe_div(g_b['H'], g_b['AB'])
g_b['SLG']=safe_div(g_b['TB'], g_b['AB'])
g_b['ISO']=g_b['SLG']-g_b['AVG']
g_b['BABIP']=safe_div(g_b['H']-g_b['HR'], g_b['AB']-g_b['SO']-g_b['HR']+g_b['SF'])
g_b['BB_pct']=safe_div(g_b['BB'], g_b['PA'])
g_b['K_pct']=safe_div(g_b['SO'], g_b['PA'])
# simple wOBA (weights ~ FanGraphs scale)
g_b['wOBA']=safe_div(0.69*g_b['BB']+0.72*g_b['HBP']+0.89*g_b['1B']+1.27*g_b['2B']+1.62*g_b['3B']+2.10*g_b['HR'], g_b['PA'])
g_b['role']='bat'

# ---- Pitchers
p=pit.copy()
p.rename(columns={'yearID':'year'}, inplace=True)
for c in ['HR','BB','SO','HBP','IPouts','ER','BFP']: 
    if c not in p.columns: p[c]=0
p['IP']=to_num(p['IPouts'])/3.0
g_p=(p.groupby(['year','playerID'], as_index=False)
       .agg({'HR':'sum','BB':'sum','SO':'sum','HBP':'sum','IPouts':'sum','ER':'sum','BFP':'sum'}))
g_p['IP']=to_num(g_p['IPouts'])/3.0
lg=(g_p.groupby('year', as_index=False)
       .agg(HR=('HR','sum'), BB=('BB','sum'), SO=('SO','sum'), HBP=('HBP','sum'),
            ER=('ER','sum'), IPouts=('IPouts','sum')))
lg['IP']=to_num(lg['IPouts'])/3.0
lg['ERA_lg']=safe_div(9*lg['ER'], lg['IP'])
lg['FIP_raw']=safe_div(13*lg['HR'] + 3*(lg['BB']+lg['HBP']) - 2*lg['SO'], lg['IP'])
lg['FIP_const']=lg['ERA_lg']-lg['FIP_raw']
cmap=lg.set_index('year')['FIP_const'].to_dict()
g_p['FIP']=safe_div(13*g_p['HR'] + 3*(g_p['BB']+g_p['HBP']) - 2*g_p['SO'], g_p['IP']) + g_p['year'].map(cmap)
g_p['HR9']=safe_div(9*g_p['HR'], g_p['IP'])
g_p['K_BB_pct']=safe_div(g_p['SO']-g_p['BB'], g_p['BFP'])
g_p['role']='pit'

adv=pd.concat([
    g_b[['year','playerID','role','PA','AVG','SLG','ISO','BABIP','BB_pct','K_pct','wOBA']],
    g_p[['year','playerID','role','IP','FIP','HR9','K_BB_pct']]
], ignore_index=True)
adv.to_csv(OUT/'advanced_metrics.csv', index=False)
print("[72] advanced_metrics.csv rows=", len(adv))
