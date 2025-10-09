import pandas as pd, numpy as np, re
from pathlib import Path

ROOT=Path.cwd(); OUT=ROOT/'output'; OUT.mkdir(exist_ok=True)

def _find(*pats):
    for pat in pats:
        c=list((ROOT/'data'/'lahman_extracted').rglob(pat)) or list((ROOT/'data').rglob(pat))
        if c:
            c.sort(key=lambda p:p.stat().st_size, reverse=True)
            return c[0]
    return None

def norm_id_ser(s):
    s = pd.Series(s, dtype="object")
    s = s.astype(str).str.strip()
    s = s.str.replace(r'\.0$', '', regex=True)
    s = s.replace({'nan':'','NaN':'','<NA>':'','None':'','NULL':'','NaT':''})
    return s

src = OUT/'mart_star.csv'
df  = pd.read_csv(src, dtype=str, low_memory=False)

# 보장 컬럼
for k in ['bbrefID','retroID','mlbam','fgID','player_name']:
    if k not in df.columns: df[k]=''

# 표준화
for k in ['bbrefID','retroID','mlbam','fgID']:
    df[k] = norm_id_ser(df[k])

# People/Master (bbrefID <-> retroID)
people_path = _find('People.csv','Master.csv')
if people_path:
    people = pd.read_csv(people_path, dtype=str, low_memory=False).fillna('')
    keep_p = [c for c in ['bbrefID','retroID','nameFirst','nameLast','birthYear'] if c in people.columns]
    people = people[keep_p].copy()
    for k in ['bbrefID','retroID']:
        if k in people: people[k] = norm_id_ser(people[k])
else:
    people = pd.DataFrame(columns=['bbrefID','retroID','nameFirst','nameLast','birthYear'])

# Chadwick register (mlbam/fgID)
reg = None
for pat in ['chadwick*register*.csv','*Chadwick*.csv','*chadwick*.csv','chadwick_register.csv']:
    cc = list((ROOT/'data').rglob(pat))
    if cc:
        cc.sort(key=lambda p:p.stat().st_size, reverse=True)
        reg = cc[0]; break

if reg is not None:
    r = pd.read_csv(reg, dtype=str, low_memory=False).fillna('')
    alias = {'key_bbref':'bbrefID','bbref':'bbrefID',
             'key_retro':'retroID','retro':'retroID',
             'key_mlbam':'mlbam','mlbam':'mlbam','mlb_id':'mlbam',
             'key_fangraphs':'fgID','fg_id':'fgID','fangraphs':'fgID',
             'name_first':'nameFirst','name_last':'nameLast','birth_year':'birthYear'}
    r.columns = [alias.get(c,c) for c in r.columns]
    keep_r = [c for c in ['bbrefID','retroID','mlbam','fgID','nameFirst','nameLast','birthYear'] if c in r.columns]
    r = r[keep_r].copy()
    for k in ['bbrefID','retroID','mlbam','fgID']:
        if k in r: r[k] = norm_id_ser(r[k])
else:
    r = pd.DataFrame(columns=['bbrefID','retroID','mlbam','fgID','nameFirst','nameLast','birthYear'])

# 이름 파싱
nm = df['player_name'].fillna('').str.split(',', n=1, expand=True)
df['nameLast']  = nm[0].str.strip() if nm.shape[0] else ''
df['nameFirst'] = nm[1].str.strip() if nm.shape[1] > 1 else ''
df['birthYear'] = ''  # 로컬에 없으면 공란

# 1) bbref -> retro (People)
if not people.empty and set(['bbrefID','retroID']).issubset(people.columns):
    m = df.merge(people[['bbrefID','retroID']].drop_duplicates(), on='bbrefID', how='left', suffixes=('','_p1'))
    df['retroID'] = np.where(df['retroID'].str.len()>0, df['retroID'], norm_id_ser(m['retroID_p1']))
# 2) retro -> bbref (People)
if not people.empty and set(['bbrefID','retroID']).issubset(people.columns):
    m = df.merge(people[['bbrefID','retroID']].drop_duplicates(), on='retroID', how='left', suffixes=('','_p2'))
    df['bbrefID'] = np.where(df['bbrefID'].str.len()>0, df['bbrefID'], norm_id_ser(m['bbrefID_p2']))

# 3) reg로 mlbam/fgID 보강 (bbrefID 기준)
if not r.empty and 'bbrefID' in r.columns:
    m = df.merge(r[['bbrefID','mlbam','fgID']].drop_duplicates(), on='bbrefID', how='left', suffixes=('','_r1'))
    df['mlbam'] = np.where(df['mlbam'].str.len()>0, df['mlbam'], norm_id_ser(m['mlbam_r1']))
    df['fgID']  = np.where(df['fgID'].str.len()>0, df['fgID'],  norm_id_ser(m['fgID_r1']))
# 4) reg로 보강 (retroID 기준)
if not r.empty and 'retroID' in r.columns:
    m = df.merge(r[['retroID','mlbam','fgID']].drop_duplicates(), on='retroID', how='left', suffixes=('','_r2'))
    df['mlbam'] = np.where(df['mlbam'].str.len()>0, df['mlbam'], norm_id_ser(m['mlbam_r2']))
    df['fgID']  = np.where(df['fgID'].str.len()>0, df['fgID'],  norm_id_ser(m['fgID_r2']))
# 5) 이름(+생년) 보조키
keycols = [c for c in ['nameFirst','nameLast','birthYear'] if c in r.columns]
if not r.empty and keycols:
    m = df.merge(r[keycols+['mlbam','fgID']].drop_duplicates(), on=[c for c in keycols if c in df.columns], how='left', suffixes=('','_rnm'))
    df['mlbam'] = np.where(df['mlbam'].str.len()>0, df['mlbam'], norm_id_ser(m['mlbam_rnm']))
    df['fgID']  = np.where(df['fgID'].str.len()>0, df['fgID'],  norm_id_ser(m['fgID_rnm']))

# 최종 표준화
for k in ['bbrefID','retroID','mlbam','fgID']:
    df[k] = norm_id_ser(df[k])

use = ['year','teamID','playerID','player_name','bbrefID','mlbam','retroID','fgID']
df[use].to_csv(OUT/'mart_star_idfix.csv', index=False)
print(f"[IDMAP-FULL] wrote -> {OUT/'mart_star_idfix.csv'} rows={len(df)}")
