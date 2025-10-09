import pandas as pd, numpy as np
from pathlib import Path
ROOT=Path.cwd(); OUT=ROOT/'output'; OUT.mkdir(exist_ok=True)
def _find(*pats):
    for pat in pats:
        c=list((ROOT/'data'/'lahman_extracted').rglob(pat)) or list((ROOT/'data').rglob(pat))
        if c: c.sort(key=lambda p:p.stat().st_size, reverse=True); return c[0]
    return None

# People/Master 최소 컬럼만 로드(문자열)
people = pd.read_csv(_find('People.csv','Master.csv'), dtype=str, low_memory=False)
keep_cols = [c for c in ['bbrefID','retroID','nameFirst','nameLast','birthYear'] if c in people.columns]
people = people[keep_cols].fillna('').drop_duplicates()

# Chadwick register (있으면)
reg = None
for pat in ['chadwick*register*.csv','*Chadwick*.csv','*chadwick*.csv','chadwick_register.csv']:
    p = list((ROOT/'data').rglob(pat))
    if p: 
        p.sort(key=lambda x:x.stat().st_size, reverse=True)
        reg = pd.read_csv(p[0], dtype=str, low_memory=False)
        break
if reg is not None:
    alias = {'key_bbref':'bbrefID','bbref':'bbrefID','key_retro':'retroID','retro':'retroID','retro_id':'retroID',
             'key_mlbam':'mlbam','mlbam':'mlbam','mlb_id':'mlbam','key_fangraphs':'fgID','fg_id':'fgID','fangraphs':'fgID',
             'name_first':'nameFirst','name_last':'nameLast','birth_year':'birthYear'}
    reg.columns = [alias.get(c,c) for c in reg.columns]
    reg = reg[[c for c in ['bbrefID','retroID','mlbam','fgID','nameFirst','nameLast','birthYear'] if c in reg.columns]].fillna('')
else:
    reg = pd.DataFrame(columns=['bbrefID','retroID','mlbam','fgID','nameFirst','nameLast','birthYear'])

src = OUT/'mart_star.csv'
use = ['year','teamID','playerID','player_name','bbrefID','mlbam','retroID','fgID']
out = OUT/'mart_star_idfix.csv'
if out.exists(): out.unlink()

hdr = pd.read_csv(src, nrows=1, dtype=str, low_memory=False).columns.tolist()
use_read = [c for c in use if c in hdr]
it = pd.read_csv(src, dtype=str, usecols=use_read, chunksize=100_000, low_memory=False)

rows=0
for ch in it:
    ch = ch.reindex(columns=use)
    for c in use:
        if c not in ch.columns: ch[c]=''
    # 이름 파싱(보조 키)
    nm = ch['player_name'].fillna('').str.split(',', n=1, expand=True) if 'player_name' in ch.columns else None
    ch['nameLast'] = nm[0].str.strip() if nm is not None else ''
    ch['nameFirst']= nm[1].str.strip() if (nm is not None and nm.shape[1]>1) else ''
    ch['birthYear']= ''

    # 1) bbrefID로 People 매칭 → retroID 보강
    if 'bbrefID' in ch.columns and 'bbrefID' in people.columns:
        ch = ch.merge(people[['bbrefID','retroID']], on='bbrefID', how='left', suffixes=('','_p1'))
        ch['retroID'] = np.where(ch['retroID'].astype(str).str.len()>0, ch['retroID'],
                                 ch.get('retroID_p1','').astype(str))
        ch.drop(columns=[c for c in ch.columns if c.endswith('_p1')], inplace=True, errors='ignore')

    # 2) retroID로 People 역보강 → bbrefID 보강
    if 'retroID' in ch.columns and 'retroID' in people.columns:
        ch = ch.merge(people[['bbrefID','retroID']], on='retroID', how='left', suffixes=('','_p2'))
        ch['bbrefID'] = np.where(ch['bbrefID'].astype(str).str.len()>0, ch['bbrefID'],
                                 ch.get('bbrefID_p2','').astype(str))
        ch.drop(columns=[c for c in ch.columns if c.endswith('_p2')], inplace=True, errors='ignore')

    # 3) Chadwick로 mlbam/fgID 채움 (bbref/retro 우선)
    if not reg.empty:
        for key in ['bbrefID','retroID']:
            if key in ch.columns and key in reg.columns:
                ch = ch.merge(reg[[key,'mlbam','fgID']], on=key, how='left', suffixes=('','_r'+key))
                for k in ['mlbam','fgID']:
                    ch[k] = np.where(ch[k].astype(str).str.len()>0, ch[k], ch.get(k+'_r'+key,'').astype(str))
                ch.drop(columns=[k for k in ch.columns if k.endswith('_r'+key)], inplace=True, errors='ignore')
        # 이름 보조키(가벼운 컬럼만)
        keycols = [c for c in ['nameFirst','nameLast','birthYear'] if c in reg.columns]
        if keycols:
            r2 = reg[keycols+['mlbam','fgID']].drop_duplicates()
            ch = ch.merge(r2, on=[c for c in keycols if c in ch.columns], how='left', suffixes=('','_rnm'))
            for k in ['mlbam','fgID']:
                ch[k] = np.where(ch[k].astype(str).str.len()>0, ch[k], ch.get(k+'_rnm','').astype(str))
            ch.drop(columns=[k for k in ch.columns if k.endswith('_rnm')], inplace=True, errors='ignore')

    for k in ['mlbam','fgID','bbrefID','retroID']:
        ch[k] = ch[k].replace({'nan':'','<NA>':''})
    ch.to_csv(out, mode='a', index=False, header=not out.exists())
    rows += len(ch)
    if rows % 100000 == 0:
        print(f"[IDMAP] processed={rows}", flush=True)

print(f"[IDMAP-ENRICH-SAFE] {out} rows={rows}")
