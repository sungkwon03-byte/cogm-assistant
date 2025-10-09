import pandas as pd, numpy as np, re
from pathlib import Path
ROOT=Path.cwd(); OUT=ROOT/'output'; OUT.mkdir(exist_ok=True)

def _find(*pats):
    for pat in pats:
        c=list((ROOT/'data'/'lahman_extracted').rglob(pat)) or list((ROOT/'data').rglob(pat))
        if c: c.sort(key=lambda p:p.stat().st_size, reverse=True); return c[0]
    return None

def norm_id(s):
    s = pd.Series(s, dtype="object")
    s = s.astype(str).str.strip()
    # 소수점 꼬리 .0 제거
    s = s.str.replace(r'\.0$', '', regex=True)
    # 노이즈 제거
    s = s.replace({'nan':'','NaN':'','<NA>':'','None':'','NULL':'','NaT':''})
    return s

src = OUT/'mart_star.csv'
out = OUT/'mart_star_idfix.csv'
use = ['year','teamID','playerID','player_name','bbrefID','mlbam','retroID','fgID']

# 참조 테이블 로드
people = pd.read_csv(_find('People.csv','Master.csv'), dtype=str, low_memory=False) if _find('People.csv','Master.csv') else pd.DataFrame()
if not people.empty:
    keep_p = [c for c in ['bbrefID','retroID','nameFirst','nameLast','birthYear'] if c in people.columns]
    people = people[keep_p].copy().fillna('')
    for k in ['bbrefID','retroID']: 
        if k in people: people[k] = norm_id(people[k])

# Chadwick register
reg = None
for pat in ['chadwick*register*.csv','*Chadwick*.csv','*chadwick*.csv','chadwick_register.csv']:
    cc=list((ROOT/'data').rglob(pat))
    if cc:
        cc.sort(key=lambda p:p.stat().st_size, reverse=True); reg=cc[0]; break
if reg is not None:
    reg = pd.read_csv(reg, dtype=str, low_memory=False).fillna('')
    alias = {'key_bbref':'bbrefID','bbref':'bbrefID',
             'key_retro':'retroID','retro':'retroID',
             'key_mlbam':'mlbam','mlbam':'mlbam','mlb_id':'mlbam',
             'key_fangraphs':'fgID','fg_id':'fgID','fangraphs':'fgID',
             'name_first':'nameFirst','name_last':'nameLast','birth_year':'birthYear'}
    reg.columns = [alias.get(c,c) for c in reg.columns]
    keep_r = [c for c in ['bbrefID','retroID','mlbam','fgID','nameFirst','nameLast','birthYear'] if c in reg.columns]
    reg = reg[keep_r].copy()
    for k in ['bbrefID','retroID','mlbam','fgID']:
        if k in reg: reg[k] = norm_id(reg[k])
else:
    reg = pd.DataFrame(columns=['bbrefID','retroID','mlbam','fgID','nameFirst','nameLast','birthYear'])

# 이미 처리된 행수(헤더 제외)
done = 0
if out.exists():
    with open(out, 'rb') as f:
        done = max(0, sum(1 for _ in f) - 1)

chunksize = 100_000
seen = 0
wrote = 0

def enrich(ch: pd.DataFrame) -> pd.DataFrame:
    ch = ch.copy()
    for c in use:
        if c not in ch.columns: ch[c] = ''
    ch = ch[use]

    # 이름 보조키 분해
    nm = ch['player_name'].fillna('').str.split(',', n=1, expand=True)
    ch['nameLast']  = nm[0].str.strip() if nm.shape[0] else ''
    ch['nameFirst'] = nm[1].str.strip() if nm.shape[1] > 1 else ''
    ch['birthYear'] = ''  # 로컬에 없으면 공란

    # ID 표준화
    for k in ['bbrefID','retroID','mlbam','fgID']:
        ch[k] = norm_id(ch[k])

    # 1) bbrefID -> retroID 채움
    if (not people.empty) and ('bbrefID' in ch.columns) and ('bbrefID' in people.columns):
        ch = ch.merge(people[['bbrefID','retroID']], on='bbrefID', how='left', suffixes=('','_p1'))
        if 'retroID_p1' in ch.columns:
            ch['retroID'] = np.where(ch['retroID'].str.len()>0, ch['retroID'], norm_id(ch['retroID_p1']))
            ch.drop(columns=['retroID_p1'], inplace=True, errors='ignore')

    # 2) retroID -> bbrefID 채움
    if (not people.empty) and ('retroID' in ch.columns) and ('retroID' in people.columns):
        ch = ch.merge(people[['bbrefID','retroID']], on='retroID', how='left', suffixes=('','_p2'))
        if 'bbrefID_p2' in ch.columns:
            ch['bbrefID'] = np.where(ch['bbrefID'].str.len()>0, ch['bbrefID'], norm_id(ch['bbrefID_p2']))
            ch.drop(columns=['bbrefID_p2'], inplace=True, errors='ignore')

    # 3) reg를 통해 mlbam/fgID 보강
    if not reg.empty:
        for key in ['bbrefID','retroID']:
            if (key in ch.columns) and (key in reg.columns):
                ch = ch.merge(reg[[key,'mlbam','fgID']], on=key, how='left', suffixes=('','_r'+key))
                for k in ['mlbam','fgID']:
                    k2 = k+'_r'+key
                    if k2 in ch.columns:
                        ch[k] = np.where(ch[k].str.len()>0, ch[k], norm_id(ch[k2]))
                ch.drop(columns=[c for c in ch.columns if c.endswith('_r'+key)], inplace=True, errors='ignore')

        # 이름+생년 보조키가 있으면 활용
        keycols = [c for c in ['nameFirst','nameLast','birthYear'] if c in reg.columns]
        if keycols:
            r2 = reg[keycols+['mlbam','fgID']].drop_duplicates()
            ch = ch.merge(r2, on=[c for c in keycols if c in ch.columns], how='left', suffixes=('','_rnm'))
            for k in ['mlbam','fgID']:
                k2 = k+'_rnm'
                if k2 in ch.columns:
                    ch[k] = np.where(ch[k].str.len()>0, ch[k], norm_id(ch[k2]))
            ch.drop(columns=[c for c in ch.columns if c.endswith('_rnm')], inplace=True, errors='ignore')

    # 최종 클리닝
    for k in ['bbrefID','retroID','mlbam','fgID']:
        ch[k] = norm_id(ch[k])

    return ch[use]

it = pd.read_csv(src, dtype=str, chunksize=chunksize, low_memory=False)
for chunk in it:
    n = len(chunk)
    # 이미 본 청크는 통째로 스킵
    if seen + n <= done:
        seen += n
        continue
    # 중간 청크는 슬라이스해서 시작
    if seen < done:
        start = done - seen
        chunk = chunk.iloc[start:].copy()
        seen = done

    en = enrich(chunk)
    out_exists = out.exists()
    en.to_csv(out, mode='a', index=False, header=not out_exists)
    wrote += len(en)
    seen += n
    print(f"[RESUME] appended={wrote} total_done={done+wrote}", flush=True)

print(f"[IDMAP-RESUME] wrote={wrote}")
