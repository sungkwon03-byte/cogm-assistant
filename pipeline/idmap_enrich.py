import pandas as pd, numpy as np, re
from pathlib import Path

ROOT=Path.cwd(); OUT=(ROOT/'output'); OUT.mkdir(exist_ok=True)

def _ff_first(patterns):
    c=[]
    for pat in patterns:
        c+=list((ROOT/'data'/'lahman_extracted').rglob(pat))
        c+=list((ROOT/'data').rglob(pat))
    if not c: return None
    c.sort(key=lambda p:p.stat().st_size, reverse=True)
    return c[0]

def _read_csv_safe(p):
    return pd.read_csv(p, low_memory=False) if p and Path(p).exists() else None

# --- 0) 베이스: mart_star.csv
star_path = OUT/'mart_star.csv'
if not star_path.exists():
    raise SystemExit(f"[idmap_enrich] {star_path} not found")
star = pd.read_csv(star_path, low_memory=False)

for col in ['playerID','bbrefID','retroID','mlbam','fgID','player_name','year','teamID']:
    if col not in star.columns: star[col]=pd.NA

# --- 1) Lahman People/Master → retroID/bbrefID/이름/생년
people_p = _ff_first(['People.csv','Master.csv'])
people   = _read_csv_safe(people_p)
if people is not None:
    # 컬럼 통일
    rename_map = {}
    if 'nameFirst' not in people.columns and 'nameFirst' in people.columns: pass
    if 'nameFirst' not in people.columns and 'nameFirst'.lower() in map(str.lower, people.columns):
        for c in people.columns:
            if c.lower()=='namefirst': rename_map[c]='nameFirst'
            if c.lower()=='namelast':  rename_map[c]='nameLast'
            if c.lower()=='birthyear': rename_map[c]='birthYear'
    people = people.rename(columns=rename_map)

    keep_cols = [c for c in ['playerID','retroID','bbrefID','nameFirst','nameLast','birthYear'] if c in people.columns]
    ppl = people[keep_cols].copy()
    for c in ['playerID','retroID','bbrefID']: 
        if c in ppl.columns: ppl[c]=ppl[c].astype(str).str.strip()

    star = star.merge(ppl, on='playerID', how='left', suffixes=('','_ppl'))
    for a,b in [('retroID','retroID_ppl'),('bbrefID','bbrefID_ppl')]:
        if b in star.columns:
            star[a] = np.where(star[a].isna() | star[a].astype(str).isin(['nan','<NA>','']), star[b], star[a])

    if 'player_name' in star.columns and 'nameFirst' in star.columns and 'nameLast' in star.columns:
        star['player_name'] = star['player_name'].fillna((star['nameFirst'].fillna('')+' '+star['nameLast'].fillna('')).str.strip()).replace('', np.nan)

# --- 2) Chadwick register → mlbam/fgID/retro/bbref 보강
reg_p = _ff_first(['chadwick*register*.csv','*Chadwick*register*.csv','*Chadwick*.csv','*register*.csv'])
reg   = _read_csv_safe(reg_p)
if reg is not None:
    # 키 통일
    cols = {c.lower():c for c in reg.columns}
    def pick(*names):
        for n in names:
            if n in cols: return cols[n]
        return None
    k_bbref = pick('key_bbref','bbrefid','bbref_id','bbref')
    k_retro = pick('key_retro','retroid','retro_id','retro')
    k_mlbam = pick('key_mlbam','mlbam','mlbamid','mlbam_id','mlb_id','key_mlb')
    k_fg    = pick('key_fg','key_fangraphs','fangraphs','fg','fg_id')

    sub = reg[[c for c in [k_bbref,k_retro,k_mlbam,k_fg,'name_first','name_last','birth_year'] if c]].copy()
    sub = sub.rename(columns={k_bbref:'key_bbref', k_retro:'key_retro', k_mlbam:'key_mlbam', k_fg:'key_fg'})

    for c in ['key_bbref','key_retro','key_mlbam','key_fg','name_first','name_last','birth_year']:
        if c in sub.columns:
            if c in ['key_mlbam','key_fg','birth_year']:
                sub[c] = pd.to_numeric(sub[c], errors='coerce').astype('Int64').astype(str)
            else:
                sub[c] = sub[c].astype(str).str.strip()

    # 2-1) bbrefID 매치
    if 'bbrefID' in star.columns and 'key_bbref' in sub.columns:
        star = star.merge(sub[['key_bbref','key_mlbam','key_fg','key_retro']], left_on='bbrefID', right_on='key_bbref', how='left')
        for a,b in [('mlbam','key_mlbam'),('fgID','key_fg'),('retroID','key_retro')]:
            if b in star.columns:
                star[a] = np.where(star[a].isna() | star[a].astype(str).isin(['nan','<NA>','']), star[b], star[a])
        star.drop(columns=['key_bbref','key_mlbam','key_fg','key_retro'], errors='ignore', inplace=True)

    # 2-2) retroID 매치
    if 'retroID' in star.columns and 'key_retro' in sub.columns:
        star = star.merge(sub[['key_retro','key_mlbam','key_fg','key_bbref']], left_on='retroID', right_on='key_retro', how='left')
        for a,b in [('mlbam','key_mlbam'),('fgID','key_fg'),('bbrefID','key_bbref')]:
            if b in star.columns:
                star[a] = np.where(star[a].isna() | star[a].astype(str).isin(['nan','<NA>','']), star[b], star[a])
        star.drop(columns=['key_retro','key_mlbam','key_fg','key_bbref'], errors='ignore', inplace=True)

    # 2-3) 이름+생년 보조키 매치
    if all(c in star.columns for c in ['nameFirst','nameLast','birthYear']) and all(c in sub.columns for c in ['name_first','name_last','birth_year']):
        star = star.merge(sub, left_on=['nameFirst','nameLast','birthYear'], right_on=['name_first','name_last','birth_year'], how='left', suffixes=('','_reg2'))
        for a,b in [('mlbam','key_mlbam'),('fgID','key_fg'),('retroID','key_retro'),('bbrefID','key_bbref')]:
            b2 = f"{b}_reg2"
            if b2 in star.columns:
                star[a] = np.where(star[a].isna() | star[a].astype(str).isin(['nan','<NA>','']), star[b2], star[a])
        star.drop(columns=[c for c in ['key_mlbam_reg2','key_fg_reg2','key_retro_reg2','key_bbref_reg2','name_first','name_last','birth_year'] if c in star.columns], inplace=True)

    # 2-4) **retroID 역매핑**: mlbam→retro
    if 'mlbam' in star.columns and 'key_mlbam' in sub.columns and 'key_retro' in sub.columns:
        m = sub[['key_mlbam','key_retro']].dropna().drop_duplicates()
        m = m.rename(columns={'key_mlbam':'mlbam','key_retro':'retro_from_mlbam'})
        star = star.merge(m, on='mlbam', how='left')
        if 'retro_from_mlbam' in star.columns:
            star['retroID'] = np.where(star['retroID'].isna() | star['retroID'].astype(str).isin(['nan','<NA>','']),
                                       star['retro_from_mlbam'], star['retroID'])
            star.drop(columns=['retro_from_mlbam'], inplace=True)

# --- 3) 타입 정규화 (ID float 방지)
for c in ['mlbam','fgID']:
    if c in star.columns:
        star[c] = pd.to_numeric(star[c], errors='coerce').astype('Int64').astype(str)
for c in ['retroID','bbrefID','playerID']:
    if c in star.columns:
        star[c] = star[c].astype(str).str.strip()

# --- 4) 저장
out = OUT/'mart_star_idfix.csv'
star.to_csv(out, index=False)
print(f"[IDMAP-ENRICH] {out} rows={len(star)}")
