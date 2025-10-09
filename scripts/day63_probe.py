import json, pandas as pd, numpy as np
from pathlib import Path

ROOT = Path.cwd()
LOGS = ROOT/'logs'; LOGS.mkdir(exist_ok=True)

def find_lahman_csv(name):
    c = list((ROOT/'data'/'lahman_extracted').rglob(name))
    if not c: c = list((ROOT/'data').rglob(name))
    if not c: raise FileNotFoundError(f"{name} not found under data/")
    c.sort(key=lambda p: p.stat().st_size, reverse=True)
    return c[0]

def read_ms(path):
    use = ['year','teamID','role','HR','PA','ER','IPouts']
    df = pd.read_csv(path, low_memory=False)
    for col in use:
        if col not in df.columns: df[col] = np.nan
    df = df[use].copy()
    df['year']   = pd.to_numeric(df['year'], errors='coerce')
    df['teamID'] = df['teamID'].astype(str).str.strip().str.upper()
    df['role']   = df['role'].astype(str).str.strip().str.lower()
    for c in ['HR','PA','ER','IPouts']:
        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
    return df.dropna(subset=['year'])

def read_batting(path):
    head_cols = list(pd.read_csv(path, nrows=0).columns)
    use = [c for c in ['yearID','teamID','AB','BB','HBP','SF','SH','HR'] if c in head_cols]
    df = pd.read_csv(path, usecols=use, low_memory=False)
    df['teamID']=df['teamID'].astype(str).str.strip().str.upper()
    df['yearID']=pd.to_numeric(df['yearID'], errors='coerce')
    for c in ['AB','BB','HBP','SF','SH','HR']:
        if c not in df.columns: df[c]=0
        df[c]=pd.to_numeric(df[c], errors='coerce').fillna(0)
    df['PA'] = df[['AB','BB','HBP','SF','SH']].sum(axis=1)
    return df

def read_pitching(path):
    head_cols = list(pd.read_csv(path, nrows=0).columns)
    use = [c for c in ['yearID','teamID','ER','IPouts','IP'] if c in head_cols]
    df = pd.read_csv(path, usecols=use, low_memory=False)
    df['teamID']=df['teamID'].astype(str).str.strip().str.upper()
    df['yearID']=pd.to_numeric(df['yearID'], errors='coerce')
    if 'ER' in df.columns: df['ER']=pd.to_numeric(df['ER'], errors='coerce').fillna(0)
    if 'IPouts' not in df.columns:
        def ip_to_outs(x):
            try:
                f=float(x); i=int(f); frac=round(f-i,1)
                return i*3 + (1 if abs(frac-0.1)<1e-8 else 2 if abs(frac-0.2)<1e-8 else 0)
            except: return np.nan
        df['IPouts'] = df['IP'].map(ip_to_outs)
    df['IPouts']=pd.to_numeric(df['IPouts'], errors='coerce').fillna(0)
    return df

def rate(df, a, b, tol=0.01):
    d = (df[a].fillna(0) - df[b].fillna(0)).abs() <= tol
    return float(d.mean())

# --- load
ms = read_ms(ROOT/'output'/'mart_star.csv')
bat = read_batting(find_lahman_csv('Batting.csv'))
pit = read_pitching(find_lahman_csv('Pitching.csv'))

# --- group
mart_b = ms[ms['role']=='bat'].groupby(['year','teamID'], as_index=False).agg(HR=('HR','sum'), PA=('PA','sum'))
mart_p = ms[ms['role']=='pit'].groupby(['year','teamID'], as_index=False).agg(ER=('ER','sum'), IPouts=('IPouts','sum'))

lah_b = bat.groupby(['yearID','teamID'], as_index=False).agg(HR=('HR','sum'), PA=('PA','sum')).rename(columns={'yearID':'year'})
lah_p = pit.groupby(['yearID','teamID'], as_index=False).agg(ER=('ER','sum'), IPouts=('IPouts','sum')).rename(columns={'yearID':'year'})

# --- outer merge (원인 특정)
cmp_b = mart_b.merge(lah_b, on=['year','teamID'], how='outer', suffixes=('_mart','_lah'), indicator=True)
cmp_p = mart_p.merge(lah_p, on=['year','teamID'], how='outer', suffixes=('_mart','_lah'), indicator=True)

summary = {
  "cmp_b_rows": int(len(cmp_b)),
  "cmp_p_rows": int(len(cmp_p)),
  "b_merge_counts": cmp_b['_merge'].value_counts().to_dict(),
  "p_merge_counts": cmp_p['_merge'].value_counts().to_dict(),
  "HR_match_rate": rate(cmp_b, 'HR_mart','HR_lah'),
  "PA_match_rate": rate(cmp_b, 'PA_mart','PA_lah'),
  "ER_match_rate": rate(cmp_p, 'ER_mart','ER_lah'),
  "IPouts_match_rate": rate(cmp_p, 'IPouts_mart','IPouts_lah'),
  "mart_teams_only": sorted(set(mart_b['teamID']) - set(lah_b['teamID'])),
  "lah_teams_only":  sorted(set(lah_b['teamID'])  - set(mart_b['teamID'])),
}

# 미싱 키 목록
miss = []
miss.append(cmp_b.loc[cmp_b['_merge']!='both', ['year','teamID','_merge']].assign(kind='bat'))
miss.append(cmp_p.loc[cmp_p['_merge']!='both', ['year','teamID','_merge']].assign(kind='pit'))
miss = pd.concat(miss, ignore_index=True)
miss.to_csv(LOGS/'day63_probe_missing_keys.csv', index=False)

# 상위 오차
def topdiff(df, a, b, k=30):
    d = df[['year','teamID',a,b]].copy()
    d['abs_diff'] = (d[a].fillna(0)-d[b].fillna(0)).abs()
    return d.sort_values('abs_diff', ascending=False).head(k)

detail = pd.concat([
    topdiff(cmp_b, 'HR_mart','HR_lah'),
    topdiff(cmp_b, 'PA_mart','PA_lah'),
    topdiff(cmp_p, 'ER_mart','ER_lah'),
    topdiff(cmp_p, 'IPouts_mart','IPouts_lah'),
], ignore_index=True)
detail.to_csv(LOGS/'day63_probe_detail.csv', index=False)

(Path(LOGS/'day63_probe_summary.json')).write_text(json.dumps(summary, indent=2), encoding='utf-8')
print(f"[PROBE] summary -> {LOGS/'day63_probe_summary.json'}")
print(f"[PROBE] missing -> {LOGS/'day63_probe_missing_keys.csv'}")
print(f"[PROBE] detail  -> {LOGS/'day63_probe_detail.csv'}")
