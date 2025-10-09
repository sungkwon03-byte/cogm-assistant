import pandas as pd, numpy as np, json
from pathlib import Path
ROOT=Path.cwd(); OUT=ROOT/'output'; LOG=ROOT/'logs'; LOG.mkdir(exist_ok=True)

def norm_id_ser(s):
    s = pd.Series(s, dtype="object")
    s = s.astype(str).str.strip()
    s = s.str.replace(r'\.0$', '', regex=True)
    s = s.replace({'nan':'','NaN':'','<NA>':'','None':'','NULL':'','NaT':''})
    return s

src = OUT/'mart_star_idfix.csv' if (OUT/'mart_star_idfix.csv').exists() and (OUT/'mart_star_idfix.csv').stat().st_size>0 else OUT/'mart_star.csv'
df  = pd.read_csv(src, dtype=str, low_memory=False)

for k in ['bbrefID','mlbam','retroID','fgID']:
    if k not in df.columns: df[k]=''
    df[k] = norm_id_ser(df[k])

use = ['year','teamID','playerID','player_name','bbrefID','mlbam','retroID','fgID']
df = df.reindex(columns=use)

# Chadwick register
reg = None
for pat in ['chadwick*register*.csv','*Chadwick*.csv','*chadwick*.csv','chadwick_register.csv']:
    cc=list((ROOT/'data').rglob(pat))
    if cc:
        cc.sort(key=lambda p:p.stat().st_size, reverse=True); reg=cc[0]; break

if reg is not None:
    r = pd.read_csv(reg, dtype=str, low_memory=False).fillna('')
    alias = {'key_bbref':'bbrefID','bbref':'bbrefID',
             'key_retro':'retroID','retro':'retroID',
             'key_mlbam':'mlbam','mlbam':'mlbam','mlb_id':'mlbam',
             'key_fangraphs':'fgID','fg_id':'fgID','fangraphs':'fgID'}
    r.columns = [alias.get(c,c) for c in r.columns]
    r = r[[c for c in ['bbrefID','retroID','mlbam','fgID'] if c in r.columns]].drop_duplicates()
    for k in ['bbrefID','retroID','mlbam','fgID']:
        if k in r: r[k] = norm_id_ser(r[k])
else:
    r = pd.DataFrame(columns=['bbrefID','retroID','mlbam','fgID'])

def isin_reg(col):
    if col not in df.columns or col not in r.columns or r.empty: 
        return pd.Series([False]*len(df))
    return df[col].isin(set(r[col].dropna()))

df['bbrefID_in_reg'] = isin_reg('bbrefID')
df['mlbam_in_reg']   = isin_reg('mlbam')
df['retroID_in_reg'] = isin_reg('retroID')
df['fgID_in_reg']    = isin_reg('fgID')

def issues_row(s):
    miss=[]
    for k in ['bbrefID','mlbam','retroID','fgID']:
        if not str(s.get(k,'')):
            miss.append(f"MISSING:{k}")
    return ",".join(miss)

df['issues'] = df.apply(issues_row, axis=1)

outp = OUT/'idmap_audit.csv'
df.to_csv(outp, index=False)
print(f"[IDMAP] {outp} rows={len(df)}")

summary = {
  "total_rows": int(len(df)),
  "unique_players": int(df['playerID'].nunique()) if 'playerID' in df.columns else None,
  "bbref_coverage": float(df['bbrefID'].astype(bool).mean()) if 'bbrefID' in df.columns else 0.0,
  "mlbam_coverage": float(df['mlbam'].astype(bool).mean()) if 'mlbam' in df.columns else 0.0,
  "retro_coverage": float(df['retroID'].astype(bool).mean()) if 'retroID' in df.columns else 0.0,
  "fg_coverage": float(df['fgID'].astype(bool).mean()) if 'fgID' in df.columns else 0.0,
  "register_path": str(reg) if reg is not None else None
}
(LOG/'idmap_audit_summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
print(f"[IDMAP] summary -> {LOG/'idmap_audit_summary.json'}")
