import json, pandas as pd
from pathlib import Path
ROOT=Path.cwd(); OUT=ROOT/'output'; LOG=ROOT/'logs'
src = OUT/'mart_star_idfix.csv' if (OUT/'mart_star_idfix.csv').exists() else OUT/'mart_star.csv'
df  = pd.read_csv(src, dtype=str, low_memory=False).fillna('')
for k in ['bbrefID','mlbam','retroID','fgID']: 
    if k not in df.columns: df[k]=''
for k in ['bbrefID','mlbam','retroID','fgID']: 
    df[k]=df[k].astype(str).str.strip().replace({'nan':'','<NA>':''})
cov={
 "total_rows": int(len(df)),
 "unique_players": int(df['playerID'].nunique()) if 'playerID' in df.columns else None,
 "bbref_coverage": float(df['bbrefID'].astype(bool).mean()),
 "mlbam_coverage": float(df['mlbam'].astype(bool).mean()),
 "retro_coverage": float(df['retroID'].astype(bool).mean()),
 "fg_coverage": float(df['fgID'].astype(bool).mean()),
}
(Path(LOG)/'idmap_audit_summary.json').write_text(json.dumps(cov, indent=2), encoding='utf-8')
print(json.dumps(cov, indent=2))
