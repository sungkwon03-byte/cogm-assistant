# fallback for z_whiff_rate: 1 - z_contact_rate if missing
import pandas as pd, numpy as np
from pathlib import Path
from _util_safe import wmean
ROOT=Path.cwd(); OUT=ROOT/'output'
f=OUT/'statcast_pitch_mix_detailed_plus_bat.csv'
df=pd.read_csv(f, low_memory=False)
# 열 가드
for c in ['z_contact_rate','edge_rate','zone_rate','chase_rate','z_csw_rate','hardhit_rate','barrel_rate','whiff_rate','pitches']:
    if c not in df.columns: df[c]=np.nan
if 'z_whiff_rate' not in df.columns:
    if {'Z_Whiffs','Z_Swings'}.issubset(df.columns):
        zwh = pd.to_numeric(df['Z_Whiffs'],errors='coerce')/pd.to_numeric(df['Z_Swings'],errors='coerce')
        df['z_whiff_rate']=zwh.replace([np.inf,-np.inf],np.nan)
    else:
        df['z_whiff_rate']=1.0 - pd.to_numeric(df['z_contact_rate'], errors='coerce')
agg=(df.groupby(['year','mlbam','vhb'])
       .apply(lambda d: pd.Series({
         'pitches': d['pitches'].sum(),
         'z_whiff_rate': wmean(d['z_whiff_rate'], d['pitches']),
         'o_swing_rate': wmean(d['o_swing_rate'] if 'o_swing_rate' in d else np.nan, d['pitches']),
         'z_contact_rate': wmean(d['z_contact_rate'], d['pitches']),
         'hardhit_rate': wmean(d['hardhit_rate'], d['pitches']),
         'barrel_rate' : wmean(d['barrel_rate'], d['pitches']),
         'chase_rate'  : wmean(d['chase_rate'], d['pitches']),
         'z_csw_rate'  : wmean(d['z_csw_rate'], d['pitches']),
       }))).reset_index()
agg.to_csv(OUT/'platoon_map_player_year.csv', index=False)
print("[OK] platoon_map_player_year.csv", len(agg))
