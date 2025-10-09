import pandas as pd, numpy as np
from pathlib import Path
ROOT=Path.cwd(); OUT=ROOT/'output'; OUT.mkdir(exist_ok=True)
f = OUT/'statcast_features_player_year.csv'
df = pd.read_csv(f, low_memory=False)

def safe_div(a,b):
    a = pd.to_numeric(a, errors='coerce'); b = pd.to_numeric(b, errors='coerce')
    out = a / b
    return out.replace([np.inf,-np.inf], np.nan).fillna(0)

# 표준화: avg_ev 생성(없으면 EV->avg_ev로 매핑)
if 'avg_ev' not in df.columns:
    if 'EV' in df.columns:
        df['avg_ev'] = pd.to_numeric(df['EV'], errors='coerce')
    else:
        df['avg_ev'] = np.nan

# 기본 레이트들 (없으면 0으로)
for c in ['Pitches','Swings','Whiffs','Z_Pitches','O_Pitches','Z_Swings','O_Swings','Z_Whiffs','O_Whiffs','CS']:
    if c not in df.columns: df[c]=0

# 기존 값 보존 + 확장
df['whiff_rate']    = safe_div(df['Whiffs'],    df['Swings'])
df['z_whiff_rate']  = safe_div(df['Z_Whiffs'],  df['Z_Swings'])
df['o_whiff_rate']  = safe_div(df['O_Whiffs'],  df['O_Swings'])
df['z_swing_rate']  = safe_div(df['Z_Swings'],  df['Z_Pitches']) if 'z_swing_rate' not in df.columns else df['z_swing_rate']
df['o_swing_rate']  = safe_div(df['O_Swings'],  df['O_Pitches']) if 'o_swing_rate' not in df.columns else df['o_swing_rate']
df['zone_rate']     = safe_div(df['Z_Pitches'], df['Pitches'])
df['chase_rate']    = df['o_swing_rate']  # 명시적 alias
df['contact_rate']  = 1.0 - df['whiff_rate']
df['z_contact_rate']= 1.0 - df['z_whiff_rate']
df['o_contact_rate']= 1.0 - df['o_whiff_rate']

# CSW: (Called Strike + Whiff) / Pitches
if 'csw_rate' not in df.columns:
    df['csw_rate'] = safe_div(df.get('CS',0)+df.get('Whiffs',0), df.get('Pitches',0))

# Z-CSW%
df['z_csw_rate'] = safe_div(df.get('Z_Whiffs',0) + safe_div(df.get('CS',0),1)*df['zone_rate'], df.get('Z_Pitches',0))

# 하드히트/배럴 레이트 명확화(있으면 유지)
for num,den,name in [('Hard','BBE','hardhit_rate'), ('Barrel','BBE','barrel_rate')]:
    if name not in df.columns:
        df[name] = safe_div(df.get(num,0), df.get(den,0))

df.to_csv(f, index=False)
print("[POST] enriched ->", f)
