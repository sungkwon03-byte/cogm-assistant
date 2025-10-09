import os, pandas as pd, numpy as np
from pathlib import Path

ROOT=Path.cwd(); OUT=ROOT/'output'; OUT.mkdir(exist_ok=True)
base = ROOT/'output'/'cache'/'statcast_clean'
if not base.exists(): base = ROOT/'output'/'cache'/'statcast'
assert base.exists(), "statcast cache dir not found"

limit = int(os.getenv("STATCAST_MAX_FILES", "999999"))
files = sorted(base.glob("*.csv"))[:limit]

need_cols = [
    'game_year','pitch_type','description','zone','plate_x','plate_z','sz_top','sz_bot',
    'balls','strikes','batter','pitcher','p_throws','stand',
    'release_spin_rate','release_extension','pfx_x','pfx_z','player_name'
]

def in_zone(row):
    # 1) zone 코드 존재하면 1..9를 스트존으로
    if not pd.isna(row.get('zone')):
        try:
            z = int(row['zone'])
            return 1 <= z <= 9
        except:
            pass
    # 2) 좌우 0.83ft, 상하 sz_bot~sz_top 간단 근사
    try:
        return (abs(float(row['plate_x'])) <= 0.83) and (float(row['sz_bot']) <= float(row['plate_z']) <= float(row['sz_top']))
    except:
        return False

def is_swing(desc:str):
    if not isinstance(desc,str): return False
    d = desc.lower()
    return any(k in d for k in ['swinging_strike','foul','foul_tip','hit_into_play'])

def is_whiff(desc:str):
    if not isinstance(desc,str): return False
    d = desc.lower()
    return any(k in d for k in ['swinging_strike','swinging_strike_blocked','missed_bunt'])

def is_called_strike(desc:str):
    if not isinstance(desc,str): return False
    return 'called_strike' in desc.lower()

rows=[]
for i,fp in enumerate(files, 1):
    try:
        df = pd.read_csv(fp, low_memory=False, usecols=[c for c in need_cols if c])  # 일부 파일은 없는 컬럼 존재해도 ok
    except Exception:
        df = pd.read_csv(fp, low_memory=False)
    for c in need_cols:
        if c not in df.columns: df[c]=np.nan
    df['in_zone'] = df.apply(in_zone, axis=1)
    df['swing']   = df['description'].map(is_swing)
    df['whiff']   = df['description'].map(is_whiff)
    df['cs']      = df['description'].map(is_called_strike)
    df['two_strike'] = (pd.to_numeric(df['strikes'], errors='coerce')==2)

    # 카운트 상황(투수 관점): ahead(스트라이크>볼), behind(볼>스트라이크)
    b = pd.to_numeric(df['balls'], errors='coerce').fillna(0)
    s = pd.to_numeric(df['strikes'], errors='coerce').fillna(0)
    df['ahead']   = (s > b)
    df['behind']  = (b > s)

    # 공통 파생
    df['h_mov_in'] = pd.to_numeric(df['pfx_x'], errors='coerce') * 12.0
    df['v_mov_in'] = pd.to_numeric(df['pfx_z'], errors='coerce') * 12.0
    df['spin']     = pd.to_numeric(df['release_spin_rate'], errors='coerce')
    df['ext']      = pd.to_numeric(df['release_extension'], errors='coerce')

    # 세그먼트 집계 함수
    def seg_agg(sub, who, pid, pname):
        tot = len(sub)
        if tot==0: return []
        def rate(num,den): den = den if den>0 else np.nan; val = (num/den) if den else 0; return 0.0 if np.isnan(val) else val
        pitches = tot
        swings  = int(sub['swing'].sum())
        whiffs  = int(sub['whiff'].sum())
        z_p     = int(sub['in_zone'].sum())
        o_p     = pitches - z_p
        z_s     = int((sub['swing'] & sub['in_zone']).sum())
        o_s     = swings - z_s
        z_w     = int((sub['whiff'] & sub['in_zone']).sum())
        o_w     = whiffs - z_w
        cs      = int(sub['cs'].sum())

        return [{
            'year': int(sub['game_year'].mode().iloc[0]) if 'game_year' in sub and not sub['game_year'].isna().all() else np.nan,
            'role': who,
            'mlbam': int(pid) if pd.notna(pid) else np.nan,
            'player_name': pname if isinstance(pname,str) else np.nan,
            'pitch_type': sub['pitch_type'].mode().iloc[0] if not sub['pitch_type'].isna().all() else np.nan,
            'pitches': pitches,
            'usage_rate': None,  # 후술 계산(선수-연도 기준 정규화)
            'zone_rate': rate(z_p, pitches),
            'whiff_rate': rate(whiffs, swings),
            'z_whiff_rate': rate(z_w, z_s),
            'o_whiff_rate': rate(o_w, o_s),
            'chase_rate': rate(o_s, o_p),
            'csw_rate': rate(cs+whiffs, pitches),
            'avg_spin': float(sub['spin'].mean()),
            'avg_ext':  float(sub['ext'].mean()),
            'h_mov_in': float(sub['h_mov_in'].mean()),
            'v_mov_in': float(sub['v_mov_in'].mean()),
        }]

    # 투수 기준 피치믹스
    grp = df.groupby(['game_year','pitcher','pitch_type'], dropna=False)
    for (yr,pid,ptype), sub in grp:
        pname = None  # pitcher_name 컬럼이 없을 수 있어 비워둠
        rows += seg_agg(sub, 'pit', pid, pname)
        # 세그먼트: two_strike / ahead / behind
        for seg_col, seg_name in [('two_strike','two_strike'), ('ahead','ahead'), ('behind','behind')]:
            ssub = sub[sub[seg_col]]
            rows += [r | {'segment': seg_name} for r in seg_agg(ssub, 'pit', pid, pname)]

print("[SEG] collected rows =", len(rows))
mix = pd.DataFrame(rows)

# usage_rate(선수-연도 총구종 투구 대비)
tot = mix.groupby(['year','role','mlbam'], as_index=False)['pitches'].sum().rename(columns={'pitches':'player_year_total'})
mix = mix.merge(tot, on=['year','role','mlbam'], how='left')
mix['usage_rate'] = np.where(mix['player_year_total']>0, mix['pitches']/mix['player_year_total'], 0.0)
mix.drop(columns=['player_year_total'], inplace=True)

out = OUT/'statcast_pitch_mix_detailed.csv'
mix.to_csv(out, index=False)
print("[OUT] ->", out, "rows=", len(mix))
