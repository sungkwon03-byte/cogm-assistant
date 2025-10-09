import os, pandas as pd, numpy as np
from pathlib import Path

ROOT=Path.cwd(); OUT=ROOT/'output'; OUT.mkdir(exist_ok=True)
base = ROOT/'output'/'cache'/'statcast_clean'
if not base.exists(): base = ROOT/'output'/'cache'/'statcast'
assert base.exists(), "statcast cache dir not found"

limit = int(os.getenv("STATCAST_MAX_FILES","999999"))
files = sorted(base.glob("*.csv"))[:limit]

need = ['game_year','pitch_type','description','zone','plate_x','plate_z','sz_top','sz_bot',
        'balls','strikes','batter','pitcher','p_throws','stand']

def safe_div(n, d):
    n = pd.to_numeric(n, errors='coerce')
    d = pd.to_numeric(d, errors='coerce')
    out = n / d
    return out.replace([np.inf, -np.inf], np.nan)

frames=[]
for fp in files:
    try:
        df = pd.read_csv(fp, low_memory=False)
    except Exception:
        continue
    if not set(need).issubset(df.columns): 
        continue
    d = df[need].copy()
    d = d.rename(columns={'game_year':'year','batter':'mlbam'})
    # numeric cast
    for c in ['zone','plate_x','plate_z','sz_top','sz_bot','balls','strikes','mlbam']:
        d[c] = pd.to_numeric(d[c], errors='coerce')
    d['pitch_type'] = d['pitch_type'].astype('string')
    d['description'] = d['description'].astype('string')
    d['stand'] = d['stand'].astype('string')

    # flags
    desc = d['description'].str.lower()
    swing  = desc.str.contains('swing', na=False)
    whiff  = desc.str.contains('swinging_strike', na=False) | desc.str.contains('swinging_strike_blocked', na=False)
    called = desc.str.contains('called_strike', na=False)
    contact= desc.str.contains('foul|in_play', na=False)

    in_zone = d['zone'].between(1,9, inclusive='both')
    edge    = d['zone'].isin([2,4,6,8])
    heart   = d['zone'].isin([5])

    # segment (object로 만들어 dtype 충돌 회피)
    seg = pd.Series(pd.NA, index=d.index, dtype='object')
    seg[np.where(d['strikes']>=2)[0]] = 'two_strike'
    seg[np.where((d['balls']-d['strikes'])>=2)[0]] = 'ahead'
    seg[np.where((d['strikes']-d['balls'])>=2)[0]] = 'behind'

    vhb = np.where(d['stand'].str.upper().eq('L'), 'vsL', 'vsR')

    g = pd.DataFrame({
        'year'        : d['year'],
        'mlbam'       : d['mlbam'],
        'pitch_type'  : d['pitch_type'],
        'segment'     : seg,
        'vhb'         : vhb,
        'pitches'     : 1,
        'Z_Pitches'   : in_zone.astype(int),
        'O_Pitches'   : (~in_zone).astype(int),
        'Z_Swings'    : (swing & in_zone).astype(int),
        'O_Swings'    : (swing & ~in_zone).astype(int),
        'Z_Whiffs'    : (whiff & in_zone).astype(int),
        'O_Whiffs'    : (whiff & ~in_zone).astype(int),
        'CS'          : (called).astype(int),
        'edge_cnt'    : edge.astype(int),
        'heart_cnt'   : heart.astype(int),
        'chase_cnt'   : (swing & ~in_zone).astype(int),
    })
    frames.append(g)

if not frames:
    raise SystemExit("[BAT+] no input rows; check cache shards")

raw = pd.concat(frames, ignore_index=True)
agg = (raw.groupby(['year','mlbam','pitch_type','segment','vhb'], dropna=False)
          .sum(numeric_only=True).reset_index())

# usage_rate는 같은 (year,mlbam,segment,vhb) 내 점유율
agg['group_total'] = (agg.groupby(['year','mlbam','segment','vhb'], dropna=False)['pitches']
                        .transform('sum'))
agg['usage_rate'] = safe_div(agg['pitches'], agg['group_total'])

# rates
agg['zone_rate']     = safe_div(agg['Z_Pitches'], agg['pitches'])
agg['z_swing_rate']  = safe_div(agg['Z_Swings'],  agg['Z_Pitches'])
agg['o_swing_rate']  = safe_div(agg['O_Swings'],  agg['O_Pitches'])
agg['z_contact_rate']= 1.0 - safe_div(agg['Z_Whiffs'], agg['Z_Swings'])
agg['o_contact_rate']= 1.0 - safe_div(agg['O_Whiffs'], agg['O_Swings'])
agg['z_csw_rate']    = safe_div(agg['Z_Whiffs'] + 0, agg['Z_Pitches'])  # whiff in-zone 비중(간이)
agg['csw_rate']      = safe_div(agg['Z_Whiffs'] + agg['O_Whiffs'] + agg['CS'], agg['pitches'])
agg['edge_rate']     = safe_div(agg['edge_cnt'], agg['pitches'])
agg['heart_rate']    = safe_div(agg['heart_cnt'], agg['pitches'])
agg['chase_rate']    = safe_div(agg['chase_cnt'], agg['O_Pitches'])

agg.insert(0, 'role', 'bat')

out = OUT/'statcast_pitch_mix_detailed_plus_bat.csv'
tmp = OUT/'_tmp_statcast_pitch_mix_detailed_plus_bat.csv'
agg.to_csv(tmp, index=False)
tmp.replace(out)
print(f"[BAT+] -> {out} rows={len(agg)}")
