# -*- coding: utf-8 -*-
"""
고급 Statcast 집계:
- Batters: whiff/chase(Z/O 구분), Z/O-Contact%, Barrel%(Savant-approx), EV, xwOBA 등
- Pitchers: CSW%, whiff/chase(Z/O), 회전수, 익스텐션, 수평/수직 무브(inches), Pitch Mix(usage/velo/spin/CSW/Whiff)
주의: CSV 스키마가 시기별로 달라 누락 컬럼은 NaN 처리.
"""
import os, math
import pandas as pd, numpy as np
from pathlib import Path

ROOT=Path.cwd(); OUT=ROOT/'output'; OUT.mkdir(parents=True, exist_ok=True)
BASES=[OUT/'cache'/'statcast_clean', OUT/'cache'/'statcast']
MAX_FILES=int(os.environ.get('STATCAST_MAX_FILES','64'))

def _rate(n,d): 
    n=float(n); d=float(d); 
    return (n/d) if d>0 else np.nan

def _load_files():
    files=[]
    for b in BASES:
        if b.exists():
            files += sorted(list(b.glob("*.csv")))
    if MAX_FILES>0: files=files[:MAX_FILES]
    return [f for f in files if f.stat().st_size>0]

def _read_csv(p):
    try:
        return pd.read_csv(p, low_memory=False, on_bad_lines='skip')
    except Exception:
        return pd.DataFrame()

def _year_of(df):
    if 'game_year' in df.columns:
        return pd.to_numeric(df['game_year'], errors='coerce')
    if 'game_date' in df.columns:
        return pd.to_datetime(df['game_date'], errors='coerce').dt.year
    return pd.Series(np.nan, index=df.index)

def _zone_masks(df):
    if 'zone' in df.columns:
        inzone = df['zone'].between(1,9)
        return inzone, ~inzone
    if {'plate_x','plate_z','sz_top','sz_bot'}.issubset(df.columns):
        inzone = (df['plate_x'].abs()<=0.83) & (df['plate_z'].between(df['sz_bot'], df['sz_top']))
        return inzone, ~inzone
    s = pd.Series(False, index=df.index)
    return s, s

def _swing_masks(df):
    desc = df['description'].astype(str) if 'description' in df.columns else pd.Series('', index=df.index)
    swing = desc.isin(['swinging_strike','swinging_strike_blocked','foul','foul_tip','hit_into_play'])
    whiff = desc.isin(['swinging_strike','swinging_strike_blocked'])
    cs    = desc.eq('called_strike')
    return swing, whiff, cs

def _pa_mask(df):
    ev = df['events'].astype(str) if 'events' in df.columns else pd.Series('', index=df.index)
    des = df['description'].astype(str) if 'description' in df.columns else pd.Series('', index=df.index)
    return ev.ne('') | des.isin(['hit_into_play','strikeout','walk','hit_by_pitch','sac_fly','sac_bunt'])


def _barrel_mask(df):
    """
    보수적 배럴 근사:
      - Exit Velo(ev) >= 98mph
      - Launch Angle(la) ∈ [low, high]
        low = 26 - 0.5*(ev-98)+ (하한 8°)
        high= 30 + 0.5*(ev-98)+ (상한 45°)
    ※ 정확 정의(Statcast barrel table)를 쓰려면 향후 테이블 기반으로 고도화.
    """
    import numpy as np
    import pandas as pd

    def pick(colcands):
        for c in colcands:
            if c in df.columns:
                return pd.to_numeric(df[c], errors='coerce')
        return pd.Series(np.nan, index=df.index)

    ev = pick(['launch_speed','exit_velocity','ev'])      # mph
    la = pick(['launch_angle','la','launch_angle_deg'])   # degrees

    dv   = (ev - 98.0).clip(lower=0)
    low  = (26.0 - 0.5*dv).clip(lower=8.0)
    high = (30.0 + 0.5*dv).clip(upper=45.0)

    return (ev >= 98.0) & (la >= low) & (la <= high)

def aggregate():
    files=_load_files()
    if not files:
        (OUT/'statcast_features_player_year.csv').write_text("", encoding='utf-8')
        (OUT/'statcast_pitch_mix_player_year.csv').write_text("", encoding='utf-8')
        (OUT/'statcast_agg_player_year.csv').write_text("", encoding='utf-8')
        print("[STATCAST][ENRICH] no files")
        return

    bat_rows=[]; pit_rows=[]; mix_rows=[]
    for f in files:
        df=_read_csv(f)
        if df.empty:
            print(f"[STATCAST][SKIP] {f.name}: empty")
            continue
        df.columns=[c.strip() for c in df.columns]
        df['year']=_year_of(df)

        inzone, outzone = _zone_masks(df)
        swing, whiff, cs = _swing_masks(df)
        is_pa = _pa_mask(df)

        bbe   = df['launch_speed'].notna() if 'launch_speed' in df.columns else pd.Series(False, index=df.index)
        hard  = bbe & (pd.to_numeric(df['launch_speed'], errors='coerce')>=95)
        barrel= _barrel_mask(df)

        # ---------- Batters ----------
        if 'batter' in df.columns:
            grp=['year','batter']
            add_name = 'batter_name' if 'batter_name' in df.columns else ('player_name' if 'player_name' in df.columns else None)
            if add_name: grp.append(add_name)
            g=df.groupby(grp, dropna=True)

            bat = g.agg(
                Pitches   =('pitch_type','count'),
                PA        =('year', lambda x: int(is_pa.loc[x.index].sum())),
                Swings    =('year', lambda x: int(swing.loc[x.index].sum())),
                Whiffs    =('year', lambda x: int(whiff.loc[x.index].sum())),
                Z_Pitches =('year', lambda x: int(inzone.loc[x.index].sum())),
                O_Pitches =('year', lambda x: int(outzone.loc[x.index].sum())),
                Z_Swings  =('year', lambda x: int((swing & inzone).loc[x.index].sum())),
                O_Swings  =('year', lambda x: int((swing & outzone).loc[x.index].sum())),
                Z_Whiffs  =('year', lambda x: int((whiff & inzone).loc[x.index].sum())),
                O_Whiffs  =('year', lambda x: int((whiff & outzone).loc[x.index].sum())),
                BBE       =('year', lambda x: int(bbe.loc[x.index].sum())),
                Hard      =('year', lambda x: int(hard.loc[x.index].sum())),
                Barrel    =('year', lambda x: int(barrel.loc[x.index].sum())),
                EV        =('launch_speed','mean'),
                xwOBA     =('estimated_woba_using_speedangle','mean') if 'estimated_woba_using_speedangle' in df.columns else ('woba_value','mean')
            ).reset_index()

            bat.rename(columns={'batter':'mlbam', add_name:'player_name' if add_name else 'player_name'}, inplace=True)
            bat['role']='bat'
            # Rates
            bat['whiff_rate']   = bat.apply(lambda r: _rate(r['Whiffs'],   r['Swings']),    axis=1)
            bat['z_swing_rate'] = bat.apply(lambda r: _rate(r['Z_Swings'], r['Z_Pitches']), axis=1)
            bat['o_swing_rate'] = bat.apply(lambda r: _rate(r['O_Swings'], r['O_Pitches']), axis=1)  # chase%
            bat['z_contact_rate']=bat.apply(lambda r: _rate(r['Z_Swings']-r['Z_Whiffs'], r['Z_Swings']), axis=1)
            bat['o_contact_rate']=bat.apply(lambda r: _rate(r['O_Swings']-r['O_Whiffs'], r['O_Swings']), axis=1)
            bat['hardhit_rate'] = bat.apply(lambda r: _rate(r['Hard'], r['BBE']), axis=1)
            bat['barrel_rate']  = bat.apply(lambda r: _rate(r['Barrel'], r['BBE']), axis=1)
            bat_rows.append(bat)

        # ---------- Pitchers ----------
        if 'pitcher' in df.columns:
            grp=['year','pitcher']
            add_name = 'player_name' if 'player_name' in df.columns else None
            if add_name: grp.append(add_name)
            g=df.groupby(grp, dropna=True)

            pit = g.agg(
                Pitches   =('pitch_type','count'),
                Swings    =('year', lambda x: int(swing.loc[x.index].sum())),
                Whiffs    =('year', lambda x: int(whiff.loc[x.index].sum())),
                Z_Pitches =('year', lambda x: int(inzone.loc[x.index].sum())),
                O_Pitches =('year', lambda x: int(outzone.loc[x.index].sum())),
                Z_Swings  =('year', lambda x: int((swing & inzone).loc[x.index].sum())),
                O_Swings  =('year', lambda x: int((swing & outzone).loc[x.index].sum())),
                Z_Whiffs  =('year', lambda x: int((whiff & inzone).loc[x.index].sum())),
                O_Whiffs  =('year', lambda x: int((whiff & outzone).loc[x.index].sum())),
                CS        =('year', lambda x: int((_read_str(df,'description').eq('called_strike')).loc[x.index].sum()) if 'description' in df.columns else 0),
                EV        =('launch_speed','mean'),
                ext       =('release_extension','median') if 'release_extension' in df.columns else ('year','size'),
                spin      =('release_spin_rate','median') if 'release_spin_rate' in df.columns else ('year','size'),
                pfx_x     =('pfx_x','median') if 'pfx_x' in df.columns else ('year','size'),
                pfx_z     =('pfx_z','median') if 'pfx_z' in df.columns else ('year','size')
            ).reset_index()

            pit.rename(columns={'pitcher':'mlbam', add_name:'player_name' if add_name else 'player_name'}, inplace=True)
            pit['role']='pit'
            pit['whiff_rate']  = pit.apply(lambda r: _rate(r['Whiffs'], r['Swings']), axis=1)
            pit['z_contact_rate']=pit.apply(lambda r: _rate(r['Z_Swings']-r['Z_Whiffs'], r['Z_Swings']), axis=1)
            pit['o_contact_rate']=pit.apply(lambda r: _rate(r['O_Swings']-r['O_Whiffs'], r['O_Swings']), axis=1)
            pit['chase_rate']  = pit.apply(lambda r: _rate(r['O_Swings'], r['O_Pitches']), axis=1)  # O-Swing%
            pit['csw_rate']    = pit.apply(lambda r: _rate(r['CS']+r['Whiffs'], r['Pitches']), axis=1)
            pit['avg_ext']     = pd.to_numeric(pit['ext'], errors='coerce')
            pit['avg_spin']    = pd.to_numeric(pit['spin'], errors='coerce')
            pit['h_mov_in']    = pd.to_numeric(pit['pfx_x'], errors='coerce')*12  # feet->inch
            pit['v_mov_in']    = pd.to_numeric(pit['pfx_z'], errors='coerce')*12
            pit_rows.append(pit)

            # Pitch mix (연/투수/피치타입)
            if 'pitch_type' in df.columns:
                cols = ['release_speed','release_spin_rate','release_extension','pfx_x','pfx_z','description','swing','whiff','cs']
                tmp = df[['year','pitcher','pitch_type','launch_speed']+[c for c in cols if c in df.columns]].copy()
                # 보조 마스크
                desc = tmp['description'].astype(str) if 'description' in tmp.columns else pd.Series('', index=tmp.index)
                swing_m = desc.isin(['swinging_strike','swinging_strike_blocked','foul','foul_tip','hit_into_play']) if 'description' in tmp.columns else pd.Series(False, index=tmp.index)
                whiff_m = desc.isin(['swinging_strike','swinging_strike_blocked']) if 'description' in tmp.columns else pd.Series(False, index=tmp.index)
                cs_m    = desc.eq('called_strike') if 'description' in tmp.columns else pd.Series(False, index=tmp.index)

                g2 = tmp.groupby(['year','pitcher','pitch_type'])
                mix = g2.agg(
                    Pitches=('pitch_type','count'),
                    whiffs =('year', lambda x:int(whiff_m.loc[x.index].sum())),
                    swings =('year', lambda x:int(swing_m.loc[x.index].sum())),
                    cs     =('year', lambda x:int(cs_m.loc[x.index].sum())),
                    velo   =('release_speed','mean') if 'release_speed' in tmp.columns else ('year','size'),
                    spin   =('release_spin_rate','mean') if 'release_spin_rate' in tmp.columns else ('year','size'),
                    ext    =('release_extension','mean') if 'release_extension' in tmp.columns else ('year','size'),
                    pfx_x  =('pfx_x','mean') if 'pfx_x' in tmp.columns else ('year','size'),
                    pfx_z  =('pfx_z','mean') if 'pfx_z' in tmp.columns else ('year','size')
                ).reset_index()
                mix['whiff_rate']=mix.apply(lambda r: _rate(r['whiffs'], r['swings']), axis=1)
                mix['csw_rate']  =mix.apply(lambda r: _rate(r['cs']+r['whiffs'], r['Pitches']), axis=1)
                mix['h_mov_in']  = pd.to_numeric(mix['pfx_x'], errors='coerce')*12
                mix['v_mov_in']  = pd.to_numeric(mix['pfx_z'], errors='coerce')*12
                mix_rows.append(mix)

    # ---- 합치고 저장 ----
    bat_df = pd.concat(bat_rows, ignore_index=True) if bat_rows else pd.DataFrame()
    pit_df = pd.concat(pit_rows, ignore_index=True) if pit_rows else pd.DataFrame()
    all_df = pd.concat([bat_df, pit_df], ignore_index=True) if (not bat_df.empty or not pit_df.empty) else pd.DataFrame()

    if all_df.empty:
        (OUT/'statcast_features_player_year.csv').write_text("", encoding='utf-8')
        (OUT/'statcast_agg_player_year.csv').write_text("", encoding='utf-8')
        print("[STATCAST][ENRICH] no rows aggregated")
    else:
        # 보장 컬럼들
        need=['year','mlbam','player_name','role','Pitches','PA','BBE','EV','xwOBA',
              'Swings','Whiffs','whiff_rate','z_swing_rate','o_swing_rate',
              'z_contact_rate','o_contact_rate','hardhit_rate','barrel_rate',
              'chase_rate','csw_rate','avg_ext','avg_spin','h_mov_in','v_mov_in']
        for c in need:
            if c not in all_df.columns: all_df[c]=np.nan
        all_df.to_csv(OUT/'statcast_features_player_year.csv', index=False)

        slim=all_df[['year','mlbam','player_name','role','xwOBA']].copy()
        slim['avg_ev']=pd.to_numeric(all_df['EV'], errors='coerce')  # 이전 호환
        slim['hardhit_rate']=all_df['hardhit_rate']
        slim.to_csv(OUT/'statcast_agg_player_year.csv', index=False)

        print(f"[STATCAST][ENRICH] rows={len(all_df)} -> {OUT/'statcast_features_player_year.csv'}")

    if mix_rows:
        mix_df=pd.concat(mix_rows, ignore_index=True)
        mix_df.to_csv(OUT/'statcast_pitch_mix_player_year.csv', index=False)
        print(f"[STATCAST][PITCH-MIX] rows={len(mix_df)} -> {OUT/'statcast_pitch_mix_player_year.csv'}")
    else:
        (OUT/'statcast_pitch_mix_player_year.csv').write_text("", encoding='utf-8')

def _read_str(df,col): return df[col].astype(str)

if __name__=="__main__":
    aggregate()
