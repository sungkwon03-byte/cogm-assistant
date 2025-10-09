
import pandas as pd, numpy as np, datetime as dt
from pathlib import Path

ROOT=Path.cwd(); OUT=ROOT/'output'; OUT.mkdir(exist_ok=True)

lines = ["# Weekly MLB Report — Auto\n\n",
         f"- Generated: {dt.datetime.utcnow().isoformat()}Z\n\n"]

f = OUT/'statcast_features_player_year.csv'
if f.exists():
    df = pd.read_csv(f, low_memory=False)
    # 필수 컬럼 가드
    need = ["role","year","player_name","xwOBA","avg_ev","hardhit_rate","barrel_rate",
            "whiff_rate","z_whiff_rate","o_swing_rate","z_contact_rate","o_contact_rate",
            "csw_rate","avg_spin","avg_ext","h_mov_in","v_mov_in"]
    for c in need:
        if c not in df.columns: df[c] = np.nan

    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    if 'role' not in df.columns: df['role'] = 'bat'

    yr = int(df['year'].max())
    bat = df[(df['role']=='bat') & (df['year']==yr)].copy()
    pit = df[(df['role']=='pit') & (df['year']==yr)].copy()

    # 타자: xwOBA Top 10
    lines.append(f"## {yr} Hitters — xwOBA Top 10\n")
    topb = bat.sort_values('xwOBA', ascending=False).head(10)
    for _,r in topb.iterrows():
        lines.append(f"- {r.get('player_name','N/A')}: "
                     f"xwOBA {r.get('xwOBA',np.nan):.3f}, "
                     f"EV {r.get('avg_ev',np.nan):.1f}, "
                     f"Barrel {r.get('barrel_rate',np.nan):.3f}, "
                     f"Whiff {r.get('whiff_rate',np.nan):.3f}\n")

    # 투수: CSW% Top 10 (보조로 Z-Whiff / Spin / Extension / 무브)
    lines.append(f"\n## {yr} Pitchers — CSW% Top 10\n")
    topp = pit.sort_values('csw_rate', ascending=False).head(10)
    for _,r in topp.iterrows():
        lines.append(f"- {r.get('player_name','N/A')}: "
                     f"CSW {r.get('csw_rate',np.nan):.3f}, "
                     f"Z-Whiff {r.get('z_whiff_rate',np.nan):.3f}, "
                     f"Spin {r.get('avg_spin',np.nan):.0f}, "
                     f"Ext {r.get('avg_ext',np.nan):.2f}, "
                     f"H {r.get('h_mov_in',np.nan):.1f} in / "
                     f"V {r.get('v_mov_in',np.nan):.1f} in\n")

(OUT/'weekly_report.md').write_text("".join(lines), encoding='utf-8')
print("[DAY70] output/weekly_report.md written")
