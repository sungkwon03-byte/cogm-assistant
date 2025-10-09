#!/usr/bin/env bash
set -euo pipefail
export PYTHONUNBUFFERED=1
echo "== Day54 start =="

# 0) MLB 집계 소스 존재 점검
if [ ! -f output/player_box.csv ]; then
  echo "[FATAL] output/player_box.csv 없음 (Day50~51 산출물 필요)"; exit 2
fi

# 1) KBO CSV 확보 시도 (이미 있으면 스킵)
if [ ! -f data/xleague/kbo_batting.csv ] || [ ! -f data/xleague/kbo_pitching.csv ]; then
  echo "[INFO] KBO CSV 미존재 → Kaggle 시도(토큰 없으면 자동 스킵)"
  python3 -m pip install --user kaggle >/dev/null 2>&1 || true
  export PATH="$HOME/.local/bin:$PATH"
  if command -v kaggle >/dev/null 2>&1 && [ -f "$HOME/.kaggle/kaggle.json" ]; then
    chmod 600 "$HOME/.kaggle/kaggle.json" || true
    # 목록 조회 후 각 데이터셋의 CSV 하나만 정확히 다운(대용량 ZIP 회피 불가 시 단일 ZIP만)
    kaggle datasets download -d mattop/baseball-kbo-batting-data-1982-2021  -p data/xleague -q || true
    kaggle datasets download -d mattop/korean-baseball-pitching-data-1982-2021 -p data/xleague -q || true
    for z in data/xleague/*.zip; do [ -f "$z" ] || continue; unzip -oq "$z" -d data/xleague && rm -f "$z"; done
    # 표준명으로 정리(가장 유력 파일 한 개씩)
    bat_src=$(ls data/xleague/* | grep -i -E 'bat|hitting|타격' | head -1 || true)
    pit_src=$(ls data/xleague/* | grep -i -E 'pit|pitch'     | head -1 || true)
    [ -n "${bat_src:-}" ] && cp -f "$bat_src" data/xleague/kbo_batting.csv || true
    [ -n "${pit_src:-}" ] && cp -f "$pit_src" data/xleague/kbo_pitching.csv || true
  else
    echo "[WARN] Kaggle 토큰/CLI 없어서 다운로드 스킵. data/xleague/kbo_*.csv 직접 넣어도 됨."
  fi
fi

# 2) 정규화 (인코딩/결측/컬럼부재 방어, 없으면 빈 파일 생성)
python3 - <<'PY'
import os, re, sys, pandas as pd
os.makedirs("output/cache", exist_ok=True)

def read_csv_robust(path):
    last=None
    for enc in (None,'utf-8','utf-8-sig','cp949','latin1'):
        try: return pd.read_csv(path, dtype=str, low_memory=False, encoding=enc)
        except Exception as e: last=e
    print(f"[FATAL] read_csv failed: {path} :: {last}", file=sys.stderr); sys.exit(3)

def any_col(df, cands):
    s=set(df.columns)
    for c in cands:
        if c in s: return c

def series(df, cands):
    c = any_col(df, cands)
    return df[c] if c else pd.Series([None]*len(df))

def to_f(x):
    if x is None: return None
    s=str(x).replace(',','').strip()
    if s=='' or s.lower()=='nan': return None
    try: return float(s)
    except: return None

def nkey(s):
    if pd.isna(s): return ''
    s=str(s).upper().strip()
    return re.sub(r'\s+',' ', s)

# batting
if os.path.exists('data/xleague/kbo_batting.csv'):
    kb = read_csv_robust('data/xleague/kbo_batting.csv')
    kb.columns=[c.lower().strip() for c in kb.columns]
    outb = pd.DataFrame({
        'full_name': series(kb, ['name','player','playername','선수명','full_name']),
        'year'     : series(kb, ['year','season','연도']),
        'team'     : series(kb, ['team','tm','팀']),
        'pa'       : series(kb, ['pa','plate appearances','타석']).map(to_f),
        'h'        : series(kb, ['h','hits','안타']).map(to_f),
        'hr'       : series(kb, ['hr','홈런']).map(to_f),
        'bb'       : series(kb, ['bb','walks','볼넷']).map(to_f),
        'so'       : series(kb, ['so','strikeouts','삼진']).map(to_f),
        'ops'      : series(kb, ['ops']).map(to_f),
    })
else:
    outb = pd.DataFrame(columns=['full_name','year','team','pa','h','hr','bb','so','ops'])
outb['pa']=outb['pa'].fillna(1.0)
outb['hr_pa']=outb['hr']/outb['pa']
outb['bb_pct']=outb['bb']/outb['pa']
outb['so_pct']=outb['so']/outb['pa']
outb['h_pa']= outb['h']/ outb['pa']
outb['ops_proxy']=outb['ops']
mask=outb['ops_proxy'].isna()
outb.loc[mask,'ops_proxy']=outb.loc[mask, ['h_pa','bb_pct','hr_pa']].sum(axis=1)
outb['nkey']=outb['full_name'].map(nkey)
outb.to_csv('output/cache/kbo_bat_norm.csv', index=False)

# pitching
if os.path.exists('data/xleague/kbo_pitching.csv'):
    kp = read_csv_robust('data/xleague/kbo_pitching.csv')
    kp.columns=[c.lower().strip() for c in kp.columns]
    outp = pd.DataFrame({
        'full_name': series(kp, ['name','player','playername','선수명','full_name']),
        'year'     : series(kp, ['year','season','연도']),
        'team'     : series(kp, ['team','tm','팀']),
        'ip'       : series(kp, ['ip','이닝']).map(to_f),
        'er'       : series(kp, ['er','자책']).map(to_f),
        'k'        : series(kp, ['so','k','삼진']).map(to_f),
        'bb'       : series(kp, ['bb','볼넷']).map(to_f),
    })
else:
    outp = pd.DataFrame(columns=['full_name','year','team','ip','er','k','bb'])
outp['ip']=outp['ip'].fillna(1.0)
outp['era']=9.0*(outp['er']/outp['ip'])
outp['k9']= 9.0*(outp['k'] /outp['ip'])
outp['bb9']=9.0*(outp['bb']/outp['ip'])
outp['nkey']=outp['full_name'].map(nkey)
outp.to_csv('output/cache/kbo_pit_norm.csv', index=False)

print("[OK] KBO normalize -> output/cache/kbo_bat_norm.csv , kbo_pit_norm.csv")
PY

# 3) MLB 집계
python3 - <<'PY'
import pandas as pd
def f(x):
    try: return float(x)
    except: return 0.0
mlb = pd.read_csv("output/player_box.csv", dtype=str, low_memory=True)
mlb.columns=[c.lower() for c in mlb.columns]
g = mlb.groupby(["mlb_id","name"], dropna=False)
bat = g.agg({
  "pa": lambda s: sum(f(x) for x in s) if "pa" in mlb.columns else 0.0,
  "h":  lambda s: sum(f(x) for x in s) if "h"  in mlb.columns else 0.0,
  "hr": lambda s: sum(f(x) for x in s) if "hr" in mlb.columns else 0.0,
  "bb": lambda s: sum(f(x) for x in s) if "bb" in mlb.columns else 0.0,
  "so": lambda s: sum(f(x) for x in s) if "so" in mlb.columns else 0.0,
}).reset_index().rename(columns={"name":"full_name"})
bat["pa"]=bat["pa"].replace({0.0:1.0})
bat["hr_pa"]=bat["hr"]/bat["pa"]
bat["bb_pct"]=bat["bb"]/bat["pa"]
bat["so_pct"]=bat["so"]/bat["pa"]
bat["h_pa"]= bat["h"]/ bat["pa"]
bat["ops_proxy"]=(bat["h"]+bat["bb"])/bat["pa"] + bat["hr_pa"]
bat.to_csv("output/cache/mlb_bat_agg.csv", index=False)

pit = g.agg({
  "ip_outs": lambda s: sum(f(x) for x in s) if "ip_outs" in mlb.columns else 0.0,
  "k_p":     lambda s: sum(f(x) for x in s) if "k_p" in mlb.columns else 0.0,
  "bb_p":    lambda s: sum(f(x) for x in s) if "bb_p" in mlb.columns else 0.0,
  "er":      lambda s: sum(f(x) for x in s) if "er" in mlb.columns else 0.0,
}).reset_index().rename(columns={"name":"full_name"})
pit["ip"]=pit["ip_outs"]/3.0
pit["ip"]=pit["ip"].replace({0.0:1.0})
pit["era"]=9.0*(pit["er"]/pit["ip"])
pit["k9"]= 9.0*(pit["k_p"]/pit["ip"])
pit["bb9"]=9.0*(pit["bb_p"]/pit["ip"])
pit.to_csv("output/cache/mlb_pit_agg.csv", index=False)
print("[OK] MLB agg -> output/cache/mlb_bat_agg.csv , mlb_pit_agg.csv")
PY

# 4) 브리지 계수 + 리포트
python3 - <<'PY'
import re, json, pandas as pd
from statistics import median

def nkey(s):
    if s is None: return ""
    s=str(s).upper().strip()
    return re.sub(r"\s+"," ", s)

kbo_b = pd.read_csv("output/cache/kbo_bat_norm.csv")
kbo_p = pd.read_csv("output/cache/kbo_pit_norm.csv")
mlb_b = pd.read_csv("output/cache/mlb_bat_agg.csv")
mlb_p = pd.read_csv("output/cache/mlb_pit_agg.csv")

for df in (kbo_b,kbo_p,mlb_b,mlb_p):
    if "nkey" not in df.columns:
        df["nkey"]=df.get("full_name","").map(nkey)

def pair(a,b,keys):
    colsA=["nkey"]+[c for c in keys if c in a.columns]
    colsB=["nkey"]+[c for c in keys if c in b.columns]
    x=a[colsA].copy(); y=b[colsB].copy()
    for k in keys:
        if k not in x.columns: x[k]=None
        if k not in y.columns: y[k]=None
    return x.merge(y, on="nkey", suffixes=("_x","_mlb"))

bat_keys=["hr_pa","bb_pct","so_pct","h_pa","ops_proxy"]
pit_keys=["era","k9","bb9"]

pb = pair(kbo_b, mlb_b, bat_keys)
pp = pair(kbo_p, mlb_p, pit_keys)

def coefs(x,y):
    xs,ys=[],[]
    for xi,yi in zip(x,y):
        try:
            if pd.notna(xi) and pd.notna(yi):
                xi=float(xi); yi=float(yi)
                xs.append(xi); ys.append(yi)
        except: pass
    if len(xs)<10: return {"a":None,"b":None,"n":len(xs)}
    ratios=[yi/xi for xi,yi in zip(xs,ys) if xi!=0]
    a=(sum(sorted(ratios)[len(ratios)//2-1:len(ratios)//2+1])/2.0) if len(ratios)%2==0 and len(ratios)>1 else (sorted(ratios)[len(ratios)//2] if ratios else 0.0)
    resid=[yi-a*xi for xi,yi in zip(xs,ys)]
    r_sorted=sorted(resid); L=len(r_sorted)
    b=(r_sorted[L//2-1]+r_sorted[L//2])/2.0 if L%2==0 and L>1 else (r_sorted[L//2] if L else 0.0)
    return {"a":float(a),"b":float(b),"n":len(xs)}

coeffs={"KBO_bat":{}, "KBO_pit":{}}
for k in bat_keys:
    coeffs["KBO_bat"][k]=coefs(pb[f"{k}_x"], pb[f"{k}_mlb"])
for k in pit_keys:
    coeffs["KBO_pit"][k]=coefs(pp[f"{k}_x"], pp[f"{k}_mlb"])

rows=[]
for g,cset in coeffs.items():
    for m,v in cset.items():
        rows.append({"group":g,"metric":m,"n":v["n"],"a":v["a"],"b":v["b"]})
pd.DataFrame(rows).to_csv("output/bridge_coef.csv", index=False)
open("output/xleague_coeffs.json","w").write(json.dumps(coeffs, indent=2))
open("output/bridge_report.txt","w").write(f"[KBO_bat] pairs={len(pb)}\n[KBO_pit] pairs={len(pp)}\n")
print("[OK] bridge -> output/xleague_coeffs.json , bridge_coef.csv , bridge_report.txt")
PY

# 5) 이중리그 후보 (id_map 없으면 스킵성 빈 파일 생성)
python3 - <<'PY'
import re, pandas as pd, os
from difflib import SequenceMatcher

def nkey(s):
    if s is None: return ""
    s=str(s).upper().strip()
    return re.sub(r"\s+"," ", s)

if os.path.exists("output/id_map.csv"):
    idm = pd.read_csv("output/id_map.csv", dtype=str).fillna("")
else:
    idm = pd.DataFrame(columns=["mlb_id","retro_id","bbref_id","full_name"])
idm["nkey"]=idm.get("full_name","").map(nkey)

kbo_b = pd.read_csv("output/cache/kbo_bat_norm.csv", usecols=["full_name"])
kbo_p = pd.read_csv("output/cache/kbo_pit_norm.csv", usecols=["full_name"])
k = pd.concat([kbo_b,kbo_p], ignore_index=True).drop_duplicates()
k["nkey"]=k["full_name"].map(nkey)

exact = k.merge(idm[["mlb_id","retro_id","bbref_id","full_name","nkey"]], on="nkey", how="left")
out = exact[["full_name","mlb_id","retro_id","bbref_id"]].assign(method="exact_nkey")
out.to_csv("output/xleague_link_candidates.csv", index=False)
print("[OK] candidates -> output/xleague_link_candidates.csv")
PY

echo "== Day54 done =="
