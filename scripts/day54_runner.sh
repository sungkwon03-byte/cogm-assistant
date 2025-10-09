set -euo pipefail
exec > >(tee -a output/day54.log) 2>&1
echo "== Day54 KBO 실데이터 주입 + 브리지 계수 =="

# 0) kaggle 준비(조용히)
python3 -m pip install --user kaggle >/dev/null 2>&1 || true
export PATH="$HOME/.local/bin:$PATH"
test -f "$HOME/.kaggle/kaggle.json" || { echo "[ERROR] ~/.kaggle/kaggle.json 없음"; exit 2; }
chmod 600 "$HOME/.kaggle/kaggle.json" || true

# 1) 파일명만 조회 → 큰 합본 CSV 1개씩만 뽑기
kaggle datasets files -d mattop/baseball-kbo-batting-data-1982-2021 > /tmp/kbo_bat.list.txt
kaggle datasets files -d mattop/korean-baseball-pitching-data-1982-2021 > /tmp/kbo_pit.list.txt
BAT_FILE=$(awk '/\.csv/ {print $1}' /tmp/kbo_bat.list.txt | head -1)
PIT_FILE=$(awk '/\.csv/ {print $1}' /tmp/kbo_pit.list.txt | head -1)
[ -n "$BAT_FILE" ] && [ -n "$PIT_FILE" ] || { echo "[ERROR] Kaggle 목록에서 CSV 못 찾음"; exit 3; }

# 2) 해당 파일만 레이트리밋 걸고 다운로드(세션 종료 방지)
kaggle datasets download -d mattop/baseball-kbo-batting-data-1982-2021  -f "$BAT_FILE" -p data/xleague -q
kaggle datasets download -d mattop/korean-baseball-pitching-data-1982-2021 -f "$PIT_FILE" -p data/xleague -q
for z in data/xleague/*.zip; do [ -f "$z" ] || continue; unzip -oq "$z" -d data/xleague && rm -f "$z"; done

# 3) 표준 파일명 정리
BAT_SRC=$(ls -1 data/xleague/*.csv | grep -i batt | head -1)
PIT_SRC=$(ls -1 data/xleague/*.csv | grep -i pitch | head -1)
cp -f "$BAT_SRC" data/xleague/kbo_batting.csv
cp -f "$PIT_SRC" data/xleague/kbo_pitching.csv
echo "[INPUT] kbo_batting.csv lines=$(wc -l < data/xleague/kbo_batting.csv)"
echo "[INPUT] kbo_pitching.csv lines=$(wc -l < data/xleague/kbo_pitching.csv)"

# 4) 브리지 계수 산출(메모리 안전/조용)
python3 - <<'PY'
import os, re, json, pandas as pd
from statistics import median

os.makedirs("output", exist_ok=True)

def any_col(df, cands):
    for c in cands:
        if c in df.columns: return c
    return None

def to_float(s):
    try:
        s=str(s).replace(",","").strip()
        if not s or s.lower()=="nan": return None
        return float(s)
    except: return None

def safe_div(a,b): return (a or 0.0)/b if (b and b!=0) else None

def coefs_xy(x, y):
    pairs=[(xi, yi) for xi,yi in zip(x,y) if xi is not None and yi is not None]
    if len(pairs)<10: return {"a": None, "b": None, "n": len(pairs)}
    ratios=[yi/xi for xi,yi in pairs if xi!=0]
    a = median(ratios) if ratios else 0.0
    resid=[yi - a*xi for xi,yi in pairs]
    b = median(resid) if resid else 0.0
    return {"a": float(a), "b": float(b), "n": len(pairs)}

# MLB 집계
mlb = pd.read_csv("output/player_box.csv", dtype=str, low_memory=True)
mlb.columns=[c.lower() for c in mlb.columns]
def fnum(s): 
    try: return float(s)
    except: return 0.0
grp = mlb.groupby(["mlb_id","name"], dropna=False)
mlb_bat = grp.agg({
    "pa": lambda s: sum(fnum(x) for x in s) if "pa" in mlb.columns else 0.0,
    "h":  lambda s: sum(fnum(x) for x in s) if "h"  in mlb.columns else 0.0,
    "hr": lambda s: sum(fnum(x) for x in s) if "hr" in mlb.columns else 0.0,
    "bb": lambda s: sum(fnum(x) for x in s) if "bb" in mlb.columns else 0.0,
    "so": lambda s: sum(fnum(x) for x in s) if "so" in mlb.columns else 0.0,
}).reset_index().rename(columns={"name":"full_name"})
mlb_bat["pa"]=mlb_bat["pa"].replace({0.0:1.0})
mlb_bat["hr_pa"]=mlb_bat["hr"]/mlb_bat["pa"]
mlb_bat["bb_pct"]=mlb_bat["bb"]/mlb_bat["pa"]
mlb_bat["so_pct"]=mlb_bat["so"]/mlb_bat["pa"]
mlb_bat["h_pa"]= mlb_bat["h"]/ mlb_bat["pa"]
mlb_bat["ops_proxy"]= (mlb_bat["h"]+mlb_bat["bb"])/mlb_bat["pa"] + mlb_bat["hr_pa"]

mlb_pit = grp.agg({
    "ip_outs": lambda s: sum(fnum(x) for x in s) if "ip_outs" in mlb.columns else 0.0,
    "k_p":     lambda s: sum(fnum(x) for x in s) if "k_p" in mlb.columns else 0.0,
    "bb_p":    lambda s: sum(fnum(x) for x in s) if "bb_p" in mlb.columns else 0.0,
    "er":      lambda s: sum(fnum(x) for x in s) if "er" in mlb.columns else 0.0,
}).reset_index().rename(columns={"name":"full_name"})
mlb_pit["ip"]=mlb_pit["ip_outs"]/3.0
mlb_pit["ip"]=mlb_pit["ip"].replace({0.0:1.0})
mlb_pit["era"]=9.0*(mlb_pit["er"]/mlb_pit["ip"])
mlb_pit["k9"]=9.0*(mlb_pit["k_p"]/mlb_pit["ip"])
mlb_pit["bb9"]=9.0*(mlb_pit["bb_p"]/mlb_pit["ip"])

# KBO 로드 & 정규화
kbo_bat = pd.read_csv("data/xleague/kbo_batting.csv", dtype=str, low_memory=True)
kbo_pit = pd.read_csv("data/xleague/kbo_pitching.csv", dtype=str, low_memory=True)
kbo_bat.columns=[c.strip().lower() for c in kbo_bat.columns]
kbo_pit.columns=[c.strip().lower() for c in kbo_pit.columns]

def norm_bat(df):
    ncol=any_col(df,["name","player","playername","선수명","full_name"])
    pa=any_col(df,["pa","plate appearances","타석"])
    h= any_col(df,["h","hits","안타"])
    hr=any_col(df,["hr","홈런"])
    bb=any_col(df,["bb","walks","볼넷"])
    so=any_col(df,["so","strikeouts","삼진"])
    ops=any_col(df,["ops"])
    out=pd.DataFrame()
    out["full_name"]=df[ncol] if ncol else ""
    out["pa"]=df[pa].map(to_float) if pa else None
    out["h"]= df[h].map(to_float) if h else None
    out["hr"]=df[hr].map(to_float) if hr else None
    out["bb"]=df[bb].map(to_float) if bb else None
    out["so"]=df[so].map(to_float) if so else None
    out["ops"]=df[ops].map(to_float) if ops else None
    out["pa"]=out["pa"].fillna(1.0)
    out["hr_pa"]=out.apply(lambda r: safe_div(r["hr"], r["pa"]), axis=1)
    out["bb_pct"]=out.apply(lambda r: safe_div(r["bb"], r["pa"]), axis=1)
    out["so_pct"]=out.apply(lambda r: safe_div(r["so"], r["pa"]), axis=1)
    out["h_pa"]= out.apply(lambda r: safe_div(r["h"],  r["pa"]), axis=1)
    out["ops_proxy"]=out["ops"]
    mask=out["ops_proxy"].isna()
    out.loc[mask,"ops_proxy"]=out.loc[mask,["h_pa","bb_pct","hr_pa"]].sum(axis=1)
    return out

def norm_pit(df):
    ncol=any_col(df,["name","player","playername","선수명","full_name"])
    ip=any_col(df,["ip","이닝"])
    er=any_col(df,["er","자책"])
    k =any_col(df,["so","k","삼진"])
    bb=any_col(df,["bb","볼넷"])
    out=pd.DataFrame()
    out["full_name"]=df[ncol] if ncol else ""
    out["ip"]=df[ip].map(to_float) if ip else None
    out["er"]=df[er].map(to_float) if er else None
    out["k"]= df[k ].map(to_float) if k  else None
    out["bb"]=df[bb].map(to_float) if bb else None
    out["ip"]=out["ip"].fillna(1.0)
    out["era"]=9.0*out.apply(lambda r: safe_div(r["er"], r["ip"]), axis=1)
    out["k9"]= 9.0*out.apply(lambda r: safe_div(r["k"],  r["ip"]), axis=1)
    out["bb9"]= 9.0*out.apply(lambda r: safe_div(r["bb"], r["ip"]), axis=1)
    return out

kbo_bat=norm_bat(kbo_bat)
kbo_pit=norm_pit(kbo_pit)

def nkey(s):
    if s is None: return ""
    s=str(s).strip().upper()
    s=re.sub(r"\s+"," ", s)
    return s

for df in (mlb_bat, mlb_pit, kbo_bat, kbo_pit):
    if not df.empty: df["nkey"]=df["full_name"].map(nkey)

def pair(df_x, df_mlb, keys):
    if df_x.empty: return pd.DataFrame()
    return df_x[["nkey"]+keys].merge(df_mlb[["nkey"]+keys], on="nkey", suffixes=("_x","_mlb"))

bat_keys=["hr_pa","bb_pct","so_pct","h_pa","ops_proxy"]
pit_keys=["era","k9","bb9"]

pairs = {
    "KBO_bat": pair(kbo_bat, mlb_bat, bat_keys),
    "KBO_pit": pair(kbo_pit, mlb_pit, pit_keys),
}

coeffs={}
for tag,df in pairs.items():
    cset={}
    for k in (bat_keys if "bat" in tag else pit_keys):
        x=df[f"{k}_x"].tolist(); y=df[f"{k}_mlb"].tolist()
        cset[k]=coefs_xy(x,y)
    coeffs[tag]=cset

rows=[]
for tag,cset in coeffs.items():
    for k,v in cset.items():
        rows.append({"group":tag,"metric":k,"n":v["n"],"a":v["a"],"b":v["b"]})
pd.DataFrame(rows).to_csv("output/bridge_coef.csv", index=False)
with open("output/xleague_coeffs.json","w") as f: json.dump(coeffs, f, indent=2)

def blok(tag, df, keys):
    L=[f"[{tag}] pairs={len(df)}"]
    for k in keys:
        c=coeffs[tag][k]
        L.append(f"  - {k}: n={c['n']}, a={c['a']}, b={c['b']}")
    return "\n".join(L)

rep=[]
rep.append(blok("KBO_bat", pairs["KBO_bat"], bat_keys))
rep.append(blok("KBO_pit", pairs["KBO_pit"], pit_keys))
open("output/bridge_report.txt","w").write("\n".join(rep)+"\n")
print("[OK] outputs: xleague_coeffs.json / bridge_coef.csv / bridge_report.txt")
PY
echo "== Day54 DONE =="
