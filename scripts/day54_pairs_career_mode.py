#!/usr/bin/env python3
import os, re, pandas as pd, numpy as np

# ---- util ----
def nkey(s):
    s = "" if pd.isna(s) else str(s)
    s = s.replace("."," ").replace("-"," ")
    return re.sub(r"\s+"," ", s).upper().strip()

def tkey(s):
    toks = re.findall(r"[A-Z0-9]+", nkey(s))
    toks.sort()
    return " ".join(toks)

def to_num(sr):
    return pd.to_numeric(sr, errors="coerce").fillna(0)

def ip_to_float(ip):
    # '201.2' -> 201 + 2/3, '100.1' -> 100 + 1/3
    if pd.isna(ip): return 0.0
    s = str(ip).strip()
    if s=="":
        return 0.0
    try:
        if "." in s:
            a,b = s.split(".",1)
            a = int(a)
            if b=="1":  frac=1/3
            elif b=="2": frac=2/3
            else:
                # 이미 소수(예: 1055.3333)
                return float(s)
            return a + frac
        return float(s)
    except:
        try: return float(s)
        except: return 0.0

# ---- paths (있는 것만 사용) ----
KBO_BAT = "data/xleague/kbo_batting.csv"
KBO_PIT = "data/xleague/kbo_pitching.csv"
MLB_TOT_BAT = "output/cache/mlb_totals_bat.csv"
MLB_TOT_PIT = "output/cache/mlb_totals_pit.csv"
NAME2MLBID = "output/cache/name2mlbid.csv"
IDMAP = "output/id_map.csv"

os.makedirs("output", exist_ok=True)

# ---- load KBO batting ----
kb = pd.read_csv(KBO_BAT, dtype=str, low_memory=False)
# 이름/필드 찾기
def pick_name(cols):
    for c in ["full_name","Name","name","player","선수명","선수","이름"]:
        if c in cols: return c
    for c in cols:
        if re.search(r"name|player|선수|이름", str(c), re.I): return c
    return None
kn = pick_name(kb.columns)
PA = next((c for c in ["PA","pa"] if c in kb.columns), None)
H  = next((c for c in ["H","h"] if c in kb.columns), None)
HR = next((c for c in ["HR","hr"] if c in kb.columns), None)
BB = next((c for c in ["BB","bb"] if c in kb.columns), None)
SO = next((c for c in ["SO","so","K","k"] if c in kb.columns), None)
if not all([kn, PA, H, HR, BB, SO]):
    raise SystemExit(f"[FATAL] KBO batting 필수 컬럼 누락: name={kn},PA={PA},H={H},HR={HR},BB={BB},SO={SO}")

kb["_nkey"] = kb[kn].map(nkey)
kb["_tkey"] = kb[kn].map(tkey)
for c in [PA,H,HR,BB,SO]:
    kb[c] = to_num(kb[c])
kb_btot = kb.groupby(["_nkey","_tkey"], as_index=False).agg(
    PA=(PA,"sum"), H=(H,"sum"), HR=(HR,"sum"), BB=(BB,"sum"), SO=(SO,"sum")
)

# ---- load KBO pitching ----
kp = pd.read_csv(KBO_PIT, dtype=str, low_memory=False)
pn = pick_name(kp.columns)
IP = next((c for c in ["IP","ip"] if c in kp.columns), None)
ER = next((c for c in ["ER","er"] if c in kp.columns), None)
Kp = next((c for c in ["SO","so","K","k"] if c in kp.columns), None)
BBp= next((c for c in ["BB","bb"] if c in kp.columns), None)
if not all([pn, IP, ER, Kp, BBp]):
    raise SystemExit(f"[FATAL] KBO pitching 필수 컬럼 누락: name={pn},IP={IP},ER={ER},K={Kp},BB={BBp}")

kp["_nkey"] = kp[pn].map(nkey)
kp["_tkey"] = kp[pn].map(tkey)
kp["_IPf"]  = kp[IP].map(ip_to_float)
for c in [ER,Kp,BBp]:
    kp[c] = to_num(kp[c])
kp_ptot = kp.groupby(["_nkey","_tkey"], as_index=False).agg(
    IPf=("_IPf","sum"), ER=(ER,"sum"), K=(Kp,"sum"), BB=(BBp,"sum")
)

# ---- MLB totals ----
mb = pd.read_csv(MLB_TOT_BAT, dtype=str, low_memory=False)
mp = pd.read_csv(MLB_TOT_PIT, dtype=str, low_memory=False)
mb["mlb_id"] = mb["mlb_id"].astype(str)
mp["mlb_id"] = mp["mlb_id"].astype(str)

# ---- name->mlb_id 맵 (우선순위: name2mlbid → id_map) ----
mmap = pd.DataFrame(columns=["_nkey","_tkey","mlb_id"])
if os.path.exists(NAME2MLBID):
    m1 = pd.read_csv(NAME2MLBID, dtype=str, low_memory=False)
    m1 = m1.rename(columns=str.lower)
    if "full_name" in m1.columns and "mlb_id" in m1.columns:
        m1["_nkey"] = m1["full_name"].map(nkey)
        m1["_tkey"] = m1["full_name"].map(tkey)
        mmap = pd.concat([mmap, m1[["_nkey","_tkey","mlb_id"]]], ignore_index=True)
if os.path.exists(IDMAP):
    m2 = pd.read_csv(IDMAP, dtype=str, low_memory=False)
    m2 = m2.rename(columns=str.lower)
    if "full_name" in m2.columns and "mlb_id" in m2.columns:
        m2["_nkey"] = m2["full_name"].map(nkey)
        m2["_tkey"] = m2["full_name"].map(tkey)
        mmap = pd.concat([mmap, m2[["_nkey","_tkey","mlb_id"]]], ignore_index=True)

mmap = mmap.dropna(subset=["mlb_id"]).drop_duplicates()

# ---- KBO totals ↔ MLB totals 매칭 (tkey 우선, 실패시 nkey) ----
def attach_mlb_id(df):
    out = df.merge(mmap, on=["_tkey"], how="left")
    miss = out["mlb_id"].isna()
    if miss.any():
        out.loc[miss, :] = out[miss].drop(columns=["mlb_id"]).merge(
            mmap[["_nkey","mlb_id"]].drop_duplicates(), on="_nkey", how="left"
        )
    return out

kb_btot = attach_mlb_id(kb_btot)
kp_ptot = attach_mlb_id(kp_ptot)

# 남은 미매칭 제거(mlb_id 없는 건 페어 만들 수 없음)
kb_btot = kb_btot[kb_btot["mlb_id"].notna()]
kp_ptot = kp_ptot[kp_ptot["mlb_id"].notna()]

# ---- MLB totals join
mb_use = mb[["mlb_id","PA","H","HR","BB","SO","hr_pa","bb_pct","so_pct","h_pa","ops_proxy"]].copy()
mp_use = mp[["mlb_id","IP","ER","K","BB","ERA","K9","BB9"]].copy()

# 수치화
for c in ["PA","H","HR","BB","SO"]:
    if c in mb_use.columns: mb_use[c] = to_num(mb_use[c])
for c in ["hr_pa","bb_pct","so_pct","h_pa","ops_proxy"]:
    if c in mb_use.columns: mb_use[c] = pd.to_numeric(mb_use[c], errors="coerce")
for c in ["IP","ER","K","BB","ERA","K9","BB9"]:
    if c in mp_use.columns: mp_use[c] = pd.to_numeric(mp_use[c], errors="coerce")

# ---- batting pairs (career)
bat = kb_btot.merge(mb_use, on="mlb_id", how="inner", suffixes=("_kbo","_mlb"))
bat["pa_kbo"] = bat["PA"]
pairs_b=[]
for _,r in bat.iterrows():
    pa_k = max(float(r["PA"]), 1.0)
    pairs_b += [
        ("hr_pa",  float(r["HR"])/pa_k,        float(r["hr_pa"]),  r["mlb_id"], "career"),
        ("bb_pct", float(r["BB"])/pa_k,        float(r["bb_pct"]), r["mlb_id"], "career"),
        ("so_pct", float(r["SO"])/pa_k,        float(r["so_pct"]), r["mlb_id"], "career"),
        ("h_pa",   float(r["H"])/pa_k,         float(r["h_pa"]),   r["mlb_id"], "career"),
    ]
    # ops_proxy: KBO에 OBP/SLG 없으므로 생략(원하면 OPS로 대체 가능)
dfb = pd.DataFrame(pairs_b, columns=["metric","kbo_val","mlb_val","player_id","season"])
dfb.to_csv("output/KBO_bat_pairs.csv", index=False)

# ---- pitching pairs (career)
pit = kp_ptot.merge(mp_use, on="mlb_id", how="inner", suffixes=("_kbo","_mlb"))
pairs_p=[]
for _,r in pit.iterrows():
    ipk = max(float(r["IPf"]), 1e-9)
    # ERA/K9/BB9(KBO)
    era_k = (float(r["ER"])*9.0)/ipk
    k9_k  = (float(r["K"])*9.0)/ipk
    bb9_k = (float(r["BB"])*9.0)/ipk
    pairs_p += [
        ("ERA", era_k, float(r["ERA"]), r["mlb_id"], "career"),
        ("K9",  k9_k,  float(r["K9"]),  r["mlb_id"], "career"),
        ("BB9", bb9_k, float(r["BB9"]), r["mlb_id"], "career"),
    ]
dfp = pd.DataFrame(pairs_p, columns=["metric","kbo_val","mlb_val","player_id","season"])
dfp.to_csv("output/KBO_pit_pairs.csv", index=False)

print("[OK] wrote output/KBO_bat_pairs.csv , output/KBO_pit_pairs.csv")
