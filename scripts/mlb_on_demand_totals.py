import os, json, time, argparse, hashlib, re
from typing import Dict, Any, List
import requests, pandas as pd

# ---- Config ----
ROUTES = {
    # 검색 라우트는 사용하지 않는다 (전량 금지)
    "people_stats_career": "https://statsapi.mlb.com/api/v1/people/{id}/stats?group={group}&stats=career&gameType=R"
}
CACHE_DIR = "output/cache/statsapi"
os.makedirs(CACHE_DIR, exist_ok=True)
TTL_SEC = 14*24*3600  # 14 days

# ---- Utils ----
def _norm(s:str)->str:
    s = "" if s is None else str(s)
    s = s.replace("."," ").replace("-"," ")
    s = re.sub(r"\s+"," ", s).strip().upper()
    return s

def _cache_path(url:str)->str:
    h = hashlib.sha1(url.encode()).hexdigest()
    return os.path.join(CACHE_DIR, f"{h}.json")

def _get_json(url:str, retries=3, backoff=0.7)->Dict[str,Any]:
    cp = _cache_path(url)
    if os.path.exists(cp) and (time.time()-os.path.getmtime(cp) < TTL_SEC):
        try:
            return json.load(open(cp))
        except Exception:
            pass
    last=None
    for i in range(retries):
        try:
            r = requests.get(url, timeout=30)
            if r.ok:
                data = r.json()
                json.dump(data, open(cp,"w"))
                return data
            last=f"HTTP {r.status_code}"
        except Exception as e:
            last=repr(e)
        time.sleep(backoff*(2**i))
    # 실패해도 빈 dict로 돌려 안정화
    return {}

def _ip_to_float(ip_str:str)->float:
    if not ip_str:
        return 0.0
    try:
        s=str(ip_str)
        if "." in s:
            whole, frac = s.split(".")
            if frac=="1": return float(whole) + 1/3
            if frac=="2": return float(whole) + 2/3
            return float(s)
        return float(s)
    except Exception:
        return 0.0

# ---- Local Name→ID Index ----
_NAME_INDEX = None
def _load_name_index()->Dict[str,str]:
    global _NAME_INDEX
    if _NAME_INDEX is not None:
        return _NAME_INDEX
    path = "output/cache/name2mlbid.csv"
    base = {}
    if os.path.exists(path):
        df = pd.read_csv(path, dtype=str).fillna("")
        for _, r in df.iterrows():
            full = str(r.get("full_name",""))
            mid  = str(r.get("mlb_id","") or "")
            if not mid: 
                continue
            k = _norm(full)
            base.setdefault(k, mid)
            base.setdefault(k.replace(" ",""), mid)  # no-space key
    _NAME_INDEX = base
    return _NAME_INDEX

def resolve_id_by_name(name:str)->str:
    idx = _load_name_index()
    k = _norm(name)
    for key in (k, k.replace(" ","")):
        v = idx.get(key,"")
        if v:
            return v
    parts = k.split(" ")
    if len(parts)==2:
        rev = f"{parts[1]} {parts[0]}"
        for key in (rev, rev.replace(" ","")):
            v = idx.get(key,"")
            if v:
                return v
    return ""

def career_stats(mlb_id:str, group:str)->Dict[str,Any]:
    url = ROUTES["people_stats_career"].format(id=mlb_id, group=group)
    js  = _get_json(url)
    try:
        stats = js.get("stats", [])
        if not stats: 
            return {}
        splits = stats[0].get("splits", [])
        if not splits:
            return {}
        stat = splits[0].get("stat", {})
        return stat if isinstance(stat, dict) else {}
    except Exception:
        return {}

# ---- Main ----
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--names-file", help="CSV with a column of names", default="")
    ap.add_argument("--names-col",  help="column name for names", default="mlb_name")
    ap.add_argument("--ids-file",   help="CSV with mlb_id column", default="")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    names:List[str]=[]
    ids:List[str]=[]

    if args.names_file:
        df = pd.read_csv(args.names_file, dtype=str).fillna("")
        col = args.names_col if args.names_col in df.columns else df.columns[0]
        names = [x for x in df[col].tolist() if x]

    if args.ids_file:
        di = pd.read_csv(args.ids_file, dtype=str).fillna("")
        ids = [str(x) for x in di.get("mlb_id",[]).tolist() if str(x)]

    targets=[]
    for x in names: targets.append(("name", x))
    for x in ids:   targets.append(("id",   x))

    if args.limit>0:
        targets = targets[:args.limit]

    bat_rows=[]; pit_rows=[]

    for typ, val in targets:
        if typ=="name":
            mlb_id = resolve_id_by_name(val)
            if not mlb_id:
                print(f"[WARN] name not resolved: {val}")
                continue
            disp = val
        else:
            mlb_id = val
            disp = ""

        # hitting
        bat = career_stats(mlb_id, "hitting")
        if isinstance(bat, dict) and bat:
            def f(x): 
                try: return float(x)
                except: return 0.0
            pa = f(bat.get("plateAppearances",0))
            h  = f(bat.get("hits",0))
            hr = f(bat.get("homeRuns",0))
            bb = f(bat.get("baseOnBalls",0))
            so = f(bat.get("strikeOuts",0))
            pa = pa if pa>0 else 1.0
            bat_rows.append({
                "mlb_id": mlb_id, "full_name": disp or bat.get("player",""),
                "PA": pa, "H": h, "HR": hr, "BB": bb, "SO": so,
                "hr_pa": hr/pa, "bb_pct": bb/pa, "so_pct": so/pa, "h_pa": h/pa,
                "ops_proxy": (h+bb)/pa + hr/pa
            })

        # pitching
        pit = career_stats(mlb_id, "pitching")
        if isinstance(pit, dict) and pit:
            def f(x): 
                try: return float(x)
                except: return 0.0
            ip = _ip_to_float(pit.get("inningsPitched","0"))
            er = f(pit.get("earnedRuns",0))
            k  = f(pit.get("strikeOuts",0))
            bb = f(pit.get("baseOnBalls",0))
            ip = ip if ip>0 else 1.0
            pit_rows.append({
                "mlb_id": mlb_id, "full_name": disp or pit.get("player",""),
                "IP": ip, "ER": er, "K": k, "BB": bb,
                "ERA": 9.0*er/ip, "K9": 9.0*k/ip, "BB9": 9.0*bb/ip
            })

    pd.DataFrame(bat_rows).to_csv("output/cache/mlb_totals_bat.csv", index=False)
    pd.DataFrame(pit_rows).to_csv("output/cache/mlb_totals_pit.csv", index=False)
    print(f"[OK] wrote output/cache/mlb_totals_bat.csv ({len(bat_rows)}) , mlb_totals_pit.csv ({len(pit_rows)})")

if __name__=="__main__":
    main()
