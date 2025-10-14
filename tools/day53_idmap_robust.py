import sys, pandas as pd, numpy as np, re, unicodedata as U
from pathlib import Path

def norm(s:str)->str:
    if s is None or (isinstance(s,float) and np.isnan(s)): return ""
    s=str(s)
    s=U.normalize("NFKD", s)
    s="".join(ch for ch in s if not U.combining(ch))
    s=re.sub(r"[.\-']", " ", s).strip()
    s=re.sub(r"\s+", " ", s).upper()
    return s

def pick(df, cands):
    cols = list(df.columns)
    low  = {c.lower():c for c in cols}
    # exact
    for c in cands:
        if c in cols: return c
        if c in low: return low[c]
    # looser (case-insensitive)
    for c in cands:
        lc=c.lower()
        if lc in low: return low[lc]
    return None

def load_lahman(path):
    # Lahman People.csv: nameFirst/nameLast/retroID/bbrefID/ (mlbam은 보통 없음)
    a = pd.read_csv(path, dtype=str, low_memory=False)
    fn = pick(a, ["name_first","nameFirst","namefirst","name_given","nameGiven"])
    ln = pick(a, ["name_last","nameLast","namelast"])
    retro = pick(a, ["retro_id","retroID","retroId"])
    bbref = pick(a, ["bbref_id","bbrefID","bbrefId"])
    # 일부 포크에는 key_mlbam이 있을 수 있음
    mlbam = pick(a, ["mlb_id","key_mlbam","mlbam","mlbamID"])
    out = pd.DataFrame({
        "name_first": a[fn] if fn else np.nan,
        "name_last":  a[ln] if ln else np.nan,
        "retro_id":   a[retro] if retro else np.nan,
        "bbref_id":   a[bbref] if bbref else np.nan,
        "mlb_id":     pd.to_numeric(a[mlbam], errors="coerce") if mlbam else pd.Series([np.nan]*len(a))
    })
    out["full_name"] = (out["name_first"].fillna("")+" "+out["name_last"].fillna("")).str.strip()
    out["_n"] = out["full_name"].map(norm)
    return out

def load_chadwick(path):
    # Chadwick register people-*.csv: key_mlbam/key_retro/key_bbref/name_first/name_last
    b = pd.read_csv(path, dtype=str, low_memory=False)
    fn = pick(b, ["name_first","nameFirst"])
    ln = pick(b, ["name_last","nameLast"])
    retro = pick(b, ["key_retro"])
    bbref = pick(b, ["key_bbref"])
    mlbam = pick(b, ["key_mlbam","mlbam"])
    out = pd.DataFrame({
        "name_first": b[fn] if fn else np.nan,
        "name_last":  b[ln] if ln else np.nan,
        "retro_id":   b[retro] if retro else np.nan,
        "bbref_id":   b[bbref] if bbref else np.nan,
        "mlb_id":     pd.to_numeric(b[mlbam], errors="coerce") if mlbam else pd.Series([np.nan]*len(b))
    })
    out["full_name"] = (out["name_first"].fillna("")+" "+out["name_last"].fillna("")).str.strip()
    out["_n"] = out["full_name"].map(norm)
    return out

def unify(lah, ch):
    # 우선 Chadwick(mlbam 보유) → Lahman 보강
    base = pd.concat([ch, lah], ignore_index=True)
    # key: mlb_id 있으면 그것, 없으면 정규화 이름
    key = base["mlb_id"].astype("Int64").astype(str)
    key = key.where(~key.isin(["<NA>","nan","None"]), base["_n"])
    base["key"] = key
    # 우선비: mlb_id 채워진 행을 우선 유지
    base["has_mlb"] = base["mlb_id"].notna()
    base = base.sort_values(["has_mlb"], ascending=False)
    base = base.drop_duplicates(subset=["key"], keep="first")
    out = base[["mlb_id","retro_id","bbref_id","name_first","name_last","full_name"]].copy()
    # dtype 정리
    out["mlb_id"] = pd.to_numeric(out["mlb_id"], errors="coerce").astype("Int64")
    return out

def main(lah_path, ch_path, out_path):
    lah = load_lahman(lah_path)
    ch  = load_chadwick(ch_path)
    out = unify(lah, ch)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)
    print(f"[Day53 robust] -> {out_path} ({len(out)} rows)")

if __name__=="__main__":
    lah = sys.argv[1] if len(sys.argv)>1 else "data/lahman_people.csv"
    ch  = sys.argv[2] if len(sys.argv)>2 else "data/chadwick_register.csv"
    out = sys.argv[3] if len(sys.argv)>3 else "output/id_map.csv"
    main(lah, ch, out)
