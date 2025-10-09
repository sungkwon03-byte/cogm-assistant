import os, glob, re, sys
import pandas as pd
from unidecode import unidecode

os.makedirs("output", exist_ok=True)

def norm_name(s):
    s = "" if pd.isna(s) else str(s)
    s = unidecode(s).replace(".", " ")
    s = re.sub(r"[^0-9A-Za-z\s'\-]", " ", s)
    return re.sub(r"\s+", " ", s).strip().upper()

def _year(s):
    if pd.isna(s): return None
    m = re.search(r"(\d{4})", str(s))
    return int(m.group(1)) if m else None

def load_lahman():
    p = "data/id/Lahman/People.csv"
    hdr = pd.read_csv(p, nrows=0).columns
    use = [c for c in ["nameFirst","nameLast","name","bbrefID","retroID","mlbamID","mlbID","birthYear","bats","throws","debut"] if c in hdr]
    L = pd.read_csv(p, usecols=use, low_memory=False)
    fn = next((c for c in ["nameFirst","name_first","given_name","first_name"] if c in L.columns), None)
    ln = next((c for c in ["nameLast","name_last","surname","last_name"] if c in L.columns), None)
    if fn and ln: L["full_name"] = (L[fn].astype(str)+" "+L[ln].astype(str)).str.strip()
    elif "name" in L.columns: L["full_name"] = L["name"].astype(str)
    else: L["full_name"] = ""
    L = L.rename(columns={"bbrefID":"bbref_id","retroID":"retro_id","mlbamID":"mlb_id","mlbID":"mlb_id"})
    for c in ["mlb_id","retro_id","bbref_id","full_name","birthYear","bats","throws","debut"]:
        if c not in L.columns: L[c] = pd.NA
    L["name_norm"] = L["full_name"].map(norm_name)
    L["birth_year"] = L["birthYear"]
    L["debut_year"] = L["debut"].map(_year)
    return L[["name_norm","full_name","mlb_id","retro_id","bbref_id","birth_year","bats","throws","debut_year"]].drop_duplicates()

def load_chadwick():
    parts=[]
    for p in sorted(glob.glob("data/id/Chadwick/people-*.csv")):
        parts.append(pd.read_csv(p, low_memory=False))
    C = pd.concat(parts, ignore_index=True)
    if {"name_first","name_last"}.issubset(C.columns):
        C["full_name"] = (C["name_first"].astype(str)+" "+C["name_last"].astype(str)).str.strip()
    elif "name_full" in C.columns:
        C["full_name"] = C["name_full"].astype(str)
    else:
        C["full_name"] = C.get("name","").astype(str)
    C = C.rename(columns={"key_mlbam":"mlb_id","key_retro":"retro_id","key_bbref":"bbref_id"})
    for c in ["mlb_id","retro_id","bbref_id","full_name","birth_year","bats","throws","debut","final_game"]:
        if c not in C.columns: C[c] = pd.NA
    C["name_norm"] = C["full_name"].map(norm_name)
    C["debut_year"] = C["debut"].map(_year)
    return C[["name_norm","full_name","mlb_id","retro_id","bbref_id","birth_year","bats","throws","debut_year"]].drop_duplicates()

def build_base(L, C):
    M = pd.concat([L.assign(src="L"), C.assign(src="C")], ignore_index=True)
    score = (M["mlb_id"].notna().astype(int)*3 +
             M["retro_id"].notna().astype(int)*2 +
             M["bbref_id"].notna().astype(int))
    src_rank = M["src"].map({"C":1,"L":2}).fillna(9)
    base = (M.assign(_score=score,_src_rank=src_rank)
              .sort_values(["name_norm","_score","_src_rank"], ascending=[True,False,True])
              .groupby("name_norm", as_index=False)
              .first())
    out = base[["mlb_id","retro_id","bbref_id","full_name","name_norm","birth_year","bats","throws","debut_year"]].copy()
    return out

def cross_backfill(base, C):
    # 인덱스 3종
    by_mlb   = C[["mlb_id","retro_id","bbref_id"]].dropna(how="all").drop_duplicates().sort_values("mlb_id").groupby("mlb_id", as_index=False).first()
    by_retro = C[["retro_id","mlb_id","bbref_id"]].dropna(how="all").drop_duplicates().sort_values("retro_id").groupby("retro_id", as_index=False).first()
    by_bbr   = C[["bbref_id","mlb_id","retro_id"]].dropna(how="all").drop_duplicates().sort_values("bbref_id").groupby("bbref_id", as_index=False).first()

    out = base.copy()
    # mlb → retro/bbref
    out = out.merge(by_mlb.rename(columns={"retro_id":"retro_m","bbref_id":"bbr_m"}), on="mlb_id", how="left")
    out["retro_id"] = out["retro_id"].fillna(out["retro_m"])
    out["bbref_id"] = out["bbref_id"].fillna(out["bbr_m"])
    out.drop(columns=["retro_m","bbr_m"], inplace=True)

    # retro → mlb/bbref
    out = out.merge(by_retro.rename(columns={"mlb_id":"mlb_r","bbref_id":"bbr_r"}), on="retro_id", how="left")
    out["mlb_id"]   = out["mlb_id"].fillna(out["mlb_r"])
    out["bbref_id"] = out["bbref_id"].fillna(out["bbr_r"])
    out.drop(columns=["mlb_r","bbr_r"], inplace=True)

    # bbref → mlb/retro
    out = out.merge(by_bbr.rename(columns={"mlb_id":"mlb_b","retro_id":"ret_b"}), on="bbref_id", how="left")
    out["mlb_id"]   = out["mlb_id"].fillna(out["mlb_b"])
    out["retro_id"] = out["retro_id"].fillna(out["ret_b"])
    out.drop(columns=["mlb_b","ret_b"], inplace=True)
    return out

def name_birth_backfill(base, C):
    # 이름정규화 + 생년 동일/±1 허용 매칭
    cand = base.merge(C[["name_norm","birth_year","bats","throws","debut_year","mlb_id","retro_id","bbref_id"]]
                      .rename(columns={"mlb_id":"mlb_c","retro_id":"retro_c","bbref_id":"bbr_c"}),
                      on="name_norm", how="left")
    def birth_ok(by, cy):
        if pd.isna(by) or pd.isna(cy): return False
        try: return abs(int(by)-int(cy)) <= 1
        except: return False
    ok = cand["birth_year"].combine(cand["birth_year_y"], birth_ok)
    cand = cand[ok].copy()

    def score(r):
        s=0
        if (pd.notna(r.get("bats")) and pd.notna(r.get("bats_y")) and r["bats"]==r["bats_y"]): s+=2
        if (pd.notna(r.get("throws")) and pd.notna(r.get("throws_y")) and r["throws"]==r["throws_y"]): s+=2
        if (pd.notna(r.get("debut_year")) and pd.notna(r.get("debut_year_y")) and abs(int(r["debut_year"])-int(r["debut_year_y"]))<=1): s+=1
        return s
    cand["__score__"]=cand.apply(score,axis=1)

    key = ["name_norm","full_name","birth_year","bats","throws","debut_year","mlb_id","retro_id","bbref_id"]
    best = (cand.sort_values(key+["__score__"], ascending=[True]*len(key)+[False])
                .groupby(key, as_index=False).first())

    out = best.copy()
    out["retro_id"] = out["retro_id"].fillna(out["retro_c"])
    out["bbref_id"] = out["bbref_id"].fillna(out["bbr_c"])
    out["mlb_id"]   = out["mlb_id"].fillna(out["mlb_c"])
    return out[key]

def finalize(df):
    # 세 키 모두 결측은 제거, 문자열화(빈칸 유지), mlb_id의 불필요 .0 제거
    out = df.copy()
    out["mlb_id"] = out["mlb_id"].astype(str).str.replace(r"\.0$","",regex=True)
    for c in ["mlb_id","retro_id","bbref_id","full_name"]:
        out[c] = out[c].fillna("")
    keep = out[["mlb_id","retro_id","bbref_id"]].ne("").any(axis=1)
    out = out[keep].drop_duplicates(subset=["mlb_id","retro_id","bbref_id","full_name"])
    out = out[["mlb_id","retro_id","bbref_id","full_name"]]
    out.to_csv("output/id_map.csv", index=False)

    # QC
    tot = len(out)
    null_mlb  = (out["mlb_id"]=="").mean()*100
    null_ret  = (out["retro_id"]=="").mean()*100
    null_bbr  = (out["bbref_id"]=="").mean()*100
    with open("output/id_map_qc.txt","w") as f:
        f.write(f"rows={tot}\nnull% mlb_id={null_mlb:.2f}, retro_id={null_ret:.2f}, bbref_id={null_bbr:.2f}\n")

def main():
    L = load_lahman()
    C = load_chadwick()

    base = build_base(L, C)
    base = cross_backfill(base, C)
    base = name_birth_backfill(base, C)
    finalize(base)
    print("OK Day53 robust → output/id_map.csv , output/id_map_qc.txt")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        sys.stderr.write(f"[FAIL] {type(e).__name__}: {e}\n")
        sys.exit(1)
