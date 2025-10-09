#!/usr/bin/env python3
import os, re, glob, json, argparse
import pandas as pd
import numpy as np

MLB_ORGS={"ARI","ATL","BAL","BOS","CHC","CHW","CIN","CLE","COL","DET","HOU","KCR","LAA","LAD","MIA","MIL","MIN","NYM","NYY","OAK","PHI","PIT","SDP","SEA","SFG","STL","TBR","TEX","TOR","WSN"}
KBO_ORGS={"KIA","LOT","DOO","LG","SSG","KIW","HAN","SAM","NC","KT"}

def _series(df, cands, default=None):
    for c in cands:
        if c in df.columns: return df[c]
    return pd.Series([default]*len(df), index=df.index)

def _norm_name(s: pd.Series)->pd.Series:
    return (s.astype(str).str.normalize("NFKD").str.encode("ascii","ignore").str.decode("ascii")
            .str.strip().str.lower().str.replace(r"\s+","_",regex=True))

def build_uid(df: pd.DataFrame, season: pd.Series, league: str)->pd.Series:
    base=_norm_name(_series(df,["Name","Player","player_name"],"unknown"))
    team=_series(df,["Team","Tm","team","org","Org"],"NA").astype(str).str.upper().replace({"": "NA"})
    pos=_series(df,["Pos","position"],"XX").astype(str).str.upper().replace({"": "XX"})
    lg=(league or "UNK").upper()
    return base + "_" + season.astype(str) + "_" + team + "_" + pos + "_" + lg

def infer_season_from(df: pd.DataFrame, path:str, fallback:int|None)->pd.Series:
    # 1) Season/Year 컬럼
    for c in ["Season","season","Year","year","season_std"]:
        if c in df.columns:
            s=pd.to_numeric(df[c], errors="coerce").astype("Int64")
            if s.notna().any(): return s
    # 2) 파일명에서 4자리 시즌
    m=re.findall(r"(19|20)\d{2}", os.path.basename(path))
    if m:
        y=int(m[0][:4])
        return pd.Series([y]*len(df), index=df.index, dtype="Int64")
    # 3) 폴백
    if fallback is None:
        return pd.Series([pd.NA]*len(df), index=df.index, dtype="Int64")
    return pd.Series([fallback]*len(df), index=df.index, dtype="Int64")

def normalize_team(s: pd.Series)->pd.Series:
    alias={"WSH":"WSN","WAS":"WSN","KC":"KCR","TB":"TBR","SD":"SDP","SF":"SFG"}
    x=s.astype(str).str.upper().str.replace(r"\s+","",regex=True)
    return x.replace(alias)

def detect_league_hint(path:str)->str:
    p=path.lower()
    if "minor" in p or "milb" in p: return "MiLB"
    if "kbo" in p: return "KBO"
    if "mlb" in p: return "MLB"
    return "UNK"

def collect_paths():
    # 우선순위: raw/ → data/ → mart/
    roots=["raw","data/fangraphs","data","mart","external","inputs","downloads","."]
    milb=[]  # [DAY70] MiLB disabled
    others=sorted(sum([glob.glob(os.path.join(r,"**","*.csv"), recursive=True) for r in roots],[]))
    return milb, others

def ingest_milb(season_hint:int|None):
    # [DAY70] MiLB ingestion disabled
    return {}
    milb_paths, _ = collect_paths()
    milb_paths=[p for p in milb_paths if os.path.getsize(p)>0]
    if not milb_paths:
        return {}
    out_by_season={}
    for p in milb_paths:
        try:
            df=pd.read_csv(p)
        except Exception:
            continue
        if df.empty: continue
        season=infer_season_from(df,p,season_hint)
        team=_series(df,["Team","Tm","team","org","Org"],"UNK")
        df=df.assign(season=season, Team=normalize_team(team))
        # 리그 라벨: MiLB 고정
        df["league"]="MiLB"
        # UID
        df["player_uid"]=build_uid(df, df["season"].fillna(season_hint).astype("Int64"), "MiLB")
        # 시즌별 누적
        for y,chunk in df.groupby(df["season"].fillna(season_hint).astype("Int64")):
            y=int(y) if pd.notna(y) else int(season_hint) if season_hint else 0
            out_by_season.setdefault(y,[]).append(chunk)
    # 저장
    os.makedirs("mart", exist_ok=True)
    written={}
    frames_all=[]
    for y,parts in out_by_season.items():
        merged=pd.concat(parts, ignore_index=True, sort=False)
        # 팀/선수 최소 정제
        merged["Team"]=normalize_team(merged["Team"])
        merged.to_csv(f"mart/milb_{y}_players.csv", index=False)
        written[y]=len(merged)
        frames_all.append(merged)
        print(f"[OK] MiLB mart/milb_{y}_players.csv ({len(merged)} rows)")
    if frames_all:
        all_df=pd.concat(frames_all, ignore_index=True, sort=False)
        all_df.to_csv("mart/milb_all_players.csv", index=False)
        print(f"[OK] MiLB mart/milb_all_players.csv ({len(all_df)} rows)")
    return written

def ingest_mlb_kbo():
    _, others = collect_paths()
    # MLB/KBO는 “전체 역사”를 긁어 모은다 (시즌 컬럼 기준 분할 저장)
    # 후보 파일에서 리그 판별
    mlb_parts=[]
    kbo_parts=[]
    for p in others:
        try:
            if os.path.basename(p).startswith("milb_"): 
                continue  # MiLB 파일은 제외
            df=pd.read_csv(p)
        except Exception:
            continue
        if df.empty: continue
        team=_series(df,["Team","Tm","team","org","Org"],None)
        if team is None: continue
        season=infer_season_from(df,p,None)
        # 팀 코드 정규화
        df=df.assign(Team=normalize_team(team), season=season)
        hint=detect_league_hint(p)
        # 리그 결정 로직: 팀 코드로 1차 필터
        if df["Team"].isin(MLB_ORGS).any():
            df["league"]="MLB"; mlb_parts.append(df[df["Team"].isin(MLB_ORGS)])
        elif df["Team"].isin(KBO_ORGS).any():
            df["league"]="KBO"; kbo_parts.append(df[df["Team"].isin(KBO_ORGS)])
        else:
            # 힌트로 보조
            if hint=="MLB":
                df["league"]="MLB"; mlb_parts.append(df)
            elif hint=="KBO":
                df["league"]="KBO"; kbo_parts.append(df)
            else:
                continue

    os.makedirs("mart", exist_ok=True)
    def write_by_season(parts, lg):
        if not parts: return {}
        merged=pd.concat(parts, ignore_index=True, sort=False)
        # 시즌별 분할 저장
        written={}
        for y,chunk in merged.groupby(pd.to_numeric(merged["season"], errors="coerce").astype("Int64")):
            if pd.isna(y): continue
            y=int(y)
            # UID
            chunk=chunk.copy()
            chunk["player_uid"]=build_uid(chunk, chunk["season"].astype("Int64"), lg)
            out=f"mart/{lg.lower()}_{y}_players.csv"
            chunk.to_csv(out, index=False)
            written[y]=len(chunk)
            print(f"[OK] {lg} {out} ({len(chunk)} rows)")
        # 통합본
        if written:
            merged["player_uid"]=build_uid(merged, pd.to_numeric(merged["season"], errors="coerce").fillna(0).astype("Int64"), lg)
            merged.to_csv(f"mart/{lg.lower()}_all_players.csv", index=False)
            print(f"[OK] {lg} mart/{lg.lower()}_all_players.csv ({len(merged)} rows)")
        return written

    mlb_written=write_by_season(mlb_parts,"MLB")
    kbo_written=write_by_season(kbo_parts,"KBO")
    return mlb_written, kbo_written

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--season", type=int, default=None, help="입력 시즌(UID 폴백/필드 병합 시 사용). MiLB는 멀티시즌 자동 수집.")
    args=ap.parse_args()

    print("[INFO] MiLB 멀티시즌 인제스트 시작")
    milb_written=ingest_milb(args.season)

    print("[INFO] MLB/KBO 전체 역사 인제스트 시작")
    mlb_written, kbo_written = ingest_mlb_kbo()

    # 요약 저장
    summary={
        "MiLB": milb_written,
        "MLB":  mlb_written,
        "KBO":  kbo_written
    }
    os.makedirs("logs", exist_ok=True)
    with open("logs/ingest_summary.json","w",encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print("[DONE] ingest summary -> logs/ingest_summary.json")
