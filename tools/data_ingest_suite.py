# === Co-GM Week8 Data Suite (Day50–56) ===
# 목적: 리얼데이터 ETL 표준화 (입력스키마 → 정규화 → 검증/캐시)
# 서브커맨드:
#  day50_mlb     : MLB Stats API box/line 정규화
#  day51_statcast: Statcast EV/LA/xwOBA 집계
#  day52_splits  : FG/BBRef splits 표준스키마 병합
#  day53_idmap   : Lahman/Chadwick/MLB-ID 매핑 테이블 생성
#  day54_bridge  : KBO/NPB↔MLB 보정계수 산출(또는 설정값 고정)
#  day55_xleague : cross-league player_card 생성
#  day56_validate: 검증 리포트 + 캐시(parquet) 생성

import argparse, json, pandas as pd, numpy as np, os, sys
from pathlib import Path

def _read_csv(path): return pd.read_csv(path)
def _read_json(path): return json.load(open(path, "r"))
def _save_csv(df, path): Path(path).parent.mkdir(parents=True, exist_ok=True); df.to_csv(path, index=False); print(f"-> {path} ({len(df)} rows)")
def _save_parquet(df, path): Path(path).parent.mkdir(parents=True, exist_ok=True); df.to_parquet(path, index=False); print(f"-> {path} ({len(df)} rows)")

# ---------- Day50: MLB Stats API box/line ----------
# 입력(JSON): boxscore.json, linescore.json (원본 키 유지)
# 출력(CSV): games.csv, team_box.csv, player_box.csv
def cmd_day50_mlb(args):
    box = _read_json(args.boxscore)
    line = _read_json(args.linescore)

    game_pk = line.get("gamePk") or box.get("gameId")
    date = line.get("gameDate") or box.get("gameDate")
    venue = (line.get("venue") or {}).get("name", "")
    home = ((line.get("teams") or {}).get("home") or {}).get("team", {}).get("abbreviation", "")
    away = ((line.get("teams") or {}).get("away") or {}).get("team", {}).get("abbreviation", "")
    home_runs = ((line.get("teams") or {}).get("home") or {}).get("score", np.nan)
    away_runs = ((line.get("teams") or {}).get("away") or {}).get("score", np.nan)

    games = pd.DataFrame([{"game_pk": game_pk, "date": date, "venue": venue, "home": home, "away": away, "home_runs": home_runs, "away_runs": away_runs}])

    # team box (간략화)
    trows=[]
    for side in ["home","away"]:
        t=((box.get("teams") or {}).get(side) or {})
        trows.append({
            "game_pk": game_pk,
            "team": t.get("team",{}).get("abbreviation",""),
            "hits": t.get("teamStats",{}).get("batting",{}).get("hits",np.nan),
            "runs": t.get("teamStats",{}).get("batting",{}).get("runs",np.nan),
            "hr":   t.get("teamStats",{}).get("batting",{}).get("homeRuns",np.nan),
            "so_p": t.get("teamStats",{}).get("pitching",{}).get("strikeOuts",np.nan),
            "bb_p": t.get("teamStats",{}).get("pitching",{}).get("baseOnBalls",np.nan),
        })
    team_box=pd.DataFrame(trows)

    # player box(핵심만)
    prows=[]
    for side in ["home","away"]:
        players=((box.get("teams") or {}).get(side) or {}).get("players",{}) or {}
        for pid, pdata in players.items():
            info=pdata.get("person",{})
            stats=pdata.get("stats",{})
            bat=stats.get("batting",{})
            pit=stats.get("pitching",{})
            prows.append({
                "game_pk": game_pk,
                "team": ((box.get("teams") or {}).get(side) or {}).get("team",{}).get("abbreviation",""),
                "mlb_id": info.get("id"),
                "name": info.get("fullName"),
                "PA": bat.get("atBats",0)+bat.get("baseOnBalls",0)+bat.get("hitByPitch",0)+bat.get("sacBunts",0)+bat.get("sacFlies",0),
                "H": bat.get("hits",0),
                "HR": bat.get("homeRuns",0),
                "BB": bat.get("baseOnBalls",0),
                "SO": bat.get("strikeOuts",0),
                "IP_outs": pit.get("outs",0),
                "K_p": pit.get("strikeOuts",0),
                "BB_p": pit.get("baseOnBalls",0),
                "ER": pit.get("earnedRuns",0),
            })
    player_box=pd.DataFrame(prows)

    _save_csv(games, args.out_games)
    _save_csv(team_box, args.out_team_box)
    _save_csv(player_box, args.out_player_box)

# ---------- Day51: Statcast 집계 ----------
# 입력(CSV): statcast.csv (cols: game_pk, mlb_id, EV, LA, xwOBA, pitch_type, is_bip(0/1))
# 출력(CSV): statcast_hit_agg.csv, statcast_pit_agg.csv
def cmd_day51_statcast(args):
    sc=_read_csv(args.statcast)
    hit=sc.groupby(["mlb_id"]).agg(EV_mean=("EV","mean"), LA_mean=("LA","mean"), xwOBA_mean=("xwOBA","mean"), BBE=("is_bip","sum")).reset_index()
    pit=sc.groupby(["mlb_id","pitch_type"]).agg(Velo=("EV","mean"), Usage=("pitch_type","count")).reset_index()
    _save_csv(hit, args.out_hit)
    _save_csv(pit, args.out_pit)

# ---------- Day52: FG/BBRef splits 병합 ----------
# 입력: fg_splits.csv (player, vsL_wRCplus, vsR_wRCplus), bbref_splits.csv (player, vsL_OPS, vsR_OPS)
# 출력: splits_merged.csv
def cmd_day52_splits(args):
    fg=_read_csv(args.fg); bb=_read_csv(args.bbref)
    out=fg.merge(bb, on="player", how="outer")
    _save_csv(out, args.out)

# ---------- Day53: ID 매핑 ----------
# 입력: lahman_people.csv (lahmanID, mlb_id, retro_id, bbref_id, name_first, name_last)
#      chadwick_register.csv (key_uuid, key_mlbam, key_retro, key_bbref, name_first, name_last)
# 출력: id_map.csv
def cmd_day53_idmap(args):
    a=_read_csv(args.lahman); b=_read_csv(args.chadwick)
    a["full_name"]=(a["name_first"].fillna("")+" "+a["name_last"].fillna("")).str.strip()
    b["full_name"]=(b["name_first"].fillna("")+" "+b["name_last"].fillna("")).str.strip()
    m=a.merge(b, left_on=["mlb_id","retro_id","bbref_id"], right_on=["key_mlbam","key_retro","key_bbref"], how="outer", suffixes=("_lah","_ch"))
    # fallback by name
    m2=a.merge(b, on="full_name", how="outer", suffixes=("_lah","_ch"))
    out=pd.concat([m, m2]).drop_duplicates(subset=["mlb_id","key_mlbam","retro_id","key_retro","bbref_id","key_bbref","full_name"], keep="first")
    out=_dedup(out)
    _save_csv(out, args.out)

def _dedup(df):
    return df.drop_duplicates()

# ---------- Day54: 브리지 계수 ----------
# 입력: bridge_input.csv (league_from, metric, coef)
# 옵션: auto모드 — overlap.csv (player, league_from, metric, value_from, value_mlb)
# 출력: bridge_coef.csv
def cmd_day54_bridge(args):
    if args.mode=="manual":
        out=_read_csv(args.bridge_input)
    else:
        ov=_read_csv(args.overlap)
        # 단순 회귀: coef = mean(value_mlb/value_from)
        ov=ov[ov["value_from"]>0]
        g=ov.groupby(["league_from","metric"]).apply(lambda d: pd.Series({"coef": (d["value_mlb"]/d["value_from"]).mean()})).reset_index()
        out=g
    _save_csv(out, args.out)

# ---------- Day55: Cross-league player_card ----------
# 입력: xleague_base.csv (player,league,metric,value) + bridge_coef.csv
# 출력: player_card_xleague.csv (player, metric, value_mlb_eq)
def cmd_day55_xleague(args):
    base=_read_csv(args.base); coef=_read_csv(args.coef)
    out=base.merge(coef, left_on=["league","metric"], right_on=["league_from","metric"], how="left")
    out["value_mlb_eq"]=np.where(out["league"]=="MLB", out["value"], out["value"]*out["coef"].fillna(1.0))
    out=out[["player","metric","value_mlb_eq"]].groupby(["player","metric"], as_index=False).mean()
    _save_csv(out, args.out)

# ---------- Day56: 검증 + 캐시 ----------
# 입력: 핵심 출력물들 경로
# 출력: validate_report.csv, cache/*.parquet
def cmd_day56_validate(args):
    report=[]
    def stat(path, cols=None):
        try:
            df=_read_csv(path)
            ok=True if (cols is None or all(c in df.columns for c in cols)) else False
            report.append({"file": path, "rows": len(df), "cols_ok": ok})
            return df
        except Exception as e:
            report.append({"file": path, "rows": -1, "cols_ok": False, "error": str(e)})
            return None

    games=stat(args.games, ["game_pk","home","away"])
    team=stat(args.team_box, ["game_pk","team","runs"])
    player=stat(args.player_box, ["game_pk","mlb_id","PA"])
    sc_hit=stat(args.sc_hit, ["mlb_id","EV_mean"])
    sc_pit=stat(args.sc_pit, ["mlb_id","pitch_type","Usage"])
    splits=stat(args.splits, ["player"])
    idmap=stat(args.idmap, ["full_name"])
    bridge=stat(args.bridge, ["league_from","metric","coef"])
    xcard=stat(args.xcard, ["player","metric","value_mlb_eq"])

    rep=pd.DataFrame(report)
    _save_csv(rep, args.out_report)

    # 캐시: 핵심 테이블만 parquet
    if games is not None: _save_parquet(games, args.cache_dir+"/games.parquet")
    if player is not None: _save_parquet(player, args.cache_dir+"/player_box.parquet")
    if sc_hit is not None: _save_parquet(sc_hit, args.cache_dir+"/statcast_hit.parquet")
    if xcard is not None: _save_parquet(xcard, args.cache_dir+"/xleague_card.parquet")

# ---------- CLI ----------
def main():
    ap=argparse.ArgumentParser(prog="data_ingest_suite")
    sp=ap.add_subparsers(dest="cmd", required=True)

    p50=sp.add_parser("day50_mlb")
    p50.add_argument("--boxscore", required=True)
    p50.add_argument("--linescore", required=True)
    p50.add_argument("--out_games", required=True)
    p50.add_argument("--out_team_box", required=True)
    p50.add_argument("--out_player_box", required=True)
    p50.set_defaults(func=cmd_day50_mlb)

    p51=sp.add_parser("day51_statcast")
    p51.add_argument("--statcast", required=True)
    p51.add_argument("--out_hit", required=True)
    p51.add_argument("--out_pit", required=True)
    p51.set_defaults(func=cmd_day51_statcast)

    p52=sp.add_parser("day52_splits")
    p52.add_argument("--fg", required=True)
    p52.add_argument("--bbref", required=True)
    p52.add_argument("--out", required=True)
    p52.set_defaults(func=cmd_day52_splits)

    p53=sp.add_parser("day53_idmap")
    p53.add_argument("--lahman", required=True)
    p53.add_argument("--chadwick", required=True)
    p53.add_argument("--out", required=True)
    p53.set_defaults(func=cmd_day53_idmap)

    p54=sp.add_parser("day54_bridge")
    p54.add_argument("--mode", choices=["manual","auto"], required=True)
    p54.add_argument("--bridge_input")
    p54.add_argument("--overlap")
    p54.add_argument("--out", required=True)
    p54.set_defaults(func=cmd_day54_bridge)

    p55=sp.add_parser("day55_xleague")
    p55.add_argument("--base", required=True)
    p55.add_argument("--coef", required=True)
    p55.add_argument("--out", required=True)
    p55.set_defaults(func=cmd_day55_xleague)

    p56=sp.add_parser("day56_validate")
    p56.add_argument("--games", required=True)
    p56.add_argument("--team_box", required=True)
    p56.add_argument("--player_box", required=True)
    p56.add_argument("--sc_hit", required=True)
    p56.add_argument("--sc_pit", required=True)
    p56.add_argument("--splits", required=True)
    p56.add_argument("--idmap", required=True)
    p56.add_argument("--bridge", required=True)
    p56.add_argument("--xcard", required=True)
    p56.add_argument("--out_report", required=True)
    p56.add_argument("--cache_dir", required=True)
    p56.set_defaults(func=cmd_day56_validate)

    args=ap.parse_args(); args.func(args)

if __name__=="__main__":
    main()
# === End Week8 Data Suite ===
# === Co-GM append start (Week8 fix: parquet+empty CSV) ===
import pandas as _pd
from pathlib import Path as _Path

def _save_parquet(df, path):
    # parquet 엔진 없으면 CSV로 폴백
    _Path(path).parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(path, index=False)
        print(f"-> {path} ({len(df)} rows)")
    except Exception as e:
        alt = path.rsplit(".",1)[0] + ".csv"
        df.to_csv(alt, index=False)
        print(f"-> {alt} ({len(df)} rows) [fallback parquet->csv: {type(e).__name__}]")

def _save_csv(df, path):
    # player_box가 비어도 스키마 헤더 보장
    _Path(path).parent.mkdir(parents=True, exist_ok=True)
    if ("player_box" in path) and (df is not None) and (df.shape[1] == 0):
        df = _pd.DataFrame(columns=[
            "game_pk","team","mlb_id","name","PA","H","HR","BB","SO","IP_outs","K_p","BB_p","ER"
        ])
    df.to_csv(path, index=False)
    print(f"-> {path} ({0 if df is None else len(df)} rows)")
# === Co-GM append end (Week8 fix: parquet+empty CSV) ===
# === Co-GM append start (Week8 hard override) ===
from pathlib import Path as __Path
import pandas as __pd, os as __os

def __save_parquet_fallback(df, path):
    __Path(path).parent.mkdir(parents=True, exist_ok=True)
    try:
        import pyarrow  # noqa: F401
        df.to_parquet(path, index=False)
        print(f"-> {path} ({len(df)} rows)")
    except Exception as e:
        alt = path.rsplit(".",1)[0] + ".csv"
        df.to_csv(alt, index=False)
        print(f"-> {alt} ({len(df)} rows) [fallback parquet->csv: {type(e).__name__}]")

def __save_csv_fixed(df, path):
    __Path(path).parent.mkdir(parents=True, exist_ok=True)
    # player_box가 비어 있으면 스키마 헤더 강제
    if ("player_box" in path) and (df is not None) and (df.shape[1] == 0):
        df = __pd.DataFrame(columns=[
            "game_pk","team","mlb_id","name","PA","H","HR","BB","SO","IP_outs","K_p","BB_p","ER"
        ])
    df.to_csv(path, index=False)
    print(f"-> {path} ({0 if df is None else len(df)} rows)")

# 기존 심볼을 강제로 덮어씀 (이 아래부터는 무조건 새 구현 사용)
_save_parquet = __save_parquet_fallback
_save_csv = __save_csv_fixed

# 기존에 생성된 빈 player_box.csv가 있으면 헤더만 보정
def __ensure_player_box_header(path="output/player_box.csv"):
    if __os.path.exists(path):
        try:
            df = __pd.read_csv(path)
            ok = ("mlb_id" in df.columns)
        except Exception:
            ok = False
        if not ok:
            __pd.DataFrame(columns=[
                "game_pk","team","mlb_id","name","PA","H","HR","BB","SO","IP_outs","K_p","BB_p","ER"
            ]).to_csv(path, index=False)
            print(f"[fix] init empty player_box header -> {path}")

__ensure_player_box_header()
# === Co-GM append end (Week8 hard override) ===
# === Co-GM append start (Day50 robust normalize) ===
import re as _re

def _fallback_pk_from_path(path:str):
    m=_re.search(r'(\d{6,})', str(path))
    return int(m.group(1)) if m else None

def _safe_get(d, path_list, default=""):
    cur=d
    try:
        for k in path_list:
            cur = (cur.get(k) if isinstance(cur, dict) else None) or {}
        return cur if cur not in ({}, None, "") else default
    except Exception:
        return default

def _extract_team_abbr_from_box(box, side):
    return _safe_get(box, ["teams", side, "team", "abbreviation"], "")

def _build_day50_tables(box, line, linescore_path):
    pk = line.get("gamePk") or box.get("gameId") or _fallback_pk_from_path(linescore_path) or -1
    date = line.get("gameDate") or box.get("gameDate") or ""
    venue = _safe_get(line, ["venue","name"], "")
    home_abbr = _safe_get(line, ["teams","home","team","abbreviation"], "") or _extract_team_abbr_from_box(box,"home")
    away_abbr = _safe_get(line, ["teams","away","team","abbreviation"], "") or _extract_team_abbr_from_box(box,"away")
    home_runs = _safe_get(line, ["teams","home","runs"], None)
    away_runs = _safe_get(line, ["teams","away","runs"], None)

    games = pd.DataFrame([{
        "game_pk": pk, "date": date, "venue": venue,
        "home": home_abbr, "away": away_abbr,
        "home_runs": home_runs, "away_runs": away_runs
    }])

    def _team_row(side):
        t = (box.get("teams") or {}).get(side, {}) or {}
        batting = (t.get("teamStats") or {}).get("batting", {}) or {}
        pitching= (t.get("teamStats") or {}).get("pitching", {}) or {}
        return {
            "game_pk": pk,
            "team": _extract_team_abbr_from_box(box, side) or (home_abbr if side=="home" else away_abbr),
            "hits": batting.get("hits"),
            "runs": batting.get("runs"),
            "hr": batting.get("homeRuns"),
            "so_p": pitching.get("strikeOuts"),
            "bb_p": pitching.get("baseOnBalls"),
        }

    team_box = pd.DataFrame([_team_row("home"), _team_row("away")])

    # 선수 레벨(이미 기존 패치 있음) — 재사용
    prows = _extract_player_rows(box, pk, "home") + _extract_player_rows(box, pk, "away")
    player_box = pd.DataFrame(prows) if prows else pd.DataFrame(columns=[
        "game_pk","team","mlb_id","name","PA","H","HR","BB","SO","IP_outs","K_p","BB_p","ER"
    ])
    return games, team_box, player_box

# 최종 오버라이드: cmd_day50_mlb 재정의
try:
    _DAY50_STRICT
except NameError:
    _DAY50_STRICT=True
    def cmd_day50_mlb(args):
        box = _read_json(args.boxscore)
        line = _read_json(args.linescore)
        g,t,p = _build_day50_tables(box, line, args.linescore)
        _save_csv(g, args.out_games)
        _save_csv(t, args.out_team_box)
        _save_csv(p, args.out_player_box)
# === Co-GM append end (Day50 robust normalize) ===
# === Co-GM append start (Day50 fix v2: boxscore-first) ===
import re as ___re
def __pk_from_path(path:str):
    m=___re.search(r'(\d{6,})', str(path)); return int(m.group(1)) if m else None

def __abbr(box, side):
    try:
        return ((box.get("teams") or {}).get(side) or {}).get("team",{}).get("abbreviation","")
    except Exception:
        return ""

def __runs_from_box(box, side):
    try:
        return ((box.get("teams") or {}).get(side) or {}).get("teamStats",{}).get("batting",{}).get("runs", None)
    except Exception:
        return None

def __build_tables_box_first(box, line, linescore_path):
    pk = (line.get("gamePk") if isinstance(line, dict) else None) or box.get("gameId") or __pk_from_path(linescore_path) or -1
    date = (line.get("gameDate") if isinstance(line, dict) else "") or ""
    venue = (line.get("venue",{}) or {}).get("name","")

    home_abbr = __abbr(box,"home")
    away_abbr = __abbr(box,"away")
    home_runs = __runs_from_box(box,"home")
    away_runs = __runs_from_box(box,"away")

    import pandas as pd
    games = pd.DataFrame([{
        "game_pk": pk, "date": date, "venue": venue,
        "home": home_abbr, "away": away_abbr,
        "home_runs": home_runs, "away_runs": away_runs
    }])

    # team box (boxscore만)
    trows=[]
    for side in ["home","away"]:
        trows.append({
            "game_pk": pk,
            "team": __abbr(box, side),
            "hits": ((box.get("teams",{}).get(side,{}).get("teamStats",{}).get("batting",{})).get("hits", None)),
            "runs": ((box.get("teams",{}).get(side,{}).get("teamStats",{}).get("batting",{})).get("runs", None)),
            "hr":   ((box.get("teams",{}).get(side,{}).get("teamStats",{}).get("batting",{})).get("homeRuns", None)),
            "so_p": ((box.get("teams",{}).get(side,{}).get("teamStats",{}).get("pitching",{})).get("strikeOuts", None)),
            "bb_p": ((box.get("teams",{}).get(side,{}).get("teamStats",{}).get("pitching",{})).get("baseOnBalls", None)),
        })
    team_box = pd.DataFrame(trows)

    # player box: 기존 추출기 재사용
    prows = _extract_player_rows(box, pk, "home") + _extract_player_rows(box, pk, "away")
    player_box = pd.DataFrame(prows) if prows else pd.DataFrame(columns=[
        "game_pk","team","mlb_id","name","PA","H","HR","BB","SO","IP_outs","K_p","BB_p","ER"
    ])

    # runs 보강: games.csv의 runs가 비면 team_box에서 채움
    try:
        if pd.isna(games.loc[0,"home_runs"]) or games.loc[0,"home_runs"]=="":
            games.loc[0,"home_runs"] = int(team_box[team_box["team"]==home_abbr]["runs"].iloc[0])
        if pd.isna(games.loc[0,"away_runs"]) or games.loc[0,"away_runs"]=="":
            games.loc[0,"away_runs"] = int(team_box[team_box["team"]==away_abbr]["runs"].iloc[0])
    except Exception:
        pass

    return games, team_box, player_box

# 최종 오버라이드
def cmd_day50_mlb(args):
    box = _read_json(args.boxscore)
    line = _read_json(args.linescore)
    g,t,p = __build_tables_box_first(box, line, args.linescore)
    _save_csv(g, args.out_games)
    _save_csv(t, args.out_team_box)
    _save_csv(p, args.out_player_box)
# === Co-GM append end (Day50 fix v2: boxscore-first) ===
# === Co-GM append start (Day52_splits_v2) ===
import pandas as _pd, re as _re

def _read_csv_safe(path):
    try: return _pd.read_csv(path)
    except Exception: return _pd.DataFrame()

def _norm_cols(df):
    # 소문자화
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    lower = {c.lower(): c for c in df.columns}

    def find(*cands):
        for c in cands:
            if c in lower: return lower[c]
        for c in cands:
            # 느슨한 매칭
            hits=[orig for l,orig in lower.items() if _re.fullmatch(c.replace("%","%?"), l)]
            if hits: return hits[0]
        return None

    # ID/이름
    id_col  = find("mlb_id","playerid","player_id","batter","pitcher","id")
    namecol = find("name","player","playername","player_name")

    # 타자 스플릿
    vsL_wRC = find("vsl_wrc+","vsl_wrcplus","wrc+ vsvl","wrc+ vsl","wrc+ (vsl)","vsl_wrc\\+")
    vsR_wRC = find("vsr_wrc+","vsr_wrcplus","wrc+ vsvr","wrc+ vsr","wrc+ (vsr)","vsr_wrc\\+")
    vsL_OPS = find("vsl_ops","ops vsl","vsl_ops+","ops (vsl)")
    vsR_OPS = find("vsr_ops","ops vsr","vsr_ops+","ops (vsr)")

    # 투수 지표
    fip    = find("fip")
    kperc  = find("k%","k% (pit)","k_pct")
    bbperc = find("bb%","bb% (pit)","bb_pct")

    out = _pd.DataFrame()
    if not df.empty:
        out["mlb_id"] = df[id_col] if id_col else _pd.NA
        out["name"]   = df[namecol] if namecol else _pd.NA
        out["vsL_wRCplus"] = _pd.to_numeric(df[vsL_wRC], errors="coerce") if vsL_wRC else _pd.NA
        out["vsR_wRCplus"] = _pd.to_numeric(df[vsR_wRC], errors="coerce") if vsR_wRC else _pd.NA
        out["vsL_OPS"]     = _pd.to_numeric(df[vsL_OPS], errors="coerce") if vsL_OPS else _pd.NA
        out["vsR_OPS"]     = _pd.to_numeric(df[vsR_OPS], errors="coerce") if vsR_OPS else _pd.NA
        out["FIP"]         = _pd.to_numeric(df[fip], errors="coerce") if fip else _pd.NA
        out["K_pct"]       = _pd.to_numeric(df[kperc], errors="coerce") if kperc else _pd.NA
        out["BB_pct"]      = _pd.to_numeric(df[bbperc], errors="coerce") if bbperc else _pd.NA

        # 키 보정: ID가 모두 결측이면 name을 키로 사용
        if out["mlb_id"].isna().all():
            out["mlb_id"] = _pd.NA

    return out

def cmd_day52_splits_v2(args):
    fg = _read_csv_safe(getattr(args,"fg",None))
    br = _read_csv_safe(getattr(args,"bbref",None))
    # 정규화
    fg_n = _norm_cols(fg)
    br_n = _norm_cols(br)

    # 키 생성: (mlb_id 있으면 그것, 없으면 name)
    def make_key(df):
        key = df["mlb_id"].astype("Int64").astype(str)
        key = _pd.Series(_pd.NA if k in ("<NA>","nan","None") else k for k in key)
        key = key.where(~key.isna(), df["name"])
        return key

    if fg_n.empty and br_n.empty:
        merged = _pd.DataFrame(columns=["mlb_id","name","vsL_wRCplus","vsR_wRCplus","vsL_OPS","vsR_OPS","FIP","K_pct","BB_pct","source"])
    else:
        if fg_n.empty: merged = br_n.copy(); merged["source"]="bbref"
        elif br_n.empty: merged = fg_n.copy(); merged["source"]="fangraphs"
        else:
            fg_n["_key"]=make_key(fg_n); br_n["_key"]=make_key(br_n)
            merged = _pd.merge(fg_n.drop_duplicates("_key"), br_n.drop_duplicates("_key"),
                               on="_key", how="outer", suffixes=("_fg","_br"))
            # 선택 규칙: wRC+는 FG 우선, OPS는 BBRef 우선, FIP는 FG 있으면 FG
            merged["mlb_id"] = merged["mlb_id_fg"].combine_first(merged["mlb_id_br"])
            merged["name"]   = merged["name_fg"].combine_first(merged["name_br"])
            merged["vsL_wRCplus"] = merged["vsL_wRCplus_fg"].combine_first(merged["vsL_wRCplus_br"])
            merged["vsR_wRCplus"] = merged["vsR_wRCplus_fg"].combine_first(merged["vsR_wRCplus_br"])
            merged["vsL_OPS"]     = merged["vsL_OPS_br"].combine_first(merged["vsL_OPS_fg"])
            merged["vsR_OPS"]     = merged["vsR_OPS_br"].combine_first(merged["vsR_OPS_fg"])
            merged["FIP"]         = merged["FIP_fg"].combine_first(merged["FIP_br"])
            merged["K_pct"]       = merged["K_pct_fg"].combine_first(merged["K_pct_br"])
            merged["BB_pct"]      = merged["BB_pct_fg"].combine_first(merged["BB_pct_br"])
            merged["source"]      = "fg_bbref_merge"
            merged = merged[["mlb_id","name","vsL_wRCplus","vsR_wRCplus","vsL_OPS","vsR_OPS","FIP","K_pct","BB_pct","source"]]

    _save_csv(merged, args.out)
# 등록
try:
    _DAY52_V2
except NameError:
    _DAY52_V2=True
    _ap = globals().get("_SP","_SP")
    # argparse의 서브커맨드에 등록
    def _register_day52_v2(sp):
        p = sp.add_parser("day52_v2", help="Fangraphs/BBRef splits robust merge")
        p.add_argument("--fg", type=str, default="data/fg_splits.csv")
        p.add_argument("--bbref", type=str, default="data/bbref_splits.csv")
        p.add_argument("--out", type=str, default="output/splits_merged.csv")
        p.set_defaults(func=cmd_day52_splits_v2)
    try:
        _register_day52_v2(_SP)
    except Exception:
        pass
# === Co-GM append end (Day52_splits_v2) ===
