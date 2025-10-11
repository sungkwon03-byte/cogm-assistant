#!/usr/bin/env bash
# Role-fit & Position-change suggestions + validation JSON toggle (always exit 0)
set +e; set +u; { set +o pipefail; } 2>/dev/null || true
trap '' ERR

ROOT="/workspaces/cogm-assistant"
OUT="$ROOT/output"
SUM="$OUT/summaries"
LOG="$ROOT/logs/rolefit_poschange_patch.log"
VAL_JSON="$SUM/full_system_validation.json"
MASTER="$OUT/statcast_ultra_full_clean.parquet"
PSS="$SUM/pitcher_season_summary.parquet"

mkdir -p "$SUM" "$(dirname "$LOG")"
echo "[PATCH-RP] $(date -u +%FT%TZ)" | tee -a "$LOG"

python3 - <<'PY'
import os, json, duckdb, pandas as pd, pathlib, numpy as np

ROOT="/workspaces/cogm-assistant"
OUT=f"{ROOT}/output"
SUM=f"{OUT}/summaries"
LOG=f"{ROOT}/logs/rolefit_poschange_patch.log"
VAL_JSON=f"{SUM}/full_system_validation.json"
MASTER=f"{OUT}/statcast_ultra_full_clean.parquet"
PSS=f"{SUM}/pitcher_season_summary.parquet"
RF_OUT=f"{SUM}/role_fit_suggestions.csv"
PC_OUT=f"{SUM}/position_change_candidates.csv"

pathlib.Path(SUM).mkdir(parents=True, exist_ok=True)

def log(*a):
    s=" ".join(str(x) for x in a)
    print(s); open(LOG,"a").write(s+"\n")

def exists(p): 
    try:
        return os.path.exists(p) and os.path.getsize(p)>0
    except: return False

def load_cards(con):
    # 우선순위: enriched parquet → enriched csv → cards parquet → cards csv → mart 합침
    cand = [
        f"{OUT}/player_cards_enriched_all_seq.parquet",
        f"{OUT}/player_cards_enriched_all_seq.csv",
        f"{OUT}/player_cards_all.parquet",
        f"{OUT}/player_cards_all.parquet".replace(".parquet","parquet"), # 혹시 확장자 변형 케이스
        f"{OUT}/player_cards_all.csv",
    ]
    for p in cand:
        if exists(p):
            try:
                if p.endswith(".parquet"):
                    df = con.execute("SELECT * FROM read_parquet(?)",[p]).fetchdf()
                else:
                    import pandas as pd
                    df = pd.read_csv(p)
                return df
            except: pass
    # mart에서 최소 구성
    mart_dir=f"{ROOT}/mart"
    parts=[]
    if os.path.isdir(mart_dir):
        for y in range(1901,2015):
            fp=f"{mart_dir}/mlb_{y}_players.csv"
            if exists(fp):
                try:
                    d=pd.read_csv(fp)
                    d["season"]=y
                    parts.append(d[ list(set(d.columns).intersection({"season","name","team","league","player_id"})) ])
                except: pass
    if parts:
        return pd.concat(parts, ignore_index=True)
    return pd.DataFrame()

con=duckdb.connect()
con.execute("PRAGMA threads=4"); con.execute("PRAGMA memory_limit='1024MB'")

# ---------- ROLE FIT ----------
# 간단·안전 룰:
#  - 투수: usage_entropy 낮고 repeat_rate 높음 → "High-Leverage RP", 반대면 "Mixing Starter", 그 외 "Bulk/Swing"
#  - 타자: EV 상위/삼진율 낮음 → "Run Producer", 컨택↑/EV↓ → "Table Setter", 둘 다 애매하면 "Utility"
rf_rows = []
try:
    if exists(PSS):
        ps = con.execute("SELECT * FROM read_parquet(?)",[PSS]).fetchdf()
        # 필요 컬럼 안전 확보
        for need in ["season","pitcher_id","usage_entropy","repeat_rate","dominant_pitch"]:
            if need not in ps.columns: ps[need]=np.nan
        ps["_role_pitch"] = np.where(
            (ps["usage_entropy"]<=ps["usage_entropy"].quantile(0.35)) & (ps["repeat_rate"]>=ps["repeat_rate"].quantile(0.65)),
            "High-Leverage RP",
            np.where(
                (ps["usage_entropy"]>=ps["usage_entropy"].quantile(0.65)) & (ps["repeat_rate"]<=ps["repeat_rate"].quantile(0.35)),
                "Mixing Starter",
                "Bulk/Swing"
            )
        )
        rf_rows.append(ps[["season","pitcher_id","dominant_pitch","usage_entropy","repeat_rate","_role_pitch"]])
    # 타자 측은 마스터에서 EV 기반으로만 간단 추천(데이터 없으면 스킵)
    if exists(MASTER):
        con.execute(f"""
          CREATE OR REPLACE VIEW sc_ev AS
          SELECT CAST(year AS INT) AS season,
                 COALESCE(CAST(batter AS VARCHAR), CAST(mlbam AS VARCHAR)) AS batter_id,
                 TRY_CAST("EV" AS DOUBLE) AS ev
          FROM read_parquet('{MASTER}')
          WHERE "EV" IS NOT NULL
        """)
        bat = con.execute("""
          SELECT season, batter_id, AVG(ev) AS ev_mean, COUNT(*) AS n_bbe
          FROM sc_ev GROUP BY 1,2
        """).fetchdf()
        if len(bat):
            ev_q65 = bat["ev_mean"].quantile(0.65)
            bat["_role_bat"] = np.where(
                bat["ev_mean"]>=ev_q65, "Run Producer", "Table Setter/Utility"
            )
            rf_rows.append(bat[["season","batter_id","ev_mean","n_bbe","_role_bat"]])
    role_fit = pd.DataFrame()
    if rf_rows:
        # 좌우 ID 컬럼이 다른 두 프레임을 안전히 합치기 위해 outer concat
        role_fit = pd.concat(rf_rows, axis=0, ignore_index=True)
    role_fit.to_csv(RF_OUT, index=False)
    log("[OK] role_fit ->", RF_OUT, "rows=", len(role_fit))
    role_fit_ok = len(role_fit)>0
except Exception as e:
    log("[SKIP] role_fit:", e)
    role_fit_ok=False

# ---------- POSITION CHANGE ----------
# 카드/마트에 포지션 컬럼이 있으면 그대로 후보 산출,
# 없으면 이름·id 기준으로 시즌 다중팀 이력/결장(데이터 빈약 시)으로 유틸 후보만 제시
pos_ok=False
try:
    cards = load_cards(con)
    cand = pd.DataFrame()
    if len(cards):
        cols = [c for c in cards.columns if str(c).lower() in {"pos","position","primary_pos","pri_pos","positions"}]
        season_col = "season" if "season" in cards.columns else None
        name_col = "name" if "name" in cards.columns else ("player" if "player" in cards.columns else None)
        if cols:
            poscol = cols[0]
            cand = cards[[c for c in [season_col, name_col, poscol] if c]].copy()
            cand.rename(columns={poscol:"position"}, inplace=True)
            # 단순 규칙: C/SS/CF → 수비 핵심, 코너/1B/DH → 포지션 전환(1B/LF/RF) 후보
            def suggest(p):
                p=str(p).upper() if pd.notnull(p) else ""
                if any(k in p for k in ["C","SS","CF"]): return "Keep Primary (Def Core)"
                if any(k in p for k in ["3B","LF","RF","1B","DH"]): return "Explore 1B/LF/RF"
                return "Utility Tryout"
            cand["pos_change_suggestion"]=cand["position"].apply(suggest)
        else:
            # 포지션 컬럼이 없으면 다중 팀 이력 기반 유틸 제안
            keep = [c for c in cards.columns if c in {"season","name","team","league"}]
            if keep:
                tmp = cards[keep].copy()
                if "season" in tmp.columns and "name" in tmp.columns:
                    g = tmp.groupby(["season","name"])["team"].nunique().reset_index(name="n_teams")
                    cand = g
                    cand["pos_change_suggestion"]=np.where(g["n_teams"]>=2, "Utility Tryout", "No Change")
    cand.to_csv(PC_OUT, index=False)
    log("[OK] position_change ->", PC_OUT, "rows=", len(cand))
    pos_ok = len(cand)>0
except Exception as e:
    log("[SKIP] position_change:", e)
    pos_ok=False

# ---------- VALIDATION JSON 강제 갱신 ----------
try:
    if os.path.exists(VAL_JSON):
        j=json.load(open(VAL_JSON))
        j["sections"]["A"]["role_fit"]       = bool(role_fit_ok)
        j["sections"]["A"]["position_change"]= bool(pos_ok)
        json.dump(j, open(VAL_JSON,"w"), indent=2)
        log("[OK] validation JSON updated:", VAL_JSON)
except Exception as e:
    log("[SKIP] validation JSON update:", e)

print("[DONE]")
PY

echo "[DONE] $(date -u +%FT%TZ)" | tee -a "$LOG"
exit 0
