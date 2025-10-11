#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Statcast Expanded Features (one-shot):
- 이름 백필(리더보드)
- 2025 투수 피처 (dominant_pitch / usage / repeat_rate)
- n-그램(bigram, trigram) 전환확률
- 구간 길이 분포(연속 k회)
- 카운트별 전환행렬(있으면)
- 타자 관점(연속 타석 LA/EV 변화, 전환 이벤트 전/후)
- 구역(heart/edge/chase)별 반복/전환률(있으면)
- 팀 단위 집계(리그/팀-시즌 반복률/엔트로피; 팀정보 없으면 스킵)
- 리더보드 다양화(팀 내 상위, 구종별 스페셜리스트, 상승/하락)
- DuckDB 질의 템플릿 출력
"""
import os, sys, json, datetime as dt
import duckdb, pandas as pd

ROOT = "/workspaces/cogm-assistant"
OUT  = f"{ROOT}/output"
SUM  = f"{OUT}/summaries"
REP  = f"{OUT}/reports"
LOGF = f"{ROOT}/logs/statcast_expanded_features.log"

PARQ = f"{OUT}/statcast_ultra_full.parquet"          # v4.3 마스터
LB_FILES = [
    f"{SUM}/leaderboard_entropy_top10.csv",
    f"{SUM}/leaderboard_repeat_low_top10.csv",
    f"{SUM}/leaderboard_repeat_high_top10.csv",
]
OUT_2025 = f"{SUM}/statcast_features_pitcher_2025.csv"
TEMPLATES_TXT = f"{SUM}/duckdb_query_templates.txt"

os.makedirs(OUT, exist_ok=True)
os.makedirs(SUM, exist_ok=True)
os.makedirs(REP, exist_ok=True)
os.makedirs(os.path.dirname(LOGF), exist_ok=True)

def log(msg):
    ts = dt.datetime.now(dt.timezone.utc).isoformat()
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOGF, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def safe_run(name, fn):
    try:
        fn()
        log(f"[OK] {name}")
    except Exception as e:
        log(f"[SKIP] {name}: {e}")

def con_open():
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.execute("PRAGMA memory_limit='1024MB'")
    return con

# ------------------------------------------------------------------------------
# 공통 뷰
# ------------------------------------------------------------------------------
def ensure_views(con):
    # 마스터 스키마 조사
    p = PARQ.replace("'", "''")
    cols = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{p}')").fetchdf()['column_name'].str.lower().tolist()

    def first_present(*cands):
        for c in cands:
            if c and c.lower() in cols:
                return c
        return None

    # 원본 컬럼 이름 추적
    col_pitcher = first_present('pitcher', 'mlb_id', 'mlbam', 'pitcher_id')
    col_batter  = first_present('batter', 'mlbam', 'batter_id')
    col_name    = first_present('player_name', 'name')

    col_ev   = first_present('ev', 'EV', 'exit_velocity', 'exitvelo')
    col_la   = first_present('la', 'LA', 'launch_angle')
    col_velo = first_present('velo', 'Velo', 'release_speed', 'pitch_velo', 'velocity')
    col_spin = first_present('spin', 'Spin', 'spin_rate', 'SpinRate')
    col_pfx_x= first_present('pfx_x', 'h_mov_in', 'pfxX')
    col_pfx_z= first_present('pfx_z', 'v_mov_in', 'pfxZ')
    col_ext  = first_present('ext', 'extension')

    col_edge  = first_present('edge')
    col_heart = first_present('heart')
    col_chase = first_present('chase')

    # 카운트 컬럼 (여러 이름 후보를 지원; 없으면 None)
    col_balls   = first_present('balls', 'b_count', 'ball_count', 'count_balls')
    col_strikes = first_present('strikes', 's_count', 'strike_count', 'count_strikes')

    # 안전한 SELECT 문자열 구성 (없는 컬럼은 NULL로 대체)
    def sel(c, cast_as=None):
        if not c:
            return f"CAST(NULL AS {cast_as})" if cast_as else "NULL"
        if cast_as:
            return f"TRY_CAST({c} AS {cast_as})"
        return c

    sel_pitcher = sel(col_pitcher, "VARCHAR") if col_pitcher else "CAST(NULL AS VARCHAR)"
    sel_batter  = sel(col_batter,  "VARCHAR") if col_batter  else "CAST(NULL AS VARCHAR)"
    sel_name    = sel(col_name,    "VARCHAR") if col_name    else "CAST(NULL AS VARCHAR)"

    con.execute(f"""
      CREATE OR REPLACE TEMP VIEW sc AS
      SELECT
        TRY_CAST(year AS INT)                                        AS season,
        {sel_pitcher}                                                AS pitcher_id,
        {sel_batter}                                                 AS batter_id,
        {sel_name}                                                   AS player_name,
        COALESCE(pitch_type, 'UNK')                                  AS pitch_type,
        {sel(col_ev,   'DOUBLE')}                                    AS ev,
        {sel(col_la,   'DOUBLE')}                                    AS la,
        {sel(col_velo, 'DOUBLE')}                                    AS velo,
        {sel(col_spin, 'DOUBLE')}                                    AS spin,
        {sel(col_pfx_x,'DOUBLE')}                                    AS pfx_x,
        {sel(col_pfx_z,'DOUBLE')}                                    AS pfx_z,
        {sel(col_ext,  'DOUBLE')}                                    AS extension,
        {"CASE WHEN "+col_edge+" IN (0,1) THEN "+col_edge+" END" if col_edge else "CAST(NULL AS INTEGER)"} AS edge_flag,
        {"CASE WHEN "+col_heart+" IN (0,1) THEN "+col_heart+" END" if col_heart else "CAST(NULL AS INTEGER)"} AS heart_flag,
        {"CASE WHEN "+col_chase+" IN (0,1) THEN "+col_chase+" END" if col_chase else "CAST(NULL AS INTEGER)"} AS chase_flag,
        {sel(col_balls,   'INT')}                                     AS balls_cnt,
        {sel(col_strikes, 'INT')}                                     AS strikes_cnt,
        COALESCE(game_pk, 0)                                          AS game_pk
      FROM read_parquet('{p}')
    """)

# ------------------------------------------------------------------------------
# 0) 리더보드 이름 백필
# ------------------------------------------------------------------------------
def backfill_leaderboard_names():
    con = con_open()
    ensure_views(con)
    sc_df = con.execute("""
      SELECT season, pitcher_id, MAX(player_name) AS name_any
      FROM sc GROUP BY 1,2
    """).fetchdf()
    for f in LB_FILES:
        if not os.path.isfile(f):
            log(f"[SKIP] leaderboard not found: {f}")
            continue
        lb = pd.read_csv(f)
        merged = lb.merge(sc_df, on=["season","pitcher_id"], how="left")
        merged["name"] = merged.get("name", "").astype(str)
        merged["name"] = merged.apply(
            lambda r: r["name_any"] if (r["name"]=="" or r["name"].lower()=="nan") else r["name"],
            axis=1
        )
        merged.drop(columns=["name_any"], inplace=True, errors="ignore")
        cols = [c for c in ["season","pitcher_id","name","dominant_pitch","usage_entropy","repeat_rate","rk"] if c in merged.columns]
        if cols: merged = merged[cols]
        merged.to_csv(f, index=False)
        log(f"[OK] backfilled -> {f}")
    con.close()

# ------------------------------------------------------------------------------
# 1) 2025 투수 피처
# ------------------------------------------------------------------------------
def build_pitcher_features_2025():
    con = con_open()
    ensure_views(con)
    feat = con.execute("""
      WITH base AS (
        SELECT * FROM sc WHERE season=2025
      ),
      pairs AS (
        SELECT season, pitcher_id, pitch_type,
               LAG(pitch_type) OVER(PARTITION BY season, pitcher_id ORDER BY season, game_pk, batter_id) AS prev_pt
        FROM base WHERE pitch_type IS NOT NULL
      ),
      rpt AS (
        SELECT season, pitcher_id,
               SUM(CASE WHEN prev_pt=pitch_type THEN 1 ELSE 0 END)*1.0/NULLIF(COUNT(*),0) AS repeat_rate
        FROM pairs
        GROUP BY 1,2
      ),
      usage AS (
        SELECT season, pitcher_id, pitch_type, COUNT(*) AS n
        FROM base WHERE pitch_type IS NOT NULL
        GROUP BY 1,2,3
      ),
      tot AS (
        SELECT season, pitcher_id, SUM(n) AS tot FROM usage GROUP BY 1,2
      ),
      joinu AS (
        SELECT u.season, u.pitcher_id, u.pitch_type, u.n, t.tot,
               (u.n*1.0)/NULLIF(t.tot,0) AS usage_rate
        FROM usage u JOIN tot t USING (season, pitcher_id)
      ),
      ranked AS (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY season, pitcher_id ORDER BY usage_rate DESC, n DESC) AS rk
        FROM joinu
      ),
      top1 AS (
        SELECT season, pitcher_id,
               MAX(CASE WHEN rk=1 THEN pitch_type END) AS dominant_pitch,
               MAX(CASE WHEN rk=1 THEN usage_rate END) AS dominant_usage
        FROM ranked GROUP BY 1,2
      ),
      name_any AS (
        SELECT season, pitcher_id, MAX(player_name) AS name
        FROM base GROUP BY 1,2
      )
      SELECT n.season, n.pitcher_id, n.name,
             t.dominant_pitch, t.dominant_usage, r.repeat_rate
      FROM name_any n
      LEFT JOIN top1 t USING (season, pitcher_id)
      LEFT JOIN rpt  r USING (season, pitcher_id)
      ORDER BY name NULLS LAST, pitcher_id
    """).fetchdf()
    feat.to_csv(OUT_2025, index=False)
    con.close()
    log(f"[OK] wrote 2025 features -> {OUT_2025} (rows={len(feat)})")

# ------------------------------------------------------------------------------
# 2) n-그램 전환확률 (bigram, trigram)
# ------------------------------------------------------------------------------
def build_ngrams():
    con = con_open()
    ensure_views(con)
    # bigram
    con.execute("""
      CREATE OR REPLACE TEMP VIEW w AS
      SELECT season, pitcher_id, game_pk, batter_id, pitch_type,
             LAG(pitch_type) OVER(PARTITION BY season, pitcher_id ORDER BY season, game_pk, batter_id) AS prev_pt,
             LAG(pitch_type,2) OVER(PARTITION BY season, pitcher_id ORDER BY season, game_pk, batter_id) AS prev2_pt
      FROM sc WHERE pitch_type IS NOT NULL
    """)
    bi = con.execute("""
      SELECT season, pitcher_id, prev_pt AS from_pt, pitch_type AS to_pt,
             COUNT(*) AS n,
             COUNT(*)*1.0 / NULLIF(SUM(COUNT(*)) OVER (PARTITION BY season, pitcher_id, prev_pt),0) AS prob
      FROM w WHERE prev_pt IS NOT NULL
      GROUP BY 1,2,3,4
      ORDER BY season, pitcher_id, n DESC
    """).fetchdf()
    bi.to_csv(f"{SUM}/ngram_bigram_probs.csv", index=False)

    # trigram
    tri = con.execute("""
      SELECT season, pitcher_id, prev2_pt AS from2, prev_pt AS from1, pitch_type AS to_pt,
             COUNT(*) AS n,
             COUNT(*)*1.0 / NULLIF(SUM(COUNT(*)) OVER (PARTITION BY season, pitcher_id, prev2_pt, prev_pt),0) AS prob
      FROM w WHERE prev2_pt IS NOT NULL
      GROUP BY 1,2,3,4,5
      ORDER BY season, pitcher_id, n DESC
    """).fetchdf()
    tri.to_csv(f"{SUM}/ngram_trigram_probs.csv", index=False)
    con.close()

# ------------------------------------------------------------------------------
# 3) 구간 길이 분포(연속 k회)
# ------------------------------------------------------------------------------
def build_run_lengths():
    con = con_open()
    ensure_views(con)
    rl = con.execute("""
      WITH w AS (
        SELECT season, pitcher_id, pitch_type,
               ROW_NUMBER() OVER(PARTITION BY season, pitcher_id ORDER BY season, game_pk, batter_id)           AS rn_all,
               ROW_NUMBER() OVER(PARTITION BY season, pitcher_id, pitch_type ORDER BY season, game_pk, batter_id) AS rn_pt
        FROM sc WHERE pitch_type IS NOT NULL
      ),
      g AS (
        SELECT season, pitcher_id, pitch_type, (rn_all - rn_pt) AS grp
        FROM w
      ),
      agg AS (
        SELECT season, pitcher_id, pitch_type, grp, COUNT(*) AS run_len
        FROM g GROUP BY 1,2,3,4
      )
      SELECT season, pitcher_id, pitch_type, run_len, COUNT(*) AS n_runs
      FROM agg GROUP BY 1,2,3,4
      ORDER BY season, pitcher_id, pitch_type, run_len DESC
    """).fetchdf()
    rl.to_csv(f"{SUM}/run_length_distribution.csv", index=False)
    con.close()

# ------------------------------------------------------------------------------
# 4) 카운트별 전환행렬(있으면)
# ------------------------------------------------------------------------------
def build_count_transition():
    con = con_open()
    ensure_views(con)
    try:
        has_counts = con.execute("""
          SELECT 1
          FROM sc
          WHERE balls_cnt IS NOT NULL OR strikes_cnt IS NOT NULL
          LIMIT 1
        """).fetchone() is not None
        if not has_counts:
            log("[SKIP] count-based transition: no count columns present")
            con.close()
            return

        ct = con.execute("""
          WITH w AS (
            SELECT season, pitcher_id, balls_cnt, strikes_cnt, pitch_type,
                   LAG(pitch_type) OVER(
                     PARTITION BY season, pitcher_id, balls_cnt, strikes_cnt
                     ORDER BY season, game_pk, batter_id
                   ) AS prev_pt
            FROM sc
            WHERE pitch_type IS NOT NULL
          )
          SELECT season, pitcher_id, balls_cnt AS balls, strikes_cnt AS strikes,
                 prev_pt AS from_pt, pitch_type AS to_pt, COUNT(*) AS n
          FROM w
          WHERE prev_pt IS NOT NULL
          GROUP BY 1,2,3,4,5,6
          ORDER BY season, pitcher_id, balls, strikes, n DESC
        """).fetchdf()
        ct.to_csv(f"{SUM}/count_transition_matrix.csv", index=False)
        log("[OK] count transition")
    except Exception as e:
        log(f"[SKIP] count transition: {e}")
    con.close()

# ------------------------------------------------------------------------------
# 5) 타자 관점(연속 타석 LA/EV 변화, 전환 이벤트 전/후)
# ------------------------------------------------------------------------------
def build_batter_angle_ev():
    con = con_open()
    ensure_views(con)
    # 연속 타석 LA/EV 변화
    ba = con.execute("""
      WITH b AS (
        SELECT season, batter_id, game_pk, la, ev
        FROM sc
        WHERE batter_id IS NOT NULL
      ),
      w AS (
        SELECT b.*,
               LAG(la) OVER (PARTITION BY season, batter_id ORDER BY season, game_pk) AS lag_la,
               LAG(ev) OVER (PARTITION BY season, batter_id ORDER BY season, game_pk) AS lag_ev
        FROM b
      )
      SELECT season, batter_id,
             AVG(la)                    AS la_mean,
             STDDEV(la)                 AS la_std,
             AVG(ABS(la - lag_la))      AS la_abs_diff_mean,
             AVG(ev)                    AS ev_mean,
             STDDEV(ev)                 AS ev_std,
             AVG(ABS(ev - lag_ev))      AS ev_abs_diff_mean
      FROM w
      GROUP BY 1,2
    """).fetchdf()
    ba.to_csv(f"{SUM}/batter_la_ev_variability.csv", index=False)

    # 피치 전환 이벤트 직전/직후(타자 반응성 단순 프록시)
    try:
        resp = con.execute("""
          WITH w AS (
            SELECT season, batter_id, game_pk, la, ev, pitch_type,
                   LAG(pitch_type) OVER (PARTITION BY season, batter_id ORDER BY season, game_pk) AS prev_pt,
                   LAG(la)        OVER (PARTITION BY season, batter_id ORDER BY season, game_pk) AS prev_la,
                   LAG(ev)        OVER (PARTITION BY season, batter_id ORDER BY season, game_pk) AS prev_ev
            FROM sc
            WHERE batter_id IS NOT NULL AND pitch_type IS NOT NULL
          ),
          ch AS (
            SELECT * FROM w WHERE prev_pt IS NOT NULL AND prev_pt <> pitch_type
          )
          SELECT season, batter_id,
                 AVG(ABS(la - prev_la)) AS la_delta_after_change,
                 AVG(ABS(ev - prev_ev)) AS ev_delta_after_change,
                 COUNT(*) AS change_events
          FROM ch GROUP BY 1,2
        """).fetchdf()
        resp.to_csv(f"{SUM}/batter_mix_responsiveness.csv", index=False)
        log("[OK] batter responsiveness")
    except Exception as e:
        log(f"[SKIP] batter responsiveness: {e}")
    con.close()

# ------------------------------------------------------------------------------
# 6) 구역별 반복/전환률(heart/edge/chase 있으면)
# ------------------------------------------------------------------------------
def build_zone_repeat_transition():
    con = con_open()
    ensure_views(con)
    try:
        z = con.execute("""
          WITH base AS (
            SELECT season, pitcher_id, pitch_type,
                   heart_flag, edge_flag, chase_flag,
                   LAG(pitch_type) OVER(PARTITION BY season, pitcher_id ORDER BY season, game_pk, batter_id) AS prev_pt
            FROM sc WHERE pitch_type IS NOT NULL
          ),
          zlab AS (
            SELECT season, pitcher_id, pitch_type, prev_pt,
                   CASE
                     WHEN heart_flag=1 THEN 'heart'
                     WHEN edge_flag =1 THEN 'edge'
                     WHEN chase_flag=1 THEN 'chase'
                     ELSE 'none'
                   END AS zone
            FROM base
          )
          SELECT season, pitcher_id, zone,
                 SUM(CASE WHEN prev_pt=pitch_type THEN 1 ELSE 0 END)*1.0/NULLIF(COUNT(*),0) AS repeat_rate,
                 COUNT(*) AS n
          FROM zlab
          WHERE prev_pt IS NOT NULL
          GROUP BY 1,2,3
        """).fetchdf()
        z.to_csv(f"{SUM}/zone_repeat_rates.csv", index=False)
        log("[OK] zone repeat rates")
    except Exception as e:
        log(f"[SKIP] zone repeat/transition: {e}")
    con.close()

# ------------------------------------------------------------------------------
# 7) 팀 단위 집계(팀정보 없으면 스킵)
#   - 팀명은 master에 없을 수 있으므로 player_cards_all.csv로 보강 가능
# ------------------------------------------------------------------------------
def build_team_aggregates():
    cards_csv = f"{OUT}/player_cards_all.csv"
    if not os.path.isfile(cards_csv):
        log("[SKIP] team aggregates: player_cards_all.csv not found")
        return
    con = con_open()
    ensure_views(con)
    cards = pd.read_csv(cards_csv, dtype=str)
    # 최소 키: season, name, team/teamName
    keep = [c for c in cards.columns if c.lower() in ("season","name","team","teamname","league")]
    if "season" not in keep or "name" not in keep:
        log("[SKIP] team aggregates: missing season/name in player_cards_all.csv")
        con.close()
        return
    # team 컬럼 통일
    cards["team_"] = cards.get("team", cards.get("teamName", "")).astype(str)
    cards = cards[["season","name","team_"]].rename(columns={"season":"season_str"})
    cards["season"] = pd.to_numeric(cards["season_str"], errors="coerce").astype("Int64")
    cards.dropna(subset=["season"], inplace=True)
    cards["season"] = cards["season"].astype(int)
    con.register("cards_in", cards)

    # pitcher_id를 이름으로 매칭(완벽하진 않지만 팀 단위 러프 집계엔 충분)
    team = con.execute("""
      WITH name_any AS (
        SELECT season, pitcher_id, MAX(player_name) AS name
        FROM sc GROUP BY 1,2
      ),
      m AS (
        SELECT n.season, n.pitcher_id, n.name, c.team_ AS team
        FROM name_any n
        LEFT JOIN cards_in c ON n.season=c.season AND n.name=c.name
      ),
      pairs AS (
        SELECT s.season, s.pitcher_id, s.pitch_type,
               LAG(s.pitch_type) OVER(PARTITION BY s.season, s.pitcher_id ORDER BY s.season, s.game_pk, s.batter_id) AS prev_pt
        FROM sc s WHERE pitch_type IS NOT NULL
      ),
      rpt AS (
        SELECT season, pitcher_id,
               SUM(CASE WHEN prev_pt=pitch_type THEN 1 ELSE 0 END)*1.0/NULLIF(COUNT(*),0) AS repeat_rate
        FROM pairs WHERE prev_pt IS NOT NULL
        GROUP BY 1,2
      ),
      usage AS (
        SELECT season, pitcher_id, pitch_type, COUNT(*) AS n
        FROM sc WHERE pitch_type IS NOT NULL
        GROUP BY 1,2,3
      ),
      tot AS (SELECT season, pitcher_id, SUM(n) AS tot FROM usage GROUP BY 1,2),
      joinu AS (
        SELECT u.season, u.pitcher_id, u.pitch_type, u.n, t.tot,
               (u.n*1.0)/NULLIF(t.tot,0) AS usage_rate
        FROM usage u JOIN tot t USING (season, pitcher_id)
      ),
      ent AS (
        SELECT season, pitcher_id,
               -SUM(CASE WHEN usage_rate>0 THEN usage_rate*ln(usage_rate) ELSE 0 END) AS usage_entropy
        FROM joinu GROUP BY 1,2
      ),
      joined AS (
        SELECT m.team, COALESCE(rpt.season, ent.season) AS season,
               rpt.pitcher_id, rpt.repeat_rate, ent.usage_entropy
        FROM m
        LEFT JOIN rpt USING (season, pitcher_id)
        LEFT JOIN ent USING (season, pitcher_id)
        WHERE m.team IS NOT NULL AND m.team <> ''
      )
      SELECT team, season,
             AVG(repeat_rate) AS team_repeat_rate,
             AVG(usage_entropy) AS team_usage_entropy,
             COUNT(DISTINCT pitcher_id) AS n_pitchers
      FROM joined
      GROUP BY 1,2
      ORDER BY season DESC, team
    """).fetchdf()
    team.to_csv(f"{SUM}/team_season_aggregates.csv", index=False)
    con.close()
    log("[OK] team aggregates")

# ------------------------------------------------------------------------------
# 8) 리더보드 다양화 (팀 내 상위, 구종별 스페셜리스트, 상승/하락)
# ------------------------------------------------------------------------------
def build_more_leaderboards():
    con = con_open()
    ensure_views(con)
    # 기본 피처(시즌·투수) 한 벌 만들고 재사용
    base = con.execute("""
      WITH pairs AS (
        SELECT season, pitcher_id, pitch_type,
               LAG(pitch_type) OVER(PARTITION BY season, pitcher_id ORDER BY season, game_pk, batter_id) AS prev_pt
        FROM sc WHERE pitch_type IS NOT NULL
      ),
      rpt AS (
        SELECT season, pitcher_id,
               SUM(CASE WHEN prev_pt=pitch_type THEN 1 ELSE 0 END)*1.0/NULLIF(COUNT(*),0) AS repeat_rate
        FROM pairs WHERE prev_pt IS NOT NULL
        GROUP BY 1,2
      ),
      usage AS (
        SELECT season, pitcher_id, pitch_type, COUNT(*) AS n
        FROM sc WHERE pitch_type IS NOT NULL
        GROUP BY 1,2,3
      ),
      tot AS (SELECT season, pitcher_id, SUM(n) AS tot FROM usage GROUP BY 1,2),
      joinu AS (
        SELECT u.season, u.pitcher_id, u.pitch_type, u.n, t.tot,
               (u.n*1.0)/NULLIF(t.tot,0) AS usage_rate
        FROM usage u JOIN tot t USING (season, pitcher_id)
      ),
      ent AS (
        SELECT season, pitcher_id,
               -SUM(CASE WHEN usage_rate>0 THEN usage_rate*ln(usage_rate) ELSE 0 END) AS usage_entropy
        FROM joinu GROUP BY 1,2
      ),
      name_any AS (
        SELECT season, pitcher_id, MAX(player_name) AS name
        FROM sc GROUP BY 1,2
      )
      SELECT n.season, n.pitcher_id, n.name, e.usage_entropy, r.repeat_rate
      FROM name_any n
      LEFT JOIN ent e USING (season, pitcher_id)
      LEFT JOIN rpt r USING (season, pitcher_id)
    """).fetchdf()
    base.to_csv(f"{SUM}/pitcher_season_feature_base.csv", index=False)

    # 상승/하락: 인접 시즌 diff
    try:
        con.register("base_in", base)
        chg = con.execute("""
          WITH w AS (
            SELECT season, pitcher_id, name, usage_entropy, repeat_rate,
                   LAG(usage_entropy) OVER(PARTITION BY pitcher_id ORDER BY season) AS prev_ent,
                   LAG(repeat_rate)   OVER(PARTITION BY pitcher_id ORDER BY season) AS prev_rep
            FROM base_in
          )
          SELECT season, pitcher_id, name,
                 (usage_entropy - prev_ent) AS d_usage_entropy,
                 (repeat_rate   - prev_rep) AS d_repeat_rate
          FROM w WHERE prev_ent IS NOT NULL AND prev_rep IS NOT NULL
          ORDER BY season DESC
        """).fetchdf()
        chg.to_csv(f"{SUM}/leaderboard_season_change.csv", index=False)
    except Exception as e:
        log(f"[SKIP] season change leaders: {e}")

    # 구종별 스페셜리스트(그 구종 사용률이 가장 높은 투수들)
    try:
        spec = con.execute("""
          WITH usage AS (
            SELECT season, pitcher_id, pitch_type, COUNT(*) AS n
            FROM sc WHERE pitch_type IS NOT NULL
            GROUP BY 1,2,3
          ),
          tot AS (SELECT season, pitcher_id, SUM(n) AS tot FROM usage GROUP BY 1,2),
          joinu AS (
            SELECT u.season, u.pitcher_id, u.pitch_type, u.n, t.tot,
                   (u.n*1.0)/NULLIF(t.tot,0) AS usage_rate
            FROM usage u JOIN tot t USING (season, pitcher_id)
          ),
          name_any AS (
            SELECT season, pitcher_id, MAX(player_name) AS name
            FROM sc GROUP BY 1,2
          )
          SELECT j.season, j.pitcher_id, n.name, j.pitch_type, j.usage_rate
          FROM joinu j LEFT JOIN name_any n USING (season, pitcher_id)
          QUALIFY ROW_NUMBER() OVER (PARTITION BY j.season, j.pitch_type ORDER BY j.usage_rate DESC, j.n DESC) <= 20
          ORDER BY j.season DESC, j.pitch_type, j.usage_rate DESC
        """).fetchdf()
        spec.to_csv(f"{SUM}/leaderboard_pitch_specialists.csv", index=False)
    except Exception as e:
        log(f"[SKIP] pitch specialists: {e}")
    con.close()

# ------------------------------------------------------------------------------
# 9) DuckDB 질의 템플릿
# ------------------------------------------------------------------------------
def write_query_templates():
    txt = f"""
-- Quick slices (replace values as needed)

-- 1) 특정 투수 2024~2025 전환행렬
SELECT prev_pt, pitch_type AS cur_pt, COUNT(*) AS n
FROM (
  SELECT pitch_type,
         LAG(pitch_type) OVER(PARTITION BY pitcher_id ORDER BY season, game_pk, batter_id) AS prev_pt
  FROM sc WHERE pitcher_id = 'XXXXX' AND season BETWEEN 2024 AND 2025 AND pitch_type IS NOT NULL
)
WHERE prev_pt IS NOT NULL
GROUP BY 1,2 ORDER BY n DESC;

-- 2) 특정 팀 2025 엔트로피/반복률 평균(팀 테이블이 있을 때)
SELECT * FROM read_csv_auto('{SUM}/team_season_aggregates.csv')
WHERE season=2025 AND team='NYA';

-- 3) n-그램 bigram에서 FF -> SL 확률 상위 투수
SELECT * FROM read_csv_auto('{SUM}/ngram_bigram_probs.csv')
WHERE from_pt='FF' AND to_pt='SL'
ORDER BY prob DESC LIMIT 50;

-- 4) 연속 FF run length 분포(상위 TOP 구간)
SELECT * FROM read_csv_auto('{SUM}/run_length_distribution.csv')
WHERE pitch_type='FF' ORDER BY run_len DESC LIMIT 50;

-- 5) 카운트별 전환(0-2에서 변화)
SELECT * FROM read_csv_auto('{SUM}/count_transition_matrix.csv')
WHERE balls=0 AND strikes=2 ORDER BY n DESC LIMIT 50;
"""
    with open(TEMPLATES_TXT, "w", encoding="utf-8") as f:
        f.write(txt)
    log(f"[OK] wrote templates -> {TEMPLATES_TXT}")

# ------------------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------------------
def main():
    open(LOGF, "w").write(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] start\n")
    if not os.path.isfile(PARQ):
        log(f"❌ missing {PARQ} — 먼저 build_statcast_ultra_v4_3.py 실행 필요")
        print("⚠️ Completed with warnings (master missing)"); sys.exit(0)

    # 0) 이름 백필
    safe_run("leaderboard backfill", backfill_leaderboard_names)
    # 1) 2025 피처
    safe_run("2025 pitcher features", build_pitcher_features_2025)
    # 2) n-그램
    safe_run("ngram features", build_ngrams)
    # 3) run-length
    safe_run("run-length", build_run_lengths)
    # 4) count transition
    safe_run("count-based transition", build_count_transition)
    # 5) batter LA/EV
    safe_run("batter LA/EV", build_batter_angle_ev)
    # 6) zone repeat/transition
    safe_run("zone repeat/transition", build_zone_repeat_transition)
    # 7) 팀 집계
    safe_run("team aggregates", build_team_aggregates)
    # 8) 더 많은 리더보드
    safe_run("more leaderboards", build_more_leaderboards)
    # 9) 질의 템플릿
    safe_run("query templates", write_query_templates)

    log("[DONE] expanded features complete")
    print("✅ Expanded features completed (always exit 0)")

if __name__ == "__main__":
    main()
