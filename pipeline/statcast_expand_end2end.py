# -*- coding: utf-8 -*-
import os, sys, json, gc, datetime as dt
os.environ.setdefault("MPLBACKEND", "Agg")
import duckdb
from pathlib import Path

ROOT = "/workspaces/cogm-assistant"
OUT  = f"{ROOT}/output"
SUM  = f"{OUT}/summaries"
REP  = f"{OUT}/reports"
LOGF = f"{ROOT}/logs/statcast_expand_end2end.log"

MASTER    = f"{OUT}/statcast_ultra_full_clean.parquet"
CARDS_PARQ= f"{OUT}/player_cards_all.parquet"
CARDS_CSV = f"{OUT}/player_cards_all.csv"

PT_SUM = f"{SUM}/pitcher_season_summary.parquet"
NGRAM2 = f"{SUM}/pitch_ngram2.parquet"
NGRAM3 = f"{SUM}/pitch_ngram3.parquet"
RUNLEN = f"{SUM}/pitch_runlength.parquet"
TRANS  = f"{SUM}/pitch_transition.parquet"
ZONE   = f"{SUM}/zone_repeat_transition.parquet"
BATTER = f"{SUM}/batter_la_ev_variability.parquet"
TEAMAG = f"{SUM}/team_season_agg.parquet"

LB_ENT   = f"{SUM}/leaderboard_entropy_top10.csv"
LB_REP_H = f"{SUM}/leaderboard_repeat_high_top10.csv"
LB_REP_L = f"{SUM}/leaderboard_repeat_low_top10.csv"
FEAT25   = f"{SUM}/statcast_features_pitcher_2025.csv"

ENRICH = f"{OUT}/player_cards_enriched_all_seq.parquet"
TEMPL  = f"{SUM}/duckdb_query_templates.txt"

def log(msg: str)->None:
    ts = dt.datetime.now(dt.timezone.utc).isoformat()
    print(msg, flush=True)
    Path(LOGF).parent.mkdir(parents=True, exist_ok=True)
    with open(LOGF, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")

def copy_query(con, query: str, out_path: str, fmt: str = "PARQUET"):
    path = out_path.replace("'", "''")
    con.execute(f"COPY ({query}) TO '{path}' (FORMAT {fmt})")

def table_has_columns(con, select_sql: str, needed: set)->bool:
    cols = con.execute(f"DESCRIBE SELECT * FROM ({select_sql}) LIMIT 0").fetchdf()["column_name"]
    return set(map(str, cols)).issuperset(needed)

def ensure_cards_parquet(con):
    if os.path.isfile(CARDS_PARQ):
        return
    if os.path.isfile(CARDS_CSV):
        log("[RUN] convert player_cards_all.csv -> parquet")
        import pandas as pd
        df = pd.read_csv(CARDS_CSV)
        con.register("cards_in", df)
        copy_query(con, "SELECT * FROM cards_in", CARDS_PARQ, "PARQUET")
        log(f"[OK] -> {CARDS_PARQ}")

def main():
    Path(OUT).mkdir(parents=True, exist_ok=True)
    Path(SUM).mkdir(parents=True, exist_ok=True)
    Path(REP).mkdir(parents=True, exist_ok=True)
    Path(LOGF).write_text(f"[{dt.datetime.now(dt.timezone.utc)}] expand_end2end start\n", encoding="utf-8")

    if not os.path.isfile(MASTER):
        log(f"❌ missing {MASTER} — 먼저 클린 마스터를 생성해 주세요.")
        print("⚠️ Completed with warnings; check logs:", LOGF)
        return

    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.execute("PRAGMA memory_limit='1024MB'")

    mpath = MASTER.replace("'", "''")
    con.execute(f"""
      CREATE OR REPLACE VIEW sc AS
      SELECT
        CAST(year AS INT) AS season,
        COALESCE(CAST(pitcher AS VARCHAR), CAST(mlb_id AS VARCHAR), CAST(mlbam AS VARCHAR)) AS pitcher_id,
        COALESCE(player_name,'') AS player_name,
        pitch_type, events, *,
        COALESCE(CAST(mlb_id AS VARCHAR), CAST(mlbam AS VARCHAR), player_name) AS _row_tiebreaker
      FROM read_parquet('{mpath}')
      WHERE year BETWEEN 2015 AND 2025
    """)

    ensure_cards_parquet(con)

    # 1) Pitcher-season summary
    log("[RUN] pitcher-season summary")
    q_ptsum = """
      WITH pt_usage AS (
        SELECT season, pitcher_id, pitch_type, COUNT(*) AS n
        FROM sc
        WHERE pitch_type IS NOT NULL
        GROUP BY 1,2,3
      ),
      used AS (SELECT season, pitcher_id, SUM(n) AS tot FROM pt_usage GROUP BY 1,2),
      joined AS (
        SELECT u.season, u.pitcher_id, u.pitch_type, u.n, j.tot,
               (u.n*1.0)/NULLIF(j.tot,0) AS usage_rate
        FROM pt_usage u JOIN used j USING(season, pitcher_id)
      ),
      rankd AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY season, pitcher_id ORDER BY usage_rate DESC, n DESC) AS rk
        FROM joined
      ),
      top1 AS (
        SELECT season, pitcher_id,
               MAX(CASE WHEN rk=1 THEN pitch_type END) AS dominant_pitch,
               MAX(CASE WHEN rk=1 THEN usage_rate END) AS dominant_usage
        FROM rankd GROUP BY 1,2
      ),
      ent AS (
        SELECT season, pitcher_id,
               -SUM(CASE WHEN usage_rate>0 THEN usage_rate*ln(usage_rate) ELSE 0 END) AS usage_entropy
        FROM joined GROUP BY 1,2
      ),
      rpt AS (
        WITH w AS (
          SELECT season, pitcher_id, pitch_type,
                 LAG(pitch_type) OVER(PARTITION BY season,pitcher_id ORDER BY season, game_pk, _row_tiebreaker) AS prev_pt
          FROM sc WHERE pitch_type IS NOT NULL
        ),
        pairs AS (SELECT season, pitcher_id, prev_pt, pitch_type AS cur_pt FROM w WHERE prev_pt IS NOT NULL),
        agg AS (SELECT season, pitcher_id, prev_pt, cur_pt, COUNT(*) AS cnt FROM pairs GROUP BY 1,2,3,4),
        tot AS (
          SELECT season, pitcher_id, SUM(cnt) AS total_cnt,
                 SUM(CASE WHEN prev_pt=cur_pt THEN cnt ELSE 0 END) AS repeat_cnt
          FROM agg GROUP BY 1,2
        )
        SELECT season, pitcher_id, (repeat_cnt*1.0)/NULLIF(total_cnt,0) AS repeat_rate FROM tot
      )
      SELECT COALESCE(ps.season, ent.season, rpt.season) AS season,
             COALESCE(ps.pitcher_id, ent.pitcher_id, rpt.pitcher_id) AS pitcher_id,
             ps.dominant_pitch, ps.dominant_usage, ent.usage_entropy, rpt.repeat_rate
      FROM top1 ps
      FULL OUTER JOIN ent USING (season,pitcher_id)
      FULL OUTER JOIN rpt USING (season,pitcher_id)
    """
    try:
        copy_query(con, q_ptsum, PT_SUM, "PARQUET")
        log(f"[OK] -> {PT_SUM}")
    except Exception as e:
        log(f"[SKIP] {e}")

    # 2) n-gram(2,3)
    log("[RUN] n-gram(2,3)")
    q_bi = """
      WITH w AS (
        SELECT season, pitcher_id, game_pk, pitch_type,
               LAG(pitch_type,1) OVER(PARTITION BY season, pitcher_id ORDER BY season, game_pk, _row_tiebreaker) AS p1
        FROM sc WHERE pitch_type IS NOT NULL
      )
      SELECT season, pitcher_id, p1 || '→' || pitch_type AS bigram, COUNT(*) AS n
      FROM w WHERE p1 IS NOT NULL
      GROUP BY 1,2,3
    """
    q_tri = """
      WITH w AS (
        SELECT season, pitcher_id, game_pk, pitch_type,
               LAG(pitch_type,1) OVER(PARTITION BY season, pitcher_id ORDER BY season, game_pk, _row_tiebreaker) AS p1,
               LAG(pitch_type,2) OVER(PARTITION BY season, pitcher_id ORDER BY season, game_pk, _row_tiebreaker) AS p2
        FROM sc WHERE pitch_type IS NOT NULL
      )
      SELECT season, pitcher_id, p2 || '→' || p1 || '→' || pitch_type AS trigram, COUNT(*) AS n
      FROM w WHERE p1 IS NOT NULL AND p2 IS NOT NULL
      GROUP BY 1,2,3
    """
    try:
        copy_query(con, q_bi, NGRAM2, "PARQUET")
        copy_query(con, q_tri, NGRAM3, "PARQUET")
        log("[OK] -> ngram2/ngram3")
    except Exception as e:
        log(f"[SKIP] {e}")

    # 3) run-length (batter 미사용)
    log("[RUN] run-length")
    q_rl = """
      WITH w AS (
        SELECT season, pitcher_id, game_pk, pitch_type,
               LAG(pitch_type) OVER(PARTITION BY season,pitcher_id ORDER BY season, game_pk, _row_tiebreaker) AS prev_pt
        FROM sc WHERE pitch_type IS NOT NULL
      ),
      g AS (
        SELECT season, pitcher_id, game_pk, pitch_type,
               (pitch_type IS DISTINCT FROM prev_pt) AS is_new,
               SUM(CASE WHEN pitch_type IS DISTINCT FROM prev_pt THEN 1 ELSE 0 END)
                 OVER(PARTITION BY season,pitcher_id ORDER BY season, game_pk, _row_tiebreaker
                      ROWS UNBOUNDED PRECEDING) AS grp
        FROM w
      )
      SELECT season, pitcher_id, pitch_type, grp, COUNT(*) AS run_len
      FROM g
      GROUP BY 1,2,3,4
    """
    try:
        copy_query(con, q_rl, RUNLEN, "PARQUET")
        log(f"[OK] -> {RUNLEN}")
    except Exception as e:
        log(f"[SKIP] {e}")

    # 4) balls/strikes 전환 (있으면)
    log("[RUN] count-based transition (if balls/strikes present)")
    try:
        if table_has_columns(con, "SELECT * FROM sc", {"balls","strikes"}):
            q_tr = """
              WITH base AS (
                SELECT season, pitcher_id, game_pk, pitch_type,
                       TRY_CAST(balls AS INT) AS balls, TRY_CAST(strikes AS INT) AS strikes
                FROM sc
              ),
              w AS (
                SELECT season, pitcher_id, game_pk, pitch_type, balls, strikes,
                       LAG(pitch_type) OVER(PARTITION BY season,pitcher_id ORDER BY season, game_pk, _row_tiebreaker) AS prev_pt,
                       LAG(balls)       OVER(PARTITION BY season,pitcher_id ORDER BY season, game_pk, _row_tiebreaker) AS prev_b,
                       LAG(strikes)     OVER(PARTITION BY season,pitcher_id ORDER BY season, game_pk, _row_tiebreaker) AS prev_s
                FROM base
              )
              SELECT season, pitcher_id, prev_b AS balls, prev_s AS strikes,
                     prev_pt, pitch_type AS cur_pt, COUNT(*) AS n
              FROM w
              WHERE prev_pt IS NOT NULL AND prev_b IS NOT NULL AND prev_s IS NOT NULL
              GROUP BY 1,2,3,4,5,6
            """
            copy_query(con, q_tr, TRANS, "PARQUET")
            log(f"[OK] -> {TRANS}")
        else:
            log("[SKIP] no balls/strikes columns")
    except Exception as e:
        log(f"[SKIP] {e}")

    # 5) zone 반복/전환
    log("[RUN] zone repeat/transition (if zone columns present)")
    try:
        cols = set(con.execute("DESCRIBE SELECT * FROM sc LIMIT 0").fetchdf()["column_name"])
        zone_cols = [c for c in ["heart","edge","chase","segment"] if c in cols]
        if zone_cols:
            zone_expr = "COALESCE(" + ",".join([f"CAST({c} AS VARCHAR)" for c in zone_cols]) + ")"
            q_zone = f"""
              WITH w AS (
                SELECT season, pitcher_id, game_pk, pitch_type,
                       {zone_expr} AS zone_tag,
                       LAG(pitch_type) OVER(PARTITION BY season,pitcher_id ORDER BY season, game_pk, _row_tiebreaker) AS prev_pt,
                       LAG({zone_expr}) OVER(PARTITION BY season,pitcher_id ORDER BY season, game_pk, _row_tiebreaker) AS prev_zone
                FROM sc
                WHERE pitch_type IS NOT NULL
              )
              SELECT season, pitcher_id, prev_zone, zone_tag,
                     SUM(CASE WHEN prev_pt = pitch_type THEN 1 ELSE 0 END) AS repeat_cnt,
                     COUNT(*) AS total_cnt,
                     (SUM(CASE WHEN prev_pt = pitch_type THEN 1 ELSE 0 END)*1.0) / NULLIF(COUNT(*),0) AS repeat_rate
              FROM w
              WHERE prev_zone IS NOT NULL AND zone_tag IS NOT NULL
              GROUP BY 1,2,3,4
            """
            copy_query(con, q_zone, ZONE, "PARQUET")
            log(f"[OK] -> {ZONE}")
        else:
            log("[SKIP] no zone columns (heart/edge/chase/segment)")
    except Exception as e:
        log(f"[SKIP] {e}")

    # 6) batter LA/EV 변동성
    log("[RUN] batter LA/EV variability")
    q_bat = """
      WITH b AS (
        SELECT season, batter, game_pk,
               TRY_CAST(LA AS DOUBLE) AS la,
               TRY_CAST(EV AS DOUBLE) AS ev
        FROM sc
      ),
      w AS (
        SELECT *,
               LAG(la) OVER(PARTITION BY season,batter ORDER BY season, game_pk, _row_tiebreaker) AS lag_la,
               LAG(ev) OVER(PARTITION BY season,batter ORDER BY season, game_pk, _row_tiebreaker) AS lag_ev
        FROM b
      )
      SELECT season, batter,
             AVG(la) AS la_mean, STDDEV(la) AS la_std, AVG(ABS(la - lag_la)) AS la_abs_diff_mean,
             AVG(ev) AS ev_mean, STDDEV(ev) AS ev_std, AVG(ABS(ev - lag_ev)) AS ev_abs_diff_mean
      FROM w
      WHERE la IS NOT NULL OR ev IS NOT NULL
      GROUP BY 1,2
    """
    try:
        copy_query(con, q_bat, BATTER, "PARQUET")
        log(f"[OK] -> {BATTER}")
    except Exception as e:
        log(f"[SKIP] {e}")

    # 7) 팀 집계(있을 때)
    log("[RUN] team-season aggregates (if team columns present)")
    try:
        cols = set(con.execute("DESCRIBE SELECT * FROM sc LIMIT 0").fetchdf()["column_name"])
        team_col = "team" if "team" in cols else ("teamName" if "teamName" in cols else None)
        if team_col:
            q_team = f"""
              WITH s AS (SELECT season, {team_col} AS team_name, pitcher_id, pitch_type FROM sc WHERE pitch_type IS NOT NULL),
              pt AS (SELECT season, team_name, pitch_type, COUNT(*) AS n FROM s GROUP BY 1,2,3),
              used AS (SELECT season, team_name, SUM(n) AS tot FROM pt GROUP BY 1,2),
              joined AS (
                SELECT p.season, p.team_name, p.pitch_type, p.n, u.tot, (p.n*1.0)/NULLIF(u.tot,0) AS usage_rate
                FROM pt p JOIN used u USING(season, team_name)
              ),
              ent AS (
                SELECT season, team_name,
                       -SUM(CASE WHEN usage_rate>0 THEN usage_rate*ln(usage_rate) ELSE 0 END) AS usage_entropy
                FROM joined GROUP BY 1,2
              ),
              rpt AS (
                WITH w AS (
                  SELECT season, team_name, pitcher_id, pitch_type,
                         LAG(pitch_type) OVER(PARTITION BY season,team_name,pitcher_id ORDER BY season, game_pk, _row_tiebreaker) AS prev_pt
                  FROM sc WHERE pitch_type IS NOT NULL
                ),
                pairs AS (SELECT season, team_name, prev_pt, pitch_type AS cur_pt FROM w WHERE prev_pt IS NOT NULL),
                agg AS (SELECT season, team_name, prev_pt, cur_pt, COUNT(*) AS cnt FROM pairs GROUP BY 1,2,3,4),
                tot AS (
                  SELECT season, team_name, SUM(cnt) AS total_cnt,
                         SUM(CASE WHEN prev_pt=cur_pt THEN cnt ELSE 0 END) AS repeat_cnt
                  FROM agg GROUP BY 1,2
                )
                SELECT season, team_name, (repeat_cnt*1.0)/NULLIF(total_cnt,0) AS repeat_rate FROM tot
              )
              SELECT e.season, e.team_name, e.usage_entropy, r.repeat_rate
              FROM ent e JOIN rpt r USING(season, team_name)
            """
            copy_query(con, q_team, TEAMAG, "PARQUET")
            log(f"[OK] -> {TEAMAG}")
        else:
            log("[SKIP] no team column (team/teamName)")
    except Exception as e:
        log(f"[SKIP] {e}")

    # 8) Leaderboards (prepared param 제거)
    log("[RUN] leaderboards")
    try:
        pts_path = PT_SUM.replace("'", "''")
        con.execute(f"CREATE OR REPLACE VIEW ps AS SELECT * FROM read_parquet('{pts_path}')")
        con.execute("""
          CREATE OR REPLACE VIEW id2name AS
          SELECT season, pitcher_id, ANY_VALUE(player_name) AS name
          FROM sc
          GROUP BY 1,2
        """)
        con.execute("""
          CREATE OR REPLACE TABLE lb_entropy AS
          SELECT p.season, p.pitcher_id, n.name, p.dominant_pitch, p.usage_entropy,
                 ROW_NUMBER() OVER(PARTITION BY p.season ORDER BY p.usage_entropy DESC NULLS LAST) AS rk
          FROM ps p LEFT JOIN id2name n USING(season,pitcher_id)
          WHERE p.usage_entropy IS NOT NULL
        """)
        copy_query(con, "SELECT * FROM lb_entropy WHERE rk<=10", LB_ENT, "CSV")

        con.execute("""
          CREATE OR REPLACE TABLE lb_repeat_high AS
          SELECT p.season, p.pitcher_id, n.name, p.dominant_pitch, p.repeat_rate,
                 ROW_NUMBER() OVER(PARTITION BY p.season ORDER BY p.repeat_rate DESC NULLS LAST) AS rk
          FROM ps p LEFT JOIN id2name n USING(season,pitcher_id)
          WHERE p.repeat_rate IS NOT NULL
        """)
        copy_query(con, "SELECT * FROM lb_repeat_high WHERE rk<=10", LB_REP_H, "CSV")

        con.execute("""
          CREATE OR REPLACE TABLE lb_repeat_low AS
          SELECT p.season, p.pitcher_id, n.name, p.dominant_pitch, p.repeat_rate,
                 ROW_NUMBER() OVER(PARTITION BY p.season ORDER BY p.repeat_rate ASC NULLS LAST) AS rk
          FROM ps p LEFT JOIN id2name n USING(season,pitcher_id)
          WHERE p.repeat_rate IS NOT NULL
        """)
        copy_query(con, "SELECT * FROM lb_repeat_low WHERE rk<=10", LB_REP_L, "CSV")
        log("[OK] leaderboards")
    except Exception as e:
        log(f"[SKIP] {e}")

    # 9) 2025 features export
    log("[RUN] 2025 pitcher features export")
    try:
        copy_query(con, "SELECT * FROM ps WHERE season=2025", FEAT25, "CSV")
        log(f"[OK] -> {FEAT25}")
    except Exception as e:
        log(f"[SKIP] {e}")

    # 10) 카드 병합
    try:
        if os.path.isfile(CARDS_PARQ):
            log("[RUN] merge cards + summary (season+name)")
            cpath = CARDS_PARQ.replace("'", "''")
            con.execute(f"CREATE OR REPLACE VIEW cards AS SELECT * FROM read_parquet('{cpath}')")
            con.execute("""
              CREATE OR REPLACE TABLE cards_enriched AS
              SELECT
                c.*,
                p.dominant_pitch, p.usage_entropy, p.repeat_rate
              FROM cards c
              LEFT JOIN (
                SELECT season, ANY_VALUE(name) AS name, pitcher_id, dominant_pitch, usage_entropy, repeat_rate
                FROM (
                  SELECT ps.season, COALESCE(n.name,'') AS name, ps.pitcher_id, ps.dominant_pitch, ps.usage_entropy, ps.repeat_rate
                  FROM ps ps LEFT JOIN id2name n USING(season,pitcher_id)
                )
                GROUP BY 1,3,4,5,6
              ) p
              ON c.season = p.season
             AND REPLACE(LOWER(TRIM(c.name)),'  ',' ') = REPLACE(LOWER(TRIM(p.name)),'  ',' ')
            """)
            copy_query(con, "SELECT * FROM cards_enriched", ENRICH, "PARQUET")
            log(f"[OK] -> {ENRICH}")
        else:
            log("[SKIP] no player_cards_all.(parquet/csv) for enrichment")
    except Exception as e:
        log(f"[SKIP] {e}")

    # 11) 템플릿
    Path(TEMPL).write_text(
        """-- DuckDB quick templates

-- n-gram
-- SELECT * FROM read_parquet('output/summaries/pitch_ngram2.parquet')
-- WHERE season=2025 AND pitcher_id='######' ORDER BY n DESC LIMIT 20;

-- run-length
-- SELECT run_len, COUNT(*) c FROM read_parquet('output/summaries/pitch_runlength.parquet')
-- WHERE season=2025 AND pitcher_id='######' GROUP BY 1 ORDER BY 1;

-- count transition (if available)
-- SELECT * FROM read_parquet('output/summaries/pitch_transition.parquet')
-- WHERE season=2025 AND balls=1 AND strikes=2 ORDER BY n DESC LIMIT 30;

-- team-season (if available)
-- SELECT * FROM read_parquet('output/summaries/team_season_agg.parquet')
-- WHERE season=2025 ORDER BY usage_entropy DESC LIMIT 10;
""",
        encoding="utf-8",
    )
    log(f"[OK] templates -> {TEMPL}")
    con.close(); gc.collect()
    print("✅ expand_end2end completed (always exit 0)")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"[SKIP] top-level: {e}")
        print("⚠️ Completed with warnings; check logs:", LOGF)
        sys.exit(0)
