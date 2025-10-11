import os, sys, datetime as dt
import duckdb

LOG = "logs/statcast_seq_features.log"
os.makedirs("logs", exist_ok=True)
os.makedirs("output/summaries", exist_ok=True)

def log(msg):
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {msg}\n")
    print(msg)

PARQ = "output/statcast_ultra_full.parquet"              # v4.3 마스터
SUMD = "output/summaries"                                # 요약 출력 폴더
CARDS_ALL = "output/player_cards_all.csv"                # 전시즌 카드 합본
ENRICH_OUT = "output/player_cards_enriched_all_seq.csv"  # 최종 병합 산출

def exists(p):
    try: return os.path.exists(p)
    except: return False

def safe_copy_sql(con, sql, out_path, fmt="PARQUET"):
    out_path = os.path.abspath(out_path)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    p = out_path.replace("'", "''")
    con.execute(f"COPY ({sql}) TO '{p}' (FORMAT {fmt});")

def main():
    open(LOG, "w").write(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] seq features start\n")
    if not exists(PARQ):
        log(f"❌ missing {PARQ} — 먼저 build_statcast_ultra_v4_3.py 실행 필요")
        return

    con = duckdb.connect()
    con.execute(f"CREATE VIEW sc_raw AS SELECT * FROM read_parquet('{PARQ}');")
    con.execute("""
        CREATE OR REPLACE VIEW sc AS
        SELECT
            CAST(year AS INT) AS season,
            COALESCE(CAST(pitcher AS VARCHAR), CAST(mlb_id AS VARCHAR)) AS pitcher_id,
            COALESCE(CAST(batter  AS VARCHAR), CAST(mlbam  AS VARCHAR)) AS batter_id,
            player_name,
            COALESCE(pitch_type,'UNK') AS pitch_type,
            TRY_CAST(velo  AS DOUBLE) AS velo,
            TRY_CAST(spin  AS DOUBLE) AS spin,
            TRY_CAST(pfx_x AS DOUBLE) AS pfx_x,
            TRY_CAST(pfx_z AS DOUBLE) AS pfx_z,
            TRY_CAST(ext   AS DOUBLE) AS extension,
            TRY_CAST(EV    AS DOUBLE) AS ev,
            TRY_CAST(LA    AS DOUBLE) AS la,
            COALESCE(events,'') AS events,
            COALESCE(game_pk, 0) AS game_pk
        FROM sc_raw
        WHERE CAST(year AS INT) BETWEEN 1900 AND 2100
          AND pitcher IS NOT NULL
    """)

    log("[RUN] sequence-level features (lag/delta)")
    con.execute("""
        CREATE OR REPLACE TABLE tmp_seq_pitch AS
        WITH base AS (
          SELECT season, pitcher_id, batter_id, game_pk, pitch_type, velo, spin, pfx_x, pfx_z, extension, ev, la
          FROM sc
        ),
        w AS (
          SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY season, pitcher_id ORDER BY season, game_pk, batter_id) AS rn,
            LAG(pitch_type) OVER (PARTITION BY season, pitcher_id ORDER BY season, game_pk, batter_id) AS lag_pitch_type,
            LAG(velo)       OVER (PARTITION BY season, pitcher_id ORDER BY season, game_pk, batter_id) AS lag_velo,
            LAG(spin)       OVER (PARTITION BY season, pitcher_id ORDER BY season, game_pk, batter_id) AS lag_spin,
            LAG(pfx_x)      OVER (PARTITION BY season, pitcher_id ORDER BY season, game_pk, batter_id) AS lag_pfx_x,
            LAG(pfx_z)      OVER (PARTITION BY season, pitcher_id ORDER BY season, game_pk, batter_id) AS lag_pfx_z,
            LAG(extension)  OVER (PARTITION BY season, pitcher_id ORDER BY season, game_pk, batter_id) AS lag_ext,
            LAG(la)         OVER (PARTITION BY season, batter_id  ORDER BY season, game_pk, pitcher_id) AS lag_la_batter
          FROM base
        )
        SELECT
          season, pitcher_id, batter_id, game_pk, pitch_type, lag_pitch_type,
          (pitch_type = lag_pitch_type) AS repeat_pitch,
          velo, spin, pfx_x, pfx_z, extension,
          (velo - lag_velo)     AS d_velo,
          (spin - lag_spin)     AS d_spin,
          (pfx_x - lag_pfx_x)   AS d_mov_x,
          (pfx_z - lag_pfx_z)   AS d_mov_z,
          ev, la, lag_la_batter,
          ABS(la - lag_la_batter) AS d_la_batter
        FROM w
    """)
    safe_copy_sql(con, "SELECT * FROM tmp_seq_pitch", f"{SUMD}/seq_pitch_features.parquet", "PARQUET")
    log("[OK] seq_pitch_features.parquet")

    log("[RUN] pitcher-season transition matrix")
    con.execute("""
        CREATE OR REPLACE TABLE tmp_trans AS
        WITH w AS (
          SELECT season, pitcher_id, game_pk, pitch_type,
                 LAG(pitch_type) OVER (PARTITION BY season, pitcher_id ORDER BY season, game_pk) AS prev_pt
          FROM sc
        ),
        pairs AS (
          SELECT season, pitcher_id, prev_pt, pitch_type AS cur_pt
          FROM w WHERE prev_pt IS NOT NULL
        ),
        agg AS (
          SELECT season, pitcher_id, prev_pt, cur_pt, COUNT(*) AS cnt
          FROM pairs
          GROUP BY 1,2,3,4
        ),
        tot AS (
          SELECT season, pitcher_id, SUM(cnt) AS total_cnt,
                 SUM(CASE WHEN prev_pt = cur_pt THEN cnt ELSE 0 END) AS repeat_cnt
          FROM agg
          GROUP BY 1,2
        ),
        probs AS (
          SELECT a.*, t.total_cnt,
                 (a.cnt * 1.0) / NULLIF(t.total_cnt,0) AS prob,
                 t.repeat_cnt,
                 (t.repeat_cnt * 1.0) / NULLIF(t.total_cnt,0) AS repeat_rate
          FROM agg a JOIN tot t USING(season, pitcher_id)
        )
        SELECT * FROM probs
    """)
    safe_copy_sql(con, "SELECT * FROM tmp_trans", f"{SUMD}/pitcher_transition_matrix.parquet", "PARQUET")
    log("[OK] pitcher_transition_matrix.parquet")

    log("[RUN] pitcher-season summary (dominant, entropy approx)")
    con.execute("""
        CREATE OR REPLACE TABLE tmp_pitcher_season AS
        WITH pt_usage AS (
          SELECT season, pitcher_id, pitch_type, COUNT(*) AS n
          FROM sc
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
          FROM rankd
          GROUP BY 1,2
        ),
        ent AS (
          SELECT season, pitcher_id,
                 -SUM(CASE WHEN usage_rate>0 THEN usage_rate * ln(usage_rate) ELSE 0 END) AS usage_entropy
          FROM joined
          GROUP BY 1,2
        ),
        rpt AS (
          SELECT season, pitcher_id, AVG(repeat_rate) AS repeat_rate
          FROM (SELECT DISTINCT season, pitcher_id, repeat_rate FROM tmp_trans)
          GROUP BY 1,2
        )
        SELECT
          COALESCE(ps.season, ent.season, rpt.season) AS season,
          COALESCE(ps.pitcher_id, ent.pitcher_id, rpt.pitcher_id) AS pitcher_id,
          ps.dominant_pitch, ps.dominant_usage,
          ent.usage_entropy,
          rpt.repeat_rate
        FROM top1 ps
        FULL OUTER JOIN ent USING (season, pitcher_id)
        FULL OUTER JOIN rpt USING (season, pitcher_id)
    """)
    safe_copy_sql(con, "SELECT * FROM tmp_pitcher_season", f"{SUMD}/pitcher_season_summary.parquet", "PARQUET")
    log("[OK] pitcher_season_summary.parquet")

    log("[RUN] batter-season angle variability")
    con.execute("""
        CREATE OR REPLACE TABLE tmp_batter_la AS
        WITH b AS (
          SELECT season, batter_id, game_pk, la
          FROM sc
          WHERE la IS NOT NULL
        ),
        w AS (
          SELECT b.*,
                 LAG(la) OVER (PARTITION BY season, batter_id ORDER BY season, game_pk) AS lag_la
          FROM b b
        )
        SELECT
          season, batter_id,
          AVG(la)               AS la_mean,
          STDDEV(la)            AS la_std,
          AVG(ABS(la - lag_la)) AS la_abs_diff_mean
        FROM w
        GROUP BY 1,2
    """)
    safe_copy_sql(con, "SELECT * FROM tmp_batter_la", f"{SUMD}/batter_season_angle.parquet", "PARQUET")
    log("[OK] batter_season_angle.parquet")

    # --- 이름 매핑(시즌+이름으로 폴백 매칭용) ---
    con.execute("""
      CREATE OR REPLACE TABLE id_name_pitcher AS
      SELECT season, pitcher_id, MIN(player_name) AS name
      FROM sc
      GROUP BY 1,2;
    """)
    con.execute("""
      CREATE OR REPLACE TABLE id_name_batter AS
      SELECT season, batter_id,  MIN(player_name) AS name
      FROM sc
      GROUP BY 1,2;
    """)
    log("[OK] id-name maps")

    if not exists(CARDS_ALL):
        log(f"⚠️ {CARDS_ALL} 없음 — build_all_player_cards.sh 먼저 실행 필요")
    else:
        log("[RUN] merge into player_cards_enriched_all_seq.csv (season+name 매칭 폴백)")
        sql_merge = f"""
          WITH ps AS (
            SELECT s.*, m.name AS p_name
            FROM read_parquet('{SUMD}/pitcher_season_summary.parquet') s
            LEFT JOIN id_name_pitcher m USING(season, pitcher_id)
          ),
          ba AS (
            SELECT s.*, m.name AS b_name
            FROM read_parquet('{SUMD}/batter_season_angle.parquet') s
            LEFT JOIN id_name_batter m ON s.season=m.season AND s.batter_id=m.batter_id
          )
          SELECT
            c.*,
            ps.dominant_pitch, ps.dominant_usage, ps.usage_entropy, ps.repeat_rate,
            ba.la_mean, ba.la_std, ba.la_abs_diff_mean
          FROM read_csv_auto('{CARDS_ALL}', HEADER=TRUE) c
          LEFT JOIN ps
            ON CAST(c.season AS INT)=ps.season
           AND lower(c.name)=lower(ps.p_name)
          LEFT JOIN ba
            ON CAST(c.season AS INT)=ba.season
           AND lower(c.name)=lower(ba.b_name)
        """
        safe_copy_sql(con, sql_merge, ENRICH_OUT, "CSV")
        log(f"[OK] enriched cards -> {ENRICH_OUT}")

    con.close()
    log("[DONE] seq features pipeline complete")

if __name__ == "__main__":
    try:
        main()
        sys.exit(0)  # 항상 0으로 종료
    except Exception as e:
        with open(LOG, "a") as f: f.write(f"❌ ERROR: {e}\n")
        print(f"❌ ERROR: {e}")
        sys.exit(0)  # 에러여도 0 종료
