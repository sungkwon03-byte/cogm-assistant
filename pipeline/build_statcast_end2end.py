import os, sys, glob, gc, datetime as dt
os.environ.setdefault("MPLBACKEND","Agg")
import duckdb

# -------------------- 설정값 --------------------
LOG       = "logs/statcast_end2end.log"
OUT       = "output"
REP       = "output/reports"
SUM       = "output/summaries"
PARQ      = f"{OUT}/statcast_ultra_full.parquet"      # 마스터 출력(존재 시 재사용)
CARDS_ALL = f"{OUT}/player_cards_all.csv"             # 전시즌 카드 합본
ENRICH_OUT= f"{OUT}/player_cards_enriched_all_seq.csv"

CAND = [
    "/workspaces/cogm-assistant/output/cache/statcast_clean",
    "/workspaces/cogm-assistant/output",
    "/workspaces/cogm-assistant"
]
Y_FROM = int(os.environ.get("SC_FROM", "2015"))
Y_TO   = int(os.environ.get("SC_TO",   "2025"))

os.makedirs("logs", exist_ok=True)
os.makedirs(OUT, exist_ok=True)
os.makedirs(REP, exist_ok=True)
os.makedirs(SUM, exist_ok=True)

def log(msg):
    ts = dt.datetime.now(dt.timezone.utc).isoformat()
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")
    print(msg, flush=True)

def q(p: str) -> str:
    return p.replace("'", "''")

def find_files():
    exts = ("parquet","csv")
    pats = []
    for root in CAND:
        for y in range(Y_FROM, Y_TO+1):
            pats += [
                os.path.join(root, f"{y}", f"{y}-*", "**", f"*.{ext}") for ext in exts
            ] + [
                os.path.join(root, f"{y}", "**", f"*.{ext}") for ext in exts
            ] + [
                os.path.join(root, f"statcast*{y}*.{ext}") for ext in exts
            ] + [
                os.path.join(root, f"*statcast*{y}*.{ext}") for ext in exts
            ] + [
                os.path.join(root, f"statcast_*_player_year.{ext}") for ext in exts
            ]
    files = []
    for p in pats:
        files.extend(glob.glob(p, recursive=True))
    return sorted(set(files))

def build_master_if_needed():
    if os.path.exists(PARQ):
        log(f"[SKIP] master exists: {PARQ}")
        return
    files = find_files()
    parq = [f for f in files if f.lower().endswith(".parquet")]
    csv  = [f for f in files if f.lower().endswith(".csv")]
    if not parq and not csv:
        log("⚠️ statcast 소스 없음(캐시 연결 확인)")
        con = duckdb.connect()
        con.execute("CREATE TABLE _empty(dummy INTEGER)")
        con.execute(f"COPY (SELECT * FROM _empty WHERE 1=0) TO '{q(PARQ)}' (FORMAT PARQUET)")
        con.close()
        return
    log(f"[FILES] parq={len(parq)} csv={len(csv)}")
    con = duckdb.connect()
    con.execute("PRAGMA disable_progress_bar")
    con.execute("PRAGMA threads="+str(os.cpu_count() or 4))
    if parq:
        con.execute("CREATE TEMP TABLE parq_files(path TEXT)")
        con.executemany("INSERT INTO parq_files VALUES (?)", [(p,) for p in parq])
        con.execute("CREATE TEMP VIEW _parq AS SELECT rp.* FROM parq_files pf, read_parquet(pf.path) rp")
    if csv:
        con.execute("CREATE TEMP TABLE csv_files(path TEXT)")
        con.executemany("INSERT INTO csv_files VALUES (?)", [(p,) for p in csv])
        con.execute("CREATE TEMP VIEW _csv AS SELECT rc.* FROM csv_files cf, read_csv_auto(cf.path, HEADER=TRUE) rc")
    parts = []
    if parq: parts.append("SELECT * FROM _parq")
    if csv : parts.append("SELECT * FROM _csv")
    con.execute(f"COPY ({' UNION ALL '.join(parts)}) TO '{q(PARQ)}' (FORMAT PARQUET)")
    con.close()
    log(f"[OK] master -> {PARQ}")

def create_people_view(con):
    chad = "data/chadwick/people.csv"
    lahm = "data/lahman/People.csv"
    src = None
    if os.path.exists(chad):
        src = chad
        log("[MAP] using Chadwick people.csv")
    elif os.path.exists(lahm):
        src = lahm
        log("[MAP] using Lahman People.csv")
    else:
        log("⚠️ no external people file; creating empty view")
        con.execute("CREATE OR REPLACE VIEW vw_map AS SELECT NULL::VARCHAR retro, NULL::VARCHAR mlbam, NULL::VARCHAR name LIMIT 0")
        return

    cols = set(con.execute(f"SELECT * FROM read_csv_auto('{q(src)}', HEADER=TRUE) LIMIT 0").fetchdf().columns)

    # 후보 열 결정
    retro_cols = [c for c in ["retroID","key_retro","retro_id","key_retro_id"] if c in cols]
    mlbam_cols = [c for c in ["key_mlbam","mlbam","key_mlbam_id","mlbam_id"] if c in cols]
    first_cols = [c for c in ["nameFirst","name_first","name_first_ascii","first_name"] if c in cols]
    last_cols  = [c for c in ["nameLast","name_last","name_last_ascii","last_name"] if c in cols]

    retro_expr = retro_cols[0] if retro_cols else "NULL"
    mlbam_expr = mlbam_cols[0] if mlbam_cols else "NULL"
    first_expr = first_cols[0] if first_cols else "NULL"
    last_expr  = last_cols[0]  if last_cols  else "NULL"

    # 이름 만들기(둘 다 없으면 NULL)
    name_expr = f"NULLIF(TRIM(COALESCE({first_expr},'') || ' ' || COALESCE({last_expr},'')),'')"

    sql = f"""
        CREATE OR REPLACE VIEW vw_map AS
        SELECT
          {retro_expr}::VARCHAR AS retro,
          {mlbam_expr}::VARCHAR AS mlbam,
          {name_expr}          AS name
        FROM read_csv_auto('{q(src)}', HEADER=TRUE)
    """
    con.execute(sql)

def build_seq_transition_and_summaries():
    con = duckdb.connect()
    con.execute("PRAGMA disable_progress_bar")
    con.execute("PRAGMA threads="+str(os.cpu_count() or 4))

    con.execute(f"CREATE VIEW sc_raw AS SELECT * FROM read_parquet('{q(PARQ)}')")
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

    log("[RUN] sequence-level features")
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
    con.execute(f"COPY (SELECT * FROM tmp_seq_pitch) TO '{q(os.path.join(SUM,'seq_pitch_features.parquet'))}' (FORMAT PARQUET)")
    log("[OK] seq_pitch_features.parquet")

    log("[RUN] pitcher-season transition matrix")
    con.execute("""
        CREATE OR REPLACE TABLE tmp_trans AS
        WITH w AS (
          SELECT season, pitcher_id, game_pk, pitch_type,
                 LAG(pitch_type) OVER (PARTITION BY season, pitcher_id ORDER BY season, game_pk) AS prev_pt
          FROM sc
        ),
        pairs AS ( SELECT season, pitcher_id, prev_pt, pitch_type AS cur_pt FROM w WHERE prev_pt IS NOT NULL ),
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
        )
        SELECT a.*, t.total_cnt,
               (a.cnt * 1.0) / NULLIF(t.total_cnt,0) AS prob,
               t.repeat_cnt,
               (t.repeat_cnt * 1.0) / NULLIF(t.total_cnt,0) AS repeat_rate
        FROM agg a JOIN tot t USING(season, pitcher_id)
    """)
    con.execute(f"COPY (SELECT * FROM tmp_trans) TO '{q(os.path.join(SUM,'pitcher_transition_matrix.parquet'))}' (FORMAT PARQUET)")
    log("[OK] pitcher_transition_matrix.parquet")

    log("[RUN] pitcher-season summary")
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
    con.execute(f"COPY (SELECT * FROM tmp_pitcher_season) TO '{q(os.path.join(SUM,'pitcher_season_summary.parquet'))}' (FORMAT PARQUET)")
    log("[OK] pitcher_season_summary.parquet")

    log("[RUN] batter-season angle variability")
    con.execute("""
        CREATE OR REPLACE TABLE tmp_batter_la AS
        WITH b AS ( SELECT season, batter_id, game_pk, la FROM sc WHERE la IS NOT NULL ),
        w AS (
          SELECT b.*, LAG(la) OVER (PARTITION BY season, batter_id ORDER BY season, game_pk) AS lag_la
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
    con.execute(f"COPY (SELECT * FROM tmp_batter_la) TO '{q(os.path.join(SUM,'batter_season_angle.parquet'))}' (FORMAT PARQUET)")
    log("[OK] batter_season_angle.parquet")

    # id-name 맵 & 외부 people 맵
    con.execute("CREATE OR REPLACE TABLE id_name_pitcher AS SELECT season, pitcher_id, MIN(player_name) AS name FROM sc GROUP BY 1,2")
    con.execute("CREATE OR REPLACE TABLE id_name_batter  AS SELECT season, batter_id,  MIN(player_name) AS name FROM sc GROUP BY 1,2")
    log("[OK] id-name maps")
    create_people_view(con)

    # 카드 병합(있을 때만)
    if os.path.exists(CARDS_ALL):
        log("[RUN] merge into enriched cards (ID map + name fallback)")
        con.execute(f"CREATE OR REPLACE TABLE tmp_cards AS SELECT * FROM read_csv_auto('{q(CARDS_ALL)}', HEADER=TRUE)")
        con.execute("""
          CREATE OR REPLACE TABLE tmp_cards_idmap AS
          SELECT c.*, m.mlbam AS mlbam_map
          FROM tmp_cards c LEFT JOIN vw_map m
          ON lower(c.player_uid) = lower(m.retro)
        """)
        con.execute("""
          CREATE OR REPLACE TABLE tmp_cards_pitch AS
          WITH ps AS (
            SELECT s.*, p.name AS p_name
            FROM read_parquet('output/summaries/pitcher_season_summary.parquet') s
            LEFT JOIN id_name_pitcher p USING(season, pitcher_id)
          )
          SELECT c.*, ps.dominant_pitch, ps.dominant_usage, ps.usage_entropy, ps.repeat_rate
          FROM tmp_cards_idmap c LEFT JOIN ps
            ON CAST(c.season AS INT) = ps.season
           AND ( ps.pitcher_id = CAST(c.mlbam_map AS VARCHAR)
              OR lower(ps.p_name) = lower(c.name) )
        """)
        con.execute("""
          CREATE OR REPLACE TABLE tmp_cards_full AS
          WITH ba AS (
            SELECT s.*, b.name AS b_name
            FROM read_parquet('output/summaries/batter_season_angle.parquet') s
            LEFT JOIN id_name_batter b ON s.season=b.season AND s.batter_id=b.batter_id
          )
          SELECT p.*, ba.la_mean, ba.la_std, ba.la_abs_diff_mean
          FROM tmp_cards_pitch p LEFT JOIN ba
            ON CAST(p.season AS INT) = ba.season
           AND lower(ba.b_name) = lower(p.name)
        """)
        con.execute(f"COPY (SELECT * FROM tmp_cards_full) TO '{q(ENRICH_OUT)}' (FORMAT CSV, HEADER=TRUE)")
        log(f"[OK] enriched cards -> {ENRICH_OUT}")
    else:
        log(f"⚠️ {CARDS_ALL} not found; skip card enrichment")

    cov = con.execute("SELECT MIN(season) AS min_y, MAX(season) AS max_y, COUNT(*) AS n_distinct_seasons FROM (SELECT DISTINCT season FROM sc)").fetchdf()
    log(f"[COVERAGE] {cov.to_dict(orient='records')}")
    has_2025 = con.execute("SELECT COUNT(*)>0 FROM sc WHERE season=2025").fetchone()[0]
    log("[OK] 2025 season detected" if has_2025 else "⚠️ 2025 season not detected")

    con.close()
    gc.collect()

def main():
    open(LOG, "w").write(f"[{dt.datetime.now(dt.timezone.utc)}] end2end start\n")
    build_master_if_needed()
    build_seq_transition_and_summaries()
    log("[DONE] end2end pipeline complete")

if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        log(f"❌ ERROR: {e}")
        sys.exit(0)
