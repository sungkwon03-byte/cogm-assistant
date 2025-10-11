#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, datetime as dt, duckdb, pandas as pd

ROOT="/workspaces/cogm-assistant"
OUT=f"{ROOT}/output"; SUM=f"{OUT}/summaries"; REP=f"{OUT}/reports"
LOG=f"{ROOT}/logs/full_system_patch_and_expand.log"
os.makedirs(SUM, exist_ok=True); os.makedirs(os.path.dirname(LOG), exist_ok=True)

def log(m):
    ts=dt.datetime.now(dt.timezone.utc).isoformat()
    print(f"[{ts}] {m}")
    with open(LOG,"a",encoding="utf-8") as f: f.write(f"[{ts}] {m}\n")

def prefer_master():
    for p in [f"{OUT}/statcast_ultra_full_clean.parquet", f"{OUT}/statcast_ultra_full.parquet"]:
        if os.path.isfile(p): return p
    return None

def have_column(con, parq, col):
    try:
        con.execute(f"SELECT {col} FROM read_parquet(?) LIMIT 0",[parq]); return True
    except: return False

def safe_copy(con, sql, path, fmt):
    try:
        if fmt.upper()=="CSV":
            con.execute(f"COPY ({sql}) TO '{path}' (FORMAT CSV, HEADER TRUE)")
        else:
            con.execute(f"COPY ({sql}) TO '{path}' (FORMAT PARQUET)")
        return True
    except Exception as e:
        log(f"[SKIP] copy {os.path.basename(path)}: {e}"); return False

def main():
    open(LOG,"w").write(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] start\n")
    con=duckdb.connect(); con.execute("PRAGMA threads=4"); con.execute("PRAGMA memory_limit='1024MB'")
    parq=prefer_master()
    if not parq: log("❌ master missing"); return

    con.execute("""
      CREATE OR REPLACE VIEW sc AS
      SELECT
        CAST(year AS INT) AS season,
        COALESCE(CAST(pitcher AS VARCHAR), CAST(mlb_id AS VARCHAR), CAST(mlbam AS VARCHAR)) AS pitcher_id,
        COALESCE(CAST(batter  AS VARCHAR), CAST(mlbam  AS VARCHAR), CAST(mlb_id AS VARCHAR)) AS batter,
        COALESCE(pitch_type,'') AS pitch_type,
        TRY_CAST(EV AS DOUBLE) AS ev,
        TRY_CAST(LA AS DOUBLE) AS la,
        events, game_pk,
        ROW_NUMBER() OVER () AS _tb,
        *
      FROM read_parquet(?)
      WHERE season BETWEEN 1901 AND 2100
    """,[parq])

    # 1) Pitcher-season summary
    log("[RUN] pitcher-season summary")
    con.execute("""
      CREATE OR REPLACE TABLE ps AS
      WITH base AS (SELECT season, pitcher_id, pitch_type FROM sc WHERE pitch_type<>'' ),
      u AS (SELECT season,pitcher_id,pitch_type,COUNT(*) n FROM base GROUP BY 1,2,3),
      t AS (SELECT season,pitcher_id,SUM(n) tot FROM u GROUP BY 1,2),
      j AS (SELECT u.*,t.tot,(u.n*1.0)/NULLIF(t.tot,0) usage_rate FROM u JOIN t USING(season,pitcher_id)),
      r AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY season,pitcher_id ORDER BY usage_rate DESC,n DESC) rk
        FROM j
      ),
      top1 AS (
        SELECT season,pitcher_id,
               MAX(CASE WHEN rk=1 THEN pitch_type END) AS dominant_pitch,
               MAX(CASE WHEN rk=1 THEN usage_rate END) AS dominant_usage
        FROM r GROUP BY 1,2
      ),
      ent AS (
        SELECT season,pitcher_id, -SUM(CASE WHEN usage_rate>0 THEN usage_rate*ln(usage_rate) ELSE 0 END) AS usage_entropy
        FROM j GROUP BY 1,2
      ),
      rpt AS (
        WITH w AS (
          SELECT season,pitcher_id,pitch_type,
                 LAG(pitch_type) OVER (PARTITION BY season,pitcher_id ORDER BY season, game_pk, _tb) prev_pt
          FROM sc WHERE pitch_type<>''
        ),
        a AS (
          SELECT season,pitcher_id,(prev_pt=pitch_type) is_repeat, COUNT(*) n
          FROM w WHERE prev_pt IS NOT NULL GROUP BY 1,2,3
        )
        SELECT season,pitcher_id,
               SUM(CASE WHEN is_repeat THEN n ELSE 0 END)*1.0/NULLIF(SUM(n),0) repeat_rate
        FROM a GROUP BY 1,2
      )
      SELECT COALESCE(top1.season,ent.season,rpt.season) season,
             COALESCE(top1.pitcher_id,ent.pitcher_id,rpt.pitcher_id) pitcher_id,
             top1.dominant_pitch, top1.dominant_usage, ent.usage_entropy, rpt.repeat_rate
      FROM top1
      FULL OUTER JOIN ent USING(season,pitcher_id)
      FULL OUTER JOIN rpt USING(season,pitcher_id)
    """)
    safe_copy(con,"SELECT * FROM ps",f"{SUM}/pitcher_season_summary.parquet","PARQUET")

    # 2) n-gram(2,3)
    log("[RUN] n-gram(2,3)")
    con.execute("""
      CREATE OR REPLACE TABLE ngram2 AS
      WITH w AS (
        SELECT season,pitcher_id,
               LAG(pitch_type) OVER (PARTITION BY season,pitcher_id ORDER BY season, game_pk, _tb) p1,
               pitch_type p2
        FROM sc WHERE pitch_type<>''
      )
      SELECT season,pitcher_id,p1,p2,COUNT(*) n FROM w WHERE p1 IS NOT NULL GROUP BY 1,2,3,4
    """)
    safe_copy(con,"SELECT * FROM ngram2",f"{SUM}/pitch_ngram2.parquet","PARQUET")
    con.execute("""
      CREATE OR REPLACE TABLE ngram3 AS
      WITH w AS (
        SELECT season,pitcher_id,
               LAG(pitch_type,2) OVER (PARTITION BY season,pitcher_id ORDER BY season, game_pk, _tb) p1,
               LAG(pitch_type,1) OVER (PARTITION BY season,pitcher_id ORDER BY season, game_pk, _tb) p2,
               pitch_type p3
        FROM sc WHERE pitch_type<>''
      )
      SELECT season,pitcher_id,p1,p2,p3,COUNT(*) n
      FROM w WHERE p1 IS NOT NULL AND p2 IS NOT NULL GROUP BY 1,2,3,4,5
    """)
    safe_copy(con,"SELECT * FROM ngram3",f"{SUM}/pitch_ngram3.parquet","PARQUET")

    # 3) run-length
    log("[RUN] run-length")
    try:
        con.execute("""
          CREATE OR REPLACE TABLE runlen AS
          WITH w AS (
            SELECT season,pitcher_id,game_pk,pitch_type,
                   LAG(pitch_type) OVER (PARTITION BY season,pitcher_id ORDER BY season, game_pk, _tb) prev_pt
            FROM sc WHERE pitch_type<>''
          ),
          g AS (
            SELECT season,pitcher_id,game_pk,pitch_type,
                   (pitch_type IS DISTINCT FROM prev_pt) is_new,
                   SUM(CASE WHEN pitch_type IS DISTINCT FROM prev_pt THEN 1 ELSE 0 END)
                     OVER (PARTITION BY season,pitcher_id ORDER BY season, game_pk, _tb) grp
            FROM w
          )
          SELECT season,pitcher_id,pitch_type,grp,COUNT(*) run_len FROM g GROUP BY 1,2,3,4
        """)
        safe_copy(con,"SELECT * FROM runlen",f"{SUM}/pitch_run_length.parquet","PARQUET")
    except Exception as e:
        log(f"[SKIP] run-length: {e}")

    # 4) count-based transition
    log("[RUN] count-based transition")
    if all(have_column(con, parq, c) for c in ["balls","strikes"]):
        try:
            con.execute("""
              CREATE OR REPLACE TABLE count_trans AS
              WITH w AS (
                SELECT season,pitcher_id, CAST(balls AS INT) b, CAST(strikes AS INT) s,
                       LAG(pitch_type) OVER (PARTITION BY season,pitcher_id ORDER BY season, game_pk, _tb) prev_pt,
                       pitch_type cur_pt
                FROM sc WHERE pitch_type<>''
              )
              SELECT season,pitcher_id,b,s,prev_pt,cur_pt,COUNT(*) n
              FROM w WHERE prev_pt IS NOT NULL GROUP BY 1,2,3,4,5,6
            """)
            safe_copy(con,"SELECT * FROM count_trans",f"{SUM}/count_transition.parquet","PARQUET")
        except Exception as e:
            log(f"[SKIP] count-based: {e}")
    else:
        log("[SKIP] balls/strikes missing")

    # 5) zone transition
    log("[RUN] zone repeat/transition")
    if all(have_column(con, parq, c) for c in ["edge","heart","chase"]):
        con.execute("""
          CREATE OR REPLACE TABLE zone_trans AS
          WITH z AS (
            SELECT season,pitcher_id,
                   CASE WHEN heart IS NOT NULL AND heart>0 THEN 'HEART'
                        WHEN edge  IS NOT NULL AND edge>0  THEN 'EDGE'
                        WHEN chase IS NOT NULL AND chase>0 THEN 'CHASE'
                        ELSE 'OTHER' END AS zone,
                   LAG(CASE WHEN heart IS NOT NULL AND heart>0 THEN 'HEART'
                            WHEN edge  IS NOT NULL AND edge>0  THEN 'EDGE'
                            WHEN chase IS NOT NULL AND chase>0 THEN 'CHASE'
                            ELSE 'OTHER' END)
                     OVER (PARTITION BY season,pitcher_id ORDER BY season, game_pk, _tb) AS prev_zone
            FROM sc
          )
          SELECT season,pitcher_id, prev_zone, zone, COUNT(*) n
          FROM z WHERE prev_zone IS NOT NULL GROUP BY 1,2,3,4
        """)
        safe_copy(con,"SELECT * FROM zone_trans",f"{SUM}/zone_repeat_transition.parquet","PARQUET")
    else:
        log("[SKIP] zone columns missing")

    # 6) batter LA/EV variability
    log("[RUN] batter LA/EV")
    try:
        con.execute("""
          CREATE OR REPLACE TABLE batter_la_ev AS
          WITH b AS (
            SELECT season,batter,game_pk, TRY_CAST(la AS DOUBLE) la, TRY_CAST(ev AS DOUBLE) ev, _tb
            FROM sc WHERE batter IS NOT NULL
          ),
          w AS (
            SELECT *, LAG(la) OVER (PARTITION BY season,batter ORDER BY season, game_pk, _tb) lag_la FROM b
          )
          SELECT season,batter, AVG(la) la_mean, STDDEV(la) la_std, AVG(ABS(la - lag_la)) la_abs_diff_mean, AVG(ev) ev_mean
          FROM w GROUP BY 1,2
        """)
        safe_copy(con,"SELECT * FROM batter_la_ev",f"{SUM}/batter_la_ev_variability.parquet","PARQUET")
    except Exception as e:
        log(f"[SKIP] batter LA/EV: {e}")

    # 7) leaderboards 3종
    log("[RUN] leaderboards")
    safe_copy(con, "SELECT season,pitcher_id,dominant_pitch,usage_entropy, ROW_NUMBER() OVER (PARTITION BY season ORDER BY usage_entropy DESC NULLS LAST) rk FROM ps WHERE usage_entropy IS NOT NULL QUALIFY rk<=10", f"{SUM}/leaderboard_entropy_top10.csv","CSV")
    safe_copy(con, "SELECT season,pitcher_id,dominant_pitch,repeat_rate,  ROW_NUMBER() OVER (PARTITION BY season ORDER BY repeat_rate DESC NULLS LAST) rk  FROM ps WHERE repeat_rate IS NOT NULL QUALIFY rk<=10", f"{SUM}/leaderboard_repeat_high_top10.csv","CSV")
    safe_copy(con, "SELECT season,pitcher_id,dominant_pitch,repeat_rate,  ROW_NUMBER() OVER (PARTITION BY season ORDER BY repeat_rate ASC  NULLS LAST) rk  FROM ps WHERE repeat_rate IS NOT NULL QUALIFY rk<=10", f"{SUM}/leaderboard_repeat_low_top10.csv","CSV")

    # 8) 2025 pitcher features
    log("[RUN] 2025 features")
    safe_copy(con,"SELECT * FROM ps WHERE season=2025",f"{SUM}/statcast_features_pitcher_2025.csv","CSV")

    # 9) 카드 병합(있으면)
    log("[RUN] cards enrichment")
    cards_parq=f"{OUT}/player_cards_allparquet"; cards_csv=f"{OUT}/player_cards_all.csv"
    if os.path.isfile(cards_parq) or os.path.isfile(cards_csv):
        df=pd.read_parquet(cards_parq) if os.path.isfile(cards_parq) else pd.read_csv(cards_csv)
        df["pitcher_id"]=df.get("player_uid", "").astype(str)
        ps_df=duckdb.connect().execute("SELECT * FROM ps").fetchdf(); ps_df["pitcher_id"]=ps_df["pitcher_id"].astype(str)
        if "season" in df.columns:
            m=df.merge(ps_df[["season","pitcher_id","dominant_pitch","usage_entropy","repeat_rate"]], on=["season","pitcher_id"], how="left")
        else:
            m=df
        out_csv=f"{OUT}/player_cards_enriched_all_seq.csv"
        try: m.to_csv(out_csv,index=False); log(f"[OK] enriched -> {out_csv}")
        except Exception as e: log(f"[SKIP] enriched CSV: {e}")
    else:
        log("[SKIP] no cards found")

    log("[DONE] patch+expand complete")

if __name__=="__main__":
    try: main()
    finally:
        import sys; sys.exit(0)
