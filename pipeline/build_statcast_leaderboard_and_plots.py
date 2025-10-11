#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, json, datetime as dt, gc
os.environ.setdefault("MPLBACKEND","Agg")
import duckdb
import matplotlib.pyplot as plt

ROOT = "/workspaces/cogm-assistant"
OUT  = f"{ROOT}/output"
SUM  = f"{OUT}/summaries"
REP  = f"{OUT}/reports"
LOGF = f"{ROOT}/logs/statcast_leaderboard_plots.log"
PARQ = f"{OUT}/statcast_ultra_full.parquet"   # v4.3 마스터
# 산출물
LB_ENT = f"{SUM}/leaderboard_entropy_top10.csv"
LB_RPT_LOW  = f"{SUM}/leaderboard_repeat_low_top10.csv"   # 가장 다양한 투구(낮은 repeat_rate)
LB_RPT_HIGH = f"{SUM}/leaderboard_repeat_high_top10.csv"  # 가장 반복적인 투구(높은 repeat_rate)
TREND_CSV   = f"{SUM}/trend_entropy_repeat_by_year.csv"
PLOT_TREND  = f"{REP}/trend_entropy_repeat_by_year.png"

def log(msg):
    ts = dt.datetime.now(dt.timezone.utc).isoformat()
    os.makedirs(os.path.dirname(LOGF), exist_ok=True)
    with open(LOGF, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")
    print(msg)

def safe_rm(path):
    try:
        if os.path.isfile(path):
            os.remove(path)
    except Exception:
        pass

def main():
    os.makedirs(OUT, exist_ok=True)
    os.makedirs(SUM, exist_ok=True)
    os.makedirs(REP, exist_ok=True)
    log("[START] leaderboard+plots")

    # DuckDB 연결 (리소스 절제)
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.execute("PRAGMA memory_limit='1024MB'")

    # 마스터 체크
    if not os.path.isfile(PARQ):
        log(f"❌ missing {PARQ} — 먼저 build_statcast_ultra_v4_3.py 로 마스터를 만들어줘")
        return 0

    # 원천 뷰
    con.execute(f"""
        CREATE OR REPLACE VIEW sc_raw AS
        SELECT * FROM read_parquet('{PARQ}');
    """)

    # 가용 컬럼 점검 및 표준화된 뷰 생성
    # pitcher_id: pitcher/ mlb_id/ mlbam 중 존재 컬럼 우선
    cols = con.execute("PRAGMA show_tables").fetchdf()
    # 이름 컬럼이 혼재할 수 있어 player_name이 없으면 빈 문자열
    con.execute("""
        CREATE OR REPLACE VIEW sc AS
        SELECT
            CAST(year AS INT) AS season,
            COALESCE(CAST(pitcher AS VARCHAR),
                     CAST(mlb_id AS VARCHAR),
                     CAST(mlbam AS VARCHAR)) AS pitcher_id,
            COALESCE(player_name, '') AS player_name,
            pitch_type
        FROM sc_raw
        WHERE season BETWEEN 1900 AND 2100
    """)

    # 2025 커버리지/최소·최대 시즌 기록
    cov = con.execute("""
        SELECT COUNT(*) AS rows, MIN(season) AS min_y, MAX(season) AS max_y
        FROM sc
    """).fetchdf()
    rows = int(cov.iloc[0]["rows"])
    min_y = int(cov.iloc[0]["min_y"]) if rows else None
    max_y = int(cov.iloc[0]["max_y"]) if rows else None
    log(f"[COVERAGE] rows={rows:,} season_range={min_y}–{max_y}")

    # ─────────────────────────────────────────────────────────────────
    # Pitcher-season summary (dominant_pitch, usage_entropy, repeat_rate)
    # ─────────────────────────────────────────────────────────────────
    log("[RUN] compute pitcher-season summary")

    con.execute("""
        -- pitch type usage by pitcher-season
        CREATE OR REPLACE TEMP VIEW pt_usage AS
        SELECT season, pitcher_id, pitch_type, COUNT(*) AS n
        FROM sc
        WHERE pitch_type IS NOT NULL
        GROUP BY 1,2,3;

        CREATE OR REPLACE TEMP VIEW pt_tot AS
        SELECT season, pitcher_id, SUM(n) AS tot
        FROM pt_usage
        GROUP BY 1,2;

        CREATE OR REPLACE TEMP VIEW pt_join AS
        SELECT u.season, u.pitcher_id, u.pitch_type, u.n, t.tot,
               (u.n * 1.0) / NULLIF(t.tot,0) AS usage_rate
        FROM pt_usage u
        JOIN pt_tot t USING(season, pitcher_id);

        CREATE OR REPLACE TEMP VIEW pt_rank AS
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY season, pitcher_id
                                  ORDER BY usage_rate DESC, n DESC) AS rk
        FROM pt_join;

        CREATE OR REPLACE TEMP VIEW top1 AS
        SELECT season, pitcher_id,
               MAX(CASE WHEN rk=1 THEN pitch_type  END) AS dominant_pitch,
               MAX(CASE WHEN rk=1 THEN usage_rate  END) AS dominant_usage
        FROM pt_rank
        GROUP BY 1,2;

        CREATE OR REPLACE TEMP VIEW ent AS
        SELECT season, pitcher_id,
               -SUM(CASE WHEN usage_rate>0 THEN usage_rate * ln(usage_rate) ELSE 0 END) AS usage_entropy
        FROM pt_join
        GROUP BY 1,2;

        -- repeat rate: 이전 구종과 동일 비율
        CREATE OR REPLACE TEMP VIEW w AS
        SELECT season, pitcher_id, pitch_type,
               LAG(pitch_type) OVER (PARTITION BY season, pitcher_id ORDER BY season) AS prev_pt
        FROM sc
        WHERE pitch_type IS NOT NULL;

        CREATE OR REPLACE TEMP VIEW pairs AS
        SELECT season, pitcher_id, prev_pt, pitch_type AS cur_pt
        FROM w WHERE prev_pt IS NOT NULL;

        CREATE OR REPLACE TEMP VIEW agg AS
        SELECT season, pitcher_id, prev_pt, cur_pt, COUNT(*) AS cnt
        FROM pairs
        GROUP BY 1,2,3,4;

        CREATE OR REPLACE TEMP VIEW tot AS
        SELECT season, pitcher_id, SUM(cnt) AS total_cnt,
               SUM(CASE WHEN prev_pt=cur_pt THEN cnt ELSE 0 END) AS repeat_cnt
        FROM agg GROUP BY 1,2;

        CREATE OR REPLACE TEMP VIEW rpt AS
        SELECT season, pitcher_id,
               (repeat_cnt * 1.0) / NULLIF(total_cnt,0) AS repeat_rate
        FROM tot;

        -- 이름 매핑(가능하면 사용)
        CREATE OR REPLACE TEMP VIEW name_map AS
        SELECT season, pitcher_id, MAX(player_name) AS name
        FROM sc
        WHERE player_name IS NOT NULL AND LENGTH(player_name) > 0
        GROUP BY 1,2;

        CREATE OR REPLACE TEMP VIEW pitcher_season_summary AS
        SELECT
            COALESCE(t.season, e.season, r.season) AS season,
            COALESCE(t.pitcher_id, e.pitcher_id, r.pitcher_id) AS pitcher_id,
            COALESCE(n.name, '') AS name,
            t.dominant_pitch,
            t.dominant_usage,
            e.usage_entropy,
            r.repeat_rate
        FROM top1 t
        FULL OUTER JOIN ent e USING(season, pitcher_id)
        FULL OUTER JOIN rpt r USING(season, pitcher_id)
        LEFT JOIN name_map n USING(season, pitcher_id);
    """)

    # ─────────────────────────────────────────────────────────────────
    # (1) 리더보드: 시즌별 Top10 (엔트로피 높은 순) & 리피트레이트 상/하위 Top10
    # ─────────────────────────────────────────────────────────────────
    for p in (LB_ENT, LB_RPT_LOW, LB_RPT_HIGH, TREND_CSV, PLOT_TREND):
        safe_rm(p)

    log("[RUN] leaderboards")
    con.execute(f"""
        COPY (
          SELECT *
          FROM (
            SELECT season, pitcher_id, name, dominant_pitch, usage_entropy,
                   ROW_NUMBER() OVER (PARTITION BY season ORDER BY usage_entropy DESC NULLS LAST) AS rk
            FROM pitcher_season_summary
            WHERE season >= 2015 AND usage_entropy IS NOT NULL
          )
          WHERE rk <= 10
          ORDER BY season DESC, rk ASC
        ) TO '{LB_ENT}' (FORMAT CSV, HEADER TRUE);
    """)

    con.execute(f"""
        COPY (
          SELECT *
          FROM (
            SELECT season, pitcher_id, name, dominant_pitch, repeat_rate,
                   ROW_NUMBER() OVER (PARTITION BY season ORDER BY repeat_rate ASC NULLS LAST) AS rk
            FROM pitcher_season_summary
            WHERE season >= 2015 AND repeat_rate IS NOT NULL
          )
          WHERE rk <= 10
          ORDER BY season DESC, rk ASC
        ) TO '{LB_RPT_LOW}' (FORMAT CSV, HEADER TRUE);
    """)

    con.execute(f"""
        COPY (
          SELECT *
          FROM (
            SELECT season, pitcher_id, name, dominant_pitch, repeat_rate,
                   ROW_NUMBER() OVER (PARTITION BY season ORDER BY repeat_rate DESC NULLS LAST) AS rk
            FROM pitcher_season_summary
            WHERE season >= 2015 AND repeat_rate IS NOT NULL
          )
          WHERE rk <= 10
          ORDER BY season DESC, rk ASC
        ) TO '{LB_RPT_HIGH}' (FORMAT CSV, HEADER TRUE);
    """)

    # ─────────────────────────────────────────────────────────────────
    # (2) 시각화: 연도별 평균 엔트로피/리피트레이트 추이 + CSV
    # ─────────────────────────────────────────────────────────────────
    log("[RUN] trends by year")
    trend = con.execute("""
        SELECT
          season AS year,
          AVG(usage_entropy) AS avg_usage_entropy,
          AVG(repeat_rate)   AS avg_repeat_rate
        FROM pitcher_season_summary
        WHERE season >= 2015
        GROUP BY 1
        ORDER BY 1
    """).fetch_df()
    trend.to_csv(TREND_CSV, index=False)

    # matplotlib 단일 플롯 2축
    try:
        plt.figure(figsize=(9,5))
        # x
        xs = trend["year"].tolist()
        # y1, y2
        y1 = trend["avg_usage_entropy"].tolist()
        y2 = trend["avg_repeat_rate"].tolist()

        ax1 = plt.gca()
        l1 = ax1.plot(xs, y1, marker='o', label="Avg Usage Entropy")
        ax1.set_xlabel("Season")
        ax1.set_ylabel("Avg Usage Entropy")

        ax2 = ax1.twinx()
        l2 = ax2.plot(xs, y2, marker='s', linestyle='--', label="Avg Repeat Rate")
        ax2.set_ylabel("Avg Repeat Rate")

        # 범례
        lines = l1 + l2
        labels = [ln.get_label() for ln in lines]
        plt.legend(lines, labels, loc="best")
        plt.title("Pitch Repertoire Diversity vs Repeat Rate by Season (2015+)")

        plt.tight_layout()
        plt.savefig(PLOT_TREND, dpi=120)
        plt.close()
        log(f"[OK] trend plot -> {PLOT_TREND}")
    except Exception as e:
        log(f"[WARN] plotting skipped: {e}")

    # ─────────────────────────────────────────────────────────────────
    # 2025 커버리지 상태 리포트 저장
    # ─────────────────────────────────────────────────────────────────
    has_2025 = False
    try:
        has_2025 = bool(con.execute("""
            SELECT COUNT(*)>0 AS has_2025
            FROM pitcher_season_summary
            WHERE season=2025
              AND (usage_entropy IS NOT NULL OR repeat_rate IS NOT NULL)
        """).fetchdf().iloc[0]["has_2025"])
    except Exception:
        pass

    status = {
        "rows_in_master": rows,
        "min_year": min_y,
        "max_year": max_y,
        "has_2025_statcast_features": has_2025
    }
    with open(f"{SUM}/statcast_coverage_status.json", "w", encoding="utf-8") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)
    log(f"[COVERAGE] saved -> {SUM}/statcast_coverage_status.json : {status}")

    con.close()
    gc.collect()
    log("[DONE] leaderboard+plots")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main() or 0)
    except Exception as e:
        log(f"❌ ERROR: {e}")
        # 요구조건: 터미널 안터지게 → 0으로 종료
        sys.exit(0)
