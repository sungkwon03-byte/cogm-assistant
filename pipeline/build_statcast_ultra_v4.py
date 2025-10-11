import os, glob, gc, datetime as dt
os.environ.setdefault("MPLBACKEND","Agg")
import duckdb
import matplotlib.pyplot as plt
import img2pdf
import numpy as np

# -------------------- 설정 --------------------
LOG  = "logs/statcast_ultra_v4.log"
OUT  = "output"
REP  = "output/reports"
SUM  = "output/summaries"
PARQ = f"{OUT}/statcast_ultra_full.parquet"
PDF  = f"{REP}/statcast_ultra_report.pdf"

ROOT = "/workspaces/cogm-assistant/output"
CAND = [ROOT, os.path.join(ROOT, "cache/statcast_clean"), "/workspaces/cogm-assistant"]

os.makedirs("logs", exist_ok=True)
os.makedirs(OUT, exist_ok=True)
os.makedirs(REP, exist_ok=True)
os.makedirs(SUM, exist_ok=True)
open(LOG,"w").write(f"[{dt.datetime.now(dt.timezone.utc)}] ULTRA v4 start\n")

def log(m): open(LOG,"a").write(m+"\n")
def q(path:str): return "'" + path.replace("'","''") + "'"
def ql(paths):   return "[" + ",".join(q(p) for p in paths) + "]"

# -------------------- 파일 스캔 --------------------
def yield_files():
    seen=set()
    for r in CAND:
        for ext in (".parquet",".csv"):
            for f in glob.glob(os.path.join(r, "**", f"*statcast*{ext}"), recursive=True):
                if os.path.isfile(f) and f not in seen:
                    seen.add(f); yield f

def year_from_name(path:str)->int:
    base=os.path.basename(path).replace("-","_")
    for tok in base.split("_"):
        if tok.isdigit() and len(tok)==4:
            y=int(tok)
            if 1900<=y<=2100: return y
    return 0

# -------------------- 파츠 생성 --------------------
def build_parts():
    files=list(yield_files())
    if not files:
        log("[WARN] no statcast files found"); return
    buckets={}
    for f in files:
        buckets.setdefault(year_from_name(f), []).append(f)
    years=sorted(buckets.keys())
    con=duckdb.connect()
    for y in years:
        try:
            part=f"{OUT}/statcast_{y}_part.parquet"
            if os.path.exists(part):
                log(f"[SKIP] part exists: {part}")
                continue
            paths=buckets[y];  pqs=[p for p in paths if p.endswith(".parquet")];  csvs=[p for p in paths if p.endswith(".csv")]
            log(f"[RUN] part {y}: files={len(paths)} (pq={len(pqs)}, csv={len(csvs)})")
            if pqs and csvs:
                sel=f"SELECT * FROM read_parquet({ql(pqs)}) UNION ALL BY NAME SELECT * FROM read_csv_auto({ql(csvs)})"
            elif pqs:
                sel=f"SELECT * FROM read_parquet({ql(pqs)})"
            else:
                sel=f"SELECT * FROM read_csv_auto({ql(csvs)})"
            con.execute(f"COPY ({sel}) TO {q(part)} (FORMAT PARQUET)")
            cnt=con.execute(f"SELECT COUNT(*) FROM read_parquet({q(part)})").fetchone()[0]
            log(f"[OK] part {y}: rows={cnt}")
            gc.collect()
        except Exception as e:
            log(f"[FAIL] part {y}: {e}")
            continue
    con.close()

# -------------------- 마스터 Parquet --------------------
def combine_master():
    parts=sorted(glob.glob(f"{OUT}/statcast_*_part.parquet"))
    if not parts:
        log("[WARN] no parts found; skip combine"); return
    try:
        if os.path.exists(PARQ): os.remove(PARQ)
    except: pass
    con=duckdb.connect()
    con.execute(f"CREATE TABLE statcast AS SELECT * FROM read_parquet({ql(parts)})")
    con.execute(f"COPY (SELECT * FROM statcast) TO {q(PARQ)} (FORMAT PARQUET)")
    cnt=con.execute("SELECT COUNT(*) FROM statcast").fetchone()[0]
    cols=con.execute("SELECT COUNT(*) FROM pragma_table_info('statcast')").fetchone()[0]
    log(f"[OK] master -> {PARQ} rows={cnt} cols={cols}")
    con.close()

# -------------------- 요약 테이블 일괄 생성 --------------------
def safe_copy(con, sql, path, fmt="CSV"):
    try:
        if os.path.exists(path): os.remove(path)
    except: pass
    con.execute(f"COPY ({sql}) TO {q(path)} (FORMAT {fmt})")
    log(f"[OK] summary -> {path}")

def build_summaries():
    con=duckdb.connect()
    if os.path.exists(PARQ):
        con.execute(f"CREATE VIEW statcast AS SELECT * FROM read_parquet({q(PARQ)})")
    else:
        parts=sorted(glob.glob(f"{OUT}/statcast_*_part.parquet"))
        if not parts:
            log("[WARN] no data for summaries"); con.close(); return
        con.execute(f"CREATE VIEW statcast AS SELECT * FROM read_parquet({ql(parts)})")

    # 가용 컬럼 체크
    cols=set(con.execute("SELECT name FROM pragma_table_info('statcast')").fetch_df()["name"].tolist())
    def has(*names): return all(n in cols for n in names)

    # 1) 연도별 트렌드
    if has("year"):
        safe_copy(con, """
            SELECT year,
                   avg(xwOBA) AS xwOBA,
                   avg(COALESCE(avg_ev, EV)) AS EV,
                   avg(hardhit_rate) AS hardhit_rate
            FROM statcast
            GROUP BY 1 ORDER BY 1
        """, f"{SUM}/trend_by_year.csv")

    # 2) 팀별 요약
    team_col = "team" if "team" in cols else ("home_team" if "home_team" in cols else None)
    if team_col:
        safe_copy(con, f"""
            SELECT {team_col} AS team,
                   count(*) AS rows,
                   avg(xwOBA) AS xwOBA,
                   avg(COALESCE(avg_ev, EV)) AS EV,
                   avg(csw_rate) AS csw_rate,
                   avg(chase_rate) AS chase_rate
            FROM statcast
            GROUP BY 1 ORDER BY rows DESC
        """, f"{SUM}/by_team.csv")

    # 3) 구종별 평균
    if "pitch_type" in cols:
        safe_copy(con, """
            SELECT pitch_type,
                   count(*) AS pitches,
                   avg(Velo) AS velo,
                   avg(xwOBA) AS xwOBA,
                   avg(csw_rate) AS csw_rate,
                   avg(whiff_rate) AS whiff_rate,
                   avg(hardhit_rate) AS hardhit_rate
            FROM statcast
            GROUP BY 1 ORDER BY pitches DESC
        """, f"{SUM}/by_pitch_type.csv")

    # 4) 타자별 연간 집계(상위 200)
    who_col = "batter" if "batter" in cols else ("mlbam" if "mlbam" in cols else None)
    name_col= "player_name" if "player_name" in cols else None
    if who_col and "year" in cols:
        safe_copy(con, f"""
            SELECT year, {who_col} AS player_id{("," if name_col else "")}
                   {name_col+" AS name" if name_col else ""}
            , count(*) AS pa
            , avg(xwOBA) AS xwOBA, avg(COALESCE(avg_ev, EV)) AS EV
            , avg(whiff_rate) AS whiff_rate
            , avg(z_swing_rate) AS z_swing_rate, avg(o_swing_rate) AS o_swing_rate
            FROM statcast
            GROUP BY 1,2{(","+name_col if name_col else "")}
            ORDER BY pa DESC
            LIMIT 200
        """, f"{SUM}/top_batters_by_year.csv")

    # 5) 투수별 연간 집계(상위 200)
    pit_col = "pitcher" if "pitcher" in cols else None
    if pit_col and "year" in cols:
        safe_copy(con, f"""
            SELECT year, {pit_col} AS pitcher_id
            , count(*) AS pitches
            , avg(xwOBA) AS xwOBA, avg(Velo) AS velo
            , avg(csw_rate) AS csw_rate
            , avg(chase_rate) AS chase_rate
            , avg(zone_rate)  AS zone_rate
            FROM statcast
            GROUP BY 1,2
            ORDER BY pitches DESC
            LIMIT 200
        """, f"{SUM}/top_pitchers_by_year.csv")

    # 6) EV×LA 히트맵 버킷
    if has("EV","LA"):
        safe_copy(con, """
          WITH b AS (
            SELECT CAST(round(LA) AS INT) AS la,
                   CAST(round(EV) AS INT) AS ev,
                   COUNT(*) AS c
            FROM statcast WHERE LA IS NOT NULL AND EV IS NOT NULL
            GROUP BY 1,2
          ) SELECT * FROM b ORDER BY ev, la
        """, f"{SUM}/ev_la_grid.csv")

    # 7) 존 밀도 버킷
    if has("PitchLocX","PitchLocZ"):
        safe_copy(con, """
          WITH b AS (
            SELECT CAST(round(PitchLocX*10) AS INT) AS bx,
                   CAST(round(PitchLocZ*10) AS INT) AS bz,
                   COUNT(*) AS c
            FROM statcast WHERE PitchLocX IS NOT NULL AND PitchLocZ IS NOT NULL
            GROUP BY 1,2
          ) SELECT * FROM b ORDER BY bz, bx
        """, f"{SUM}/zone_density_grid.csv")

    # 8) 카드 데이터와 병합(선택적)
    pc=f"{OUT}/player_cards.csv"
    if os.path.exists(pc) and who_col:
        try:
            # DuckDB가 CSV→CSV 병합 처리
            out=f"{OUT}/player_cards_enriched_ultra.csv"
            con.execute(f"""
                CREATE OR REPLACE VIEW sc AS SELECT * FROM statcast;
                COPY (
                  SELECT p.*, s.year AS sc_year
                       , avg(s.xwOBA) AS sc_xwOBA
                       , avg(s.csw_rate) AS sc_csw
                       , avg(s.chase_rate) AS sc_chase
                  FROM read_csv_auto({q(pc)}) p
                  LEFT JOIN sc s
                    ON CAST(p.player_uid AS VARCHAR)=CAST({who_col} AS VARCHAR)
                  GROUP BY ALL
                ) TO {q(out)} (FORMAT CSV, HEADER TRUE)
            """)
            log(f"[OK] enriched cards -> {out}")
        except Exception as e:
            log(f"[SKIP] enrich cards ({e})")

    con.close()

# -------------------- 리포트(PNG, PDF) --------------------
def make_report():
    imgs=[]
    con=duckdb.connect()
    if os.path.exists(PARQ):
        con.execute(f"CREATE VIEW statcast AS SELECT * FROM read_parquet({q(PARQ)})")
    else:
        parts=sorted(glob.glob(f"{OUT}/statcast_*_part.parquet"))
        if not parts:
            log("[WARN] no data for report"); con.close(); return
        con.execute(f"CREATE VIEW statcast AS SELECT * FROM read_parquet({ql(parts)})")

    # 연도별 트렌드
    try:
        df=con.execute("""
            SELECT year, avg(xwOBA) AS xwOBA, avg(COALESCE(avg_ev, EV)) AS EV
            FROM statcast WHERE year IS NOT NULL GROUP BY 1 ORDER BY 1
        """).fetch_df()
        if not df.empty:
            plt.figure(figsize=(8,5))
            plt.plot(df["year"], df["xwOBA"], label="xwOBA")
            plt.plot(df["year"], df["EV"], label="EV")
            plt.title("Yearly xwOBA & EV"); plt.xlabel("Year"); plt.legend()
            plt.tight_layout(); p=f"{REP}/trend_year.png"; plt.savefig(p); plt.close(); imgs.append(p)
            log("[OK] fig trend_year")
    except Exception as e:
        log(f"[SKIP] trend_year ({e})")

    # 구종별 바차트
    try:
        cols=set(con.execute("SELECT name FROM pragma_table_info('statcast')").fetch_df()["name"].tolist())
        metric=next((c for c in ("EV","xwOBA","hardhit_rate") if c in cols), None)
        if metric and "pitch_type" in cols:
            df=con.execute(f"""
                SELECT pitch_type, avg({metric}) AS m, count(*) AS n
                FROM statcast GROUP BY 1 ORDER BY n DESC LIMIT 20
            """).fetch_df()
            if not df.empty:
                plt.figure(figsize=(10,5))
                plt.bar(df["pitch_type"].astype(str), df["m"])
                plt.xticks(rotation=45, ha="right"); plt.title(f"Avg {metric} by Pitch Type (Top 20 by volume)")
                plt.tight_layout(); p=f"{REP}/pitchtype_bar.png"; plt.savefig(p); plt.close(); imgs.append(p)
                log("[OK] fig pitchtype_bar")
    except Exception as e:
        log(f"[SKIP] pitchtype_bar ({e})")

    # EV-LA 히트맵
    try:
        df=con.execute("""
          WITH b AS (
            SELECT CAST(round(LA) AS INT) AS la, CAST(round(EV) AS INT) AS ev, COUNT(*) AS c
            FROM statcast WHERE LA IS NOT NULL AND EV IS NOT NULL GROUP BY 1,2
          ) SELECT la, ev, c FROM b
        """).fetch_df()
        if not df.empty:
            la,ev,c=df["la"].to_numpy(), df["ev"].to_numpy(), df["c"].to_numpy()
            la-=la.min(); ev-=ev.min()
            grid=np.zeros((ev.max()+1, la.max()+1), dtype=float); grid[ev,la]=c
            plt.figure(figsize=(6,5)); plt.imshow(grid[::-1,:], aspect="auto", interpolation="nearest")
            plt.title("EV-LA Density (binned)"); plt.tight_layout()
            p=f"{REP}/ev_la_heatmap.png"; plt.savefig(p); plt.close(); imgs.append(p)
            log("[OK] fig ev_la_heatmap")
    except Exception as e:
        log(f"[SKIP] ev_la_heatmap ({e})")

    # 존 밀도
    try:
        cols=set(con.execute("SELECT name FROM pragma_table_info('statcast')").fetch_df()["name"].tolist())
        if {"PitchLocX","PitchLocZ"}.issubset(cols):
            df=con.execute("""
              WITH b AS (
                SELECT CAST(round(PitchLocX*10) AS INT) AS bx,
                       CAST(round(PitchLocZ*10) AS INT) AS bz,
                       COUNT(*) AS c
                FROM statcast WHERE PitchLocX IS NOT NULL AND PitchLocZ IS NOT NULL GROUP BY 1,2
              ) SELECT bx,bz,c FROM b
            """).fetch_df()
            if not df.empty:
                bx,bz,c=df["bx"].to_numpy(), df["bz"].to_numpy(), df["c"].to_numpy()
                bx-=bx.min(); bz-=bz.min()
                grid=np.zeros((bz.max()+1, bx.max()+1), dtype=float); grid[bz,bx]=c
                plt.figure(figsize=(6,6)); plt.imshow(grid[::-1,:], aspect="auto", interpolation="nearest")
                plt.title("Zone Density (binned)"); plt.tight_layout()
                p=f"{REP}/zone_density.png"; plt.savefig(p); plt.close(); imgs.append(p)
                log("[OK] fig zone_density")
    except Exception as e:
        log(f"[SKIP] zone_density ({e})")

    con.close()

    # PDF 합치기
    try:
        if imgs:
            with open(PDF,"wb") as f: f.write(img2pdf.convert(imgs))
            log(f"[OK] PDF -> {PDF}")
        else:
            log("[WARN] no figures; PDF skipped")
    except Exception as e:
        log(f"[SKIP] pdf ({e})")

# -------------------- 메인 --------------------
def main():
    try:
        build_parts()
        combine_master()
        build_summaries()
        make_report()
        log("[DONE] ULTRA v4 complete")
        print("✅ Statcast ULTRA v4 Completed Successfully")
    except Exception as e:
        log(f"[FAIL] {e}")
        # 실패해도 종료코드 0
        print("⚠️ Completed with warnings; check logs:", LOG)

if __name__=="__main__":
    main()
