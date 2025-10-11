import os, glob, gc, datetime as dt
os.environ.setdefault("MPLBACKEND","Agg")
import duckdb
import matplotlib.pyplot as plt
import img2pdf
import numpy as np

LOG  = "logs/statcast_ultra_v4.log"
OUT  = "output"
REP  = "output/reports"
SUM  = "output/summaries"
PARQ = f"{OUT}/statcast_ultra_full.parquet"

ROOT = "/workspaces/cogm-assistant/output"
CAND = [ROOT, os.path.join(ROOT, "cache/statcast_clean"), "/workspaces/cogm-assistant"]

os.makedirs("logs", exist_ok=True)
os.makedirs(OUT, exist_ok=True)
os.makedirs(REP, exist_ok=True)
os.makedirs(SUM, exist_ok=True)
open(LOG,"w").write(f"[{dt.datetime.now(dt.timezone.utc)}] ULTRA v4.1 start\n")

def log(m): open(LOG,"a").write(m+"\n")
def q(path:str): return "'" + path.replace("'","''") + "'"
def ql(paths):   return "[" + ",".join(q(p) for p in paths) + "]"

# ---------- 파일 스캔 ----------
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

# ---------- 파츠 ----------
def build_parts():
    files=list(yield_files())
    if not files:
        log("[WARN] no statcast files found"); return
    buckets={}
    for f in files: buckets.setdefault(year_from_name(f), []).append(f)
    years=sorted(buckets.keys())
    con=duckdb.connect()
    for y in years:
        try:
            part=f"{OUT}/statcast_{y}_part.parquet"
            if os.path.exists(part):
                log(f"[SKIP] part exists: {part}"); continue
            paths=buckets[y]; pqs=[p for p in paths if p.endswith(".parquet")]; csvs=[p for p in paths if p.endswith(".csv")]
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
            log(f"[FAIL] part {y}: {e}"); continue
    con.close()

# ---------- 마스터 ----------
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

# ---------- 공통: 숫자형 뷰 구성 ----------
NUM_METRICS = [
  "xwOBA","EV","avg_ev","hardhit_rate","csw_rate","chase_rate",
  "Velo","whiff_rate","z_swing_rate","o_swing_rate","zone_rate",
  "avg_spin","spin","z_contact_rate","o_contact_rate"
]

def create_views(con):
    # 기본 뷰
    if os.path.exists(PARQ):
        con.execute(f"CREATE VIEW statcast AS SELECT * FROM read_parquet({q(PARQ)})")
    else:
        parts=sorted(glob.glob(f"{OUT}/statcast_*_part.parquet"))
        if not parts: raise RuntimeError("no data for views")
        con.execute(f"CREATE VIEW statcast AS SELECT * FROM read_parquet({ql(parts)})")

    # 가용 컬럼
    cols=set(con.execute("SELECT name FROM pragma_table_info('statcast')").fetch_df()["name"].tolist())

    # 숫자 캐스팅 컬럼만 동적으로 추가
    proj=[]
    for c in NUM_METRICS:
        if c in cols:
            proj.append(f"TRY_CAST({c} AS DOUBLE) AS {c}_n")
    if proj:
        con.execute(f"CREATE OR REPLACE VIEW scn AS SELECT *, {', '.join(proj)} FROM statcast")
    else:
        con.execute(f"CREATE OR REPLACE VIEW scn AS SELECT * FROM statcast")
    return set(con.execute("SELECT name FROM pragma_table_info('scn')").fetch_df()["name"].tolist())

def pick(scn_cols, col, default="NULL"):
    # scn에 {col}_n 있으면 그걸 쓰고, 원본 있으면 TRY_CAST, 없으면 NULL
    if f"{col}_n" in scn_cols: return f"{col}_n"
    if col in scn_cols:        return f"TRY_CAST({col} AS DOUBLE)"
    return default

# ---------- 요약 ----------
def safe_copy(con, sql, path, fmt="CSV"):
    try:
        if os.path.exists(path): os.remove(path)
    except: pass
    con.execute(f"COPY ({sql}) TO {q(path)} (FORMAT {fmt})")
    log(f"[OK] summary -> {path}")

def build_summaries():
    con=duckdb.connect()
    try:
        scn_cols = create_views(con)
    except Exception as e:
        log(f"[WARN] summaries skipped: {e}"); con.close(); return

    # 1) 연도별 트렌드
    if "year" in scn_cols:
        xw = pick(scn_cols,"xwOBA","NULL")
        ev = f"COALESCE({pick(scn_cols,'avg_ev','NULL')}, {pick(scn_cols,'EV','NULL')})"
        hh = pick(scn_cols,"hardhit_rate","NULL")
        safe_copy(con, f"""
            SELECT year,
                   AVG({xw}) AS xwOBA,
                   AVG({ev}) AS EV,
                   AVG({hh}) AS hardhit_rate
            FROM scn
            GROUP BY 1 ORDER BY 1
        """, f"{SUM}/trend_by_year.csv")

    # 팀 컬럼
    team_col = "team" if "team" in scn_cols else ("home_team" if "home_team" in scn_cols else None)
    if team_col:
        xw = pick(scn_cols,"xwOBA","NULL")
        ev = f"COALESCE({pick(scn_cols,'avg_ev','NULL')}, {pick(scn_cols,'EV','NULL')})"
        csw= pick(scn_cols,"csw_rate","NULL")
        ch = pick(scn_cols,"chase_rate","NULL")
        safe_copy(con, f"""
            SELECT {team_col} AS team,
                   COUNT(*) AS rows,
                   AVG({xw}) AS xwOBA,
                   AVG({ev}) AS EV,
                   AVG({csw}) AS csw_rate,
                   AVG({ch})  AS chase_rate
            FROM scn
            GROUP BY 1 ORDER BY rows DESC
        """, f"{SUM}/by_team.csv")

    # 3) 구종별 평균
    if "pitch_type" in scn_cols:
        ve = pick(scn_cols,"Velo","NULL")
        xw = pick(scn_cols,"xwOBA","NULL")
        csw= pick(scn_cols,"csw_rate","NULL")
        wh = pick(scn_cols,"whiff_rate","NULL")
        hh = pick(scn_cols,"hardhit_rate","NULL")
        safe_copy(con, f"""
            SELECT pitch_type,
                   COUNT(*) AS pitches,
                   AVG({ve}) AS velo,
                   AVG({xw}) AS xwOBA,
                   AVG({csw}) AS csw_rate,
                   AVG({wh}) AS whiff_rate,
                   AVG({hh}) AS hardhit_rate
            FROM scn
            GROUP BY 1 ORDER BY pitches DESC
        """, f"{SUM}/by_pitch_type.csv")

    # 4) 타자 TOP N/연도
    who_col = "batter" if "batter" in scn_cols else ("mlbam" if "mlbam" in scn_cols else None)
    name_col= "player_name" if "player_name" in scn_cols else None
    if who_col and "year" in scn_cols:
        xw = pick(scn_cols,"xwOBA","NULL")
        ev = f"COALESCE({pick(scn_cols,'avg_ev','NULL')}, {pick(scn_cols,'EV','NULL')})"
        wh = pick(scn_cols,"whiff_rate","NULL")
        zs = pick(scn_cols,"z_swing_rate","NULL")
        os = pick(scn_cols,"o_swing_rate","NULL")
        safe_copy(con, f"""
            SELECT year, {who_col} AS player_id{("," if name_col else "")}
                   {name_col+" AS name" if name_col else ""}
            , COUNT(*) AS pa
            , AVG({xw}) AS xwOBA, AVG({ev}) AS EV
            , AVG({wh}) AS whiff_rate
            , AVG({zs}) AS z_swing_rate, AVG({os}) AS o_swing_rate
            FROM scn
            GROUP BY 1,2{(","+name_col if name_col else "")}
            ORDER BY pa DESC
            LIMIT 200
        """, f"{SUM}/top_batters_by_year.csv")

    # 5) 투수 TOP N/연도
    pit_col = "pitcher" if "pitcher" in scn_cols else None
    if pit_col and "year" in scn_cols:
        xw = pick(scn_cols,"xwOBA","NULL")
        ve = pick(scn_cols,"Velo","NULL")
        csw= pick(scn_cols,"csw_rate","NULL")
        ch = pick(scn_cols,"chase_rate","NULL")
        zr = pick(scn_cols,"zone_rate","NULL")
        safe_copy(con, f"""
            SELECT year, {pit_col} AS pitcher_id
            , COUNT(*) AS pitches
            , AVG({xw}) AS xwOBA, AVG({ve}) AS velo
            , AVG({csw}) AS csw_rate
            , AVG({ch})  AS chase_rate
            , AVG({zr})  AS zone_rate
            FROM scn
            GROUP BY 1,2
            ORDER BY pitches DESC
            LIMIT 200
        """, f"{SUM}/top_pitchers_by_year.csv")

    # 6) EV×LA 격자
    if {"EV","LA"}.intersection(scn_cols) or {"EV_n","LA_n"}.intersection(scn_cols):
        ev = pick(scn_cols,"EV","NULL")
        la = pick(scn_cols,"LA","NULL")
        safe_copy(con, f"""
          WITH b AS (
            SELECT CAST(round({la}) AS INT) AS la,
                   CAST(round({ev}) AS INT) AS ev,
                   COUNT(*) AS c
            FROM scn
            WHERE {la} IS NOT NULL AND {ev} IS NOT NULL
            GROUP BY 1,2
          ) SELECT * FROM b ORDER BY ev, la
        """, f"{SUM}/ev_la_grid.csv")

    # 7) 존 밀도
    if {"PitchLocX","PitchLocZ"}.issubset(scn_cols):
        safe_copy(con, """
          WITH b AS (
            SELECT CAST(round(PitchLocX*10) AS INT) AS bx,
                   CAST(round(PitchLocZ*10) AS INT) AS bz,
                   COUNT(*) AS c
            FROM scn WHERE PitchLocX IS NOT NULL AND PitchLocZ IS NOT NULL
            GROUP BY 1,2
          ) SELECT * FROM b ORDER BY bz, bx
        """, f"{SUM}/zone_density_grid.csv")

    # 8) 카드 보강(선택)
    pc=f"{OUT}/player_cards.csv"
    if os.path.exists(pc) and who_col:
        try:
            out=f"{OUT}/player_cards_enriched_ultra.csv"
            xw = pick(scn_cols,"xwOBA","NULL")
            csw= pick(scn_cols,"csw_rate","NULL")
            ch = pick(scn_cols,"chase_rate","NULL")
            duck = f"""
                CREATE OR REPLACE VIEW sc AS SELECT * FROM scn;
                COPY (
                  SELECT p.*, s.year AS sc_year,
                         AVG({xw}) AS sc_xwOBA,
                         AVG({csw}) AS sc_csw,
                         AVG({ch})  AS sc_chase
                  FROM read_csv_auto({q(pc)}) p
                  LEFT JOIN sc s
                    ON CAST(p.player_uid AS VARCHAR)=CAST({who_col} AS VARCHAR)
                  GROUP BY ALL
                ) TO {q(out)} (FORMAT CSV, HEADER TRUE)
            """
            con.execute(duck)
            log(f"[OK] enriched cards -> {out}")
        except Exception as e:
            log(f"[SKIP] enrich cards ({e})")

    con.close()

# ---------- 리포트 ----------
def make_report():
    imgs=[]
    con=duckdb.connect()
    try:
        # 뷰 재사용
        _ = create_views(con)
    except Exception as e:
        log(f"[WARN] report skipped: {e}"); con.close(); return

    # trend
    try:
        df=con.execute("""
            SELECT year,
                   AVG(TRY_CAST(xwOBA AS DOUBLE)) AS xwOBA,
                   AVG(COALESCE(TRY_CAST(avg_ev AS DOUBLE), TRY_CAST(EV AS DOUBLE))) AS EV
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

    # pitch type
    try:
        df=con.execute("""
            SELECT pitch_type,
                   COUNT(*) AS n,
                   AVG(TRY_CAST(Velo AS DOUBLE)) AS velo,
                   AVG(TRY_CAST(xwOBA AS DOUBLE)) AS xwOBA
            FROM statcast GROUP BY 1 ORDER BY n DESC LIMIT 20
        """).fetch_df()
        if not df.empty:
            plt.figure(figsize=(10,5))
            plt.bar(df["pitch_type"].astype(str), df["xwOBA"])
            plt.xticks(rotation=45, ha="right"); plt.title("Avg xwOBA by Pitch Type (Top 20)")
            plt.tight_layout(); p=f"{REP}/pitchtype_bar.png"; plt.savefig(p); plt.close(); imgs.append(p)
            log("[OK] fig pitchtype_bar")
    except Exception as e:
        log(f"[SKIP] pitchtype_bar ({e})")

    # EV-LA heatmap
    try:
        df=con.execute("""
          WITH b AS (
            SELECT CAST(round(TRY_CAST(LA AS DOUBLE)) AS INT) AS la,
                   CAST(round(TRY_CAST(EV AS DOUBLE)) AS INT) AS ev,
                   COUNT(*) AS c
            FROM statcast WHERE LA IS NOT NULL AND EV IS NOT NULL GROUP BY 1,2
          ) SELECT la,ev,c FROM b
        """).fetch_df()
        if not df.empty:
            la,ev,c=df["la"].to_numpy(), df["ev"].to_numpy(), df["c"].to_numpy()
            la-=la.min(); ev-=ev.min()
            grid=np.zeros((max(1,ev.max()+1), max(1,la.max()+1)), dtype=float); grid[ev,la]=c
            plt.figure(figsize=(6,5)); plt.imshow(grid[::-1,:], aspect="auto", interpolation="nearest")
            plt.title("EV-LA Density (binned)"); plt.tight_layout()
            p=f"{REP}/ev_la_heatmap.png"; plt.savefig(p); plt.close(); imgs.append(p)
            log("[OK] fig ev_la_heatmap")
    except Exception as e:
        log(f"[SKIP] ev_la_heatmap ({e})")

    con.close()

    try:
        if imgs:
            with open(f"{REP}/statcast_ultra_report.pdf","wb") as f: f.write(img2pdf.convert(imgs))
            log(f"[OK] PDF -> {REP}/statcast_ultra_report.pdf")
        else:
            log("[WARN] no figures; PDF skipped")
    except Exception as e:
        log(f"[SKIP] pdf ({e})")

# ---------- 메인 ----------
def main():
    try:
        build_parts()
        combine_master()
        build_summaries()
        make_report()
        log("[DONE] ULTRA v4.1 complete")
        print("✅ Statcast ULTRA v4.1 Completed Successfully")
    except Exception as e:
        log(f"[FAIL] {e}")
        print("⚠️ Completed with warnings; check logs:", LOG)

if __name__=="__main__":
    main()
