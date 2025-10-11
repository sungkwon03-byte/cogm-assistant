import os, glob, gc, datetime as dt
os.environ.setdefault("MPLBACKEND","Agg")
import duckdb
import matplotlib.pyplot as plt
import img2pdf
import numpy as np

LOG = "logs/statcast_ultra_safe_v3.log"
os.makedirs("logs", exist_ok=True)
os.makedirs("output/reports", exist_ok=True)
open(LOG, "w").write(f"[{dt.datetime.now(dt.timezone.utc)}] Safe ULTRA v3.2 start\n")

# 검색 루트
ROOT = "/workspaces/cogm-assistant/output"
CAND = [ROOT, os.path.join(ROOT, "cache/statcast_clean"), "/workspaces/cogm-assistant"]

OUT_PARQ = "output/statcast_ultra_full.parquet"
OUT_PDF  = "output/reports/statcast_ultra_report.pdf"

def log(msg: str):
    with open(LOG, "a") as f:
        f.write(msg + "\n")

def sql_quote(path: str) -> str:
    return "'" + path.replace("'", "''") + "'"

def sql_list(paths):
    return "[" + ",".join(sql_quote(p) for p in paths) + "]"

def yield_files():
    seen = set()
    for r in CAND:
        for ext in (".parquet", ".csv"):
            for f in glob.glob(os.path.join(r, "**", f"*statcast*{ext}"), recursive=True):
                if os.path.isfile(f) and f not in seen:
                    seen.add(f)
                    yield f

def year_from_name(path: str) -> int:
    base = os.path.basename(path).replace("-", "_")
    for tok in base.split("_"):
        if tok.isdigit() and len(tok) == 4:
            y = int(tok)
            if 1900 <= y <= 2100:
                return y
    return 0  # 연도 미표기 파일은 0 버킷

def merge_by_year_to_parts():
    files = list(yield_files())
    if not files:
        log("[WARN] no statcast files discovered")
        return
    buckets = {}
    for f in files:
        buckets.setdefault(year_from_name(f), []).append(f)
    years = sorted(buckets.keys())

    con = duckdb.connect()
    for y in years:
        try:
            part = f"output/statcast_{y}_part.parquet"
            if os.path.exists(part):
                log(f"[SKIP] part exists: {part}")
                continue

            paths = buckets[y]
            if not paths:
                continue
            pqs  = [p for p in paths if p.endswith(".parquet")]
            csvs = [p for p in paths if p.endswith(".csv")]

            log(f"[RUN] building part {y} from {len(paths)} files")
            if pqs and csvs:
                sel = f"SELECT * FROM read_parquet({sql_list(pqs)}) UNION ALL BY NAME SELECT * FROM read_csv_auto({sql_list(csvs)})"
            elif pqs:
                sel = f"SELECT * FROM read_parquet({sql_list(pqs)})"
            else:
                sel = f"SELECT * FROM read_csv_auto({sql_list(csvs)})"

            out_sql = sql_quote(part)
            con.execute(f"COPY ({sel}) TO {out_sql} (FORMAT PARQUET)")
            cnt  = con.execute(f"SELECT COUNT(*) FROM read_parquet({out_sql})").fetchone()[0]
            cols = con.execute(f"SELECT COUNT(*) FROM pragma_table_info('read_parquet({out_sql})')").fetchone()[0] \
                   if False else None  # 간단화
            log(f"[OK] part {y}: rows={cnt} -> {part}")
            gc.collect()
        except Exception as e:
            log(f"[FAIL] part {y}: {e}")
            continue
    con.close()

def combine_all_parts_to_parquet():
    parts = sorted(glob.glob("output/statcast_*_part.parquet"))
    if not parts:
        log("[WARN] no parts found; skipping combine")
        return
    try:
        if os.path.exists(OUT_PARQ):
            os.remove(OUT_PARQ)
    except:
        pass
    con = duckdb.connect()
    con.execute(f"CREATE TABLE statcast AS SELECT * FROM read_parquet({sql_list(parts)})")
    con.execute(f"COPY (SELECT * FROM statcast) TO {sql_quote(OUT_PARQ)} (FORMAT PARQUET)")
    cnt  = con.execute("SELECT COUNT(*) FROM statcast").fetchone()[0]
    cols = con.execute("SELECT COUNT(*) FROM pragma_table_info('statcast')").fetchone()[0]
    log(f"[OK] combined -> {OUT_PARQ} rows={cnt} cols={cols}")
    con.close()

def make_pngs_and_pdf():
    imgs = []
    con = duckdb.connect()
    if os.path.exists(OUT_PARQ):
        con.execute(f"CREATE VIEW statcast AS SELECT * FROM read_parquet({sql_quote(OUT_PARQ)})")
    else:
        parts = sorted(glob.glob("output/statcast_*_part.parquet"))
        if not parts:
            log("[WARN] no data for report")
            con.close()
            return
        con.execute(f"CREATE VIEW statcast AS SELECT * FROM read_parquet({sql_list(parts)})")

    # 1) Yearly trend (xwOBA / EV)
    try:
        q1 = """
          SELECT year, avg(xwOBA) AS xwOBA, avg(COALESCE(avg_ev, EV)) AS EV
          FROM statcast
          WHERE year IS NOT NULL
          GROUP BY 1 ORDER BY 1
        """
        df1 = con.execute(q1).fetch_df()
        if not df1.empty:
            plt.figure(figsize=(8,6))
            plt.plot(df1["year"], df1["xwOBA"], label="xwOBA")
            plt.plot(df1["year"], df1["EV"], label="EV")
            plt.title("Yearly xwOBA / EV"); plt.xlabel("Year"); plt.ylabel("Value"); plt.legend()
            plt.tight_layout()
            p = "output/reports/trend_year.png"; plt.savefig(p); plt.close(); imgs.append(p)
            log("[OK] fig trend_year")
    except Exception as e:
        log(f"[SKIP] trend_year ({e})")

    # 2) Pitch type bar (metric auto)
    try:
        cols = set(con.execute("SELECT name FROM pragma_table_info('statcast')").fetch_df()["name"].tolist())
        metric = next((c for c in ("EV","xwOBA","hardhit_rate") if c in cols), None)
        if metric and "pitch_type" in cols:
            q2 = f"SELECT pitch_type, avg({metric}) AS m FROM statcast GROUP BY 1 ORDER BY m DESC LIMIT 20"
            df2 = con.execute(q2).fetch_df()
            if not df2.empty:
                plt.figure(figsize=(10,5))
                plt.bar(df2["pitch_type"].astype(str), df2["m"])
                plt.xticks(rotation=45, ha="right"); plt.title(f"Avg {metric} by Pitch Type")
                plt.tight_layout()
                p = "output/reports/pitchtype_bar.png"; plt.savefig(p); plt.close(); imgs.append(p)
                log("[OK] fig pitchtype_bar")
    except Exception as e:
        log(f"[SKIP] pitchtype_bar ({e})")

    # 3) EV-LA binned heatmap
    try:
        q3 = """
        WITH b AS (
          SELECT
            CAST(round(LA) AS INT) AS la,
            CAST(round(EV) AS INT) AS ev,
            COUNT(*) AS c
          FROM statcast
          WHERE LA IS NOT NULL AND EV IS NOT NULL
          GROUP BY 1,2
        )
        SELECT la, ev, c FROM b
        """
        df3 = con.execute(q3).fetch_df()
        if not df3.empty:
            la, ev, c = df3["la"].to_numpy(), df3["ev"].to_numpy(), df3["c"].to_numpy()
            la -= la.min(); ev -= ev.min()
            grid = np.zeros((ev.max()+1, la.max()+1), dtype=float)
            grid[ev, la] = c
            plt.figure(figsize=(6,5))
            plt.imshow(grid[::-1,:], aspect="auto", interpolation="nearest")
            plt.title("EV-LA Density (binned)")
            plt.tight_layout()
            p = "output/reports/ev_la_heatmap.png"; plt.savefig(p); plt.close(); imgs.append(p)
            log("[OK] fig ev_la_heatmap")
    except Exception as e:
        log(f"[SKIP] ev_la_heatmap ({e})")

    # 4) Zone density (binned)
    try:
        cols = set(con.execute("SELECT name FROM pragma_table_info('statcast')").fetch_df()["name"].tolist())
        if {"PitchLocX","PitchLocZ"}.issubset(cols):
            q4 = """
            WITH b AS (
              SELECT
                CAST(round(PitchLocX*10) AS INT) AS bx,
                CAST(round(PitchLocZ*10) AS INT) AS bz,
                COUNT(*) AS c
              FROM statcast
              WHERE PitchLocX IS NOT NULL AND PitchLocZ IS NOT NULL
              GROUP BY 1,2
            )
            SELECT bx, bz, c FROM b
            """
            df4 = con.execute(q4).fetch_df()
            if not df4.empty:
                bx, bz, c = df4["bx"].to_numpy(), df4["bz"].to_numpy(), df4["c"].to_numpy()
                bx -= bx.min(); bz -= bz.min()
                grid = np.zeros((bz.max()+1, bx.max()+1), dtype=float)
                grid[bz, bx] = c
                plt.figure(figsize=(6,6))
                plt.imshow(grid[::-1,:], aspect="auto", interpolation="nearest")
                plt.title("Pitch Location Density (binned)")
                plt.tight_layout()
                p = "output/reports/zone_density.png"; plt.savefig(p); plt.close(); imgs.append(p)
                log("[OK] fig zone_density")
    except Exception as e:
        log(f"[SKIP] zone_density ({e})")

    con.close()

    if imgs:
        with open(OUT_PDF, "wb") as f:
            f.write(img2pdf.convert(imgs))
        log(f"[OK] PDF -> {OUT_PDF}")
    else:
        log("[WARN] no figures created; PDF skipped")

def main():
    try:
        merge_by_year_to_parts()
        combine_all_parts_to_parquet()
        make_pngs_and_pdf()
        log("[DONE] Safe ULTRA v3.2 complete")
        print("✅ Statcast ULTRA Safe v3.2 Completed Successfully")
    except Exception as e:
        log(f"[FAIL] {e}")
        print("⚠️ Completed with warnings; check logs:", LOG)

if __name__ == "__main__":
    main()
