import os, glob, duckdb, polars as pl, datetime as dt, gc
import matplotlib.pyplot as plt, seaborn as sns, img2pdf

LOG="logs/statcast_ultra_safe.log"
os.makedirs("logs",exist_ok=True); os.makedirs("output/reports",exist_ok=True)
with open(LOG,"w") as f: f.write(f"[{dt.datetime.now(dt.UTC)}] Safe ULTRA start\n")

ROOT="/workspaces/cogm-assistant/output"
CAND=[ROOT, os.path.join(ROOT,"cache/statcast_clean"), "/workspaces/cogm-assistant"]

OUT_PARQ="output/statcast_ultra_full.parquet"
OUT_PDF ="output/reports/statcast_ultra_report.pdf"

def log(m): open(LOG,"a").write(m+"\n")

def read_any(p):
    try:
        if p.endswith(".parquet"): return pl.read_parquet(p)
        return pl.read_csv(p, low_memory=True)
    except Exception as e:
        log(f"[SKIP] {p} ({e})"); return None

def yield_files():
    # 프로젝트 전체에서 statcast*.(parquet|csv) 전수 스캔
    seen=set()
    for r in CAND:
        for ext in (".parquet",".csv"):
            for f in glob.glob(os.path.join(r, f"**/*statcast*{ext}"), recursive=True):
                if os.path.isfile(f) and f not in seen:
                    seen.add(f); yield f

def merge_by_year():
    # 파일명에서 연도 추출(없으면 0)
    buckets={}
    for f in yield_files():
        y=0
        base=os.path.basename(f)
        # 파일명 토큰에서 4자리 숫자 탐색
        for tok in base.replace("-","_").split("_"):
            if tok.isdigit() and len(tok)==4:
                try:
                    y=int(tok); break
                except: pass
        buckets.setdefault(y, []).append(f)

    years=sorted(buckets.keys())
    for y in years:
        files=buckets[y]
        if not files: continue
        dfs=[]
        for f in files:
            df=read_any(f)
            if df is not None: dfs.append(df)
        if not dfs: continue
        dfy=pl.concat(dfs, how="diagonal_relaxed")
        out=f"output/statcast_{y}_part.parquet"
        dfy.write_parquet(out)
        log(f"[OK] part {y}: rows={len(dfy)} files={len(files)} -> {out}")
        del dfs, dfy; gc.collect()

def combine_all():
    parts=sorted(glob.glob("output/statcast_*_part.parquet"))
    if not parts: raise SystemExit("no parts")
    # 기존 결과가 있으면 삭제 후 생성
    if os.path.exists(OUT_PARQ):
        try: os.remove(OUT_PARQ)
        except: pass

    con=duckdb.connect()  # 메모리 DB
    # 모든 파츠를 한 번에 로드하여 Parquet로 내보냄
    con.execute("CREATE TABLE statcast AS SELECT * FROM read_parquet($parts)", {'parts': parts})
    con.execute(f"COPY (SELECT * FROM statcast) TO '{OUT_PARQ}' (FORMAT PARQUET)")
    cnt=con.execute("SELECT COUNT(*) FROM statcast").fetchone()[0]
    cols=con.execute("SELECT COUNT(*) FROM pragma_table_info('statcast')").fetchone()[0]
    log(f"[OK] combined -> {OUT_PARQ} rows={cnt} cols={cols}")
    con.close()

def report():
    # 임시 DuckDB 세션에서 Parquet를 테이블로 읽어 시각화용 샘플 쿼리만 수행
    con=duckdb.connect()
    con.execute("CREATE VIEW statcast AS SELECT * FROM read_parquet($p)", {'p': OUT_PARQ})

    # 1) 연도별 트렌드 (xwOBA / EV)
    q1="""
      SELECT year, avg(xwOBA) AS xwOBA, avg(COALESCE(avg_ev, EV)) AS EV
      FROM statcast
      WHERE year IS NOT NULL
      GROUP BY 1 ORDER BY 1
    """
    df=con.execute(q1).fetchdf()
    if not df.empty:
        plt.figure(figsize=(8,6))
        plt.plot(df["year"], df["xwOBA"], label="xwOBA")
        plt.plot(df["year"], df["EV"],     label="EV")
        plt.legend(); plt.title("Yearly xwOBA / EV")
        plt.xlabel("Year"); plt.ylabel("Value")
        plt.tight_layout(); plt.savefig("output/reports/trend_year.png"); plt.close()

    # 2) EV–LA KDE (샘플 5k)
    q2="SELECT LA, EV FROM statcast WHERE LA IS NOT NULL AND EV IS NOT NULL USING SAMPLE 5000"
    df2=con.execute(q2).fetchdf()
    if not df2.empty:
        sns.kdeplot(df2, x="LA", y="EV", fill=True, thresh=0.05)
        plt.title("EV–LA Density"); plt.tight_layout()
        plt.savefig("output/reports/ev_la_heatmap.png"); plt.close()

    # 3) Pitch Type 평균 (SpinRate 대체 자동)
    # 컬럼 존재 체크
    cols=set(con.execute("SELECT name FROM pragma_table_info('statcast')").fetchdf()["name"].tolist())
    spin_col = "SpinRate" if "SpinRate" in cols else ("spin" if "spin" in cols else ("avg_spin" if "avg_spin" in cols else None))
    if "pitch_type" in cols:
        metric = "EV" if "EV" in cols else "xwOBA" if "xwOBA" in cols else None
        if metric:
            base = f"SELECT pitch_type, avg({metric}) AS metric"
            if spin_col: base += f", avg({spin_col}) AS spin_metric"
            base += " FROM statcast GROUP BY 1"
            pt=con.execute(base).fetchdf()
            if not pt.empty:
                plt.figure(figsize=(9,5))
                sns.barplot(pt, x="pitch_type", y="metric")
                ttl = f"Avg {metric} by Pitch Type" + (f" (Spin: {spin_col})" if spin_col else "")
                plt.title(ttl); plt.xticks(rotation=45, ha="right")
                plt.tight_layout(); plt.savefig("output/reports/ev_pitchtype.png"); plt.close()

    # 4) 존 밀도 (PitchLocX/PitchLocZ)
    if {"PitchLocX","PitchLocZ"}.issubset(cols):
        df3=con.execute("SELECT PitchLocX, PitchLocZ FROM statcast WHERE PitchLocX IS NOT NULL AND PitchLocZ IS NOT NULL USING SAMPLE 3000").fetchdf()
        if not df3.empty:
            sns.kdeplot(df3, x="PitchLocX", y="PitchLocZ", fill=True, thresh=0.1)
            plt.title("Pitch Location Density"); plt.tight_layout()
            plt.savefig("output/reports/zone_density.png"); plt.close()

    con.close()

    # PNG → PDF 병합
    imgs=[os.path.join("output/reports",f) for f in sorted(os.listdir("output/reports")) if f.endswith(".png")]
    if imgs:
        with open(OUT_PDF,"wb") as f: f.write(img2pdf.convert(imgs))
        log(f"[OK] report -> {OUT_PDF}")
    else:
        log("[WARN] no figures generated; PDF skipped")

def main():
    try:
        merge_by_year()
        combine_all()
        report()
        log("[DONE] Safe ULTRA complete")
        print("✅ Statcast ULTRA Safe Completed Successfully")
    except Exception as e:
        log(f"[FAIL] {e}")
        print("❌ Error:", e)

if __name__=="__main__":
    main()
