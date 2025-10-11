import os, glob, duckdb, polars as pl, datetime as dt
import matplotlib.pyplot as plt
import seaborn as sns

LOG = "logs/statcast_ultra.log"
os.makedirs("logs", exist_ok=True)
os.makedirs("output/reports", exist_ok=True)
with open(LOG,"w") as f: f.write(f"[{dt.datetime.now(dt.UTC)}] Statcast ULTRA Start\n")

STATCAST_CAND = [
    "/workspaces/cogm-assistant/output/cache/statcast_clean",
    "/workspaces/cogm-assistant/output",
    "/workspaces/cogm-assistant"
]

OUT_PARQUET = "output/statcast_ultra_full.parquet"
OUT_CARDS   = "output/player_cards_ultra.csv"
OUT_SQLDB   = "output/statcast_ultra.duckdb"
OUT_PDF     = "output/reports/statcast_ultra_report.pdf"

def log(msg):
    with open(LOG,"a") as f: f.write(msg+"\n")

def safe_concat(dfs):
    return pl.concat(dfs, how="diagonal_relaxed") if dfs else None

def find_statcast_files():
    exts = (".parquet",".csv")
    files=[]
    for root in STATCAST_CAND:
        for ext in exts:
            files += glob.glob(os.path.join(root, f"**/*statcast*{ext}"), recursive=True)
    files=[f for f in files if os.path.isfile(f)]
    log(f"[FILES] total {len(files)} statcast files")
    return sorted(set(files))

def read_any(p):
    try:
        if p.endswith(".parquet"): return pl.read_parquet(p)
        else: return pl.read_csv(p, low_memory=True)
    except Exception as e:
        log(f"[SKIP] {p} ({e})"); return None

def merge_all():
    dfs=[]
    files=find_statcast_files()
    for i,p in enumerate(files,1):
        df=read_any(p)
        if df is not None: dfs.append(df)
        if i%10==0: log(f"  merged {i}/{len(files)} files")
    merged=safe_concat(dfs)
    if merged is None: raise SystemExit("no data found")
    merged.write_parquet(OUT_PARQUET)
    log(f"[OK] wrote {OUT_PARQUET} rows={merged.shape[0]} cols={len(merged.columns)}")
    return merged

def enrich_cards(stat):
    pc="output/player_cards.csv"
    if not os.path.exists(pc): 
        log("[WARN] player_cards.csv missing")
        return stat
    cards=pl.read_csv(pc)
    if "player_name" not in cards.columns:
        cards=cards.rename({"name":"player_name"})
    join=cards.join(stat,on="player_name",how="left")
    join.write_csv(OUT_CARDS)
    log(f"[OK] enriched cards rows={join.shape[0]} cols={len(join.columns)}")
    return join

def build_duckdb(df):
    db=OUT_SQLDB
    con=duckdb.connect(db)
    con.execute("CREATE OR REPLACE VIEW statcast AS SELECT * FROM df")
    log(f"[OK] DuckDB view created -> {db}")
    con.close()

def make_report(df):
    pdf=OUT_PDF
    plt.rcParams.update({'figure.max_open_warning':0})
    # SpinRate fallback
    spin_col = "SpinRate" if "SpinRate" in df.columns else ("spin" if "spin" in df.columns else ("avg_spin" if "avg_spin" in df.columns else None))
    # 1. yearly trend
    if "year" in df.columns:
        df_year=df.lazy().group_by("year").agg([
            pl.col("xwOBA").mean().alias("xwOBA"),
            pl.col("avg_ev").mean().alias("avg_ev"),
            pl.col("hardhit_rate").mean().alias("hardhit_rate")
        ]).collect().sort("year")
        plt.figure(figsize=(8,6))
        plt.plot(df_year["year"],df_year["xwOBA"],label="xwOBA")
        plt.plot(df_year["year"],df_year["avg_ev"],label="Avg EV")
        plt.plot(df_year["year"],df_year["hardhit_rate"],label="HardHit%")
        plt.legend();plt.title("Yearly Trends 2015–2025")
        plt.savefig("output/reports/trend_year.png"); plt.close()
    # 2. EV–LA heatmap
    if {"EV","LA"} <= set(df.columns):
        sns.kdeplot(data=df.to_pandas().sample(min(5000,len(df))),x="LA",y="EV",fill=True,thresh=0.05)
        plt.title("EV–LA Density"); plt.savefig("output/reports/ev_la_heatmap.png"); plt.close()
    # 3. Pitch Type averages
    if "pitch_type" in df.columns:
        cols=["pitch_type"]
        for c in ["EV","xwOBA",spin_col]:
            if c and c in df.columns: cols.append(c)
        pt=df.lazy().select(cols).group_by("pitch_type").mean().collect()
        plt.figure(figsize=(9,5))
        sns.barplot(pt.to_pandas(),x="pitch_type",y=pt.columns[1])
        plt.title(f"Avg {pt.columns[1]} by Pitch Type")
        plt.savefig("output/reports/ev_pitchtype.png"); plt.close()
    # 4. Zone density
    if {"PitchLocX","PitchLocZ"} <= set(df.columns):
        sns.kdeplot(data=df.to_pandas().sample(min(3000,len(df))),x="PitchLocX",y="PitchLocZ",fill=True,thresh=0.1)
        plt.title("Pitch Location Density"); plt.savefig("output/reports/zone_density.png"); plt.close()
    # 5. HardHit vs Barrel
    if {"Hard","Barrel","EV"} <= set(df.columns):
        plt.figure(figsize=(8,5))
        sns.scatterplot(df.to_pandas().sample(min(5000,len(df))),x="Hard",y="EV",hue="Barrel",alpha=0.6)
        plt.title("HardHit vs Barrel vs EV"); plt.savefig("output/reports/hardhit_barrel_ev.png"); plt.close()
    log("[OK] visual reports generated")
    import img2pdf
    imgs=[f"output/reports/{f}" for f in os.listdir("output/reports") if f.endswith(".png")]
    with open(pdf,"wb") as f: f.write(img2pdf.convert(imgs))
    log(f"[OK] merged report -> {pdf}")

def main():
    try:
        df=merge_all()
        enrich_cards(df)
        build_duckdb(df)
        make_report(df)
        log(f"[DONE] Completed at {dt.datetime.now(dt.UTC)}")
        print("✅ Statcast ULTRA Pipeline Completed Successfully")
    except Exception as e:
        log(f"[FAIL] {e}")
        print("❌ Error:", e)

if __name__=="__main__":
    main()
