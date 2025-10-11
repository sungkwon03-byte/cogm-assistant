import os, glob, duckdb, polars as pl, datetime as dt
import matplotlib.pyplot as plt

LOG = "logs/statcast_master.log"
os.makedirs("logs", exist_ok=True)
os.makedirs("output", exist_ok=True)
with open(LOG, "w") as f: f.write(f"[{dt.datetime.now(dt.UTC)}] Statcast Master Pipeline Start\n")

# ----------------------------------------------------------------------
# 경로 설정
# ----------------------------------------------------------------------
STATCAST_CAND = [
    "/workspaces/cogm-assistant/output/cache/statcast_clean",
    "/workspaces/cogm-assistant/output",
    "/workspaces/cogm-assistant"
]
OUT_FULL = "output/statcast_master_full.parquet"
OUT_CARDS = "output/player_cards_enriched_full.csv"
OUT_PDF   = "output/statcast_trend_report.pdf"

# ----------------------------------------------------------------------
# 헬퍼
# ----------------------------------------------------------------------
def safe_concat(dfs):
    if not dfs: return None
    return pl.concat(dfs, how="diagonal_relaxed")

def find_statcast_files():
    exts = (".parquet", ".csv")
    found = []
    for root in STATCAST_CAND:
        for ext in exts:
            found += glob.glob(os.path.join(root, f"**/*statcast*{ext}"), recursive=True)
    found = [f for f in found if os.path.isfile(f)]
    with open(LOG, "a") as f: f.write(f"[FILES] {len(found)} Statcast files found\n")
    return sorted(set(found))

def read_any(path):
    try:
        if path.endswith(".parquet"):
            return pl.read_parquet(path)
        else:
            return pl.read_csv(path, low_memory=True)
    except Exception as e:
        with open(LOG, "a") as f: f.write(f"[SKIP] {path}: {e}\n")
        return None

# ----------------------------------------------------------------------
# 1️⃣ 병합: 모든 Statcast 데이터
# ----------------------------------------------------------------------
def merge_statcast_all():
    files = find_statcast_files()
    dfs = []
    for i,f in enumerate(files,1):
        df = read_any(f)
        if df is not None:
            dfs.append(df)
        if i % 5 == 0:
            with open(LOG, "a") as g: g.write(f"  merged {i}/{len(files)} files\n")
    merged = safe_concat(dfs)
    if merged is None:
        raise SystemExit("❌ No valid Statcast files found.")
    merged = merged.unique(subset=[c for c in merged.columns if c in ("mlbam","year","player_name")], keep="first")
    merged.write_parquet(OUT_FULL)
    with open(LOG,"a") as f: f.write(f"[OK] wrote {OUT_FULL} rows={merged.shape[0]} cols={len(merged.columns)}\n")
    return merged

# ----------------------------------------------------------------------
# 2️⃣ 병합: Player Cards enrich
# ----------------------------------------------------------------------
def enrich_player_cards(statcast_df: pl.DataFrame):
    cards_path = "output/player_cards.csv"
    if not os.path.exists(cards_path):
        with open(LOG,"a") as f: f.write("[WARN] player_cards.csv not found\n")
        return
    cards = pl.read_csv(cards_path)
    join_key = "player_name" if "player_name" in statcast_df.columns else "name"
    if join_key not in cards.columns:
        cards = cards.rename({"name": join_key})
    joined = cards.join(statcast_df, on=join_key, how="left")
    joined.write_csv(OUT_CARDS)
    with open(LOG,"a") as f: f.write(f"[OK] enriched player_cards -> {OUT_CARDS} rows={joined.shape[0]} cols={len(joined.columns)}\n")
    return joined

# ----------------------------------------------------------------------
# 3️⃣ 시각화 리포트 생성
# ----------------------------------------------------------------------
def build_report(df):
    pdf_path = OUT_PDF
    df = df.lazy()
    agg = (df.select(["year","xwOBA","avg_ev","hardhit_rate"])
             .group_by("year")
             .mean()
             .collect()
             .sort("year"))
    if agg.height == 0:
        with open(LOG,"a") as f: f.write("[WARN] insufficient data for visualization\n")
        return
    years = agg["year"].to_list()
    plt.figure(figsize=(8,6))
    plt.plot(years, agg["xwOBA"], label="xwOBA", marker="o")
    plt.plot(years, agg["avg_ev"], label="Avg EV", marker="s")
    plt.plot(years, agg["hardhit_rate"], label="HardHit%", marker="^")
    plt.title("Statcast Trends 2015–2025")
    plt.xlabel("Year")
    plt.ylabel("Metric Value")
    plt.legend()
    plt.tight_layout()
    plt.savefig(pdf_path, format="pdf")
    with open(LOG,"a") as f: f.write(f"[OK] trend report -> {pdf_path}\n")

# ----------------------------------------------------------------------
# 4️⃣ 실행 메인
# ----------------------------------------------------------------------
def main():
    try:
        merged = merge_statcast_all()
        enrich_player_cards(merged)
        build_report(merged)
        with open(LOG,"a") as f: f.write(f"[DONE] completed at {dt.datetime.now(dt.UTC)}\n")
    except Exception as e:
        with open(LOG,"a") as f: f.write(f"[FAIL] {e}\n")
    finally:
        print("✅ Statcast Master Pipeline finished (check logs/statcast_master.log)")

if __name__ == "__main__":
    main()
