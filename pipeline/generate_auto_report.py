# /workspaces/cogm-assistant/pipeline/generate_auto_report.py
import os, json, datetime as dt, duckdb, math, traceback
os.environ.setdefault("MPLBACKEND","Agg")
import matplotlib.pyplot as plt
import img2pdf

ROOT  = "/workspaces/cogm-assistant"
OUT   = f"{ROOT}/output"
SUM   = f"{OUT}/summaries"
REP   = f"{OUT}/reports"
LOG   = f"{ROOT}/logs/generate_auto_report.log"
PDF   = f"{REP}/auto_report.pdf"

os.makedirs(REP, exist_ok=True)
os.makedirs(os.path.dirname(LOG), exist_ok=True)

def log(msg):
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    open(LOG,"a",encoding="utf-8").write(f"[{now}] {msg}\n")
    print(msg)

def have(p): 
    try:
        return os.path.isfile(p) and os.path.getsize(p) > 0
    except: 
        return False

def fig_save(path, fig):
    # 알파 채널 제거(transparent=False, 흰 배경)
    fig.savefig(path, dpi=160, bbox_inches="tight", facecolor="white", edgecolor="white", transparent=False)
    plt.close(fig)

def try_read_df(sql:str, params=None):
    try:
        con = duckdb.connect()
        return con.execute(sql, params or []).fetchdf()
    except Exception as e:
        log(f"[SKIP] read fail: {e}")
        return None

def cover_page():
    fig = plt.figure(figsize=(11.69, 8.27))  # A4 landscape
    ax = fig.add_subplot(111)
    ax.axis("off")
    title = "MLB Intelligence — Auto Report"
    sub = "1901–2014 (Lahman+Chadwick) + 2015–2025 (+Statcast)"
    when = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # coverage 요약
    cov = {}
    val_json = f"{SUM}/full_system_validation.json"
    cov_json = f"{SUM}/statcast_coverage_status.json"
    try:
        if have(val_json): cov = json.load(open(val_json,"r"))
        elif have(cov_json): cov = json.load(open(cov_json,"r"))
    except Exception as e:
        log(f"[SKIP] coverage json read: {e}")

    lines = [
        f"Generated: {when}",
        f"Master: {cov.get('paths',{}).get('master','output/statcast_ultra_full_clean.parquet')}",
        f"Rows: {cov.get('coverage',{}).get('rows_in_master','?')}  Range: {cov.get('coverage',{}).get('min_year','?')}–{cov.get('coverage',{}).get('max_year','?')}  2025: {cov.get('coverage',{}).get('has_2025','?')}",
    ]
    ax.text(0.02, 0.80, title, fontsize=26, fontweight="bold")
    ax.text(0.02, 0.72, sub, fontsize=14)
    ax.text(0.02, 0.64, "\n".join(lines), fontsize=11)
    return fig

def trends_page():
    # trend_entropy_repeat_by_year.csv 있으면 사용, 없으면 요약 계산 시도
    trend_csv = f"{SUM}/trend_entropy_repeat_by_year.csv"
    if have(trend_csv):
        df = try_read_df("SELECT * FROM read_csv_auto(?)", [trend_csv])
    else:
        # fallback: pitcher_season_summary에서 연도평균
        pss = f"{SUM}/pitcher_season_summary.parquet"
        if not have(pss): return None
        df = try_read_df("""
          SELECT season as year,
                 avg(usage_entropy) as avg_usage_entropy,
                 avg(repeat_rate)   as avg_repeat_rate
          FROM read_parquet(?)
          GROUP BY 1 ORDER BY 1
        """,[pss])
    if df is None or df.empty: return None
    fig = plt.figure(figsize=(11.69, 8.27)); ax = fig.add_subplot(111)
    ax.plot(df["year"], df["avg_usage_entropy"], marker="o", label="Entropy (avg)")
    ax.plot(df["year"], df["avg_repeat_rate"],   marker="o", label="Repeat Rate (avg)")
    ax.set_title("Yearly Trends — Entropy & Repeat Rate")
    ax.set_xlabel("Year"); ax.set_ylabel("Value"); ax.grid(True, alpha=0.3); ax.legend()
    return fig

def topk_page(title, path_csv, value_col, k=10, ascending=False):
    if not have(path_csv): return None
    df = try_read_df("SELECT * FROM read_csv_auto(?)", [path_csv])
    if df is None or df.empty: return None
    df = df.sort_values(value_col, ascending=ascending).head(k)
    fig = plt.figure(figsize=(11.69, 8.27)); ax = fig.add_subplot(111)
    ax.barh(df["name"].astype(str).fillna(""), df[value_col])
    ax.set_title(title); ax.invert_yaxis(); ax.grid(True, axis="x", alpha=0.2)
    return fig

def heatmap_from_parquet(title, path_parq, rows=30):
    if not have(path_parq): return None
    df = try_read_df("SELECT * FROM read_parquet(?) LIMIT ?", [path_parq, rows])
    if df is None or df.empty: return None
    # 피벗 가능한 두 열만 있을 경우 대비
    cols = list(df.columns)
    if len(cols) < 3: return None
    # 간단히 첫 3열 활용한 count pivot
    a,b = cols[0], cols[1]
    df["_v"]=1
    pv = df.pivot_table(index=a, columns=b, values="_v", aggfunc="sum", fill_value=0)
    fig = plt.figure(figsize=(11.69, 8.27)); ax = fig.add_subplot(111)
    im = ax.imshow(pv.values, aspect="auto", interpolation="nearest")
    ax.set_title(title); ax.set_xlabel(b); ax.set_ylabel(a)
    ax.set_xticks(range(len(pv.columns))); ax.set_xticklabels(pv.columns, rotation=90, fontsize=8)
    ax.set_yticks(range(len(pv.index)));   ax.set_yticklabels(pv.index, fontsize=8)
    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    return fig

def distribution_page(title, parq_path, col, bins=30):
    if not have(parq_path): return None
    df = try_read_df(f"SELECT TRY_CAST({col} AS DOUBLE) AS v FROM read_parquet(?) WHERE v IS NOT NULL", [parq_path])
    if df is None or df.empty: return None
    fig = plt.figure(figsize=(11.69, 8.27)); ax = fig.add_subplot(111)
    ax.hist(df["v"], bins=bins)
    ax.set_title(f"{title} — Distribution"); ax.set_xlabel(col); ax.set_ylabel("Count"); ax.grid(True, alpha=0.3)
    return fig

def validation_page():
    jpath = f"{SUM}/full_system_validation.json"
    if not have(jpath): return None
    try:
        j = json.load(open(jpath,"r"))
    except Exception as e:
        log(f"[SKIP] validation json read: {e}"); return None
    fig = plt.figure(figsize=(11.69, 8.27)); ax = fig.add_subplot(111); ax.axis("off")
    ax.text(0.02, 0.94, "Validation Snapshot", fontsize=18, fontweight="bold")
    ax.text(0.02, 0.88, json.dumps(j.get("coverage",{}), indent=2), fontsize=10)
    arts = j.get("artefacts",{})
    ax.text(0.45, 0.88, "Artefacts:", fontsize=12, fontweight="bold")
    y = 0.84
    for k in sorted(arts.keys()):
        ax.text(0.45, y, f"{k}: {'OK' if arts[k] else 'MISSING'}", fontsize=10); y -= 0.03
    return fig

def main():
    open(LOG,"w").write(f"[{dt.datetime.now(dt.timezone.utc)}] auto report start\n")
    figs = []

    # 1) 커버
    try:
        f = cover_page()
        if f: figs.append(("cover", f))
    except Exception:
        log("[SKIP] cover_page:\n"+traceback.format_exc())

    # 2) 트렌드
    try:
        f = trends_page()
        if f: figs.append(("trend", f))
    except Exception:
        log("[SKIP] trends_page:\n"+traceback.format_exc())

    # 3) 리더보드(엔트로피 TOP10 / 반복률 HIGH/LOW)
    try:
        f = topk_page("Entropy Top10", f"{SUM}/leaderboard_entropy_top10.csv", "usage_entropy", ascending=False)
        if f: figs.append(("lb_entropy", f))
    except Exception:
        log("[SKIP] lb_entropy:\n"+traceback.format_exc())
    try:
        f = topk_page("Repeat Rate HIGH Top10", f"{SUM}/leaderboard_repeat_high_top10.csv", "repeat_rate", ascending=False)
        if f: figs.append(("lb_rep_hi", f))
    except Exception:
        log("[SKIP] lb_rep_hi:\n"+traceback.format_exc())
    try:
        f = topk_page("Repeat Rate LOW Top10", f"{SUM}/leaderboard_repeat_low_top10.csv", "repeat_rate", ascending=True)
        if f: figs.append(("lb_rep_lo", f))
    except Exception:
        log("[SKIP] lb_rep_lo:\n"+traceback.format_exc())

    # 4) n-그램 / run-length / zone / batter LA/EV
    try:
        f = heatmap_from_parquet("Pitch N-gram 2 — Sample", f"{SUM}/pitch_ngram2.parquet")
        if f: figs.append(("ng2", f))
    except Exception:
        log("[SKIP] ng2:\n"+traceback.format_exc())
    try:
        f = heatmap_from_parquet("Pitch N-gram 3 — Sample", f"{SUM}/pitch_ngram3.parquet")
        if f: figs.append(("ng3", f))
    except Exception:
        log("[SKIP] ng3:\n"+traceback.format_exc())
    try:
        f = distribution_page("Run-length (group count) — Sample", f"{SUM}/run_length.parquet", "run_len", bins=25)
        if f: figs.append(("runlen", f))
    except Exception:
        log("[SKIP] runlen:\n"+traceback.format_exc())
    try:
        f = heatmap_from_parquet("Zone Repeat/Transition — Sample", f"{SUM}/zone_repeat_transition.parquet")
        if f: figs.append(("zone", f))
    except Exception:
        log("[SKIP] zone:\n"+traceback.format_exc())
    try:
        f = distribution_page("Batter LA abs diff mean — Sample", f"{SUM}/batter_la_ev_variability.parquet", "la_abs_diff_mean", bins=25)
        if f: figs.append(("batter_var", f))
    except Exception:
        log("[SKIP] batter_var:\n"+traceback.format_exc())

    # 5) 검증 스냅샷
    try:
        f = validation_page()
        if f: figs.append(("validation", f))
    except Exception:
        log("[SKIP] validation_page:\n"+traceback.format_exc())

    # 6) 이미지 → PDF
    imgs = []
    for name, f in figs:
        p = f"{REP}/auto_{name}.png"
        fig_save(p, f)
        if have(p): imgs.append(p)
    if imgs:
        with open(PDF, "wb") as out:
            out.write(img2pdf.convert(imgs))
        log(f"[OK] report -> {PDF} (pages={len(imgs)})")
    else:
        log("[SKIP] no pages, pdf not created")
    log("[DONE] auto report end")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"[WARN] outer: {e}")
        # 항상 종료코드 0 유지
        pass
