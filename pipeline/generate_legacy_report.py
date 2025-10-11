#!/usr/bin/env python3
import argparse
from pathlib import Path
import duckdb, pandas as pd
import matplotlib.pyplot as plt
import img2pdf
from datetime import datetime as dt

OUT=Path("output"); REP=OUT/"reports"
OUT.mkdir(parents=True, exist_ok=True); REP.mkdir(parents=True, exist_ok=True)

def set_theme():
    import matplotlib as mpl
    mpl.rcParams.update({
        "figure.facecolor":"white",
        "axes.facecolor":"white",
        "axes.edgecolor":"black",
        "axes.labelcolor":"black",
        "xtick.color":"black", "ytick.color":"black",
        "grid.color":"#DDDDDD",
        "font.family":["DejaVu Sans"],
        "axes.titleweight":"bold",
    })

def caption(ax, text): ax.text(0.0, -0.12, text, transform=ax.transAxes, ha="left", va="top", fontsize=9)

def page_title(title, subtitle="", logo=None, save_to=None):
    fig, ax = plt.subplots(figsize=(8.27, 11.69))
    ax.axis("off")
    ax.text(0.05, 0.85, title, fontsize=26, weight="bold", transform=ax.transAxes)
    if subtitle: ax.text(0.05, 0.80, subtitle, fontsize=12, transform=ax.transAxes)
    if logo and Path(logo).exists():
        import matplotlib.image as mpimg
        try:
            img=mpimg.imread(logo); ax.imshow(img, extent=(0.70,0.97,0.82,0.97), transform=ax.transAxes)
        except Exception: pass
    caption(ax, f"Generated {dt.utcnow().strftime('%Y-%m-%d %H:%M UTC')}  •  Data: Lahman/Chadwick (1901–2014+) ")
    if save_to: fig.savefig(save_to, dpi=150, bbox_inches="tight")
    plt.close(fig)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--theme",default="mono")
    ap.add_argument("--accent",default="red")
    ap.add_argument("--logo",default="")
    ap.add_argument("--out",default="output/reports/legacy_report_v2.pdf")
    args=ap.parse_args()
    set_theme()

    con=duckdb.connect()
    # OPS 폴백 뷰 (OPS 있으면 사용, 없으면 OBP+SLG)
    cols=set(con.execute("SELECT * FROM read_parquet('output/player_cards_all.parquet') LIMIT 0").fetchdf().columns)
    if "OPS" in cols:
        ops_expr="TRY_CAST(OPS AS DOUBLE)"
    elif "OBP" in cols and "SLG" in cols:
        ops_expr="TRY_CAST(OBP AS DOUBLE)+TRY_CAST(SLG AS DOUBLE)"
    else:
        ops_expr="NULL"
    con.execute(
        "CREATE OR REPLACE VIEW cards_ops AS "
        f"SELECT *, {ops_expr} AS OPS FROM read_parquet('output/player_cards_all.parquet')"
    )

    ops = con.execute("""
        SELECT OPS FROM cards_ops
        WHERE OPS IS NOT NULL
        ORDER BY random() LIMIT 20000
    """).fetchdf()

    pages=[]; tmp=[]
    p1=REP/"legacy_v2_p1.png"; page_title("Legacy Era Report (1901–2014)", "OPS distribution", args.logo, p1); pages.append(p1); tmp.append(p1)

    fig,ax=plt.subplots(figsize=(8.27,5.5))
    s=pd.to_numeric(ops["OPS"], errors="coerce").dropna()
    if len(s)==0: s=pd.Series([0.0])
    ax.hist(s, bins=50); ax.set_title("OPS Distribution (sample)"); ax.set_xlabel("OPS"); ax.grid(True, alpha=.3)
    caption(ax, "Source: Lahman/Chadwick consolidated")
    p2=REP/"legacy_v2_p2.png"; fig.savefig(p2,dpi=150,bbox_inches="tight"); plt.close(fig); pages.append(p2); tmp.append(p2)

    # 시즌별 상자그림
    df_year = con.execute("""
        SELECT TRY_CAST(season AS INT) AS season, OPS
        FROM cards_ops
        WHERE OPS IS NOT NULL AND season BETWEEN 1901 AND 2014
        ORDER BY random() LIMIT 40000
    """).fetchdf()
    fig,ax=plt.subplots(figsize=(8.27,5.5))
    if not df_year.empty:
        # 간단히 5개 구간으로 묶어 상자그림
        df_year["bucket"]=(df_year["season"]//25)*25
        df_year.boxplot(column="OPS", by="bucket", ax=ax)
        ax.set_title("OPS by era (25y buckets)"); ax.set_xlabel("Era start year"); ax.grid(True, alpha=.3)
        plt.suptitle("")
    else:
        ax.text(0.5,0.5,"No data",ha="center")
    caption(ax, "Source: Lahman/Chadwick")
    p3=REP/"legacy_v2_p3.png"; fig.savefig(p3,dpi=150,bbox_inches="tight"); plt.close(fig); pages.append(p3); tmp.append(p3)

    # 요약 페이지
    fig,ax=plt.subplots(figsize=(8.27,11.69)); ax.axis("off")
    try:
        q = con.execute("SELECT MIN(season), MAX(season), COUNT(*) FROM read_parquet('output/player_cards_all.parquet')").fetchone()
        ax.text(0.05,0.95, "Coverage", fontsize=16, weight="bold", transform=ax.transAxes)
        ax.text(0.05,0.90, f"Seasons: {q[0]}–{q[1]}  •  Rows: {q[2]:,}", transform=ax.transAxes)
        ax.text(0.05,0.85, f"Samp N (OPS page): {len(ops):,}", transform=ax.transAxes)
    except Exception: pass
    caption(ax, "All figures from real Lahman/Chadwick parquet in output/")
    p4=REP/"legacy_v2_p4.png"; fig.savefig(p4,dpi=150,bbox_inches="tight"); plt.close(fig); pages.append(p4); tmp.append(p4)

    with open(args.out,"wb") as f:
        f.write(img2pdf.convert([str(p) for p in pages]))
    print(f"[DONE] legacy report v2 -> {args.out} (pages={len(pages)})")

if __name__=="__main__": main()
