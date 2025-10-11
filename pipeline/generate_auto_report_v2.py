#!/usr/bin/env python3
import argparse, os
from pathlib import Path
import duckdb, pandas as pd
import matplotlib.pyplot as plt
import img2pdf
from datetime import datetime as dt

OUT = Path("output"); REP = OUT/"reports"
OUT.mkdir(parents=True, exist_ok=True); REP.mkdir(parents=True, exist_ok=True)

def set_theme(theme="mono", accent="red"):
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

def ev_la_candidates(cols:set):
    ev_opts = ["launch_speed","exit_velocity","hit_speed","EV","ev"]
    la_opts = ["launch_angle","hit_angle","LA","la"]
    ev = next((c for c in ev_opts if c in cols), None)
    la = next((c for c in la_opts if c in cols), None)
    return ev, la

def caption(ax, text):
    ax.text(0.0, -0.12, text, transform=ax.transAxes, ha="left", va="top", fontsize=9)

def page_title(title, subtitle="", logo=None, save_to=None):
    fig, ax = plt.subplots(figsize=(8.27, 11.69)) # A4 portrait
    ax.axis("off")
    ax.text(0.05, 0.85, title, fontsize=28, weight="bold", transform=ax.transAxes)
    if subtitle:
        ax.text(0.05, 0.80, subtitle, fontsize=12, transform=ax.transAxes)
    if logo and Path(logo).exists():
        import matplotlib.image as mpimg
        try:
            img=mpimg.imread(logo); ax.imshow(img, extent=(0.70,0.97,0.82,0.97), transform=ax.transAxes)
        except Exception: pass
    caption(ax, f"Generated {dt.utcnow().strftime('%Y-%m-%d %H:%M UTC')}  •  Data: Statcast (2015–)")
    if save_to: fig.savefig(save_to, dpi=150, bbox_inches="tight")
    plt.close(fig)

def simple_hist(ax, s, title, xlabel):
    s = pd.to_numeric(s, errors="coerce").dropna()
    if len(s)==0: s = pd.Series([0.0])
    ax.hist(s, bins=40)
    ax.set_title(title); ax.set_xlabel(xlabel); ax.grid(True, alpha=.3)

def simple_scatter(ax, x, y, title, xlabel, ylabel):
    x=pd.to_numeric(x, errors="coerce"); y=pd.to_numeric(y, errors="coerce")
    m = x.notna() & y.notna()
    if m.sum()==0: ax.text(0.5,0.5,"No data",ha="center"); 
    else: ax.scatter(x[m], y[m], s=3, alpha=.35)
    ax.set_title(title); ax.set_xlabel(xlabel); ax.set_ylabel(ylabel); ax.grid(True, alpha=.3)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--theme",default="mono")
    ap.add_argument("--accent",default="red")
    ap.add_argument("--logo",default="")
    ap.add_argument("--out",default="output/reports/auto_report_v2.pdf")
    args=ap.parse_args()
    set_theme(args.theme, args.accent)

    con=duckdb.connect()
    # 실데이터 스키마 자동 탐지 → EV/LA 별칭 포함한 원본 뷰
    cols=set(con.execute("SELECT * FROM read_parquet('output/statcast_ultra_full_clean.parquet') LIMIT 0").fetchdf().columns)
    ev, la = ev_la_candidates(cols)
    evx = f"TRY_CAST({ev} AS DOUBLE)" if ev else "NULL"
    lax = f"TRY_CAST({la} AS DOUBLE)" if la else "NULL"
    con.execute(
        "CREATE OR REPLACE VIEW sc_base AS "
        f"SELECT *, {evx} AS launch_speed, {lax} AS launch_angle "
        "FROM read_parquet('output/statcast_ultra_full_clean.parquet')"
    )

    # 샘플링: DuckDB 표준 (SAMPLE 제거 → ORDER BY random() LIMIT)
    df = con.execute("""
        SELECT launch_speed, launch_angle
        FROM sc_base
        WHERE launch_speed IS NOT NULL AND launch_angle IS NOT NULL
        ORDER BY random() LIMIT 5000
    """).fetchdf()

    # 페이지들
    pages=[]; tmp=[]
    p1=REP/"auto_v2_p1.png"; page_title("Automated Ops Report (Statcast)", "EV/LA overview", args.logo, p1); pages.append(p1); tmp.append(p1)
    fig,ax=plt.subplots(figsize=(8.27,5.5)); simple_scatter(ax, df["launch_speed"], df["launch_angle"], "EV vs LA (sample)", "Exit Velocity", "Launch Angle"); caption(ax,"Source: Statcast"); p2=REP/"auto_v2_p2.png"; fig.savefig(p2,dpi=150,bbox_inches="tight"); plt.close(fig); pages.append(p2); tmp.append(p2)
    fig,ax=plt.subplots(figsize=(8.27,5.5)); simple_hist(ax, df["launch_speed"], "Exit Velocity Distribution", "mph"); caption(ax,"Source: Statcast"); p3=REP/"auto_v2_p3.png"; fig.savefig(p3,dpi=150,bbox_inches="tight"); plt.close(fig); pages.append(p3); tmp.append(p3)
    fig,ax=plt.subplots(figsize=(8.27,5.5)); simple_hist(ax, df["launch_angle"], "Launch Angle Distribution", "deg"); caption(ax,"Source: Statcast"); p4=REP/"auto_v2_p4.png"; fig.savefig(p4,dpi=150,bbox_inches="tight"); plt.close(fig); pages.append(p4); tmp.append(p4)
    # 요약 텍스트 페이지
    fig,ax=plt.subplots(figsize=(8.27,11.69)); ax.axis("off")
    ax.text(0.05,0.95,"Quick Summary",fontsize=16,weight="bold",transform=ax.transAxes)
    try:
        ax.text(0.05,0.90,f"Rows sampled: {len(df):,}", transform=ax.transAxes)
        ax.text(0.05,0.86,f"EV mean/50th/95th: {df.launch_speed.mean():.1f} / {df.launch_speed.median():.1f} / {df.launch_speed.quantile(.95):.1f}", transform=ax.transAxes)
        ax.text(0.05,0.82,f"LA mean/50th/95th: {df.launch_angle.mean():.1f} / {df.launch_angle.median():.1f} / {df.launch_angle.quantile(.95):.1f}", transform=ax.transAxes)
    except Exception: pass
    caption(ax,"All figures from real Statcast parquet in output/")
    p5=REP/"auto_v2_p5.png"; fig.savefig(p5,dpi=150,bbox_inches="tight"); plt.close(fig); pages.append(p5); tmp.append(p5)

    with open(args.out,"wb") as f:
        f.write(img2pdf.convert([str(p) for p in pages]))
    print(f"[DONE] auto report v2 -> {args.out} (pages={len(pages)})")
    # 임시 PNG들은 남겨둠(디버그용)
if __name__=="__main__": main()
