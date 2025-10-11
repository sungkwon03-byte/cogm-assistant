#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, json, datetime as dt, subprocess, shutil
import duckdb, pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT="/workspaces/cogm-assistant"
OUT=f"{ROOT}/output"; SUM=f"{OUT}/summaries"; REP=f"{OUT}/reports"
LOG=f"{ROOT}/logs/mock_and_calendar_wrappers.log"
os.makedirs(SUM, exist_ok=True); os.makedirs(REP, exist_ok=True); os.makedirs(os.path.dirname(LOG), exist_ok=True)

def log(m):
    ts=dt.datetime.now(dt.timezone.utc).isoformat()
    print(f"[{ts}] {m}")
    with open(LOG,"a",encoding="utf-8") as f: f.write(f"[{ts}] {m}\n")

def prefer_master():
    for p in [f"{OUT}/statcast_ultra_full_clean.parquet", f"{OUT}/statcast_ultra_full.parquet"]:
        if os.path.isfile(p): return p
    return None

def run_if_exists(py_path, args):
    if os.path.isfile(py_path):
        try:
            cp=subprocess.run(["python3", py_path, *args], check=False, capture_output=True, text=True)
            log(f"[EXT] ran {py_path} rc={cp.returncode}")
            if cp.stdout: log(cp.stdout.strip()[:2000])
            if cp.stderr: log(cp.stderr.strip()[:2000])
            return True
        except Exception as e:
            log(f"[EXT] fail {py_path}: {e}")
    return False

def fallback_mock_trades():
    # ps 요약 기반 간이 밸류 → 상위 N 매칭
    try:
        con=duckdb.connect()
        if not os.path.isfile(f"{SUM}/pitcher_season_summary.parquet"):
            log("[MOCK] ps summary missing, skip fallback")
            return
        df=con.execute("""
          SELECT season, pitcher_id,
                 COALESCE(usage_entropy,0) AS ent,
                 COALESCE(repeat_rate,0)   AS rpt
          FROM read_parquet('output/summaries/pitcher_season_summary.parquet')
          WHERE season IN (2023,2024,2025)
        """).fetchdf()
        if df.empty:
            log("[MOCK] ps empty")
            return
        # 간이 밸류: V = ent * (1 - |rpt-0.5|)
        df["value"]=df["ent"]*(1-abs(df["rpt"]-0.5))
        df=df.sort_values(["season","value"], ascending=[True,False])
        # 두 팀 레이블이 없으므로 가상 팀 A/B로 페어링
        pairs=[]
        for y,grp in df.groupby("season"):
            g=grp.head(20).reset_index(drop=True)
            for i in range(0, min(10,len(g)-1), 2):
                a=g.iloc[i]; b=g.iloc[i+1]
                pairs.append({
                    "season": int(y),
                    "from_team": "TEAM_A","to_team":"TEAM_B",
                    "offer":{"pitcher_id": str(a["pitcher_id"]), "value": float(a["value"])},
                    "ask":{"pitcher_id": str(b["pitcher_id"]), "value": float(b["value"])},
                    "delta_value": float(a["value"]-b["value"]),
                    "status": "prototype"
                })
        out=f"{REP}/mock_trades_sample.json"
        with open(out,"w",encoding="utf-8") as f: json.dump({"generated_at":dt.datetime.now(dt.timezone.utc).isoformat(),"trades":pairs},f,indent=2)
        log(f"[MOCK] wrote {out} ({len(pairs)} proposals)")
    except Exception as e:
        log(f"[MOCK] fallback error: {e}")

def fallback_schedule():
    # 스케줄 소스가 없으니 master에서 game_pk/연도 월별 카운트로 ‘혼잡도’ 대체 지표 생성
    try:
        parq=prefer_master()
        if not parq:
            log("[SCHED] master missing, skip")
            return
        con=duckdb.connect()
        # game_pk가 날짜를 내포하지 않을 수 있어 월 추정 불가 → 연도 단위 카운트만이라도
        df=con.execute("""
          SELECT CAST(year AS INT) AS year, COUNT(*) AS n_rows
          FROM read_parquet(?)
          GROUP BY 1 ORDER BY 1
        """,[parq]).fetchdf()
        if df.empty:
            log("[SCHED] no rows")
            return
        # 저장
        out=f"{SUM}/schedule_analysis_summary.csv"
        df.to_csv(out,index=False)
        # 간단 플롯(연도별 행수 = 노이즈이지만 대용)
        plt.figure()
        plt.plot(df["year"], df["n_rows"], marker="o")
        plt.title("Schedule Congestion (proxy by rows/year)"); plt.xlabel("Year"); plt.ylabel("Rows")
        png=f"{REP}/schedule_congestion_by_month.png"
        plt.savefig(png, bbox_inches="tight"); plt.close()
        log(f"[SCHED] wrote {out}, {png}")
    except Exception as e:
        log(f"[SCHED] fallback error: {e}")

def main():
    open(LOG,"w").write(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] start\n")
    # 1) 외부 유틸 탐지·실행
    ext_ok=False
    ext_ok |= run_if_exists(f"{ROOT}/tools/mock_trade.py", ["--out", f"{REP}/mock_trades_sample.json"])
    ext_ok |= run_if_exists(f"{ROOT}/tools/schedule_tools.py", ["--out", f"{SUM}/schedule_analysis_summary.csv"])
    # 2) 없으면 대체 산출
    if not os.path.isfile(f"{REP}/mock_trades_sample.json"): fallback_mock_trades()
    if not os.path.isfile(f"{SUM}/schedule_analysis_summary.csv"): fallback_schedule()
    print("[DONE] wrappers complete")

if __name__=="__main__":
    try: main()
    finally:
        import sys; sys.exit(0)
