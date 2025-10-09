#!/usr/bin/env python3
import os, sys, json, datetime
import pandas as pd
import numpy as np

NOW = datetime.datetime.now(datetime.timezone.utc).isoformat()
REPORT = "output/tests_day1_57.txt"
PASS = FAIL = WARN = 0
LINES = []

def log(line):
    LINES.append(line)
    print(line)

def out():
    with open(REPORT, "w", encoding="utf-8") as f:
        f.write("\n".join(LINES))

def expect(cond, ok, bad):
    global PASS, FAIL
    if bool(cond):
        PASS += 1; log(f"[OK] {ok}")
        return True
    else:
        FAIL += 1; log(f"[FAIL] {bad}")
        return False

def warn(cond, msg):
    global WARN
    if cond:
        WARN += 1; log(f"[WARN] {msg}")

# ---------- 시작 ----------
log(f"[START] Smoke Tests Day1–57 @ {NOW}")

# 0) 필수 산출물 존재
req_files = [
    "output/player_cards.csv",
    "output/player_cards_schema.json",
    "output/team_agg.csv",
    "output/verify_report.txt",
]
for p in req_files:
    expect(os.path.exists(p) and os.path.getsize(p) > 0, f"존재/비어있지 않음: {p}", f"누락/빈 파일: {p}")
expect(os.path.isdir("logs"), "logs 디렉토리 존재", "logs 디렉토리 없음")

# 1) player_cards 검증
pc = pd.read_csv("output/player_cards.csv")
expect(len(pc) > 0, "player_cards 행수 > 0", "player_cards=0행")
allowed = {"MLB","KBO","MiLB"}
expect(set(pc["league"]).issubset(allowed), f"league 값 허용세트: {sorted(set(pc['league']))}", f"비허용 리그 포함: {sorted(set(pc['league'])-allowed)}")
# PK 유니크
if {"player_uid","season","league"}.issubset(pc.columns):
    uniq = len(pc.drop_duplicates(["player_uid","season","league"]))
    expect(uniq == len(pc), "PK 유니크 통과", f"PK 중복 {len(pc)-uniq}건")
else:
    expect(False, "", "PK 컬럼 누락")

# 핵심 컬럼 null → 경고(정보성)
core = ["player_uid","season","league","team_id","name_full","age","pa_or_bf","war"]
missing_core = [c for c in core if c not in pc.columns]
expect(len(missing_core)==0, "핵심 컬럼 모두 존재", f"핵심 컬럼 누락: {missing_core}")
if len(missing_core)==0:
    nulls = {c:int(pc[c].isna().sum()) for c in core}
    warn(any(v>0 for v in nulls.values()), f"핵심 컬럼 null 존재: {nulls}")

# 2) team_agg 검증
ta = pd.read_csv("output/team_agg.csv")
expect(len(ta) > 0, "team_agg 행수 > 0", "team_agg=0행")
need = {"league","season","group_role","group_id","players","total_war","avg_war","total_pa_bf","avg_age"}
expect(need.issubset(ta.columns), "team_agg 필수 컬럼 존재", f"team_agg 누락 컬럼: {sorted(list(need-set(ta.columns)))}")
expect(set(ta["league"]).issubset(allowed), f"team_agg league 값 허용세트", f"team_agg 비허용 리그 포함: {sorted(set(ta['league'])-allowed)}")
# 키 유니크
expect(not ta[["league","season","group_role","group_id"]].duplicated().any(), "team_agg 키 유니크 통과", "team_agg 키 중복 존재")
# org/team 라벨 합리성
if "src_league" in ta.columns:
    bad_milb_org = ta[(ta["league"]=="MiLB") & (ta["group_role"]=="org")]
    warn(len(bad_milb_org)>0, f"MiLB 라벨인데 org {len(bad_milb_org)}건 — 라벨링 점검 필요")

# 합계 일치(정보성): 리그별 total_war 합 = league_total_war 합
if {"league_total_war"}.issubset(ta.columns):
    s1 = ta.groupby(["league","season"])["total_war"].sum().round(6)
    s2 = ta.groupby(["league","season"])["league_total_war"].first().fillna(0).round(6)
    # tolerance
    diff = (s1 - s2).abs()
    warn((diff > 1e-3).any(), f"합계 불일치: {diff[diff>1e-3].to_dict()}")

# 3) verify_report 존재 및 주요 문구
vr = open("output/verify_report.txt","r",encoding="utf-8",errors="ignore").read()
expect("[DONE] verify_report 저장 완료" in vr, "verify_report 완료 문구", "verify_report 완료 문구 없음")

# 4) 로그 최소 1건 이상
log_files = [p for p in os.listdir("logs") if os.path.isfile(os.path.join("logs",p))]
expect(len(log_files)>0, f"logs 파일 개수: {len(log_files)}", "logs 파일 없음")

# 5) 최종 집계
log(f"[SUMMARY] PASS={PASS}, WARN={WARN}, FAIL={FAIL}")
status = "PASS" if FAIL==0 else "FAIL"
log(f"[END] Smoke Tests status={status}")
out()
sys.exit(0 if FAIL==0 else 2)
