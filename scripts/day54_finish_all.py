#!/usr/bin/env python3
import subprocess, sys
def sh(c):
    r = subprocess.run(c, shell=True)
    if r.returncode!=0: sys.exit(f"[FATAL] {c}")
# 1) KBO 통합(여러 시대/소스 → 표준)
sh("python3 scripts/day54_ingest_kbo_all.py")
# 2) ②–③ + 링크 보정/검증까지
sh("python3 scripts/day54_finish.py")
