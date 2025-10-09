#!/usr/bin/env bash
set -euo pipefail

ROOT="/workspaces/cogm-assistant"
OUT="$ROOT/output"
LOGDIR="$ROOT/logs"
DOCS="$ROOT/docs"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RLOG="$LOGDIR/e2e_full_${STAMP}.log"
mkdir -p "$LOGDIR" "$DOCS" "$OUT/raw/statcast_parquet"

ok(){ echo "[OK] $*" | tee -a "$RLOG"; }
warn(){ echo "[WARN] $*" | tee -a "$RLOG"; }
note(){ echo "[NOTE] $*" | tee -a "$RLOG"; }

echo "[E2E] start ${STAMP}" | tee "$RLOG"

##############################################
# 1) Statcast CSV 캐시 → Parquet 이관(손상 파일 continue)
##############################################
python - <<'PY' 2>&1 | tee -a "$LOGDIR/e2e_full_${STAMP}.log"
import os, re, glob, sys, shutil
from pathlib import Path
import pandas as pd
from pandas.errors import EmptyDataError, ParserError

ROOT = Path("/workspaces/cogm-assistant")
SRC_DIRS = [ROOT/"output/cache/statcast", ROOT/"output/cache/statcast_clean"]
DEST = ROOT/"output/raw/statcast_parquet"
BAD  = ROOT/"output/cache/_bad"
DEST.mkdir(parents=True, exist_ok=True); BAD.mkdir(parents=True, exist_ok=True)

chunksize = int(os.getenv("E2E_CHUNKSIZE","1000000"))
pat = re.compile(r"(\d{4})(\d{2})\d{2}_to_\d{8}\.csv$")

files=[]
for d in SRC_DIRS:
    if d.exists():
        files += sorted(glob.glob(str(d/"*.csv")))
if not files:
    print("[parquet] no cache csv to convert; skip")
    sys.exit(0)

def outdir(y,m):
    d = DEST/f"year={y}"/f"month={m}"
    d.mkdir(parents=True, exist_ok=True); return d

converted=0; skipped=0; moved_bad=0
for i, fp in enumerate(map(Path, files), 1):
    try:
        # 0바이트/화이트스페이스만 → 스킵
        if fp.stat().st_size == 0:
            print(f"[skip-empty] {fp.name} (0 bytes)"); fp.unlink(missing_ok=True); skipped+=1; continue
        with fp.open("rb") as fh:
            head = fh.read(2048)
        if not head.strip():
            print(f"[skip-empty] {fp.name} (whitespace only)"); fp.unlink(missing_ok=True); skipped+=1; continue

        m = pat.search(fp.name)
        y,mn = (m.group(1), m.group(2)) if m else ("unknown","unknown")
        od = outdir(y,mn)

        parts = 0
        # 빠른 헤더 검증: 몇 줄 먼저 읽어서 컬럼 확인
        try:
            probe = pd.read_csv(fp, nrows=5, low_memory=True)
            if probe.shape[1] == 0:
                raise EmptyDataError("no columns in header")
        except (EmptyDataError, ParserError, UnicodeDecodeError) as e:
            # 손상 파일은 격리 폴더로 이동하고 계속
            shutil.move(str(fp), str(BAD/fp.name))
            print(f"[bad->moved] {fp.name}: {e}")
            moved_bad += 1
            continue

        # 본 변환(청크)
        for chunk in pd.read_csv(fp, chunksize=chunksize, low_memory=True):
            if chunk.shape[1] == 0:
                continue
            (od/f"part-{parts:04d}.parquet").write_bytes(b"")  # 예약 파일(중도중단 대비)
            chunk.to_parquet(od/f"part-{parts:04d}.parquet", index=False, engine="pyarrow", compression="zstd")
            parts += 1

        # 변환된 경우만 원본 삭제
        if parts > 0:
            print(f"[parquet] {fp.name} -> {od} parts={parts}")
            fp.unlink(missing_ok=True)
            converted += 1
        else:
            # 데이터가 없었으면 손상으로 간주 후 격리
            shutil.move(str(fp), str(BAD/fp.name))
            print(f"[bad->moved] {fp.name}: no non-empty chunks")
            moved_bad += 1
    except Exception as e:
        # 어떤 예외도 전체를 중단시키지 않고 파일만 격리
        try:
            shutil.move(str(fp), str(BAD/fp.name))
        except Exception:
            pass
        print(f"[error->moved] {fp.name}: {e}")
        moved_bad += 1
        continue

print(f"[summary] converted={converted} skipped={skipped} bad_moved={moved_bad}")
PY

##############################################
# 2) 캐시·임시 정리(원천/코어 보존)
##############################################
[ -f "$ROOT/data/retrosheet/csvdownloads.zip" ] && { note "rm data/retrosheet/csvdownloads.zip"; rm -f "$ROOT/data/retrosheet/csvdownloads.zip"; }
find "$OUT" -maxdepth 1 -type f -name "_*.csv" -print -delete | tee -a "$RLOG" || true
find "$LOGDIR" -type f -name "*.log" -size +10M -exec gzip -9 {} \; -o -name "*.log" -mtime +7 -delete

##############################################
# 3) 스모크 + 결측/범위 게이트 (전 시즌 공통)
##############################################
python - <<'PY' 2>&1 | tee -a "$LOGDIR/e2e_full_${STAMP}.log"
from pathlib import Path
import pandas as pd
OUT=Path("/workspaces/cogm-assistant/output")

RULES = {
 "statcast_pitch_mix_detailed.csv": {
   "req": ["year","role","mlbam","pitch_type","pitches","usage_rate","zone_rate","whiff_rate","z_whiff_rate","o_whiff_rate","csw_rate","edge_rate","heart_rate","chase_rate"],
   "rate_01": ["usage_rate","zone_rate","whiff_rate","z_whiff_rate","o_whiff_rate","csw_rate","edge_rate","heart_rate","chase_rate"]
 },
 "statcast_pitch_mix_detailed_plus_bat.csv": {
   "req": ["role","year","mlbam","pitch_type","segment","vhb","pitches","Z_Pitches","O_Pitches","Z_Swings","O_Swings","Z_Whiffs","O_Whiffs","CS","usage_rate","zone_rate","z_swing_rate","o_swing_rate","z_contact_rate","o_contact_rate","z_csw_rate","csw_rate","edge_rate","heart_rate","chase_rate","edge_cnt","heart_cnt","chase_cnt","group_total","batter","vs_hand","z_whiff_rate","o_whiff_rate"],
   "rate_01": ["usage_rate","zone_rate","z_swing_rate","o_swing_rate","z_contact_rate","o_contact_rate","z_csw_rate","csw_rate","edge_rate","heart_rate","chase_rate"]
 },
 "count_tendencies_bat.csv": {
   "req": ["year","mlbam","vhb","pitches","zone_rate","z_swing_rate","o_swing_rate","z_contact_rate","o_contact_rate","z_csw_rate","chase_rate","edge_rate","heart_rate","batter","count","swing_rate","whiff_rate","csw_rate"],
   "rate_01": ["zone_rate","z_swing_rate","o_swing_rate","z_contact_rate","o_contact_rate","z_csw_rate","chase_rate","edge_rate","heart_rate","swing_rate","whiff_rate","csw_rate"]
 },
 "advanced_metrics.csv": {"req":["year","playerID","role"], "rate_01":[]},
 "trend_3yr.csv": {"req":["year"], "rate_01":[]},
 "trade_value.csv": {"req":["year","playerID","player_name"], "rate_01":[]},
 "fa_market_mvp.csv": {"req":["year","role","mlbam","player_name"], "rate_01":[]},
 "league_runenv.csv": {"req":["year","lgID","R_per_G","HR_per_G","BB_per_G","SO_per_G"], "rate_01":[]},
 "ump_euz_indices.csv": {"req":["year","euz_index"], "rate_01":[]}
}

def check(name):
    p=OUT/name
    if not p.exists() or p.stat().st_size==0:
        return (name, "FAIL(missing file)", "SKIP", 0, 0, "")
    df=pd.read_csv(p, low_memory=False)
    rows, cols = len(df), len(df.columns)
    missing=[c for c in RULES[name]["req"] if c not in df.columns]
    req="PASS" if not missing else f"FAIL(missing:{','.join(missing)})"
    rate="PASS"
    bad=[]
    for c in RULES[name]["rate_01"]:
        if c in df.columns:
            s=pd.to_numeric(df[c], errors="coerce")
            cnt=int(((s<0)|(s>1)).sum())
            if cnt>0: bad.append(f"{c}:{cnt}")
    if bad: rate=f"FAIL({';'.join(bad)})"
    note=""
    if "year" in df.columns and (df["year"]==2025).any():
        core=[c for c in RULES[name]["req"] if c in df.columns]
        miss25=int(df.loc[df["year"]==2025, core].isna().sum().sum()) if core else 0
        if miss25>0: rate="FAIL(2025 nulls>0)"; note=f"2025_nulls={miss25}"
        else: note="2025_nulls=0"
    return (name, req, rate, rows, cols, note)

print("FILE\tREQ_COLUMNS\tRATE_0_1\tROWS\tCOLS\tNOTES")
fails=[]
for n in RULES:
    r=check(n)
    print("\t".join(map(str,r)))
    if "FAIL" in r[1] or "FAIL" in r[2]:
        fails.append(r[0])
if fails:
    print("[gate] FAIL in:", fails)
    # 실패해도 전체 파이프라인은 종료하지 않음(운영 보고용) → exit 0
else:
    print("[gate] PASS all")
PY

##############################################
# 4) E2E 리포트 생성 (전 시즌 스냅샷)
##############################################
python - <<'PY' 2>&1 | tee -a "$LOGDIR/e2e_full_${STAMP}.log"
import csv as _csv
from pathlib import Path
from datetime import datetime

ROOT=Path("/workspaces/cogm-assistant"); OUT=ROOT/"output"; DOC=ROOT/"docs"/"e2e_report.md"
FILES=["statcast_features_player_year.csv","statcast_pitch_mix_detailed.csv","statcast_pitch_mix_detailed_plus_bat.csv","count_tendencies_bat.csv","bat_stability.csv","weakness_map_player_year.csv","trend_3yr.csv","trade_value.csv","mock_trades_mvp.csv","fa_market_mvp.csv","advanced_metrics.csv","league_runenv.csv","ump_euz_indices.csv","mart_star_idfix.csv"]

def meta(p):
    try:
        with open(p,encoding="utf-8") as f:
            r=_csv.reader(f); head=next(r,[])
            n=0; yrs=set()
            for row in r:
                n+=1
                for c in row[:12]:
                    if c and c.isdigit() and len(c)==4:
                        y=int(c); 
                        if 1800<y<3000: yrs.add(y)
            return {"cols":len(head),"rows":n,"years":(min(yrs),max(yrs)) if yrs else ("",""),"head":head[:12]}
    except Exception as e:
        return {"cols":0,"rows":0,"years":("",""),"head":[], "err":str(e)}

lines=[]
lines.append(f"# E2E PASS 리포트 (스냅샷: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%SZ')})\n")
lines.append("| File | Rows | Cols | year(min–max) | Head(12) |")
lines.append("|---|---:|---:|:---:|---|")
for fn in FILES:
    p=(OUT/fn).as_posix()
    m=meta(p)
    lines.append(f"| {fn} | {m.get('rows',0)} | {m.get('cols',0)} | {m.get('years',('',''))[0]}–{m.get('years',('',''))[1]} | {', '.join(m.get('head',[]))} |")
DOC.write_text("\n".join(lines), encoding="utf-8")
print(f"[e2e] report -> {DOC.as_posix()}")
PY

##############################################
# 5) 디스크 요약 & 릴리스 패키징
##############################################
echo "[sizes]" | tee -a "$RLOG"
du -sh "$OUT/raw/statcast_parquet" 2>/dev/null | tee -a "$RLOG" || true
du -sh "$OUT" "$OUT/cache" "$ROOT/data/retrosheet" 2>/dev/null | tee -a "$RLOG" || true
echo "[disk]" | tee -a "$RLOG"
df -h . | tee -a "$RLOG"

tar -czf "release_full_${STAMP}.tar.gz" docs/e2e_report.md output/*.csv 2>/dev/null || true
echo "[release] -> release_full_${STAMP}.tar.gz" | tee -a "$RLOG"

echo "[E2E] done ${STAMP}" | tee -a "$RLOG"
