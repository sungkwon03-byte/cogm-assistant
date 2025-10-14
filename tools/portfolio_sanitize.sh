#!/usr/bin/env bash
set -euo pipefail

# ----------------------------
# Portfolio Sanitizer (FINAL)
# ----------------------------

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# defaults
TARGET="$ROOT/output"
DRYRUN=0
MIN_IMG=200     # bytes
MIN_PDF=400
MIN_CSV=10

# args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --target) TARGET="${2:?}"; shift 2 ;;
    --dry-run) DRYRUN=1; shift ;;
    --min-img) MIN_IMG="${2:?}"; shift 2 ;;
    --min-pdf) MIN_PDF="${2:?}"; shift 2 ;;
    --min-csv) MIN_CSV="${2:?}"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 2 ;;
  esac
done

NOW="$(date -u +%Y%m%dT%H%M%SZ)"
QROOT="$TARGET/.quarantine"
QDIR="$QROOT/$NOW"
LOG="$QROOT/sanitize_$NOW.log"

mkdir -p "$QDIR"
echo "[SANITIZE] start $NOW  TARGET=$TARGET  DRYRUN=$DRYRUN  MIN_IMG=$MIN_IMG MIN_PDF=$MIN_PDF MIN_CSV=$MIN_CSV" | tee "$LOG"

python3 - "$TARGET" "$QROOT" "$QDIR" "$LOG" "$DRYRUN" "$MIN_IMG" "$MIN_PDF" "$MIN_CSV" << 'PY'
import sys, shutil
from pathlib import Path

TARGET = Path(sys.argv[1]).resolve()
QROOT  = Path(sys.argv[2]).resolve()
QDIR   = Path(sys.argv[3]).resolve()
LOGF   = Path(sys.argv[4]).resolve()
DRY    = bool(int(sys.argv[5]))
MIN_I  = int(sys.argv[6]); MIN_P = int(sys.argv[7]); MIN_C = int(sys.argv[8])

def log(msg: str):
    print(msg, flush=True)
    with open(LOGF, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

if not TARGET.exists():
    log(f"[DONE] TARGET missing: {TARGET}")
    raise SystemExit(0)

# imports
try:
    from PIL import Image, UnidentifiedImageError
    PIL_OK = True
except Exception as e:
    PIL_OK = False
    log(f"[WARN] Pillow not available: {e}")

try:
    import pandas as pd
    PD_OK = True
except Exception as e:
    PD_OK = False
    log(f"[WARN] pandas not available: {e}")

img_exts = {".png",".jpg",".jpeg",".webp",".gif"}

def is_in_quarantine(p: Path) -> bool:
    rp = p.resolve()
    return str(rp).startswith(str(QROOT))

def is_lfs_pointer_like(p: Path) -> bool:
    try:
        with p.open("rb") as f:
            head = f.read(256)
        return (b"git-lfs" in head) or head.startswith(b"version https://git-lfs.github.com/spec")
    except Exception:
        return False

def quarantine(p: Path, reason: str):
    if is_in_quarantine(p):
        return
    tgt = QDIR / p.relative_to(TARGET)
    tgt.parent.mkdir(parents=True, exist_ok=True)
    if DRY:
        log(f"[DRY] MOVE {p} -> {tgt} :: {reason}")
        return
    try:
        shutil.move(str(p), str(tgt))
        log(f"[MOVE] {p} -> {tgt} :: {reason}")
    except Exception as e:
        log(f"[WARN] move failed {p}: {e}")

kept = moved = skipped = 0
for p in TARGET.rglob("*"):
    if not p.is_file():
        continue
    if is_in_quarantine(p):
        skipped += 1
        continue

    ext = p.suffix.lower()

    if ext in img_exts:
        if p.stat().st_size < MIN_I:
            quarantine(p, f"too small (<{MIN_I}B)"); continue
        if is_lfs_pointer_like(p):
            quarantine(p, "lfs pointer"); continue
        if PIL_OK:
            try:
                with Image.open(p) as im:
                    im.verify()
                kept += 1
            except Exception as e:
                quarantine(p, f"image verify failed: {e}")
        else:
            kept += 1
        continue

    if ext == ".pdf":
        try:
            if p.stat().st_size < MIN_P:
                quarantine(p, f"too small PDF (<{MIN_P}B)"); continue
            with p.open("rb") as f:
                head = f.read(8)
            if not head.startswith(b"%PDF-"):
                quarantine(p, "invalid PDF header"); continue
            kept += 1
        except Exception as e:
            quarantine(p, f"pdf check error: {e}")
        continue

    if ext == ".csv":
        if p.stat().st_size < MIN_C:
            quarantine(p, f"too small CSV (<{MIN_C}B)"); continue
        if PD_OK:
            try:
                pd.read_csv(p, nrows=50)
                kept += 1
            except Exception as e:
                quarantine(p, f"csv parse failed: {e}")
        else:
            kept += 1
        continue

    quarantine(p, "unwanted extension")

log("[SANITIZE] done")
PY

echo "[SANITIZE] quarantine dir: $QDIR"
echo "[SANITIZE] log: $LOG"

echo "---- kept (top) ----" | tee -a "$LOG"
ls -lh "$TARGET" 2>/dev/null | head -n 30 | tee -a "$LOG" || true
echo "---- quarantined ----" | tee -a "$LOG"
ls -lh "$QDIR" 2>/dev/null | tee -a "$LOG" || true
echo "✅ Done."
