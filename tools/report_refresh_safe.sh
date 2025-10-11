#!/usr/bin/env bash
# Rebuild plots if needed, then build v2 PDF. Never crash the terminal.
set +e; set +u; { set +o pipefail; } 2>/dev/null || true
trap '' ERR

ROOT="/workspaces/cogm-assistant"
OUT="$ROOT/output"
SUM="$OUT/summaries"
REP="$OUT/reports"
MASTER="$OUT/statcast_ultra_full_clean.parquet"
LOGO_DEFAULT="$ROOT/assets/logos/mlb_logo.png"

mkdir -p "$REP" "$SUM" "$ROOT/logs"

echo "[START] $(date -u +%FT%TZ)"
if [ ! -f "$MASTER" ]; then
  echo "❌ MASTER missing: $MASTER (run pipeline first)"; echo "[DONE]"; exit 0
fi

# plots check
NEED=0
for P in "$REP/trend_entropy_repeat_by_year.png" "$REP/ev_la_heatmap.png" "$REP/pitchtype_bar.png" "$REP/trend_year.png"; do
  [ -f "$P" ] || NEED=1
done
[ "$NEED" -eq 1 ] && echo "[RUN] plots" && python3 "$ROOT/pipeline/build_statcast_leaderboard_and_plots.py" || echo "[SKIP] plots exist"

# theme/logo (env overrideable)
THEME="${THEME:-light}"
LOGO="${LOGO:-$LOGO_DEFAULT}"
[ -f "$LOGO" ] || { echo "[WARN] logo not found: $LOGO"; LOGO=""; }

# generate v2 (never hard-fail)
if [ -n "$LOGO" ]; then
  python3 "$ROOT/pipeline/generate_auto_report_v2.py" \
    --master "$MASTER" --theme "$THEME" --logo "$LOGO" \
    --out "$REP/auto_report_v2.pdf" || echo "[WARN] report generation returned non-zero"
else
  python3 "$ROOT/pipeline/generate_auto_report_v2.py" \
    --master "$MASTER" --theme "$THEME" \
    --out "$REP/auto_report_v2.pdf" || echo "[WARN] report generation returned non-zero"
fi

# verify without crashing
if [ -f "$REP/auto_report_v2.pdf" ]; then
  ls -lh "$REP/auto_report_v2.pdf"
  echo "[OK] report ready"
else
  echo "⚠️  report missing — check pipeline/generate_auto_report_v2.py logs above"
fi
echo "[DONE] $(date -u +%FT%TZ)"
exit 0
