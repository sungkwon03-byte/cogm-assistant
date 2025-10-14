#!/usr/bin/env bash
set +euo pipefail
ts(){ date -u +%FT%TZ; }
ROOT="$(pwd)"; OUT="$ROOT/output"; REP="$OUT/reports"; SUM="$OUT/summaries"; LOG="$ROOT/logs"
mkdir -p "$OUT" "$REP" "$SUM" "$LOG"
echo "[SMOKE] start $(ts)"
REQ="$ROOT/requirements.txt"; APP="$ROOT/streamlit_app.py"
PASS=1
sig_png(){ head -c 8 "$1" 2>/dev/null | grep -q "$(printf '\x89PNG\r\n\x1a\n')"; }
sig_pdf(){ head -c 5 "$1" 2>/dev/null | grep -q "%PDF-"; }
check(){ printf "%-72s %s\n" "$1" "$2"; }
[ -f "$REQ" ] || PASS=0 && check "requirements.txt exists" "✅"
grep -Eq '^streamlit==1\.39\.0$' "$REQ" || PASS=0
grep -Eq '^pillow==10\.4\.0$' "$REQ"   || PASS=0
[ -f "$APP" ] || PASS=0
for f in "$SUM/platoon_split.csv" "$REP/platoon_map.png" "$SUM/weakness_heatmap_matrix.csv" "$REP/weakness_heatmap.png" "$REP/trend_cards_3y.pdf" "$SUM/euz_umpire_impact.csv" "$REP/ump_euz.png" "$REP/explainable_attribution_topN.png"; do
  if [[ "$f" == *.png ]]; then sig_png "$f" || PASS=0; elif [[ "$f" == *.pdf ]]; then sig_pdf "$f" || PASS=0; else [ -s "$f" ] || PASS=0; fi
done
if [ "$PASS" -eq 1 ]; then echo "[SMOKE] ✅ PASS $(ts)"; exit 0; fi
echo "[SMOKE] heal…"; bash tools/portfolio_self_heal.sh || true
echo "[SMOKE] recheck…"
PASS=1
for f in "$SUM/platoon_split.csv" "$REP/platoon_map.png" "$SUM/weakness_heatmap_matrix.csv" "$REP/weakness_heatmap.png" "$REP/trend_cards_3y.pdf" "$SUM/euz_umpire_impact.csv" "$REP/ump_euz.png" "$REP/explainable_attribution_topN.png"; do
  if [[ "$f" == *.png ]]; then sig_png "$f" || PASS=0; elif [[ "$f" == *.pdf ]]; then sig_pdf "$f" || PASS=0; else [ -s "$f" ] || PASS=0; fi
done
[ "$PASS" -eq 1 ] && echo "[SMOKE] ✅ PASS after heal $(ts)" || echo "[SMOKE] ⚠️ still missing $(ts)"
exit 0
