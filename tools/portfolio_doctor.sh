#!/usr/bin/env bash
set +e
ts(){ date -u +%FT%TZ; }; say(){ printf "[%s] %s\n" "$(ts)" "$*"; }
ROOT="$(pwd)"; OUT="$ROOT/output"; REP="$OUT/reports"; SUM="$OUT/summaries"; LOG="$ROOT/logs"
mkdir -p "$OUT" "$REP" "$SUM" "$LOG"
say "[DOCTOR] start"

chmod +x tools/*.sh 2>/dev/null || true

python3 - <<'PY'
from pathlib import Path
rep=Path("output/reports"); rep.mkdir(parents=True, exist_ok=True)
def sig_ok(pat, magic):
    for p in pat:
        try:
            with open(p,'rb') as f:
                if not f.read(len(magic))==magic:
                    print("[FIX] drop invalid:", p); p.unlink()
        except: pass
sig_ok(rep.glob("*.png"), b"\x89PNG\r\n\x1a\n")
sig_ok(rep.glob("*.pdf"), b"%PDF-")
PY

python3 - <<'PY'
from pathlib import Path, json
cards = Path("output/player_cards_all.parquet")
sc = Path("output/statcast_ultra_full_clean.parquet")
qc = {"cards_exists": cards.exists(), "statcast_exists": sc.exists()}
Path("output/summaries/full_system_validation.json").write_text(json.dumps(qc, indent=2))
print(json.dumps(qc))
PY

say "[DOCTOR] done"
exit 0
