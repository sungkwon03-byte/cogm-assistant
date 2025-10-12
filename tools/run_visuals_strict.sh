#!/usr/bin/env bash
set -euo pipefail
ROOT="/workspaces/cogm-assistant"
echo "[RUN] real build"
python3 "$ROOT/pipeline/visuals_final_realbuild.py"

echo "[RUN] validate"
python3 "$ROOT/tools/validate_real_outputs.py"

echo "[RUN] QC json"
bash "$ROOT/pipeline/final_fullbuild_nofail.sh" >/dev/null || true
cat "$ROOT/output/full_system_validation.json" || true

echo "[RUN] bundle"
bash "$ROOT/tools/hf_finalize_bundle.sh" || bash "$ROOT/tools/hf_finalize_bundle_nofail.sh"
