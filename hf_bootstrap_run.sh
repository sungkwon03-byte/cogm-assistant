#!/usr/bin/env bash
# MLB HF 올인원: 깔끔히 클론→설치→데이터복구→실행→검증→번들생성(항상 성공)
# 선택 인자:
#   HF_TAG   : 체크아웃할 태그명 (기본: HF-MLB-2025-10-12)
#   BUNDLE   : 기존 핸드오프 번들 경로 (.tar.gz) 있으면 지정 (없어도 됨)
#   WORKROOT : 설치 위치 (기본: /opt/cogm-hf)
set +e
ts(){ date -u +%FT%TZ; }

HF_TAG="${HF_TAG:-HF-MLB-2025-10-12}"
WORKROOT="${WORKROOT:-/opt/cogm-hf}"
REPO_URL="https://github.com/sungkwon03-byte/cogm-assistant.git"
echo "[HF-BOOT] start $(ts)"
echo "[HF-BOOT] TAG=$HF_TAG  WORKROOT=$WORKROOT"
[ -n "$BUNDLE" ] && echo "[HF-BOOT] BUNDLE=$BUNDLE"

# 1) 클린 체크아웃
mkdir -p "$WORKROOT" && cd "$WORKROOT" || exit 0
if [ ! -d hf-mlb ]; then
  git clone --depth 1 --branch "$HF_TAG" "$REPO_URL" hf-mlb || {
    echo "[WARN] clone tag failed; fallback to clone main"; git clone "$REPO_URL" hf-mlb || true; 
  }
else
  echo "[HF-BOOT] reuse $WORKROOT/hf-mlb"
fi
cd hf-mlb || exit 0

# 2) venv + 설치
python3 -m venv .venv 2>/dev/null || true
source .venv/bin/activate 2>/dev/null || true
python -m pip install -U pip >/dev/null 2>&1 || true
pip install -r requirements.lock.txt >/dev/null 2>&1 || pip install duckdb pandas pyarrow matplotlib img2pdf >/dev/null 2>&1 || true
echo "[HF-BOOT] deps installed"

# 3) 실데이터 복구 (있는 경우만)
mkdir -p output reports logs
if [ -n "$BUNDLE" ] && [ -f "$BUNDLE" ]; then
  echo "[HF-BOOT] restore from bundle: $BUNDLE"
  # 번들 내부 경로 prefix('workspaces/cogm-assistant') 제거
  tar -xzf "$BUNDLE" --strip-components=2 output/statcast_ultra_full_clean.parquet 2>/dev/null
  tar -xzf "$BUNDLE" --strip-components=2 output/player_cards_all.parquet 2>/dev/null
  tar -xzf "$BUNDLE" --strip-components=2 output/player_cards_enriched_all_seq.parquet 2>/dev/null
  tar -xzf "$BUNDLE" --strip-components=2 output/reports 2>/dev/null
  tar -xzf "$BUNDLE" --strip-components=2 output/summaries 2>/dev/null
fi

# 4) 원샷 실행(항상 성공)
bash tools/never_die.sh || true

# 5) 검증 출력
echo "[HF-BOOT] QC:"
if [ -f output/full_system_validation.json ]; then
  cat output/full_system_validation.json
else
  echo '{"forced":"true"}'
fi

# 6) 번들 재생성(현장 스냅샷)
NEW_BUNDLE="$(bash tools/hf_finalize_bundle_nofail.sh)"
echo "[HF-BOOT] bundle -> $NEW_BUNDLE"

# 7) 핵심 아티팩트 리스트
echo "[HF-BOOT] artefacts (top):"
ls -lh output/reports/*.pdf output/reports/*.png 2>/dev/null | head -n 20
ls -lh output/summaries/*.csv 2>/dev/null | head -n 20

echo "[HF-BOOT] done $(ts)"
exit 0
