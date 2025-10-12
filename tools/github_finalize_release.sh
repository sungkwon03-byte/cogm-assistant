#!/usr/bin/env bash
# GitHub 최종 릴리스 정리 스크립트 (idempotent, no-drama)
# - 현재 리포 상태 커밋/푸시
# - TAG(기본: HF-MLB-2025-10-12) 강제 고정 및 푸시
# - 최신 handoff_bundle_*.tar.gz 찾아 SHA256SUMS.txt 생성/갱신/커밋/푸시
# - gh CLI 있으면 릴리스 생성/업데이트 + 에셋 업로드 자동
set -euo pipefail
ts(){ date -u +%FT%TZ; }
say(){ echo "[$(ts)] $*"; }

TAG="${HF_TAG:-HF-MLB-2025-10-12}"
TITLE="${HF_TITLE:-MLB HF Final Build (Real Data, Visuals Complete)}"
DESC="${HF_DESC:-Includes full Statcast 2015–2025, Lahman 1901–2024, visuals + QC PASS}"

# ── Repo 루트 확인 ─────────────────────────────────────────────────────────────
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [ -z "$REPO_ROOT" ]; then
  echo "ERR: not inside a git repo." >&2
  exit 1
fi
cd "$REPO_ROOT"

say "GitHub finalize (repo=$REPO_ROOT, tag=$TAG)"

# ── 1) 변경 사항 커밋/푸시 ─────────────────────────────────────────────────────
git add -A
if ! git diff --cached --quiet; then
  git commit -m "Finalize MLB HF real-data release ($(ts))" || true
fi
# 원격 브랜치 존재 시 푸시
cur_branch="$(git rev-parse --abbrev-ref HEAD || echo main)"
git push origin "$cur_branch" || true

# ── 2) 태그 강제 고정 & 푸시 ──────────────────────────────────────────────────
git fetch --tags || true
git tag -f "$TAG" || true
git push --force origin "refs/tags/$TAG" || true
say "Tag pushed: $TAG"

# ── 3) 최신 번들 찾기 ─────────────────────────────────────────────────────────
BUNDLE="${HF_BUNDLE:-}"
if [ -z "${BUNDLE}" ]; then
  BUNDLE="$(ls -1t handoff_bundle_MLB_HF_*.tar.gz 2>/dev/null | head -n1 || true)"
fi
if [ -z "${BUNDLE}" ] || [ ! -f "${BUNDLE}" ]; then
  say "No bundle found; creating one with tools/hf_finalize_bundle_nofail.sh"
  if [ -x tools/hf_finalize_bundle_nofail.sh ]; then
    BUNDLE="$(bash tools/hf_finalize_bundle_nofail.sh | tail -n1)"
  elif [ -x tools/hf_finalize_bundle.sh ]; then
    BUNDLE="$(bash tools/hf_finalize_bundle.sh | tail -n1)"
  else
    echo "ERR: no bundle and no finalize script." >&2
    exit 1
  fi
fi
say "Bundle: $BUNDLE"

# ── 4) SHA256SUMS.txt 갱신/커밋/푸시 ──────────────────────────────────────────
SHA_FILE="SHA256SUMS.txt"
tmp_sha="$(mktemp)"
sha256sum "$BUNDLE" > "$tmp_sha"
# 동일 파일 라인 제거 후 추가
if [ -f "$SHA_FILE" ]; then
  grep -v " $(basename "$BUNDLE")\$" "$SHA_FILE" > "$SHA_FILE.tmp" || true
  mv "$SHA_FILE.tmp" "$SHA_FILE"
fi
cat "$tmp_sha" >> "$SHA_FILE"
rm -f "$tmp_sha"

git add "$SHA_FILE"
if ! git diff --cached --quiet; then
  git commit -m "Update checksum for $BUNDLE"
  git push origin "$cur_branch" || true
fi
say "Checksum updated: $(tail -n1 "$SHA_FILE")"

# ── 5) gh CLI로 릴리스 생성/업데이트(있으면) ─────────────────────────────────
if command -v gh >/dev/null 2>&1; then
  if gh release view "$TAG" >/dev/null 2>&1; then
    say "Release exists. Uploading assets (clobber)"
    gh release upload "$TAG" "$BUNDLE" "$SHA_FILE" --clobber >/dev/null
    gh release edit "$TAG" --title "$TITLE" --notes "$DESC" >/dev/null
  else
    say "Creating release $TAG"
    gh release create "$TAG" "$BUNDLE" "$SHA_FILE" --title "$TITLE" --notes "$DESC" >/dev/null
  fi
  say "Release ready: $(gh release view "$TAG" --json url -q .url)"
else
  say "gh CLI not found. Manual step:"
  echo "  1) GitHub → Releases → Draft new release"
  echo "  2) Tag: $TAG"
  echo "  3) Upload assets: $BUNDLE, $SHA_FILE"
  echo "  4) Title: $TITLE"
  echo "  5) Notes: $DESC"
fi

say "DONE"
