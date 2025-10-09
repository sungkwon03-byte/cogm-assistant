set -euo pipefail
cd /workspaces/cogm-assistant || cd "$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
echo "=== DISK BEFORE ==="; df -h .

# 절대 보존(삭제 금지): Retrosheet/Statcast/Lahman/Chadwick/샤드/캐시/코드/핵심 산출물
# 삭제 대상: KBO/NPB/MiLB, samples/backup/quarantine/raw/tmp, __pycache__, *.bak, 거대 로그

# 1) 리그별 데이터 제거 (MLB 제외)
find . -type f \( -ipath "./data/*kbo*" -o -ipath "./data/*/kbo*" -o -ipath "./output/*kbo*" \
                 -o -ipath "./data/*npb*" -o -ipath "./output/*npb*" \
                 -o -ipath "./data/*milb*" -o -ipath "./output/*milb*" \) \
     -print -delete

# 2) 샘플/백업/임시/원시
rm -rf samples quarantine backup raw tmp 2>/dev/null || true
find . -type d -name "__pycache__" -prune -exec rm -rf {} + 2>/dev/null || true
find . -type f -name "*.pyc" -delete
find . -type f \( -name "*.bak" -o -name "*.bak.*" -o -name "*.tmp" -o -name "*.log.old" \) -delete

# 3) 거대 로그(>50MB) 정리
find logs -type f -size +50M -print -delete 2>/dev/null || true

# 4) 빈 디렉토리 청소
find data   -type d -empty -delete 2>/dev/null || true
find output -type d -empty -delete 2>/dev/null || true

echo "=== BIG DIRS (data) ===";   du -xh --max-depth=1 data 2>/dev/null | sort -hr | head -n 20
echo "=== BIG DIRS (output) ==="; du -xh --max-depth=1 output 2>/dev/null | sort -hr | head -n 20
echo "=== DISK AFTER ==="; df -h .
