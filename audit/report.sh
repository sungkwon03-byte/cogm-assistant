set -euo pipefail

MF="audit/manifest_full.tsv"
TAG="audit/manifest_tagged.tsv"
CSVPROF="audit/csv_profile/csv_profile.tsv"

echo "# Co-GM Workspace Audit Report" > audit/report.md
echo "" >> audit/report.md
echo "생성시각: $(date -u +"%Y-%m-%dT%H:%M:%SZ")" >> audit/report.md

# 1) 기본 통계
TOTAL=$(wc -l < "$MF")
echo "## 1) 기본" >> audit/report.md
echo "- 파일 수: $TOTAL" >> audit/report.md
echo "" >> audit/report.md

# 2) 확장자 TOP 15
echo "## 2) 확장자 TOP 15" >> audit/report.md
awk -F'\t' '{n=$1; ext="(noext)"; if (match(n, /\.[^./]+$/)) ext=substr(n, RSTART+1); e[ext]++} END{for(k in e) printf "%s\t%d\n",k,e[k]}' "$MF" \
| sort -k2,2nr | head -n 15 | awk -F'\t' 'BEGIN{print "|확장자|개수|"; print "|---:|---:|"}{printf("|%s|%s|\n",$1,$2)}' >> audit/report.md
echo "" >> audit/report.md

# 3) 최상위 디렉터리 TOP 15 (개수/용량)
echo "## 3) 최상위 디렉터리 TOP 15 (개수/용량)" >> audit/report.md
awk -F'\t' '{
  path=$1; size=$3+0; sub(/^\.\//,"",path); split(path,a,"/"); top=(a[1]==""?".":a[1]);
  c[top]++; s[top]+=size
} END {for(k in c) printf "%s\t%d\t%f\n",k,c[k],s[k]/1024/1024}' "$MF" \
| sort -k2,2nr -k3,3nr | head -n 15 \
| awk -F'\t' 'BEGIN{print "|디렉터리|파일수|용량(MB)|"; print "|:--|--:|--:|"}{printf("|%s|%s|%.2f|\n",$1,$2,$3)}' >> audit/report.md
echo "" >> audit/report.md

# 4) 대용량 파일 TOP 20 (>= 100MB)
echo "## 4) 대용량 파일 TOP 20 (>=100MB)" >> audit/report.md
awk -F'\t' '$3+0>=100*1024*1024 {print $3 "\t" $1}' "$MF" | sort -k1,1nr | head -n 20 \
| awk -F'\t' 'BEGIN{print "|크기(MB)|경로|"; print "|--:|:--|"}{printf("|%.2f|%s|\n",$1/1024/1024,$2)}' >> audit/report.md
echo "" >> audit/report.md

# 5) 휴리스틱 태깅(샘플/테스트/임시/캐시/realish) TOP 50 정리 후보
if [ -f "$TAG" ]; then
  echo "## 5) 정리 후보 TOP 50 (sample/test/temp/cache 우선 + 대용량 가점)" >> audit/report.md
  awk -F'\t' 'NR>1{
    path=$1; size=$3+0; tags=$9; score=0
    if (tags ~ /sample/) score+=5
    if (tags ~ /test/)   score+=4
    if (tags ~ /temp/)   score+=3
    if (tags ~ /cache/)  score+=3
    if (size>100000000)  score+=2
    printf "%d\t%.0f\t%s\t%s\n", score, size, tags, path
  }' "$TAG" | sort -k1,1nr -k2,2nr | head -n 50 \
  | awk -F'\t' 'BEGIN{print "|스코어|크기(MB)|태그|경로|"; print "|--:|--:|:--|:--|"}{printf("|%s|%.2f|%s|%s|\n",$1,$2/1024/1024,$3,$4)}' >> audit/report.md
  echo "" >> audit/report.md
fi

# 6) CSV/TSV 프로파일 샘플 (헤더 확인용)
if [ -f "$CSVPROF" ]; then
  echo "## 6) CSV/TSV 헤더 샘플 (상위 20)" >> audit/report.md
  (head -n 1 "$CSVPROF"; tail -n +2 "$CSVPROF" | head -n 20) \
  | awk -F'\t' 'BEGIN{print "|path|size|rows|cols|headers|sample_first_row|"; print "|:--|--:|--:|--:|:--|:--|"} NR>1{gsub(/\r/,""); printf("|%s|%s|%s|%s|%s|%s|\n",$1,$2,$3,$4,$5,$6)}' >> audit/report.md
  echo "" >> audit/report.md
fi

# 7) 로그/JSON/파이썬/노트북 건수 요약 (빠른 감)
echo "## 7) 유형별 개수(로그/JSON/파이썬/노트북 등)" >> audit/report.md
awk -F'\t' '{
  p=$1
  if(p ~ /\.log$/)   L++
  if(p ~ /\.json$/)  J++
  if(p ~ /\.py$/)    P++
  if(p ~ /\.ipynb$/) N++
} END {
  printf "|유형|개수|\n|--:|--:|\n|log|%d|\n|json|%d|\n|py|%d|\n|ipynb|%d|\n", L+0, J+0, P+0, N+0
}' "$MF" >> audit/report.md

