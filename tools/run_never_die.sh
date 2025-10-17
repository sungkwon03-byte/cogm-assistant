#!/usr/bin/env bash
set +e
ts(){ date -u +%FT%TZ; }; log(){ printf "[%s] %s\n" "$(ts)" "$*"; }
SCRIPT="$1"; shift || true
LOGFILE="${LOGFILE:-/tmp/cogm_run.log}"
mkdir -p "$(dirname "$LOGFILE")"
log "[RUNNER] starting: $SCRIPT $*"
echo "---- $(ts) START $SCRIPT $* ----" >>"$LOGFILE"
bash "$SCRIPT" "$@" >>"$LOGFILE" 2>&1
RC=$?
if [ $RC -ne 0 ]; then
  log "[RUNNER] script exited with RC=$RC (non-fatal). Keeping session alive."
  echo "---- $(ts) RC=$RC ----" >>"$LOGFILE"
else
  log "[RUNNER] script finished RC=0."
fi
log "[RUNNER] tailing logs (Ctrl+C로 나가도 runner 유지)"
( tail -n 200 -f "$LOGFILE" & )
while :; do sleep 3600; done
