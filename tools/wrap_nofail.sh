#!/usr/bin/env bash
# 사용법: tools/wrap_nofail.sh <command...>
"$@"; rc=$?
if [ $rc -ne 0 ]; then
  echo "[WARN] $* -> exit $rc (forced to 0)" >&2
fi
exit 0
