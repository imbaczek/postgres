#!/bin/sh
# Pinned Patroni direct launch/config smoke; no HA loop or DCS is started.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
PYTHON=${PATRONI_PYTHON:-/tmp/pg-patroni-4.1.5-venv/bin/python3}
test -x "$PYTHON" || { echo 'set PATRONI_PYTHON to the pinned test environment' >&2; exit 1; }
PORT=${PORT:-5898}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-patroni-start.XXXXXX")
status=0
"$PYTHON" "$BASE/t/interop/patroni-start.py" "$BASE" "$TMP" "$PORT" >"$TMP/driver.log" 2>&1 || status=$?
cat "$TMP/driver.log"
echo "patroni-start: artifacts=$TMP"
exit "$status"
