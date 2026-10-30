#!/bin/sh
# Exercise suspended portal ownership in an isolated native server.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
PORT=${PORT:-6788}
PYTHON=${PYTHON:-python3}
ROOT=$(mktemp -d "${TMPDIR:-/tmp}/pg-portal-lifecycle.XXXXXX")
NATIVE=
cleanup() {
    status=$?
    trap - EXIT
    if [ -n "$NATIVE" ]; then
        "$PYTHON" "$BASE/t/interop/signal-owned-server.py" "$NATIVE" "$ROOT/native" INT 20 || \
            "$PYTHON" "$BASE/t/interop/signal-owned-server.py" "$NATIVE" "$ROOT/native" KILL 5 || :
        wait "$NATIVE" 2>/dev/null || :
    fi
    if [ "$status" -eq 0 ]; then
        rm -rf "$ROOT"
    else
        echo "portal-lifecycle: artifacts=$ROOT"
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
"$BASE/pg-lisp.sh" -p "$PORT" -D "$ROOT/native" --control-socket "$ROOT/native.sock" >"$ROOT/native.log" 2>&1 &
NATIVE=$!
test_wait_owned_sql_server "$NATIVE" "$PORT" "${PSQL:-psql}"
timeout --foreground --kill-after=5 180 "$PYTHON" "$BASE/t/interop/portal_lifecycle.py" \
    "$PORT" "$BASE" "$ROOT" "$NATIVE"
