#!/bin/sh
# Exclusive local PGDATA ownership plus real Patroni process/driver smoke tests.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
PYTHON=${PATRONI_PYTHON:-/tmp/pg-patroni-4.1.5-venv/bin/python3}
test -x "$PYTHON" || { echo 'set PATRONI_PYTHON to the pinned test environment' >&2; exit 1; }
PSQL=${PSQL:-psql}
SBCL=${SBCL:-sbcl}
PORT=${PORT:-5894}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-postmaster-pid.XXXXXX")
SERVER= OTHER=
cleanup() {
    status=$?
    trap - EXIT
    for pid in "$SERVER" "$OTHER"; do
        if [ -n "$pid" ]; then kill -INT "$pid" 2>/dev/null || :; kill -TERM "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; fi
    done
    echo "postmaster-pid: artifacts=$TMP"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
start() {
    rm -f "$TMP/control.sock"
    "$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/data" \
        --control-socket "$TMP/control.sock" >>"$TMP/server.log" 2>&1 &
    SERVER=$!
    test_wait_owned_sql_server "$SERVER" "$PORT" "$PSQL"
}
rejected_start() {
    status=0
    timeout 10 "$BASE/pg-lisp.sh" -p "$((PORT + 1))" -D "$1" >"$2" 2>&1 || status=$?
    test "$status" -eq 1
}
start
"$PYTHON" "$BASE/t/interop/patroni-process.py" "$TMP/data" "$SERVER" "$PORT" check
mkdir -m 700 "$TMP/wrong-start"
"$PYTHON" - "$TMP/data/postmaster.pid" "$TMP/wrong-start/postmaster.pid" <<'PY'
from pathlib import Path
import sys
lines = Path(sys.argv[1]).read_text().splitlines()
lines[2] = str(int(lines[2]) + 1000)
Path(sys.argv[2]).write_text("\n".join(lines) + "\n")
PY
"$PYTHON" "$BASE/t/interop/patroni-process.py" "$TMP/wrong-start" "$SERVER" "$PORT" reject-start-time
cp "$TMP/data/postmaster.pid" "$TMP/owner.pid"
rejected_start "$TMP/data" "$TMP/duplicate.log"
grep -q 'already owned' "$TMP/duplicate.log"
cmp "$TMP/owner.pid" "$TMP/data/postmaster.pid"
printf '(pg::reload)\n(quit)\n' | "$SBCL" --script "$BASE/t/control-client.lisp" unix "$TMP/control.sock" >"$TMP/reload.log"
grep -q 'reload complete' "$TMP/reload.log"
cmp "$TMP/owner.pid" "$TMP/data/postmaster.pid"
# The verifier is a sibling process, not the server's parent: Patroni correctly
# rejects its own child as a candidate postmaster.
"$PYTHON" "$BASE/t/interop/patroni-process.py" "$TMP/data" "$SERVER" "$PORT" stop
wait "$SERVER"
SERVER=
test ! -e "$TMP/data/postmaster.pid"
test -f "$TMP/data/.pg_lisp.lock"
start
"$PYTHON" "$BASE/t/interop/patroni-process.py" "$TMP/data" "$SERVER" "$PORT" check
kill -KILL "$SERVER"
wait "$SERVER" 2>/dev/null || :
SERVER=
test -f "$TMP/data/postmaster.pid"
start
"$PYTHON" "$BASE/t/interop/patroni-process.py" "$TMP/data" "$SERVER" "$PORT" check
# A new directory may acquire its owner before failing to bind, but may never
# keep a stale ready PID file on an ordinary startup error.
status=0
timeout 10 "$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/bind-failure" >"$TMP/bind-failure.log" 2>&1 || status=$?
test "$status" -eq 1
test ! -e "$TMP/bind-failure/postmaster.pid"
mkdir -m 755 "$TMP/bad-mode"
rejected_start "$TMP/bad-mode" "$TMP/bad-mode.log"
grep -q 'permissions' "$TMP/bad-mode.log"
test ! -e "$TMP/bad-mode/.pg_lisp.lock"
mkdir -m 700 "$TMP/foreign"
sleep 1000 &
OTHER=$!
printf '%s\n' "$OTHER" >"$TMP/foreign/postmaster.pid"
rejected_start "$TMP/foreign" "$TMP/foreign.log"
grep -q 'live process' "$TMP/foreign.log"
test ! -e "$TMP/foreign/PG_VERSION"
test "$(cat "$TMP/foreign/postmaster.pid")" = "$OTHER"
kill -TERM "$OTHER"
wait "$OTHER" 2>/dev/null || :
OTHER=
mkdir -m 700 "$TMP/empty-pid"
: >"$TMP/empty-pid/postmaster.pid"
rejected_start "$TMP/empty-pid" "$TMP/empty-pid.log"
grep -q 'invalid postmaster.pid length' "$TMP/empty-pid.log"
test ! -e "$TMP/empty-pid/PG_VERSION"
mkdir -m 700 "$TMP/fifo-pid"
mkfifo "$TMP/fifo-pid/postmaster.pid"
rejected_start "$TMP/fifo-pid" "$TMP/fifo-pid.log"
grep -q 'not a regular file' "$TMP/fifo-pid.log"
test ! -e "$TMP/fifo-pid/PG_VERSION"
echo 'postmaster-pid: PASS'
