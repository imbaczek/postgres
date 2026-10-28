#!/bin/sh
# Compare defaults, CHECKs and aggregates with task-owned C and Lisp servers.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
PG_C_BINDIR=${PG_C_BINDIR:?set PG_C_BINDIR to the repository C PostgreSQL bin directory}
PYTHON=${PYTHON:-python3}
PORT=${PORT:-6732}
REFERENCE_PORT=${REFERENCE_PORT:-6733}
ROOT=$(mktemp -d "${TMPDIR:-/tmp}/pg-defaults-checks-aggregates.XXXXXX")
NATIVE= C_RUNNING=
cleanup() {
    status=$?
    trap - EXIT
    trap 'status=130' HUP INT TERM
    if [ -n "$NATIVE" ]; then
        "$PYTHON" "$BASE/t/interop/signal-owned-server.py" \
            "$NATIVE" "$ROOT/native" INT 20 || \
        "$PYTHON" "$BASE/t/interop/signal-owned-server.py" \
            "$NATIVE" "$ROOT/native" KILL 5 || :
        wait "$NATIVE" 2>/dev/null || :
    fi
    if [ -n "$C_RUNNING" ]; then
        "$PG_C_BINDIR/pg_ctl" -D "$ROOT/c" -m fast -w stop >"$ROOT/c-stop.log" 2>&1 || :
    fi
    echo "defaults-checks-aggregates: artifacts=$ROOT"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
export LD_LIBRARY_PATH="${PG_C_LIBDIR:-$PG_C_BINDIR/../lib}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
"$PG_C_BINDIR/initdb" -D "$ROOT/c" -U postgres -A trust --no-locale -E UTF8 >"$ROOT/c-init.log"
C_RUNNING=1
"$PG_C_BINDIR/pg_ctl" -D "$ROOT/c" -l "$ROOT/c.log" \
    -o "-h 127.0.0.1 -p $REFERENCE_PORT -k $ROOT" -w -t 20 start >"$ROOT/c-start.log"
read -r C_PID <"$ROOT/c/postmaster.pid"
test_wait_owned_sql_server "$C_PID" "$REFERENCE_PORT" "${PSQL:-psql}"
"$BASE/pg-lisp.sh" -p "$PORT" -D "$ROOT/native" \
    --control-socket "$ROOT/native.sock" >"$ROOT/native.log" 2>&1 &
NATIVE=$!
test_wait_owned_sql_server "$NATIVE" "$PORT" "${PSQL:-psql}"
timeout --foreground --kill-after=5 120 "$PYTHON" \
    "$BASE/t/interop/defaults_checks_aggregates.py" "$REFERENCE_PORT" "$PORT" "$BASE" "$ROOT/native.sock"

# Reopen persisted expression metadata in a fresh process, retaining the C peer.
"$PYTHON" "$BASE/t/interop/signal-owned-server.py" "$NATIVE" "$ROOT/native" INT 20
wait "$NATIVE"
NATIVE=
"$BASE/pg-lisp.sh" -p "$PORT" -D "$ROOT/native" \
    --control-socket "$ROOT/native.sock" >>"$ROOT/native.log" 2>&1 &
NATIVE=$!
test_wait_owned_sql_server "$NATIVE" "$PORT" "${PSQL:-psql}"
"$PYTHON" "$BASE/t/interop/defaults_checks_aggregates.py" "$REFERENCE_PORT" "$PORT" --restart
