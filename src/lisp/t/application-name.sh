#!/bin/sh
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
PG_C_BINDIR=${PG_C_BINDIR:?set PG_C_BINDIR to the pinned C PostgreSQL bin directory}
PATRONI_PYTHON=${PATRONI_PYTHON:-/tmp/pg-patroni-4.1.5-venv/bin/python3}
PORT=${PORT:-6700}
REFERENCE_PORT=${REFERENCE_PORT:-6701}
ROOT=$(mktemp -d "${TMPDIR:-/tmp}/pg-application-name.XXXXXX")
NATIVE= C_RUNNING=
cleanup() {
    status=$?
    trap - EXIT
    interrupted=0
    trap 'status=130; interrupted=1' HUP INT TERM
    if [ "$status" -ne 0 ] && [ "${PARKING_KEEP_ON_FAILURE:-0}" = 1 ]; then
        echo "application-name: retained owned native PID=$NATIVE PGDATA=$ROOT/native control=$ROOT/native.sock"
        echo "application-name: debug artifacts=$ROOT"
        remaining=${PARKING_DEBUG_SECONDS:-600}
        while [ ! -f "$ROOT/debug-release" ] && [ "$remaining" -gt 0 ] && [ "$interrupted" -eq 0 ]; do
            sleep 1
            remaining=$((remaining - 1))
        done
    fi
    if [ -n "$NATIVE" ]; then
        if "$PATRONI_PYTHON" "$BASE/t/interop/signal-owned-server.py" "$NATIVE" "$ROOT/native" KILL 5; then
            wait "$NATIVE" 2>/dev/null || :
        fi
    fi
    if [ -n "$C_RUNNING" ]; then
        "$PG_C_BINDIR/pg_ctl" -D "$ROOT/c" -m fast -w stop >"$ROOT/c-stop.log" 2>&1 || :
    fi
    echo "application-name: artifacts=$ROOT"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
export LD_LIBRARY_PATH="${PG_C_LIBDIR:-$PG_C_BINDIR/../lib}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
unset PGAPPNAME
"$PG_C_BINDIR/initdb" -D "$ROOT/c" -U postgres -A trust --no-locale -E UTF8 >"$ROOT/c-init.log"
C_RUNNING=1
"$PG_C_BINDIR/pg_ctl" -D "$ROOT/c" -l "$ROOT/c.log" \
    -o "-h 127.0.0.1 -p $REFERENCE_PORT -k $ROOT -c timezone=UTC" -w -t 20 start >"$ROOT/c-start.log"
read -r C_PID <"$ROOT/c/postmaster.pid"
test_wait_owned_sql_server "$C_PID" "$REFERENCE_PORT" "${PSQL:-psql}"
"$BASE/pg-lisp.sh" -p "$PORT" -D "$ROOT/native" --control-socket "$ROOT/native.sock" >"$ROOT/native.log" 2>&1 &
NATIVE=$!
test_wait_owned_sql_server "$NATIVE" "$PORT" "${PSQL:-psql}"
timeout --foreground --kill-after=5 90 "$PATRONI_PYTHON" "$BASE/t/interop/application-name.py" \
    "$REFERENCE_PORT" "$PORT" "$BASE" "$ROOT" "${SBCL:-sbcl}"
"$PATRONI_PYTHON" "$BASE/t/interop/signal-owned-server.py" "$NATIVE" "$ROOT/native" INT 20
wait "$NATIVE"
NATIVE=
