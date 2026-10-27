#!/bin/sh
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
PATRONI_PYTHON=${PATRONI_PYTHON:-/tmp/pg-patroni-4.1.5-venv/bin/python3}
PG_C_BINDIR=${PG_C_BINDIR:?set PG_C_BINDIR to the pinned C PostgreSQL bin directory}
REFERENCE_PORT=${REFERENCE_PORT:-6690}
PORT=${PORT:-6680}
MATRIX=$(mktemp -d "${TMPDIR:-/tmp}/pg-executor-parking.XXXXXX")
C_RUNNING=
cleanup() {
    status=$?
    trap - EXIT
    trap 'status=130' HUP INT TERM
    if [ -n "$C_RUNNING" ]; then
        "$PG_C_BINDIR/pg_ctl" -D "$MATRIX/c-data" -m fast -w stop >"$MATRIX/c-stop.log" 2>&1 || :
    fi
    echo "executor-parking: matrix artifacts=$MATRIX"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
export LD_LIBRARY_PATH="${PG_C_LIBDIR:-$PG_C_BINDIR/../lib}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
"$PG_C_BINDIR/initdb" -D "$MATRIX/c-data" -U postgres -A trust --no-locale -E UTF8 >"$MATRIX/c-init.log"
C_RUNNING=1
"$PG_C_BINDIR/pg_ctl" -D "$MATRIX/c-data" -l "$MATRIX/c-server.log" \
    -o "-h 127.0.0.1 -p $REFERENCE_PORT -k $MATRIX" -w -t 20 start >"$MATRIX/c-start.log"
read -r C_PID <"$MATRIX/c-data/postmaster.pid"
test_wait_owned_sql_server "$C_PID" "$REFERENCE_PORT" "${PSQL:-psql}"
export PARKING_DRIVER="$BASE/t/interop/executor-parking.py"
export PARKING_SETUP_DRIVER="$PARKING_DRIVER"
export RESOURCE_PARKING_ORACLE="$MATRIX/expected.json"
timeout --foreground --kill-after=5 "${PARKING_DRIVER_TIMEOUT:-180}s" \
    "$PATRONI_PYTHON" "$PARKING_DRIVER" --oracle "$REFERENCE_PORT" "$RESOURCE_PARKING_ORACLE"
if [ "$#" -eq 0 ]; then
    set -- $("$PATRONI_PYTHON" "$PARKING_DRIVER" --list-cases)
fi
for RESOURCE_PARKING_CASE do
    export RESOURCE_PARKING_CASE PORT
    echo "executor-parking: starting $RESOURCE_PARKING_CASE"
    "$BASE/t/live-operation-parking.sh"
done
echo 'executor-parking: requested executor promotion matrix PASS'
