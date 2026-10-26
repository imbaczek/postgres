#!/bin/sh
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
SBCL=${SBCL:-sbcl}
PSQL=${PSQL:-psql}
PATRONI_PYTHON=${PATRONI_PYTHON:-/tmp/pg-patroni-4.1.5-venv/bin/python3}
PORT=${PORT:-6586}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-control-data.XXXXXX")
SERVER= CLIENTS=
cleanup() {
    status=$?
    trap - EXIT
    for pid in "$CLIENTS" "$SERVER"; do
        if [ -n "$pid" ]; then kill -KILL "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; fi
    done
    echo "control-data: artifacts=$TMP"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
sql() { PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres -qAt -v ON_ERROR_STOP=1 -c "$1"; }
start() {
    rm -f "$CASE/control.sock"
    if [ -n "${PAUSE:-}" ]; then
        PG_CONTROL_PAUSE=$PAUSE PG_CONTROL_DATA="$CASE/data" PG_CONTROL_PORT="$PORT" \
            PG_CONTROL_SOCKET="$CASE/control.sock" PG_CONTROL_MARKER="$CASE/marker" \
            "$SBCL" --script "$BASE/t/control-data-native.lisp" >>"$CASE/server.log" 2>&1 &
    else
        "$BASE/pg-lisp.sh" -p "$PORT" -D "$CASE/data" --control-socket "$CASE/control.sock" >>"$CASE/server.log" 2>&1 &
    fi
    SERVER=$!
}
ready() { test_wait_owned_sql_server "$SERVER" "$PORT" "$PSQL"; }
stop() { kill -"$1" "$SERVER"; wait "$SERVER"; SERVER=; }
crash() { kill -KILL "$SERVER"; wait "$SERVER" 2>/dev/null || :; SERVER=; }
inspect() { "$PATRONI_PYTHON" "$BASE/t/interop/control-data-inspect.py" "$CASE/data" "$1"; }
clone() {
    CASE=$TMP/$1
    mkdir -m 700 "$CASE"
    cp -a "$TMP/base/data" "$CASE/data"
}
barrier() {
    attempt=0
    while [ ! -e "$CASE/marker" ]; do
        kill -0 "$SERVER" 2>/dev/null || { cat "$CASE/server.log" >&2; exit 1; }
        attempt=$((attempt + 1)); test "$attempt" -lt 300; sleep 0.1
    done
    read -r marker_pid <"$CASE/marker"
    test "$marker_pid" = "$SERVER"
}
clients() {
    "$PATRONI_PYTHON" "$BASE/t/interop/control-data-clients.py" "$PORT" "$CASE" >"$CASE/clients.log" 2>&1 &
    CLIENTS=$!
    attempt=0
    while [ ! -e "$CASE/clients-ready" ]; do
        kill -0 "$CLIENTS" 2>/dev/null || { cat "$CASE/clients.log" >&2; exit 1; }
        attempt=$((attempt + 1)); test "$attempt" -lt 200; sleep 0.1
    done
}
end_clients() { kill -TERM "$CLIENTS" 2>/dev/null || :; wait "$CLIENTS" 2>/dev/null || :; CLIENTS=; }
CASE=$TMP/base
mkdir -m 700 "$CASE"
PAUSE=
start; ready
sql 'CREATE TABLE lifecycle (id integer PRIMARY KEY); INSERT INTO lifecycle VALUES (1),(3)'
inspect 'in production'
stop TERM
inspect 'shut down'
for mode in TERM INT QUIT KILL; do
    clone "$mode"
    start; ready
    if [ "$mode" != TERM ]; then clients; fi
    sql 'INSERT INTO lifecycle VALUES (4)'
    sql CHECKPOINT
    if [ "$mode" = KILL ]; then crash; else stop "$mode"; fi
    if [ -n "$CLIENTS" ]; then end_clients; fi
    case "$mode" in
        TERM|INT) inspect 'shut down' ;;
        *) inspect 'in production' ;;
    esac
    start; ready
    test "$(sql 'SELECT count(*) FROM lifecycle')" = 3
    test "$(sql 'SELECT count(*) FROM lifecycle WHERE id=2')" = 0
    sql 'INSERT INTO lifecycle VALUES (2)'
    stop INT
    inspect 'shut down'
    echo "control-data: $mode transaction and snapshot cleanup PASS"
done
for point in after-primary-startup-dirty after-primary-shutdown-dirty after-primary-shutdown-checkpoint after-primary-shutdown-clean; do
    clone "$point"
    PAUSE=$point
    start
    if [ "$point" != after-primary-startup-dirty ]; then
        ready
        sql 'INSERT INTO lifecycle VALUES (4)'
        kill -INT "$SERVER"
    fi
    barrier
    case "$point" in
        after-primary-startup-dirty) inspect 'in crash recovery' ;;
        after-primary-shutdown-clean) inspect 'shut down' ;;
        *) inspect 'shutting down' ;;
    esac
    crash
    PAUSE=
    start; ready
    expected=3
    if [ "$point" = after-primary-startup-dirty ]; then expected=2; fi
    test "$(sql 'SELECT count(*) FROM lifecycle')" = "$expected"
    stop INT
    inspect 'shut down'
    echo "control-data: $point process crash PASS"
done
echo 'control-data: primary lifecycle, durable state and read-only inspection PASS'
