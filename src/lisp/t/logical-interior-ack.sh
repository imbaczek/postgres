#!/bin/sh
# Real publisher crash/restart after a flush inside its commit record.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
PORT=${PORT_BASE:-5820}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-interior-ack.XXXXXX")
SERVER_PID=
cleanup() {
    status=$?
    trap - EXIT
    if [ -n "$SERVER_PID" ]; then
        kill "$SERVER_PID" 2>/dev/null || :
        wait "$SERVER_PID" 2>/dev/null || :
    fi
    if [ "$status" -ne 0 ] || [ "${KEEP_ARTIFACTS:-0}" = 1 ]; then
        echo "logical-interior-ack: artifacts $TMP" >&2
    else rm -rf "$TMP"; fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
start_server() {
    "$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/data" >>"$TMP/server.log" 2>&1 &
    SERVER_PID=$!
    test_wait_owned_sql_server "$SERVER_PID" "$PORT"
}
crash_restart() {
    kill -KILL "$SERVER_PID"
    wait "$SERVER_PID" 2>/dev/null || :
    SERVER_PID=
    start_server
}
run_phase() {
    "${SBCL:-sbcl}" --script "$BASE/t/interop/interior-ack-client.lisp" \
        "$1" "$PORT" "$TMP/evidence.sexp" >"$TMP/$1.log" 2>&1 || {
        cat "$TMP/$1.log" >&2
        return 1
    }
    cat "$TMP/$1.log"
}
start_server
psql -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres -v ON_ERROR_STOP=1 \
    -c 'CREATE TABLE interior_ack_rows (id integer PRIMARY KEY); CREATE PUBLICATION interior_ack_pub FOR TABLE interior_ack_rows' >/dev/null
run_phase capture
crash_restart
run_phase replay
crash_restart
run_phase confirmed
echo 'logical-interior-ack: PASS'
