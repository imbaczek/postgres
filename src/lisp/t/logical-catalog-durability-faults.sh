#!/bin/sh
# DDL remains published when an injected error arrives after its WAL terminal.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
PORT=${PORT_BASE:-5630}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-catalog-faults.XXXXXX")
DATA=$TMP/data
SERVER_PID=

sql() {
    SQL_TEXT=$1
    psql -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres \
        -q -v ON_ERROR_STOP=1 -A -t -F'|' -c "$SQL_TEXT"
}

start_server() {
    SERVER_LOG=$1
    if [ "$#" -eq 3 ]; then
        FAULT_ACTION=$2
        FAULT_MARKER=$3
        PG_LISP_TEST_FAILPOINT=after-catalog-event-wal-terminal \
        PG_LISP_TEST_FAILPOINT_ACTION=$FAULT_ACTION \
        PG_LISP_TEST_FAILPOINT_ONCE_FILE=$FAULT_MARKER \
            "$BASE/pg-lisp.sh" -p "$PORT" -D "$DATA" > "$SERVER_LOG" 2>&1 &
    else
        "$BASE/pg-lisp.sh" -p "$PORT" -D "$DATA" > "$SERVER_LOG" 2>&1 &
    fi
    SERVER_PID=$!
    i=0
    while [ "$i" -lt 200 ]; do
        if psql -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres \
                -c '' >/dev/null 2>&1; then
            return 0
        fi
        if ! kill -0 "$SERVER_PID" 2>/dev/null; then
            echo "logical-catalog-durability-faults: server exited: $SERVER_LOG" >&2
            return 1
        fi
        i=$((i + 1))
        sleep 0.1
    done
    echo "logical-catalog-durability-faults: server did not start" >&2
    return 1
}

stop_server() {
    [ -n "$SERVER_PID" ] || return 0
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    SERVER_PID=
}

wait_for_file() {
    FILE_PATH=$1
    i=0
    while [ "$i" -lt 100 ]; do
        [ -f "$FILE_PATH" ] && return 0
        i=$((i + 1))
        sleep 0.1
    done
    echo "logical-catalog-durability-faults: missing marker $FILE_PATH" >&2
    return 1
}

cleanup() {
    stop_server
    rm -rf "$TMP"
}

on_exit() {
    status=$?
    trap - EXIT
    if [ "$status" -ne 0 ]; then
        echo "logical-catalog-durability-faults: failure; logs follow" >&2
        for log in "$TMP"/*.log; do
            [ -f "$log" ] || continue
            echo "--- $log" >&2
            cat "$log" >&2
        done
    fi
    cleanup
    exit "$status"
}

trap on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Prepare ALTER TABLE's initial relation without an armed failpoint.
start_server "$TMP/prepare.log"
sql "CREATE TABLE durable_alter (id integer, value integer); INSERT INTO durable_alter VALUES (1, 10), (2, 20)" >/dev/null
stop_server

# CREATE TABLE's cleanup must not erase a catalog image whose event terminal
# is already durable. save-catalog() uses this same terminal boundary as the
# subscriber's catalog-event high-water mark.
start_server "$TMP/create-table.log" error "$TMP/create-table.marker"
if sql "CREATE TABLE durable_create (id integer PRIMARY KEY)" \
        > "$TMP/create-table-client.log" 2>&1; then
    echo "logical-catalog-durability-faults: CREATE TABLE did not report the injected error" >&2
    exit 1
fi
wait_for_file "$TMP/create-table.marker"
if [ "$(sql "SELECT count(*) FROM durable_create")" != 0 ]; then
    echo "logical-catalog-durability-faults: durable CREATE TABLE was rolled back in memory" >&2
    exit 1
fi
stop_server

# ALTER TABLE's cleanup must preserve both its constraint index and nullability
# after the catalog terminal is durable.
start_server "$TMP/alter-table.log" error "$TMP/alter-table.marker"
if sql "ALTER TABLE durable_alter ADD CONSTRAINT durable_alter_value_key UNIQUE (value)" \
        > "$TMP/alter-table-client.log" 2>&1; then
    echo "logical-catalog-durability-faults: ALTER TABLE did not report the injected error" >&2
    exit 1
fi
wait_for_file "$TMP/alter-table.marker"
if sql "INSERT INTO durable_alter VALUES (3, 10)" >/dev/null 2>&1; then
    echo "logical-catalog-durability-faults: durable ALTER TABLE constraint was rolled back" >&2
    exit 1
fi
stop_server

# Sequence DDL uses call-with-catalog-durability-rollback; the durable-error
# flag must cover the after-terminal hook as well as wal-commit-catalog errors.
start_server "$TMP/create-sequence.log" error "$TMP/create-sequence.marker"
if sql "CREATE SEQUENCE durable_sequence START WITH 10" \
        > "$TMP/create-sequence-client.log" 2>&1; then
    echo "logical-catalog-durability-faults: CREATE SEQUENCE did not report the injected error" >&2
    exit 1
fi
wait_for_file "$TMP/create-sequence.marker"
if [ "$(sql "SELECT nextval('durable_sequence')")" != 10 ]; then
    echo "logical-catalog-durability-faults: durable CREATE SEQUENCE was rolled back" >&2
    exit 1
fi

echo "logical-catalog-durability-faults: PASS"
