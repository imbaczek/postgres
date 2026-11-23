#!/bin/sh
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
PSQL=${PSQL:-psql}
PATRONI_PYTHON=${PATRONI_PYTHON:-/tmp/pg-patroni-4.1.5-venv/bin/python3}
PORT=${PORT:-6610}
TARGET_PORT=$((PORT + 1))
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-physical-readers.XXXXXX")
SOURCE= TARGET= READER=
sql() { PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$1" -U postgres -d postgres -qAt -v ON_ERROR_STOP=1 -c "$2"; }
checked() {
    test_lisp_control_checked "$TMP/$1.sock" "$2" "$TMP/$3.log"
}
cleanup() {
    status=$?
    trap - EXIT
    if [ "$status" -ne 0 ] && [ -S "$TMP/target.sock" ]; then
        test_lisp_control "$TMP/target.sock" '(list :failed *physical-recovery-failed-p* :applied *last-replay-lsn* :state *physical-recovery-state* :snapshots *active-snapshots*)' \
            5 >"$TMP/failure-state.log" 2>&1 || :
    fi
    : >"$TMP/reader-release"
    if [ -n "$READER" ]; then kill -TERM "$READER" 2>/dev/null || :; wait "$READER" 2>/dev/null || :; fi
    for pid in "$TARGET" "$SOURCE"; do
        if [ -n "$pid" ]; then kill -KILL "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; fi
    done
    echo "physical-readers: artifacts=$TMP"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
start_target() {
    test_launch_lisp_server "$TARGET_PORT" "$TMP/copy" "$TMP/target.sock" "$TMP/target.log"
    TARGET=$!
    test_wait_owned_sql_server "$TARGET" "$TARGET_PORT" "$PSQL"
}
mkdir -m 700 "$TMP/copy"
test_launch_lisp_server "$PORT" "$TMP/source" "$TMP/source.sock" "$TMP/source.log"
SOURCE=$!
test_wait_owned_sql_server "$SOURCE" "$PORT" "$PSQL"
sql "$PORT" "CREATE TABLE promotion_sql_items (id integer PRIMARY KEY,label text); INSERT INTO promotion_sql_items VALUES (1,'before'); CREATE TABLE row_barriers (id integer); CREATE TABLE row_spill (id integer, value text)"
checked source "(physical-base-backup \"$TMP/copy/\" \"row_readers\")" backup
start_target
checked target "(physical-follow-start \"127.0.0.1\" $PORT \"row_readers\")" follow
"$PATRONI_PYTHON" "$BASE/t/interop/physical-readers.py" "$BASE" "$TMP" "$PORT" "$TARGET_PORT"
for point in after-redo after-progress; do
    rm -f "$TMP/reader-ready" "$TMP/reader-release" "$TMP/crash-marker"
    "$PATRONI_PYTHON" "$BASE/t/interop/physical-readers.py" "$BASE" "$TMP" "$PORT" "$TARGET_PORT" "$point" >"$TMP/$point-reader.log" 2>&1 &
    READER=$!
    attempt=0
    while [ ! -e "$TMP/reader-ready" ]; do
        kill -0 "$READER" 2>/dev/null || { cat "$TMP/$point-reader.log" >&2; exit 1; }
        attempt=$((attempt + 1)); test "$attempt" -lt 200; sleep 0.05
    done
    sql "$PORT" "UPDATE promotion_sql_items SET label='$point' WHERE id=2"
    attempt=0
    while [ ! -e "$TMP/crash-marker" ]; do
        kill -0 "$TARGET"
        attempt=$((attempt + 1)); test "$attempt" -lt 300; sleep 0.05
    done
    read -r marker_pid <"$TMP/crash-marker"
    test "$marker_pid" = "$TARGET"
    kill -KILL "$TARGET"; wait "$TARGET" 2>/dev/null || :; TARGET=
    : >"$TMP/reader-release"
    wait "$READER"; READER=
    start_target
    test "$(sql "$TARGET_PORT" 'SELECT label FROM promotion_sql_items WHERE id=2')" = "$point"
    echo "physical-readers: $point row replay crash/restart PASS"
done
kill -INT "$TARGET"; wait "$TARGET"; TARGET=
kill -INT "$SOURCE"; wait "$SOURCE"; SOURCE=
echo 'physical-readers: PASS'
