#!/bin/sh
# Exercise a real source DataRow above the 8 MiB copy batching target.
set -eu
BASE=$(cd "$(dirname "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
PORT_BASE=${PORT_BASE:-5890}
SOURCE_PORT=$PORT_BASE
TARGET_PORT=$((PORT_BASE + 1))
TMP=$(mktemp -d /tmp/pg-logical-large-copy.XXXXXX)
SOURCE_PID=
TARGET_PID=
WORKER_PID=
cleanup() {
    status=$?
    trap - EXIT
    for pid in "$WORKER_PID" "$SOURCE_PID" "$TARGET_PID"; do
        [ -z "$pid" ] || kill "$pid" 2>/dev/null || true
    done
    for pid in "$WORKER_PID" "$SOURCE_PID" "$TARGET_PID"; do
        [ -z "$pid" ] || wait "$pid" 2>/dev/null || true
    done
    if [ "$status" -ne 0 ]; then
        echo "logical-large-copy: artifacts $TMP" >&2
    else
        rm -rf "$TMP"
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
sql() {
    psql -X -qAt -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$1" -U postgres -d postgres -c "$2"
}
"$BASE/pg-lisp.sh" -p "$SOURCE_PORT" -D "$TMP/source" >"$TMP/source.log" 2>&1 &
SOURCE_PID=$!
test_wait_owned_sql_server "$SOURCE_PID" "$SOURCE_PORT" psql 300
"$BASE/pg-lisp.sh" -p "$TARGET_PORT" -D "$TMP/target" >"$TMP/target.log" 2>&1 &
TARGET_PID=$!
test_wait_owned_sql_server "$TARGET_PID" "$TARGET_PORT" psql 300
sql "$SOURCE_PORT" 'CREATE TABLE big_copy (id integer PRIMARY KEY, payload text)' >/dev/null
awk 'BEGIN { printf "INSERT INTO big_copy VALUES (1, \047"; for (i=0;i<8388736;i++) printf "x"; printf "\047);\n" }' >"$TMP/seed.sql"
psql -X -q -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$SOURCE_PORT" -U postgres -d postgres -f "$TMP/seed.sql" >/dev/null
PG_LISP_TEST_FAILPOINT=after-copy-chunk-commit \
PG_LISP_TEST_FAILPOINT_ACTION=crash \
PG_LISP_TEST_FAILPOINT_ONCE_FILE="$TMP/copy-crashed" \
    "$BASE/pg-lisp-subscribe" --slot large_copy --source-port "$SOURCE_PORT" --target-port "$TARGET_PORT" --table big_copy >"$TMP/worker.log" 2>&1 &
WORKER_PID=$!
i=0
until [ -f "$TMP/copy-crashed" ]; do
    kill -0 "$WORKER_PID" 2>/dev/null || { cat "$TMP/worker.log" >&2; exit 1; }
    i=$((i + 1))
    [ "$i" -lt 1200 ] || { echo 'logical-large-copy: copy timed out' >&2; exit 1; }
    sleep 0.25
done
if wait "$WORKER_PID"; then
    echo 'logical-large-copy: copy failpoint did not crash' >&2
    exit 1
else
    worker_status=$?
fi
WORKER_PID=
[ "$worker_status" -eq 137 ]
[ "$(sql "$TARGET_PORT" 'SELECT length(payload) FROM big_copy WHERE id=1')" = 8388736 ]
"$BASE/pg-lisp-subscribe" --slot large_copy --source-port "$SOURCE_PORT" --target-port "$TARGET_PORT" --table big_copy >>"$TMP/worker.log" 2>&1 &
WORKER_PID=$!
i=0
until [ "$(sql "$TARGET_PORT" "SELECT state FROM pg_lisp_subscription_progress WHERE source_slot='large_copy'" 2>/dev/null || true)" = ready ]; do
    kill -0 "$WORKER_PID" 2>/dev/null || { cat "$TMP/worker.log" >&2; exit 1; }
    i=$((i + 1))
    [ "$i" -lt 1200 ] || { echo 'logical-large-copy: resume timed out' >&2; exit 1; }
    sleep 0.25
done
kill "$WORKER_PID"
wait "$WORKER_PID" 2>/dev/null || true
WORKER_PID=
kill "$TARGET_PID"
wait "$TARGET_PID" 2>/dev/null || true
TARGET_PID=
"$BASE/pg-lisp.sh" -p "$TARGET_PORT" -D "$TMP/target" >>"$TMP/target.log" 2>&1 &
TARGET_PID=$!
test_wait_owned_sql_server "$TARGET_PID" "$TARGET_PORT" psql 300
[ "$(sql "$TARGET_PORT" "SELECT state FROM pg_lisp_subscription_progress WHERE source_slot='large_copy'")" = ready ]
awk 'BEGIN { printf "SELECT payload = \047"; for (i=0;i<8388736;i++) printf "x"; printf "\047 FROM big_copy WHERE id=1;\n" }' >"$TMP/check.sql"
[ "$(psql -X -qAt -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$TARGET_PORT" -U postgres -d postgres -f "$TMP/check.sql")" = t ]
echo 'logical-large-copy: PASS'
