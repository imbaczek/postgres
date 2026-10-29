#!/bin/sh
# One committed transaction exceeds 65 MiB while every row stays below its event limit.
set -eu
BASE=$(cd "$(dirname "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
PORT_BASE=${PORT_BASE:-5934}
SOURCE_PORT=$PORT_BASE
TARGET_PORT=$((PORT_BASE + 1))
TMP=$(mktemp -d /tmp/pg-logical-large-tx.XXXXXX)
SOURCE_PID= TARGET_PID= WORKER_PID=
cleanup() {
    status=$?
    trap - EXIT
    for pid in "$WORKER_PID" "$SOURCE_PID" "$TARGET_PID"; do
        [ -z "$pid" ] || kill "$pid" 2>/dev/null || true
    done
    for pid in "$WORKER_PID" "$SOURCE_PID" "$TARGET_PID"; do
        [ -z "$pid" ] || wait "$pid" 2>/dev/null || true
    done
    if [ "$status" -ne 0 ]; then echo "logical-large-transaction: artifacts $TMP" >&2; else rm -rf "$TMP"; fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
sql() { psql -X -qAt -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$1" -U postgres -d postgres -c "$2"; }
start_source() {
    "$BASE/pg-lisp.sh" -p "$SOURCE_PORT" -D "$TMP/source" --control-socket "$TMP/source.sock" >>"$TMP/source.log" 2>&1 &
    SOURCE_PID=$!
    test_wait_owned_sql_server "$SOURCE_PID" "$SOURCE_PORT" psql 300
}
start_target() {
    "$BASE/pg-lisp.sh" -p "$TARGET_PORT" -D "$TMP/target" --control-socket "$TMP/target.sock" >>"$TMP/target.log" 2>&1 &
    TARGET_PID=$!
    test_wait_owned_sql_server "$TARGET_PID" "$TARGET_PORT" psql 300
}
start_worker() {
    "$BASE/pg-lisp-subscribe" --slot large_tx --source-port "$SOURCE_PORT" --target-port "$TARGET_PORT" --table big_tx >>"$TMP/worker.log" 2>&1 &
    WORKER_PID=$!
}
start_source
start_target
sql "$SOURCE_PORT" 'CREATE TABLE big_tx (id int PRIMARY KEY, v text)' >/dev/null
start_worker
i=0
until [ "$(sql "$TARGET_PORT" "SELECT state FROM pg_lisp_subscription_progress WHERE source_slot='large_tx'" 2>/dev/null || true)" = ready ]; do
    kill -0 "$WORKER_PID" 2>/dev/null || { cat "$TMP/worker.log" >&2; exit 1; }
    i=$((i + 1)); [ "$i" -lt 1200 ] || { echo 'copy timed out' >&2; exit 1; }; sleep 0.25
done
kill "$WORKER_PID"; wait "$WORKER_PID" 2>/dev/null || true; WORKER_PID=
# Four individually fragmented 17 MiB events total more than the old 65 MiB cap.
awk 'BEGIN { printf "BEGIN; "; for (row=1;row<=4;row++) { printf "INSERT INTO big_tx VALUES (%d,\047",row; for (i=0;i<17825792;i++) printf "%d",row; printf "\047); "; } print "COMMIT;" }' >"$TMP/change.sql"
psql -X -q -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$SOURCE_PORT" -U postgres -d postgres -f "$TMP/change.sql" >/dev/null
# Replay from durable WAL, not a live backend spool.
kill -KILL "$SOURCE_PID"; wait "$SOURCE_PID" 2>/dev/null || true; SOURCE_PID=
rm -f "$TMP/source.sock"
start_source
start_worker
i=0
until [ "$(sql "$TARGET_PORT" 'SELECT count(*) FROM big_tx' 2>/dev/null || true)" = 4 ]; do
    kill -0 "$WORKER_PID" 2>/dev/null || { cat "$TMP/worker.log" >&2; exit 1; }
    i=$((i + 1)); [ "$i" -lt 2400 ] || { echo 'apply timed out' >&2; exit 1; }; sleep 0.25
done
kill "$WORKER_PID"; wait "$WORKER_PID" 2>/dev/null || true; WORKER_PID=
# Seventy smaller events cross the same boundary through legacy WAL chunks.
awk 'BEGIN { printf "BEGIN; "; for (row=5;row<=74;row++) { printf "INSERT INTO big_tx VALUES (%d,\047",row; for (i=0;i<1048576;i++) printf "x"; printf "\047); "; } print "COMMIT;" }' >"$TMP/change-small.sql"
psql -X -q -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$SOURCE_PORT" -U postgres -d postgres -f "$TMP/change-small.sql" >/dev/null
kill -KILL "$SOURCE_PID"; wait "$SOURCE_PID" 2>/dev/null || true; SOURCE_PID=
rm -f "$TMP/source.sock"
start_source
start_worker
i=0
until [ "$(sql "$TARGET_PORT" 'SELECT count(*) FROM big_tx' 2>/dev/null || true)" = 74 ]; do
    kill -0 "$WORKER_PID" 2>/dev/null || { cat "$TMP/worker.log" >&2; exit 1; }
    i=$((i + 1)); [ "$i" -lt 2400 ] || { echo 'small-event apply timed out' >&2; exit 1; }; sleep 0.25
done
[ "$(sql "$SOURCE_PORT" 'SELECT count(*) FROM big_tx WHERE id <= 4 AND length(v) = 17825792')" = 4 ]
[ "$(sql "$SOURCE_PORT" 'SELECT count(*) FROM big_tx WHERE id >= 5 AND length(v) = 1048576')" = 70 ]
kill "$WORKER_PID"; wait "$WORKER_PID" 2>/dev/null || true; WORKER_PID=
kill "$TARGET_PID"; wait "$TARGET_PID" 2>/dev/null || true; TARGET_PID=
start_target
sql "$SOURCE_PORT" 'SELECT id, length(v), v FROM big_tx ORDER BY id' >"$TMP/source.out"
sql "$TARGET_PORT" 'SELECT id, length(v), v FROM big_tx ORDER BY id' >"$TMP/target.out"
cmp "$TMP/source.out" "$TMP/target.out"
echo 'logical-large-transaction: PASS'
