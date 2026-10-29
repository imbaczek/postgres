#!/bin/sh
# An UPDATE of another column must not log two copies of a large TOAST value.
set -eu
BASE=$(cd "$(dirname "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
PORT_BASE=${PORT_BASE:-5896}
SOURCE_PORT=$PORT_BASE
TARGET_PORT=$((PORT_BASE + 1))
TMP=$(mktemp -d /tmp/pg-logical-unchanged-toast.XXXXXX)
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
        echo "logical-unchanged-toast: artifacts $TMP" >&2
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
start_source() {
    "$BASE/pg-lisp.sh" -p "$SOURCE_PORT" -D "$TMP/source" >>"$TMP/source.log" 2>&1 &
    SOURCE_PID=$!
    test_wait_owned_sql_server "$SOURCE_PID" "$SOURCE_PORT" psql 300
}
start_target() {
    "$BASE/pg-lisp.sh" -p "$TARGET_PORT" -D "$TMP/target" >>"$TMP/target.log" 2>&1 &
    TARGET_PID=$!
    test_wait_owned_sql_server "$TARGET_PID" "$TARGET_PORT" psql 300
}
start_worker() {
    "$BASE/pg-lisp-subscribe" --slot unchanged_toast --source-port "$SOURCE_PORT" --target-port "$TARGET_PORT" --table unchanged_toast >>"$TMP/worker.log" 2>&1 &
    WORKER_PID=$!
}
stop_worker() {
    kill "$WORKER_PID"
    wait "$WORKER_PID" 2>/dev/null || true
    WORKER_PID=
}
start_source
start_target
sql "$SOURCE_PORT" 'CREATE TABLE unchanged_toast (id int PRIMARY KEY, marker int, v text)' >/dev/null
sql "$SOURCE_PORT" 'ALTER TABLE unchanged_toast ALTER COLUMN v SET STORAGE EXTERNAL' >/dev/null
start_worker
i=0
until [ "$(sql "$TARGET_PORT" "SELECT state FROM pg_lisp_subscription_progress WHERE source_slot='unchanged_toast'" 2>/dev/null || true)" = ready ]; do
    kill -0 "$WORKER_PID" 2>/dev/null || { cat "$TMP/worker.log" >&2; exit 1; }
    i=$((i + 1)); [ "$i" -lt 1200 ] || { echo 'logical-unchanged-toast: copy timed out' >&2; exit 1; }
    sleep 0.25
done
stop_worker
sql "$TARGET_PORT" 'ALTER TABLE unchanged_toast ALTER COLUMN v SET STORAGE EXTERNAL' >/dev/null
awk 'BEGIN { printf "INSERT INTO unchanged_toast VALUES (1, 0, \047"; for (i=0;i<41943040;i++) printf "x"; printf "\047);\n" }' >"$TMP/insert.sql"
psql -X -q -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$SOURCE_PORT" -U postgres -d postgres -f "$TMP/insert.sql" >/dev/null
before_update=$(sql "$SOURCE_PORT" 'SELECT pg_current_wal_lsn()')
sql "$SOURCE_PORT" 'UPDATE unchanged_toast SET marker=1 WHERE id=1' >/dev/null
after_update=$(sql "$SOURCE_PORT" 'SELECT pg_current_wal_lsn()')
[ "$(sql "$SOURCE_PORT" "SELECT pg_wal_lsn_diff('$after_update'::pg_lsn, '$before_update'::pg_lsn) < 1048576")" = t ]
sql "$SOURCE_PORT" 'INSERT INTO unchanged_toast VALUES (1,2,NULL) ON CONFLICT (id) DO UPDATE SET marker=excluded.marker' >/dev/null
sql "$SOURCE_PORT" 'UPDATE unchanged_toast SET id=4 WHERE id=1' >/dev/null
sql "$SOURCE_PORT" 'INSERT INTO unchanged_toast VALUES (2, 2, NULL)' >/dev/null
sql "$SOURCE_PORT" "BEGIN; INSERT INTO unchanged_toast VALUES (3,0,'same transaction'); UPDATE unchanged_toast SET marker=3 WHERE id=3; COMMIT" >/dev/null
kill "$SOURCE_PID"; wait "$SOURCE_PID" 2>/dev/null || true; SOURCE_PID=
start_source
[ "$(sql "$SOURCE_PORT" 'SELECT marker, length(v) FROM unchanged_toast WHERE id=4')" = '2|41943040' ]
start_worker
i=0
until [ "$(sql "$TARGET_PORT" 'SELECT count(*) FROM unchanged_toast' 2>/dev/null || true)" = 3 ] &&
      [ "$(sql "$TARGET_PORT" 'SELECT marker FROM unchanged_toast WHERE id=4' 2>/dev/null || true)" = 2 ]; do
    kill -0 "$WORKER_PID" 2>/dev/null || { cat "$TMP/worker.log" >&2; exit 1; }
    i=$((i + 1)); [ "$i" -lt 1200 ] || { echo 'logical-unchanged-toast: apply timed out' >&2; exit 1; }
    sleep 0.25
done
stop_worker
kill "$TARGET_PID"; wait "$TARGET_PID" 2>/dev/null || true; TARGET_PID=
start_target
[ "$(sql "$TARGET_PORT" 'SELECT marker, length(v) FROM unchanged_toast WHERE id=4')" = '2|41943040' ]
[ "$(sql "$TARGET_PORT" 'SELECT v IS NULL FROM unchanged_toast WHERE id=2')" = t ]
[ "$(sql "$TARGET_PORT" 'SELECT marker, v FROM unchanged_toast WHERE id=3')" = '3|same transaction' ]
awk 'BEGIN { for (i=0;i<41943040;i++) printf "x"; printf "\n" }' >"$TMP/expected"
sql "$TARGET_PORT" 'SELECT v FROM unchanged_toast WHERE id=4' >"$TMP/actual"
cmp "$TMP/expected" "$TMP/actual"
echo 'logical-unchanged-toast: PASS'
