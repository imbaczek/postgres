#!/bin/sh
# Replicate one bytea value larger than a logical WAL record.
set -eu
BASE=$(cd "$(dirname "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
PORT_BASE=${PORT_BASE:-6990}
SOURCE_PORT=$PORT_BASE
TARGET_PORT=$((PORT_BASE + 1))
TMP=$(mktemp -d /tmp/pg-logical-large-bytea.XXXXXX)
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
        echo "logical-large-bytea: artifacts $TMP" >&2
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
    "$BASE/pg-lisp-subscribe" --slot large_bytea --source-port "$SOURCE_PORT" --target-port "$TARGET_PORT" --table big_bytea >>"$TMP/worker.log" 2>&1 &
    WORKER_PID=$!
}
stop_worker() {
    kill "$WORKER_PID"
    wait "$WORKER_PID" 2>/dev/null || true
    WORKER_PID=
}
assert_external() {
    printf '%s\n(quit)\n' '(let* ((db (gethash "postgres" (catalog-databases *catalog*))) (table (gethash "big_bytea" (database-tables db))) (snapshot (make-snapshot 0)) (found nil)) (heapam-scan (pg-class-relfilenode table) (pg-class-columns table) snapshot (lambda (tuple) (let ((values (htup-values tuple))) (when (= (aref values 0) 1) (unless (heap-external-varlena-p (aref values 1)) (error "bytea is not external")) (setf found t))))) (unless found (error "missing bytea row")) (format t "EXTERNAL-OK~%"))' | sbcl --script "$BASE/t/control-client.lisp" unix "$1" >"$TMP/external.out"
    if grep -q ';; error:' "$TMP/external.out"; then cat "$TMP/external.out" >&2; exit 1; fi
    grep -q 'EXTERNAL-OK' "$TMP/external.out"
}
start_source
start_target
sql "$SOURCE_PORT" 'CREATE TABLE big_bytea (id int PRIMARY KEY, v bytea)' >/dev/null
sql "$SOURCE_PORT" 'ALTER TABLE big_bytea ALTER COLUMN v SET STORAGE EXTERNAL' >/dev/null
start_worker
i=0
until [ "$(sql "$TARGET_PORT" "SELECT state FROM pg_lisp_subscription_progress WHERE source_slot='large_bytea'" 2>/dev/null || true)" = ready ]; do
    kill -0 "$WORKER_PID" 2>/dev/null || { cat "$TMP/worker.log" >&2; exit 1; }
    i=$((i + 1)); [ "$i" -lt 1200 ] || { echo 'logical-large-bytea: copy timed out' >&2; exit 1; }
    sleep 0.25
done
stop_worker
sql "$TARGET_PORT" 'ALTER TABLE big_bytea ALTER COLUMN v SET STORAGE EXTERNAL' >/dev/null
# 16,777,500 raw bytes exceed the 16 MiB record limit, and contain zero and
# high-bit octets throughout. The following update depends on an earlier insert.
awk 'BEGIN {
  printf "BEGIN; INSERT INTO big_bytea VALUES (1, \047\\x"
  for (i=0;i<5592500;i++) printf "0080ff"
  printf "\047::bytea), (2, NULL), (3, \047\\x00\047::bytea); UPDATE big_bytea SET v=\047\\x80ff00\047::bytea WHERE id=3; COMMIT;\n"
}' >"$TMP/change.sql"
psql -X -q -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$SOURCE_PORT" -U postgres -d postgres -f "$TMP/change.sql" >/dev/null
awk 'BEGIN { printf "\\x"; for (i=0;i<5592500;i++) printf "0080ff"; printf "\n" }' >"$TMP/expected"
kill "$SOURCE_PID"; wait "$SOURCE_PID" 2>/dev/null || true; SOURCE_PID=
start_source
sql "$SOURCE_PORT" 'SELECT v FROM big_bytea WHERE id=1' >"$TMP/source-value"
cmp "$TMP/expected" "$TMP/source-value"
assert_external "$TMP/source.sock"
start_worker
i=0
until [ "$(sql "$TARGET_PORT" 'SELECT count(*) FROM big_bytea' 2>/dev/null || true)" = 3 ]; do
    kill -0 "$WORKER_PID" 2>/dev/null || { cat "$TMP/worker.log" >&2; exit 1; }
    i=$((i + 1)); [ "$i" -lt 1200 ] || { echo 'logical-large-bytea: apply timed out' >&2; exit 1; }
    sleep 0.25
done
stop_worker
kill "$TARGET_PID"; wait "$TARGET_PID" 2>/dev/null || true; TARGET_PID=
start_target
sql "$TARGET_PORT" 'SELECT v FROM big_bytea WHERE id=1' >"$TMP/target-value"
cmp "$TMP/expected" "$TMP/target-value"
assert_external "$TMP/target.sock"
[ "$(sql "$TARGET_PORT" 'SELECT v IS NULL FROM big_bytea WHERE id=2')" = t ]
[ "$(sql "$TARGET_PORT" 'SELECT v FROM big_bytea WHERE id=3')" = '\x80ff00' ]
echo 'logical-large-bytea: PASS'
