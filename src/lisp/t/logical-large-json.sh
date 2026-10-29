#!/bin/sh
# Exercise fragmented logical capture and apply of external JSON and JSONB.
set -eu
BASE=$(cd "$(dirname "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
PORT_BASE=${PORT_BASE:-5892}
SOURCE_PORT=$PORT_BASE
TARGET_PORT=$((PORT_BASE + 1))
TMP=$(mktemp -d /tmp/pg-logical-large-json.XXXXXX)
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
        echo "logical-large-json: artifacts $TMP" >&2
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
    "$BASE/pg-lisp-subscribe" --slot large_json --source-port "$SOURCE_PORT" --target-port "$TARGET_PORT" --table big_json >>"$TMP/worker.log" 2>&1 &
    WORKER_PID=$!
}
stop_worker() {
    kill "$WORKER_PID"
    wait "$WORKER_PID" 2>/dev/null || true
    WORKER_PID=
}
assert_external() {
    printf '%s\n(quit)\n' '(let* ((db (gethash "postgres" (catalog-databases *catalog*))) (table (gethash "big_json" (database-tables db))) (snapshot (make-snapshot 0)) (found nil)) (heapam-scan (pg-class-relfilenode table) (pg-class-columns table) snapshot (lambda (tuple) (let ((values (htup-values tuple))) (when (= (aref values 0) 1) (unless (and (heap-external-varlena-p (aref values 1)) (heap-external-varlena-p (aref values 2))) (error "JSON values are not external")) (setf found t))))) (unless found (error "missing JSON row")) (format t "EXTERNAL-OK~%"))' | sbcl --script "$BASE/t/control-client.lisp" unix "$1" >"$TMP/external.out"
    if grep -q ';; error:' "$TMP/external.out"; then cat "$TMP/external.out" >&2; exit 1; fi
    grep -q 'EXTERNAL-OK' "$TMP/external.out"
}
start_source
start_target
sql "$SOURCE_PORT" 'CREATE TABLE big_json (id int PRIMARY KEY, j json, b jsonb)' >/dev/null
sql "$SOURCE_PORT" 'ALTER TABLE big_json ALTER COLUMN j SET STORAGE EXTERNAL' >/dev/null
sql "$SOURCE_PORT" 'ALTER TABLE big_json ALTER COLUMN b SET STORAGE EXTERNAL' >/dev/null
start_worker
i=0
until [ "$(sql "$TARGET_PORT" "SELECT state FROM pg_lisp_subscription_progress WHERE source_slot='large_json'" 2>/dev/null || true)" = ready ]; do
    kill -0 "$WORKER_PID" 2>/dev/null || { cat "$TMP/worker.log" >&2; exit 1; }
    i=$((i + 1)); [ "$i" -lt 1200 ] || { echo 'logical-large-json: copy timed out' >&2; exit 1; }
    sleep 0.25
done
stop_worker
sql "$TARGET_PORT" 'ALTER TABLE big_json ALTER COLUMN j SET STORAGE EXTERNAL' >/dev/null
sql "$TARGET_PORT" 'ALTER TABLE big_json ALTER COLUMN b SET STORAGE EXTERNAL' >/dev/null
# The first row's two 8.4 MB fields make one logical row exceed a 16 MiB WAL
# record. Other changes in the same transaction check fragment ordering.
awk 'BEGIN {
  printf "BEGIN; INSERT INTO big_json VALUES (1, \047 { \"z\" : \""
  for (i=0;i<8400000;i++) printf "x"
  printf "\", \"a\":1, \"a\":2 } \047::json, \047{\"z\":\""
  for (i=0;i<8400000;i++) printf "y"
  printf "\",\"a\":1,\"a\":2}\047::jsonb); INSERT INTO big_json VALUES (2,NULL,\047null\047::jsonb),(3,\047null\047::json,NULL),(4,\047null\047::json,NULL); UPDATE big_json SET b=\047null\047::jsonb WHERE id=3; COMMIT;\n"
}' >"$TMP/change.sql"
psql -X -q -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$SOURCE_PORT" -U postgres -d postgres -f "$TMP/change.sql" >/dev/null
awk 'BEGIN { printf " { \"z\" : \""; for (i=0;i<8400000;i++) printf "x"; printf "\", \"a\":1, \"a\":2 } \n" }' >"$TMP/expected-json"
awk 'BEGIN { printf "{\"a\": 2, \"z\": \""; for (i=0;i<8400000;i++) printf "y"; printf "\"}\n" }' >"$TMP/expected-jsonb"
kill "$SOURCE_PID"; wait "$SOURCE_PID" 2>/dev/null || true; SOURCE_PID=
start_source
sql "$SOURCE_PORT" 'SELECT j::text FROM big_json WHERE id=1' >"$TMP/source-json"
sql "$SOURCE_PORT" 'SELECT b::text FROM big_json WHERE id=1' >"$TMP/source-jsonb"
cmp "$TMP/expected-json" "$TMP/source-json"
cmp "$TMP/expected-jsonb" "$TMP/source-jsonb"
assert_external "$TMP/source.sock"
start_worker
i=0
until [ "$(sql "$TARGET_PORT" 'SELECT count(*) FROM big_json' 2>/dev/null || true)" = 4 ]; do
    kill -0 "$WORKER_PID" 2>/dev/null || { cat "$TMP/worker.log" >&2; exit 1; }
    i=$((i + 1)); [ "$i" -lt 1200 ] || { echo 'logical-large-json: apply timed out' >&2; exit 1; }
    sleep 0.25
done
stop_worker
kill "$TARGET_PID"; wait "$TARGET_PID" 2>/dev/null || true; TARGET_PID=
start_target
sql "$TARGET_PORT" 'SELECT j::text FROM big_json WHERE id=1' >"$TMP/target-json"
sql "$TARGET_PORT" 'SELECT b::text FROM big_json WHERE id=1' >"$TMP/target-jsonb"
cmp "$TMP/expected-json" "$TMP/target-json"
cmp "$TMP/expected-jsonb" "$TMP/target-jsonb"
assert_external "$TMP/target.sock"
[ "$(sql "$TARGET_PORT" "SELECT (j IS NULL), b::text FROM big_json WHERE id=2")" = 't|null' ]
[ "$(sql "$TARGET_PORT" "SELECT j::text, b::text, (b IS NULL) FROM big_json WHERE id=3")" = 'null|null|f' ]
[ "$(sql "$TARGET_PORT" "SELECT j::text, (b IS NULL) FROM big_json WHERE id=4")" = 'null|t' ]
echo 'logical-large-json: PASS'
