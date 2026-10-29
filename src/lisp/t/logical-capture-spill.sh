#!/bin/sh
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
PORT=${PORT:-5894}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-logical-capture-spill.XXXXXX")
SERVER= CLIENT=
cleanup() {
    status=$?
    trap - EXIT
    for pid in "$CLIENT" "$SERVER"; do
        if [ -n "$pid" ]; then kill "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; fi
    done
    echo "logical-capture-spill: artifacts=$TMP"
    exit "$status"
}
trap cleanup EXIT
sql() {
    PGCONNECT_TIMEOUT=2 psql -X -qAt -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$PORT" -U postgres -d postgres -c "$1"
}
start_server() {
    rm -f "$TMP/control.sock"
    "$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/data" --control-socket "$TMP/control.sock" >>"$TMP/server.log" 2>&1 &
    SERVER=$!
    test_wait_owned_sql_server "$SERVER" "$PORT" psql 300
}
wait_for_line() {
    i=0
    until grep -Fxq "$1" "$TMP/client.log"; do
        kill -0 "$CLIENT" 2>/dev/null || { cat "$TMP/client.log" >&2; exit 1; }
        i=$((i + 1)); [ "$i" -lt 300 ] || exit 1; sleep 0.1
    done
}
start_client() {
    rm -f "$TMP/client.fifo" "$TMP/client.log"
    mkfifo "$TMP/client.fifo"
    PGCONNECT_TIMEOUT=2 psql -X -qAt -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$PORT" -U postgres -d postgres <"$TMP/client.fifo" >"$TMP/client.log" 2>&1 &
    CLIENT=$!
    exec 7>"$TMP/client.fifo"
}
finish_client() {
    exec 7>&-
    wait "$CLIENT"
    CLIENT=
}
spill_count() {
    (rg --files --hidden --no-ignore "$TMP/data/pgsql_tmp" || :) | wc -l | tr -d ' '
}
start_server
sql 'CREATE TABLE spool_rows (id integer PRIMARY KEY, value text)' >/dev/null
printf '%s\n(quit)\n' "(progn (setf *logical-capture-memory-bytes* 0) (setf (symbol-function 'logical-spool-events-in-order) (lambda (spool) (declare (ignore spool)) (error \"unexpected whole-spool materialization\"))))" |
    sbcl --script "$BASE/t/control-client.lisp" unix "$TMP/control.sock" >"$TMP/control.log"
if grep -q ';; error:' "$TMP/control.log"; then cat "$TMP/control.log" >&2; exit 1; fi
start_client
printf '%s\n' 'BEGIN;' "INSERT INTO spool_rows VALUES (1, 'first');" "INSERT INTO spool_rows VALUES (2, 'second');" "SELECT 'captured';" >&7
wait_for_line captured
[ "$(spill_count)" = 1 ]
printf '%s\n' 'COMMIT;' '\q' >&7
finish_client
[ "$(spill_count)" = 0 ]
[ "$(sql 'SELECT id,value FROM spool_rows ORDER BY id')" = "$(printf '1|first\n2|second')" ]
start_client
printf '%s\n' 'BEGIN;' "INSERT INTO spool_rows VALUES (3, 'rollback');" "SELECT 'rollback-ready';" >&7
wait_for_line rollback-ready
[ "$(spill_count)" = 1 ]
printf '%s\n' 'ROLLBACK;' '\q' >&7
finish_client
[ "$(spill_count)" = 0 ]
[ "$(sql 'SELECT count(*) FROM spool_rows WHERE id=3')" = 0 ]
start_client
printf '%s\n' 'BEGIN;' "INSERT INTO spool_rows VALUES (4, 'disconnect');" "SELECT 'disconnect-ready';" >&7
wait_for_line disconnect-ready
[ "$(spill_count)" = 1 ]
printf '%s\n' '\q' >&7
finish_client
i=0
until [ "$(spill_count)" = 0 ]; do i=$((i + 1)); [ "$i" -lt 300 ] || exit 1; sleep 0.1; done
[ "$(sql 'SELECT count(*) FROM spool_rows WHERE id=4')" = 0 ]
kill "$SERVER"
wait "$SERVER" 2>/dev/null || :
SERVER=
start_server
[ "$(sql 'SELECT id,value FROM spool_rows ORDER BY id')" = "$(printf '1|first\n2|second')" ]
# Corrupt the second event only after the first fragment is in WAL. The reader
# must detect the changed spool and leave the transaction without a terminal.
printf '%s\n(quit)\n' '(progn (setf *logical-capture-memory-bytes* 0) (setf *logical-spool-test-after-wal-record* (lambda (spool index) (cond ((= index 1) (wal-flush) (let* ((stream (logical-spool-stream spool)) (resume (file-position stream))) (file-position stream 0) (let* ((first-size (logical-spool-read-u32 stream)) (position (+ 16 first-size))) (file-position stream position) (let ((byte (read-byte stream))) (file-position stream position) (write-byte (logxor byte 255) stream) (finish-output stream))) (file-position stream resume))) ((= index 2) (setf *logical-spool-test-after-wal-record* nil) (wal-flush) (with-open-file (out "'"$TMP"'/fragment-durable" :direction :output :if-exists :supersede) (write-line "durable" out)))))))' |
    sbcl --script "$BASE/t/control-client.lisp" unix "$TMP/control.sock" >"$TMP/control-fault.log"
if grep -q ';; error:' "$TMP/control-fault.log"; then cat "$TMP/control-fault.log" >&2; exit 1; fi
awk 'BEGIN { print "BEGIN; INSERT INTO spool_rows VALUES (10, \047first fragment\047);"; printf "INSERT INTO spool_rows VALUES (11, \047"; for (i=0;i<16778240;i++) printf "x"; print "\047); COMMIT;" }' >"$TMP/corrupt.sql"
if PGCONNECT_TIMEOUT=2 psql -X -qAt -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$PORT" -U postgres -d postgres -f "$TMP/corrupt.sql" >"$TMP/corrupt.out" 2>&1; then
    echo 'logical-capture-spill: corrupted spool unexpectedly committed' >&2
    exit 1
fi
grep -F 'logical spool checksum mismatch' "$TMP/corrupt.out" >/dev/null
[ "$(cat "$TMP/fragment-durable")" = durable ]
if sql "INSERT INTO spool_rows VALUES (12, 'must fail')" >"$TMP/poison.out" 2>&1; then
    echo 'logical-capture-spill: WAL did not fail-stop after partial publication' >&2
    exit 1
fi
i=0
until grep -Fq 'WAL I/O previously failed' "$TMP/poison.out" || grep -Fq 'WAL I/O previously failed' "$TMP/server.log"; do
    i=$((i + 1)); [ "$i" -lt 300 ] || exit 1; sleep 0.1
done
kill -KILL "$SERVER" 2>/dev/null || :
wait "$SERVER" 2>/dev/null || :
SERVER=
start_server
[ "$(sql 'SELECT count(*) FROM spool_rows WHERE id >= 10')" = 0 ]
sql "INSERT INTO spool_rows VALUES (12, 'recovered')" >/dev/null
[ "$(sql 'SELECT value FROM spool_rows WHERE id=12')" = recovered ]
echo 'logical-capture-spill: PASS'
