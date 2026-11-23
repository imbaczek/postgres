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
    "$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/data" -c wal_level=logical --control-socket "$TMP/control.sock" >>"$TMP/server.log" 2>&1 &
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
# Preserve the original failure even if fast shutdown closes the SQL connection
# before it can report the checksum error.
printf '%s\n(quit)\n' '(let ((original (symbol-function '\''wal-fail-stop))) (setf (symbol-function '\''wal-fail-stop) (lambda (condition) (ignore-errors (with-open-file (out "'"$TMP"'/wal-failure.log" :direction :output :if-exists :append :if-does-not-exist :create) (format out "~a~%" condition))) (funcall original condition))))' |
    sbcl --script "$BASE/t/control-client.lisp" unix "$TMP/control.sock" >"$TMP/control-failure.log"
if grep -q ';; error:' "$TMP/control-failure.log"; then cat "$TMP/control-failure.log" >&2; exit 1; fi
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
grep -F 'logical spool checksum mismatch' "$TMP/wal-failure.log" >/dev/null
[ "$(cat "$TMP/fragment-durable")" = durable ]
i=0
until grep -Fq 'received fast shutdown request' "$TMP/server.log"; do
    i=$((i + 1)); [ "$i" -lt 300 ] || exit 1; sleep 0.1
done
kill -KILL "$SERVER" 2>/dev/null || :
wait "$SERVER" 2>/dev/null || :
SERVER=
start_server
[ "$(sql 'SELECT count(*) FROM spool_rows WHERE id >= 10')" = 0 ]
sql "INSERT INTO spool_rows VALUES (12, 'recovered')" >/dev/null
[ "$(sql 'SELECT value FROM spool_rows WHERE id=12')" = recovered ]
test "$(sql "SELECT slot_name FROM pg_create_logical_replication_slot('retirement_probe','pgoutput')")" = retirement_probe
slot_row_sql="SELECT slot_name, plugin, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'retirement_probe'"
slot_row=$(sql "$slot_row_sql")
test -n "$slot_row"
kill -INT "$SERVER"
wait "$SERVER"
SERVER=
slot_state="$TMP/data/pg_replslot/retirement_probe/state"
cp -p "$slot_state" "$TMP/current-slot"
PYTHONPATH="$BASE/t/interop${PYTHONPATH:+:$PYTHONPATH}" python3 - "$TMP" <<'PY'
import sys
from pathlib import Path

from crc32c_reference import crc32c

root = Path(sys.argv[1])
line = (root / "current-slot").read_bytes()
assert line.endswith(b"\n")
prefix, checksum = line[:-1].rsplit(b" ", 1)
assert int(checksum, 16) == crc32c(prefix)
magic, name, database, restart, consistent, confirmed, plugin, profile, *provenance = prefix.split()
assert (magic, name, plugin, profile) == (b"PGSLOT6", b"retirement_probe", b"pgoutput", b"text")
assert provenance
status = b"reserved"


def write_slot(path, fields):
    prefix = b" ".join(fields)
    path.write_bytes(prefix + f" {crc32c(prefix):08X}\n".encode())


legacy = ((name, restart, consistent),
          (name, restart, consistent, confirmed, status),
          (name, database, restart, consistent, confirmed, status))
for version, fields in enumerate(legacy, 1):
    write_slot(root / f"retired-slot-{version}", (f"PGSLOT{version}".encode(), *fields))
write_slot(root / "supported-slot-4",
           (b"PGSLOT4", name, database, restart, consistent, confirmed, status, plugin))
write_slot(root / "supported-slot-5",
           (b"PGSLOT5", name, database, restart, consistent, confirmed, status,
            b"pg_lisp", b"binary-bytea"))
PY
for version in 1 2 3; do
    retired="$TMP/retired-slot-$version"
    cp "$retired" "$slot_state"
    status=0
    timeout 30 "$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/data" -c wal_level=logical \
        >"$retired.log" 2>&1 || status=$?
    test "$status" -eq 1
    grep -Fq 'replication slot file retirement_probe is malformed' "$retired.log"
    cmp "$retired" "$slot_state"
done
for version in 4 5; do
    supported="$TMP/supported-slot-$version"
    cp "$supported" "$slot_state"
    start_server
    expected="$slot_row"
    if [ "$version" -eq 5 ]; then
        expected=$(printf '%s\n' "$slot_row" | sed 's/|pgoutput|/|pg_lisp|/')
    fi
    test "$(sql "$slot_row_sql")" = "$expected"
    cmp "$supported" "$slot_state"
    kill -INT "$SERVER"
    wait "$SERVER"
    SERVER=
done
cp -p "$TMP/current-slot" "$slot_state"
start_server
test "$(sql "$slot_row_sql")" = "$slot_row"
cmp "$TMP/current-slot" "$slot_state"
test "$(sql 'SELECT value FROM spool_rows WHERE id=12')" = recovered
echo 'logical-capture-spill: PASS'
