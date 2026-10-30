#!/bin/sh
# Exercise the explicit native reporter in an owned server, before startup wiring.
set -eu

BASE=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
SBCL=${SBCL:-sbcl}
PORT=${PORT:-5589}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-resource-crash.XXXXXX")
SOCK="$TMP/control.sock"
LOG="$TMP/server.log"
SRV=
. "$BASE/t/test-support.sh"
test_debug_setup resource-parameters-crash "$PORT" "$TMP" "$LOG"

cleanup() {
    if [ -n "$SRV" ]; then
        kill "$SRV" 2>/dev/null || true
        wait "$SRV" 2>/dev/null || true
    fi
    if [ "${KEEP_TEST_TMP:-0}" = 1 ]; then
        echo "resource-parameters-crash: artifacts $TMP"
    else
        rm -rf "$TMP"
    fi
}
test_debug_install_traps cleanup

start() {
    # Every previous owned PID has been reaped; SIGKILL leaves its socket node.
    [ -z "$SRV" ]
    rm -f "$SOCK"
    test_debug_start_server "$LOG" -p "$PORT" -D "$TMP/data" --control-socket "$SOCK" \
        -c max_locks_per_transaction=64
    test_wait_owned_sql_server "$SRV" "$PORT" psql 300
    control "(assert (equal (namestring *pgdata*) \"$TMP/data/\"))"
}

control() {
    response=$(printf '(assert (= (sb-posix:getpid) %s))\n%s\n(quit)\n' "$SRV" "$1" |
        timeout 30 "$SBCL" --script "$BASE/t/control-client.lisp" unix "$SOCK")
    case "$response" in
        *';; error:'*|*';; read error:'*) echo "$response" >&2; return 1 ;;
    esac
}

sql() {
    PGCONNECT_TIMEOUT=3 timeout 20 psql -X -h 127.0.0.1 -p "$PORT" -U postgres \
        -d postgres -qAt -v ON_ERROR_STOP=1 -c "$1"
}

check_control() {
    python3 - "$BASE" "$TMP/data/global/pg_wal_control" "$1" <<'PY'
import runpy
import struct
import sys
import zlib
from pathlib import Path

if not __debug__:
    raise RuntimeError("resource crash assertions require Python assertions")
base, path, expected = sys.argv[1:]
line = Path(path).read_text().strip()
prefix, crc = line.rsplit(" ", 1)
assert zlib.crc32(prefix.encode()) == int(crc, 16), line
fields = prefix.split()
runpy.run_path(str(Path(base) / "t/interop/resource_control_fields.py"))["check_optional_fields"](fields)
index = fields.index("R")
actual = struct.unpack_from("<5I", bytes.fromhex(fields[index + 1]), 8)
assert actual == tuple(map(int, expected.split(","))), actual
PY
}

wait_for_sigkill() {
    attempt=0
    while kill -0 "$SRV" 2>/dev/null; do
        if [ "$attempt" -ge 100 ]; then
            echo 'resource-parameters-crash: hook did not kill the owned server' >&2
            return 1
        fi
        attempt=$((attempt + 1))
        sleep 0.1
    done
    status=0
    wait "$SRV" || status=$?
    [ "$status" -eq 137 ] || {
        echo "resource-parameters-crash: expected SIGKILL, got status $status" >&2
        return 1
    }
    SRV=
}

start
sql 'CREATE TABLE resource_crash_witness (id int); INSERT INTO resource_crash_witness VALUES (2)'

for point in after-wal-flush after-control-save; do
    control '(wal-report-resource-parameters (make-resource-parameters 100 8 10 0 128))'
    check_control 100,8,10,0,128
    control "(setf *wal-resource-parameters-test-hook*
      (lambda (point end) (declare (ignore end))
        (when (eq point :$point) (sb-posix:kill (sb-posix:getpid) sb-posix:sigkill))))"
    # EOF is expected: the hook kills exactly the server identified above.
    control '(wal-report-resource-parameters (make-resource-parameters 200 16 20 0 256))' || true
    wait_for_sigkill
    if [ "$point" = after-wal-flush ]; then
        check_control 100,8,10,0,128
    else
        check_control 200,16,20,0,256
    fi
    start
    check_control 100,8,10,0,64
    python3 "$BASE/t/interop/resource-report-history.py" "$TMP/data" \
        200,16,20,0,256 100,8,10,0,64
    [ "$(sql 'SELECT id FROM resource_crash_witness')" = 2 ]
    control '(let ((end *wal-end-lsn*))
      (wal-report-resource-parameters *installed-resource-parameters*)
      (assert (= end *wal-end-lsn*)))'
done

# A lower tuple must survive checkpoint recycling and another fresh image.
control '(wal-report-resource-parameters (make-resource-parameters 50 4 5 0 64))'
sql CHECKPOINT
mkdir "$TMP/backup"
control "(physical-base-backup \"$TMP/backup\" \"resource_seed\")"
python3 "$BASE/t/interop/checkpoint-snapshot.py" "$TMP/data" --save "$TMP/checkpoint.json"
python3 "$BASE/t/interop/checkpoint-snapshot.py" "$TMP/backup" --expect "$TMP/checkpoint.json"
control "(let ((*pgdata* #p\"$TMP/backup/\"))
  (multiple-value-bind (state control) (physical-recovery-verify-base-state)
    (declare (ignore control))
    (assert (equal '(50 4 5 0 64)
      (getf (getf state :source-checkpoint) :resource-parameters)))))"
kill "$SRV"
wait "$SRV"
SRV=
start
check_control 100,8,10,0,64
[ "$(sql 'SELECT id FROM resource_crash_witness')" = 2 ]
echo 'resource-parameters-crash: PASS (flush/control SIGKILL, replay repair, decrease, checkpoint backup and restart)'
