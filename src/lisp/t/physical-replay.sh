#!/bin/sh
# Actual standby redo using manually transported native frames, not network streaming.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
SBCL=${SBCL:-sbcl}
PSQL=${PSQL:-psql}
PORT=${PORT:-5874}
TARGET_PORT=$((PORT + 1))
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-physical-replay.XXXXXX")
SOURCE= TARGET= TX=
cleanup() {
    status=$?
    trap - EXIT
    for pid in "$TX" "$TARGET" "$SOURCE"; do
        if [ -n "$pid" ]; then kill -INT "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; fi
    done
    echo "physical-replay: artifacts=$TMP"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
sql() {
    PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$1" -U postgres -d postgres \
        -qAt -v ON_ERROR_STOP=1 -c "$2"
}
control() {
    printf '%s\n(quit)\n' "$2" | "$SBCL" --script "$BASE/t/control-client.lisp" unix "$TMP/$1.sock"
}
checked() {
    control "$1" "$2" >"$TMP/$3.log"
    if grep -q ';; error:' "$TMP/$3.log"; then cat "$TMP/$3.log" >&2; exit 1; fi
}
start_target() {
    rm -f "$TMP/target.sock"
    "$BASE/pg-lisp.sh" -p "$TARGET_PORT" -D "$TMP/copy" \
        --control-socket "$TMP/target.sock" >>"$TMP/target.log" 2>&1 &
    TARGET=$!
    test_wait_owned_sql_server "$TARGET" "$TARGET_PORT" "$PSQL"
    checked target "(load \"$BASE/t/physical-replay-native.lisp\")" target-hooks
}
compare() {
    sql "$PORT" 'SELECT id,label FROM data ORDER BY id' >"$TMP/source.rows"
    sql "$TARGET_PORT" 'SELECT id,label FROM data ORDER BY id' >"$TMP/target.rows"
    diff -u "$TMP/source.rows" "$TMP/target.rows"
    test "$(sql "$TARGET_PORT" 'SELECT pg_is_in_recovery()')" = t
}
export_wal() {
    cursor=$(sql "$TARGET_PORT" 'SELECT pg_last_wal_replay_lsn()')
    checked source "(physical-test-export-wal (parse-replication-lsn \"$cursor\") \"$TMP/frames.bin\")" export
}
receive_all() {
    checked target "(physical-test-receive-file \"$TMP/frames.bin\")" receive
}
mkdir -m 700 "$TMP/copy"
"$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/source" \
    --control-socket "$TMP/source.sock" >"$TMP/source.log" 2>&1 &
SOURCE=$!
test_wait_owned_sql_server "$SOURCE" "$PORT" "$PSQL"
sql "$PORT" "CREATE TABLE data (id integer PRIMARY KEY,label varchar(32)); INSERT INTO data VALUES (1,'base'); CREATE SEQUENCE replay_seq START WITH 7; CREATE TABLE base_empty (id integer); CREATE TABLE base_drop (id integer); INSERT INTO base_drop VALUES (10)"
# Multiple base heap pages ensure that later updates do not carry every base
# page in post-base WAL. Recreating a lost heap from those records would lose rows.
base_rows=$(python3 - <<'PY'
print(",".join(f"({i},'untouched')" for i in range(1000, 1200)))
PY
)
sql "$PORT" "INSERT INTO data VALUES $base_rows"
base_count=$(sql "$PORT" 'SELECT count(*) FROM data')
checked source "(load \"$BASE/t/physical-replay-native.lisp\")" source-hooks
checked source "(physical-base-backup \"$TMP/copy/\" \"redo_base\")" backup
start_target
checked target '(physical-recovery-enable-streaming)' enable
compare
# Primary SELECTs use real transaction IDs and may leave empty commit batches.
# Drain those before isolating the intentionally incomplete INSERT batch below.
export_wal
receive_all
checked target '(physical-recovery-replay)' drain-read-batches
before=$(sql "$TARGET_PORT" 'SELECT pg_last_wal_replay_lsn()')
sql "$PORT" "INSERT INTO data VALUES (2,'first')"
export_wal
checked target "(physical-test-receive-file \"$TMP/frames.bin\" :omit-last t)" partial
checked target '(physical-recovery-replay)' partial-replay
test "$(sql "$TARGET_PORT" 'SELECT pg_last_wal_replay_lsn()')" = "$before"
test "$(sql "$TARGET_PORT" 'SELECT count(*) FROM data')" = "$base_count"
PGCONNECT_TIMEOUT=2 "$PSQL" -X \
    -d "host=127.0.0.1 port=$TARGET_PORT user=postgres dbname=postgres replication=database" \
    -qAt -v ON_ERROR_STOP=1 -c IDENTIFY_SYSTEM >"$TMP/identify-received"
test "$(awk -F'|' '{print $3}' "$TMP/identify-received")" = "$(sql "$TARGET_PORT" 'SELECT pg_last_wal_receive_lsn()')"
checked target "(physical-test-receive-file \"$TMP/frames.bin\" :only-last t)" terminal
checked target '(physical-recovery-replay)' first-replay
compare
test "$(sql "$TARGET_PORT" 'SELECT pg_last_wal_receive_lsn()')" = "$(sql "$TARGET_PORT" 'SELECT pg_last_wal_replay_lsn()')"
checked target '(reload)' live-reload
grep -q 'reload complete' "$TMP/live-reload.log"
# Retained transactions delay replay without blocking the COMMIT needed to release them.
mkfifo "$TMP/tx.fifo"
PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$TARGET_PORT" -U postgres -d postgres \
    -qAt -v ON_ERROR_STOP=1 <"$TMP/tx.fifo" >"$TMP/tx.log" 2>&1 &
TX=$!
exec 7>"$TMP/tx.fifo"
printf 'BEGIN ISOLATION LEVEL REPEATABLE READ;\nSELECT count(*) FROM data;\n' >&7
attempt=0
while ! grep -q "^$((base_count + 1))$" "$TMP/tx.log"; do
    attempt=$((attempt + 1)); test "$attempt" -lt 100; sleep 0.1
done
sql "$PORT" "UPDATE data SET label='updated' WHERE id=1; INSERT INTO data VALUES (3,'next'); SELECT nextval('replay_seq'); CREATE TABLE extra (id integer); INSERT INTO extra VALUES (42); CREATE TABLE empty_after_base (id integer); DROP TABLE base_drop"
export_wal
receive_all
checked target '(physical-recovery-replay)' deferred
grep -q ':DEFERRED' "$TMP/deferred.log"
test "$(sql "$TARGET_PORT" 'SELECT count(*) FROM data')" = "$((base_count + 1))"
printf 'COMMIT;\n\\q\n' >&7
exec 7>&-
wait "$TX"
TX=
checked target '(physical-recovery-replay)' catchup
compare
test "$(sql "$TARGET_PORT" 'SELECT id FROM extra')" = 42
test "$(sql "$TARGET_PORT" "SELECT pg_lisp_sequence_state('replay_seq')")" = "$(sql "$PORT" "SELECT pg_lisp_sequence_state('replay_seq')")"
# Kill after the authoritative pointer is durable, before receive-floor publication.
checked target '(setf *physical-restartpoint-test-hook* (lambda (point) (when (eq point :after-pointer) (sb-posix:kill (sb-posix:getpid) sb-posix:sigkill))))' pointer-crash-hook
control target '(physical-recovery-restartpoint)' >"$TMP/pointer-crash.log" 2>&1 || :
crash_status=0
wait "$TARGET" 2>/dev/null || crash_status=$?
test "$crash_status" -eq 137
TARGET=
start_target
compare
checked target '(assert (< (physical-store-floor-lsn *physical-receive-store*) *last-replay-lsn*))' old-floor-new-pointer
checked target '(physical-recovery-restartpoint)' first-restartpoint
# Redo failure after mutating pages must recover from the full retained prefix.
sql "$PORT" "DELETE FROM data WHERE id=2; INSERT INTO data VALUES (4,'crash')"
export_wal
receive_all
checked target '(physical-test-crash-after-redo)' crash-hook
control target '(physical-recovery-replay)' >"$TMP/crash.log" 2>&1 || :
crash_status=0
wait "$TARGET" 2>/dev/null || crash_status=$?
test "$crash_status" -eq 137
TARGET=
start_target
compare
test "$(sql "$TARGET_PORT" 'SELECT id FROM extra')" = 42
test "$(sql "$TARGET_PORT" 'SELECT count(*) FROM base_empty')" = 0
test "$(sql "$TARGET_PORT" 'SELECT count(*) FROM empty_after_base')" = 0
# Kill with the new floor durable but before segment deletion/generation GC.
checked target '(setf *physical-store-test-hook* (lambda (point store) (declare (ignore store)) (when (eq point :after-floor-publication) (sb-posix:kill (sb-posix:getpid) sb-posix:sigkill))))' floor-crash-hook
control target '(physical-recovery-restartpoint)' >"$TMP/floor-crash.log" 2>&1 || :
crash_status=0
wait "$TARGET" 2>/dev/null || crash_status=$?
test "$crash_status" -eq 137
TARGET=
start_target
compare
checked target '(physical-recovery-restartpoint)' second-restartpoint
checked target '(assert (= 1 (length (directory (merge-pathnames "*.rp" (physical-restartpoint-directory))))))' generation-gc
# A Lisp error must not reopen SQL over partially applied state.
sql "$PORT" "INSERT INTO data VALUES (5,'error-restart')"
export_wal
receive_all
checked target '(physical-test-error-after-redo)' error-hook
control target '(physical-recovery-replay)' >"$TMP/error-replay.log"
grep -q 'injected redo failure' "$TMP/error-replay.log"
if PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$TARGET_PORT" -U postgres -d postgres \
    -qAt -v ON_ERROR_STOP=1 -v VERBOSITY=sqlstate -c 'SELECT * FROM data' >"$TMP/failed-sql.log" 2>&1; then exit 1; fi
grep -q 57P03 "$TMP/failed-sql.log"
kill -KILL "$TARGET"
wait "$TARGET" 2>/dev/null || :
TARGET=
start_target
compare
if sql "$TARGET_PORT" "INSERT INTO data VALUES (9,'forbidden')" >"$TMP/write.log" 2>&1; then exit 1; fi
grep -q 'during recovery' "$TMP/write.log"
checked target '(format t "BASEHEAP=~d~%" (pg-class-relfilenode (gethash "data" (database-tables (gethash "postgres" (catalog-databases *catalog*))))))' base-heap
base_heap=$(awk -F= '/BASEHEAP=/{print $2}' "$TMP/base-heap.log")
test -n "$base_heap"
kill -INT "$TARGET"
wait "$TARGET"
TARGET=
for damage in missing truncated catalog-hidden; do
    python3 - "$TMP" "$damage" "$base_heap" <<'PY'
from pathlib import Path
import shutil
import sys
root, damage, rel = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
image = root / ("damaged-" + damage)
shutil.copytree(root / "copy", image)
heap = image / "base" / rel
if damage != "truncated":
    heap.unlink()
else:
    with heap.open("r+b") as stream:
        stream.truncate(0)
PY
    if [ "$damage" = catalog-hidden ]; then
        checked source "(physical-test-hide-disk-table \"$TMP/damaged-$damage/\" \"data\")" hidden-catalog
    fi
    "$BASE/pg-lisp.sh" -p "$TARGET_PORT" -D "$TMP/damaged-$damage" >"$TMP/damaged-$damage.log" 2>&1 &
    TARGET=$!
    attempt=0
    while kill -0 "$TARGET" 2>/dev/null; do
        case $(ps -o stat= -p "$TARGET" 2>/dev/null || :) in Z*) break;; esac
        attempt=$((attempt + 1)); test "$attempt" -lt 100; sleep 0.1
    done
    if wait "$TARGET"; then TARGET=; exit 1; fi
    TARGET=
    grep -q 'still-live base relation fork' "$TMP/damaged-$damage.log"
done
echo 'physical-replay: PASS'
