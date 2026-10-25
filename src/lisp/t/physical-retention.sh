#!/bin/sh
# Flush advances source retention even while standby replay is delayed.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
SBCL=${SBCL:-sbcl}
PSQL=${PSQL:-psql}
PORT=${PORT:-5880}
TARGET_PORT=$((PORT + 1))
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-physical-retention.XXXXXX")
SOURCE= TARGET= TX=
sql() {
    port=$1; shift
    PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$port" -U postgres -d postgres \
        -qAt -v ON_ERROR_STOP=1 "$@"
}
control() {
    printf '%s\n(quit)\n' "$2" | "$SBCL" --script "$BASE/t/control-client.lisp" unix "$TMP/$1.sock"
}
checked() {
    control "$1" "$2" >"$TMP/$3.log"
    if grep -q ';; error:' "$TMP/$3.log"; then cat "$TMP/$3.log" >&2; exit 1; fi
}
cleanup() {
    status=$?
    trap - EXIT
    if [ "$status" -ne 0 ] && [ -S "$TMP/target.sock" ]; then
        control target '(list :follow *physical-follow-runtime* :apply *last-replay-lsn* :receive (getf *physical-recovery-state* :receive-lsn))' >"$TMP/failure-state.log" 2>&1 || :
    fi
    for pid in "$TX" "$TARGET" "$SOURCE"; do
        if [ -n "$pid" ]; then kill -INT "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; fi
    done
    echo "physical-retention: artifacts=$TMP"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
start_source() {
    rm -f "$TMP/source.sock"
    "$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/source" \
        --control-socket "$TMP/source.sock" >>"$TMP/source.log" 2>&1 &
    SOURCE=$!
    test_wait_owned_sql_server "$SOURCE" "$PORT" "$PSQL"
}
start_target() {
    rm -f "$TMP/target.sock"
    "$BASE/pg-lisp.sh" -p "$TARGET_PORT" -D "$TMP/copy" \
        --control-socket "$TMP/target.sock" >>"$TMP/target.log" 2>&1 &
    TARGET=$!
    # Before local R publication this fixture has over 30 MiB to replay;
    # later restarts must also work after its original receive segment is gone.
    test_wait_owned_sql_server "$TARGET" "$TARGET_PORT" "$PSQL" 1500
}
wait_target() {
    attempt=0
    while :; do
        kill -0 "$TARGET" 2>/dev/null || return 1
        case $(ps -o stat= -p "$TARGET" 2>/dev/null || :) in Z*) return 1;; esac
        if actual=$(sql "$TARGET_PORT" -c "$1" 2>>"$TMP/poll-errors.log"); then
            if [ "$actual" = "$2" ]; then return 0; fi
        fi
        attempt=$((attempt + 1)); test "$attempt" -lt 1000; sleep 0.1
    done
}
mkdir -m 700 "$TMP/copy"
start_source
sql "$PORT" -c "CREATE TABLE data (id integer PRIMARY KEY,payload varchar(120)); INSERT INTO data VALUES (-1,'base')"
checked source "(physical-base-backup \"$TMP/copy/\" \"advance\")" backup
checked source '(format t "OLDSEG=~d~%" (floor (physical-source-pin-restart-lsn (gethash "advance" *physical-source-pins*)) +wal-segment-size+))' old-segment
old_segment=$(awk -F= '/OLDSEG=/{print $2}' "$TMP/old-segment.log")
start_target
checked target "(physical-follow-start \"127.0.0.1\" $PORT \"advance\")" follow
mkfifo "$TMP/tx.fifo"
PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$TARGET_PORT" -U postgres -d postgres \
    -qAt -v ON_ERROR_STOP=1 <"$TMP/tx.fifo" >"$TMP/tx.log" 2>&1 &
TX=$!
exec 7>"$TMP/tx.fifo"
printf 'BEGIN ISOLATION LEVEL REPEATABLE READ;\nSELECT count(*) FROM data;\n' >&7
attempt=0
while ! grep -q '^1$' "$TMP/tx.log"; do
    attempt=$((attempt + 1)); test "$attempt" -lt 100; sleep 0.1
done
printf 'SELECT 12345;\n' >&7
attempt=0
while ! grep -q '^12345$' "$TMP/tx.log"; do
    attempt=$((attempt + 1)); test "$attempt" -lt 100; sleep 0.1
done
replay_before=$(sql "$TARGET_PORT" -c 'SELECT pg_last_wal_replay_lsn()')
# A due automatic restartpoint must use applied progress, not receive flush.
checked target '(setf *physical-restartpoint-timeout* 300 *physical-restartpoint-wal-bytes* 1)' automatic-byte-trigger
python3 - "$TMP/writes.sql" <<'PY'
import sys
with open(sys.argv[1], "w") as stream:
    for index in range(1800):
        stream.write("INSERT INTO data VALUES (%d,'%s');\n" % (index, "x" * 100))
PY
sql "$PORT" -f "$TMP/writes.sql" >"$TMP/writes.log"
source_flush=$(sql "$PORT" -c 'SELECT pg_current_wal_flush_lsn()')
wait_target "SELECT pg_last_wal_receive_lsn() >= '$source_flush'::pg_lsn" t
test "$(sql "$TARGET_PORT" -c 'SELECT pg_last_wal_replay_lsn()')" = "$replay_before"
# Wait for the source to durably consume the receiver's feedback too.
attempt=0
while :; do
    control source "(assert (>= (or (physical-source-pin-confirmed-lsn (gethash \"advance\" *physical-source-pins*)) 0) (parse-replication-lsn \"$source_flush\")))" >"$TMP/confirmed.log"
    if ! grep -q ';; error:' "$TMP/confirmed.log"; then break; fi
    attempt=$((attempt + 1)); test "$attempt" -lt 100; sleep 0.1
done
sql "$PORT" -c CHECKPOINT
checked source "(progn (assert (> (floor (physical-source-pin-restart-lsn (gethash \"advance\" *physical-source-pins*)) +wal-segment-size+) $old_segment)) (assert (not (probe-file (wal-segment-path $old_segment)))))" recycled
# Crash after the new confirmed marker is durable but before its live floor
# changes. The writer can lose its reply; its WAL must already be durable.
checked source '(let ((floor *wal-durable-lsn*)) (setf *physical-source-test-hook* (lambda (point pin) (when (and (eq point :after-save) (not (physical-source-pin-lost-p pin)) (physical-source-pin-confirmed-lsn pin) (> (physical-source-pin-confirmed-lsn pin) floor)) (sb-posix:kill (sb-posix:getpid) sb-posix:sigkill)))))' confirm-crash-hook
sql "$PORT" -c "INSERT INTO data VALUES (-2,'confirmed-crash')" >"$TMP/confirm-writer.log" 2>&1 || :
crash_status=0
wait "$SOURCE" 2>/dev/null || crash_status=$?
test "$crash_status" -eq 137
SOURCE=
start_source
checked source "(assert (not (probe-file (wal-segment-path $old_segment))))" source-restarted
sql "$PORT" -c 'SELECT id,payload FROM data ORDER BY id' >"$TMP/expected.rows"
printf 'COMMIT;\n\\q\n' >&7
exec 7>&-
wait "$TX"
TX=
wait_target 'SELECT count(*) FROM data' 1802
sql "$TARGET_PORT" -c 'SELECT id,payload FROM data ORDER BY id' >"$TMP/actual.rows"
diff -u "$TMP/expected.rows" "$TMP/actual.rows"
# The maintenance worker publishes local R after apply catches up; S is independent.
attempt=0
while :; do
    control target "(progn (assert (> (floor (physical-store-floor-lsn *physical-receive-store*) +wal-segment-size+) $old_segment)) (assert (not (probe-file (physical-store-segment-path *physical-receive-store* $old_segment)))))" >"$TMP/local-recycled.log"
    if ! grep -q ';; error:' "$TMP/local-recycled.log"; then break; fi
    attempt=$((attempt + 1)); test "$attempt" -lt 200; sleep 0.1
done
checked target '(setf *physical-restartpoint-wal-bytes* (* 1024 1024 1024))' reset-byte-trigger
checked target '(reload)' restartpoint-reload
kill -KILL "$TARGET"
wait "$TARGET" 2>/dev/null || :
TARGET=
start_target
wait_target 'SELECT count(*) FROM data' 1802
sql "$TARGET_PORT" -c 'SELECT id,payload FROM data ORDER BY id' >"$TMP/restarted.rows"
diff -u "$TMP/expected.rows" "$TMP/restarted.rows"
# A capped, disconnected consumer becomes durably lost rather than advancing
# past data it has never flushed. It must not prevent primary startup.
checked target '(physical-follow-stop)' stop
checked source '(setf *max-slot-wal-keep-bytes* 0)' cap
checked source '(setf *physical-source-test-hook* (lambda (point pin) (when (and (eq point :after-save) (physical-source-pin-lost-p pin)) (sb-posix:kill (sb-posix:getpid) sb-posix:sigkill))))' lost-crash-hook
sql "$PORT" -c "INSERT INTO data VALUES (9999,'unreceived'); CHECKPOINT" >"$TMP/lost-writer.log" 2>&1 || :
crash_status=0
wait "$SOURCE" 2>/dev/null || crash_status=$?
test "$crash_status" -eq 137
SOURCE=
start_source
test "$(sql "$PORT" -c 'SELECT count(*) FROM data')" = 1803
checked source '(assert (physical-source-pin-lost-p (gethash "advance" *physical-source-pins*)))' lost-restarted
checked target "(physical-follow-start \"127.0.0.1\" $PORT \"advance\")" lost-follow
attempt=0
while ! grep -q 'retention is lost; reseed is required' "$TMP/target.log"; do
    attempt=$((attempt + 1)); test "$attempt" -lt 200; sleep 0.1
done
test "$(sql "$TARGET_PORT" -c 'SELECT count(*) FROM data WHERE id=9999')" = 0
checked target '(physical-follow-stop)' lost-stop
echo 'physical-retention: PASS'
