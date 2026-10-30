#!/bin/sh
# Complete native WAL records survive receiver disconnect/stop below batch size.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
SBCL=${SBCL:-sbcl}
PSQL=${PSQL:-psql}
PORT=${PORT:-5926}
TARGET_PORT=$((PORT + 1))
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-physical-exit-flush.XXXXXX")
SOURCE= TARGET= STOP=
sql() {
    PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$1" -U postgres -d postgres \
        -qAt -v ON_ERROR_STOP=1 -v VERBOSITY=verbose -c "$2"
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
    for phase in disconnect stop failure poisoned; do : >"$TMP/$phase.release"; done
    if [ -n "$STOP" ]; then wait "$STOP" 2>/dev/null || :; fi
    if [ "$status" -ne 0 ] && [ -S "$TMP/target.sock" ]; then
        control target '(list :follow *physical-follow-runtime* :receive (getf *physical-recovery-state* :receive-lsn) :failed *physical-recovery-failed-p*)' >"$TMP/failure-state.log" 2>&1 || :
    fi
    for pid in "$TARGET" "$SOURCE"; do
        if [ -n "$pid" ]; then kill -INT "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; fi
    done
    echo "physical-exit-flush: artifacts=$TMP"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
start_source() {
    rm -f "$TMP/source.sock"
    "$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/source" --control-socket "$TMP/source.sock" >>"$TMP/source.log" 2>&1 &
    SOURCE=$!
    test_wait_owned_sql_server "$SOURCE" "$PORT" "$PSQL"
}
start_target() {
    rm -f "$TMP/target.sock"
    "$BASE/pg-lisp.sh" -p "$TARGET_PORT" -D "$TMP/copy" --control-socket "$TMP/target.sock" >>"$TMP/target.log" 2>&1 &
    TARGET=$!
    test_wait_owned_sql_server "$TARGET" "$TARGET_PORT" "$PSQL"
}
wait_target() {
    attempt=0
    while [ "$(sql "$TARGET_PORT" "$1")" != "$2" ]; do
        attempt=$((attempt + 1)); test "$attempt" -lt 200
        kill -0 "$TARGET"; sleep 0.05
    done
}
wait_file() {
    attempt=0
    while [ ! -f "$1" ]; do
        attempt=$((attempt + 1)); test "$attempt" -lt 100
        kill -0 "$TARGET"; sleep 0.02
    done
}
follow() {
    checked target "(physical-follow-start \"127.0.0.1\" $PORT \"exit_flush_base\")" "$1"
}
arm_pending_write() {
    phase=$1
    extra=
    if [ "$phase" = failure ]; then
        extra='(setf *physical-store-test-hook* (lambda (point store) (declare (ignore store)) (when (eq point :before-control-fsync) (setf *physical-store-test-hook* nil) (error "exit control fsync failpoint"))))'
    elif [ "$phase" = poisoned ]; then
        extra='(with-physical-store (*physical-receive-store*) (error "receiver transport poisoned store failpoint"))'
    elif [ "$phase" = stop ]; then
        # Emulate an active frame compiled before exit flushing was installed.
        # Restore the helper before stop's post-join fallback calls it.
        extra="(let ((original (symbol-function 'physical-follow-flush-on-exit))) (setf (symbol-function 'physical-follow-flush-on-exit) (lambda (runtime) (declare (ignore runtime)) (setf (symbol-function 'physical-follow-flush-on-exit) original) (with-open-file (out \"$TMP/old-frame-cleanup\" :direction :output) (write-line \"old frame skipped exit flush\" out)) :old-frame)))"
    fi
    # This hook runs outside operation admission, before feedback, with no store
    # or lifecycle lock. A bounded wait lets another control client request stop.
    checked target "(setf *physical-receive-flush-bytes* 67108864 *physical-stream-test-hook* (lambda (point) (when (and (eq point :before-feedback) (< (physical-store-flushed-lsn *physical-receive-store*) (physical-store-written-lsn *physical-receive-store*))) (setf *physical-stream-test-hook* nil) (with-open-file (out \"$TMP/$phase.end\" :direction :output) (write-line (wal-format-lsn (physical-store-written-lsn *physical-receive-store*)) out)) (loop repeat 160 until (probe-file \"$TMP/$phase.release\") do (sleep 0.05)) (unless (probe-file \"$TMP/$phase.release\") (error \"exit flush barrier timed out\")) $extra)))" "$phase-hook"
}
restart_target_without_follow() {
    kill -KILL "$TARGET"; wait "$TARGET" 2>/dev/null || :; TARGET=
    # A failed stop retains its saved follow configuration. Disable auto-follow
    # while offline so restart checks observe only the previously durable spool.
    if [ -n "${1:-}" ]; then
        test -e "$TMP/copy/physical-follow.control"
        mv "$TMP/copy/physical-follow.control" "$TMP/$1-follow-before-restart.control"
    fi
    start_target
    test ! -e "$TMP/copy/physical-follow.control"
}
request_stop() {
    control target '(physical-follow-stop)' >"$TMP/$1-stop.log" &
    STOP=$!
    wait_target "SELECT status FROM pg_stat_get_wal_receiver()" stopping
}
finish_stop() {
    : >"$TMP/$1.release"
    wait "$STOP"; STOP=
}

mkdir -m 700 "$TMP/copy"
start_source
sql "$PORT" 'CREATE TABLE exit_data (id integer PRIMARY KEY); INSERT INTO exit_data VALUES (1)'
checked source "(physical-base-backup \"$TMP/copy/\" \"exit_flush_base\")" backup
start_target
follow initial-follow
wait_target "SELECT status FROM pg_stat_get_wal_receiver()" streaming
sql "$TARGET_PORT" 'SELECT pg_wal_replay_pause()' >/dev/null

# A real network loss: stop the source while a complete, unflushed record waits
# for feedback. The receiver must flush before every reconnect resets its decoder.
arm_pending_write disconnect
sql "$PORT" 'INSERT INTO exit_data VALUES (2)'
wait_file "$TMP/disconnect.end"
IFS= read -r disconnected_end <"$TMP/disconnect.end"
kill -KILL "$SOURCE"; wait "$SOURCE" 2>/dev/null || :; SOURCE=
: >"$TMP/disconnect.release"
wait_target "SELECT pg_last_wal_receive_lsn() = '$disconnected_end'::pg_lsn" t
checked target '(physical-follow-stop)' disconnect-stop
restart_target_without_follow
checked target "(assert (= (physical-store-flushed-lsn *physical-receive-store*) (parse-replication-lsn \"$disconnected_end\")))" disconnect-reopened
start_source
follow disconnect-resume
wait_target 'SELECT count(*) FROM exit_data' 2

# Explicit stop must join without holding admission, then preserve its final
# complete record across SIGKILL/restart without applying an incomplete batch.
sql "$TARGET_PORT" 'SELECT pg_wal_replay_pause()' >/dev/null
arm_pending_write stop
sql "$PORT" 'INSERT INTO exit_data VALUES (3)'
wait_file "$TMP/stop.end"
IFS= read -r stopped_end <"$TMP/stop.end"
request_stop stop
finish_stop stop
test -f "$TMP/old-frame-cleanup"
test "$(sql "$TARGET_PORT" "SELECT pg_last_wal_receive_lsn() = '$stopped_end'::pg_lsn")" = t
test "$(sql "$TARGET_PORT" 'SELECT count(*) FROM exit_data')" = 2
if grep -q ';; error:' "$TMP/stop-stop.log"; then cat "$TMP/stop-stop.log" >&2; exit 1; fi
restart_target_without_follow
checked target "(assert (= (physical-store-flushed-lsn *physical-receive-store*) (parse-replication-lsn \"$stopped_end\")))" stop-reopened
follow stop-resume
wait_target 'SELECT count(*) FROM exit_data' 3

# A failed receive-control fsync during exit must not acknowledge the write or
# report a clean stop. The old durable manifest must remain usable on restart.
sql "$TARGET_PORT" 'SELECT pg_wal_replay_pause()' >/dev/null
baseline=$(sql "$TARGET_PORT" 'SELECT pg_last_wal_receive_lsn()')
arm_pending_write failure
sql "$PORT" 'INSERT INTO exit_data VALUES (4)'
wait_file "$TMP/failure.end"
request_stop failure
finish_stop failure
grep -q ';; error:' "$TMP/failure-stop.log"
checked target "(assert (and *physical-recovery-failed-p* (physical-store-failed-p *physical-receive-store*) (physical-follow-runtime-stopped-p *physical-follow-runtime*) (null (physical-follow-runtime-client *physical-follow-runtime*)) (null (physical-follow-statistics)) (search \"exit control fsync failpoint\" (physical-follow-runtime-last-error *physical-follow-runtime*)) (= (physical-store-flushed-lsn *physical-receive-store*) (parse-replication-lsn \"$baseline\")) (= (getf *physical-recovery-state* :receive-lsn) (parse-replication-lsn \"$baseline\"))))" failure-latched
if sql "$TARGET_PORT" 'SELECT 1' >"$TMP/failed-sql.log" 2>&1; then exit 1; fi
grep -q 57P03 "$TMP/failed-sql.log"
restart_target_without_follow failure
checked target "(assert (= (physical-store-flushed-lsn *physical-receive-store*) (parse-replication-lsn \"$baseline\")))" failure-reopened
test "$(sql "$TARGET_PORT" 'SELECT count(*) FROM exit_data')" = 3
follow failure-resume
wait_target 'SELECT count(*) FROM exit_data' 4

# A fault before cleanup has already poisoned the store. Stop the maintenance
# worker at its next iteration boundary so it cannot close SQL admission for us.
# Replay is paused too; only receiver cleanup can latch the recovery failure.
sql "$TARGET_PORT" 'SELECT pg_wal_replay_pause()' >/dev/null
checked target '(let ((original (symbol-function (quote physical-restartpoint-worker-step)))) (setf (symbol-function (quote physical-restartpoint-worker-step)) (lambda () (setf (symbol-function (quote physical-restartpoint-worker-step)) original) (sb-thread:return-from-thread :test-stopped))))' poisoned-stop-maintenance
checked target '(loop repeat 100 while (sb-thread:thread-alive-p *physical-restartpoint-worker*) do (sleep 0.02) finally (assert (not (sb-thread:thread-alive-p *physical-restartpoint-worker*))))' poisoned-maintenance-stopped
baseline=$(sql "$TARGET_PORT" 'SELECT pg_last_wal_receive_lsn()')
arm_pending_write poisoned
sql "$PORT" 'INSERT INTO exit_data VALUES (5)'
wait_file "$TMP/poisoned.end"
checked target '(assert (not *physical-recovery-failed-p*))' poisoned-before-cleanup
: >"$TMP/poisoned.release"
checked target '(loop repeat 100 until (physical-follow-runtime-stopped-p *physical-follow-runtime*) do (sleep 0.02) finally (assert (physical-follow-runtime-stopped-p *physical-follow-runtime*)))' poisoned-receiver-stopped
if sql "$TARGET_PORT" 'SELECT 1' >"$TMP/poisoned-sql.log" 2>&1; then
    echo 'poisoned store admitted SELECT after receiver cleanup' >&2; exit 1
fi
grep -q 57P03 "$TMP/poisoned-sql.log"
checked target "(assert (and *physical-recovery-failed-p* (physical-store-failed-p *physical-receive-store*) (null (physical-follow-runtime-client *physical-follow-runtime*)) (search \"receiver transport poisoned store failpoint\" (physical-follow-runtime-last-error *physical-follow-runtime*)) (= (physical-store-flushed-lsn *physical-receive-store*) (parse-replication-lsn \"$baseline\"))))" poisoned-latched
restart_target_without_follow poisoned
test "$(sql "$TARGET_PORT" 'SELECT count(*) FROM exit_data')" = 4
follow poisoned-resume
wait_target 'SELECT count(*) FROM exit_data' 5
checked target '(physical-follow-stop)' final-stop
checked source '(physical-source-pin-release "exit_flush_base")' release
echo 'physical-exit-flush: disconnect/stop durability, restart, exit-fsync and poisoned-store failure PASS'
