#!/bin/sh
# Explicit standby shutdown preparation; postmaster signal integration is pending.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
SBCL=${SBCL:-sbcl}
PSQL=${PSQL:-psql}
PORT=${PORT:-5966}
TARGET_PORT=$((PORT + 1))
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-physical-shutdown.XXXXXX")
SOURCE= TARGET= PREP=
sql() {
    PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$1" -U postgres -d postgres \
        -qAt -v ON_ERROR_STOP=1 -v VERBOSITY=verbose -c "$2"
}
control() {
    printf '%s\n(quit)\n' "$2" | timeout 20 "$SBCL" --script "$BASE/t/control-client.lisp" unix "$TMP/$1.sock"
}
checked() {
    control "$1" "$2" >"$TMP/$3.log"
    if grep -q ';; error:' "$TMP/$3.log"; then cat "$TMP/$3.log" >&2; exit 1; fi
}
rejected() {
    control target "$1" >"$TMP/$2.log"
    grep -q ';; error: 55000' "$TMP/$2.log"
}
cleanup() {
    status=$?
    trap - EXIT
    : >"$TMP/maintenance-release"
    if [ -n "$PREP" ]; then wait "$PREP" 2>/dev/null || :; fi
    if [ "$status" -ne 0 ] && [ -S "$TMP/target.sock" ]; then
        control target '(list :phase *physical-shutdown-state* :follow *physical-follow-runtime* :worker *physical-restartpoint-worker* :state *physical-recovery-state*)' >"$TMP/failure-state.log" 2>&1 || :
    fi
    for pid in "$TARGET" "$SOURCE"; do
        if [ -n "$pid" ]; then kill -INT "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; fi
    done
    echo "physical-shutdown-prepare: artifacts=$TMP"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
start_target() {
    rm -f "$TMP/target.sock"
    "$BASE/pg-lisp.sh" -p "$TARGET_PORT" -D "$TMP/copy" --control-socket "$TMP/target.sock" >>"$TMP/target.log" 2>&1 &
    TARGET=$!
    test_wait_owned_sql_server "$TARGET" "$TARGET_PORT" "$PSQL"
}
restart_target() {
    kill -KILL "$TARGET"; wait "$TARGET" 2>/dev/null || :; TARGET=
    start_target
}
wait_target() {
    attempt=0
    while [ "$(sql "$TARGET_PORT" "$1")" != "$2" ]; do
        attempt=$((attempt + 1)); test "$attempt" -lt 200
        kill -0 "$TARGET"; sleep 0.05
    done
}
follow() {
    checked target "(physical-follow-start \"127.0.0.1\" $PORT \"shutdown_base\")" "$1"
}
mkdir -m 700 "$TMP/copy"
"$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/source" --control-socket "$TMP/source.sock" >"$TMP/source.log" 2>&1 &
SOURCE=$!
test_wait_owned_sql_server "$SOURCE" "$PORT" "$PSQL"
sql "$PORT" 'CREATE TABLE shutdown_data (id integer PRIMARY KEY); INSERT INTO shutdown_data VALUES (1)'
checked source "(physical-base-backup \"$TMP/copy/\" \"shutdown_base\")" backup
start_target
checked target '(physical-recovery-prepare-shutdown)' immutable-prepare
checked target '(assert (and (eq *physical-shutdown-state* :prepared) (null *physical-receive-store*)))' immutable-state
restart_target
follow initial-follow
checked target "(let ((original (symbol-function 'physical-recovery-prepare-shutdown))) (setf (symbol-function 'physical-recovery-prepare-shutdown) (lambda () (setf (symbol-function 'physical-recovery-prepare-shutdown) original) (with-open-file (out \"$TMP/preparation-entered\" :direction :output) (write-line \"entered\" out)) (funcall original))))" preparation-entry-hook
# Arm before replay advances, so the control client disconnects before the
# maintenance worker closes admission. Its hook holds the actual publication
# gate/store/reload locks until the shell releases it.
checked target "(setf *physical-restartpoint-timeout* 0 *physical-restartpoint-test-hook* (lambda (point) (when (eq point :after-generation) (setf *physical-restartpoint-test-hook* nil) (with-open-file (out \"$TMP/maintenance-generation\" :direction :output) (write-line \"publishing\" out)) (loop repeat 160 until (probe-file \"$TMP/maintenance-release\") do (sleep 0.05)) (unless (probe-file \"$TMP/maintenance-release\") (error \"maintenance publication barrier timed out\")))))" maintenance-hook
sql "$PORT" 'INSERT INTO shutdown_data VALUES (2)'
attempt=0
while [ ! -f "$TMP/maintenance-generation" ]; do
    attempt=$((attempt + 1)); test "$attempt" -lt 200
    kill -0 "$TARGET"; sleep 0.02
done
control target '(physical-recovery-prepare-shutdown)' >"$TMP/maintenance-prepare.log" &
PREP=$!
attempt=0
while [ ! -f "$TMP/preparation-entered" ]; do
    attempt=$((attempt + 1)); test "$attempt" -lt 100
    kill -0 "$TARGET"; sleep 0.02
done
: >"$TMP/maintenance-release"
wait "$PREP"; PREP=
if grep -q ';; error:' "$TMP/maintenance-prepare.log"; then cat "$TMP/maintenance-prepare.log" >&2; exit 1; fi
checked target '(assert (and (eq *physical-shutdown-state* :prepared) (null *physical-restartpoint-worker*) (null *physical-follow-runtime*)))' maintenance-joined
restart_target
wait_target 'SELECT count(*) FROM shutdown_data' 2

# Paused replay must checkpoint A and preserve the received-ahead suffix F>A.
sql "$TARGET_PORT" 'SELECT pg_wal_replay_pause()' >/dev/null
applied=$(sql "$TARGET_PORT" 'SELECT pg_last_wal_replay_lsn()')
sql "$PORT" 'INSERT INTO shutdown_data VALUES (3)'
source_flush=$(sql "$PORT" 'SELECT pg_current_wal_flush_lsn()')
wait_target "SELECT pg_last_wal_receive_lsn() >= '$source_flush'::pg_lsn" t
cp "$TMP/copy/physical-follow.control" "$TMP/saved-follow.control"
checked target '(physical-recovery-prepare-shutdown)' paused-prepare
cmp "$TMP/saved-follow.control" "$TMP/copy/physical-follow.control"
checked target "(assert (and (eq *physical-shutdown-state* :prepared) (null *physical-follow-runtime*) (null *physical-restartpoint-worker*) *physical-replay-paused-p* (= *last-replay-lsn* (parse-replication-lsn \"$applied\")) (= (physical-store-floor-lsn *physical-receive-store*) *last-replay-lsn*) (> (physical-store-flushed-lsn *physical-receive-store*) *last-replay-lsn*) (= (getf (physical-restartpoint-read *physical-recovery-state*) :lsn) *last-replay-lsn*)))" paused-state
test "$(sql "$TARGET_PORT" 'SELECT count(*) FROM shutdown_data')" = 2
rejected '(physical-recovery-replay)' reject-replay
rejected '(physical-recovery-enable-streaming)' reject-enable
rejected '(physical-follow-stop)' reject-disable
rejected '(physical-recovery-receive 0 #())' reject-receive
rejected '(physical-recovery-flush)' reject-flush
rejected '(physical-restartpoint-start-worker)' reject-maintenance
rejected "(physical-follow-start \"127.0.0.1\" $PORT \"shutdown_base\")" reject-follow
if sql "$TARGET_PORT" CHECKPOINT >"$TMP/reject-checkpoint.log" 2>&1; then exit 1; fi
grep -q 55000 "$TMP/reject-checkpoint.log"
if sql "$TARGET_PORT" 'SELECT pg_wal_replay_resume()' >"$TMP/reject-resume.log" 2>&1; then exit 1; fi
grep -q 55000 "$TMP/reject-resume.log"
test "$(sql "$TARGET_PORT" 'SELECT pg_is_wal_replay_paused()')" = t
checked target '(physical-recovery-prepare-shutdown)' idempotent-prepare
restart_target
wait_target 'SELECT count(*) FROM shutdown_data' 3
wait_target 'SELECT status FROM pg_stat_get_wal_receiver()' streaming

# Receive one genuine source record manually, with no follow runtime. It remains
# below the flush threshold and may leave the source's batch incomplete.
checked target '(physical-follow-stop)' detach
sql "$PORT" 'INSERT INTO shutdown_data VALUES (4)'
checked target "(let* ((runtime (make-physical-follow-runtime :host \"127.0.0.1\" :port $PORT :pin \"shutdown_base\")) (cursor (physical-store-flushed-lsn *physical-receive-store*)) (client nil)) (unwind-protect (progn (setf *physical-receive-flush-bytes* 67108864 client (sb-ext:with-timeout 5 (pg-client-connect \"127.0.0.1\" $PORT \"postgres\" :replication-p t))) (physical-follow-check-identity (sb-ext:with-timeout 5 (pg-client-query client \"IDENTIFY_SYSTEM\"))) (let ((query (physical-follow-start-query runtime cursor))) (sb-ext:with-timeout 5 (pg-client-start-replication client query))) (loop repeat 10 do (multiple-value-bind (type payload) (sb-ext:with-timeout 5 (pg-client-replication-frame client)) (when (char= type #\\E) (error (pg-client-error-from-payload payload))) (assert (char= type #\\d)) (physical-follow-process-frame payload) (when (= (aref payload 0) (char-code #\\w)) (return)) (physical-follow-feedback client))) (assert (> (physical-store-written-lsn *physical-receive-store*) (physical-store-flushed-lsn *physical-receive-store*)))) (pg-client-close client)))" direct-receive
checked target '(physical-recovery-prepare-shutdown)' direct-prepare
checked target '(assert (and (null *physical-follow-runtime*) (= (physical-store-written-lsn *physical-receive-store*) (physical-store-flushed-lsn *physical-receive-store*)) (> (physical-store-flushed-lsn *physical-receive-store*) *last-replay-lsn*)))' direct-state
test ! -e "$TMP/copy/physical-follow.control"
restart_target
test "$(sql "$TARGET_PORT" 'SELECT count(*) FROM shutdown_data')" = 3
follow direct-resume
wait_target 'SELECT count(*) FROM shutdown_data' 4

# Publication failure must not become a prepared shutdown or clear follow config.
sql "$TARGET_PORT" 'SELECT pg_wal_replay_pause()' >/dev/null
sql "$PORT" 'INSERT INTO shutdown_data VALUES (5)'
source_flush=$(sql "$PORT" 'SELECT pg_current_wal_flush_lsn()')
wait_target "SELECT pg_last_wal_receive_lsn() >= '$source_flush'::pg_lsn" t
cp "$TMP/copy/physical-follow.control" "$TMP/failure-follow.control"
checked target '(setf *physical-restartpoint-test-hook* (lambda (point) (when (eq point :after-generation) (error "shutdown publication failpoint"))))' publication-hook
control target '(physical-recovery-prepare-shutdown)' >"$TMP/failed-prepare.log"
grep -q ';; error:' "$TMP/failed-prepare.log"
checked target '(assert (and (eq *physical-shutdown-state* :failed) *physical-recovery-failed-p* (null *physical-follow-runtime*) (null *physical-restartpoint-worker*)))' failed-state
cmp "$TMP/failure-follow.control" "$TMP/copy/physical-follow.control"
if sql "$TARGET_PORT" 'SELECT 1' >"$TMP/failed-sql.log" 2>&1; then exit 1; fi
grep -q 57P03 "$TMP/failed-sql.log"
restart_target
wait_target 'SELECT count(*) FROM shutdown_data' 5
wait_target 'SELECT status FROM pg_stat_get_wal_receiver()' streaming
checked target '(physical-recovery-prepare-shutdown)' final-prepare
checked source '(physical-source-pin-release "shutdown_base")' release
echo 'physical-shutdown-prepare: paused/direct receive, worker joins, restart/autofollow, and publication failure PASS'
