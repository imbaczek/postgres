#!/bin/sh
# Native recovery shutdown from actual process signals, without coordination.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
SBCL=${SBCL:-sbcl}
PSQL=${PSQL:-psql}
PYTHON=${PYTHON:-python3}
PORT=${PORT:-5986}
TARGET_PORT=$((PORT + 1))
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-physical-shutdown-signals.XXXXXX")
SOURCE= TARGET= HELD= FD_OPEN=0 START=0 STAGE=startup
sql() {
    PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$1" -U postgres -d postgres \
        -qAt -v ON_ERROR_STOP=1 -c "$2"
}
control() {
    printf '%s\n(quit)\n' "$2" | timeout 20 "$SBCL" --script "$BASE/t/control-client.lisp" unix "$TMP/$1.sock"
}
checked() {
    control "$1" "$2" >"$TMP/$3.log"
    if grep -q ';; error:' "$TMP/$3.log"; then cat "$TMP/$3.log" >&2; exit 1; fi
}
control_state() {
    "$PYTHON" "$BASE/t/interop/recovery-control-state.py" \
        "$TMP/copy/physical-recovery.control" "$@"
}
snapshot_data() {
    "$PYTHON" - "$TMP/copy" <<'PY'
import hashlib, json, sys
from pathlib import Path
root = Path(sys.argv[1])
result = {}
for directory in ('base', 'global', 'pg_wal', 'pg_xact'):
    for path in sorted((root / directory).rglob('*')):
        if path.is_file():
            result[str(path.relative_to(root))] = hashlib.sha256(path.read_bytes()).hexdigest()
print(json.dumps(result, sort_keys=True))
PY
}
close_held() {
    if [ "$FD_OPEN" = 1 ]; then exec 7>&-; FD_OPEN=0; fi
    if [ -n "$HELD" ]; then wait "$HELD" 2>/dev/null || :; HELD=; fi
}
cleanup() {
    status=$?
    trap - EXIT
    if [ "$status" -ne 0 ]; then echo "physical-shutdown-signals: failed during $STAGE" >&2; fi
    close_held
    for pid in "$TARGET" "$SOURCE"; do
        if [ -n "$pid" ]; then
            kill -QUIT "$pid" 2>/dev/null || :
            attempt=0
            while kill -0 "$pid" 2>/dev/null && [ "$attempt" -lt 50 ]; do
                attempt=$((attempt + 1)); sleep 0.1
            done
            kill -KILL "$pid" 2>/dev/null || :
            wait "$pid" 2>/dev/null || :
        fi
    done
    echo "physical-shutdown-signals: artifacts=$TMP"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
start_target() {
    START=$((START + 1))
    rm -f "$TMP/target.sock"
    "$BASE/pg-lisp.sh" -p "$TARGET_PORT" -D "$TMP/copy" --control-socket "$TMP/target.sock" \
        >"$TMP/target-$START.log" 2>&1 &
    TARGET=$!
    test_wait_owned_sql_server "$TARGET" "$TARGET_PORT" "$PSQL"
    control_state active
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
        attempt=$((attempt + 1)); test "$attempt" -lt 200
        kill -0 "$TARGET"; sleep 0.02
    done
}
wait_exit() {
    expected=$1
    attempt=0
    while kill -0 "$TARGET" 2>/dev/null; do
        attempt=$((attempt + 1)); test "$attempt" -lt 100
        sleep 0.1
    done
    actual=0
    wait "$TARGET" || actual=$?
    TARGET=
    test "$actual" -eq "$expected"
}
hold_connection() {
    rm -f "$TMP/hold-in" "$TMP/hold-out"
    mkfifo "$TMP/hold-in"
    "$PSQL" -X -h 127.0.0.1 -p "$TARGET_PORT" -U postgres -d postgres -qAt \
        <"$TMP/hold-in" >"$TMP/hold-out" 2>&1 &
    HELD=$!
    exec 7>"$TMP/hold-in"; FD_OPEN=1
    printf 'BEGIN; SELECT pg_backend_pid();\n' >&7
    attempt=0
    until grep -q '^[0-9][0-9]*$' "$TMP/hold-out"; do
        attempt=$((attempt + 1)); test "$attempt" -lt 100
        sleep 0.05
    done
}
receive_ahead() {
    sql "$TARGET_PORT" 'SELECT pg_wal_replay_pause()' >/dev/null
    applied=$(sql "$TARGET_PORT" 'SELECT pg_last_wal_replay_lsn()')
    sql "$PORT" "INSERT INTO signal_data VALUES ($1)"
    flushed=$(sql "$PORT" 'SELECT pg_current_wal_flush_lsn()')
    wait_target "SELECT pg_last_wal_receive_lsn() >= '$flushed'::pg_lsn" t
}
verify_preparation() {
    checked target "(let ((original (symbol-function 'physical-recovery-prepare-shutdown))) (setf (symbol-function 'physical-recovery-prepare-shutdown) (lambda () (setf (symbol-function 'physical-recovery-prepare-shutdown) original) (assert (zerop *live-operation-depth*)) (assert (not *shutdown-finished*)) (prog1 (funcall original) (assert (and (eq *physical-shutdown-state* :prepared) (null *physical-follow-runtime*) (null *physical-restartpoint-worker*) *physical-replay-paused-p* (= *last-replay-lsn* (parse-replication-lsn \"$applied\")) (= (physical-store-floor-lsn *physical-receive-store*) *last-replay-lsn*) (> (physical-store-flushed-lsn *physical-receive-store*) *last-replay-lsn*) (= (getf (physical-restartpoint-read *physical-recovery-state*) :lsn) *last-replay-lsn*))) (with-open-file (out \"$TMP/$1\" :direction :output) (write-line \"prepared\" out))))))" "$1-hook"
}
mkdir -m 700 "$TMP/copy"
"$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/source" --control-socket "$TMP/source.sock" >"$TMP/source.log" 2>&1 &
SOURCE=$!
test_wait_owned_sql_server "$SOURCE" "$PORT" "$PSQL"
sql "$PORT" 'CREATE TABLE signal_data (id integer PRIMARY KEY); INSERT INTO signal_data VALUES (1)'
checked source "(physical-base-backup \"$TMP/copy/\" \"signals_base\")" backup
start_target
checked target "(physical-follow-start \"127.0.0.1\" $PORT \"signals_base\")" follow
sql "$PORT" 'INSERT INTO signal_data VALUES (2)'
wait_target 'SELECT count(*) FROM signal_data' 2

# Smart shutdown waits for the last admitted connection, then finalizes at A.
STAGE=smart
receive_ahead 3
cp "$TMP/copy/physical-follow.control" "$TMP/saved-follow.control"
verify_preparation smart-prepared
hold_connection
kill -TERM "$TARGET"
sleep 0.3
kill -0 "$TARGET"
test ! -f "$TMP/smart-prepared"
close_held
wait_exit 0
control_state clean --applied "$applied"
test -f "$TMP/smart-prepared"
cmp "$TMP/saved-follow.control" "$TMP/copy/physical-follow.control"
start_target
wait_target 'SELECT count(*) FROM signal_data' 3

# Fast shutdown terminates an idle read transaction and preserves F>A too.
STAGE=fast
receive_ahead 4
verify_preparation fast-prepared
hold_connection
kill -INT "$TARGET"
wait_exit 0
control_state clean --applied "$applied"
close_held
test -f "$TMP/fast-prepared"
cmp "$TMP/saved-follow.control" "$TMP/copy/physical-follow.control"
start_target
wait_target 'SELECT count(*) FROM signal_data' 4

# The signal worker must remain responsive while main-thread preparation waits.
STAGE=immediate-escalation
receive_ahead 5
checked target "(let ((original (symbol-function 'physical-recovery-prepare-shutdown))) (setf (symbol-function 'physical-recovery-prepare-shutdown) (lambda () (setf (symbol-function 'physical-recovery-prepare-shutdown) original) (with-open-file (out \"$TMP/preparation-entered\" :direction :output) (write-line \"entered\" out)) (sleep 30) (with-open-file (out \"$TMP/incorrect-preparation-completed\" :direction :output) (write-line \"wrong\" out)) (funcall original))))" block-preparation
kill -INT "$TARGET"
wait_file "$TMP/preparation-entered"
kill -QUIT "$TARGET"
wait_exit 0
control_state active --applied "$applied"
test ! -f "$TMP/incorrect-preparation-completed"
grep -q 'received immediate shutdown request' "$TMP/target-$START.log"
start_target
wait_target 'SELECT count(*) FROM signal_data' 5

# A final restartpoint failure must exit nonzero, never announce clean shutdown.
STAGE=publication-failure
receive_ahead 6
checked target '(setf *physical-restartpoint-test-hook* (lambda (point) (when (eq point :after-generation) (error "signal shutdown publication failpoint"))))' fail-publication
kill -INT "$TARGET"
wait_exit 1
control_state active --applied "$applied"
grep -q 'signal shutdown publication failpoint' "$TMP/target-$START.log"
if grep -q 'database system is shut down' "$TMP/target-$START.log"; then exit 1; fi
cmp "$TMP/saved-follow.control" "$TMP/copy/physical-follow.control"
start_target
wait_target 'SELECT count(*) FROM signal_data' 6

# Failure of the final clean-control fsync must not publish clean shutdown.
STAGE=shutdown-control-fsync
receive_ahead 7
checked target '(setf *physical-recovery-control-test-hook* (lambda (point state) (when (and (eq point :before-fsync) (= state +db-shutdowned-in-recovery+)) (error "shutdown recovery control fsync failpoint"))))' fail-clean-control
kill -INT "$TARGET"
wait_exit 1
control_state active --applied "$applied"
grep -q 'shutdown recovery control fsync failpoint' "$TMP/target-$START.log"
if grep -q 'database system is shut down' "$TMP/target-$START.log"; then exit 1; fi
start_target
wait_target 'SELECT count(*) FROM signal_data' 7

# A duplicate restartpoint still transitions the durable state to clean.
STAGE=unchanged-restartpoint
sql "$TARGET_PORT" CHECKPOINT
applied=$(sql "$TARGET_PORT" 'SELECT pg_last_wal_replay_lsn()')
cp "$TMP/copy/physical-restartpoint.control" "$TMP/unchanged-point.control"
kill -INT "$TARGET"
wait_exit 0
control_state clean --applied "$applied"
cmp "$TMP/unchanged-point.control" "$TMP/copy/physical-restartpoint.control"

# Startup must fail before changing data if it cannot durably mark recovery active.
STAGE=startup-control-fsync
snapshot_data >"$TMP/data-before.json"
startup_status=0
timeout -k 5 20 "$SBCL" --script "$BASE/t/physical-control-startup-failure.lisp" \
    -p "$TARGET_PORT" -D "$TMP/copy" >"$TMP/startup-control-failure.log" 2>&1 || startup_status=$?
test "$startup_status" -eq 1
grep -q 'startup recovery control fsync failpoint' "$TMP/startup-control-failure.log"
snapshot_data >"$TMP/data-after.json"
cmp "$TMP/data-before.json" "$TMP/data-after.json"
control_state clean --applied "$applied"

# Legacy state is unknown, never inferred clean from a valid applied position.
STAGE=legacy-control-migration
control_state legacy --legacy-fixture --applied "$applied"
start_target
wait_target 'SELECT count(*) FROM signal_data' 7
checked target '(assert (null (getf *physical-recovery-state* :previous-db-state)))' legacy-unknown
test "$(sql "$TARGET_PORT" 'SELECT pg_is_in_recovery()')" = t
if sql "$TARGET_PORT" 'INSERT INTO signal_data VALUES (999)' >"$TMP/reject-primary-write.log" 2>&1; then exit 1; fi
kill -INT "$TARGET"
wait_exit 0
control_state clean --applied "$applied"
checked source '(assert (loop repeat 100 thereis (with-wal-critical (not (gethash "signals_base" *physical-source-senders*))) do (sleep 0.05)))' sender-stopped
checked source '(physical-source-pin-release "signals_base")' release
echo 'physical-shutdown-signals: clean/active state, migration, startup/shutdown fsync, signals and restart PASS'
