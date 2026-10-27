#!/bin/sh
# Native standby maintenance without a follower, then with fresh-process recovery.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
SBCL=${SBCL:-sbcl}
PSQL=${PSQL:-psql}
PORT=${PORT:-5890}
TARGET_PORT=$((PORT + 1))
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-restartpoint-auto.XXXXXX")
SOURCE= TARGET= TX=
cleanup() {
    status=$?
    trap - EXIT
    for pid in "$TX" "$TARGET" "$SOURCE"; do
        if [ -n "$pid" ]; then kill -INT "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; fi
    done
    echo "physical-restartpoint-auto: artifacts=$TMP"
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
ship() {
    cursor=$(sql "$TARGET_PORT" 'SELECT pg_last_wal_replay_lsn()')
    checked source "(physical-test-export-wal (parse-replication-lsn \"$cursor\") \"$TMP/frames.bin\")" export
    checked target "(physical-test-receive-file \"$TMP/frames.bin\")" receive
    checked target '(physical-recovery-replay)' apply
}
wait_floor() {
    attempt=0
    while :; do
        kill -0 "$TARGET" 2>/dev/null || return 1
        control target "(assert (>= (physical-store-floor-lsn *physical-receive-store*) (parse-replication-lsn \"$1\")))" >"$TMP/floor.log"
        if ! grep -q ';; error:' "$TMP/floor.log"; then return 0; fi
        attempt=$((attempt + 1)); test "$attempt" -lt 100; sleep 0.1
    done
}
compare() {
    sql "$PORT" 'SELECT id FROM data ORDER BY id' >"$TMP/source.rows"
    sql "$TARGET_PORT" 'SELECT id FROM data ORDER BY id' >"$TMP/target.rows"
    diff -u "$TMP/source.rows" "$TMP/target.rows"
}
mkdir -m 700 "$TMP/copy"
"$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/source" \
    --control-socket "$TMP/source.sock" >"$TMP/source.log" 2>&1 &
SOURCE=$!
test_wait_owned_sql_server "$SOURCE" "$PORT" "$PSQL"
sql "$PORT" 'CREATE TABLE data (id integer); INSERT INTO data VALUES (1)'
checked source "(load \"$BASE/t/physical-replay-native.lisp\")" source-hooks
case "${SOURCE_WAL_NAMESPACE:-legacy}" in
    timeline) checked source '(wal-migrate-timeline-namespace)' source-namespace ;;
    legacy) : ;;
    *) echo 'invalid SOURCE_WAL_NAMESPACE' >&2; exit 1 ;;
esac
checked source "(physical-base-backup \"$TMP/copy/\" \"auto_r\")" backup
start_target
# Static recovery has no newer terminal. CHECKPOINT must remain a successful no-op.
sql "$TARGET_PORT" 'BEGIN; CHECKPOINT; ROLLBACK'
checked target '(physical-recovery-enable-streaming)' enable
checked target '(assert (and *physical-restartpoint-worker* (sb-thread:thread-alive-p *physical-restartpoint-worker*)))' worker-started
sql "$PORT" 'INSERT INTO data VALUES (2)'
ship
base=$(sql "$TARGET_PORT" 'SELECT pg_last_wal_replay_lsn()')
mkfifo "$TMP/tx.fifo"
PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$TARGET_PORT" -U postgres -d postgres \
    -qAt -v ON_ERROR_STOP=1 <"$TMP/tx.fifo" >"$TMP/tx.log" 2>&1 &
TX=$!
exec 7>"$TMP/tx.fifo"
printf 'BEGIN ISOLATION LEVEL REPEATABLE READ;\nSELECT count(*) FROM data;\n' >&7
attempt=0
while ! grep -q '^2$' "$TMP/tx.log"; do
    attempt=$((attempt + 1)); test "$attempt" -lt 100; sleep 0.1
done
checked target '(setf *physical-restartpoint-timeout* 0)' timer-on
checked target '(physical-recovery-restartpoint)' readers-defer
grep -q ':DEFERRED' "$TMP/readers-defer.log"
# A timer gets an opportunity to run; neither a held snapshot nor its COMMIT is starved.
sleep 1
checked target "(assert (< (physical-store-floor-lsn *physical-receive-store*) (parse-replication-lsn \"$base\")))" reader-floor
# SQL admission pins apply. SQL CHECKPOINT itself may sync an already applied R
# while another reader retains a snapshot; it changes no visible database data.
sql "$TARGET_PORT" 'BEGIN; CHECKPOINT; ROLLBACK'
wait_floor "$base"
printf 'SELECT count(*) FROM data;\nCOMMIT;\n\\q\n' >&7
exec 7>&-
wait "$TX"
TX=
test "$(grep -c '^2$' "$TMP/tx.log")" -eq 2
sql "$PORT" 'INSERT INTO data VALUES (3)'
ship
timed=$(sql "$TARGET_PORT" 'SELECT pg_last_wal_replay_lsn()')
wait_floor "$timed"
checked target '(setf *physical-restartpoint-timeout* 300 *physical-restartpoint-wal-bytes* 1)' bytes-on
sql "$PORT" 'INSERT INTO data VALUES (4)'
ship
bytes=$(sql "$TARGET_PORT" 'SELECT pg_last_wal_replay_lsn()')
wait_floor "$bytes"
checked target '(reload)' reload
grep -q 'reload complete' "$TMP/reload.log"
# A configuration reload restores configured thresholds; reinstall this private
# test-only byte trigger before the intentional automatic-publication failure.
checked target '(setf *physical-restartpoint-wal-bytes* 1)' reinstall-byte-trigger
checked target '(assert (= 1 (count "native standby restartpoints" (sb-thread:list-all-threads) :test (function string=) :key (function sb-thread:thread-name))))' one-worker
compare
# C permits CHECKPOINT inside a transaction; it must not commit its pending row.
sql "$PORT" 'BEGIN; INSERT INTO data VALUES (5); CHECKPOINT; ROLLBACK'
test "$(sql "$PORT" 'SELECT count(*) FROM data')" = 4
ship
compare
stable=$(sql "$TARGET_PORT" 'SELECT pg_last_wal_replay_lsn()')
wait_floor "$stable"
checked target '(setf *physical-restartpoint-test-hook* (lambda (point) (when (eq point :after-generation) (error "injected automatic restartpoint error"))))' error-hook
sql "$PORT" 'INSERT INTO data VALUES (6)'
ship
attempt=0
while :; do
    if ! sql "$TARGET_PORT" 'SELECT count(*) FROM data' >"$TMP/failed-sql.log" 2>&1; then
        if grep -q 'physical recovery failed' "$TMP/failed-sql.log"; then break; fi
    fi
    attempt=$((attempt + 1)); test "$attempt" -lt 100; sleep 0.1
done
kill -KILL "$TARGET"
wait "$TARGET" 2>/dev/null || :
TARGET=
start_target
compare
test "$(sql "$TARGET_PORT" 'SELECT count(*) FROM data')" = 5
checked target '(assert (= 1 (count "native standby restartpoints" (sb-thread:list-all-threads) :test (function string=) :key (function sb-thread:thread-name))))' restarted-worker
# Crash after selecting a newer restartpoint while receive control still names
# the old floor/root, then run actual startup recovery and another compaction.
sql "$TARGET_PORT" 'CHECKPOINT'
checked target "(with-open-file (stream \"$TMP/old-floor\" :direction :output :if-exists :supersede) (format stream \"~d~%\" (physical-store-floor-lsn *physical-receive-store*)))" save-old-floor
read -r old_floor <"$TMP/old-floor"
checked target "(setf *physical-restartpoint-wal-bytes* 1 *physical-restartpoint-test-hook* (lambda (point) (when (eq point :after-pointer) (wal-atomic-write-bytes (pathname \"$TMP/pointer.marker\") (string->bytes (format nil \"~d~%\" (sb-posix:getpid)))) (loop repeat 300 do (sleep 0.1)) (error \"pointer crash was not delivered\"))))" pointer-hook
sql "$PORT" 'INSERT INTO data VALUES (7)'
ship
attempt=0
while [ ! -f "$TMP/pointer.marker" ]; do
    kill -0 "$TARGET" 2>/dev/null || exit 1
    attempt=$((attempt + 1)); test "$attempt" -lt 100; sleep 0.1
done
read -r marked_pid <"$TMP/pointer.marker"
test "$marked_pid" = "$TARGET"
kill -KILL "$TARGET"
wait "$TARGET" 2>/dev/null || :
TARGET=
start_target
compare
test "$(sql "$TARGET_PORT" 'SELECT count(*) FROM data')" = 6
checked target "(assert (= $old_floor (physical-store-floor-lsn *physical-receive-store*)))" pointer-old-floor
sql "$TARGET_PORT" 'CHECKPOINT'
stable=$(sql "$TARGET_PORT" 'SELECT pg_last_wal_replay_lsn()')
wait_floor "$stable"
python3 "$BASE/t/interop/restartpoint-prefix-inspect.py" "$TMP/copy" "$TMP/source"
# A CRC-valid incomplete promotion must fail before redo or a control rewrite.
kill -INT "$TARGET"
wait "$TARGET"
TARGET=
cp -a "$TMP/copy" "$TMP/pending"
python3 "$BASE/t/interop/restartpoint-prefix-inspect.py" --pending-fixture \
    "$TMP/pending" "$TMP/source" "$TMP/pending-before.json"
set +e
timeout 15 "$BASE/pg-lisp.sh" -p "$TARGET_PORT" -D "$TMP/pending" \
    >"$TMP/pending-startup.log" 2>&1
pending_status=$?
set -e
test "$pending_status" -eq 1
grep -q 'pending promotion lost its exact selected generation' "$TMP/pending-startup.log"
python3 "$BASE/t/interop/restartpoint-prefix-inspect.py" --verify-pending \
    "$TMP/pending" "$TMP/pending-before.json"
echo 'physical-restartpoint-auto: PASS'
