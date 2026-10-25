#!/bin/sh
# Continuous native physical WAL over TCP, with independently delayed replay.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
SBCL=${SBCL:-sbcl}
PSQL=${PSQL:-psql}
PORT=${PORT:-5876}
TARGET_PORT=$((PORT + 1))
PATRONI_PYTHON=${PATRONI_PYTHON:-/tmp/pg-patroni-4.1.5-venv/bin/python3}
test -x "$PATRONI_PYTHON" || {
    echo 'set PATRONI_PYTHON to the pinned Patroni 4.1.5 test environment' >&2
    exit 1
}
"$PATRONI_PYTHON" -c 'import importlib.metadata, psycopg2; from patroni.postgresql import Postgresql; assert importlib.metadata.version("patroni") == "4.1.5"'
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-physical-follow.XXXXXX")
SOURCE= TARGET= TX=
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
cleanup() {
    status=$?
    trap - EXIT
    : >"$TMP/receiver-connect-release"
    if [ "$status" -ne 0 ] && [ -S "$TMP/target.sock" ]; then
        control target '(list :follow *physical-follow-runtime* :receive (getf *physical-recovery-state* :receive-lsn) :apply *last-replay-lsn* :failed *physical-recovery-failed-p*)' >"$TMP/failure-state.log" 2>&1 || :
    fi
    for pid in "$TX" "$TARGET" "$SOURCE"; do
        if [ -n "$pid" ]; then kill -INT "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; fi
    done
    echo "physical-follow: artifacts=$TMP"
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
    test_wait_owned_sql_server "$TARGET" "$TARGET_PORT" "$PSQL"
}
wait_target() {
    attempt=0
    while [ "$(sql "$TARGET_PORT" "$1")" != "$2" ]; do
        attempt=$((attempt + 1))
        if [ "$attempt" -gt 200 ]; then echo "follow timeout: $1" >&2; return 1; fi
        kill -0 "$TARGET"
        sleep 0.1
    done
}
compare() {
    sql "$PORT" 'SELECT id,label FROM data ORDER BY id' >"$TMP/source.rows"
    sql "$TARGET_PORT" 'SELECT id,label FROM data ORDER BY id' >"$TMP/target.rows"
    diff -u "$TMP/source.rows" "$TMP/target.rows"
}
mkdir -m 700 "$TMP/copy"
start_source
sql "$PORT" "CREATE TABLE data (id integer PRIMARY KEY,label varchar(32)); INSERT INTO data VALUES (1,'base'); CREATE TABLE empty_base (id integer)"
checked source "(physical-base-backup \"$TMP/copy/\" \"network_base\")" backup
start_target
"$PATRONI_PYTHON" "$BASE/t/interop/patroni-tl-lsn.py" "$PORT" primary
"$PATRONI_PYTHON" "$BASE/t/interop/patroni-tl-lsn.py" "$TARGET_PORT" standby
"$PATRONI_PYTHON" "$BASE/t/interop/receiver-sql.py" "$TARGET_PORT" absent native
test "$(sql "$TARGET_PORT" "SELECT pg_wal_lsn_diff(COALESCE(pg_last_wal_receive_lsn(),'0/0'),'0/0')::bigint")" = 0
# Install a one-shot hook in the actual standby image. It restores itself before
# waiting, holds no admission/storage lock, and has a five-second bound.
checked target "(let ((original (symbol-function 'pg-client-connect))) (setf (symbol-function 'pg-client-connect) (lambda (&rest arguments) (setf (symbol-function 'pg-client-connect) original) (with-open-file (out \"$TMP/receiver-connecting\" :direction :output) (write-line \"connecting\" out)) (loop repeat 100 until (probe-file \"$TMP/receiver-connect-release\") do (sleep 0.05)) (unless (probe-file \"$TMP/receiver-connect-release\") (error \"receiver connect barrier timed out\")) (apply original arguments))))" connecting-hook
checked target "(physical-follow-start \"127.0.0.1\" $PORT \"network_base\")" follow
attempt=0
while [ ! -f "$TMP/receiver-connecting" ]; do
    attempt=$((attempt + 1)); test "$attempt" -lt 100; sleep 0.02
done
"$PATRONI_PYTHON" "$BASE/t/interop/receiver-sql.py" "$TARGET_PORT" connecting native
: >"$TMP/receiver-connect-release"
sql "$PORT" "INSERT INTO data VALUES (2,'network')"
wait_target 'SELECT count(*) FROM data' 2
test "$(sql "$TARGET_PORT" "SELECT pg_wal_lsn_diff(COALESCE(pg_last_wal_receive_lsn(),'0/0'),'0/0')::bigint > 0")" = t
compare
"$PATRONI_PYTHON" "$BASE/t/interop/receiver-sql.py" "$TARGET_PORT" streaming native "$PORT"
checked source '(reload)' source-reload
checked target '(reload)' target-reload
test "$(sql "$TARGET_PORT" 'SELECT COALESCE(NULL,1)')" = 1
"$PATRONI_PYTHON" "$BASE/t/interop/patroni-tl-lsn.py" "$TARGET_PORT" standby
"$PATRONI_PYTHON" "$BASE/t/interop/receiver-sql.py" "$TARGET_PORT" streaming native "$PORT"
# SQL pause stops only apply: durable receipt and feedback continue.
test "$(sql "$TARGET_PORT" 'SELECT pg_get_wal_replay_pause_state()')" = 'not paused'
test "$(sql "$TARGET_PORT" 'SELECT pg_wal_replay_pause()')" = ''
test "$(sql "$TARGET_PORT" 'SELECT pg_is_wal_replay_paused()')" = t
test "$(sql "$TARGET_PORT" 'SELECT pg_get_wal_replay_pause_state()')" = paused
paused_replay=$(sql "$TARGET_PORT" 'SELECT pg_last_wal_replay_lsn()')
sql "$PORT" "UPDATE data SET label='received-while-paused' WHERE id=1"
paused_flush=$(sql "$PORT" 'SELECT pg_current_wal_flush_lsn()')
wait_target "SELECT pg_last_wal_receive_lsn() >= '$paused_flush'::pg_lsn" t
test "$(sql "$TARGET_PORT" 'SELECT pg_last_wal_replay_lsn()')" = "$paused_replay"
test "$(sql "$TARGET_PORT" 'SELECT label FROM data WHERE id=1')" = base
"$PATRONI_PYTHON" "$BASE/t/interop/patroni-tl-lsn.py" "$TARGET_PORT" standby paused
"$PATRONI_PYTHON" "$BASE/t/interop/receiver-sql.py" "$TARGET_PORT" paused native "$PORT"
checked target '(physical-recovery-replay)' paused-direct-replay
grep -q '^:PAUSED$' "$TMP/paused-direct-replay.log"
checked target '(reload)' paused-reload
test "$(sql "$TARGET_PORT" 'SELECT pg_get_wal_replay_pause_state()')" = paused
"$PATRONI_PYTHON" "$BASE/t/interop/receiver-sql.py" "$TARGET_PORT" paused native "$PORT"
test "$(sql "$TARGET_PORT" 'SELECT pg_wal_replay_resume()')" = ''
wait_target "SELECT label FROM data WHERE id=1" received-while-paused
"$PATRONI_PYTHON" "$BASE/t/interop/patroni-tl-lsn.py" "$TARGET_PORT" standby
test "$(sql "$TARGET_PORT" 'SELECT pg_is_wal_replay_paused()')" = f
# Release while connected must fail without poisoning source WAL.
control source '(physical-source-pin-release "network_base")' >"$TMP/release-active.log"
grep -q 'active sender' "$TMP/release-active.log"
# A stalled reply times out at the sender, then retries instead of latching XX000.
# The delay runs outside admission, so SQL remains usable throughout.
checked target '(setf *physical-stream-test-hook* (lambda (point) (when (eq point :before-feedback) (setf *physical-stream-test-hook* nil) (sleep 11))))' delayed-feedback
test "$(sql "$TARGET_PORT" 'SELECT count(*) FROM data')" = 2
attempt=0
while ! grep -q 'native physical receiver retrying' "$TMP/target.log"; do
    attempt=$((attempt + 1)); test "$attempt" -lt 200; sleep 0.1
done
sql "$PORT" "UPDATE data SET label='after-feedback-timeout' WHERE id=1"
wait_target "SELECT label FROM data WHERE id=1" after-feedback-timeout
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
replay_before=$(sql "$TARGET_PORT" 'SELECT pg_last_wal_replay_lsn()')
checked source "(setf *physical-stream-test-hook* (lambda (point stream partial) (when (and (eq point :after-fragment) partial) (setf *physical-stream-test-hook* nil) (with-open-file (out \"$TMP/partial-disconnect\" :direction :output) (write-line \"hit\" out)) (error 'end-of-file :stream stream))))" partial-hook
rows=$(python3 - <<'PY'
print(",".join(f"({i},'fragmented-network')" for i in range(1000, 2000)))
PY
)
sql "$PORT" "INSERT INTO data VALUES $rows; CREATE TABLE network_empty (id integer)"
source_flush=$(sql "$PORT" 'SELECT pg_current_wal_flush_lsn()')
wait_target "SELECT pg_last_wal_receive_lsn() >= '$source_flush'::pg_lsn" t
test -f "$TMP/partial-disconnect"
test "$(sql "$TARGET_PORT" 'SELECT pg_last_wal_replay_lsn()')" = "$replay_before"
test "$(sql "$TARGET_PORT" 'SELECT count(*) FROM data')" = 2
printf 'COMMIT;\n\\q\n' >&7
exec 7>&-
wait "$TX"
TX=
wait_target 'SELECT count(*) FROM data' 1002
wait_target 'SELECT count(*) FROM network_empty' 0
compare
# Restart the source manually; receiver reconnects from its durable spool cursor.
kill -KILL "$SOURCE"
wait "$SOURCE" 2>/dev/null || :
SOURCE=
start_source
sql "$PORT" "UPDATE data SET label='after-source-crash' WHERE id=1; INSERT INTO data VALUES (3,'reconnect')"
wait_target "SELECT label FROM data WHERE id=1" after-source-crash
wait_target 'SELECT count(*) FROM data' 1003
compare
"$PATRONI_PYTHON" "$BASE/t/interop/receiver-sql.py" "$TARGET_PORT" streaming native "$PORT"
# Persisted follow configuration starts exactly one receiver/replayer after restart.
sql "$TARGET_PORT" 'SELECT pg_wal_replay_pause()' >/dev/null
sql "$PORT" "UPDATE data SET label='durable-paused-before-crash' WHERE id=1"
restart_flush=$(sql "$PORT" 'SELECT pg_current_wal_flush_lsn()')
wait_target "SELECT pg_last_wal_receive_lsn() >= '$restart_flush'::pg_lsn" t
test "$(sql "$TARGET_PORT" 'SELECT label FROM data WHERE id=1')" = after-source-crash
kill -KILL "$TARGET"
wait "$TARGET" 2>/dev/null || :
TARGET=
start_target
test "$(sql "$TARGET_PORT" 'SELECT pg_get_wal_replay_pause_state()')" = 'not paused'
wait_target "SELECT label FROM data WHERE id=1" durable-paused-before-crash
sql "$PORT" "DELETE FROM data WHERE id=2; INSERT INTO data VALUES (4,'target-restart')"
wait_target "SELECT label FROM data WHERE id=4" target-restart
compare
"$PATRONI_PYTHON" "$BASE/t/interop/receiver-sql.py" "$TARGET_PORT" streaming native "$PORT"
test "$(sql "$TARGET_PORT" 'SELECT pg_is_in_recovery()')" = t
checked target '(physical-follow-stop)' stop
"$PATRONI_PYTHON" "$BASE/t/interop/receiver-sql.py" "$TARGET_PORT" absent native
test ! -e "$TMP/copy/physical-follow.control"
sql "$PORT" "INSERT INTO data VALUES (5,'stopped')"
test "$(sql "$TARGET_PORT" 'SELECT count(*) FROM data WHERE id=5')" = 0
checked source '(physical-source-pin-release "network_base")' release
# A new primary at the same endpoint must not be adopted by identity alone.
kill -INT "$SOURCE"
wait "$SOURCE"
SOURCE=
rm -f "$TMP/source.sock"
"$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/foreign-source" \
    --control-socket "$TMP/source.sock" >>"$TMP/foreign-source.log" 2>&1 &
SOURCE=$!
test_wait_owned_sql_server "$SOURCE" "$PORT" "$PSQL"
checked target "(physical-follow-start \"127.0.0.1\" $PORT \"network_base\")" wrong-peer
attempt=0
while ! grep -q 'source identity or timeline differs' "$TMP/target.log"; do
    attempt=$((attempt + 1)); test "$attempt" -lt 100; sleep 0.1
done
checked target '(assert (physical-follow-runtime-stopped-p *physical-follow-runtime*))' wrong-peer-stopped
test "$(sql "$TARGET_PORT" 'SELECT count(*) FROM data')" = 1003
checked target '(physical-follow-stop)' wrong-peer-stop
echo 'physical-follow: PASS'
