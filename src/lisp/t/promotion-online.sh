#!/bin/sh
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
SBCL=${SBCL:-sbcl}
PSQL=${PSQL:-psql}
PATRONI_PYTHON=${PATRONI_PYTHON:-/tmp/pg-patroni-4.1.5-venv/bin/python3}
PORT=${PORT:-6530}
TARGET_PORT=$((PORT + 1))
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-promotion-online.XXXXXX")
SOURCE= TARGET= READER= UTILITY=
cleanup() {
    status=$?
    trap - EXIT
    exec 3>&-
    for pid in "$UTILITY" "$READER" "$TARGET" "$SOURCE"; do
        if [ -n "$pid" ]; then kill -KILL "$pid" 2>/dev/null || :; wait "$pid" 2>/dev/null || :; fi
    done
    echo "promotion-online: artifacts=$TMP"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
sql() {
    PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$1" -U postgres -d postgres -qAt -v ON_ERROR_STOP=1 -c "$2"
}
checked() {
    printf '%s\n(quit)\n' "$2" | "$SBCL" --script "$BASE/t/control-client.lisp" unix "$TMP/$1.sock" >"$TMP/$3.log"
    if grep -q ';; error:' "$TMP/$3.log"; then cat "$TMP/$3.log" >&2; exit 1; fi
}
start_target() {
    rm -f "$TMP/target.sock"
    "$BASE/pg-lisp.sh" -p "$TARGET_PORT" -D "$TMP/copy" --control-socket "$TMP/target.sock" >>"$TMP/target.log" 2>&1 &
    TARGET=$!
    test_wait_owned_sql_server "$TARGET" "$TARGET_PORT" "$PSQL"
}
wait_value() {
    attempt=0
    while [ "$(sql "$TARGET_PORT" "$1")" != "$2" ]; do
        kill -0 "$TARGET"
        attempt=$((attempt + 1)); test "$attempt" -lt 300
        sleep 0.1
    done
}
mkdir -m 700 "$TMP/copy"
"$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/source" --control-socket "$TMP/source.sock" >"$TMP/source.log" 2>&1 &
SOURCE=$!
test_wait_owned_sql_server "$SOURCE" "$PORT" "$PSQL"
sql "$PORT" 'CREATE TABLE data (id integer PRIMARY KEY); INSERT INTO data VALUES (1)'
checked source "(physical-base-backup \"$TMP/copy/\" \"online_promotion\")" backup
start_target
checked target '(physical-recovery-enable-streaming)' streaming
checked target "(physical-follow-start \"127.0.0.1\" $PORT \"online_promotion\")" follow
sql "$PORT" 'INSERT INTO data VALUES (2),(3),(4),(5)'
wait_value 'SELECT count(*) FROM data' 5
sql "$TARGET_PORT" 'SELECT pg_wal_replay_pause()'
"$PATRONI_PYTHON" "$BASE/t/interop/promotion-idle-client.py" "$TARGET_PORT" "$TMP" >"$TMP/reader.log" 2>&1 &
READER=$!
attempt=0
while [ ! -e "$TMP/reader-ready" ]; do
    kill -0 "$READER"
    attempt=$((attempt + 1)); test "$attempt" -lt 200; sleep 0.1
done
sql "$PORT" 'INSERT INTO data VALUES (6),(7),(8),(9),(10)'
end=$(sql "$PORT" 'SELECT pg_current_wal_flush_lsn()')
wait_value "SELECT pg_last_wal_receive_lsn() >= '$end'::pg_lsn" t
test "$(sql "$TARGET_PORT" 'SELECT count(*) FROM data')" = 5
"$PATRONI_PYTHON" "$BASE/t/interop/pg-ctl-promotion-reject.py" "$TMP/copy" "$SOURCE"
if [ "${PROMOTION_REQUEST:-patroni}" = wait ]; then
    "$BASE/bin/pg_ctl" promote -D "$TMP/copy" -w -t 30 >"$TMP/utility.log" 2>&1 &
    UTILITY=$!
else
    "$PATRONI_PYTHON" "$BASE/t/interop/patroni-promote-request.py" "$TMP/copy"
fi
attempt=0
while [ -e "$TMP/copy/promote" ]; do
    kill -0 "$TARGET"
    attempt=$((attempt + 1)); test "$attempt" -lt 200; sleep 0.1
done
attempt=0
while :; do
    checked target '(if *physical-promotion-stopping-p* :stopping :waiting)' promotion-pending
    if grep -q '^:STOPPING$' "$TMP/promotion-pending.log"; then break; fi
    kill -0 "$TARGET"
    attempt=$((attempt + 1)); test "$attempt" -lt 200; sleep 0.1
done
test "$(sql "$TARGET_PORT" 'SELECT pg_is_in_recovery()')" = t
test "$(sql "$TARGET_PORT" 'SELECT count(*) FROM data')" = 5
if sql "$TARGET_PORT" 'SELECT pg_wal_replay_pause()' >"$TMP/pause-rejected.log" 2>&1; then exit 1; fi
grep -q 'standby promotion is ongoing' "$TMP/pause-rejected.log"
checked target '(reload)' promotion-reload
if [ -n "${PROMOTION_SHUTDOWN:-}" ]; then
    kill -"$PROMOTION_SHUTDOWN" "$TARGET"
    if [ "$PROMOTION_SHUTDOWN" = TERM ]; then
        checked target '(assert (not *physical-recovery-failed-p*))' shutdown-not-failed
    else
        wait "$TARGET"
        TARGET=
    fi
    kill -TERM "$READER" 2>/dev/null || :
    wait "$READER" 2>/dev/null || :
    READER=
    if [ -n "$TARGET" ]; then wait "$TARGET"; TARGET=; fi
    grep -q 'database system is shut down' "$TMP/target.log"
    if grep -q 'physical recovery failed' "$TMP/target.log"; then cat "$TMP/target.log" >&2; exit 1; fi
    test -e "$TMP/copy/physical-recovery.control"
    test ! -e "$TMP/copy/pg_wal/00000002.history"
    echo "promotion-online: $PROMOTION_SHUTDOWN during reader wait PASS"
    exit 0
fi
if [ -n "$UTILITY" ]; then kill -0 "$UTILITY"; fi
if "$BASE/bin/pg_ctl" promote -D "$TMP/copy" -w -t 0 >"$TMP/wait-timeout.log" 2>&1; then exit 1; fi
grep -q 'server did not promote in time' "$TMP/wait-timeout.log"
: >"$TMP/reader-release"
wait "$READER"
READER=
if [ -n "$UTILITY" ]; then wait "$UTILITY"; UTILITY=; fi
exec 3>&-
wait_value 'SELECT pg_is_in_recovery()' f
test "$(sql "$TARGET_PORT" 'SELECT count(*) FROM data')" = 12
checked target '(assert (and (not *physical-follow-runtime*) (not *physical-restartpoint-worker*) (not *physical-promotion-stopping-p*) (not *physical-recovery-state*) (not *physical-recovery-failed-p*)))' primary
kill -INT "$SOURCE"; wait "$SOURCE"; SOURCE=
sql "$TARGET_PORT" 'CREATE TABLE after_promotion (id integer); INSERT INTO after_promotion VALUES (11)'
EXPECTED_TIMELINE=2 "$PATRONI_PYTHON" "$BASE/t/interop/patroni-tl-lsn.py" "$TARGET_PORT" primary
kill -KILL "$TARGET"; wait "$TARGET" 2>/dev/null || :; TARGET=
start_target
test "$(sql "$TARGET_PORT" 'SELECT pg_is_in_recovery()')" = f
test "$(sql "$TARGET_PORT" 'SELECT count(*) FROM data')" = 12
test "$(sql "$TARGET_PORT" 'SELECT id FROM after_promotion')" = 11
test ! -e "$TMP/copy/physical-follow.control"
test ! -e "$TMP/copy/promote"
if "$BASE/bin/pg_ctl" promote -D "$TMP/copy" -W >"$TMP/primary-reject.log" 2>&1; then exit 1; fi
grep -q 'server is not in standby mode' "$TMP/primary-reject.log"
kill -INT "$TARGET"; wait "$TARGET"; TARGET=
echo 'promotion-online: signal request, paused backlog, reader drain and primary crash/restart PASS'
