#!/bin/sh
# t/shutdown.sh -- PostgreSQL-compatible shutdown and backend signaling.
#
# Covers:
#   * pg_backend_pid(), pg_cancel_backend(), pg_terminate_backend(),
#     pg_reload_conf() including their errors and warnings
#   * cancelling/terminating a backend blocked on a unique conflict
#   * SIGTERM = smart shutdown (wait for clients, reject new ones)
#   * SIGINT  = fast shutdown (terminate clients, deliver FATAL)
#   * SIGQUIT = immediate shutdown (do not wait for clients; still exit 0)
#   * smart -> fast escalation
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
SBCL=${SBCL:-sbcl}
PORT=${PORT:-5466}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-shutdown.XXXXXX")
LOG="$TMP/server.log"
SRV=
STALLED=

cleanup() {
    close_fd7
    close_fd8
    if [ -n "$STALLED" ]; then
        kill "$STALLED" 2>/dev/null || true
        wait "$STALLED" 2>/dev/null || true
    fi
    if [ -n "$SRV" ]; then
        kill -9 "$SRV" 2>/dev/null || true
        wait "$SRV" 2>/dev/null || true
    fi
    rm -rf "$TMP"
}
trap cleanup EXIT INT TERM

fail() {
    echo "shutdown: FAIL $1" >&2
    [ -f "$LOG" ] && cat "$LOG" >&2
    exit 1
}

psqlq() {
    PGPASSWORD=x psql -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres -q -A -t "$@"
}

q() {
    psqlq -c "$1"
}

# FIFO writers for sessions we hold open.  Closing an already-closed fd makes
# a POSIX shell exit on the exec redirection error, so track them explicitly.
FD7_OPEN=0
FD8_OPEN=0

open_fd7() { exec 7> "$1"; FD7_OPEN=1; }
open_fd8() { exec 8> "$1"; FD8_OPEN=1; }
close_fd7() { if [ "$FD7_OPEN" = 1 ]; then exec 7>&-; FD7_OPEN=0; fi; }
close_fd8() { if [ "$FD8_OPEN" = 1 ]; then exec 8>&-; FD8_OPEN=0; fi; }
close_fds() { close_fd7; close_fd8; }

start_server() {
    rm -rf "$TMP/data"
    : > "$LOG"
    "$BASE/pg-lisp.sh" -p "$PORT" -D "$TMP/data" > "$LOG" 2>&1 &
    SRV=$!
    i=0
    while [ "$i" -lt 150 ]; do
        if psqlq -c '' >/dev/null 2>&1; then
            return 0
        fi
        i=$((i + 1))
        sleep 0.2
    done
    fail "server did not start"
}

stop_server() {
    close_fd7
    close_fd8
    if [ -n "$SRV" ] && kill -0 "$SRV" 2>/dev/null; then
        kill -TERM "$SRV" 2>/dev/null || true
    fi
    i=0
    while [ "$i" -lt 100 ] && [ -n "$SRV" ] && kill -0 "$SRV" 2>/dev/null; do
        i=$((i + 1))
        sleep 0.1
    done
    if [ -n "$SRV" ]; then
        kill -9 "$SRV" 2>/dev/null || true
        wait "$SRV" 2>/dev/null || true
    fi
    SRV=
}

wait_for_exit() {
    i=0
    while [ "$i" -lt 100 ]; do
        if ! kill -0 "$SRV" 2>/dev/null; then
            return 0
        fi
        i=$((i + 1))
        sleep 0.1
    done
    return 1
}

# Read the pid a psql session reports on its first line.
learn_pid() {
    pid=
    i=0
    while [ "$i" -lt 50 ]; do
        pid=$(grep -m1 -E '^[0-9]+$' "$1" 2>/dev/null || true)
        [ -n "$pid" ] && return 0
        i=$((i + 1))
        sleep 0.1
    done
    return 1
}

# ---- admin functions ------------------------------------------------------

start_server

pid=$(q 'select pg_backend_pid()')
case "$pid" in
    ''|*[!0-9]*) fail "pg_backend_pid did not return an integer: '$pid'" ;;
esac
[ "$pid" -gt 0 ] || fail "pg_backend_pid returned $pid"

# PIDs are OS thread ids, so concurrent connections must never collide.  (A
# shared counter used to race here and leak *N-BACKENDS*, which made smart and
# fast shutdown wait forever.)
mkdir -p "$TMP/pids"
pids_jobs=
i=0
while [ "$i" -lt 30 ]; do
    (psqlq -c 'select pg_backend_pid()' > "$TMP/pids/$i" 2>&1) &
    pids_jobs="$pids_jobs $!"
    i=$((i + 1))
done
for j in $pids_jobs; do wait "$j"; done
dups=$(cat "$TMP"/pids/* | sort | uniq -d)
[ -z "$dups" ] || fail "duplicate backend pids: $dups"
n=$(cat "$TMP"/pids/* | grep -cE '^[0-9]+$' || true)
[ "$n" -eq 30 ] || fail "expected 30 backend pids, got $n"

out=$(printf 'begin;\nselect pg_cancel_backend(pg_backend_pid());\nselect 1;\ncommit;\n' |
      PGPASSWORD=x psql -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres 2>&1 || true)
echo "$out" | grep -q 'canceling statement due to user request' \
    || fail "self cancel did not raise 57014"
echo "$out" | grep -q 'current transaction is aborted' \
    || fail "self cancel did not abort the transaction"
echo "$out" | grep -q 'ROLLBACK' \
    || fail "COMMIT after a cancel did not roll back"

out=$(q 'select pg_terminate_backend(pg_backend_pid())' 2>&1 || true)
echo "$out" | grep -q 'FATAL:  terminating connection due to administrator command' \
    || fail "self terminate did not raise FATAL 57P01"

out=$(q 'select pg_terminate_backend(999999)' 2>&1 || true)
echo "$out" | grep -q 'WARNING:  PID 999999 is not a PostgreSQL backend process' \
    || fail "unknown pid did not produce a warning"
echo "$out" | grep -q '^f$' \
    || fail "unknown pid did not return false"

out=$(q 'select pg_terminate_backend(1, -1)' 2>&1 || true)
echo "$out" | grep -q '"timeout" must not be negative' \
    || fail "negative timeout was not rejected"

# Argument coercion: PID is int4 and timeout is int8; neither function is
# strict, so NULL is 0, and a string literal is parsed like the target type.
out=$(q 'select pg_terminate_backend(null)' 2>&1 || true)
echo "$out" | grep -q 'WARNING:  PID 0 is not a PostgreSQL backend process' \
    || fail "NULL pid did not coerce to 0"
out=$(q "select pg_terminate_backend(999999, '5')" 2>&1 || true)
echo "$out" | grep -q 'WARNING:  PID 999999 is not a PostgreSQL backend process' \
    || fail "string timeout did not coerce to bigint"
echo "$out" | grep -q '^f$' || fail "string-timeout call did not return false"
out=$(q "select pg_terminate_backend('abc')" 2>&1 || true)
echo "$out" | grep -q 'invalid input syntax for type integer: "abc"' \
    || fail "bad pid string was not rejected with 22P02"
out=$(q 'select pg_terminate_backend(999999, 9223372036854775808)' 2>&1 || true)
echo "$out" | grep -q 'bigint out of range' \
    || fail "timeout outside bigint range was accepted"
out=$(q 'select pg_cancel_backend(true)' 2>&1 || true)
echo "$out" | grep -q 'function pg_cancel_backend(boolean) does not exist' \
    || fail "boolean pid did not fail function resolution"
out=$(q 'select pg_terminate_backend(1, true)' 2>&1 || true)
echo "$out" | grep -q 'function pg_terminate_backend(integer, boolean) does not exist' \
    || fail "boolean timeout did not fail function resolution"
out=$(q 'select pg_cancel_backend(null)' 2>&1 || true)
echo "$out" | grep -q 'WARNING:  PID 0 is not a PostgreSQL backend process' \
    || fail "NULL cancel pid did not warn about PID 0"
out=$(q 'select pg_backend_pid(1)' 2>&1 || true)
echo "$out" | grep -q 'function pg_backend_pid(integer) does not exist' \
    || fail "arity error did not name the argument type"

before=$(grep -c 'reload complete' "$LOG" 2>/dev/null || true)
[ "$(q 'select pg_reload_conf()')" = "t" ] \
    || fail "pg_reload_conf did not return true"
i=0
while [ "$i" -lt 100 ]; do
    now=$(grep -c 'reload complete' "$LOG" 2>/dev/null || true)
    [ "$now" -gt "$before" ] && break
    i=$((i + 1))
    sleep 0.2
done
now=$(grep -c 'reload complete' "$LOG" 2>/dev/null || true)
[ "$now" -gt "$before" ] || fail "pg_reload_conf did not trigger a reload"

# ---- remote cancel and terminate -----------------------------------------

q 'create table t (a int primary key)' >/dev/null

rm -f "$TMP/a_in" "$TMP/b_in"
mkfifo "$TMP/a_in" "$TMP/b_in"
psqlq < "$TMP/a_in" > "$TMP/a_out" 2>&1 &
psqlq < "$TMP/b_in" > "$TMP/b_out" 2>&1 &
open_fd7 "$TMP/a_in"
open_fd8 "$TMP/b_in"

# A holds an uncommitted key; B will block on the same key.
printf 'begin;\ninsert into t values (1);\n' >&7
sleep 0.6
printf 'select pg_backend_pid();\n' >&8
learn_pid "$TMP/b_out" || fail "could not learn the blocking backend pid"
bpid=$pid
printf 'insert into t values (1);\n' >&8
sleep 0.6

[ "$(q "select pg_cancel_backend($bpid)")" = "t" ] \
    || fail "pg_cancel_backend returned false"
sleep 0.5
grep -q 'ERROR:  canceling statement due to user request' "$TMP/b_out" \
    || fail "blocked backend was not cancelled"

# The cancelled backend must survive: it is still able to run a query.
printf 'select 99;\n' >&8
sleep 0.5
grep -q '^99$' "$TMP/b_out" \
    || fail "cancelled backend did not survive"

printf 'insert into t values (1);\n' >&8
sleep 0.6
[ "$(q "select pg_terminate_backend($bpid, 2000)")" = "t" ] \
    || fail "pg_terminate_backend returned false"
sleep 0.5
grep -q 'FATAL:  terminating connection due to administrator command' "$TMP/b_out" \
    || fail "blocked backend was not terminated"

printf 'rollback;\n' >&7
sleep 0.3
close_fd7
close_fd8
stop_server
grep -q 'received smart shutdown request' "$LOG" \
    || fail "stop_server did not take the smart path"

# ---- SIGTERM: smart shutdown ---------------------------------------------

start_server
rm -f "$TMP/hold"
mkfifo "$TMP/hold"
psqlq < "$TMP/hold" > "$TMP/hold.log" 2>&1 &
open_fd7 "$TMP/hold"
sleep 0.5

kill -TERM "$SRV"
sleep 0.6
kill -0 "$SRV" 2>/dev/null \
    || fail "smart shutdown did not wait for the connected client"

out=$(psqlq -c 'select 1' 2>&1 || true)
echo "$out" | grep -q 'the database system is shutting down' \
    || fail "new connection was not rejected during smart shutdown"

close_fd7
wait_for_exit || fail "server did not exit after the last client left"
set +e
wait "$SRV" 2>/dev/null
status=$?
set -e
SRV=
[ "$status" -eq 0 ] || fail "smart shutdown exited with status $status"
grep -q 'received smart shutdown request' "$LOG" \
    || fail "missing smart shutdown log line"
grep -q 'database system is shut down' "$LOG" \
    || fail "missing shutdown-complete log line"

# ---- SIGINT: fast shutdown with a stalled startup handshake --------------

start_server
STALLED_PORT=$PORT STALLED_READY="$TMP/stalled-ready" \
    "$SBCL" --script "$BASE/t/stalled-startup-client.lisp" &
STALLED=$!
i=0
while [ "$i" -lt 50 ] && [ ! -f "$TMP/stalled-ready" ] \
      && kill -0 "$STALLED" 2>/dev/null; do
    i=$((i + 1))
    sleep 0.1
done
[ -f "$TMP/stalled-ready" ] || fail "startup-stalled client did not connect"
kill -0 "$STALLED" 2>/dev/null \
    || fail "startup-stalled client exited before shutdown"

kill -INT "$SRV"
wait_for_exit || fail "fast shutdown did not terminate a stalled startup"
set +e
wait "$SRV" 2>/dev/null
status=$?
set -e
SRV=
[ "$status" -eq 0 ] || fail "startup-stalled fast shutdown exited with status $status"
kill "$STALLED" 2>/dev/null || true
wait "$STALLED" 2>/dev/null || true
STALLED=
grep -q 'received fast shutdown request' "$LOG" \
    || fail "missing fast shutdown log line for startup-stalled client"

# ---- SIGINT: fast shutdown with a blocked backend ------------------------

start_server
q 'create table t (a int primary key)' >/dev/null
rm -f "$TMP/a_in" "$TMP/b_in"
mkfifo "$TMP/a_in" "$TMP/b_in"
psqlq < "$TMP/a_in" > "$TMP/a_out" 2>&1 &
psqlq < "$TMP/b_in" > "$TMP/b_out" 2>&1 &
open_fd7 "$TMP/a_in"
open_fd8 "$TMP/b_in"
printf 'begin;\ninsert into t values (2);\n' >&7
sleep 0.6
printf 'select pg_backend_pid();\n' >&8
learn_pid "$TMP/b_out" || fail "could not learn the blocking backend pid"
printf 'insert into t values (2);\n' >&8
sleep 0.6

kill -INT "$SRV"
wait_for_exit || fail "fast shutdown did not exit"
set +e
wait "$SRV" 2>/dev/null
status=$?
set -e
SRV=
[ "$status" -eq 0 ] || fail "fast shutdown exited with status $status"
sleep 0.3
grep -q 'FATAL:  terminating connection due to administrator command' "$TMP/b_out" \
    || fail "fast shutdown did not deliver FATAL to the active backend"
grep -q 'received fast shutdown request' "$LOG" \
    || fail "missing fast shutdown log line"
grep -q 'aborting any active transactions' "$LOG" \
    || fail "missing aborted-transactions log line"
close_fd7
close_fd8

# ---- SIGQUIT: immediate shutdown -----------------------------------------

start_server
kill -QUIT "$SRV"
wait_for_exit || fail "immediate shutdown did not exit"
set +e
wait "$SRV" 2>/dev/null
status=$?
set -e
SRV=
[ "$status" -eq 0 ] || fail "immediate shutdown exited with status $status"
grep -q 'received immediate shutdown request' "$LOG" \
    || fail "missing immediate shutdown log line"
# PostgreSQL's postmaster still exits cleanly (UnlinkLockFiles logs this and
# removes the socket); only the children are killed abruptly.
grep -q 'database system is shut down' "$LOG" \
    || fail "missing shutdown-complete log line after immediate shutdown"

# ---- smart -> fast escalation --------------------------------------------

start_server
rm -f "$TMP/hold"
mkfifo "$TMP/hold"
psqlq < "$TMP/hold" > "$TMP/hold.log" 2>&1 &
open_fd7 "$TMP/hold"
sleep 0.5

kill -TERM "$SRV"
sleep 0.5
kill -0 "$SRV" 2>/dev/null || fail "server exited before escalation"
kill -INT "$SRV"
wait_for_exit || fail "escalated shutdown did not exit"
set +e
wait "$SRV" 2>/dev/null
status=$?
set -e
SRV=
[ "$status" -eq 0 ] || fail "escalated shutdown exited with status $status"
grep -q 'received smart shutdown request' "$LOG" \
    || fail "missing smart request before escalation"
grep -q 'received fast shutdown request' "$LOG" \
    || fail "missing fast request after escalation"
close_fd7

echo "shutdown: PASS (admin functions, cancel/terminate, smart/fast/immediate, escalation)"
