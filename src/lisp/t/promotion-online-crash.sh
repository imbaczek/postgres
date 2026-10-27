#!/bin/sh
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$BASE/t/test-support.sh"
SBCL=${SBCL:-sbcl}
PSQL=${PSQL:-psql}
PORT=${PORT:-6542}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-promotion-online-crash.XXXXXX")
SERVER=
cleanup() {
    status=$?
    trap - EXIT
    if [ -n "$SERVER" ]; then kill -KILL "$SERVER" 2>/dev/null || :; wait "$SERVER" 2>/dev/null || :; fi
    echo "promotion-online-crash: artifacts=$TMP"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
if [ -n "${PROMOTION_SEED:-}" ]; then SEED=$PROMOTION_SEED
else
    PORT=$PORT "$BASE/t/physical-restartpoint-auto.sh" >"$TMP/seed.log" 2>&1
    SEED=$(sed -n 's/^physical-restartpoint-auto: artifacts=//p' "$TMP/seed.log")
fi
sql() { PGCONNECT_TIMEOUT=2 "$PSQL" -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres -qAt -v ON_ERROR_STOP=1 -c "$1"; }
checked() {
    printf '%s\n(quit)\n' "$1" | "$SBCL" --script "$BASE/t/control-client.lisp" unix "$CASE/control.sock" >"$CASE/$2.log"
    if grep -q ';; error:' "$CASE/$2.log"; then cat "$CASE/$2.log" >&2; exit 1; fi
}
clone() {
    CASE=$TMP/$1
    mkdir -m 700 "$CASE"
    cp -a "$SEED/copy" "$CASE/data"
    rm -f "$CASE/data/physical-follow.control"
}
start() {
    rm -f "$CASE/control.sock"
    "$BASE/pg-lisp.sh" -p "$PORT" -D "$CASE/data" --control-socket "$CASE/control.sock" >>"$CASE/server.log" 2>&1 &
    SERVER=$!
    test_wait_owned_sql_server "$SERVER" "$PORT" "$PSQL"
}
stop() { kill -INT "$SERVER"; wait "$SERVER"; SERVER=; }
crash() { kill -KILL "$SERVER"; wait "$SERVER" 2>/dev/null || :; SERVER=; }
barrier() {
    attempt=0
    while [ ! -e "$CASE/marker" ]; do
        kill -0 "$SERVER" 2>/dev/null || { cat "$CASE/server.log" >&2; exit 1; }
        attempt=$((attempt + 1)); test "$attempt" -lt 300; sleep 0.1
    done
    read -r marker_pid <"$CASE/marker"
    test "$marker_pid" = "$SERVER"
}
for point in before-online-intent after-online-intent after-child-control-rename; do
    clone "$point"
    PG_PROMOTION_DATA="$CASE/data" PG_PROMOTION_PORT="$PORT" PG_PROMOTION_SOCKET="$CASE/control.sock" \
        PG_PROMOTION_PAUSE="$point" PG_PROMOTION_MARKER="$CASE/marker" \
        "$SBCL" --script "$BASE/t/promotion-online-native.lisp" >"$CASE/server.log" 2>&1 &
    SERVER=$!
    test_wait_owned_sql_server "$SERVER" "$PORT" "$PSQL"
    "$BASE/bin/pg_ctl" promote -D "$CASE/data" -W
    barrier
    crash
    start
    if [ "$point" = before-online-intent ]; then
        test "$(sql 'SELECT pg_is_in_recovery()')" = t
        "$BASE/bin/pg_ctl" promote -D "$CASE/data" -w -t 20
    fi
    test "$(sql 'SELECT pg_is_in_recovery()')" = f
    test "$(sql 'SELECT count(*) FROM data')" = 5
    sql 'CREATE TABLE online_crash_write (id integer); INSERT INTO online_crash_write VALUES (1)'
    stop
    echo "promotion-online-crash: $point PASS"
done
if [ "${LIVE_CHILD_ADOPTION:-0}" = 1 ]; then
    for point in after-live-child-xid-floor after-live-child-runtime after-retire-pending after-child-primary-ready; do
        clone "live-$point"
        start
        checked "(setf *physical-promotion-test-hook*
          (lambda (point)
            (when (eq point :before-online-intent)
              (setf (symbol-function (quote physical-promotion-start-child-primary))
                    (function physical-promotion-start-live-child-primary)))
            (when (eq point :$point)
              (with-open-file (stream \"$CASE/marker\" :direction :output
                                      :if-exists :error :if-does-not-exist :create)
                (format stream \"~d~%\" (sb-posix:getpid))
                (finish-output stream))
              (loop (sleep 0.05)))))" live-hook
        "$BASE/bin/pg_ctl" promote -D "$CASE/data" -W
        barrier
        crash
        start
        test "$(sql 'SELECT pg_is_in_recovery()')" = f
        test "$(sql 'SELECT count(*) FROM data')" = 5
        sql 'CREATE TABLE live_crash_write (id integer); INSERT INTO live_crash_write VALUES (1)'
        stop
        echo "promotion-online-crash: live $point PASS"
    done
fi
for kind in stale queued; do
    clone "startup-$kind"
    : >"$CASE/data/promote"
    PG_PROMOTION_DATA="$CASE/data" PG_PROMOTION_PORT="$PORT" PG_PROMOTION_SOCKET="$CASE/control.sock" \
        PG_PROMOTION_PAUSE=after-promotion-request-initialization PG_PROMOTION_MARKER="$CASE/marker" \
        PG_PROMOTION_RELEASE="$CASE/release" "$SBCL" --script "$BASE/t/promotion-online-native.lisp" >"$CASE/server.log" 2>&1 &
    SERVER=$!
    barrier
    test ! -e "$CASE/data/promote"
    if [ "$kind" = queued ]; then : >"$CASE/data/promote"; fi
    kill -USR1 "$SERVER"
    : >"$CASE/release"
    test_wait_owned_sql_server "$SERVER" "$PORT" "$PSQL"
    if [ "$kind" = stale ]; then
        checked '(assert (and (recovery-in-progress-p) (not *physical-promotion-stopping-p*)))' stale-check
    else
        attempt=0
        while [ "$(sql 'SELECT pg_is_in_recovery()')" != f ]; do
            kill -0 "$SERVER"; attempt=$((attempt + 1)); test "$attempt" -lt 200; sleep 0.1
        done
    fi
    stop
    echo "promotion-online-crash: startup-$kind PASS"
done
clone incomplete-batch
start
checked '(let* ((cursor (physical-store-flushed-lsn *physical-receive-store*)) (bytes (wal-encode-record +wal-record-batch-start+ 0 cursor #()))) (physical-recovery-receive cursor bytes) (physical-recovery-flush))' incomplete-batch
"$BASE/bin/pg_ctl" promote -D "$CASE/data" -w -t 20
test "$(sql 'SELECT count(*) FROM data')" = 5
test "$(sql 'SELECT pg_is_in_recovery()')" = f
stop
echo 'promotion-online-crash: process boundaries, startup signal handling and unfinished receive batch PASS'
