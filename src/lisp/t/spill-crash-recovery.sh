#!/bin/sh
# t/spill-crash-recovery.sh -- reclaim abandoned executor spill files on startup.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
PORT=${PORT:-5486}
ROWS=${ROWS:-30000}
BATCH=${BATCH:-1000}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-spill-crash.XXXXXX")
PGDATA="$TMP/data"
LOG="$TMP/server.log"
SRV=
QUERY=
. "$BASE/t/test-support.sh"
test_debug_setup spill-crash-recovery "$PORT" "$TMP" "$LOG"

cleanup() {
    : > "$TMP/spill-release"
    [ -z "$QUERY" ] || kill "$QUERY" 2>/dev/null || true
    [ -z "$SRV" ] || kill "$SRV" 2>/dev/null || true
    [ -z "$QUERY" ] || wait "$QUERY" 2>/dev/null || true
    [ -z "$SRV" ] || wait "$SRV" 2>/dev/null || true
    rm -rf "$TMP"
}
test_debug_install_traps cleanup

fail() {
    echo "spill-crash-recovery: FAIL $1" >&2
    [ ! -f "$TMP/limited.out" ] || cat "$TMP/limited.out" >&2
    [ ! -f "$LOG" ] || cat "$LOG" >&2
    exit 1
}

run() {
    PGPASSWORD=x psql -X -q -A -t -h 127.0.0.1 -p "$PORT" \
        -U postgres -d postgres -v ON_ERROR_STOP=1 "$@"
}

start_server() {
    mode=${1:-}
    limit=${2:-}
    if [ "$mode" = pause ]; then
        PG_QUERY_TEMP_TEST_READY_FILE="$TMP/spill-ready" \
            PG_QUERY_TEMP_TEST_RELEASE_FILE="$TMP/spill-release" \
            "$BASE/pg-lisp.sh" -p "$PORT" -D "$PGDATA" \
            >> "$LOG" 2>&1 &
    elif [ "$mode" = limited ]; then
        "$BASE/pg-lisp.sh" -p "$PORT" -D "$PGDATA" \
            --temp-file-limit-bytes "$limit" >> "$LOG" 2>&1 &
    else
        "$BASE/pg-lisp.sh" -p "$PORT" -D "$PGDATA" >> "$LOG" 2>&1 &
    fi
    SRV=$!
    i=0
    while [ "$i" -lt 150 ]; do
        if run -c '' >/dev/null 2>&1; then
            return 0
        fi
        if ! kill -0 "$SRV" 2>/dev/null; then
            fail "server exited during startup"
        fi
        i=$((i + 1))
        sleep 0.2
    done
    fail "server did not start"
}

wait_for_spill() {
    i=0
    while [ "$i" -lt 3000 ]; do
        [ ! -f "$TMP/spill-ready" ] || return 0
        if ! kill -0 "$SRV" 2>/dev/null; then
            fail "server exited before writing a spill record"
        fi
        if [ -n "$QUERY" ] && ! kill -0 "$QUERY" 2>/dev/null; then
            fail "sort query exited before creating a spill file"
        fi
        i=$((i + 1))
        sleep 0.02
    done
    fail "timed out waiting for the spill-write marker"
}

start_server pause
run -c "create table spill_crash_rows (id int)" >/dev/null
awk -v rows="$ROWS" -v batch="$BATCH" 'BEGIN {
    for (i = 1; i <= rows; i++) {
        if ((i - 1) % batch == 0) printf "insert into spill_crash_rows values "
        printf "%s(%d)", (i - 1) % batch == 0 ? "" : ", ", i
        if (i % batch == 0 || i == rows) print ";"
    }
}' > "$TMP/populate.sql"
run < "$TMP/populate.sql" > "$TMP/populate.out" 2>&1 \
    || fail "could not populate sort input"

run -c "select id from spill_crash_rows order by id" \
    > "$TMP/sort.out" 2>&1 &
QUERY=$!
wait_for_spill
spill_file=$(find "$PGDATA/pgsql_tmp" -type f -name 'pg-query-*.tmp' -print -quit)
[ -n "$spill_file" ] && [ -s "$spill_file" ] \
    || fail "external sort did not leave a nonempty spill file before the crash"
crashed_spill_file=$spill_file

kill -KILL "$SRV" 2>/dev/null || true
wait "$SRV" 2>/dev/null || true
SRV=
kill "$QUERY" 2>/dev/null || true
wait "$QUERY" 2>/dev/null || true
QUERY=

start_server
stale_namespace=$(find "$PGDATA/pgsql_tmp" -mindepth 1 -maxdepth 1 \
    -type d -name 'pgsql_tmp-*' -print -quit)
[ -z "$stale_namespace" ] \
    || fail "startup left abandoned spill namespace $stale_namespace"
count=$(run -c "select count(*) from spill_crash_rows")
[ "$count" = "$ROWS" ] || fail "committed rows changed across crash recovery"

kill -TERM "$SRV" 2>/dev/null || true
wait "$SRV" 2>/dev/null || true
SRV=
start_server limited 1
if run -c "select id from spill_crash_rows order by id" \
        > "$TMP/limited.out" 2>&1; then
    fail "configured temporary file limit did not reject the spilling sort"
fi
grep -Fq 'temporary file size exceeds temp_file_limit' "$TMP/limited.out" \
    || fail "spill limit error did not report the configured quota failure"
spill_file=$(find "$PGDATA/pgsql_tmp" -type f -name 'pg-query-*.tmp' -print -quit)
[ -z "$spill_file" ] || fail "spill-limit error left temporary files behind"
count_after_limit=$(run -c "select count(*) from spill_crash_rows")
[ "$count_after_limit" = "$ROWS" ] \
    || fail "server stopped serving queries after a temp-file-limit error"

echo "spill-crash-recovery: PASS (reclaimed $crashed_spill_file; $count rows survived; limit enforced)"
