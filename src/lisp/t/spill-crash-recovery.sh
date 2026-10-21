#!/bin/sh
# t/spill-crash-recovery.sh -- reclaim abandoned executor spill files on startup.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
PORT=${PORT:-5486}
CONTROL_PORT=${CONTROL_PORT:-$((PORT + 1))}
SBCL=${SBCL:-sbcl}
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
        -U postgres -d postgres -v ON_ERROR_STOP=1 -v VERBOSITY=verbose "$@"
}

control() {
    { cat; printf '%s\n' '(quit)'; } \
        | timeout 25 "$SBCL" --script "$BASE/t/control-client.lisp" \
            tcp "127.0.0.1:$CONTROL_PORT"
}

control_eval() {
    printf '%s\n' "$1" | control
}

control_number() {
    marker=$1
    form=$2
    output=$(control_eval "$form") || fail "control REPL form failed: $form"
    value=$(printf '%s\n' "$output" \
        | sed -n "s/.*${marker}=\\([0-9][0-9]*\\).*/\\1/p" | tail -n 1)
    [ -n "$value" ] || fail "control REPL did not report $marker: $output"
    printf '%s\n' "$value"
}

start_server() {
    mode=${1:-}
    limit=${2:-}
    if [ "$mode" = pause ]; then
        PG_QUERY_TEMP_TEST_READY_FILE="$TMP/spill-ready" \
            PG_QUERY_TEMP_TEST_RELEASE_FILE="$TMP/spill-release" \
            "$BASE/pg-lisp.sh" -p "$PORT" -D "$PGDATA" \
            --control-port "$CONTROL_PORT" \
            >> "$LOG" 2>&1 &
    elif [ "$mode" = limited ]; then
        "$BASE/pg-lisp.sh" -p "$PORT" -D "$PGDATA" \
            --temp-file-limit-bytes "$limit" \
            --control-port "$CONTROL_PORT" >> "$LOG" 2>&1 &
    else
        "$BASE/pg-lisp.sh" -p "$PORT" -D "$PGDATA" \
            --control-port "$CONTROL_PORT" >> "$LOG" 2>&1 &
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

kill -TERM "$SRV" 2>/dev/null || true
wait "$SRV" 2>/dev/null || true
SRV=
start_server

run -c "create table spill_quota_outer (k int); create table spill_quota_inner (k int); insert into spill_quota_outer values (10); insert into spill_quota_inner values (10), (20)" >/dev/null || fail "could not prepare FULL JOIN quota input"
control <<'LISP' >/dev/null
(setf pg::*query-work-memory-bytes* 1 pg::*query-temp-file-limit-bytes* nil)
(defparameter pg::*spill_test_match_count* 0)
(defparameter pg::*spill_test_match_base* 0)
(let ((original (symbol-function 'pg::query-match-map-spill)))
  (setf (symbol-function 'pg::query-match-map-spill)
        (lambda (match-map)
          (incf pg::*spill_test_match_count*)
          (setf pg::*spill_test_match_base*
                (pg::query-temp-quota-used-bytes pg::*query-temp-quota*))
          (funcall original match-map))))
LISP
run -c "select o.k, i.k from spill_quota_outer o full join spill_quota_inner i on o.k = i.k" > "$TMP/full-join.out" 2>&1 || fail "unlimited FULL JOIN did not complete"
full_rows=$(wc -l < "$TMP/full-join.out" | tr -d ' ')
[ "$full_rows" = 2 ] || fail "FULL JOIN baseline returned $full_rows rows instead of 2"
match_base=$(control_number MATCH_BASE '(format nil "MATCH_BASE=~d" pg::*spill_test_match_base*)')
match_calls=$(control_number MATCH_COUNT '(format nil "MATCH_COUNT=~d" pg::*spill_test_match_count*)')
[ "$match_calls" = 1 ] && [ "$match_base" -gt 0 ] || fail "FULL JOIN baseline did not record one match-map spill with a row-spool baseline"
control_eval "(setf pg::*query-temp-file-limit-bytes* $((match_base + 1)) pg::*spill_test_match_count* 0)" >/dev/null
if run -c "select o.k, i.k from spill_quota_outer o full join spill_quota_inner i on o.k = i.k" > "$TMP/full-join-limit.out" 2>&1; then
    fail "FULL JOIN match-map spill exceeded no quota"
fi
grep -Fq '53400' "$TMP/full-join-limit.out" || fail "FULL JOIN quota failure did not report SQLSTATE 53400"
match_calls=$(control_number MATCH_COUNT '(format nil "MATCH_COUNT=~d" pg::*spill_test_match_count*)')
[ "$match_calls" = 1 ] || fail "FULL JOIN quota did not fail after entering the binary match-map spill path"
spill_file=$(find "$PGDATA/pgsql_tmp" -type f -name 'pg-query-*.tmp' -print -quit)
[ -z "$spill_file" ] || fail "FULL JOIN quota error left temporary files behind"
control_eval '(setf pg::*query-temp-file-limit-bytes* nil)' >/dev/null

run -c "create table spill_merge_rows (id int); insert into spill_merge_rows values $(awk 'BEGIN { for (i = 1; i <= 16; i++) printf "%s(%d)", i == 1 ? "" : ",", i; print "" }')" >/dev/null || fail "could not prepare external merge quota input"
control <<'LISP' >/dev/null
(defparameter pg::*spill_test_merge_count* 0)
(defparameter pg::*spill_test_merge_high_water* 0)
(let ((original (symbol-function 'pg::merge-query-sort-runs)))
  (setf (symbol-function 'pg::merge-query-sort-runs)
        (lambda (left-path right-path order)
          (incf pg::*spill_test_merge_count*)
          (funcall original left-path right-path order))))
(let ((original (symbol-function 'pg::query-temp-account-space)))
  (setf (symbol-function 'pg::query-temp-account-space)
        (lambda (pathname bytes &optional stream)
          (prog1 (funcall original pathname bytes stream)
            (when (<= 1 pg::*spill_test_merge_count* 12)
              (setf pg::*spill_test_merge_high_water*
                    (max pg::*spill_test_merge_high_water*
                         (pg::query-temp-quota-used-bytes pg::*query-temp-quota*))))))))
LISP
run -c "select id from spill_merge_rows order by id" > "$TMP/merge-baseline.out" 2>&1 || fail "unlimited external merge query did not complete"
merge_rows=$(wc -l < "$TMP/merge-baseline.out" | tr -d ' ')
[ "$merge_rows" = 16 ] || fail "external merge baseline returned $merge_rows rows instead of 16"
merge_calls=$(control_number MERGE_COUNT '(format nil "MERGE_COUNT=~d" pg::*spill_test_merge_count*)')
merge_limit=$(control_number MERGE_HIGH_WATER '(format nil "MERGE_HIGH_WATER=~d" pg::*spill_test_merge_high_water*)')
[ "$merge_calls" = 15 ] && [ "$merge_limit" -gt 0 ] || fail "external merge baseline did not complete the expected 15 merges"
control_eval "(setf pg::*query-temp-file-limit-bytes* $merge_limit pg::*spill_test_merge_count* 0 pg::*spill_test_merge_high_water* 0)" >/dev/null
if run -c "select id from spill_merge_rows order by id" > "$TMP/merge-limit.out" 2>&1; then
    fail "external merge quota did not fail after the calibrated passes"
fi
grep -Fq '53400' "$TMP/merge-limit.out" || fail "external merge quota failure did not report SQLSTATE 53400"
merge_calls=$(control_number MERGE_COUNT '(format nil "MERGE_COUNT=~d" pg::*spill_test_merge_count*)')
[ "$merge_calls" = 13 ] || fail "external merge quota failed at merge $merge_calls instead of merge 13"
spill_file=$(find "$PGDATA/pgsql_tmp" -type f -name 'pg-query-*.tmp' -print -quit)
[ -z "$spill_file" ] || fail "external merge quota error left temporary files behind"
count_after_merge_limit=$(run -c "select count(*) from spill_merge_rows")
[ "$count_after_merge_limit" = 16 ] || fail "server stopped serving queries after external merge quota failure"

echo "spill-crash-recovery: PASS (reclaimed $crashed_spill_file; $count rows survived; sort, FULL JOIN and merge quotas enforced)"
