#!/bin/sh
# t/vacuum-update-concurrency.sh -- ordinary VACUUM with same-page writers.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
SBCL=${SBCL:-sbcl}
PORT=${PORT:-5475}
CONTROL_PORT=${CONTROL_PORT:-$((PORT + 2000))}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-vacuum-update.XXXXXX")
LOG="$TMP/server.log"
SRV=
VACUUM_PID=
WRITER1=
WRITER2=
WATCHDOG_PID=
. "$BASE/t/test-support.sh"
test_debug_setup vacuum-update-concurrency "$PORT" "$TMP" "$LOG"

cleanup() {
    : > "$TMP/page-retry.release"
    for pid in "$WRITER1" "$WRITER2" "$VACUUM_PID" "$WATCHDOG_PID"; do
        [ -z "$pid" ] || kill "$pid" 2>/dev/null || true
    done
    for pid in "$WRITER1" "$WRITER2" "$VACUUM_PID" "$WATCHDOG_PID"; do
        [ -z "$pid" ] || wait "$pid" 2>/dev/null || true
    done
    if [ -n "$SRV" ]; then
        kill -TERM "$SRV" 2>/dev/null || true
        wait "$SRV" 2>/dev/null || true
    fi
    rm -rf "$TMP"
}
test_debug_install_traps cleanup

fail() {
    echo "vacuum-update-concurrency: FAIL $1" >&2
    for output in "$TMP"/*.out; do
        [ -f "$output" ] || continue
        echo "--- $output" >&2
        sed -n '1,80p' "$output" >&2
    done
    [ -f "$LOG" ] && cat "$LOG" >&2
    exit 1
}

run() {
    PGPASSWORD=x psql -X -q -A -t -h 127.0.0.1 -p "$PORT" \
        -U postgres -d postgres -v ON_ERROR_STOP=1 "$@"
}

control() {
    timeout 25 "$SBCL" --script "$BASE/t/control-client.lisp" \
        tcp "127.0.0.1:$CONTROL_PORT"
}

wait_for_file() {
    file=$1
    i=0
    while [ "$i" -lt 300 ]; do
        [ ! -f "$file" ] || return 0
        i=$((i + 1))
        sleep 0.1
    done
    fail "timed out waiting for $file"
}

test_debug_start_server "$LOG" -p "$PORT" -D "$TMP/data" \
    --control-port "$CONTROL_PORT"
i=0
while [ "$i" -lt 150 ]; do
    if run -c '' >/dev/null 2>&1; then
        break
    fi
    i=$((i + 1))
    sleep 0.2
done
[ "$i" -lt 150 ] || fail "server did not start"

# Keep the fixture on one page. Two rows per writer leave enough space for
# repeated non-HOT updates while still making page conflicts deterministic.
run -c "create table vacuum_writer_race (id int, category int)" >/dev/null
awk 'BEGIN {
    printf "insert into vacuum_writer_race values "
    for (id = 1; id <= 64; id++)
        printf "%s(%d, 0)", id == 1 ? "" : ", ", id
    print ";"
}' > "$TMP/initial.sql"
run < "$TMP/initial.sql" > "$TMP/initial.out" 2>&1 \
    || fail "initial insert failed"
run -c "create index vacuum_writer_race_category_idx on vacuum_writer_race (category)" \
    > "$TMP/index.out" 2>&1 || fail "populated index build failed"
run -c "delete from vacuum_writer_race where id > 62" \
    > "$TMP/delete.out" 2>&1 || fail "dead-TID fixture delete failed"

install_output=$(control <<EOF
(defvar *vacuum-writer-race-heap* nil)
(defvar *vacuum-writer-race-original-publish* nil)
(defvar *vacuum-writer-race-paused* nil)
(defvar *vacuum-writer-race-results* nil)
(let* ((db (gethash "postgres" (catalog-databases *catalog*)))
       (table (gethash "vacuum_writer_race" (database-tables db))))
  (unless table (error "VACUUM writer-race table is missing"))
  (setf *vacuum-writer-race-heap* (pg-class-relfilenode table)))
(setf *vacuum-writer-race-original-publish*
      (symbol-function 'try-vacuum-page-publication)
      *vacuum-writer-race-paused* nil
      *vacuum-writer-race-results* nil
      (symbol-function 'try-vacuum-page-publication)
      (lambda (relfilenode block source-page &rest options)
        (when (and (= relfilenode *vacuum-writer-race-heap*)
                   (= block 0)
                   (not *vacuum-writer-race-paused*))
          (setf *vacuum-writer-race-paused* t)
          (with-open-file (out "$TMP/page-retry.ready"
                               :direction :output :if-exists :supersede)
            (write-string "ready" out))
          (loop until (probe-file "$TMP/page-retry.release")
                do (sleep 0.01)))
        (let ((published
                (apply *vacuum-writer-race-original-publish*
                       relfilenode block source-page options)))
          (when (and (= relfilenode *vacuum-writer-race-heap*) (= block 0))
            (push published *vacuum-writer-race-results*))
          published)))
(quit)
EOF
) || fail "could not install page-retry scheduler"
echo "$install_output" | grep -q 'bye' \
    || fail "page-retry scheduler did not install"
echo "$install_output" | grep -Eq ';; (read )?error:' \
    && fail "page-retry scheduler evaluation failed"

run -c "vacuum vacuum_writer_race" > "$TMP/vacuum.out" 2>&1 &
VACUUM_PID=$!
wait_for_file "$TMP/page-retry.ready"

# C's plain VACUUM takes ShareUpdateExclusiveLock, which permits concurrent
# DML; its page cleanup lock coordinates pruning. This implementation pauses
# before compare/publish so both writers can change the same heap page and make
# VACUUM discard its first page proposal.
awk 'BEGIN {
    for (n = 1; n <= 16; n++)
        print "update vacuum_writer_race set category = category + 1 where id >= 1 and id <= 2;"
}' > "$TMP/writer1.sql"
awk 'BEGIN {
    for (n = 1; n <= 16; n++)
        print "update vacuum_writer_race set category = category + 1 where id >= 3 and id <= 4;"
}' > "$TMP/writer2.sql"
run < "$TMP/writer1.sql" > "$TMP/writer1.out" 2>&1 &
WRITER1=$!
run < "$TMP/writer2.sql" > "$TMP/writer2.out" 2>&1 &
WRITER2=$!
(
    i=0
    while [ "$i" -lt 600 ]; do
        [ ! -f "$TMP/page-retry.release" ] || exit 0
        sleep 0.1
        i=$((i + 1))
    done
    if [ ! -f "$TMP/page-retry.release" ]; then
        : > "$TMP/page-retry.timeout"
        for pid in "$WRITER1" "$WRITER2" "$VACUUM_PID"; do
            [ -z "$pid" ] || kill "$pid" 2>/dev/null || true
        done
    fi
) &
WATCHDOG_PID=$!
wait "$WRITER1" || fail "first same-page writer failed"
WRITER1=
wait "$WRITER2" || fail "second same-page writer failed"
WRITER2=

page_count=$(control <<'EOF'
(let* ((db (gethash "postgres" (catalog-databases *catalog*)))
       (table (gethash "vacuum_writer_race" (database-tables db))))
  (list (buf-nblocks (pg-class-relfilenode table))))
(quit)
EOF
) || fail "could not inspect the writer heap page count"
echo "$page_count" | grep -q '(1)' \
    || fail "writers extended the heap beyond their shared page: $page_count"

if [ -f "$TMP/page-retry.timeout" ]; then
    fail "same-page writers did not finish before the page-retry watchdog"
fi
: > "$TMP/page-retry.release"
wait "$WATCHDOG_PID" || fail "page-retry watchdog failed"
WATCHDOG_PID=
if [ -f "$TMP/page-retry.timeout" ]; then
    fail "same-page writers did not finish before the page-retry watchdog"
fi
wait "$VACUUM_PID" || fail "ordinary VACUUM failed after page conflict"
VACUUM_PID=

retry_output=$(control <<'EOF'
(list (and (> (length *vacuum-writer-race-results*) 1)
           (not (null (member nil *vacuum-writer-race-results*)))
           (not (null (member t *vacuum-writer-race-results*)))))
(setf (symbol-function 'try-vacuum-page-publication)
      *vacuum-writer-race-original-publish*)
(quit)
EOF
) || fail "could not inspect and restore VACUUM page publication"
echo "$retry_output" | grep -q '(T)' \
    || fail "VACUUM did not observe a failed page compare and retry: $retry_output"
echo "$retry_output" | grep -Eq ';; (read )?error:' \
    && fail "VACUUM retry inspection failed"

plan=$(run -c 'explain select id from vacuum_writer_race where category = 15')
[ "$plan" = "Index Scan using vacuum_writer_race_category_idx on vacuum_writer_race" ] \
    || fail "stale index entries were not checked through IndexScan: $plan"
plan=$(run -c 'explain select id from vacuum_writer_race where category = 16')
[ "$plan" = "Index Scan using vacuum_writer_race_category_idx on vacuum_writer_race" ] \
    || fail "post-VACUUM lookup did not use the index: $plan"

capture_results() {
    for category in 0 1 8 15 16; do
        printf 'category=%s\n' "$category"
        run -c "select id from vacuum_writer_race where category = $category order by id" \
            || return 1
    done
}

capture_results > "$TMP/indexed-results.out" \
    || fail "indexed results after concurrent VACUUM failed"
[ "$(run -c 'select id from vacuum_writer_race where category = 16 order by id')" = "1
2
3
4" ] \
    || fail "indexed scan did not find all latest writer versions"
[ -z "$(run -c 'select id from vacuum_writer_race where category = 15')" ] \
    || fail "indexed scan exposed obsolete versions from before VACUUM"

run -c "drop index vacuum_writer_race_category_idx" \
    > "$TMP/drop-index.out" 2>&1 || fail "could not drop index for comparison"
plan=$(run -c 'explain select id from vacuum_writer_race where category = 15')
[ "$plan" = "Seq Scan on vacuum_writer_race" ] \
    || fail "sequential comparison did not use SeqScan: $plan"
capture_results > "$TMP/sequential-results.out" \
    || fail "sequential results after concurrent VACUUM failed"
if ! cmp -s "$TMP/indexed-results.out" "$TMP/sequential-results.out"; then
    echo "vacuum-update-concurrency: indexed/sequential mismatch:" >&2
    diff -u "$TMP/sequential-results.out" "$TMP/indexed-results.out" >&2 || true
    fail "indexed results differed from sequential results after VACUUM"
fi

echo "vacuum-update-concurrency: PASS"
