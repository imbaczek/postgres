#!/bin/sh
# t/index-build-contention.sh -- relation-gated bulk build and atomic publish.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
SBCL=${SBCL:-sbcl}
PORT=${PORT:-5479}
CONTROL_PORT=${CONTROL_PORT:-$((PORT + 2000))}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-index-build.XXXXXX")
LOG="$TMP/server.log"
SRV=
BUILDER=
SAME_WRITER=
OTHER_WRITER=
RACE_BUILDER=
CRASH_BUILDER=
PUBLICATION_RELFILE=
DURABLE_RELFILE=
. "$BASE/t/test-support.sh"
test_debug_setup index-build-contention "$PORT" "$TMP" "$LOG"

cleanup() {
    : > "$TMP/bulk-build.release"
    : > "$TMP/name-race.release"
    : > "$TMP/crash-build.release"
    for pid in "$SAME_WRITER" "$OTHER_WRITER" "$BUILDER" \
            "$RACE_BUILDER" "$CRASH_BUILDER"; do
        [ -z "$pid" ] || kill "$pid" 2>/dev/null || true
    done
    [ -z "$SRV" ] || kill -KILL "$SRV" 2>/dev/null || true
    for pid in "$SAME_WRITER" "$OTHER_WRITER" "$BUILDER" \
            "$RACE_BUILDER" "$CRASH_BUILDER"; do
        [ -z "$pid" ] || wait "$pid" 2>/dev/null || true
    done
    [ -z "$SRV" ] || wait "$SRV" 2>/dev/null || true
    rm -rf "$TMP"
}
test_debug_install_traps cleanup

fail() {
    echo "index-build-contention: FAIL $1" >&2
    for output in "$TMP"/*.out; do
        [ -f "$output" ] || continue
        echo "--- $output" >&2
        sed -n '1,100p' "$output" >&2
    done
    [ ! -f "$LOG" ] || cat "$LOG" >&2
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
    while [ "$i" -lt 250 ]; do
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

run -c "create table build_gate_a (id int, category int)" >/dev/null
run -c "create table build_gate_other (id int)" >/dev/null
awk 'BEGIN {
    printf "insert into build_gate_a values "
    for (i = 1; i <= 2000; i++)
        printf "%s(%d, %d)", i == 1 ? "" : ", ", i, i % 10
    print ";"
}' > "$TMP/populate.sql"
run < "$TMP/populate.sql" > "$TMP/populate.out" 2>&1 \
    || fail "could not populate build table"

install_output=$(control <<EOF
(defvar *index-build-test-original-entries* nil)
(defvar *index-build-test-original-writer* nil)
(defvar *index-build-test-original-wait* nil)
(defvar *index-build-test-heap* nil)
(defvar *index-build-test-writer-heap* nil)
(defvar *index-build-test-writer-xid* nil)
(defvar *index-build-test-label* nil)
(defvar *index-build-test-paused* nil)
(let* ((db (gethash "postgres" (catalog-databases *catalog*)))
       (table (gethash "build_gate_a" (database-tables db))))
  (unless table (error "build test table is missing"))
  (setf *index-build-test-heap* (pg-class-relfilenode table)
        *index-build-test-writer-heap* (pg-class-relfilenode table)))
(setf *index-build-test-original-entries* (symbol-function 'build-index-entries)
      *index-build-test-original-writer* (symbol-function 'register-relation-writer)
      *index-build-test-original-wait* (symbol-function 'condition-wait-with-timeout))
(setf (symbol-function 'build-index-entries)
      (lambda (&rest args)
        (let ((table (second args)))
          (when (and *index-build-test-label*
                     (not *index-build-test-paused*)
                     (= (pg-class-relfilenode table) *index-build-test-heap*))
            (setf *index-build-test-paused* t)
            (with-open-file (out (format nil "$TMP/~a.ready" *index-build-test-label*)
                                 :direction :output :if-exists :supersede)
              (write-string "ready" out))
              (with-open-file (out
                               (format nil "$TMP/~a.relfilenode"
                                       *index-build-test-label*)
                               :direction :output :if-exists :supersede)
                (format out "~a" (pg-index-relfilenode (first args))))
            (loop repeat 6000
                  until (probe-file
                         (format nil "$TMP/~a.release" *index-build-test-label*))
                  do (sleep 0.01)))
        (apply *index-build-test-original-entries* args))))
(setf (symbol-function 'register-relation-writer)
      (lambda (relfilenode xid)
        (when (and *index-build-test-writer-heap*
                   (= relfilenode *index-build-test-writer-heap*))
          (setf *index-build-test-writer-xid* xid)
          (with-open-file (out "$TMP/same-writer-attempting"
                               :direction :output :if-exists :supersede)
            (write-string "attempting" out)))
        (funcall *index-build-test-original-writer* relfilenode xid)))
(setf (symbol-function 'condition-wait-with-timeout)
      (lambda (waitqueue mutex)
        (when (and *backend* *index-build-test-writer-xid*
                   (eql mutex *relation-lock-mutex*)
                   (eql (backend-xid *backend*) *index-build-test-writer-xid*))
          (with-open-file (out "$TMP/same-writer-waiting"
                               :direction :output :if-exists :supersede)
            (write-string "waiting" out)))
        (funcall *index-build-test-original-wait* waitqueue mutex)))
(setf *index-build-test-label* "bulk-build")
(quit)
EOF
) || fail "could not install index-build scheduler"
echo "$install_output" | grep -Eq ';; (read )?error:' \
    && fail "index-build scheduler setup failed"

(run -c "create index build_gate_category_idx on build_gate_a (category)" \
    > "$TMP/build.out" 2>&1 && : > "$TMP/build.done") &
BUILDER=$!
wait_for_file "$TMP/bulk-build.ready"

plan=$(run -c 'explain select id from build_gate_a where category = 3')
[ "$plan" = "Seq Scan on build_gate_a" ] \
    || fail "an unfinished index build became visible: $plan"
publication_output=$(control <<'EOF'
(let ((db (gethash "postgres" (catalog-databases *catalog*))))
  (if (gethash "build_gate_category_idx" (database-indexes db))
      :PUBLISHED
      :NOT-PUBLISHED))
(quit)
EOF
) || fail "could not inspect the in-progress index catalog entry"
echo "$publication_output" | grep -q ':NOT-PUBLISHED' \
    || fail "an unfinished index build was already in the catalog"
echo "$publication_output" | grep -Eq ';; (read )?error:' \
    && fail "in-progress index publication inspection failed"
(
    run -c 'insert into build_gate_a values (9999, 9)' \
        > "$TMP/same-writer.out" 2>&1 && : > "$TMP/same-writer.done"
) &
SAME_WRITER=$!
wait_for_file "$TMP/same-writer-waiting"
(
    run -c 'insert into build_gate_other values (1)' \
        > "$TMP/other-writer.out" 2>&1 && : > "$TMP/other-writer.done"
) &
OTHER_WRITER=$!
wait_for_file "$TMP/other-writer.done"
[ ! -f "$TMP/build.done" ] \
    || fail "index build finished before its release hook"
[ ! -f "$TMP/same-writer.done" ] \
    || fail "a writer on the build table bypassed its relation gate"

: > "$TMP/bulk-build.release"
wait_for_file "$TMP/build.done"
wait_for_file "$TMP/same-writer.done"
wait "$BUILDER" || fail "populated index build failed"
BUILDER=
wait "$SAME_WRITER" || fail "same-table writer failed after build"
SAME_WRITER=
wait "$OTHER_WRITER" || fail "unrelated writer failed"
OTHER_WRITER=
plan=$(run -c 'explain select id from build_gate_a where category = 3')
[ "$plan" = "Index Scan using build_gate_category_idx on build_gate_a" ] \
    || fail "published index was not selected: $plan"
run -c 'select id from build_gate_a where category = 9 order by id' \
    > "$TMP/indexed.out" || fail "indexed query failed"
run -c "drop index build_gate_category_idx" >/dev/null \
    || fail "could not drop built index"
run -c 'select id from build_gate_a where category = 9 order by id' \
    > "$TMP/sequential.out" || fail "sequential query failed"
if ! cmp -s "$TMP/indexed.out" "$TMP/sequential.out"; then
    diff -u "$TMP/sequential.out" "$TMP/indexed.out" >&2 || true
    fail "index results differed after the gated writer completed"
fi

run -c "create table build_race_left (id int, category int)" >/dev/null
run -c "create table build_race_right (id int, category int)" >/dev/null
run -c "insert into build_race_left values (1, 1)" >/dev/null
run -c "insert into build_race_right values (2, 2)" >/dev/null
race_setup=$(control <<EOF
(let* ((db (gethash "postgres" (catalog-databases *catalog*)))
       (table (gethash "build_race_left" (database-tables db))))
  (setf *index-build-test-heap* (pg-class-relfilenode table)
        *index-build-test-writer-heap* nil
        *index-build-test-label* "name-race"
        *index-build-test-paused* nil))
(quit)
EOF
) || fail "could not configure index-name race"
echo "$race_setup" | grep -Eq ';; (read )?error:' \
    && fail "index-name race setup failed"

(run -c "create index shared_race_idx on build_race_left (category)" \
    > "$TMP/race-first.out" 2>&1 && : > "$TMP/race-first.done") &
RACE_BUILDER=$!
wait_for_file "$TMP/name-race.ready"
run -c "create index shared_race_idx on build_race_right (category)" \
    > "$TMP/race-second.out" 2>&1 \
    || fail "the competing index name could not be published"
plan=$(run -c 'explain select id from build_race_right where category = 2')
[ "$plan" = "Index Scan using shared_race_idx on build_race_right" ] \
    || fail "the published same-name index belongs to the wrong table: $plan"
: > "$TMP/name-race.release"
if run -c "create index shared_race_idx on build_race_left (category)" \
    > "$TMP/race-retry.out" 2>&1; then
    fail "CREATE INDEX did not reject the name published during its build"
fi
wait "$RACE_BUILDER" || true
RACE_BUILDER=
grep -q 'already exists' "$TMP/race-first.out" \
    || fail "the first builder did not report the publication-time name conflict"
plan=$(run -c 'explain select id from build_race_left where category = 1')
[ "$plan" = "Seq Scan on build_race_left" ] \
    || fail "the losing builder left an index attached to its table: $plan"

restore_output=$(control <<'EOF'
(setf (symbol-function 'register-relation-writer) *index-build-test-original-writer*
      (symbol-function 'condition-wait-with-timeout) *index-build-test-original-wait*)
(quit)
EOF
) || fail "could not restore index-build instrumentation"
echo "$restore_output" | grep -Eq ';; (read )?error:' \
    && fail "index-build instrumentation restore failed"

# Fail before catalog persistence after the index has entered the in-memory
# catalog. The rollback removes the metadata but retains the relfile for startup
# orphan cleanup.
run -c "create table build_failure_a (id int, category int)" >/dev/null
run -c "insert into build_failure_a values (1, 3)" >/dev/null
run -c "create table build_failure_b (id int, category int)" >/dev/null
run -c "insert into build_failure_b values (1, 3)" >/dev/null
failure_setup=$(control <<EOF
(defvar *index-publication-failure-original-save* nil)
(defvar *index-publication-failure-name* "publication_failure_idx")
(defvar *index-publication-failure-phase* :before)
(defvar *index-publication-failure-relfilenode* nil)
(defvar *index-publication-failure-fired* nil)
(setf *index-publication-failure-original-save* (symbol-function 'save-catalog)
      *index-publication-failure-fired* nil)
(setf (symbol-function 'save-catalog)
      (lambda (&rest args)
        (let* ((db (gethash "postgres" (catalog-databases *catalog*)))
               (idx (gethash *index-publication-failure-name*
                             (database-indexes db))))
          (if (and idx (not *index-publication-failure-fired*))
              (progn
                (setf *index-publication-failure-fired* t
                      *index-publication-failure-relfilenode*
                      (pg-index-relfilenode idx))
                (with-open-file (out
                                 (if (eq *index-publication-failure-phase* :before)
                                     "$TMP/publication-failure.relfilenode"
                                     "$TMP/durable-failure.relfilenode")
                                     :direction :output :if-exists :supersede)
                  (format out "~a" *index-publication-failure-relfilenode*))
                (when (eq *index-publication-failure-phase* :before)
                  (error "injected pre-durable catalog save failure"))
                (prog1 (apply *index-publication-failure-original-save* args)
                  (when (eq *index-publication-failure-phase* :after)
                    (error "injected post-durable catalog save failure"))))
              (apply *index-publication-failure-original-save* args)))))
(quit)
EOF
) || fail "could not install publication failure hook"
echo "$failure_setup" | grep -Eq ';; (read )?error:' \
    && fail "publication failure hook setup failed"
if run -c "create index publication_failure_idx on build_failure_a (category)" \
        > "$TMP/publication-failure.out" 2>&1; then
    fail "injected catalog failure did not abort CREATE INDEX"
fi
grep -q 'injected pre-durable catalog save failure' "$TMP/publication-failure.out" \
    || fail "CREATE INDEX did not report the injected catalog failure"
failure_output=$(control <<'EOF'
(let ((db (gethash "postgres" (catalog-databases *catalog*))))
  (if (and *index-publication-failure-fired*
           (null (gethash "publication_failure_idx" (database-indexes db))))
      :PUBLICATION-ROLLED-BACK
      :PUBLICATION-REMAINS))
(quit)
EOF
) || fail "could not inspect failed index publication"
echo "$failure_output" | grep -q ':PUBLICATION-ROLLED-BACK' \
    || fail "failed index publication remained in the in-memory catalog"
echo "$failure_output" | grep -Eq ';; (read )?error:' \
    && fail "failed index publication inspection failed"
PUBLICATION_RELFILE=$(cat "$TMP/publication-failure.relfilenode")
[ -e "$TMP/data/base/$PUBLICATION_RELFILE" ] \
    || fail "publication rollback removed the relfile before recovery"
plan=$(run -c 'explain select id from build_failure_a where category = 3')
[ "$plan" = "Seq Scan on build_failure_a" ] \
    || fail "failed index publication remained usable by SELECT"
restore_failure_output=$(control <<'EOF'
(setf *index-publication-failure-name* "durable_failure_idx"
      *index-publication-failure-phase* :after
      *index-publication-failure-fired* nil)
(quit)
EOF
) || fail "could not configure post-durable publication failure"
echo "$restore_failure_output" | grep -Eq ';; (read )?error:' \
    && fail "post-durable publication failure setup failed"

if run -c "create index durable_failure_idx on build_failure_b (category)" \
        > "$TMP/durable-failure.out" 2>&1; then
    fail "injected post-durable failure did not abort CREATE INDEX"
fi
grep -q 'injected post-durable catalog save failure' "$TMP/durable-failure.out" \
    || fail "CREATE INDEX did not report the post-durable failure"
failure_output=$(control <<'EOF'
(let ((db (gethash "postgres" (catalog-databases *catalog*))))
  (if (and *index-publication-failure-fired*
           (gethash "durable_failure_idx" (database-indexes db)))
      :DURABLE-PUBLICATION-REMAINS
      :DURABLE-PUBLICATION-MISSING))
(quit)
EOF
) || fail "could not inspect post-durable index publication"
echo "$failure_output" | grep -q ':DURABLE-PUBLICATION-REMAINS' \
    || fail "durably published index was removed from the in-memory catalog"
echo "$failure_output" | grep -Eq ';; (read )?error:' \
    && fail "post-durable publication inspection failed"
DURABLE_RELFILE=$(cat "$TMP/durable-failure.relfilenode")
plan=$(run -c 'explain select id from build_failure_b where category = 3')
[ "$plan" = "Index Scan using durable_failure_idx on build_failure_b" ] \
    || fail "durably published index was not usable after the reported failure: $plan"
run -c 'insert into build_failure_b values (2, 3)' >/dev/null \
    || fail "writer failed to maintain durably published index"
run -c 'select id from build_failure_b where category = 3 order by id' \
    > "$TMP/durable-query.out" || fail "durably published index query failed"
printf '1\n2\n' > "$TMP/durable-expected.out"
cmp -s "$TMP/durable-expected.out" "$TMP/durable-query.out" \
    || fail "durably published index returned incorrect live results"

restore_failure_output=$(control <<'EOF'
(setf (symbol-function 'save-catalog)
      *index-publication-failure-original-save*)
(quit)
EOF
) || fail "could not restore catalog persistence hook"
echo "$restore_failure_output" | grep -Eq ';; (read )?error:' \
    && fail "catalog persistence hook restore failed"

# Crash after OID reservation and btree initialization, before any index entry
# is built. Startup recovery must discard the unpublished relation file.
crash_setup=$(control <<EOF
(let* ((db (gethash "postgres" (catalog-databases *catalog*)))
       (table (gethash "build_gate_a" (database-tables db))))
  (setf *index-build-test-heap* (pg-class-relfilenode table)
        *index-build-test-writer-heap* nil
        *index-build-test-label* "crash-build"
        *index-build-test-paused* nil))
(quit)
EOF
) || fail "could not configure detached-build crash test"
echo "$crash_setup" | grep -Eq ';; (read )?error:' \
    && fail "detached-build crash setup failed"

(run -c "create index crash_recovery_idx on build_gate_a (category)" \
    > "$TMP/crash-build.out" 2>&1 && : > "$TMP/crash-build.done") &
CRASH_BUILDER=$!
wait_for_file "$TMP/crash-build.ready"
crash_relfilenode=$(cat "$TMP/crash-build.relfilenode")
[ -e "$TMP/data/base/$crash_relfilenode" ] \
    || fail "detached index relation file was not created before the crash"
kill -KILL "$SRV" 2>/dev/null || true
wait "$SRV" 2>/dev/null || true
SRV=
wait "$CRASH_BUILDER" 2>/dev/null || true
CRASH_BUILDER=

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
[ "$i" -lt 150 ] || fail "server did not restart after detached-build crash"
[ ! -e "$TMP/data/base/$PUBLICATION_RELFILE" ] \
    || fail "startup recovery left the failed-publication index relation file"
[ ! -e "$TMP/data/base/$crash_relfilenode" ] \
    || fail "startup recovery left the unpublished index relation file"
[ -e "$TMP/data/base/$DURABLE_RELFILE" ] \
    || fail "startup recovery removed the durable index relation file"
plan=$(run -c 'explain select id from build_gate_a where category = 3')
[ "$plan" = "Seq Scan on build_gate_a" ] \
    || fail "an interrupted index build became visible: $plan"
plan=$(run -c 'explain select id from build_failure_b where category = 3')
[ "$plan" = "Index Scan using durable_failure_idx on build_failure_b" ] \
    || fail "restart recovery lost the durable index: $plan"
run -c 'select id from build_failure_b where category = 3 order by id' \
    > "$TMP/recovered-durable-query.out" \
    || fail "durable index query failed after restart"
cmp -s "$TMP/durable-expected.out" "$TMP/recovered-durable-query.out" \
    || fail "durable index recovery returned incorrect results"

echo "index-build-contention: PASS"
