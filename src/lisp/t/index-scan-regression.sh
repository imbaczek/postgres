#!/bin/sh
# t/index-scan-regression.sh -- differential results and reader/DDL handoff.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
SBCL=${SBCL:-sbcl}
PORT=${PORT:-5472}
CONTROL_PORT=${CONTROL_PORT:-$((PORT + 2000))}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-index-scan.XXXXXX")
LOG="$TMP/server.log"
SRV=
READER=
DROPPER=
. "$BASE/t/test-support.sh"
test_debug_setup index-scan-regression "$PORT" "$TMP" "$LOG"

cleanup() {
    : > "$TMP/drop-handoff.release"
    : > "$TMP/drop-handoff.ddl-release"
    [ -z "$DROPPER" ] || kill "$DROPPER" 2>/dev/null || true
    [ -z "$READER" ] || kill "$READER" 2>/dev/null || true
    [ -z "$SRV" ] || kill "$SRV" 2>/dev/null || true
    [ -z "$DROPPER" ] || wait "$DROPPER" 2>/dev/null || true
    [ -z "$READER" ] || wait "$READER" 2>/dev/null || true
    [ -z "$SRV" ] || wait "$SRV" 2>/dev/null || true
    rm -rf "$TMP"
}
test_debug_install_traps cleanup

fail() {
    echo "index-scan-regression: FAIL $1" >&2
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

wait_for_file() {
    file=$1
    i=0
    while [ "$i" -lt 150 ]; do
        [ ! -f "$file" ] || return 0
        i=$((i + 1))
        sleep 0.1
    done
    fail "timed out waiting for $file"
}

wait_for_marker() {
    file=$1
    marker=$2
    i=0
    while [ "$i" -lt 150 ]; do
        if [ -f "$file" ] && grep -q "$marker" "$file"; then
            return 0
        fi
        i=$((i + 1))
        sleep 0.1
    done
    fail "timed out waiting for $marker"
}

control() {
    timeout 25 "$SBCL" --script "$BASE/t/control-client.lisp" \
        tcp "127.0.0.1:$CONTROL_PORT"
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

# Build a non-unique index over an already populated table, then mutate and
# vacuum it. The deterministic PRNG keeps the workload reproducible while
# generating duplicates, NULLs, updates, and deletes.
run -c "create table index_diff (id int, category int, score int)" >/dev/null
awk 'BEGIN {
    state = 918273645
    printf "insert into index_diff values "
    for (id = 1; id <= 384; id++) {
        state = (state * 48271) % 2147483647
        category = (state % 13 == 0) ? "NULL" : state % 8
        state = (state * 48271) % 2147483647
        score = state % 100
        printf "%s(%d, %s, %d)", id == 1 ? "" : ", ", id, category, score
    }
    print ";"
}' > "$TMP/initial.sql"
run < "$TMP/initial.sql" > "$TMP/initial.out" 2>&1 || fail "initial insert failed"
run -c "create index index_diff_category_idx on index_diff (category)" \
    > "$TMP/build.out" 2>&1 || fail "populated non-unique index build failed"

awk 'BEGIN {
    state = 123456789
    for (n = 1; n <= 64; n++) {
        state = (state * 48271) % 2147483647
        id = state % 384 + 1
        state = (state * 48271) % 2147483647
        category = (state % 7 == 0) ? "NULL" : state % 8
        state = (state * 48271) % 2147483647
        score = state % 100
        printf "update index_diff set category = %s, score = %d where id = %d;\n", category, score, id
    }
    for (n = 1; n <= 32; n++) {
        state = (state * 48271) % 2147483647
        id = state % 384 + 1
        printf "delete from index_diff where id = %d;\n", id
    }
}' > "$TMP/mutations.sql"
run < "$TMP/mutations.sql" > "$TMP/mutations.out" 2>&1 \
    || fail "randomized updates/deletes failed"
run -c "vacuum index_diff" > "$TMP/vacuum.out" 2>&1 \
    || fail "VACUUM after mutations failed"

plan=$(run -c 'explain select id from index_diff where category = 3')
plan_contains_indented_node "$plan" "Index Scan using index_diff_category_idx on index_diff" \
    || fail "equality query did not use the populated index: $plan"

capture_results() {
    for category in 0 1 2 3 4 5 6 7; do
        printf 'category=%s\n' "$category"
        run -c "select id from index_diff where category = $category order by id" \
            || return 1
    done
    printf 'category-null\n'
    run -c 'select id from index_diff where category is null order by id' \
        || return 1
    printf 'residual-order-limit\n'
    run -c 'select id, score from index_diff where category = 3 and score > 50 order by id desc limit 9' \
        || return 1
    printf 'residual-projection\n'
    run -c 'select id, category from index_diff where category = 6 and id > 200 order by id limit 11' \
        || return 1
}

capture_results > "$TMP/indexed-results.out" \
    || fail "indexed result capture failed"
run -c "drop index index_diff_category_idx" > "$TMP/drop-diff-index.out" 2>&1 \
    || fail "could not drop index for sequential comparison"
capture_results > "$TMP/sequential-results.out" \
    || fail "sequential result capture failed"
if ! cmp -s "$TMP/indexed-results.out" "$TMP/sequential-results.out"; then
    echo "index-scan-regression: indexed/sequential mismatch:" >&2
    diff -u "$TMP/sequential-results.out" "$TMP/indexed-results.out" >&2 || true
    fail "randomized differential results differed"
fi

# Pause SELECT immediately before atomic heap/index reader admission. DROP INDEX
# must finish while SELECT is waiting to register, after which SELECT revalidates
# and falls back to SeqScan.
run -c "create table drop_handoff (id int)" >/dev/null
run -c "insert into drop_handoff values (7)" >/dev/null
run -c "create index drop_handoff_idx on drop_handoff (id)" >/dev/null

install_output=$(control <<EOF
(defvar *index-drop-handoff-heap* nil)
(defvar *index-drop-handoff-index* nil)
(defvar *index-drop-handoff-xid* nil)
(defvar *index-drop-handoff-original-readers* nil)
(defvar *index-drop-handoff-original-build-locks* nil)
(defvar *index-drop-handoff-original-wait* nil)
(let* ((db (gethash "postgres" (catalog-databases *catalog*)))
       (table (gethash "drop_handoff" (database-tables db)))
       (index (gethash "drop_handoff_idx" (database-indexes db))))
  (unless (and table index) (error "handoff test catalog entries are missing"))
  (setf *index-drop-handoff-heap* (pg-class-relfilenode table)
        *index-drop-handoff-index* (pg-index-relfilenode index)))
(setf *index-drop-handoff-original-readers*
      (symbol-function 'register-relation-readers)
      *index-drop-handoff-original-build-locks*
      (symbol-function 'acquire-relation-build-locks)
      *index-drop-handoff-original-wait*
      (symbol-function 'condition-wait-with-timeout))
(setf (symbol-function 'register-relation-readers)
      (lambda (relfilenodes xid)
        (when (member *index-drop-handoff-heap* relfilenodes :test #'=)
          (setf *index-drop-handoff-xid* xid)
          (with-open-file (out "$TMP/drop-handoff.ready"
                               :direction :output :if-exists :supersede)
            (write-string "ready" out))
          (loop repeat 3000
                until (probe-file "$TMP/drop-handoff.release")
                do (sleep 0.01)))
        (funcall *index-drop-handoff-original-readers* relfilenodes xid)))
(setf (symbol-function 'acquire-relation-build-locks)
      (lambda (relfilenodes &optional own-xid wait-readers-p on-acquired)
        (let ((token (funcall *index-drop-handoff-original-build-locks*
                              relfilenodes own-xid wait-readers-p on-acquired)))
          (when (and (member *index-drop-handoff-heap* relfilenodes :test #'=)
                     (member *index-drop-handoff-index* relfilenodes :test #'=))
            (with-open-file (out "$TMP/drop-handoff.gates"
                                 :direction :output :if-exists :supersede)
              (write-string "gated" out))
            (loop repeat 3000
                  until (probe-file "$TMP/drop-handoff.ddl-release")
                  do (sleep 0.01)))
          token)))
(setf (symbol-function 'condition-wait-with-timeout)
      (lambda (waitqueue mutex)
        (when (and *backend*
                   *index-drop-handoff-xid*
                   (eql mutex *relation-lock-mutex*)
                   (= (or (backend-xid *backend*) 0)
                      *index-drop-handoff-xid*))
          (with-open-file (out "$TMP/drop-handoff.reader-waiting"
                               :direction :output :if-exists :supersede)
            (write-string "waiting" out)))
        (funcall *index-drop-handoff-original-wait* waitqueue mutex)))
(quit)
EOF
) || fail "could not install handoff scheduler"
echo "$install_output" | grep -q 'bye' || fail "handoff scheduler did not install"
echo "$install_output" | grep -Eq ';; (read )?error:' \
    && fail "handoff scheduler evaluation failed"

mkfifo "$TMP/drop-reader.sql"
run < "$TMP/drop-reader.sql" > "$TMP/drop-reader.out" 2>&1 &
READER=$!
exec 3> "$TMP/drop-reader.sql"
printf "begin;\nselect id from drop_handoff where id = 7;\nselect 'handoff-reader-ready';\n" >&3
wait_for_file "$TMP/drop-handoff.ready"

(
    run -c "drop index drop_handoff_idx" > "$TMP/drop-handoff-ddl.out" 2>&1 \
        || exit $?
    : > "$TMP/drop-handoff.ddl-done"
) &
DROPPER=$!
wait_for_file "$TMP/drop-handoff.gates"
: > "$TMP/drop-handoff.release"
wait_for_file "$TMP/drop-handoff.reader-waiting"
state_output=$(control <<'EOF'
(sb-thread:with-mutex (*relation-lock-mutex*)
  (and (not (member *index-drop-handoff-xid*
                    (gethash *index-drop-handoff-heap* *relation-readers*)
                    :test #'=))
       (not (member *index-drop-handoff-xid*
                    (gethash *index-drop-handoff-index* *relation-readers*)
                    :test #'=))))
(quit)
EOF
) || fail "could not inspect blocked batch admission"
echo "$state_output" | grep -q 'T' \
    || fail "blocked SELECT left a partial relation-reader registration"
echo "$state_output" | grep -Eq ';; (read )?error:' \
    && fail "could not inspect blocked batch admission"
: > "$TMP/drop-handoff.ddl-release"
wait_for_file "$TMP/drop-handoff.ddl-done"
wait_for_marker "$TMP/drop-reader.out" handoff-reader-ready
grep -q '^7$' "$TMP/drop-reader.out" \
    || fail "SELECT failed after revalidating to a sequential scan"
printf 'commit;\n' >&3
exec 3>&-
wait "$READER" || fail "reader failed after the handoff"
READER=
wait "$DROPPER" || fail "DROP INDEX failed after reader commit"
DROPPER=
plan=$(run -c 'explain select id from drop_handoff where id = 7')
plan_contains_indented_node "$plan" "Seq Scan on drop_handoff" \
    || fail "dropped index remained in the SELECT path: $plan"

restore_output=$(control <<'EOF'
(setf (symbol-function 'register-relation-readers)
      *index-drop-handoff-original-readers*
      (symbol-function 'acquire-relation-build-locks)
      *index-drop-handoff-original-build-locks*
      (symbol-function 'condition-wait-with-timeout)
      *index-drop-handoff-original-wait*)
(quit)
EOF
) || fail "could not restore relation reader function"
echo "$restore_output" | grep -q 'bye' || fail "relation reader restore failed"
echo "$restore_output" | grep -Eq ';; (read )?error:' \
    && fail "relation reader restore evaluation failed"

echo "index-scan-regression: PASS"
