#!/bin/sh
# t/vacuum-full-regression.sh -- VACUUM FULL rewrite, indexes and recovery.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
SBCL=${SBCL:-sbcl}
PORT=${PORT:-5474}
CONTROL_PORT=${CONTROL_PORT:-$((PORT + 2000))}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-vacuum-full.XXXXXX")
PGDATA="$TMP/data"
LOG="$TMP/server.log"
SRV=
FULL_PID=
. "$BASE/t/test-support.sh"
test_debug_setup vacuum-full-regression "$PORT" "$TMP" "$LOG"

cleanup() {
    : > "$TMP/full-gate.release"
    : > "$TMP/full-horizon.release"
    if [ -d "$PGDATA/pg_wal" ]; then
        : > "$PGDATA/pg_wal/test-resume-after-vacuum-full-catalog-wal-append"
        : > "$PGDATA/pg_wal/test-resume-after-vacuum-full-catalog-wal-flush"
    fi
    [ -z "${FULL_PID:-}" ] || kill "$FULL_PID" 2>/dev/null || true
    [ -z "${FULL_PID:-}" ] || wait "$FULL_PID" 2>/dev/null || true
    if [ -n "${SRV:-}" ]; then
        kill -TERM "$SRV" 2>/dev/null || true
        wait "$SRV" 2>/dev/null || true
    fi
    rm -rf "$TMP"
}
test_debug_install_traps cleanup

fail() {
    echo "vacuum-full-regression: FAIL $1" >&2
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

row_count() {
    rows=$(run -c "$1") || return $?
    printf '%s\n' "$rows" | awk 'NF { n++ } END { print n + 0 }'
}

control() {
    timeout 25 "$SBCL" --script "$BASE/t/control-client.lisp" \
        tcp "127.0.0.1:$CONTROL_PORT"
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

start_server() {
    pause_at=${1:-}
    PG_WAL_TEST_PAUSE_AT="$pause_at" \
        test_debug_start_server "$LOG" -p "$PORT" -D "$PGDATA" \
            --control-port "$CONTROL_PORT"
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

stop_server() {
    kill -TERM "$SRV" 2>/dev/null || true
    wait "$SRV" 2>/dev/null || true
    SRV=
}

start_server

# Port the simple PostgreSQL vacuum.sql sequence: delete old versions, run both
# accepted FULL syntaxes, mutate again, and verify the table remains usable.
run -c "create table vacfull (id int, category int)" >/dev/null
awk 'BEGIN {
    printf "insert into vacfull values "
    for (id = 1; id <= 128; id++)
        printf "%s(%d, %d)", id == 1 ? "" : ", ", id, id % 5
    print ";"
}' > "$TMP/initial.sql"
run < "$TMP/initial.sql" > "$TMP/initial.out" 2>&1 \
    || fail "initial populated-table insert failed"
awk 'BEGIN {
    printf "insert into vacfull values "
    for (id = 1000; id < 3000; id++)
        printf "%s(%d, %d)", id == 1000 ? "" : ", ", id, id + 10000
    print ";"
}' > "$TMP/selectivity-fixture.sql"
run < "$TMP/selectivity-fixture.sql" > "$TMP/selectivity-fixture.out" 2>&1 \
    || fail "selectivity fixture insert failed"
run -c "create index vacfull_category_idx on vacfull (category)" \
    > "$TMP/index-build.out" 2>&1 || fail "populated index build failed"
run -c "update vacfull set category = category + 10 where id <= 8" \
    > "$TMP/update.out" 2>&1 || fail "UPDATE before rewrite failed"
run -c "delete from vacfull where id > 32 and id <= 128" \
    > "$TMP/delete.out" 2>&1 || fail "DELETE before rewrite failed"
run -c "vacuum vacfull" > "$TMP/vacuum.out" 2>&1 \
    || fail "ordinary VACUUM before rewrite failed"

[ "$(row_count 'select id from vacfull')" = 2032 ] \
    || fail "ordinary VACUUM changed visible rows"
run -c "analyze vacfull" >/dev/null
plan=$(run -c 'explain select id from vacfull where category = 11000')
plan_contains_indented_node "$plan" "Index Scan using vacfull_category_idx on vacfull" \
    || fail "indexed equality scan was unavailable before rewrite: $plan"
run -c 'select id from vacfull where category = 11000 order by id' \
    > "$TMP/before-full.out" || fail "indexed query before rewrite failed"

control <<'EOF' > "$TMP/old-relations.out"
(defvar *vac-full-old-table* nil)
(defvar *vac-full-old-heap* nil)
(defvar *vac-full-old-index* nil)
(let* ((db (gethash "postgres" (catalog-databases *catalog*)))
       (table (gethash "vacfull" (database-tables db)))
       (index (gethash "vacfull_category_idx" (database-indexes db))))
  (unless (and table index) (error "VACUUM FULL fixture metadata is missing"))
  (setf *vac-full-old-table* table
        *vac-full-old-heap* (pg-class-relfilenode table)
        *vac-full-old-index* (pg-index-relfilenode index)))
(quit)
EOF
grep -q 'bye' "$TMP/old-relations.out" \
    || fail "could not save pre-rewrite relation metadata"

run -c 'vacuum full vacfull' > "$TMP/full.out" 2>&1 \
    || fail "VACUUM FULL rewrite failed"
run -c "analyze vacfull" >/dev/null
control <<'EOF' > "$TMP/relation-swap.out"
(let* ((db (gethash "postgres" (catalog-databases *catalog*)))
       (table (gethash "vacfull" (database-tables db)))
       (index (gethash "vacfull_category_idx" (database-indexes db))))
  (list (not (eq *vac-full-old-table* table))
        (/= *vac-full-old-heap* (pg-class-relfilenode table))
        (not (probe-file (relation-file-path *vac-full-old-heap*)))
        (/= *vac-full-old-index* (pg-index-relfilenode index))
        (not (probe-file (relation-file-path *vac-full-old-index*)))))
(quit)
EOF
grep -q '(T T T T T)' "$TMP/relation-swap.out" \
    || fail "FULL did not publish and clean up replacement heap/index files"
run -c 'select id from vacfull where category = 11000 order by id' \
    > "$TMP/after-full.out" || fail "indexed query after rewrite failed"
cmp -s "$TMP/before-full.out" "$TMP/after-full.out" \
    || fail "VACUUM FULL changed indexed equality results"

# Exercise the parenthesized syntax and preserve an index through another
# rewrite after further writes.
run -c "update vacfull set category = 3 where id = 2" >/dev/null \
    || fail "UPDATE after rewrite failed"
run -c "insert into vacfull values (129, 3)" >/dev/null \
    || fail "INSERT after rewrite failed"
run -c 'vacuum (full) vacfull' > "$TMP/full-parenthesized.out" 2>&1 \
    || fail "VACUUM (FULL) rewrite failed"
run -c "analyze vacfull" >/dev/null
[ "$(row_count 'select id from vacfull')" = 2033 ] \
    || fail "parenthesized FULL changed visible rows"
plan=$(run -c 'explain select id from vacfull where category = 11000')
plan_contains_indented_node "$plan" "Index Scan using vacfull_category_idx on vacfull" \
    || fail "index was unavailable after parenthesized FULL: $plan"

# The command snapshot predates this DELETE, but the writer finishes before
# FULL acquires its gate. C keeps the resulting recently-dead version because
# the original snapshot still constrains OldestXmin.
run -c "create table full_horizon_race (id int)" >/dev/null
run -c "insert into full_horizon_race values (1)" >/dev/null
control <<EOF > "$TMP/horizon-scheduler.out"
(defvar *vac-full-horizon-heap* nil)
(defvar *vac-full-horizon-original-build-locks* nil)
(defvar *vac-full-horizon-paused* nil)
(let* ((db (gethash "postgres" (catalog-databases *catalog*)))
       (table (gethash "full_horizon_race" (database-tables db))))
  (unless table (error "FULL horizon table is missing"))
  (setf *vac-full-horizon-heap* (pg-class-relfilenode table)))
(setf *vac-full-horizon-original-build-locks*
      (symbol-function 'acquire-relation-build-locks)
      *vac-full-horizon-paused* nil
      (symbol-function 'acquire-relation-build-locks)
      (lambda (relfilenodes &optional own-xid wait-readers-p on-acquired)
        (when (and (not *vac-full-horizon-paused*)
                   (member *vac-full-horizon-heap* relfilenodes :test #'=))
          (setf *vac-full-horizon-paused* t)
          (with-open-file (out "$TMP/full-horizon.ready"
                               :direction :output :if-exists :supersede)
            (write-string "ready" out))
          (loop repeat 3000
                until (probe-file "$TMP/full-horizon.release")
                do (sleep 0.01)))
        (funcall *vac-full-horizon-original-build-locks*
                 relfilenodes own-xid wait-readers-p on-acquired)))
(quit)
EOF
grep -q 'bye' "$TMP/horizon-scheduler.out" \
    || fail "could not install FULL horizon scheduler"
grep -Eq ';; (read )?error:' "$TMP/horizon-scheduler.out" \
    && fail "FULL horizon scheduler evaluation failed"
run -c 'vacuum full full_horizon_race' > "$TMP/horizon-full.out" 2>&1 &
FULL_PID=$!
wait_for_file "$TMP/full-horizon.ready"
run -c 'delete from full_horizon_race where id = 1' \
    > "$TMP/horizon-delete.out" 2>&1 \
    || fail "DELETE could not commit after FULL captured its snapshot"
: > "$TMP/full-horizon.release"
wait "$FULL_PID" || fail "FULL horizon rewrite failed"
FULL_PID=
[ "$(row_count 'select id from full_horizon_race')" = 0 ] \
    || fail "FULL made a post-snapshot DELETE visible again"
control <<'EOF' > "$TMP/horizon-tuple-count.out"
(let* ((db (gethash "postgres" (catalog-databases *catalog*)))
       (table (gethash "full_horizon_race" (database-tables db)))
       (heap (pg-class-relfilenode table))
       (normal-tuples
         (loop for block below (buf-nblocks heap)
               for page = (buf-read heap block)
               sum (loop for offnum from 1 to (page-lp-count page)
                         count (= +lp-normal+
                                  (nth-value 1 (page-get-lp page offnum)))))))
  (setf (symbol-function 'acquire-relation-build-locks)
        *vac-full-horizon-original-build-locks*)
  (list normal-tuples))
(quit)
EOF
grep -q '(1)' "$TMP/horizon-tuple-count.out" \
    || fail "FULL did not retain the recently-dead version protected by its snapshot"

# Pause FULL after it captures its initial heap/index set but before it obtains
# the relation gates. CREATE INDEX can publish during this window, so FULL must
# detect the changed set, retry, and rebuild the new index against compacted TIDs.
run -c "create table full_gate_race (id int)" >/dev/null
awk 'BEGIN {
    printf "insert into full_gate_race values "
    for (id = 1; id <= 128; id++)
        printf "%s(%d)", id == 1 ? "" : ", ", id
    print ";"
}' > "$TMP/gate-race-initial.sql"
run < "$TMP/gate-race-initial.sql" > "$TMP/gate-race-initial.out" 2>&1 \
    || fail "gate-race fixture insert failed"
run -c "delete from full_gate_race where id <= 64" >/dev/null \
    || fail "gate-race fixture delete failed"
awk 'BEGIN {
    printf "insert into full_gate_race values "
    for (id = 1000; id < 3000; id++)
        printf "%s(%d)", id == 1000 ? "" : ", ", id
    print ";"
}' > "$TMP/gate-race-selectivity.sql"
run < "$TMP/gate-race-selectivity.sql" >/dev/null
run -c "analyze full_gate_race" >/dev/null
control <<EOF > "$TMP/gate-race-scheduler.out"
(defvar *vac-full-race-heap* nil)
(defvar *vac-full-race-original-build-locks* nil)
(defvar *vac-full-race-paused* nil)
(defvar *vac-full-race-armed* nil)
(defvar *vac-full-race-build-sets* nil)
(let* ((db (gethash "postgres" (catalog-databases *catalog*)))
       (table (gethash "full_gate_race" (database-tables db))))
  (unless table (error "gate-race table is missing"))
  (setf *vac-full-race-heap* (pg-class-relfilenode table)))
(setf *vac-full-race-original-build-locks*
      (symbol-function 'acquire-relation-build-locks)
      *vac-full-race-paused* nil
      *vac-full-race-build-sets* nil
      *vac-full-race-armed* t
      (symbol-function 'acquire-relation-build-locks)
      (lambda (relfilenodes &optional own-xid wait-readers-p on-acquired)
        (push (copy-list relfilenodes) *vac-full-race-build-sets*)
        (when (and *vac-full-race-armed*
                   (not *vac-full-race-paused*)
                   (member *vac-full-race-heap* relfilenodes :test #'=))
          (setf *vac-full-race-paused* t)
          (with-open-file (out "$TMP/full-gate.ready"
                               :direction :output :if-exists :supersede)
            (write-string "ready" out))
          (loop repeat 3000
                until (probe-file "$TMP/full-gate.release")
                do (sleep 0.01)))
        (funcall *vac-full-race-original-build-locks*
                 relfilenodes own-xid wait-readers-p on-acquired)))
(quit)
EOF
grep -q 'bye' "$TMP/gate-race-scheduler.out" \
    || fail "could not install FULL gate-race scheduler"
grep -Eq ';; (read )?error:' "$TMP/gate-race-scheduler.out" \
    && fail "FULL gate-race scheduler evaluation failed"
run -c 'vacuum full full_gate_race' > "$TMP/gate-race-full.out" 2>&1 &
FULL_PID=$!
wait_for_file "$TMP/full-gate.ready"
timeout 25 env PGPASSWORD=x psql -X -q -A -t -h 127.0.0.1 -p "$PORT" \
    -U postgres -d postgres -v ON_ERROR_STOP=1 \
    -c 'create index full_gate_race_idx on full_gate_race (id)' \
    > "$TMP/gate-race-index.out" 2>&1 \
    || fail "CREATE INDEX could not publish while FULL was between capture and gate"
: > "$TMP/full-gate.release"
wait "$FULL_PID" || fail "VACUUM FULL failed after index-set retry"
FULL_PID=
control <<'EOF' > "$TMP/gate-race-retry.out"
(list (some (lambda (relfilenodes) (> (length relfilenodes) 1))
            *vac-full-race-build-sets*))
(setf *vac-full-race-armed* nil
      (symbol-function 'acquire-relation-build-locks)
      *vac-full-race-original-build-locks*)
(quit)
EOF
grep -q '(T)' "$TMP/gate-race-retry.out" \
    || fail "FULL did not retry with its newly published index in the gate set"
plan=$(run -c 'explain select id from full_gate_race where id = 100')
plan_contains_indented_node "$plan" "Index Scan using full_gate_race_idx on full_gate_race" \
    || fail "gate-race index was not available after FULL: $plan"
[ "$(run -c 'select id from full_gate_race where id = 100')" = 100 ] \
    || fail "gate-race index returned a stale heap TID after FULL"

# PostgreSQL's original regression removes all rows after a FULL rewrite; keep
# rows across a restart first to verify the published catalog and rebuilt index.
run -c 'select id from vacfull where category = 11000 order by id' \
    > "$TMP/before-restart.out" || fail "indexed query before restart failed"
stop_server
start_server
[ "$(row_count 'select id from vacfull')" = 2033 ] \
    || fail "restart lost rows published by VACUUM FULL"
run -c "analyze vacfull" >/dev/null
run -c 'select id from vacfull where category = 11000 order by id' \
    > "$TMP/after-restart.out" || fail "indexed query after restart failed"
cmp -s "$TMP/before-restart.out" "$TMP/after-restart.out" \
    || fail "restart changed indexed results after VACUUM FULL"

# Crash after the terminal record is appended but before wal-flush. A complete
# record may still be replayed, so recovery must publish the replacement heap
# and index, then remove both old relfilenodes.
stop_server
python3 - "$PGDATA/global/pg_catalog.dat" > "$TMP/pre-crash-relations" <<'PY'
import re
import sys

catalog = open(sys.argv[1], encoding="utf-8").read()
for name in ("vacfull", "vacfull_category_idx"):
    match = re.search(r'"' + re.escape(name) +
                      r'"\)?\s+:relfilenode\s+(\d+)', catalog,
                      re.IGNORECASE)
    if not match:
        raise SystemExit("could not find relfilenode for " + name)
    print(match.group(1))
PY
old_heap=$(sed -n '1p' "$TMP/pre-crash-relations")
old_index=$(sed -n '2p' "$TMP/pre-crash-relations")
start_server after-vacuum-full-catalog-wal-append
run -c 'vacuum full vacfull' > "$TMP/crash-before-flush.out" 2>&1 &
FULL_PID=$!
wait_for_file \
    "$PGDATA/pg_wal/test-paused-after-vacuum-full-catalog-wal-append"
kill -KILL "$SRV" 2>/dev/null || true
wait "$SRV" 2>/dev/null || true
SRV=
wait "$FULL_PID" 2>/dev/null || true
FULL_PID=
start_server
[ "$(row_count 'select id from vacfull')" = 2033 ] \
    || fail "pre-flush crash recovery lost rows from VACUUM FULL publication"
run -c "analyze vacfull" >/dev/null
plan=$(run -c 'explain select id from vacfull where category = 11000')
plan_contains_indented_node "$plan" "Index Scan using vacfull_category_idx on vacfull" \
    || fail "pre-flush crash recovery lost the replacement index: $plan"
run -c 'select id from vacfull where category = 11000 order by id' \
    > "$TMP/after-before-flush-crash.out" \
    || fail "indexed query after pre-flush recovery failed"
cmp -s "$TMP/before-restart.out" "$TMP/after-before-flush-crash.out" \
    || fail "pre-flush crash recovery changed indexed results"
python3 - "$PGDATA" "$old_heap" "$old_index" <<'PY'
import os
import re
import sys

data_dir, old_heap, old_index = sys.argv[1:]
catalog = open(os.path.join(data_dir, "global", "pg_catalog.dat"),
               encoding="utf-8").read()

def relation_node(name):
    match = re.search(r'"' + re.escape(name) +
                      r'"\)?\s+:relfilenode\s+(\d+)', catalog,
                      re.IGNORECASE)
    if not match:
        raise SystemExit("could not find relfilenode for " + name)
    return match.group(1)

new_heap = relation_node("vacfull")
new_index = relation_node("vacfull_category_idx")
if new_heap == old_heap or new_index == old_index:
    raise SystemExit("recovery did not publish replacement relfilenodes")
for relfilenode in (old_heap, old_index):
    for suffix in ("", ".vm"):
        path = os.path.join(data_dir, "base", relfilenode + suffix)
        if os.path.exists(path):
            raise SystemExit("old relation fork survived recovery: " + path)
PY

# Crash after the replacement catalog WAL record is durable but before the
# catalog file rename. Recovery must publish the replacement heap and index,
# then remove both old relfilenodes.
stop_server
python3 - "$PGDATA/global/pg_catalog.dat" > "$TMP/pre-crash-relations" <<'PY'
import re
import sys

catalog = open(sys.argv[1], encoding="utf-8").read()
for name in ("vacfull", "vacfull_category_idx"):
    match = re.search(r'"' + re.escape(name) +
                      r'"\)?\s+:relfilenode\s+(\d+)', catalog,
                      re.IGNORECASE)
    if not match:
        raise SystemExit("could not find relfilenode for " + name)
    print(match.group(1))
PY
old_heap=$(sed -n '1p' "$TMP/pre-crash-relations")
old_index=$(sed -n '2p' "$TMP/pre-crash-relations")
start_server after-vacuum-full-catalog-wal-flush
run -c 'vacuum full vacfull' > "$TMP/crash-full.out" 2>&1 &
FULL_PID=$!
wait_for_file \
    "$PGDATA/pg_wal/test-paused-after-vacuum-full-catalog-wal-flush"
kill -KILL "$SRV" 2>/dev/null || true
wait "$SRV" 2>/dev/null || true
SRV=
wait "$FULL_PID" 2>/dev/null || true
FULL_PID=
start_server
[ "$(row_count 'select id from vacfull')" = 2033 ] \
    || fail "crash recovery lost rows from VACUUM FULL publication"
run -c "analyze vacfull" >/dev/null
plan=$(run -c 'explain select id from vacfull where category = 11000')
plan_contains_indented_node "$plan" "Index Scan using vacfull_category_idx on vacfull" \
    || fail "recovered VACUUM FULL index was unavailable: $plan"
run -c 'select id from vacfull where category = 11000 order by id' \
    > "$TMP/after-crash-full.out" \
    || fail "indexed query after VACUUM FULL crash recovery failed"
cmp -s "$TMP/before-restart.out" "$TMP/after-crash-full.out" \
    || fail "crash recovery changed indexed results after VACUUM FULL"
python3 - "$PGDATA" "$old_heap" "$old_index" <<'PY'
import os
import re
import sys

data_dir, old_heap, old_index = sys.argv[1:]
catalog = open(os.path.join(data_dir, "global", "pg_catalog.dat"),
               encoding="utf-8").read()

def relation_node(name):
    match = re.search(r'"' + re.escape(name) +
                      r'"\)?\s+:relfilenode\s+(\d+)', catalog,
                      re.IGNORECASE)
    if not match:
        raise SystemExit("could not find relfilenode for " + name)
    return match.group(1)

new_heap = relation_node("vacfull")
new_index = relation_node("vacfull_category_idx")
if new_heap == old_heap or new_index == old_index:
    raise SystemExit("recovery did not publish replacement relfilenodes")
for relfilenode in (old_heap, old_index):
    for suffix in ("", ".vm"):
        path = os.path.join(data_dir, "base", relfilenode + suffix)
        if os.path.exists(path):
            raise SystemExit("old relation fork survived recovery: " + path)
PY

run -c 'delete from vacfull' > "$TMP/delete-all.out" 2>&1 \
    || fail "DELETE ALL after restart failed"
run -c 'vacuum full vacfull' > "$TMP/empty-full.out" 2>&1 \
    || fail "FULL on an empty table failed"
[ "$(row_count 'select id from vacfull')" = 0 ] \
    || fail "FULL on an empty table changed visible rows"

echo "vacuum-full-regression: PASS"
