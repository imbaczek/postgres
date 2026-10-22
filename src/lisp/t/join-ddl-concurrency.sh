#!/bin/sh
# t/join-ddl-concurrency.sh -- atomic JOIN admission against concurrent DROP TABLE.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
SBCL=${SBCL:-sbcl}
PORT=${PORT:-5477}
CONTROL_PORT=${CONTROL_PORT:-$((PORT + 2000))}
TMP=$(mktemp -d /tmp/pg-join-ddl.XXXXXX)
LOG="$TMP/server.log"
SRV=
READER_B=
READER_C=
DROP_B=
DROP_C=
. "$BASE/t/test-support.sh"
test_debug_setup join-ddl-concurrency "$PORT" "$TMP" "$LOG"

cleanup() {
    : > "$TMP/drop-b.release"
    : > "$TMP/drop-c.release"
    for pid in "$READER_B" "$READER_C" "$DROP_B" "$DROP_C"; do
        [ -z "$pid" ] || kill "$pid" 2>/dev/null || true
    done
    if [ -n "$SRV" ]; then
        kill "$SRV" 2>/dev/null || true
        i=0
        while kill -0 "$SRV" 2>/dev/null && [ "$i" -lt 50 ]; do
            i=$((i + 1))
            sleep 0.1
        done
        kill -KILL "$SRV" 2>/dev/null || true
    fi
    for pid in "$READER_B" "$READER_C" "$DROP_B" "$DROP_C"; do
        [ -z "$pid" ] || wait "$pid" 2>/dev/null || true
    done
    [ -z "$SRV" ] || wait "$SRV" 2>/dev/null || true
    rm -rf "$TMP"
}
test_debug_install_traps cleanup

fail() {
    echo "join-ddl-concurrency: FAIL $1" >&2
    for output in "$TMP"/*.out; do
        [ -f "$output" ] || continue
        echo "--- $output" >&2
        sed -n '1,80p' "$output" >&2
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

run -c "create table join_ddl_a (id int)" >/dev/null
run -c "create table join_ddl_b (id int)" >/dev/null
run -c "create table join_ddl_c (id int)" >/dev/null
run -c "insert into join_ddl_a values (1), (2)" >/dev/null
run -c "insert into join_ddl_b values (1), (2)" >/dev/null
run -c "insert into join_ddl_c values (1), (2)" >/dev/null

install_output=$(control <<EOF
(defvar *join-ddl-heap-a* nil)
(defvar *join-ddl-heap-b* nil)
(defvar *join-ddl-heap-c* nil)
(defvar *join-ddl-xid-b* nil)
(defvar *join-ddl-xid-c* nil)
(defvar *join-ddl-original-readers* nil)
(defvar *join-ddl-original-build-locks* nil)
(defvar *join-ddl-original-wait* nil)
(let* ((db (gethash "postgres" (catalog-databases *catalog*)))
       (table-a (gethash "join_ddl_a" (database-tables db)))
       (table-b (gethash "join_ddl_b" (database-tables db)))
       (table-c (gethash "join_ddl_c" (database-tables db))))
  (unless (and table-a table-b table-c) (error "JOIN DDL tables are missing"))
  (setf *join-ddl-heap-a* (pg-class-relfilenode table-a)
        *join-ddl-heap-b* (pg-class-relfilenode table-b)
        *join-ddl-heap-c* (pg-class-relfilenode table-c)))
(setf *join-ddl-original-readers* (symbol-function 'register-relation-readers)
      *join-ddl-original-build-locks* (symbol-function 'acquire-relation-build-locks)
      *join-ddl-original-wait* (symbol-function 'condition-wait-with-timeout))
(setf (symbol-function 'register-relation-readers)
      (lambda (relfilenodes xid)
        (let ((label (cond ((member *join-ddl-heap-b* relfilenodes :test #'=) "b")
                           ((member *join-ddl-heap-c* relfilenodes :test #'=) "c"))))
          (when (and label (member *join-ddl-heap-a* relfilenodes :test #'=))
            (if (equal label "b")
                (setf *join-ddl-xid-b* xid)
                (setf *join-ddl-xid-c* xid))
            (with-open-file (out (format nil "$TMP/select-~a.attempting" label)
                                 :direction :output :if-exists :supersede)
              (write-string "attempting" out)))
          (funcall *join-ddl-original-readers* relfilenodes xid))))
(setf (symbol-function 'condition-wait-with-timeout)
      (lambda (waitqueue mutex)
        (when (and *backend* (eql mutex *relation-lock-mutex*))
          (let ((xid (backend-xid *backend*)))
            (when (eql xid *join-ddl-xid-b*)
              (with-open-file (out "$TMP/select-b.waiting"
                                   :direction :output :if-exists :supersede)
                (write-string "waiting" out)))
            (when (eql xid *join-ddl-xid-c*)
              (with-open-file (out "$TMP/select-c.waiting"
                                   :direction :output :if-exists :supersede)
                (write-string "waiting" out)))))
        (funcall *join-ddl-original-wait* waitqueue mutex)))
(setf (symbol-function 'acquire-relation-build-locks)
      (lambda (relfilenodes &optional own-xid wait-readers-p on-acquired)
        (let ((token (funcall *join-ddl-original-build-locks*
                              relfilenodes own-xid wait-readers-p on-acquired)))
          (let ((label (cond ((member *join-ddl-heap-b* relfilenodes :test #'=) "b")
                             ((member *join-ddl-heap-c* relfilenodes :test #'=) "c"))))
            (when label
              (with-open-file (out (format nil "$TMP/drop-~a.gated" label)
                                   :direction :output :if-exists :supersede)
                (write-string "gated" out))
              (loop repeat 3000
                    until (probe-file (format nil "$TMP/drop-~a.release" label))
                    do (sleep 0.01))))
          token)))
(quit)
EOF
) || { echo "$install_output" >&2; fail "could not install JOIN/DDL scheduler"; }
echo "$install_output" | grep -Eq ';; (read )?error:' \
    && fail "JOIN/DDL scheduler evaluation failed"

(run -c "drop table join_ddl_b" > "$TMP/drop-b.out" 2>&1 \
    && : > "$TMP/drop-b.done") &
DROP_B=$!
(run -c "drop table join_ddl_c" > "$TMP/drop-c.out" 2>&1 \
    && : > "$TMP/drop-c.done") &
DROP_C=$!
wait_for_file "$TMP/drop-b.gated"
wait_for_file "$TMP/drop-c.gated"

# A is admissible for both queries while each other heap is protected by its
# DROP TABLE gate. An incremental reader implementation would leave an A
# registration behind while waiting for B or C.
(
    run -c 'select join_ddl_a.id from join_ddl_a join join_ddl_b on join_ddl_a.id = join_ddl_b.id' \
        > "$TMP/reader-b.out" 2>&1 || : > "$TMP/reader-b.failed"
) &
READER_B=$!
(
    run -c 'select join_ddl_a.id from join_ddl_a join join_ddl_c on join_ddl_a.id = join_ddl_c.id' \
        > "$TMP/reader-c.out" 2>&1 || : > "$TMP/reader-c.failed"
) &
READER_C=$!
wait_for_file "$TMP/select-b.attempting"
wait_for_file "$TMP/select-c.attempting"
wait_for_file "$TMP/select-b.waiting"
wait_for_file "$TMP/select-c.waiting"

state_output=$(control <<'EOF'
(let ((xid-b *join-ddl-xid-b*)
      (xid-c *join-ddl-xid-c*))
  (sb-thread:with-mutex (*relation-lock-mutex*)
    (if (and xid-b xid-c
             (loop for xid in (list xid-b xid-c)
                   always (loop for relfilenode in
                                (list *join-ddl-heap-a* *join-ddl-heap-b*
                                      *join-ddl-heap-c*)
                                never (member xid (gethash relfilenode
                                                           *relation-readers*)
                                              :test #'=))))
        :NO-PARTIAL-READERS
        :PARTIAL-READER)))
(quit)
EOF
) || fail "could not inspect JOIN reader admission"
echo "$state_output" | grep -q ':NO-PARTIAL-READERS' \
    || fail "a JOIN registered an admissible heap while waiting for the other"
echo "$state_output" | grep -Eq ';; (read )?error:' \
    && fail "JOIN reader admission inspection failed"

: > "$TMP/drop-b.release"
: > "$TMP/drop-c.release"
wait_for_file "$TMP/drop-b.done"
wait_for_file "$TMP/drop-c.done"
wait_for_file "$TMP/reader-b.failed"
wait_for_file "$TMP/reader-c.failed"
grep -Fq 'relation "join_ddl_b" does not exist' "$TMP/reader-b.out" \
    || fail "JOIN B did not fail because DROP TABLE invalidated its plan"
grep -Fq 'relation "join_ddl_c" does not exist' "$TMP/reader-c.out" \
    || fail "JOIN C did not fail because DROP TABLE invalidated its plan"
for output in "$TMP/reader-b.out" "$TMP/reader-c.out"; do
    if awk '$0 == "1" || $0 == "2" { found = 1 } END { exit !found }' "$output"; then
        fail "JOIN returned rows after DROP TABLE invalidated its planned relation"
    fi
done

wait "$READER_B" || fail "first JOIN session failed unexpectedly"
READER_B=
wait "$READER_C" || fail "second JOIN session failed unexpectedly"
READER_C=
wait "$DROP_B" || fail "DROP TABLE B failed"
DROP_B=
wait "$DROP_C" || fail "DROP TABLE C failed"
DROP_C=

restore_output=$(control <<'EOF'
(setf (symbol-function 'register-relation-readers) *join-ddl-original-readers*
      (symbol-function 'acquire-relation-build-locks) *join-ddl-original-build-locks*
      (symbol-function 'condition-wait-with-timeout) *join-ddl-original-wait*)
(quit)
EOF
) || fail "could not restore JOIN/DDL instrumentation"
echo "$restore_output" | grep -Eq ';; (read )?error:' \
    && fail "JOIN/DDL instrumentation restore failed"

echo "join-ddl-concurrency: PASS"
