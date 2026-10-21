#!/bin/sh
# t/index-lock-contention.sh -- measure unrelated INSERTs during B-tree search.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
SBCL=${SBCL:-sbcl}
PORT=${PORT:-5473}
CONTROL_PORT=${CONTROL_PORT:-$((PORT + 2000))}
ROWS=${ROWS:-4096}
SCANS=${SCANS:-5}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-index-contention.XXXXXX")
LOG="$TMP/server.log"
SRV=
SCANNER=
WRITER=
. "$BASE/t/test-support.sh"
test_debug_setup index-lock-contention "$PORT" "$TMP" "$LOG"

cleanup() {
    i=1
    while [ "$i" -le "$SCANS" ]; do
        : > "$TMP/search-$i.release"
        i=$((i + 1))
    done
    [ -z "$WRITER" ] || kill "$WRITER" 2>/dev/null || true
    [ -z "$SCANNER" ] || kill "$SCANNER" 2>/dev/null || true
    [ -z "$SRV" ] || kill "$SRV" 2>/dev/null || true
    [ -z "$WRITER" ] || wait "$WRITER" 2>/dev/null || true
    [ -z "$SCANNER" ] || wait "$SCANNER" 2>/dev/null || true
    [ -z "$SRV" ] || wait "$SRV" 2>/dev/null || true
    rm -rf "$TMP"
}
test_debug_install_traps cleanup

fail() {
    echo "index-lock-contention: FAIL $1" >&2
    for output in "$TMP"/*.out; do
        [ -f "$output" ] || continue
        echo "--- $output" >&2
        sed -n '1,60p' "$output" >&2
    done
    [ ! -f "$LOG" ] || cat "$LOG" >&2
    exit 1
}

run() {
    PGPASSWORD=x psql -X -h 127.0.0.1 -p "$PORT" -U postgres -d postgres \
        -q -A -t -v ON_ERROR_STOP=1 "$@"
}

control() {
    timeout 25 "$SBCL" --script "$BASE/t/control-client.lisp" \
        tcp "127.0.0.1:$CONTROL_PORT"
}

wait_for_file() {
    file=$1
    i=0
    while [ "$i" -lt 1000 ]; do
        [ ! -f "$file" ] || return 0
        i=$((i + 1))
        sleep 0.01
    done
    fail "timed out waiting for $file"
}

wait_for_marker() {
    file=$1
    marker=$2
    i=0
    while [ "$i" -lt 1000 ]; do
        if [ -f "$file" ] && grep -q "$marker" "$file"; then
            return 0
        fi
        i=$((i + 1))
        sleep 0.01
    done
    fail "timed out waiting for $marker"
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

run -c "create table large_duplicate_scan (id int, category int)" >/dev/null
awk -v rows="$ROWS" 'BEGIN {
    printf "insert into large_duplicate_scan values "
    for (id = 1; id <= rows; id++)
        printf "%s(%d, 10)", id == 1 ? "" : ", ", id
    print ";"
}' > "$TMP/populate.sql"
run < "$TMP/populate.sql" > "$TMP/populate.out" 2>&1 \
    || fail "could not populate duplicate-key table"
run -c "create index large_duplicate_scan_category_idx on large_duplicate_scan (category)" \
    >/dev/null
run -c "create table unrelated_writes (id int)" >/dev/null
plan=$(run -c 'explain select id from large_duplicate_scan where category = 10')
plan_contains_indented_node "$plan" "Index Scan using large_duplicate_scan_category_idx on large_duplicate_scan" \
    || fail "large equality lookup is not an IndexScan: $plan"

# The test-only B-tree wrapper pauses inside the search after the per-index
# mutex has been acquired. The unrelated INSERT must commit before the search
# is released, and its completion time from search start is recorded.
install_output=$(control <<EOF
(defvar *contention-target-index* nil)
(defvar *contention-target-writer-heap* nil)
(defvar *contention-original-btree-collect-equal* nil)
(defvar *contention-original-register-writer* nil)
(defvar *contention-original-commit-transaction* nil)
(defvar *contention-original-exec-select* nil)
(defvar *contention-search-count* 0)
(defvar *contention-writer-count* 0)
(defvar *contention-active-search* nil)
(defvar *contention-active-writer-backend* nil)
(defvar *contention-search-starts* (make-hash-table))
(let* ((db (gethash "postgres" (catalog-databases *catalog*)))
       (index (gethash "large_duplicate_scan_category_idx"
                       (database-indexes db)))
       (table (gethash "unrelated_writes" (database-tables db))))
  (unless (and index table) (error "contention test catalog entries are missing"))
  (setf *contention-target-index* (pg-index-relfilenode index)
        *contention-target-writer-heap* (pg-class-relfilenode table)))
(setf *contention-original-btree-collect-equal*
      (symbol-function 'btree-collect-equal)
      *contention-original-register-writer* (symbol-function 'register-relation-writer)
      *contention-original-commit-transaction* (symbol-function 'commit-transaction)
      *contention-original-exec-select* (symbol-function 'exec-select))
(setf (symbol-function 'btree-collect-equal)
      (lambda (relfilenode level block key acc)
        (if (or (/= relfilenode *contention-target-index*)
                *contention-active-search*)
            (funcall *contention-original-btree-collect-equal*
                     relfilenode level block key acc)
            (let* ((n (incf *contention-search-count*))
                   (entered (format nil "$TMP/search-~d.entered" n))
                   (release (format nil "$TMP/search-~d.release" n))
                   (start (get-internal-real-time)))
              (setf *contention-active-search* n
                    (gethash n *contention-search-starts*) start)
              (with-open-file (out entered :direction :output :if-exists :supersede)
                (write-string "entered" out))
              (loop repeat 3000 until (probe-file release) do (sleep 0.01))
              (unless (probe-file release) (error "contention search gate timed out"))
              (let* ((result (funcall *contention-original-btree-collect-equal*
                                      relfilenode level block key acc))
                     (elapsed (/ (* 1000.0 (- (get-internal-real-time) start))
                                 internal-time-units-per-second)))
                (with-open-file (out (format nil "$TMP/search-~d.done" n)
                                     :direction :output :if-exists :supersede)
                  (format out "~,3f~%" elapsed))
                result)))))
(setf (symbol-function 'register-relation-writer)
      (lambda (relfilenode xid)
        (funcall *contention-original-register-writer* relfilenode xid)
        (when (= relfilenode *contention-target-writer-heap*)
          (setf *contention-active-writer-backend* *backend*)
          (let ((n (incf *contention-writer-count*)))
            (with-open-file (out (format nil "$TMP/writer-~d.registered" n)
                                 :direction :output :if-exists :supersede)
              (write-string "registered" out))))
        (values)))
(setf (symbol-function 'commit-transaction)
      (lambda ()
        (multiple-value-prog1
            (funcall *contention-original-commit-transaction*)
          (let* ((n *contention-active-search*)
                 (start (and n (gethash n *contention-search-starts*))))
            (when (and start (eq *backend* *contention-active-writer-backend*))
              (let ((elapsed (/ (* 1000.0 (- (get-internal-real-time) start))
                                internal-time-units-per-second)))
                (with-open-file (out (format nil "$TMP/insert-~d.done" n)
                                     :direction :output :if-exists :supersede)
                    (format out "~,3f~%" elapsed))))))))
(setf (symbol-function 'exec-select)
      (lambda (stream ast)
        (multiple-value-prog1
            (funcall *contention-original-exec-select* stream ast)
          (let ((n *contention-active-search*))
            (when n
              (with-open-file (out (format nil "$TMP/scan-~d.done" n)
                                   :direction :output :if-exists :supersede)
                  (write-string "done" out))
              (setf *contention-active-search* nil))))))
(quit)
EOF
) || { printf '%s\n' "$install_output" >&2; fail "could not install contention instrumentation"; }
echo "$install_output" | grep -Eq ';; (read )?error:' \
    && fail "contention instrumentation evaluation failed"

mkfifo "$TMP/reader.sql" "$TMP/writer.sql"
run < "$TMP/reader.sql" > "$TMP/reader.out" 2>&1 &
SCANNER=$!
exec 4> "$TMP/reader.sql"
run < "$TMP/writer.sql" > "$TMP/writer.out" 2>&1 &
WRITER=$!
exec 3> "$TMP/writer.sql"
printf '\\echo writer-session-ready\n' >&3
wait_for_marker "$TMP/writer.out" writer-session-ready

n=1
while [ "$n" -le "$SCANS" ]; do
    printf 'select id from large_duplicate_scan where category = 10;\n' >&4
    wait_for_file "$TMP/search-$n.entered"
    printf 'insert into unrelated_writes values (%d);\n' "$n" >&3
    wait_for_file "$TMP/writer-$n.registered"

    # A regression that holds *write-lock* while searching or while waiting for
    # this B-tree mutex cannot produce this marker until the scan is released.
    wait_for_file "$TMP/insert-$n.done"

    : > "$TMP/search-$n.release"
    wait_for_file "$TMP/search-$n.done"
    wait_for_file "$TMP/scan-$n.done"
    n=$((n + 1))
done
printf '\\q\n' >&3
printf '\\q\n' >&4
exec 3>&-
exec 4>&-
wait "$WRITER" || fail "unrelated writer session failed"
WRITER=
wait "$SCANNER" || fail "IndexScan session failed"
SCANNER=

timing_count=$((n - 1))
[ "$timing_count" -eq "$SCANS" ] || fail "missing INSERT timing records"
search_stats=$(awk '{value = $1 + 0; sum += value; if (value > max) max = value; if (NR == 1 || value < min) min = value} END {printf "mean %.3f ms, max %.3f ms", sum / NR, max}' "$TMP"/search-*.done)
insert_stats=$(awk '{value = $1 + 0; sum += value; if (value > max) max = value; if (NR == 1 || value < min) min = value} END {printf "mean %.3f ms, max %.3f ms", sum / NR, max}' "$TMP"/insert-*.done)
written_rows=$(run -c 'select id from unrelated_writes' | wc -l | tr -d '[:space:]')
[ "$written_rows" = "$SCANS" ] \
    || fail "some unrelated INSERTs were lost"

restore_output=$(control <<'EOF'
(setf (symbol-function 'btree-collect-equal)
      *contention-original-btree-collect-equal*
      (symbol-function 'register-relation-writer) *contention-original-register-writer*
      (symbol-function 'commit-transaction) *contention-original-commit-transaction*
      (symbol-function 'exec-select) *contention-original-exec-select*)
(quit)
EOF
) || fail "could not restore contention instrumentation"
echo "$restore_output" | grep -Eq ';; (read )?error:' \
    && fail "contention instrumentation restore failed"

printf 'index-lock-contention: %d synchronized duplicate-key scans over %d rows; B-tree search %s; unrelated INSERT commit completion from search start %s\n' \
    "$SCANS" "$ROWS" "$search_stats" "$insert_stats"
