#!/bin/sh
# t/join-streaming.sh -- outer-join semantics and bounded LIMIT execution.
set -eu

BASE=$(cd "$(dirname "$0")/.." && pwd)
SBCL=${SBCL:-sbcl}
PORT=${PORT:-5478}
CONTROL_PORT=${CONTROL_PORT:-$((PORT + 2000))}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pg-join-streaming.XXXXXX")
LOG="$TMP/server.log"
SRV=
. "$BASE/t/test-support.sh"
test_debug_setup join-streaming "$PORT" "$TMP" "$LOG"

cleanup() {
    [ -z "$SRV" ] || kill "$SRV" 2>/dev/null || true
    [ -z "$SRV" ] || wait "$SRV" 2>/dev/null || true
    rm -rf "$TMP"
}
test_debug_install_traps cleanup

fail() {
    echo "join-streaming: FAIL $1" >&2
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

run -c "create table join_left (id int)" >/dev/null
run -c "create table join_right (id int)" >/dev/null
run -c "insert into join_left values (1), (2), (3)" >/dev/null
run -c "insert into join_right values (2), (3), (4)" >/dev/null
run -c "create table join_limit_left (id int)" >/dev/null
run -c "create table join_limit_right (id int)" >/dev/null
awk 'BEGIN {
    printf "insert into join_limit_left values "
    for (i = 1; i <= 100; i++) printf "%s(%d)", i == 1 ? "" : ", ", i
    print ";"
}' > "$TMP/left.sql"
awk 'BEGIN {
    printf "insert into join_limit_right values "
    for (i = 1; i <= 100; i++) printf "%s(%d)", i == 1 ? "" : ", ", i
    print ";"
}' > "$TMP/right.sql"
run < "$TMP/left.sql" > "$TMP/left.out" 2>&1 \
    || fail "large left input insert failed"
run < "$TMP/right.sql" > "$TMP/right.out" 2>&1 \
    || fail "large right input insert failed"
run -c "insert into join_limit_left values (NULL)" >/dev/null

install_output=$(control <<'EOF'
(defvar *join-limit-original-eval-expr* (symbol-function 'eval-expr))
(defvar *join-limit-original-offer-row* (symbol-function 'offer-select-top-n-row))
(defvar *join-limit-qual-evaluations* 0)
(defvar *join-limit-top-n-offers* 0)
(defvar *join-limit-top-n-max-size* 0)
(setf (symbol-function 'eval-expr)
      (lambda (expr env)
        (when (equal expr '(:bool t))
          (incf *join-limit-qual-evaluations*))
        (funcall *join-limit-original-eval-expr* expr env))
      (symbol-function 'offer-select-top-n-row)
      (lambda (collector row)
        (let ((result (funcall *join-limit-original-offer-row* collector row)))
          (incf *join-limit-top-n-offers*)
          (setf *join-limit-top-n-max-size*
                (max *join-limit-top-n-max-size*
                     (length (select-top-n-collector-heap collector))))
          result)))
(quit)
EOF
) || fail "could not install JOIN evaluation counter"
echo "$install_output" | grep -Eq ';; (read )?error:' \
    && fail "JOIN evaluation counter setup failed"

{
    printf 'inner\n'
    run -c 'select l.id, r.id from join_left l join join_right r on l.id = r.id'
    printf 'left\n'
    run -c 'select l.id, r.id from join_left l left join join_right r on l.id = r.id'
    printf 'right\n'
    run -c 'select l.id, r.id from join_left l right join join_right r on l.id = r.id'
    printf 'full\n'
    run -c 'select l.id, r.id from join_left l full join join_right r on l.id = r.id'
    printf 'where-after-on\n'
    run -c 'select l.id from join_left l left join join_right r on l.id = r.id where r.id is null'
    printf 'limit\n'
    run -c 'select l.id from join_limit_left l cross join join_limit_right r limit 1'
    printf 'ordered-asc-limit\n'
    run -c 'select l.id from join_limit_left l cross join join_limit_right r order by l.id limit 1'
    printf 'ordered-desc-limit\n'
    run -c 'select l.id from join_limit_left l cross join join_limit_right r where l.id is not null order by l.id desc limit 1'
} > "$TMP/results.out" 2>&1 || fail "JOIN query failed"

cat > "$TMP/expected.out" <<'EOF'
inner
2|2
3|3
left
1|
2|2
3|3
right
2|2
3|3
|4
full
1|
2|2
3|3
|4
where-after-on
1
limit
1
ordered-asc-limit
1
ordered-desc-limit
100
EOF
if ! cmp -s "$TMP/expected.out" "$TMP/results.out"; then
    diff -u "$TMP/expected.out" "$TMP/results.out" >&2 || true
    fail "JOIN results or outer-join WHERE semantics differed"
fi

counter_output=$(control <<'EOF'
(if (and (= *join-limit-qual-evaluations* 20201)
         (= *join-limit-top-n-offers* 20100)
         (= *join-limit-top-n-max-size* 1))
    :ORDERED-CANDIDATES-CONSUMED
    (list :QUALIFICATIONS *join-limit-qual-evaluations*
          :TOP-N-OFFERS *join-limit-top-n-offers*
          :MAX-HEAP-SIZE *join-limit-top-n-max-size*))
(quit)
EOF
) || fail "could not inspect JOIN LIMIT counter"
echo "$counter_output" | grep -q ':ORDERED-CANDIDATES-CONSUMED' \
    || fail "ordered LIMIT failed to consume both join streams within its heap bound: $counter_output"
echo "$counter_output" | grep -Eq ';; (read )?error:' \
    && fail "JOIN LIMIT counter inspection failed"

restore_output=$(control <<'EOF'
(setf (symbol-function 'eval-expr) *join-limit-original-eval-expr*)
(setf (symbol-function 'offer-select-top-n-row) *join-limit-original-offer-row*)
(quit)
EOF
) || fail "could not restore JOIN expression evaluator"
echo "$restore_output" | grep -Eq ';; (read )?error:' \
    && fail "JOIN instrumentation restore failed"

echo "join-streaming: PASS"
